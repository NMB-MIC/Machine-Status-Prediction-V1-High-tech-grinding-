# service/app.py
import os
import time
import math
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from prometheus_fastapi_instrumentator import Instrumentator

# ---------- Config ----------
MODEL_DIR       = os.environ.get("MODEL_DIR", "/models")
P50_MODEL_PATH  = os.environ.get("P50_MODEL_PATH",  f"{MODEL_DIR}/lgbm_quantile_p50_final.pkl")
P90_MODEL_PATH  = os.environ.get("P90_MODEL_PATH",  f"{MODEL_DIR}/lgbm_quantile_p90_final.pkl")
TYPE_MODEL_PATH = os.environ.get("TYPE_MODEL_PATH", f"{MODEL_DIR}/lgbm_next_type.pkl")
ARTIFACTS_PATH  = os.environ.get("ARTIFACTS_PATH",  f"{MODEL_DIR}/artifacts_phase4.pkl")

MODEL_VERSION   = os.environ.get("MODEL_VERSION",   "2025-01-15_p50p90_v2")
FEATURE_VERSION = os.environ.get("FEATURE_VERSION", "feats_39_behavioral_v2")

TYPE_CONF_THRESHOLD = float(os.environ.get("TYPE_CONF_THRESHOLD", "0.6"))
P90_UI_CAP_MIN      = int(os.environ.get("P90_UI_CAP_MIN", "60"))
MIN_MARGIN_SECS     = float(os.environ.get("MIN_MARGIN_SECS", "5.0"))

# ---------- Load models & artifacts ----------
try:
    p50_model  = joblib.load(P50_MODEL_PATH)
    p90_model  = joblib.load(P90_MODEL_PATH)
    type_model = joblib.load(TYPE_MODEL_PATH)
    artifacts  = joblib.load(ARTIFACTS_PATH)
except Exception as e:
    raise RuntimeError(f"Failed to load models/artifacts: {e}")

# ── Core schema ──
feature_cols:    List[str]       = artifacts['feature_cols']
status_cols:     List[str]       = artifacts['status_cols']
last_alert_cols: List[str]       = artifacts['last_alert_cols']
classes:         List[str]       = artifacts.get('classes', ['alarm', 'fullwork', 'm/c stop', 'no work'])

# ── NaN filling ──
per_mc_medians: pd.DataFrame = artifacts['per_mc_medians']
global_medians: pd.Series    = artifacts['global_medians']

# ── P50 / P90 calibration ──
p50_scale_by_mc:  Dict[str, float] = artifacts.get('p50_scale_by_mc', {})
p90_mult_global:  float            = artifacts.get('p90_multiplier_global', 1.0)
p90_mult_by_mc:   Dict[str, float] = artifacts.get('p90_multiplier_by_mc', {})

# ── Per-machine guardrails ──
floor_by_mc:   Dict[str, float] = artifacts.get('floor_by_mc', {})
cap_by_mc:     Dict[str, float] = artifacts.get('cap_by_mc', {})
med_by_mc:     Dict[str, float] = artifacts.get('med_by_mc', {})
ev_rate_by_mc: Dict[str, float] = artifacts.get('ev_rate_by_mc', {})

# ── Machine behavioral feature maps (NEW) ──
mc_median_gap_map:  Dict[str, float]           = artifacts.get('mc_median_gap_map', {})
mc_alert_ratio_map: Dict[str, float]           = artifacts.get('mc_alert_ratio_map', {})
mc_event_rate_map:  Dict[str, float]           = artifacts.get('mc_event_rate_map', {})
activity_rate_map:  Dict[str, Dict[int, float]] = artifacts.get('activity_rate_map', {})
shift_thr_by_mc:    Dict[str, float]           = artifacts.get('shift_thr_by_mc', {})

# ── Global fallbacks for unseen machines (NEW) ──
global_floor:        float            = artifacts.get('global_floor', 5.0)
global_cap:          float            = artifacts.get('global_cap', 3600.0)
global_median:       float            = artifacts.get('global_median', 60.0)
global_event_rate:   float            = artifacts.get('global_event_rate', 0.1)
global_median_gap:   float            = artifacts.get('global_median_gap', 80.0)
global_alert_ratio:  float            = artifacts.get('global_alert_ratio', 0.5)
global_shift_thr:    float            = artifacts.get('global_shift_thr', 0.0)
global_activity:     Dict[int, float] = artifacts.get('global_activity', {})
clip_max:            float            = artifacts.get('clip_max', 2402.0)

# ---------- FastAPI app ----------
app = FastAPI(title="Alert ETA Service", version=MODEL_VERSION)
Instrumentator().instrument(app).expose(app)


# ---------- Request / Response schemas ----------
class InferRequest(BaseModel):
    mc_no: str = Field(..., description="Machine ID")
    occurred_ts: str = Field(..., description="Event time (UTC ISO-8601)")
    features: Dict[str, Any] = Field(..., description="Feature dict from Flink (29 dynamic features)")


class InferResponse(BaseModel):
    eta_p50_sec: float
    eta_p90_sec: float
    eta_p50_ts: str
    eta_p90_ts: str
    next_type: Optional[str]
    type_conf: Optional[float]
    model_version: str
    feature_version: str


# ---------- Helpers ----------
def _parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _compute_lookup_features(mc_no: str, hour: int) -> Dict[str, float]:
    """
    Compute the 5 features that are static lookups from artifacts,
    not tracked by Flink state. Uses global fallbacks for unseen machines.
    """
    # Machine behavioral (3)
    mg = mc_median_gap_map.get(mc_no, global_median_gap)
    ar = mc_alert_ratio_map.get(mc_no, global_alert_ratio)
    er = mc_event_rate_map.get(mc_no, global_event_rate)

    # In-shift / activity score (2)
    act_profile = activity_rate_map.get(mc_no, global_activity)
    thr = shift_thr_by_mc.get(mc_no, global_shift_thr)
    act_score = act_profile.get(int(hour), 0.0) if isinstance(act_profile, dict) else 0.0
    in_shift = 1.0 if act_score >= thr else 0.0

    return {
        'mc_median_gap':  mg,
        'mc_alert_ratio': ar,
        'mc_event_rate':  er,
        'in_shift':       in_shift,
        'activity_score': act_score,
    }


def _build_df(mc_no: str, feats: Dict[str, Any]) -> pd.DataFrame:
    """
    Merge Flink's dynamic features with service-side lookup features,
    then align to feature_cols with NaN filling.
    """
    # Add lookup features (overwrite if Flink accidentally sent them)
    hour = int(feats.get('hour', 0))
    lookup = _compute_lookup_features(mc_no, hour)
    merged = {**feats, **lookup}

    # Build single-row DataFrame aligned to training feature_cols
    row = {c: merged.get(c, np.nan) for c in feature_cols}
    X = pd.DataFrame([row], columns=feature_cols)

    # Per-machine median fill, then global, then zero
    if mc_no in per_mc_medians.index:
        X = X.fillna(per_mc_medians.loc[mc_no])
    X = X.fillna(global_medians)
    X = X.fillna(0.0)
    return X


def _calibrate_p50(mc_no: str, secs: float) -> float:
    scale = p50_scale_by_mc.get(mc_no, 1.0)
    return max(0.0, secs * scale)


def _calibrate_p90(mc_no: str, p50_secs: float, p90_raw: float, cap: float) -> float:
    mult = p90_mult_by_mc.get(mc_no, p90_mult_global)
    p90 = p90_raw * mult
    min_margin = max(MIN_MARGIN_SECS, 0.05 * p50_secs)
    p90 = max(p50_secs + min_margin, p90)
    p90 = min(p90, cap)
    return p90


# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_version": MODEL_VERSION,
        "feature_version": FEATURE_VERSION,
        "num_features": len(feature_cols),
        "classes": classes,
    }


@app.post("/infer", response_model=InferResponse)
def infer(req: InferRequest):
    try:
        mc_no    = req.mc_no
        occurred = _parse_utc(req.occurred_ts)

        X = _build_df(mc_no, req.features)

        # ── P50 ──
        p50_raw = float(p50_model.predict(X)[0])
        p50 = _calibrate_p50(mc_no, p50_raw)

        floor = float(floor_by_mc.get(mc_no, global_floor))
        cap   = float(cap_by_mc.get(mc_no, global_cap))
        p50   = max(floor, min(p50, cap))

        # ── P90 ──
        p90_raw = float(p90_model.predict(X)[0])
        p90 = _calibrate_p90(mc_no, p50, p90_raw, cap)

        # ── Type ──
        proba    = type_model.predict_proba(X)[0]
        type_idx = int(np.argmax(proba))
        type_name = classes[type_idx] if type_idx < len(classes) else None
        type_conf = float(np.max(proba))

        # ── Timestamps ──
        eta_p50_ts = (occurred + timedelta(seconds=p50)).isoformat()
        eta_p90_ts = (occurred + timedelta(seconds=p90)).isoformat()

        # ── Confidence policy ──
        if type_conf < TYPE_CONF_THRESHOLD:
            type_name_display = None
            type_conf_display = None
        else:
            type_name_display = type_name
            type_conf_display = type_conf

        return InferResponse(
            eta_p50_sec=p50,
            eta_p90_sec=p90,
            eta_p50_ts=eta_p50_ts,
            eta_p90_ts=eta_p90_ts,
            next_type=type_name_display,
            type_conf=type_conf_display,
            model_version=MODEL_VERSION,
            feature_version=FEATURE_VERSION,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Inference failed: {e}")