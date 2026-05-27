# flink/job.py
import os
import json
import time
import math
import pickle
import requests
from datetime import datetime, timezone, timedelta

from pyflink.common import Types, Duration, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
    KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor

# ---------- Config ----------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN  = "iot.machine.status.raw"
TOPIC_OUT = "ml.pred.alert.eta"
TOPIC_DLQ = "iot.machine.status.dlq"

# SERVICE_URL = os.environ.get("SERVICE_URL", "http://localhost:8080/infer")
SERVICE_URL = os.environ.get("SERVICE_URL", "http://172.18.0.2:30080/infer")

WATERMARK_LATENESS_MIN = 3
VERY_LATE_MAX_AGE_MIN  = 120   # 2 hours

# Everything except 'run' is an alert (matches training: is_alert = mc_status != 'run')
NON_ALERT_STATUSES = {"run"}

# One-hot keys matching sanitized training column names
STATUS_KEYS     = ["alarm", "fullwork", "m_c_stop", "no_work", "run"]
LAST_ALERT_KEYS = ["alarm", "fullwork", "m_c_stop", "no_work", "none"]


def sanitize_status(s: str) -> str:
    """Match training column naming: spaces → _, / → _"""
    return s.replace(' ', '_').replace('/', '_')


def parse_iso_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


class FeatureState:
    def __init__(self):
        self.last_status              = None
        self.last_status_change_ts    = None
        self.last_alert_ts            = None
        self.last_alert_type          = "none"   # NEW: tracks previous alert's type
        self.consecutive_same_status  = 1        # NEW: same-status run counter
        self.inter_alert = None
        self.lag1 = None
        self.lag2 = None
        self.lag3 = None
        self.alerts15 = []
        self.alerts60 = []
        self.events15 = []
        self.events60 = []

    def prune(self, ts: datetime):
        cutoff15 = ts - timedelta(minutes=15)
        cutoff60 = ts - timedelta(minutes=60)
        self.alerts15 = [t for t in self.alerts15 if t >= cutoff15]
        self.alerts60 = [t for t in self.alerts60 if t >= cutoff60]
        self.events15 = [t for t in self.events15 if t >= cutoff15]
        self.events60 = [t for t in self.events60 if t >= cutoff60]


class FeatureBuilder(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        desc = ValueStateDescriptor("fb_state", Types.PICKLED_BYTE_ARRAY())
        self.state = runtime_context.get_state(desc)

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        mc_no      = value.get("mc_no")
        status_raw = str(value.get("mc_status", "")).strip().lower()
        occurred_ts = parse_iso_utc(value.get("occurred_ts"))

        # ── Very-late event → DLQ ──
        current_wm = ctx.timer_service().current_watermark()
        if current_wm > 0:
            wm_dt = datetime.fromtimestamp(current_wm / 1000.0, tz=timezone.utc)
            if occurred_ts < (wm_dt - timedelta(minutes=VERY_LATE_MAX_AGE_MIN)):
                dlq = {
                    "dead_event": value,
                    "reason": "very_late",
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "original_topic": TOPIC_IN,
                    "schema_version": 1
                }
                yield ("DLQ", json.dumps(dlq))
                return

        raw = self.state.value()
        st = FeatureState() if raw is None else pickle.loads(raw)

        # ── Consecutive same-status counter (BEFORE updating last_status) ──
        if st.last_status is not None and status_raw == st.last_status:
            st.consecutive_same_status += 1
        else:
            st.consecutive_same_status = 1

        # ── Status change tracking ──
        if st.last_status != status_raw:
            st.last_status = status_raw
            st.last_status_change_ts = occurred_ts

        # ── Event windows ──
        st.events15.append(occurred_ts)
        st.events60.append(occurred_ts)

        # ── Alert detection ──
        is_alert = status_raw not in NON_ALERT_STATUSES

        # Capture last_alert_type BEFORE updating (matches training shift(1).ffill())
        feat_last_alert_type = st.last_alert_type

        if is_alert:
            if st.last_alert_ts is not None:
                interval = (occurred_ts - st.last_alert_ts).total_seconds()
                st.lag3 = st.lag2
                st.lag2 = st.lag1
                st.lag1 = st.inter_alert
                st.inter_alert = interval
            st.last_alert_ts = occurred_ts
            st.alerts15.append(occurred_ts)
            st.alerts60.append(occurred_ts)
            # Update AFTER capturing for features
            st.last_alert_type = status_raw

        # ── Prune windows ──
        st.prune(occurred_ts)

        # ── Compute features ──
        time_since_last_alert = (
            (occurred_ts - st.last_alert_ts).total_seconds()
            if st.last_alert_ts else -1.0
        )
        time_since_status_change = (
            (occurred_ts - st.last_status_change_ts).total_seconds()
            if st.last_status_change_ts else -1.0
        )

        hour    = occurred_ts.hour
        minute  = occurred_ts.minute
        weekday = occurred_ts.weekday()

        alerts_15m = float(len(st.alerts15))
        alerts_60m = float(len(st.alerts60))
        events_15m = float(len(st.events15))
        events_60m = float(len(st.events60))

        feats = {
            # Calendar (11)
            "hour": hour,
            "minute": minute,
            "weekday": weekday,
            "is_weekend": 1 if weekday >= 5 else 0,
            "is_night": 1 if hour in [0,1,2,3,4,5,22,23] else 0,
            "hour_sin": math.sin(2 * math.pi * hour / 24.0),
            "hour_cos": math.cos(2 * math.pi * hour / 24.0),
            "min_sin":  math.sin(2 * math.pi * minute / 60.0),
            "min_cos":  math.cos(2 * math.pi * minute / 60.0),
            "wday_sin": math.sin(2 * math.pi * weekday / 7.0),
            "wday_cos": math.cos(2 * math.pi * weekday / 7.0),
            # Temporal (3)
            "time_since_last_alert": float(time_since_last_alert),
            "time_since_status_change": float(time_since_status_change),
            "consecutive_same_status": float(st.consecutive_same_status),
            # Inter-alert (4)
            "inter_alert":      float(st.inter_alert) if st.inter_alert is not None else None,
            "inter_alert_lag1": float(st.lag1) if st.lag1 is not None else None,
            "inter_alert_lag2": float(st.lag2) if st.lag2 is not None else None,
            "inter_alert_lag3": float(st.lag3) if st.lag3 is not None else None,
            # Rolling (6)
            "alerts_15m": alerts_15m,
            "alerts_60m": alerts_60m,
            "events_15m": events_15m,
            "events_60m": events_60m,
            "alert_rate_15m": alerts_15m / max(1.0, events_15m),
            "alert_rate_60m": alerts_60m / max(1.0, events_60m),
        }

        # ── Status one-hot (5 columns, sanitized) ──
        status_san = sanitize_status(status_raw)
        for key in STATUS_KEYS:
            feats[f"status_{key}"] = 1.0 if status_san == key else 0.0

        # ── Last alert type one-hot (5 columns, sanitized) ──
        last_alert_san = sanitize_status(feat_last_alert_type)
        for key in LAST_ALERT_KEYS:
            feats[f"last_alert_{key}"] = 1.0 if last_alert_san == key else 0.0

        # NOTE: mc_median_gap, mc_alert_ratio, mc_event_rate, in_shift, activity_score
        # are computed by the service from artifact lookup maps (not Flink's job)

        out = {
            "event_id": value.get("event_id"),
            "mc_no": mc_no,
            "occurred_ts": value.get("occurred_ts"),
            "mc_status": status_raw,
            "features": feats
        }

        self.state.update(pickle.dumps(st))
        yield ("OK", json.dumps(out))


class HttpInferMap(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        self.session = requests.Session()
        self.timeout = 1.5

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def map(self, value):
        tag, payload = value
        if tag == "DLQ":
            return ("DLQ", payload)
        try:
            data = json.loads(payload)
            req = {
                "mc_no": data["mc_no"],
                "occurred_ts": data["occurred_ts"],
                "features": data["features"]
            }
            resp = self.session.post(SERVICE_URL, json=req, timeout=self.timeout)
            if resp.status_code != 200:
                dlq = {
                    "dead_event": data,
                    "reason": f"service_{resp.status_code}",
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "original_topic": TOPIC_IN,
                    "schema_version": 1
                }
                return ("DLQ", json.dumps(dlq))
            pred = resp.json()
            pred_rec = {
                "pred_id": f"{data.get('event_id','')}-{int(time.time()*1000)}",
                "source_event_id": data.get("event_id"),
                "mc_no": data["mc_no"],
                "mc_status": data.get("mc_status"),
                "now_ts": datetime.now(timezone.utc).isoformat(),
                **pred
            }
            return ("OUT", json.dumps(pred_rec))
        except Exception as e:
            dlq = {
                "dead_event": payload,
                "reason": f"exception:{e}",
                "received_at": datetime.now(timezone.utc).isoformat(),
                "original_topic": TOPIC_IN,
                "schema_version": 1
            }
            return ("DLQ", json.dumps(dlq))


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(8)

    jar_path = os.environ.get("FLINK_KAFKA_JAR", "")
    if jar_path:
        if not jar_path.startswith("file://"):
            jar_path = "file://" + os.path.abspath(jar_path)
        env.add_jars(jar_path)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_topics(TOPIC_IN) \
        .set_group_id("flink-alert-eta") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()

    def ts_assigner(e, ts):
        try:
            obj = json.loads(e)
            return int(parse_iso_utc(obj["occurred_ts"]).timestamp() * 1000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1000)

    wm = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_minutes(WATERMARK_LATENESS_MIN)) \
        .with_timestamp_assigner(ts_assigner)

    ds = env.from_source(source, wm, "kafka-source")
    parsed = ds.map(lambda s: json.loads(s))
    keyed = parsed.key_by(lambda d: d["mc_no"])

    fb = keyed.process(
        FeatureBuilder(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )

    mapped = fb.map(
        HttpInferMap(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )

    outs = mapped.filter(lambda t: t[0] == "OUT").map(
        lambda t: t[1], output_type=Types.STRING()
    )
    dlqs = mapped.filter(lambda t: t[0] == "DLQ").map(
        lambda t: t[1], output_type=Types.STRING()
    )

    sink_out = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_OUT)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    sink_dlq = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_DLQ)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    outs.sink_to(sink_out)
    dlqs.sink_to(sink_dlq)

    env.execute("alert-eta-stream")


if __name__ == "__main__":
    main()