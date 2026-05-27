# service/Dockerfile
FROM python:3.10-slim AS base

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt .
# Increased timeout to 1000s and upgraded pip first
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --default-timeout=1000 -r requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app.py /app/app.py

# Copy models/artifacts into the image (POC ease)
# Place your 4 files under service/models/ before build
COPY models /models

ENV MODEL_DIR=/models \
    P50_MODEL_PATH=/models/lgbm_quantile_p50_final.pkl \
    P90_MODEL_PATH=/models/lgbm_quantile_p90_final.pkl \
    TYPE_MODEL_PATH=/models/lgbm_next_type.pkl \
    ARTIFACTS_PATH=/models/artifacts_phase4.pkl \
    MODEL_VERSION=2025-01-15_p50p90_v2 \
    FEATURE_VERSION=feats_39_behavioral_v2 \
    TYPE_CONF_THRESHOLD=0.6 \
    P90_UI_CAP_MIN=60

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]