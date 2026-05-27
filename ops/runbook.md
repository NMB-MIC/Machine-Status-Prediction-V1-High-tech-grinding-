Prereqs:
- Docker, kind, kubectl installed on the server
- Artifacts placed under service/models/

1) Start Kafka (PLAINTEXT localhost)
- cd kafka
- docker compose up -d
- ./create-topics.sh
- Verify topics exist: docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

2) Start kind and namespace (skip if already running)
- kind get clusters # check first
- kind create cluster --name kind # only if not listed
- kubectl create namespace ml --dry-run=client -o yaml | kubectl apply -f -

3) Build and load service image
- cd service
- docker build -t alert-eta-service:v2 .
- kind load docker-image alert-eta-service:v2 --name kind

4) Deploy service
- kubectl apply -n ml -f ../k8s-kind/deployment.yaml

5) Check service
- kubectl -n ml get pods
- kubectl -n ml port-forward svc/alert-eta-service 8080:8080
- curl http://localhost:8080/health

6) Run PyFlink job
- Ensure flink-connector-kafka jar is in $FLINK_HOME/lib
- cd flink
- python3 -m venv venv && . venv/bin/activate
- pip install -r requirements.txt
- python job.py

 Test if Flink work
 
- After running job.py without any problem, try:
- In a new terminal, try: docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot.machine.status.raw --property "parse.key=true" --property "key.separator=:"
- try send: ffl-07-2:{"event_id":"test-1","mc_no":"ffl-07-2","occurred_ts":"2026-02-18T10:00:00Z","mc_status":"alarm","ingest_ts":"2025-10-18T10:00:01Z","schema_version":1}
- In another terminal, try consume: docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ml.pred.alert.eta --
from-beginning --max-messages 1
- If you see a JSON with "eta_p50_sec", "eta_p90_sec", "next_type" fields → Flink + Service pipeline works correctly! Proceed to next step.
- If nothing appears after ~10s, check:
  - port-forward is running (step 5)
  - Flink logs for HTTP errors
  - curl http://localhost:8080/health returns ok

7) Replay a day (or full) for shadow mode
- cd replay
- python3 -m venv venv && source venv/bin/activate
- pip install -r requirements.txt
- python replay.py --input /home/micml/Documents/TestML/DATA_MCSTATUS_ASSY_202601300925.csv --bootstrap localhost:9092 --topic iot.machine.status.raw --sleep 0.0

8) Validate predictions (consumer or UI)
- Consume the first 10 prediction records: docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ml.pred.alert.eta --from-beginning --max-messages 10
- Consume ml.pred.alert.eta and check fields:
  - P50 < P90, timestamps, type_conf policy, next_type/type_conf = null if conf < 0.6 (by design)
- Monitor service /metrics and Flink logs

9) Monitor during shadow mode
- cd dashboard
- python3 -m venv venv && source venv/bin/activate
- pip install -r requirements.txt
- Run:
    export KAFKA_BOOTSTRAP=localhost:9092
    export KAFKA_TOPIC=ml.pred.alert.eta
    export BUFFER_HOURS=87600 # for replay
    export BUFFER_HOURS=24 # for live
    streamlit run app.py --server.port 8503

10) Live mode
- cd mqtt_to_ml_kafka
# Edit .env — set correct MQTT_BROKER IP and KAFKA_SERVER IP
- docker build --no-cache -t mic/mqtt_ml_kafka:1.0.0 .
- docker compose up -d
- docker logs mqtt_to_ml_kafka -f   # watch for "Connected" and message counts

11) Terminal Live Prediction Viewer
Quick version (raw JSON):
- docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ml.pred.alert.eta \
  --from-beginning
Pretty version
- python3 -m venv venv && source venv/bin/activate
- pip install -r requirements.txt
- python live_monitor.py

12) Verification sequence
- Start Kafka, Kind, Service, Flink (runbook steps 1-6)
- Start MQTT bridge:       docker compose up -d (step 10)
- Start terminal monitor:  python live_monitor.py (step 11)
- Watch for first predictions (should appear within seconds of machines sending status)
- If predictions flow → start dashboard:  streamlit run app.py --server.port 8501 (step 9)

Acceptance gates (shadow mode, rolling):
- ETA: MedAE ≤ ~60s; Hit@±5m ≥ 75%; Hit@±10m ≥ 80%
- Coverage: P90 88–93% overall and per machine
- P90 per-machine (n≥200) ≥ 84%
- Type: display only if conf ≥ 0.6
- Service p95 latency < 100 ms; error rate < 1%

Nudge rule (per-machine P90):
- If P90 coverage < 88% or > 95% for 3 consecutive days for a machine:
  - Increase/decrease its multiplier by 2–5% in artifacts (config map)
  - Redeploy service