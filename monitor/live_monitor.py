# live_monitor.py — Terminal live prediction monitor
import json
import os
from datetime import datetime
from kafka import KafkaConsumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC     = os.environ.get("KAFKA_TOPIC", "ml.pred.alert.eta")
HIDE      = {"no work", "run"}
CONF_THR  = 0.6

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=f"live-monitor-{int(datetime.now().timestamp())}",
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=-1,  # block forever
)

print(f"{'='*80}")
print(f"  LIVE PREDICTION MONITOR")
print(f"  Topic: {TOPIC}  |  Bootstrap: {BOOTSTRAP}")
print(f"  Hiding: {HIDE}  |  Conf threshold: {CONF_THR}")
print(f"{'='*80}\n")

count = 0
shown = 0

for msg in consumer:
    count += 1
    p = msg.value

    mc_no     = p.get("mc_no", "?")
    mc_status = p.get("mc_status", "?")
    p50       = p.get("eta_p50_sec", 0)
    p90       = p.get("eta_p90_sec", 0)
    p50_ts    = p.get("eta_p50_ts", "")
    nxt       = p.get("next_type")
    conf      = p.get("type_conf")

    # Skip non-actionable
    if nxt and nxt.lower() in HIDE:
        continue

    shown += 1

    # Format type display
    if nxt and conf and conf >= CONF_THR:
        type_str = f"{nxt} (p={conf:.2f})"
    elif nxt is None:
        type_str = "[uncertain]"
    else:
        type_str = f"[hidden, p={conf:.2f}]"

    # Format ETA
    eta_min = p50 / 60
    p90_min = p90 / 60

    print(
        f"  {mc_no:<12} | "
        f"status={mc_status:<10} | "
        f"ETA={eta_min:5.1f}m (P90={p90_min:5.1f}m) | "
        f"type={type_str:<25} | "
        f"alert_at={p50_ts[:19]}"
    )

    # Summary every 50 shown
    if shown % 50 == 0:
        print(f"\n  --- {shown} actionable / {count} total predictions ---\n")