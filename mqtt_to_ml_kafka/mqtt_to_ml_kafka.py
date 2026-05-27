# mqtt_to_ml_kafka/mqtt_to_ml_kafka.py
from gmqtt import Client as MQTTClient
import os, asyncio, json, logging, pytz, uuid
import dotenv
from confluent_kafka import Producer
from queue import Queue
from threading import Thread
from datetime import datetime

class MqttToMlKafka:
    def __init__(self):
        dotenv.load_dotenv()
        self.client_id = f"ml-bridge-{uuid.uuid4().hex[:8]}"
        self.client = MQTTClient(self.client_id)
        self.client.set_config({'reconnect_retries': 10, 'reconnect_delay': 10})
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        # MQTT config
        self.mqtt_broker = os.environ["MQTT_BROKER"]
        self.mqtt_port = int(os.environ["MQTT_PORT"])
        self.mqtt_topic = os.environ.get("MQTT_SUB_TOPIC", "status/nhb/assy/#")

        # Kafka config
        self.kafka_server = os.environ["KAFKA_SERVER"]
        self.kafka_topic = os.environ.get("KAFKA_TOPIC", "iot.machine.status.raw")

        # Timezone
        self.tz = pytz.timezone(os.environ.get("TZ", "Asia/Bangkok"))

        self.producer = Producer({
            'bootstrap.servers': self.kafka_server,
            'batch.size': 50000,
            'linger.ms': 5,
            'queue.buffering.max.messages': 500000,
        })

        self.queue = Queue(maxsize=500000)
        Thread(target=self.kafka_producer, daemon=True).start()

        # Counter for logging
        self.msg_count = 0

        logging.basicConfig(
            filename='log/mqtt_to_ml_kafka.log',
            filemode="a",
            level=logging.WARNING,
            format='%(asctime)s - %(levelname)s - %(message)s',
            force=True
        )
        # Also log to stdout
        console = logging.StreamHandler()
        console.setLevel(logging.WARNING)
        logging.getLogger().addHandler(console)

    async def connect(self):
        await self.client.connect(self.mqtt_broker, self.mqtt_port)

    async def subscribe(self):
        self.client.subscribe(self.mqtt_topic)
        logging.warning(f"Subscribed to: {self.mqtt_topic}")
        print(f"Subscribed to: {self.mqtt_topic}")

    def on_connect(self, client, flags, rc, properties):
        if rc == 0:
            logging.warning("Connected to MQTT Broker")
            print("Connected to MQTT Broker")
        else:
            logging.error(f"MQTT connection failed with code {rc}")
            print(f"MQTT connection failed with code {rc}")

    def on_message(self, client, topic, payload, qos, properties):
        try:
            self.queue.put_nowait((payload, topic))
        except Exception as e:
            logging.error(f"Queue full or error: {e}")

    def on_disconnect(self, client, packet, exc=None):
        logging.error("Disconnected from MQTT Broker")
        print("Disconnected from MQTT Broker")

    async def start(self):
        await self.connect()
        await self.subscribe()
        print(f"ML Bridge running — MQTT({self.mqtt_broker}) → Kafka({self.kafka_server}/{self.kafka_topic})")
        await asyncio.Event().wait()

    def kafka_producer(self):
        while True:
            payload, topic = self.queue.get()
            try:
                # ── Parse MQTT topic: status/{plant}/{process}/{mc_no} ──
                parts = topic.split("/")
                if len(parts) != 4:
                    logging.error(f"Unexpected topic format: {topic}")
                    continue

                msg_type, plant, process, mc_no = parts

                # We only care about status messages
                if msg_type != "status":
                    continue

                # ── Parse payload: {"status": "RUN"} ──
                try:
                    data = json.loads(payload.decode())
                except Exception as e:
                    logging.error(f"Cannot decode payload from {topic}: {e}")
                    continue

                raw_status = data.get("status", "").strip().lower()
                if not raw_status:
                    logging.error(f"Empty status from {topic}: {data}")
                    continue

                # ── Build ML event schema ──
                now = datetime.now(self.tz)
                occurred_ts = now.isoformat()
                ingest_ts = now.isoformat()
                event_id = f"{mc_no}-{int(now.timestamp() * 1000)}"

                ml_event = {
                    "event_id": event_id,
                    "mc_no": mc_no,
                    "occurred_ts": occurred_ts,
                    "mc_status": raw_status,
                    "ingest_ts": ingest_ts,
                    "schema_version": 1
                }

                message = json.dumps(ml_event).encode("utf-8")
                key = mc_no.encode("utf-8")

                self.producer.produce(
                    topic=self.kafka_topic,
                    key=key,
                    value=message
                )
                self.producer.poll(0)

                self.msg_count += 1
                if self.msg_count % 1000 == 0:
                    print(f"Forwarded {self.msg_count} status events to {self.kafka_topic}")

            except Exception as e:
                logging.error(f"Error in kafka_producer: {e}")
            finally:
                self.queue.task_done()


async def main():
    bridge = MqttToMlKafka()
    await bridge.start()

if __name__ == "__main__":
    asyncio.run(main())