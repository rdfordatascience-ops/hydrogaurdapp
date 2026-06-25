import json
import os
import random
import time
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


SENSOR_STATIONS = [
    {"id": "SENS-BLR-01", "name": "Varthur Intake Main", "coords": "12.9406,77.7466"},
    {"id": "SENS-BLR-02", "name": "Hebbal Reservoir Feed", "coords": "13.0359,77.5978"},
    {"id": "SENS-BLR-03", "name": "Kavery Treatment Stage-2", "coords": "12.9224,77.5020"},
]

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "telemetry.water.sensors")
PUBLISH_INTERVAL_SECONDS = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "1.0"))


def build_kafka_config():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap_servers:
        return None

    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": os.getenv("KAFKA_CLIENT_ID", "hydroguard-simulator"),
        "acks": os.getenv("KAFKA_ACKS", "all"),
        "retries": int(os.getenv("KAFKA_RETRIES", "5")),
        "linger.ms": int(os.getenv("KAFKA_LINGER_MS", "100")),
        "enable.idempotence": os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() == "true",
    }

    username = os.getenv("KAFKA_API_KEY") or os.getenv("KAFKA_USERNAME")
    password = os.getenv("KAFKA_API_SECRET") or os.getenv("KAFKA_PASSWORD")
    if username and password:
        config.update(
            {
                "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
                "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
                "sasl.username": username,
                "sasl.password": password,
            }
        )
    else:
        config["security.protocol"] = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

    return config


def ensure_topic(config):
    if os.getenv("KAFKA_CREATE_TOPIC", "false").lower() != "true":
        return

    partitions = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))
    replication_factor = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "3"))
    admin = AdminClient(config)
    topic = NewTopic(TOPIC_NAME, num_partitions=partitions, replication_factor=replication_factor)
    futures = admin.create_topics([topic])

    try:
        futures[TOPIC_NAME].result(timeout=30)
        print(f"Created Kafka topic: {TOPIC_NAME}")
    except Exception as exc:
        if "already exists" in str(exc).lower():
            print(f"Kafka topic already exists: {TOPIC_NAME}")
        else:
            raise


def delivery_report(error, message):
    if error is not None:
        print(f"Kafka delivery failed: {error}")
        return

    print(
        f"Delivered to {message.topic()} [{message.partition()}] "
        f"offset {message.offset()}"
    )


def generate_telemetry_payload():
    station = random.choice(SENSOR_STATIONS)
    is_anomaly = random.random() < 0.15

    if is_anomaly:
        turbidity = round(random.uniform(15.0, 45.0), 2)
        fluorescence = round(random.uniform(55.0, 120.0), 2)
        estimated_mpn = round((fluorescence * 1.8) + (turbidity * 0.5), 1)
    else:
        turbidity = round(random.uniform(0.5, 2.8), 2)
        fluorescence = round(random.uniform(1.0, 8.5), 2)
        estimated_mpn = 0.0 if fluorescence < 5.0 else round(fluorescence * 0.1, 2)

    return {
        "sensor_id": station["id"],
        "station_name": station["name"],
        "location_coordinates": station["coords"],
        "ecoli_enzyme_fluorescence_rfu": fluorescence,
        "estimated_mpn_per_100ml": estimated_mpn,
        "turbidity_ntu": turbidity,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }


def publish(producer, payload):
    json_payload = json.dumps(payload).encode("utf-8")
    message_key = payload["sensor_id"].encode("utf-8")

    try:
        producer.produce(
            TOPIC_NAME,
            key=message_key,
            value=json_payload,
            on_delivery=delivery_report,
        )
    except BufferError:
        producer.poll(1)
        producer.produce(
            TOPIC_NAME,
            key=message_key,
            value=json_payload,
            on_delivery=delivery_report,
        )

    producer.poll(0)


def main():
    print("=" * 70)
    print("HydroGuard Real-Time E. Coli Bio-Sensor Simulator")
    print("=" * 70)

    producer = None
    kafka_config = build_kafka_config()
    if KAFKA_AVAILABLE and kafka_config:
        bootstrap_servers = kafka_config.get("bootstrap.servers")
        max_attempts = int(os.getenv("KAFKA_CONNECT_RETRIES", "15"))
        retry_interval = float(os.getenv("KAFKA_CONNECT_RETRY_INTERVAL_SECONDS", "5.0"))
        
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"Attempting to initialize Kafka and create topic (Attempt {attempt}/{max_attempts})...")
                ensure_topic(kafka_config)
                producer = Producer(kafka_config)
                print(f"Successfully connected to Kafka cluster: {bootstrap_servers}")
                print(f"Publishing to topic: {TOPIC_NAME}")
                break
            except Exception as exc:
                print(f"Kafka initialization attempt {attempt} failed: {exc}")
                if attempt < max_attempts:
                    print(f"Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                else:
                    print("All Kafka initialization attempts failed. Falling back to dry-run.")
    else:
        print("Running in dry-run mode. Set KAFKA_BOOTSTRAP_SERVERS to publish to Kafka.")

    try:
        while True:
            payload = generate_telemetry_payload()
            status = "[CONTAMINATION]" if payload["estimated_mpn_per_100ml"] > 10.0 else "[NORMAL]"
            print(
                f"{status} {payload['station_name']} | "
                f"Est. MPN: {payload['estimated_mpn_per_100ml']}/100mL | "
                f"Turbidity: {payload['turbidity_ntu']} NTU"
            )

            if producer:
                publish(producer, payload)

            time.sleep(PUBLISH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopping simulator...")
        if producer:
            producer.flush()


if __name__ == "__main__":
    main()
