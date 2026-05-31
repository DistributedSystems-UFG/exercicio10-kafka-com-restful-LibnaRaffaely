import json
import time
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "temperature-readings"
OUTPUT_TOPIC = "temperature-averages"
CONSUMER_GROUP = "temperature-processor"
WINDOW_SECONDS = 2 * 3600


def create_consumer(retries: int = 10, delay: int = 3) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
        except NoBrokersAvailable:
            print(f"[Processor] Kafka indisponivel {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Nao foi possivel conectar ao Kafka")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def process() -> None:
    consumer = create_consumer()
    producer = create_producer()
    windows: dict[str, list[tuple[int, float]]] = defaultdict(list)

    for message in consumer:
        reading: dict = message.value
        sensor_id = reading["sensor_id"]
        temperature = reading["temperature"]
        timestamp = reading["timestamp"]

        windows[sensor_id].append((timestamp, temperature))

        cutoff = int(time.time()) - WINDOW_SECONDS
        windows[sensor_id] = [(ts, temp) for ts, temp in windows[sensor_id] if ts >= cutoff]

        window = windows[sensor_id]
        if not window:
            continue

        avg_temp = sum(temp for _, temp in window) / len(window)
        window_start = min(ts for ts, _ in window)
        window_end = max(ts for ts, _ in window)

        average_event = {
            "sensor_id": sensor_id,
            "average": round(avg_temp, 2),
            "window_start": window_start,
            "window_end": window_end,
            "sample_count": len(window),
        }

        producer.send(OUTPUT_TOPIC, value=average_event)
        producer.flush()
        print(f"[Processor] {sensor_id} media={avg_temp:.2f}")


if __name__ == "__main__":
    process()
