import json
import random
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "temperature-readings"
VARIATION_THRESHOLD = 0.5
SENSORS = {
    "sensor_01": 22.0,
    "sensor_02": 18.5,
    "sensor_03": 25.0,
}


def create_producer(retries: int = 10, delay: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"[Sensor] Kafka indisponivel {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Nao foi possivel conectar ao Kafka")


def simulate() -> None:
    producer = create_producer()
    temperatures = dict(SENSORS)

    while True:
        for sensor_id in list(temperatures):
            variation = random.uniform(-2.0, 2.0)
            if abs(variation) < VARIATION_THRESHOLD:
                continue

            new_temp = temperatures[sensor_id] + variation
            new_temp = round(max(10.0, min(45.0, new_temp)), 2)
            temperatures[sensor_id] = new_temp

            reading = {
                "sensor_id": sensor_id,
                "temperature": new_temp,
                "timestamp": int(time.time()),
            }

            producer.send(TOPIC, value=reading)
            print(f"[Sensor] {sensor_id} -> {new_temp:.2f}")

        producer.flush()
        time.sleep(random.uniform(2, 5))


if __name__ == "__main__":
    simulate()
