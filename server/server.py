import json
import os
import sqlite3
import threading
import time

from flask import Flask, jsonify, request
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
READINGS_TOPIC = "temperature-readings"
AVERAGES_TOPIC = "temperature-averages"
DB_PATH = os.path.join(os.path.dirname(__file__), "temperature.db")

app = Flask(__name__)


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT NOT NULL,
            temperature REAL NOT NULL,
            timestamp INTEGER NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS averages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT NOT NULL,
            average REAL NOT NULL,
            window_start INTEGER NOT NULL,
            window_end INTEGER NOT NULL,
            sample_count INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def insert_reading(reading: dict) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO readings (sensor_id, temperature, timestamp) VALUES (?, ?, ?)",
        (reading["sensor_id"], reading["temperature"], reading["timestamp"]),
    )
    conn.commit()
    conn.close()


def insert_average(avg: dict) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO averages (sensor_id, average, window_start, window_end, sample_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            avg["sensor_id"],
            avg["average"],
            avg["window_start"],
            avg["window_end"],
            avg["sample_count"],
        ),
    )
    conn.commit()
    conn.close()


def make_consumer(topic: str, group: str, retries: int = 15, delay: int = 3) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id=group,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
        except NoBrokersAvailable:
            print(f"[Server] Kafka indisponivel para {topic} {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Nao foi possivel conectar ao Kafka")


def consume_readings() -> None:
    consumer = make_consumer(READINGS_TOPIC, "server-readings-group")
    for message in consumer:
        reading = message.value
        insert_reading(reading)
        print(f"[Server] leitura {reading['sensor_id']}={reading['temperature']}")


def consume_averages() -> None:
    consumer = make_consumer(AVERAGES_TOPIC, "server-averages-group")
    for message in consumer:
        avg = message.value
        insert_average(avg)
        print(f"[Server] media {avg['sensor_id']}={avg['average']}")


def to_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/readings/latest")
def latest_reading():
    sensor_id = request.args.get("sensor_id", "").strip()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if sensor_id:
        c.execute(
            "SELECT sensor_id, temperature, timestamp FROM readings WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT 1",
            (sensor_id,),
        )
    else:
        c.execute("SELECT sensor_id, temperature, timestamp FROM readings ORDER BY timestamp DESC LIMIT 1")

    row = c.fetchone()
    conn.close()

    if not row:
        return jsonify({"error": "Nenhuma leitura encontrada"}), 404

    return jsonify({"sensor_id": row[0], "temperature": row[1], "timestamp": row[2]})


@app.get("/averages/latest")
def latest_average():
    sensor_id = request.args.get("sensor_id", "").strip()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if sensor_id:
        c.execute(
            "SELECT sensor_id, average, window_start, window_end, sample_count FROM averages WHERE sensor_id = ? ORDER BY window_end DESC LIMIT 1",
            (sensor_id,),
        )
    else:
        c.execute(
            "SELECT sensor_id, average, window_start, window_end, sample_count FROM averages ORDER BY window_end DESC LIMIT 1"
        )

    row = c.fetchone()
    conn.close()

    if not row:
        return jsonify({"error": "Nenhuma media encontrada"}), 404

    return jsonify(
        {
            "sensor_id": row[0],
            "average": row[1],
            "window_start": row[2],
            "window_end": row[3],
            "sample_count": row[4],
        }
    )


@app.get("/readings/history")
def reading_history():
    sensor_id = request.args.get("sensor_id", "").strip()
    from_ts = to_int(request.args.get("from_timestamp"))
    to_ts = to_int(request.args.get("to_timestamp"))
    limit = to_int(request.args.get("limit")) or 200

    query = "SELECT sensor_id, temperature, timestamp FROM readings WHERE 1=1"
    params: list[int | str] = []

    if sensor_id:
        query += " AND sensor_id = ?"
        params.append(sensor_id)
    if from_ts is not None:
        query += " AND timestamp >= ?"
        params.append(from_ts)
    if to_ts is not None:
        query += " AND timestamp <= ?"
        params.append(to_ts)

    query += " ORDER BY timestamp ASC LIMIT ?"
    params.append(limit)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    data = [{"sensor_id": r[0], "temperature": r[1], "timestamp": r[2]} for r in rows]
    return jsonify(data)


@app.get("/averages/history")
def average_history():
    sensor_id = request.args.get("sensor_id", "").strip()
    from_ts = to_int(request.args.get("from_timestamp"))
    to_ts = to_int(request.args.get("to_timestamp"))
    limit = to_int(request.args.get("limit")) or 200

    query = "SELECT sensor_id, average, window_start, window_end, sample_count FROM averages WHERE 1=1"
    params: list[int | str] = []

    if sensor_id:
        query += " AND sensor_id = ?"
        params.append(sensor_id)
    if from_ts is not None:
        query += " AND window_end >= ?"
        params.append(from_ts)
    if to_ts is not None:
        query += " AND window_end <= ?"
        params.append(to_ts)

    query += " ORDER BY window_end ASC LIMIT ?"
    params.append(limit)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    data = [
        {
            "sensor_id": r[0],
            "average": r[1],
            "window_start": r[2],
            "window_end": r[3],
            "sample_count": r[4],
        }
        for r in rows
    ]
    return jsonify(data)


def start() -> None:
    init_db()
    threading.Thread(target=consume_readings, daemon=True).start()
    threading.Thread(target=consume_averages, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)


if __name__ == "__main__":
    start()
