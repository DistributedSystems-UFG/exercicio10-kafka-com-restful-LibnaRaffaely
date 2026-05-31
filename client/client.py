import time

import requests

BASE_URL = "http://localhost:5000"


def fmt_ts(ts: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def choose_sensor() -> str:
    print("sensores: sensor_01 | sensor_02 | sensor_03 | vazio = qualquer")
    return input("sensor_id: ").strip()


def show_latest_reading() -> None:
    sensor_id = choose_sensor()
    params = {"sensor_id": sensor_id} if sensor_id else {}
    r = requests.get(f"{BASE_URL}/readings/latest", params=params, timeout=10)
    if r.status_code != 200:
        print(r.json().get("error", "erro"))
        return
    data = r.json()
    print(f"sensor: {data['sensor_id']}")
    print(f"temp: {data['temperature']:.2f}")
    print(f"timestamp: {fmt_ts(data['timestamp'])}")


def show_latest_average() -> None:
    sensor_id = choose_sensor()
    params = {"sensor_id": sensor_id} if sensor_id else {}
    r = requests.get(f"{BASE_URL}/averages/latest", params=params, timeout=10)
    if r.status_code != 200:
        print(r.json().get("error", "erro"))
        return
    data = r.json()
    print(f"sensor: {data['sensor_id']}")
    print(f"media: {data['average']:.2f}")
    print(f"amostras: {data['sample_count']}")
    print(f"janela: {fmt_ts(data['window_start'])} -> {fmt_ts(data['window_end'])}")


def show_reading_history() -> None:
    sensor_id = choose_sensor()
    params = {"sensor_id": sensor_id} if sensor_id else {}
    r = requests.get(f"{BASE_URL}/readings/history", params=params, timeout=10)
    if r.status_code != 200:
        print("erro")
        return
    data = r.json()
    if not data:
        print("sem registros")
        return
    for item in data:
        print(f"{fmt_ts(item['timestamp'])} {item['sensor_id']} {item['temperature']:.2f}")


def show_average_history() -> None:
    sensor_id = choose_sensor()
    params = {"sensor_id": sensor_id} if sensor_id else {}
    r = requests.get(f"{BASE_URL}/averages/history", params=params, timeout=10)
    if r.status_code != 200:
        print("erro")
        return
    data = r.json()
    if not data:
        print("sem registros")
        return
    for item in data:
        print(
            f"{fmt_ts(item['window_end'])} {item['sensor_id']} media={item['average']:.2f} amostras={item['sample_count']}"
        )


def menu() -> None:
    while True:
        print("\n1 - ultima leitura")
        print("2 - ultima media")
        print("3 - historico de leituras")
        print("4 - historico de medias")
        print("0 - sair")
        choice = input("opcao: ").strip()

        if choice == "0":
            break
        if choice == "1":
            show_latest_reading()
        elif choice == "2":
            show_latest_average()
        elif choice == "3":
            show_reading_history()
        elif choice == "4":
            show_average_history()
        else:
            print("opcao invalida")


if __name__ == "__main__":
    menu()
