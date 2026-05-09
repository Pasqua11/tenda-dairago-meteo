"""Weather Publisher LOGO! 8.4 Dairago - formato Server Web LOGO!"""
import os
import json
import sys
import urllib.request
import paho.mqtt.client as mqtt
import ssl
import time

LATITUDE = 45.5689
LONGITUDE = 8.8653
TIMEZONE = "Europe/Rome"
MQTT_HOST = "8c349a8ad07849a4be91ec8216b1a37a.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "make_publisher"
MQTT_PASSWORD = os.environ.get("HIVEMQ_PASSWORD")
MQTT_TOPIC = "meteo/dairago/dati"
MQTT_CLIENT_ID = "github_actions_dairago"


def fetch_weather():
    url = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current=wind_speed_10m,wind_gusts_10m,precipitation,cloud_cover,weather_code&timezone={TIMEZONE}"
    print("Chiamata API Open-Meteo")
    with urllib.request.urlopen(url, timeout=15) as response:
        data = json.loads(response.read().decode("utf-8"))
    print(f"Dati ricevuti: {data}")
    return data


def to_logo_hex(value):
    value = max(0, min(65535, int(value)))
    return f"{value:04X}0000"


def build_payload(weather_data):
    current = weather_data["current"]
    vento = round(current["wind_speed_10m"] * 10)
    raffica = round(current["wind_gusts_10m"] * 10)
    pioggia = round(current["precipitation"] * 10)
    nuvolosita = round(current["cloud_cover"])
    weather_code = round(current["weather_code"])
    payload = {"state": {"V..4:20-2": to_logo_hex(vento), "V..4:22-2": to_logo_hex(raffica), "V..4:24-2": to_logo_hex(pioggia), "V..4:26-2": to_logo_hex(nuvolosita), "V..4:28-2": to_logo_hex(weather_code)}}
    print(f"Vento {vento/10:.1f} km/h, Raffica {raffica/10:.1f} km/h, Pioggia {pioggia/10:.1f} mm, Nuvol {nuvolosita}%, Code {weather_code}")
    return payload


def publish_mqtt(payload):
    if not MQTT_PASSWORD:
        print("ERRORE: HIVEMQ_PASSWORD non configurata")
        sys.exit(1)
    connected = {"value": False}
    published = {"value": False}

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("MQTT connesso")
            connected["value"] = True

    def on_publish(client, userdata, mid, properties=None, reasonCode=None):
        print(f"Pubblicato mid={mid}")
        published["value"] = True

    client = mqtt.Client(client_id=MQTT_CLIENT_ID, callback_api_version=mqtt.CallbackAPIVersion.VERSION2, clean_session=True)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    timeout = 10
    while not connected["value"] and timeout > 0:
        time.sleep(0.5)
        timeout -= 0.5
    if not connected["value"]:
        print("ERRORE: Timeout connessione")
        client.loop_stop()
        sys.exit(1)
    payload_json = json.dumps(payload)
    print(f"Pubblicazione: {payload_json}")
    client.publish(MQTT_TOPIC, payload_json, qos=1, retain=True)
    timeout = 5
    while not published["value"] and timeout > 0:
        time.sleep(0.5)
        timeout -= 0.5
    client.loop_stop()
    client.disconnect()
    if not published["value"]:
        print("ERRORE: Timeout pubblicazione")
        sys.exit(1)
    print("Completato!")


def main():
    try:
        print("Weather Publisher LOGO! 8.4 - Dairago")
        weather = fetch_weather()
        payload = build_payload(weather)
        publish_mqtt(payload)
    except Exception as e:
        print(f"ERRORE: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
