"""
Weather Publisher per LOGO! 8.4 - Dairago
Pubblica dati meteo da Open-Meteo verso broker HiveMQ Cloud.
Formato: array compatibile con LOGO! 8.4 [vento×10, raffica×10, pioggia×10, nuvolosita, weather_code]
"""

import os
import json
import sys
import urllib.request
import paho.mqtt.client as mqtt
import ssl
import time

# Configurazione
LATITUDE = 45.5689
LONGITUDE = 8.8653
TIMEZONE = "Europe/Rome"

MQTT_HOST = "8c349a8ad07849a4be91ec8216b1a37a.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "make_publisher"
MQTT_PASSWORD = os.environ.get("HIVEMQ_PASSWORD")  # Da GitHub Secrets!
MQTT_TOPIC = "meteo/dairago/dati"
MQTT_CLIENT_ID = "github_actions_dairago"


def fetch_weather():
    """Recupera dati meteo correnti da Open-Meteo per Dairago."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LATITUDE}&longitude={LONGITUDE}"
        f"&current=wind_speed_10m,wind_gusts_10m,precipitation,cloud_cover,weather_code"
        f"&timezone={TIMEZONE}"
    )
    
    print(f"Chiamata API Open-Meteo: {url}")
    
    with urllib.request.urlopen(url, timeout=15) as response:
        data = json.loads(response.read().decode("utf-8"))
    
    print(f"Dati ricevuti: {data}")
    return data


def build_payload(weather_data):
    """Costruisce array di interi per LOGO! 8.4 (Formato Array)."""
    current = weather_data["current"]
    
    # Conversione: float * 10 per mantenere il decimale come intero
    payload = [
        round(current["wind_speed_10m"] * 10),    # [0] vento × 10 → VW20
        round(current["wind_gusts_10m"] * 10),    # [1] raffica × 10 → VW22
        round(current["precipitation"] * 10),     # [2] pioggia × 10 → VW24
        round(current["cloud_cover"]),            # [3] nuvolosità → VW26
        round(current["weather_code"])            # [4] weather code → VW28
    ]
    
    print(f"Payload costruito: {payload}")
    return payload


def publish_mqtt(payload):
    """Pubblica il payload sul broker HiveMQ via MQTT/TLS."""
    if not MQTT_PASSWORD:
        print("ERRORE: HIVEMQ_PASSWORD non configurata!")
        sys.exit(1)
    
    # Stato per gestire connessione asincrona
    connected = {"value": False}
    published = {"value": False}
    
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("MQTT connesso a HiveMQ Cloud")
            connected["value"] = True
        else:
            print(f"Connessione MQTT fallita, rc={rc}")
    
    def on_publish(client, userdata, mid, properties=None, reasonCode=None):
        print(f"Messaggio pubblicato (mid={mid})")
        published["value"] = True
    
    # Crea client MQTT
    client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        clean_session=True
    )
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    
    # Configurazione TLS
    client.tls_set(
        ca_certs=None,  # Usa CA di sistema (Let's Encrypt è incluso)
        certfile=None,
        keyfile=None,
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT
    )
    
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    # Connessione
    print(f"Connessione a {MQTT_HOST}:{MQTT_PORT}...")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    
    # Loop in background
    client.loop_start()
    
    # Aspetta connessione (max 10 sec)
    timeout = 10
    while not connected["value"] and timeout > 0:
        time.sleep(0.5)
        timeout -= 0.5
    
    if not connected["value"]:
        print("ERRORE: Timeout connessione MQTT")
        client.loop_stop()
        sys.exit(1)
    
    # Pubblica
    payload_json = json.dumps(payload)
    print(f"Pubblicazione su topic '{MQTT_TOPIC}': {payload_json}")
    
    result = client.publish(
        MQTT_TOPIC,
        payload_json,
        qos=1,
        retain=True
    )
    
    # Aspetta pubblicazione (max 5 sec)
    timeout = 5
    while not published["value"] and timeout > 0:
        time.sleep(0.5)
        timeout -= 0.5
    
    # Disconnessione pulita
    client.loop_stop()
    client.disconnect()
    
    if not published["value"]:
        print("ERRORE: Timeout pubblicazione MQTT")
        sys.exit(1)
    
    print("Pubblicazione completata con successo!")


def main():
    """Esegue il ciclo completo: fetch meteo → publish MQTT."""
    try:
        print("=" * 60)
        print("Weather Publisher per LOGO! 8.4 - Dairago")
        print("=" * 60)
        
        # 1. Recupera dati meteo
        weather = fetch_weather()
        
        # 2. Costruisci payload
        payload = build_payload(weather)
        
        # 3. Pubblica via MQTT
        publish_mqtt(payload)
        
        print("=" * 60)
        print("Esecuzione completata con successo")
        print("=" * 60)
        
    except Exception as e:
        print(f"ERRORE: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
