"""
Weather Publisher per LOGO! 8.4 - Dairago
Pubblica dati meteo da Open-Meteo verso broker HiveMQ Cloud.
Formato: payload proprietario Siemens compatibile con LOGO! 8.4 "Server Web LOGO!"
   {"state":{"V..4:XX-2":"hexvalue"}}
"""

import os
import json
import sys
import urllib.request
import paho.mqtt.client as mqtt
import ssl
import time

# Configurazione meteo Dairago
LATITUDE = 45.5689
LONGITUDE = 8.8653
TIMEZONE = "Europe/Rome"

# Configurazione MQTT HiveMQ
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


def to_logo_hex(value):
    """
    Converte valore decimale (0-65535) nel formato LOGO! 8.4 little-endian a 8 caratteri.
    Esempio: 100 -> '00640000' (0x0064 = 100, padded little-endian)
    """
    value = max(0, min(65535, int(value)))
    return f"{value:04X}0000"


def build_payload(weather_data):
    """
    Costruisce payload per LOGO! 8.4 in formato 'Server Web LOGO!'.
    Sintassi: {"state":{"V..4:XX-2":"hexvalue"}}
    """
    current = weather_data["current"]
    
    # Conversione: float * 10 per mantenere il decimale come intero
    vento = round(current["wind_speed_10m"] * 10)
    raffica = round(current["wind_gusts_10m"] * 10)
    pioggia = round(current["precipitation"] * 10)
    nuvolosita = round(current["cloud_cover"])
    weather_code = round(current["weather_code"])
    
    # Formato corretto LOGO! 8.4: {"state":{"V..4:XX-2":"hexvalue"}}
    payload = {
        "state": {
            "V..4:20-2": to_logo_hex(vento),        # VW20: vento × 10
            "V..4:22-2": to_logo_hex(raffica),      # VW22: raffica × 10
            "V..4:24-2": to_logo_hex(pioggia),      # VW24: pioggia × 10
            "V..4:26-2": to_logo_hex(nuvolosita),   # VW26: nuvolosita %
            "V..4:28-2": to_logo_hex(weather_code)  # VW28: weather code
        }
    }
    
    print(f"Payload costruito:")
    print(f"  Vento:        {vento/10:.1f} km/h  -> {to_logo_hex(vento)}")
    print(f"  Raffica:      {raffica/10:.1f} km/h  -> {to_logo_hex(raffica)}")
    print(f"  Pioggia:      {pioggia/10:.1f} mm    -> {to_logo_hex(pioggia)}")
    print(f"  Nuvolosita:   {nuvolosita}%         -> {to_logo_hex(nuvolosita)}")
    print(f"  Weather code: {weather_code}        -> {to_logo_hex(weather_code)}")
    
    return payload


def publish_mqtt(payload):
    """Pubblica il payload sul broker HiveMQ via MQTT/TLS."""
    if not MQTT_PASSWORD:
        print("ERRORE: HIVEMQ_PASSWORD non configurata!")
        sys.exit(1)
    
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
    
    client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        clean_session=True
    )
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    
    client.tls_set(
        ca_certs=None,
        certfile=None,
        keyfile=None,
        cert_reqs=ssl.CERT_REQU
