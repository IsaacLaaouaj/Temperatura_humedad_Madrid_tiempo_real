import requests
from kafka import KafkaProducer
import json
import time

API_URL = "https://api.openweathermap.org/data/2.5/weather?q=Madrid&appid=API&units=metric"
KAFKA_TOPIC = "weather_data"
KAFKA_BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather():
    response = requests.get(API_URL)
    return response.json()

while True:
    weather_data = fetch_weather()
    producer.send(KAFKA_TOPIC, value=weather_data)
    print("Enviado a Kafka:", weather_data)
    time.sleep(60)  # Consultar cada minuto