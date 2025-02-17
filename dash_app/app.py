import dash
from dash import dcc, html
from kafka import KafkaConsumer
import json

app = dash.Dash(__name__)

consumer = KafkaConsumer(
    "weather_data",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def get_latest_data():
    for message in consumer:
        yield message.value

weather_data = get_latest_data()

def create_layout():
    return html.Div([
        html.H1("Datos Climáticos de Madrid"),
        dcc.Graph(id="temp-graph"),
        dcc.Interval(
            id="interval-update",
            interval=60000,  # 1 minuto
            n_intervals=0
        )
    ])

@app.callback(
    dash.dependencies.Output("temp-graph", "figure"),
    [dash.dependencies.Input("interval-update", "n_intervals")]
)
def update_graph(n):
    latest_data = next(weather_data)
    temp = latest_data["main"]["temp"]
    humidity = latest_data["main"]["humidity"]

    fig = {
        "data": [
            {"x": ["Temperatura", "Humedad"], "y": [temp, humidity], "type": "bar"}
        ],
        "layout": {"title": "Datos Actuales del Clima"}
    }
    return fig

app.layout = create_layout()

if __name__ == "__main__":
    app.run_server(debug=True)