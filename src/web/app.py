from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
import subprocess
import json
import os
import requests
from pyspark.sql import SparkSession
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from src.Kafka.producer.models.LiveMatchProducer import LiveMatchProducer
from src.Kafka.consumer.models.LiveMatchConsumer import LiveMatchConsumer
# from src.Kafka.consumer.models.spark_processing import main

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

def get_hdfs_data(league, season):
    """Fetch data from HDFS for a specific league and season"""
    try:
        # Construct the HDFS path
        hdfs_path = f"/user/root/data/datasets/{league}/season-{season}.csv"
        
        # Use docker exec to run hdfs dfs command
        cmd = f"docker exec datanode hdfs dfs -cat {hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Parse CSV data
            lines = result.stdout.strip().split('\n')
            headers = lines[0].split(',')
            data = []
            
            for line in lines[1:]:
                values = line.split(',')
                row = dict(zip(headers, values))
                data.append(row)
            
            return {"status": "success", "data": data}
        else:
            return {"status": "error", "message": f"Failed to read file: {result.stderr}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}



@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/livematches')
def get_live_matches():
    url = "https://api.sportradar.com/soccer/trial/v4/en/schedules/live/summaries.json"

    headers = {
        "accept": "application/json",
        "x-api-key": "1g5S40famoxgeinkdlhMGk1Vj9k2XjVs5bVwKwtC"
    }

    response = requests.get(url, headers=headers)
    return response.json()

@socketio.on('kafka_event_request')
def get_match_details(match_id):
    live_match = LiveMatchProducer(sport_event=match_id.get('sport_event_id'))
    live_match_thread = socketio.start_background_task(live_match.main)
    spark = SparkSession.builder.appName("KafkaToWebSocket").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0").getOrCreate()
    consumer= LiveMatchConsumer(socketio ,spark)
    consumer_thread = socketio.start_background_task(consumer.main)
    live_match_thread.join()
    consumer_thread.join()


@app.route('/match/<match_id>')
def match_detail(match_id):
    return render_template('match_detail.html', match_id=match_id)

@app.route('/api/leagues')
def get_leagues():
    """Get list of available leagues"""
    leagues = ['bundesliga', 'premier-league', 'la-liga', 'ligue-1', 'serie-a']
    return jsonify({"leagues": leagues})

@app.route('/api/seasons/<league>')
def get_seasons(league):
    """Get list of available seasons for a league"""
    try:
        cmd = f"docker exec datanode hdfs dfs -ls /user/root/data/datasets/{league}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Parse the output to get season numbers
            seasons = []
            for line in result.stdout.split('\n'):
                if 'season-' in line:
                    season = line.split('season-')[1].split('.')[0]
                    seasons.append(season)
            return jsonify({"seasons": sorted(seasons)})
        else:
            return jsonify({"error": "Failed to get seasons"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/data/<league>/<season>')
def get_league_data(league, season):
    """Get data for a specific league and season"""
    return jsonify(get_hdfs_data(league, season))


@socketio.on('connect')
def handle_connect():
    print('Client connected')
    
@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

@socketio.on('request_data')
def handle_data_request(data):
    """Handle WebSocket data requests"""
    league = data.get('league')
    season = data.get('season')
    
    if league and season:
        result = get_hdfs_data(league, season)
        socketio.emit('data_response', result)

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000) 