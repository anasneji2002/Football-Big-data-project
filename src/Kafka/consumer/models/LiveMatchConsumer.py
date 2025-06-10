from .IConsumer import IConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
import pandas as pd


player_statistics_schema = StructType([
    StructField("assists", IntegerType()),
    StructField("chances_created", IntegerType()),
    StructField("clearances", IntegerType()),
    StructField("corner_kicks", IntegerType()),
    StructField("crosses_successful", IntegerType()),
    StructField("crosses_total", IntegerType()),
    StructField("defensive_blocks", IntegerType()),
    StructField("diving_saves", IntegerType()),
    StructField("dribbles_completed", IntegerType()),
    StructField("fouls_committed", IntegerType()),
    StructField("goals_by_head", IntegerType()),
    StructField("goals_by_penalty", IntegerType()),
    StructField("goals_conceded", IntegerType()),
    StructField("goals_scored", IntegerType()),
    StructField("interceptions", IntegerType()),
    StructField("long_passes_successful", IntegerType()),
    StructField("long_passes_total", IntegerType()),
    StructField("long_passes_unsuccessful", IntegerType()),
    StructField("loss_of_possession", IntegerType()),
    StructField("minutes_played", IntegerType()),
    StructField("offsides", IntegerType()),
    StructField("own_goals", IntegerType()),
    StructField("passes_successful", IntegerType()),
    StructField("passes_total", IntegerType()),
    StructField("passes_unsuccessful", IntegerType()),
    StructField("penalties_faced", IntegerType()),
    StructField("penalties_missed", IntegerType()),
    StructField("penalties_saved", IntegerType()),
    StructField("red_cards", IntegerType()),
    StructField("shots_blocked", IntegerType()),
    StructField("shots_faced_saved", IntegerType()),
    StructField("shots_faced_total", IntegerType()),
    StructField("shots_off_target", IntegerType()),
    StructField("shots_on_target", IntegerType()),
    StructField("substituted_in", IntegerType()),
    StructField("substituted_out", IntegerType()),
    StructField("tackles_successful", IntegerType()),
    StructField("tackles_total", IntegerType()),
    StructField("was_fouled", IntegerType()),
    StructField("yellow_cards", IntegerType()),
    StructField("yellow_red_cards", IntegerType())
])

player_schema = StructType([
    StructField("statistics", player_statistics_schema),
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("starter", BooleanType())
])

competitor_statistics_schema = StructType([
    StructField("ball_possession", IntegerType()),
    StructField("cards_given", IntegerType()),
    StructField("chances_created", IntegerType()),
    StructField("clearances", IntegerType()),
    StructField("corner_kicks", IntegerType()),
    StructField("crosses_successful", IntegerType()),
    StructField("crosses_total", IntegerType()),
    StructField("crosses_unsuccessful", IntegerType()),
    StructField("defensive_blocks", IntegerType()),
    StructField("diving_saves", IntegerType()),
    StructField("dribbles_completed", IntegerType()),
    StructField("fouls", IntegerType()),
    StructField("free_kicks", IntegerType()),
    StructField("goal_kicks", IntegerType()),
    StructField("injuries", IntegerType()),
    StructField("interceptions", IntegerType()),
    StructField("long_passes_successful", IntegerType()),
    StructField("long_passes_total", IntegerType()),
    StructField("long_passes_unsuccessful", IntegerType()),
    StructField("loss_of_possession", IntegerType()),
    StructField("offsides", IntegerType()),
    StructField("passes_successful", IntegerType()),
    StructField("passes_total", IntegerType()),
    StructField("passes_unsuccessful", IntegerType()),
    StructField("penalties_missed", IntegerType()),
    StructField("red_cards", IntegerType()),
    StructField("shots_blocked", IntegerType()),
    StructField("shots_off_target", IntegerType()),
    StructField("shots_on_target", IntegerType()),
    StructField("shots_saved", IntegerType()),
    StructField("shots_total", IntegerType()),
    StructField("substitutions", IntegerType()),
    StructField("tackles_successful", IntegerType()),
    StructField("tackles_total", IntegerType()),
    StructField("tackles_unsuccessful", IntegerType()),
    StructField("throw_ins", IntegerType()),
    StructField("was_fouled", IntegerType()),
    StructField("yellow_cards", IntegerType()),
    StructField("yellow_red_cards", IntegerType())
])

competitor_schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("abbreviation", StringType()),
    StructField("qualifier", StringType()),
    StructField("statistics", competitor_statistics_schema),
    StructField("players", ArrayType(player_schema))
])

statistics_schema = StructType([
    StructField("competitors", ArrayType(competitor_schema))
])

full_schema = StructType([
    StructField("payload", StructType([
        StructField("sport_event_status", StructType([
            StructField("status", StringType()),
            StructField("match_status", StringType()),
            StructField("home_score", IntegerType()),
            StructField("away_score", IntegerType()),
            StructField("period_scores", ArrayType(StructType([
                StructField("home_score", IntegerType()),
                StructField("away_score", IntegerType()),
                StructField("type", StringType()),
                StructField("number", IntegerType())
            ]))),
            StructField("match_situation", StructType([
                StructField("status", StringType()),
                StructField("qualifier", StringType()),
                StructField("updated_at", StringType())
            ]))
        ])),
        StructField("statistics", ArrayType(statistics_schema))  # from earlier step
    ])),
    StructField("metadata", StructType([
        StructField("sport_event_id", StringType()),
        StructField("event_id", StringType()),
        StructField("channel", StringType()),
        StructField("competition_id", StringType()),
        StructField("sport_id", StringType()),
        StructField("season_id", StringType())
    ]))
])

# spark = SparkSession.builder.appName("KafkaToWebSocket").getOrCreate()
class LiveMatchConsumer(IConsumer):
    def __init__(self,socketio,spark):
        super().__init__()
        self.socketio=  socketio
        self.spark = spark

    
    def _sub(self):
        super()._sub_with_topics(["live-match"])
    

    def extract_kafka_values(self, consumer_records):
        try:
            # Join all partial values to form full JSON string
            full_value = ''.join([record.value for record in consumer_records])
            full_value = full_value.decode('utf-8') if isinstance(full_value, bytes) else full_value
            return full_value
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to parse message as JSON: {e}")
            return None

    def _on_new_message(self, message):
        message= self.extract_kafka_values (list(message.values())[0])
        print(message)
         # Get the actual message content from Kafka
        # Step 1: Decode message if bytes
        if isinstance(message, bytes):
            message = message.decode("utf-8")
    
        try:
            
            message_dict = json.loads(message)
        except Exception as e:
            
            print(f"[ERROR] Failed to parse message as JSON: {e}")
            return

        # Step 2: Optional – check payload exists
        payload = message_dict.get("payload")
        if payload is None:
            print("[INFO] Skipping heartbeat or empty payload message.")
            return

        try:
            # Step 3: Convert full data to pandas DataFrame (optional, for processing/filtering)
            df = pd.json_normalize(message_dict)

            # Step 4: Optional filtering/logic
            match_id = message_dict.get("data", {}).get("metadata", {}).get("sport_event_id", "unknown")
            event_type = message_dict.get("data", {}).get("metadata", {}).get("event_id", "none")

            

            # Step 5: Emit to WebSocket
            self.socketio.emit("kafka_event", message)  # or json.dumps(message_dict)

        except Exception as e:
            print(f"[ERROR] Failed to process message with pandas: {e}")

    def main(self):

        df_kafka = self.spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "live-match") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Convert Kafka value to string
        df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

        df_parsed = df_json.select(from_json(col("json_str"), full_schema).alias("data"))

        # Filter out heartbeat-only messages
        df_filtered = df_parsed.filter(col("data.payload").isNotNull())
        def send_to_socket(df, epoch_id):
            rows = df.collect()
            for row in rows:
                try:
                    # Convert Spark Row to a plain Python dict (recursive to keep nested structures)
                    row_dict = row.asDict(recursive=True)
                    print(json.dumps(row_dict))
                    # Emit each full row as a message (JSON string)
                    self.socketio.emit("kafka_event_response", json.dumps(row_dict))

                except Exception as e:
                    print(f"Error sending row: {e}")

    # 7. Show filtered meaningful events (non-heartbeats)
        query = df_filtered.writeStream \
            .foreachBatch(send_to_socket) \
            .outputMode("append") \
            .start()

        query.awaitTermination()
