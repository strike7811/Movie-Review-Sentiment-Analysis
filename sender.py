import json

from cffi.model import StructType
from pyspark.sql import SparkSession
import socket

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

class SocketWriter:
    def open(self, partition_id, epoch_id):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(('localhost', 9999))
        return True

    def process(self, row):
        try:
            # Serialize row to JSON
            message = json.dumps(row.asDict(), default=str) + '\n'
            self.sock.sendall(message.encode('utf-8'))
        except UnicodeDecodeError as e:
            print(f"Error decoding bytes: {e}")
        except UnicodeEncodeError as e:
            print(f"Error re-encoding string: {e}")
        except socket.error as e:
            print(f"Socket error: {e}. Attempting to reconnect and resend.")
            self.reconnect()
            self.sock.sendall(message.encode('utf-8'))

    def reconnect(self):
        """Handles reconnection attempts if the socket connection is lost."""
        self.sock.close()  # Close the existing socket if open
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(('localhost', 9999))
        print("Reconnected to the server.")

    def close(self, error):
        self.sock.close()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Socket Data Sender") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

reviews_schema = StructType([
    StructField("imdb_id", StringType(), True),
    StructField("tmdb_id", StringType(), True),
    StructField("review_ID", StringType(), True),
    StructField("author", StringType(), True),
    StructField("content", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField('date', DateType(), True)
])

# Load the review data into a DataFrame/ Testing purpose
reviews_df = spark.readStream.schema(reviews_schema)\
    .option("multiLine", "true")\
    .option("header", "true")\
    .option("escape", "\"")\
    .option("dateFormat", "d/M/yyyy")\
    .csv("C:/Users/wr431/Desktop/Course Material/Year2/Sem 3/Data Engineering/Assignment/data/review_df")

# Apply the custom writer to the DataFrame
query = reviews_df.writeStream.trigger(once=True).foreach(SocketWriter()).start()

# Await termination (or process until stopped otherwise)
query.awaitTermination()
spark.stop()

