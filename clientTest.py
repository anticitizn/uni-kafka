from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import TopicPartition
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from datetime import datetime
from dateutil import parser
import json
import pprint
import re
import pandas as pd

# Kafka topic and broker configuration
bootstrap_servers = '10.50.15.52:9092'
topic_name = 'tankerkoenig'

# Create Kafka consumer
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)

# Assign consumer to specific partition(s)
consumer.assign([TopicPartition(topic_name, 0)])

# Set consumer position to the beginning of the partition
consumer.seek_to_beginning()

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Dictionary to store aggregated data
aggregated_data = pd.DataFrame()

i = 0
# Consume messages from Kafka topic
for message in consumer:
    i+=1
    if i > 1000:
        break
    
    data = json.loads(message.value)

    # Extract relevant information from the message
    postleitzahl = data['plz']
    pE5 = data['pE5']
    pE10 = data['pE10']
    pDie = data['pDie']
    timestamp =  parser.parse(data['dat'])
    
    aggregated_data =  pd.concat([ aggregated_data ,pd.Series([postleitzahl, pE5,pE10,pDie], index=["plz","pE5","pE10","pDie"], name=timestamp)], axis=1)
    

print(aggregated_data)

# Close Kafka consumer and producer
consumer.close()
producer.close()