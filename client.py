from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import TopicPartition
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from datetime import datetime, timedelta
from dateutil import parser
import json
import pprint
import re

def is_hour_passed(timestamp1, timestamp2):
    # Convert timestamps to datetime objects
    dt1 = datetime.fromtimestamp(timestamp1)
    dt2 = datetime.fromtimestamp(timestamp2)
    
    # Calculate the difference between the two timestamps
    time_diff = dt2 - dt1
    
    # Check if the time difference is greater than or equal to one hour
    if time_diff >= timedelta(hours=1):
        return True
    else:
        return False

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
aggregated_data = {
        'pE5':{'count':0,'total_price':0},
        'pE10':{'count':0,'total_price':0},
        'pDie':{'count':0,'total_price':0}
    }   
    
 
i = 0


timestamp = parser.parse("01.01.1970 00:00:00")
# Consume messages from Kafka topic
for message in consumer:
    i+=1
    if i > 10000:
        break
    
    data = json.loads(message.value)

    # Extract relevant information from the message
    postleitzahl = data['plz']
    pE5 = data['pE5']
    pE10 = data['pE10']
    pDie = data['pDie']
    timestamp =  parser.parse(data['dat'])
    
    if postleitzahl is None:
        continue
    if not re.match('^\d{5}$',postleitzahl):
        continue
    
    print(postleitzahl)
    aggregated_data['pE5']['count'] +=1
    aggregated_data['pE5']['total_price'] +=pE5
    aggregated_data['pE10']['count'] +=1
    aggregated_data['pE10']['total_price'] +=pE10
    aggregated_data['pDie']['count'] +=1
    aggregated_data['pDie']['total_price'] +=pDie


#pprint.pprint(aggregated_data)

# Close Kafka consumer and producer
consumer.close()
producer.close()