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
aggregated_data = {}

i = 0
# Consume messages from Kafka topic
for message in consumer:
    i+=1
    if i > 2000:
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
    

    # Perform aggregation based on postleitzahl and preiskategorie
    if postleitzahl not in aggregated_data:
        aggregated_data[postleitzahl] = {}
    if "pE5" not in aggregated_data[postleitzahl]:
        aggregated_data[postleitzahl]["pE5"] = {'count': 0, 'total_price': 0}
    if "pE10" not in aggregated_data[postleitzahl]:
        aggregated_data[postleitzahl]["pE10"] = {'count': 0, 'total_price': 0}
    if "pDie" not in aggregated_data[postleitzahl]:
        aggregated_data[postleitzahl]["pDie"] = {'count': 0, 'total_price': 0}

    if pE5 != 0:  # Exclude zero prices
        aggregated_data[postleitzahl]["pE5"]['count'] += 1
        aggregated_data[postleitzahl]["pE5"]['total_price'] += pE5
    if pE10 != 0:  # Exclude zero prices
        aggregated_data[postleitzahl]["pE10"]['count'] += 1
        aggregated_data[postleitzahl]["pE10"]['total_price'] += pE10
    if pDie != 0:  # Exclude zero prices
        aggregated_data[postleitzahl]["pDie"]['count'] += 1
        aggregated_data[postleitzahl]["pDie"]['total_price'] += pDie

    # Process aggregated data and produce to a new Kafka topic
    #if timestamp.hour == 0:  # Example: Aggregate and produce hourly
        #for plz, categories in aggregated_data.items():
            
            #for category, values in categories.items():
              
                #average_price = category['total_price'] / category['count']
                #new_message = {'postleitzahl': plz, categories: category, 'durchschnittspreis': average_price}
                
                #producer.send('aggregated_data_topic', value=json.dumps(new_message).encode('utf-8'))
                #print(new_message)
                # Reset aggregation for the next hour
                #aggregated_data[plz][category] = {'count': 0, 'total_price': 0}

pprint.pprint(aggregated_data)

# Close Kafka consumer and producer
consumer.close()
producer.close()