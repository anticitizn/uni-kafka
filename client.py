from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import TopicPartition
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from datetime import datetime
import json

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

# Consume messages from Kafka topic
for message in consumer:
    data = json.loads(message.value)

    # Extract relevant information from the message
    postleitzahl = data['postleitzahl']
    preiskategorie = data['preiskategorie']
    preis = data['preis']
    timestamp = data['timestamp']

    # Perform aggregation based on postleitzahl and preiskategorie
    if postleitzahl not in aggregated_data:
        aggregated_data[postleitzahl] = {}
    if preiskategorie not in aggregated_data[postleitzahl]:
        aggregated_data[postleitzahl][preiskategorie] = {'count': 0, 'total_price': 0}

    if preis != 0:  # Exclude zero prices
        aggregated_data[postleitzahl][preiskategorie]['count'] += 1
        aggregated_data[postleitzahl][preiskategorie]['total_price'] += preis

    # Process aggregated data and produce to a new Kafka topic
    if timestamp.hour == 0:  # Example: Aggregate and produce hourly
        for plz, categories in aggregated_data.items():
            for category, values in categories.items():
                average_price = values['total_price'] / values['count']
                new_message = {'postleitzahl': plz, 'preiskategorie': category, 'durchschnittspreis': average_price}
                
                #producer.send('aggregated_data_topic', value=json.dumps(new_message).encode('utf-8'))
                print(new_message)
                # Reset aggregation for the next hour
                aggregated_data[plz][category] = {'count': 0, 'total_price': 0}

# Close Kafka consumer and producer
consumer.close()
producer.close()