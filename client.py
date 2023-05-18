from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import TopicPartition
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from datetime import datetime, timedelta, timezone
from dateutil import parser
import graphyte
import json
import pprint
import re
import threading

def is_hour_passed(timestamp1: datetime, timestamp2: datetime):   
    timestamp1 = timestamp1.replace(tzinfo=timezone.utc)
    timestamp2 = timestamp2.replace(tzinfo=timezone.utc)
    
    # Calculate the difference between the two timestamps
    time_diff = timestamp2 - timestamp1
    
    # Check if the time difference is greater than or equal to one hour
    if time_diff >= timedelta(hours=1):
        return True
    else:
        return False

def sendToGraphite(folderName, topicName, timestamp, value):
    graphyte.init('10.50.15.52', prefix='INF20.group_max.tankerkoenig.' + str(folderName))
    graphyte.send(topicName, value, timestamp=timestamp.timestamp())

def aggregateData(plz):
    # Kafka topic and broker configuration
    bootstrap_servers = '10.50.15.52:9092'
    topic_name = 'tankerkoenig'

    # Create Kafka consumer
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)

    # Assign consumer to specific partition(s)
    consumer.assign([TopicPartition(topic_name, plz)])

    # Set consumer position to the beginning of the partition
    consumer.seek_to_beginning()

    # Dictionary to store aggregated data
    aggregated_data = {
            'pE5':{'count':0,'total_price':0},
            'pE10':{'count':0,'total_price':0},
            'pDie':{'count':0,'total_price':0}
        }        

    timestampOld = parser.parse("01.01.1970 00:00:00")
    # Consume messages from Kafka topic
    for message in consumer:        
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
        
        if is_hour_passed(timestampOld, timestamp):
            averagePe5 = 0
            averagePe10 = 0
            averageDie = 0
            
            if aggregated_data['pE5']['count'] > 0:
                averagePe5 = aggregated_data['pE5']['total_price'] / aggregated_data['pE5']['count']
                sendToGraphite(plz, "e5", timestamp, averagePe5)

            if aggregated_data['pE10']['count'] > 0:
                averagePe10 = aggregated_data['pE10']['total_price'] / aggregated_data['pE10']['count']
                sendToGraphite(plz, "e10", timestamp, averagePe10)
            
            if aggregated_data['pDie']['count'] > 0:
                averageDie = aggregated_data['pDie']['total_price'] / aggregated_data['pDie']['count']
                sendToGraphite(plz, "diesel", timestamp, averageDie)

            print('\n')
            print(timestamp)
            print(message.offset)

            with(open(str(plz)+"_offset","a")) as file:
                print(timestamp, file=file)
                print(message.offset, file=file)
                print('\n', file=file)
            
            #print(averagePe5)
            #print(averagePe10)
            #print(averageDie)
            #print('\n')
            
            timestampOld = timestamp
            
            aggregated_data = {
            'pE5':{'count':0,'total_price':0},
            'pE10':{'count':0,'total_price':0},
            'pDie':{'count':0,'total_price':0}
            }

        if pE5 > 0:
            aggregated_data['pE5']['count'] +=1
            aggregated_data['pE5']['total_price'] +=pE5
        if pE10 > 0:
            aggregated_data['pE10']['count'] +=1
            aggregated_data['pE10']['total_price'] +=pDie
        if pDie > 0:
            aggregated_data['pDie']['count'] +=1
            aggregated_data['pDie']['total_price'] +=pDie
     
    # Done processing
    # Close Kafka consumer 
    consumer.close()


if __name__ == "__main__":
    for i in range(10):
        threading.Thread(target=aggregateData, args=(i,)).start()
    
    print("Main done")