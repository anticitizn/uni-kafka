from kafka import KafkaConsumer
from kafka import TopicPartition
from dateutil import parser
import graphyte
import json

def main():
    # Kafka topic and broker configuration
    bootstrap_servers = '10.50.15.52:9092'
    topic_name = 'weather'

    # Create Kafka consumer
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)

    # Assign consumer to specific partition(s)
    consumer.assign([TopicPartition(topic_name, 0)])

    # Set consumer position to the beginning of the partition
    consumer.seek_to_beginning()

    for message in consumer:
        try:
            data = json.loads(message.value)
        except ValueError:
            print("Malformed weather data encountered")

        date = parser.parse(data["timeStamp"])
        tempCur = float(data["tempCurrent"])
        tempMin = float(data["tempMin"])
        tempMax = float(data["tempMax"])
        city:str = data["city"]
        city = city.replace(' ','-')
        
        print(date)
        print(tempCur)
        print(city)
        
        sendToGraphite(city, "tempMin", date, tempMin)
        sendToGraphite(city, "tempMax", date, tempMax)
        sendToGraphite(city, "tempCur", date, tempCur)

def sendToGraphite(folderName, topicName, timestamp, value):
    metricName = folderName + '.' + topicName  # Append topic name to the metric name
    graphyte.init('10.50.15.52', prefix='INF20.group_max.weather.')
    graphyte.send(metricName, value, timestamp=timestamp.timestamp())


main()