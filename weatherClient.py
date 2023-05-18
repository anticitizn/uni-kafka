from kafka import KafkaConsumer
from dateutil import parser
import graphyte
import json

def main():
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('weather',
                            group_id='my-group',
                            bootstrap_servers=['10.50.15.52:9092'])
    for message in consumer:
        try:
            data = json.loads(message.value)
        except ValueError:
            print("Malformed weather data encountered")

        timestamp = parser.parse(data["timeStamp"])
        tempCur = float(data["tempCurrent"])
        tempMin = float(data["tempMin"])
        tempMax = float(data["tempMax"])
        city:str = data["city"]
        city = city.replace(' ','-')
        
        print(timestamp)
        print(tempCur)
        print(city)
        
        sendToGraphite(city, "tempMin", timestamp, tempMin)
        sendToGraphite(city, "tempMax", timestamp, tempMax)
        sendToGraphite(city, "tempCur", timestamp, tempCur)

def sendToGraphite(folderName, topicName, timestamp, value):
    graphyte.init('10.50.15.52', prefix='INF20.group_max.weather.' + str(folderName))
    graphyte.send(topicName, value, timestamp=timestamp.timestamp())


main()