from kafka import KafkaProducer
from kafka.errors import KafkaError
import random

producer = KafkaProducer(bootstrap_servers=['10.50.15.52:9092'])

for i in range(20):

    demoWert = random.randint(10,30)
    message ='{"temp":'+str(demoWert)+'}'
    # Asynchronous by default
    future = producer.send('vlvs_inf20_group4', message.encode('utf-8') )
    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        # Decide what to do if produce request failed...
        print(e)
        
        

    print (record_metadata.topic)
    print (record_metadata.partition)
    print (record_metadata.offset)