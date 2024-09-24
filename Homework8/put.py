from kafka import KafkaProducer
from time import sleep
producer = KafkaProducer(
bootstrap_servers=['localhost:9092'])
producer.send('sample', value=b'A20503736', key=b'MYID')
producer.send('sample', value=b'Girish Rajani', key=b'MYNAME')
producer.send('sample', value=b'Black', key=b'MYEYECOLOR')
sleep(5)
producer.close()