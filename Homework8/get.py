from kafka import KafkaConsumer
consumer = KafkaConsumer(
'sample', auto_offset_reset='earliest',
bootstrap_servers=['localhost:9092'],
consumer_timeout_ms=1000)
for message in consumer:
# value and key are raw bytes
# decode if necessary!
    print ("%s:%d:%d: key=%s value=%s" %
    (message.topic, message.partition, message.offset, message.key, message.value))
    consumer.close()