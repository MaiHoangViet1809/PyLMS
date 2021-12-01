import lib.kafka_consumer as kc

print("start read message")
n = 0

with kc.KafkaConsumerExtend() as consumer:
    for message in consumer.polling(topic_name='LOAN_1'):
        n += 1
        print('Order id: {} - message: {}'.format(n, message))

