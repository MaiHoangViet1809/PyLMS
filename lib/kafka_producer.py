# pip install lz4
# import snappy
# pip install crc32c
from kafka import KafkaProducer
from lib.setting import DEBUG_MODE
from typing import List, Callable
import uuid


def create_producer(config: dict = None):
    if config is None:
        config = {'bootstrap_servers': "localhost:9092",
                  'client_id': uuid.uuid4().hex}
    producer = KafkaProducer(**config)  # value_serializer=lambda v: json.dumps(v).encode()
    return producer


def acked(msg):
    if DEBUG_MODE > 0:
        print("Message info: topic={} partition={} offset={} timestamp={}".format(msg.topic,
                                                                                msg.partition,
                                                                                msg.offset,
                                                                                msg.timestamp))


def err_acked(error):
    if DEBUG_MODE > 0:
        print("Error: {}".format(str(error)))


def push_msg(producer: KafkaProducer, topic_name: str, value: str, call_backs: list = None, errbacks: list = None):
    # msg = snappy.compress(data=value, encoding='utf-8')
    # future = producer.send(topic=topic_name, value=value.encode())
    future = producer.send(topic=topic_name, value=value)
    if DEBUG_MODE > 0:
        if call_backs:
            for c in call_backs:
                future.add_callback(f=c)
        future.add_callback(f=acked)
        if errbacks:
            for e in errbacks:
                future.add_errback(f=e)
        future.add_errback(f=err_acked)
    # producer.flush()


# Include record headers. The format is list of tuples with string key
# and bytes value.
# producer.send('foobar', value=b'c29tZSB2YWx1ZQ==', headers=[('content-encoding', b'base64')])

# metrics = producer.metrics()
# print(metrics)

class KafkaProducerExtend:
    def __init__(self, config: dict = None, call_backs: List[Callable] = None, errbacks: List[Callable] = None):
        self.producer = create_producer(config=config)
        self.call_backs = call_backs
        self.errbacks = errbacks

    def send_msg(self, topic_name: str, value: str):
        # msg = snappy.compress(data=value, encoding='utf-8')
        # future = producer.send(topic=topic_name, value=value.encode())
        future = self.producer.send(topic=topic_name, value=value.encode())
        if DEBUG_MODE > 0:
            if self.call_backs:
                for c in self.call_backs:
                    future.add_callback(f=c)
            future.add_callback(f=acked)
            if self.errbacks:
                for e in self.errbacks:
                    future.add_errback(f=e)
            future.add_errback(f=err_acked)

    def __del__(self):
        pass

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

