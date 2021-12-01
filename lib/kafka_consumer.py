from kafka import KafkaConsumer, TopicPartition
import uuid
import asyncio
# import snappy


# Support Function ********************************************************************************************
def create_consumer(topic_name: str = None, config: dict = None):
    if config is None:
        config = {'bootstrap_servers': ['localhost:9092'],
                  'auto_offset_reset': 'earliest',
                  # 'enable_auto_commit': True,
                  # 'auto_commit_interval_ms': 1000,
                  'client_id': uuid.uuid4().hex
                  }
    if topic_name is None:
        consumer = KafkaConsumer(**config)
    else:
        consumer = KafkaConsumer(topic_name, **config)
    return consumer


def get_last_offset_single_partition(consumer, topic_name: str):
    ps = [TopicPartition(topic=topic_name, partition=p)
          for p in consumer.partitions_for_topic(topic_name)]
    step_1 = consumer.end_offsets(ps)
    last_offset, partition_ = zip(*[(step_1[p], ps.index(p)) for p in ps])
    last_pos = max(last_offset)
    last_par = partition_[last_offset.index(last_pos)]
    return last_pos, ps[partition_[last_par]]


def get_last_record(consumer, topic_partition, last_offset):
    consumer.seek(partition=topic_partition, offset=last_offset - 1)
    b_poll = consumer.poll(timeout_ms=1000)
    for tp, messages in b_poll.items():
        for g in messages:
            return g.value


# END Support Function ********************************************************************************************


def get_all_records(topic_name: str, consumer: KafkaConsumer = None):
    if consumer is None:
        consumer = create_consumer(topic_name=topic_name)
    last_pos, last_par = get_last_offset_single_partition(consumer=consumer, topic_name=topic_name)
    all_records = []
    try:
        running = True
        while running:
            msg = consumer.poll(timeout_ms=1000)
            n = consumer.position(partition=last_par)
            # print(n, last_pos)

            if n >= last_pos:
                print("EOF")
                running = False

            for tp, messages in msg.items():
                for g in messages:
                    all_records.append(g.value.decode())
                    # print(g.value.decode())
    finally:
        consumer.close()

    return all_records


def get_value(topic_name: str, consumer: KafkaConsumer = None):
    all_records = get_all_records(topic_name=topic_name, consumer=consumer)
    return all_records


def get_last_value(topic_name: str, consumer: KafkaConsumer = None):
    if consumer is None:
        consumer = create_consumer(topic_name=topic_name)
    last_pos, last_par = get_last_offset_single_partition(consumer=consumer, topic_name=topic_name)
    return get_last_record(consumer=consumer, topic_partition=last_par, last_offset=last_pos)


def subscribe(topic_name: str, consumer: KafkaConsumer = None):
    if consumer is None:
        consumer = create_consumer(topic_name=topic_name)
    else:
        consumer.subscribe(topics=[topic_name])

    for message in consumer:
        # yield snappy.uncompress(data=message.value, decoding='utf-8')
        yield message.value.decode()


def list_topics(consumer: KafkaConsumer = None):
    if consumer is None:
        consumer = create_consumer()

    list_topic = consumer.topics()
    return list_topic


class KafkaConsumerExtend:
    def __init__(self, config: dict = None):
        self.consumer = create_consumer(config=config)
        self._call_for_stop = False

    def polling(self, topic_name: str):
        self.consumer.subscribe(topics=[topic_name])
        while not self._call_for_stop:
            msg_pack = self.consumer.poll(timeout_ms=1, max_records=10)
            for tp, messages in msg_pack.items():
                for msg in messages:
                    # yield snappy.uncompress(data=msg.value, decoding='utf-8')
                    yield msg.value.decode()

    async def async_polling(self):
        while not self._call_for_stop:
            msg_pack = self.consumer.poll(timeout_ms=1, max_records=10)
            for tp, messages in msg_pack.items():
                for msg in messages:
                    # yield snappy.uncompress(data=msg.value, decoding='utf-8')
                    yield msg.value.decode()
            await asyncio.sleep(0)

    def __aiter__(self):
        return self.async_polling()

    def __anext__(self):
        return self.async_polling()

    def stop_polling(self):
        self._call_for_stop = True

    def list_topics(self):
        list_topic = self.consumer.topics()
        return list(list_topic)

    def __del__(self):
        self.consumer.close()

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

