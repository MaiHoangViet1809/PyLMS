import lib.kafka_consumer as kc


class Kafka_Consumer:
    def __init__(self):
        self.consumer = kc.create_consumer()
        self._call_for_stop = False

    def polling(self, topic_name: str):
        self.consumer.subscribe(topics=topic_name)
        while not self._call_for_stop:
            msg_pack = self.consumer.poll(timeout_ms=1, max_records=10)
            for tp, messages in msg_pack.items():
                for msg in messages:
                    yield msg.value.decode()

    def __del__(self):
        self.consumer.close()

    def __enter__(self):
        return self

    def __aenter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

    def __aexit__(self, exc_type, exc_val, exc_tb):
        self.__del__()

