import asyncio
import time
import httpx, requests
import json
import traceback
import websocket
import random

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from lib.setting import api_registry_setting as api_setting, DEBUG_MODE
from lib.project_logging import logging
from lib.kafka_consumer import KafkaConsumerExtend

NAME_OF_INSTANCE = 'ms_gateway'


# the controller will call the next microservice
class controller:
    __id = 0

    def __init__(self, topic: str, ws_api_registry: str):
        self.controller_id = controller.__id
        controller.__id += 1
        self.topic_name = topic
        self.working = True

        self.api_workers = []

        self.ws_api_registry: websocket.WebSocket = websocket.WebSocket()
        self.url_ws_api_registry = ws_api_registry
        self.ws_api_registry.connect(url=ws_api_registry)

        logging(service_name='ms_gateway',
                action='STARTUP',
                msg='Start controller for topic={}'.format(topic))

    def __del__(self):
        logging(service_name=NAME_OF_INSTANCE, action='SHUTDOWN', msg='')

    def on_worker_change(self):
        print("on_worker_change: start")
        if self.ws_api_registry.connected:
            while self.working:
                json_list_api = self.ws_api_registry.recv()
                print("json_list_api:", json_list_api)
                if len(json_list_api) > 3:
                    _list = json.loads(json_list_api)

                    # add new api
                    for address in _list:
                        if address not in self.api_workers:
                            self.api_workers.append(address)

                    # remove droped api
                    for address in self.api_workers:
                        if address not in _list:
                            self.api_workers.pop(self.api_workers.index(address))
                else:
                    time.sleep(15)
        print("on_worker_change: end")

    def process_msg_queue(self, msg_json: str, threadpool: ThreadPoolExecutor):
        def _call_worker(url):
            is_sent = False
            while not is_sent:
                try:
                    res = httpx.post(url='http://' + url + '/worker', json=json.loads(msg_json),
                                     headers={"Content-Type": "application/json; charset=utf-8"})
                    # res = requests.post(url='http://' + url + '/worker',
                    #                     json=json.loads(msg_json),
                    #                     headers={"Content-Type": "application/json; charset=utf-8"})
                    if res.status_code == 200:
                        is_sent = True
                        logging(service_name=NAME_OF_INSTANCE,
                                action='CALL-MICRO-SERVICE',
                                msg=json.dumps({'service_name': next_stage, 'status': 'sent'}),
                                is_print=True)
                    elif res.status_code == 422:
                        print("[API_GATE][worker:{}][status:{}] request body error: ".format(url, res.status_code)
                              + str(res.json()))
                        time.sleep(1)
                    else:
                        print("[API_GATE][worker:{}][status:{}] abnormal response {}".format(
                            url,
                            res.status_code,
                            res.json()
                        ))
                        time.sleep(1)

                except Exception as E:
                    is_sent = False
                    print("[API GATE][worker:{}] Error while submit task: ".format(url) + E.__str__())
                    time.sleep(1)

        msg_dict = json.loads(s=msg_json)
        current_stage = 'init'
        next_stage = msg_dict['next_stage']
        if next_stage is not None:
            try:
                url = self.api_workers[random.randrange(0, len(self.api_workers) - 1)]
                threadpool.submit(_call_worker, url)
                current_stage = 'call_ms'
            except Exception as e:
                logging(service_name=NAME_OF_INSTANCE,
                        action='CALL-MICRO-SERVICE',
                        msg='code base bug - {} - {} - {} - {}'.format(current_stage, next_stage, msg_json[1:10], e),
                        is_print=True)


# noinspection PyBroadException
def create_controller(topic: str, ws_api_registry: str):
    print('create_controller:', "init")
    object_controller = controller(topic=topic, ws_api_registry=ws_api_registry)
    with ThreadPoolExecutor(max_workers=10) as Thread_Executor, KafkaConsumerExtend() as consumer:

        Thread_Executor.submit(object_controller.on_worker_change)

        for msg in consumer.polling(topic_name=topic):
            Thread_Executor.submit(object_controller.process_msg_queue, msg, Thread_Executor)

    print("create_controller: ", 'end')


def start_all_controller():
    with KafkaConsumerExtend() as Consumer:
        list_topic = Consumer.list_topics()
        if 'LOAN_1' not in list_topic:
            list_topic.append('LOAN_1')
        print('list topic:', list_topic)
        workers = max(2, len(list_topic))

    api_registry = "ws://{host}:{port}/ws/all_workers".format(host=api_setting['host'],
                                                              port=int(api_setting['port']))

    with ProcessPoolExecutor(max_workers=workers) as executor:
        for topic_name in list_topic:
            if topic_name != 'application_log':
                executor.submit(create_controller,
                                topic_name,
                                api_registry)


if __name__ == '__main__':
    start_all_controller()
