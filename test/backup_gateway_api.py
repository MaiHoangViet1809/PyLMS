import asyncio
import time
import httpx
import json
import traceback
import websockets
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from lib.setting import api_registry_setting as api_setting, DEBUG_MODE
from lib.project_logging import logging
from lib.kafka_consumer import KafkaConsumerExtend

NAME_OF_INSTANCE = 'ms_gateway'


# the controller will call the next microservice
class controller:
    __id = 0

    def __init__(self, topic: str):
        self.controller_id = controller.__id
        controller.__id += 1
        self.topic_name = topic
        self.working = True
        logging(service_name='ms_gateway',
                action='STARTUP',
                msg='Start controller for topic={}'.format(topic))

    def __del__(self):
        logging(service_name=NAME_OF_INSTANCE, action='SHUTDOWN', msg='')


def api_call(url, method, json_data: str = None):
    if method == 'get':
        return httpx.get(url=url)
    elif method == 'post':
        return httpx.post(url=url, json=json_data)


def push_msg(msg, topic):
    api_call(url='http://127.0.0.1:7997/ms_kafka_push_msg/' + topic, method='post', json_data=msg)


# def query_list_node(host: str, port: int, ms_type: str, max_retry: int = None):
#     time_retry = 0
#     while max_retry is None or time_retry <= max_retry:
#         avail_node = api_call(url='http://{host}:{port}/api/{ms_type}'.format(host=host, port=port, ms_type=ms_type),
#                               method='get')
#         if avail_node.status_code != 404:
#             dict_node = json.loads(avail_node.text)
#             return dict_node
#         else:
#             time_retry += 1
#             logging(service_name=NAME_OF_INSTANCE,
#                     action='ERROR-CALL-MS',
#                     msg='RETRY={time_retry} - there is no ms of that type is online (in API registry)'
#                     .format(time_retry=time_retry))
#             time.sleep(5)
#     return None


def query_ms_run_time(node: str):
    req = api_call(url='{host_port}/called_time'.format(host_port=node), method='get')
    if req.status_code < 300:
        return int(req.text)
    else:
        return None


# noinspection PyBroadException
def load_balance(dict_node: dict):
    # check if node is available
    if DEBUG_MODE == 0:
        list_node = {node: query_ms_run_time(node) for node in dict_node}
        indx = min(list_node, key=list_node.get)
        return indx
    else:
        try:
            list_node = {node: query_ms_run_time(node) for node in dict_node}
            indx = min(list_node, key=list_node.get)
            return indx
        except Exception as e:
            print(traceback.format_exc())
            print(e)


def call_ms(select_node: str, next_action: str, msg: str, msg_no: str):
    time_retry = 0
    while True:
        response = api_call(url='{host_port}'.format(host_port=select_node) + '/' + next_action,
                            method='post',
                            json_data=msg)
        if response.status_code < 300:
            res = 'OK'
            # push_msg(msg=msg_no, topic='msg_handled')
        elif response.status_code == 500:
            res = 'ERROR'
            print("ERROR 500: ", response.headers, response.text)
            time.sleep(1)
        else:
            check_status = api_call(url='{host_port}'.format(host_port=select_node) + '/ping',
                                    method='get')
            if check_status is not None:
                res = 'ERROR'
                time.sleep(1)
            else:
                res = 'ERROR_NODE_NOT_READY'
                print('selected node is not ready (status={status})'.format(status=check_status))
                time.sleep(60)

        time_retry += 1
        logging(service_name=NAME_OF_INSTANCE,
                action='CALL-MICRO-SERVICE',
                msg='status_code={status} result={res} time_retry={time_retry}'
                .format(res=res, status=response.status_code, time_retry=time_retry)
                )

        if res == 'OK':
            return response


class WSClient(threading.Thread):

    def __init__(self):
        super().__init__()
        self._loop = None
        self._tasks = {}
        self._queue = None
        self._stop_event = None
        self.is_running = False

    def run(self):
        if self.is_running:
            return
        self._loop = asyncio.new_event_loop()
        self._queue = asyncio.queues.Queue(loop=self._loop)
        self._stop_event = asyncio.Event(loop=self._loop)
        self.is_running = True
        try:
            self._loop.run_until_complete(self._stop_event.wait())
            self._loop.run_until_complete(self._clean())
        finally:
            self._loop.close()

    def stop(self):
        if self.is_running:
            self._loop.call_soon_threadsafe(self._stop_event.set)

    def subscribe(self, url, sub_msg, callback, com_type: str = 'listen'):
        def _subscribe():
            if url not in self._tasks:
                task = None
                if com_type == 'listen':
                    task = self._loop.create_task(self._listen(url, sub_msg, callback))
                elif com_type == 'send':
                    task = self._loop.create_task(self._send(url))
                if task:
                    self._tasks[url] = task

        self._loop.call_soon_threadsafe(_subscribe)

    def unsubscribe(self, url):
        def _unsubscribe():
            task = self._tasks.pop(url, None)
            if task is not None:
                task.cancel()

        self._loop.call_soon_threadsafe(_unsubscribe)

    def send_msg(self, url, msg):
        def _send_msg():
            self._queue.put_nowait([url, msg])

        self._loop.call_soon_threadsafe(_send_msg)

    async def _listen(self, url, sub_msg, callback):
        try:
            while not self._stop_event.is_set():
                try:
                    ws = await websockets.connect(url, loop=self._loop)
                    if sub_msg:
                        await ws.send(sub_msg)
                    async for data in ws:
                        callback(data)
                except Exception as e:
                    print('RESTARTING SOCKET IN 2 SECONDS: ', e)
                    await asyncio.sleep(2, loop=self._loop)
        finally:
            self._tasks.pop(url, None)

    async def _send(self, url):
        try:
            while not self._stop_event.is_set():
                try:
                    queue = self._queue[url]  # asyncio.queues.Queue(loop=self._loop)
                    ws = await websockets.connect(url, loop=self._loop)
                    while not self._stop_event.is_set():
                        if queue.empty():
                            data = await queue.get()
                        else:
                            data = queue.get_nowait()
                        if data[0] == url:
                            ws.send(data[1])
                except Exception as e:
                    print('RESTARTING SOCKET IN 2 SECONDS: ', e)
                    await asyncio.sleep(2, loop=self._loop)
        finally:
            self._tasks.pop(url, None)

    async def _clean(self):
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), loop=self._loop)


# noinspection PyBroadException
def create_controller(topic: str):
    object_controller = controller(topic=topic)

    dict_api = {}
    client = WSClient()

    # noinspection PyBroadException
    def process_msg(msg_json: str, wsclient, url):
        nonlocal dict_api
        msg_dict = json.loads(s=msg_json)
        current_stage = 'init'
        next_action = msg_dict['next_action']
        if next_action is not None:
            try:
                # get list of available node of ms_type
                # dict_node = query_list_node(host=api_setting['host'],
                #                             port=api_setting['port'],
                #                             ms_type=msg_dict['next_action'])
                dict_node = dict_api[next_action]
                current_stage = 'dict_node'

                select_node = load_balance(dict_node=dict_node)
                current_stage = 'select_node'

                # request process current msg
                response = call_ms(select_node=select_node,
                                   next_action=next_action,
                                   msg=msg_json,
                                   msg_no=msg_dict['msg_no'])
                wsclient.send_msg(url=url, msg=response)
                current_stage = 'call_ms'
            except Exception as e:
                logging(service_name=NAME_OF_INSTANCE, action='CALL-MICRO-SERVICE',
                        msg='code base bug - {} - {}'.format(current_stage, e))
                print("process_msg: ", traceback.format_exc())

    with ThreadPoolExecutor(max_workers=4) as Thread_Executor:
        # for msg in Consumer.polling(topic_name=topic):
        #     Thread_Executor.submit(process_msg, msg)
        #     if not object_controller.working:
        #         logging(service_name=NAME_OF_INSTANCE, action='STOP', msg='controller is stoped')
        #         break

        api_registry_ws = "ws://{host}:{port}/ws/all_api".format(host=api_setting['host'],
                                                                 port=int(api_setting['port']))
        link_ws_kafka_get = "ws://127.0.0.1:7997/ws/{topic}/get".format(topic=topic)
        link_ws_kafka_send = "ws://127.0.0.1:7997/ws/{topic}/send".format(topic=topic)

        def on_msg(message):
            Thread_Executor.submit(process_msg, message, link_ws_kafka_send)

        def assign_value(data):
            nonlocal dict_api
            dict_api = json.loads(data)
            client.subscribe(url='')

        client.subscribe(url=api_registry_ws   , sub_msg='get', callback=assign_value, com_type='listen')
        client.subscribe(url=link_ws_kafka_get , sub_msg=None, callback=on_msg, com_type='listen')
        client.subscribe(url=link_ws_kafka_send, sub_msg=None, callback=None, com_type='send')
        client.run()

        while True:
            await asyncio.sleep(60)
            if not object_controller.working:
                logging(service_name=NAME_OF_INSTANCE, action='STOP', msg='controller is stoped')
                client.stop()
                break


def start_all_controller():
    with KafkaConsumerExtend() as Consumer:
        list_topic = Consumer.list_topics()
        if 'LOAN_1' not in list_topic:
            list_topic.append('LOAN_1')
        print('list topic:', list_topic)
        workers = max(2, len(list_topic))

    with ProcessPoolExecutor(max_workers=workers) as executor:
        for topic_name in list_topic:
            if topic_name != 'application_log':
                executor.submit(create_controller, topic_name)


if __name__ == '__main__':
    start_all_controller()
