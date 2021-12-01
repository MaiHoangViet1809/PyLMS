from fastapi import FastAPI, responses, Request, status, Body
from fastapi import BackgroundTasks
# from fastapi import WebSocket, WebSocketDisconnect, HTTPException, Response
# from fastapi import File, UploadFile, HTTPException
# from fastapi.staticfiles import StaticFiles
import sys
import time
import json
import httpx
import uuid
import asyncio
import enum

import lib.microservice_callbacks as callback

from lib.project_logging import logging
from lib.setting import api_registry_setting as api_setting, DEBUG_MODE
from lib.template_class_request_model import history_model, worker_model
from lib.kafka_producer import KafkaProducerExtend

# app.mount("/js", StaticFiles(directory="js"), name="js")
# app.mount("/images", StaticFiles(directory="images"), name="images")


class WebSocketState(enum.Enum):
    CONNECTING = 0
    CONNECTED = 1
    DISCONNECTED = 2


async def worker_execute(request_body, kafka_url: str, kafka_class: KafkaProducerExtend, host: str, port: int):
    start_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    # process json msg
    assert 'next_stage' in request_body, 'next_stage not in msg body'
    next_stage = request_body['next_stage']

    # get callable from module callback
    f_callback = getattr(callback, next_stage)
    assert f_callback is not None, 'next_stage function callback is not exist in lib.callback_microservice'

    # process msg with callable
    result_send_to_kafka = await f_callback(json.dumps(request_body))
    result_dict = json.loads(s=result_send_to_kafka)

    # return result to kafka
    kafka_class.send_msg(topic_name=result_dict["topic_name"], value=result_send_to_kafka)
    # async with httpx.AsyncClient() as client:
    #     url = kafka_url.format(topic_name=result_dict["topic_name"])
    #     is_sent = False
    #     while not is_sent:
    #         try:
    #             response : httpx.Response = await client.post(url=url, json=result_send_to_kafka)
    #             if response.status_code == 200:
    #                 is_sent = True
    #             else:
    #                 print("send back error, url:{} detail:{} ".format(url, str(response)))
    #                 await asyncio.sleep(1)
    #         except Exception as E:
    #             is_sent = False
    #             print("feed back to kafka error on Worker {}, status: {}".format(url, E))
    #             await asyncio.sleep(1)

    end_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    call_status = json.dumps({"service_id": next_stage,
                              "service_result": "OK",
                              "start_dttm": start_dttm,
                              "end_dttm": end_dttm})

    logging(service_name='worker:{host}:{port}'.format(host=host, port=port),
            action='EXECUTED',
            msg=call_status)


class MS_API(FastAPI):
    def __init__(self, host: str = None, port: int = None):
        service_name = 'worker'
        super().__init__(title=service_name)
        self.service_name = service_name
        self.service_uuid = uuid.uuid4().hex
        self.service_status = "READY"
        self.host = host
        self.port = port
        self.called_time = 0
        self.url_kafka_send = 'http://127.0.0.1:8000/{topic_name}'
        self.kafka_send_msg = KafkaProducerExtend()

        # GET SECTION -----------------------------------
        # Home
        @self.get('/')
        async def home():
            return responses.RedirectResponse(url='/docs')

        @self.get('/ping', response_class=responses.PlainTextResponse)
        async def ping():
            return 'OK'

        @self.get('/called_time', response_class=responses.PlainTextResponse)
        async def get_called_time():
            return str(self.called_time)

        @self.get(path="/status",
                  response_class=responses.PlainTextResponse,
                  status_code=status.HTTP_200_OK)
        async def get_services_status():
            return self.service_status

        # POST SECTION -----------------------------------
        # @self.post(path='/' + self.service_name,
        #            response_class=responses.PlainTextResponse,
        #            status_code=status.HTTP_200_OK)
        # async def post_run_api(request: Request, background_tasks: BackgroundTasks):
        #     self.service_status = "RUNNING"
        #     self.called_time += 1
        #     background_tasks.add_task(worker_execute, request=request, kafka_url=self.url_kafka_send,
        #                               host=self.host, port=self.port)
        #     self.service_status = "READY"
        #     return responses.PlainTextResponse(content="OK", status_code=200)

        @self.post(path='/' + self.service_name)
        async def post_run_api(background_tasks: BackgroundTasks , request_body: worker_model = Body(...)):
            self.service_status = "RUNNING"
            self.called_time += 1
            background_tasks.add_task(worker_execute, request_body=request_body.dict(),
                                      kafka_url=self.url_kafka_send,
                                      kafka_class=self.kafka_send_msg,
                                      host=self.host, port=self.port)
            self.service_status = "READY"
            return responses.PlainTextResponse(content="OK", status_code=200)

        @self.post(path='/stop')
        async def post_stop_api():
            sys.exit(0)

        # @self.post("/uploadfile/")
        # async def create_upload_file(file: UploadFile = File(...)):
        #     return {"filename": file.filename}

        # PUT SECTION -----------------------------------
        # for update information (entire entity)
        # @self.put('/{entity_category}/{entity_id}')
        # async def update(entity_category: str, entity_id: str):
        #     return entity_category, entity_id

        # Websocket ----------------------------------------------------------
        # @self.websocket(path='/ws')
        # async def websocket_channel(websocket: WebSocket):
        #     await websocket.accept()
        #     # cliend_id = uuid.uuid4().hex
        #     queue = asyncio.queues.Queue()
        #     is_stop = False
        #     is_stopped = []
        #
        #     async def read_from_socket(ws: WebSocket):
        #         nonlocal is_stop, is_stopped
        #         async for data in ws.iter_text():
        #             if is_stop:
        #                 break
        #             queue.put_nowait(data)
        #         is_stopped.append(True)
        #
        #     async def get_data_and_send():
        #         nonlocal is_stop, is_stopped
        #         async with httpx.AsyncClient() as client:
        #             while not is_stop:
        #                 # get data from queue
        #                 if queue.empty():
        #                     data = await queue.get()
        #                 else:
        #                     data = queue.get_nowait()
        #                 # process data
        #                 self.called_time += 1
        #                 result = await self.callback(data)
        #                 # feedback to kafka
        #                 topic = json.loads(s=result)["next_action"]
        #                 await client.post(url=self.url_kafka_send.format(topic_name=topic), data=result)
        #
        #         is_stopped.append(True)
        #
        #     try:
        #         await asyncio.gather(read_from_socket(websocket), get_data_and_send())
        #     except WebSocketDisconnect:
        #         return

        logging(service_name=self.service_name, action='STARTUP', msg='Ready!')

    def register_api(self):
        return register_api(self.service_name, self.host, self.port)

    def __del__(self):
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), "api_class shutdown",
              self.service_uuid,
              self.service_name,
              self.host, self.port
              )
        httpx.delete(url='http://{host}:{port}/{ms_type}/{address}'.format(host=api_setting['host'],
                                                                           port=api_setting['port'],
                                                                           ms_type=self.service_name,
                                                                           address="{}:{}".format(self.host, self.port))
                     )
        logging(service_name=self.service_name, action='SHUTDOWN', msg='')

    async def __call__(self, scope, receive, send):
        await super().__call__(scope, receive, send)


def register_api(service_name, host, port):
    # REGISTER API
    # ping api registry
    ping = httpx.get(url='http://{host}:{port}/ping'.format(host=api_setting['host'], port=api_setting['port']))
    if ping.read().decode() != 'OK':
        logging(service_name=service_name, action='STOP_INITIALIZE', msg='API Registry is not online')
        return False
    else:
        register_request = httpx.post(url='http://{host}:{port}/api/{ms_type}'.format(host=api_setting['host'],
                                                                                      port=api_setting['port'],
                                                                                      ms_type=service_name),
                                      json=json.dumps({'host': host,
                                                       'port': port})
                                      )
        msg = register_request.read().decode()
        if msg.startswith('SUCCESSED'):
            logging(service_name=service_name, action='API-REGISTRATION', msg='OK !')
            return True
        elif msg.startswith('FAILED'):
            print(msg)
            logging(service_name=service_name,
                    action='STOP_INITIALIZE',
                    msg='failed to register current microservice into API registry, {}'
                        .format(msg[len('FAILED:'):]))
            return False
        else:
            print('wrong msg from api_router !!')
            return False


def unregister_api(service_name, host, port):
    ping = httpx.get(url='http://{host}:{port}/ping'.format(host=api_setting['host'], port=api_setting['port']))
    if ping.read().decode() != 'OK':
        logging(service_name=service_name, action='STOP_INITIALIZE', msg='API Registry is not online')
        return False
    else:
        httpx.delete(url='http://{host}:{port}/{ms_type}/{address}'.format(host=api_setting['host'],
                                                                           port=api_setting['port'],
                                                                           ms_type=service_name,
                                                                           address="{}:{}".format(host, port))
                     )
        return True
