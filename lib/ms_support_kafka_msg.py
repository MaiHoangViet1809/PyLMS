from fastapi import FastAPI, Request, Body, responses, status, WebSocket, WebSocketDisconnect, BackgroundTasks
from concurrent.futures import ThreadPoolExecutor
from typing import List
import json
import uvicorn
import time
import os
import asyncio

from lib.kafka_consumer import KafkaConsumerExtend
from lib.kafka_producer import KafkaProducerExtend
from lib.template_class_api_ms import register_api
from lib.template_class_request_model import worker_model

module_name = os.path.splitext(os.path.basename(__file__))[0]

app = FastAPI(title=module_name)
app.service_name = module_name
app.host = '127.0.0.1'
app.port = 8000
# register_api(service_name=app.service_name, host=app.host, port=app.port)

if __name__ == '__main__':
    with KafkaProducerExtend() as producer, ThreadPoolExecutor(max_workers=10) as Execute:

        async def push_msg(msg_body, topic):
            try:
                Execute.submit(producer.send_msg, topic, json.dumps(msg_body) if type(msg_body) == dict else msg_body)
            except Exception as E:
                print("push_msg error:" + str(E))

        # noinspection PyBroadException
        async def send_msg(msg: str, topic_name: str):
            try:
                Execute.submit(producer.send_msg, topic_name, msg)
                return "True"
            except Exception as e:
                return "False"


        @app.get(path='/', response_class=responses.PlainTextResponse)
        async def home():
            return responses.RedirectResponse(url='/docs')


        @app.get(path='/ping', response_class=responses.PlainTextResponse)
        async def ping():
            return 'OK'

        @app.post(path='/application_log',
                  response_class=responses.PlainTextResponse,
                  status_code=status.HTTP_200_OK)
        async def application_log(back_ground_task: BackgroundTasks, msg=Body(...)):
            start_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            back_ground_task.add_task(push_msg, msg_body=msg, topic='application_log')
            end_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            result = json.dumps({"service_id": app.service_name,
                                 "service_result": "OK",
                                 "start_dttm": start_dttm,
                                 "end_dttm": end_dttm})
            return result

        @app.post(path='/{topic}',
                  response_class=responses.PlainTextResponse,
                  status_code=status.HTTP_200_OK)
        async def post_run_api(topic: str, back_ground_task: BackgroundTasks, msg=Body(...)):
            start_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            back_ground_task.add_task(push_msg, msg_body=msg, topic=topic)
            end_dttm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            result = json.dumps({"service_id": app.service_name,
                                 "service_result": "OK",
                                 "start_dttm": start_dttm,
                                 "end_dttm": end_dttm})
            return result

        @app.websocket(path='/ws/{topic_name}/send')
        async def websocket_channel(websocket: WebSocket, topic_name: str):
            await websocket.accept()
            is_stop = False

            try:
                async for data in websocket.iter_text():
                    if is_stop:
                        break
                    await send_msg(msg=data, topic_name=topic_name)
                    await asyncio.sleep(0)
            except WebSocketDisconnect:
                print(topic_name, ' is disconnected')

        print("initialize finish")
        uvicorn.run(app
                    # "{}:app".format(module_name)
                    , host=app.host, port=app.port, debug=False, log_level="warning"
                    , limit_concurrency=20000
                    , timeout_keep_alive=120
                    # , workers=2
                    )
