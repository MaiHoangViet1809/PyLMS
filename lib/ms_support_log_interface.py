from lib.kafka_consumer import create_consumer, subscribe, KafkaConsumerExtend
from sse_starlette.sse import EventSourceResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, responses, Cookie, HTTPException
from starlette.requests import Request
from starlette.middleware.sessions import SessionMiddleware
import uvicorn
import asyncio
from typing import Optional
from concurrent.futures import ProcessPoolExecutor
import time
import uuid

MSG_STREAM_DELAY = 1 / 1000  # milisecond / 1000
MSG_STREAM_TIMEOUT = 30  # second

app = FastAPI(title='kafka log streaming')

app.secret_key = uuid.uuid4().hex

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# app.add_middleware(SessionMiddleware, secret_key=app.secret_key)

sessions = {}


async def sse_stream(request: Request, session_cookie):
    topic_name = sessions[session_cookie]
    try:
        with KafkaConsumerExtend() as consumer:
            first_msg = False
            last_msg_time = time.time()
            for msg in consumer.polling(topic_name):
                if not first_msg:
                    first_msg = True
                    yield 'cls'

                if await request.is_disconnected():
                    print(request.client.host, "client disconnected!!!")
                    consumer.stop_polling()
                    break

                # if time.time() - last_msg_time > MSG_STREAM_TIMEOUT:
                #     print(request.client.host, "client time_out!!!")
                #     consumer.stop_polling()
                #     break

                last_msg_time = time.time()
                yield msg
                # await asyncio.sleep(MSG_STREAM_DELAY)
    finally:
        print('end queue')


@app.get('/', response_class=responses.HTMLResponse)
async def home():
    return responses.HTMLResponse(content=
'''
<!DOCTYPE html>
<html>
<body>
home page for streaming log SSE (server send event). </br>
<a href="/stream/application_log">Please click here to go to streaming page !</a>   
</body>
</html>
''',
                                  status_code=200)


@app.get('/stream/{topic_name}', response_class=responses.HTMLResponse, status_code=200)
async def run(topic_name: str = 'application_log'):
    cookie = uuid.uuid4().hex
    sessions[cookie] = topic_name

    html = """
    <!DOCTYPE html>
    <html>
    <head>
      <style>
      #output {
        background-color: black;
        color:white;
        height: 90vh;
        overflow-x: hidden;
        overflow-y: auto;
        text-align: left;
        padding-left:10px;
      }
      #output_table {
        background-color: black;
        color:white;
        overflow-x: hidden;
        overflow-y: auto;
        text-align: left;
        padding-left:10px;
      }
      </style>
    </head>
    
    <body>
    
    <h1>Streaming output:</h1>
    <div id="output">
    <table id="output_table">nothing here</table>
    </div>
    
    <script>
        var source = new EventSource("/stream/data/{session}");
        var msg_index = 0;
        source.onmessage = function(event) {
            var output       = document.getElementById("output");
            var output_table = document.getElementById("output_table");
            if (event.data != 'cls') {
                var row = output_table.insertRow(output_table.rows.length);
                var obs = row.insertCell(0);
                obs.innerHTML = msg_index;
                msg_index += 1;
                var cell = row.insertCell(1);
                cell.innerHTML = event.data;
                if (output_table.rows.length > 1000) {
                    output_table.deleteRow(0);
                }
            } else {
                msg_index = 0;
                output.innerHTML = '<table id="output_table"></table>';
            }
            output.scrollTop = output.scrollHeight;
        };
    </script>
    
    </body>
    </html>
           """.replace('{session}', cookie)
    res = responses.HTMLResponse(content=html)
    res.set_cookie(key='session', value=cookie)
    return res


@app.get('/stream/data/{session_cookie}')
async def run(request: Request, session_cookie: str):
    if session_cookie not in sessions:
        raise HTTPException(status_code=404, detail='session_cookie is not found!')
    event_generator = sse_stream(request, session_cookie)
    return EventSourceResponse(content=event_generator, media_type='text/event-stream')


# run the app
uvicorn.run(app, host="127.0.0.1", port=7998, debug=True)


