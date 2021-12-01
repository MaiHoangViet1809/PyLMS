from fastapi import Request, HTTPException, FastAPI, responses, WebSocket, WebSocketDisconnect
import uvicorn
import json
import lib.setting as setting
import os
import asyncio

module_name = os.path.splitext(os.path.basename(__file__))[0]


class api_schema:
    def __init__(self):
        self.api_supports = {}   # ms_type : address (host+port)
        self.api_workers = []  # address (host+port)
        self.is_dirty = False

    def add_api_address(self, ms_type: str, host: str, port: int) -> str:
        address = '{host}:{port}'.format(host=host, port=port)

        if ms_type == 'worker':
            if address not in self.api_workers:
                self.api_workers.append(address)
                result = 'SUCCESSED: OK'
            else:
                result = 'FAILED: worker is already registered.'
            return result
        else:
            # add the service to registry
            if ms_type not in self.api_supports:
                self.api_supports[ms_type] = address
                self.is_dirty = True
                result = 'SUCCESSED: OK'
            else:
                if address == self.api_supports[ms_type]:
                    result = 'SUCCESSED: micro-service is already registered.'
                else:
                    self.api_supports[ms_type] = address
                    result = 'FAILED: ms_type is already registered.'
            return result

    def remove_api_support(self, ms_type: str) -> str:
        # remove the service from registry
        if ms_type not in self.api_supports:
            return 'FAILED: service {} is not registered'.format(ms_type)
        else:
            self.api_supports.pop(ms_type)
            self.is_dirty = True
            return 'SUCCESSED: OK'

    def remove_api_worker(self, address: str) -> str:
        # remove the service from registry
        if address not in self.api_workers:
            return 'FAILED: worker {} is not registered'.format(address)
        else:
            self.api_workers.pop(self.api_workers.index(address))
            self.is_dirty = True
            return 'SUCCESSED: OK'

    def get_api_address(self, ms_type: str):
        if ms_type in self.api_supports:
            return self.api_supports[ms_type]
        else:
            return None

    def get_all_supports(self):
        return json.dumps(self.api_supports)

    def get_all_workers(self):
        return json.dumps(self.api_workers)


api_registry = api_schema()

app = FastAPI(title=module_name)
app.service_name = module_name
app.host = setting.api_registry_setting['host']
app.port = int(setting.api_registry_setting['port'])


@app.get(path='/', response_class=responses.PlainTextResponse)
async def home():
    return responses.RedirectResponse(url='/docs')


@app.get(path='/ping', response_class=responses.PlainTextResponse)
async def ping():
    return 'OK'


@app.get(path='/api/{ms_type}', response_class=responses.PlainTextResponse)
async def get_api(ms_type: str):
    result = api_registry.get_api_address(ms_type=ms_type)
    if result is not None:
        return result
    else:
        raise HTTPException(status_code=404, detail='ms_type={} is not found in API registry!'.format(ms_type))


@app.post(path='/api/{ms_type}', response_class=responses.PlainTextResponse)
async def post_api(ms_type: str, req_body: Request):
    json_body = await req_body.json()
    req_dict = json.loads(s=json_body)
    if 'host' in req_dict and 'port' in req_dict:
        print('imcomming request register: ', req_dict)
        status = api_registry.add_api_address(ms_type=ms_type,
                                              host=req_dict['host'],
                                              port=req_dict['port'])
        return status
    else:
        raise HTTPException(status_code=501, detail='Bad request, json request body must include host+port+uuid')


@app.delete(path='/api/{ms_type}', response_class=responses.PlainTextResponse)
async def delete_api(ms_type: str, address: str):
    if ms_type is not None and address is not None:
        if ms_type == 'worker':
            status = api_registry.remove_api_worker(address)
        else:
            status = api_registry.remove_api_support(ms_type=ms_type)
        return status
    else:
        raise HTTPException(status_code=501, detail='Bad request, json request body must include host+port')


# websocket section ------------------------

@app.websocket(path='/ws/all_workers')
async def get_all_api(websocket: WebSocket):
    await websocket.accept()
    # alway init send 1
    await websocket.send_text(api_registry.get_all_workers())
    try:
        while True:
            if api_registry.is_dirty:
                await websocket.send_text(api_registry.get_all_workers())
                api_registry.is_dirty = False
            else:
                await asyncio.sleep(1)
    except WebSocketDisconnect:
        return

uvicorn.run(app=app, host=app.host, port=app.port, debug=True)
