import websocket

ws = websocket.WebSocket()
ws.connect(url="ws://127.0.0.1:8001/ws/all_api")
print(ws.recv())