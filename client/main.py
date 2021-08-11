import asyncio
from os import environ

from fastapi import FastAPI
from fastapi import Request
from fastapi import WebSocket
from fastapi.templating import Jinja2Templates
from redis import Redis


app = FastAPI()
templates = Jinja2Templates(directory="templates")

stream_key = environ.get("STREAM", "jarless-1")
hostname = environ.get("REDIS_HOSTNAME", "localhost")
port = environ.get("REDIS_PORT", 6379)
redis_cli = Redis(hostname, port, retry_on_timeout=True)


@app.get("/")
def read_root(request: Request):
    return templates.TemplateResponse("index.htm", {"request": request})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    TODO:
    - Understand deeply the redis-py.xread method:
      - why we need to send a list of stream? I didn't want get the first [0] element
      - why it returns bytes, Is the .decode a good way?
      - is there a better way to convert it to python dict?
      - what is the a good number for the sleep/block?
    """
    last_id = 0
    sleep_ms = 5000

    await websocket.accept()
    while True:
        await asyncio.sleep(0.3)
        resp = redis_cli.xread({stream_key: last_id}, count=1, block=sleep_ms)
        print("Waitting...")
        if resp:
            key, messages = resp[0]  # :(
            last_id, data = messages[0]

            data_dict = {k.decode("utf-8"): data[k].decode("utf-8") for k in data}
            data_dict["id"] = last_id.decode("utf-8")
            data_dict["key"] = key.decode("utf-8")
            await websocket.send_json(data_dict)
