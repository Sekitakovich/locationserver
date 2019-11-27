import responder
from pprint import pprint
from starlette.websockets import WebSocket, WebSocketDisconnect
from typing import Dict
from logging import getLogger
from datetime import datetime as dt
from dataclasses import dataclass
import time
import json
from threading import Thread
import websocket
import argparse
from pathlib import Path

from log import LogConfigure
from dbsession import DBsession


class BroadCaster(Thread):

    def __init__(self, *, port: int):
        super().__init__()
        self.daemon = True
        self.logger = getLogger('Log')
        self.ready: bool = False

        def onMessage(ws, message):
            self.logger.debug(msg=message)
            pass

        def onError(ws, error):
            self.logger.debug(msg=error)
            pass

        def onClose(ws):
            self.ready = False
            pass

        def onOpen(ws):
            self.ready = True
            self.logger.debug(msg='start')

        url = 'ws://127.0.0.1:%d/ws' % port
        self.ws = websocket.WebSocketApp(url,
                                         on_open=onOpen, on_error=onError, on_close=onClose, on_message=onMessage)

    def run(self) -> None:
        self.ws.run_forever()

    def send(self, *, message: str):
        if self.ready:
            self.ws.send(message)


@dataclass()
class IPTables(object):
    line: str
    node: str
    join: bool = False
    alert: int = 0
    last: dt = dt.now()


class Server(object):

    def __init__(self, *, port: int = 80, tiles: str):

        self.logger = getLogger('Log')

        PGdbname: str = 'locationserver'
        PGhost: str = 'localhost'
        PGport: int = 6666
        PGuser: str = 'postgres'
        PGpass: str = 'postgres'

        self.dbsession = DBsession(host=PGhost, port=PGport, database=PGdbname, username=PGuser, password=PGpass)
        self.dbsession.start()

        self.tiles = tiles
        self.api = responder.API(debug=False)

        self.wsmember: Dict[str, WebSocket] = {}
        self.broadcaster = BroadCaster(port=port)
        self.broadcaster.start()

        self.api.add_route('/post', self.insert)
        self.api.add_route('/tiles/{name}', self.OSM)
        self.api.add_route('/ws', self.websocketServer, websocket=True)

        self.api.run(port=port, address='0.0.0.0')

    def OSM(self, message: responder.Request, reply: responder.Response, *, name: str):

        file: str = '%s/%s' % (self.tiles, name)

        try:
            with open(file, 'rb') as f:
                reply.content = f.read()
        except (OSError,) as e:
            reply.status_code = 404

    async def websocketServer(self, ws: WebSocket):

        await ws.accept()
        key: str = ws.headers.get('sec-websocket-key')
        self.wsmember[key] = ws

        clientIP: str = ws.scope.get('client')[0]
        # self.logger.debug(msg='connected from %s' % (clientIP,))

        while True:
            try:
                msg: str = await ws.receive_text()
            except (WebSocketDisconnect, IndexError, KeyError, OSError) as e:
                self.logger.info(msg=e)
                break
            else:
                # self.logger.debug(msg='Got message from %s' % (clientIP,))
                for k, dst in self.wsmember.items():
                    if k != key:  # 自らのそれは送信しない
                        # to: str = dst.scope.get('client')[0]
                        # self.logger.debug(msg='Send to %s' % (to,))
                        await dst.send_text(msg)

        await ws.close()
        del self.wsmember[key]

    async def insert(self, message: responder.Request, reply: responder.Response):
        postbody = await message.media()
        self.dbsession.quePoint.put(postbody)
        total = len(postbody)
        reply.content = b'Received %d record(s)' % total
        self.broadcaster.send(message=json.dumps(postbody))


if __name__ == '__main__':

    logpath: str = 'logs'
    logdir = Path(logpath)
    if logdir.exists() is False:
        logdir.mkdir()

    logconfig = LogConfigure(file='logs/server.log', encoding='utf-8')

    port: int = 80
    pathForTiles: str = 'E:/OSM/tiles'
    version: str = '1.0'

    parser = argparse.ArgumentParser(description='Location server')
    parser.add_argument('-p', '--port', help='port name for HTTP service (%s)' % port, type=str, default=port)
    parser.add_argument('-t', '--tiles', help='path for OSM tiles (%s)' % pathForTiles, type=str, default=pathForTiles)
    parser.add_argument('-v', '--version', help='print version', action='version', version=version)

    args = parser.parse_args()

    server = Server(port=args.port, tiles=args.tiles)
