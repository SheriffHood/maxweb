#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import logging; logging.basicConfig(level=logging.INFO)

import os, json, time

from datetime import datetime

import asyncio
from aiohttp import web

@asyncio.coroutine
def index(request):
    return web.Response(body=b'<h1>Awesome</h1>', content_type='text/html', charset='utf-8')

@asyncio.coroutine
def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', 9001)
    logging.info('server started at 127.0.0.1:9001...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
