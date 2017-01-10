#!usr/bin/env python3
#-*- coding: utf-8 -*-

import asyncio
import sys
import logging
import orm

from models import User, Blog, Comment

@asyncio.coroutine
def test(loop):
    yield from orm.create_pool(loop=loop, host='localhost', port=3306, user='root', password='password', db='awesome')
    u = User(id=12345, name='Payne', email='payne@gmail.com', passwd='password', admin=True, image='about:blank')
    yield from u.save()

    yield from orm.destroy_pool()

    print('test ok')

if __name__=='__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.close()
    if loop.is_closed():
        sys.exit(0)
