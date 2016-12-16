#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import asyncio
import sys
import aiomysql
import logging; logging.basicConfig(level=logging.INFO)
from logging import log

def log(sql, args=None):
    logging.info('SQL: [%s] args: %s' % (sql, args or []))

@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool......')
    global __pool
    __pool = yield from aiomysql.create_pool(
        loop = loop,
        host = kw.get('host', 'localhost'),
        port = kw.get('port', '3306'),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset', 'utf8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
    )

@asyncio.coroutine
def destory_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        yield from __pool.wait_closed()

@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args)
        if size:
            resultset = yield from cur.fetchmany(size)
        else:
            resultset = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(reslutset))
        conn.close()
        return resultset

@asyncio.coroutine
def execute(sql, args, autocommit=True):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        if not autocommit:
            yield from conn.begin()
        try:
            cur = yield from conn.cursor(aiomysql.DictCursor)
            yield from cur.execute(sql.replace('?', '%s'), args)
            if not autocommit:
                yield from conn.commit()
            affected = cur.rowcount
            yield from cur.close
            print('execute : ', affected)
        except BaseException as e:
            if not autocommit:
                yield from conn.rollback()
            raise e
        finally:
            conn.close()
        return affected

class Field(object):
    
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class BooleanField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'boolean', False, default)

class StringField(Field):
    def __init__(self, name=None, primary_key = False, default = None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, column_type = 'bigint', primary_key = False, default = None):
        super().__init__(name, column_type, primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=None):
        super().__init__(name, 'float', primary_key, default)

class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)

        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))

        mappings = dict()
        escaped_fields = []
        primaryKey = None

        for key, value in attrs.copy().items():
            if isinstance(value, Field):
                logging.info('  found mapping: %s ==> %s' % (key, value))
                mappings[key] = attrs.pop(key)

                if value.primary_key:
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    primaryKey = key
                else:
                    escaped_fields.append(key)
        if not primaryKey:
            raise StandardError('Primary key not found.')

        attrs['__mappings__'] = mappings 
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey 
        attrs['__fields__'] = escaped_fields + [primaryKey]

        attrs['__select__'] = 'select * from `%s`' % (tableName)
        attrs['__insert__'] = 'insert into `%s` (%s) values (%s)' % (tableName, ', '.join('`%s`' % f for f in mappings), ', '.join('?' * len(mappings)))
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (tableName, ', '.join('`%s` = ?' % f for f in escaped_fields), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`= ?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)
            
class Model(dict, metaclass=ModelMetaclass):
    
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    @asyncio.coroutine
    def findAll(cls, where=None, args=None, **kw):
        sql = [cls.__select__]

        if where:
            sql.append('where')
            sql.append(where)

        if args is None:
            args = []

        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)

        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?', '?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))

        rs = yield from select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    @asyncio.coroutine
    def findNumber(cls, selectField, where=None, args=None):
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)

        rs = yield from select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None

        return rs[0]['_num_']

    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        resultset = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        return cls(**resultset[0]) if len(resultset) else None

    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        #args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)

        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    @classmethod
    @asyncio.coroutine
    def update(self):
        args = list(map(self.getValue, self.__fields__))
        #args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__update__, args)

        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    @classmethod
    @asyncio.coroutine
    def remove(self):
        args = list(map(self.getValue, self.__fields__))
        rows = yield from execute(self.__delete__, args)

        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)
