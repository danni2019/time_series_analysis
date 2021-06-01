import redis
import configparser
import os
import json

fp = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(fp, "../../conf.ini"))


class RedisHandle:
    def __init__(self, mq_name: str = 'default'):
        host = config.get('Redis', 'host')
        port = int(config.get('Redis', 'port'))
        pwd = config.get('Redis', 'password')
        db = int(config.get('Redis', 'db'))
        self.mq_name = mq_name
        self.conn = redis.ConnectionPool(host=host, port=port, password=pwd)
        self.rds = redis.StrictRedis(connection_pool=self.conn, db=db, decode_responses=True)

    def push_msg(self, msg: bytes):
        self.rds.lpush(self.mq_name, msg)

    def get_msg(self):
        msg = self.rds.brpop(self.mq_name, timeout=0)
        return msg[1] if msg is not None else None

    def set_key(self, k, v):
        self.rds.set(k, v, ex=60*5)

    def get_key(self, k):
        return json.loads(self.rds.get(k))

    def key_exist(self, k):
        return self.rds.exists(k)

    def del_key(self, keys: list):
        self.rds.delete(keys)

    def __del__(self):
        """自动在析构时关闭连接"""
        self.rds.close()
