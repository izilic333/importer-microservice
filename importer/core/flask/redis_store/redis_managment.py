import json
from common.mixin.mixin import generate_hash_for_json
from common.redis_setup.connection.connection import conn

class RedisManagement(object):
    @classmethod
    def compare_data(cls, key, data):
        if conn.get(key) is not None:
            old_data = conn.get(key)
            new_data = data

            hash_new_data = generate_hash_for_json(new_data)
            hash_old_data = generate_hash_for_json(json.loads(old_data))

            if str(hash_new_data) != str(hash_old_data):
                cls.set_data_to_redis(key, data)
                return True
            else:
                return True

        return False

    @classmethod
    def get_data_from_redis(cls, key):
        return conn.get(key)

    @classmethod
    def get_data_from_redis_hgetall(cls, key):
        return conn.hgetall(key)

    @classmethod
    def set_data_to_redis(cls, key, data, redis_time_cache=20):
        conn.set(key, json.dumps(data), redis_time_cache)

    @classmethod
    def set_data_to_redis_permanent(cls, key, data):
        conn.set(key, json.dumps(data))

    @classmethod
    def hmset_to_redis(cls, key, data):
        conn.hmset(key, data)

    @classmethod
    def set_to_redis_expire(cls, key, time_in_seconds):
        conn.expire(key, time_in_seconds)

    @classmethod
    def redis_key_exist_check(cls, key):
        return conn.exists(key)

    @classmethod
    def set_or_get_redis_data(cls, key, data):
        status = cls.compare_data(key, data)
        if not status:
            cls.set_data_to_redis(key, data)
            return json.loads(cls.get_data_from_redis(key))
        else:
            return json.loads(cls.get_data_from_redis(key))