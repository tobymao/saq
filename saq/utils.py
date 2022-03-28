import time
import uuid


def now():
    return int(time.time() * 1000)


def uuid1():
    return str(uuid.uuid1())


def millis(s):
    return s * 1000


def seconds(ms):
    return ms / 1000
