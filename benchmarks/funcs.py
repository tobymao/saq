import asyncio
import random
import time


async def noop(ctx):
    return 1


async def sleeper(ctx):
    # 500 ms / 20 == avg 25 ms per job
    await asyncio.sleep(random.random() / 20)
    return 1


def sync_noop():
    return 1


def sync_sleeper():
    time.sleep(random.random() / 20)
    return 1
