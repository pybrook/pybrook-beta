#  PyBrook
#
#  Copyright (C) 2023  Michał Rokita
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

import multiprocessing
import signal
import subprocess
import threading
from pathlib import Path
from time import sleep

import redis.asyncio as aioredis
import pytest
import redis

from pybrook.consumers.base import BaseStreamConsumer
from pybrook.consumers.worker import WorkerManager


@pytest.fixture
def mock_processes(monkeypatch):
    monkeypatch.setattr(multiprocessing, "Process", threading.Thread)
    monkeypatch.setattr(signal, "signal", lambda *args, **kwargs: None)


TEST_REDIS_URI = "redis://localhost/"
PROJECT_ROOT = Path(__file__).parent.parent.resolve()


@pytest.fixture()
def redis_server():
    r = subprocess.Popen(
        [
            "redis-server",
            "--save",
            '""',
            "--loadmodule",
            str(
                PROJECT_ROOT / "pybrook-redismodule/target/release/libpybrook_redis.so"
            ),
        ]
    )
    c = redis.from_url(TEST_REDIS_URI, socket_connect_timeout=1)
    for i in range(10):
        try:
            c.ping()
            break
        except redis.exceptions.RedisError:
            sleep(1)
    sleep(1)
    yield True
    r.kill()
    r.wait()


@pytest.fixture
@pytest.mark.asyncio
async def redis_async(redis_server):
    redis_async: aioredis.Redis = await aioredis.from_url(
        TEST_REDIS_URI, decode_responses=True
    )
    await redis_async.flushdb()
    yield redis_async
    await redis_async.flushdb()
    await redis_async.close()
    await redis_async.connection_pool.disconnect()


@pytest.fixture
def redis_sync(redis_server):
    redis_sync: redis.Redis = redis.from_url(TEST_REDIS_URI, decode_responses=True)
    redis_sync.flushdb()
    yield redis_sync
    redis_sync.close()


@pytest.fixture
def limit_time(monkeypatch):
    from time import time

    t = time()
    state = {"active": True}
    monkeypatch.setattr(
        BaseStreamConsumer,
        "active",
        property(
            fget=lambda s: (time() < t + 10) and state["active"], fset=lambda s, v: None
        ),
    )

    def term(*args, **kwargs):
        state["active"] = False

    monkeypatch.setattr(WorkerManager, "terminate", term)
    yield state
    state["active"] = False
