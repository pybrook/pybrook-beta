import asyncio
import secrets
import signal
import sys
from concurrent import futures
from enum import Enum
from typing import Dict, Iterable, List, MutableMapping, Set, Tuple

import aioredis
import redis
from loguru import logger

CONSUMER_NAME_LENGTH = 64


class ConsumerImpl(Enum):
    GEARS = 'GEARS'
    ASYNC = 'ASYNC'
    SYNC = 'SYNC'


class BaseStreamConsumer:
    def __init__(self,
                 *,
                 redis_url: str,
                 consumer_group_name: str,
                 input_streams: Iterable[str],
                 use_thread_executor: bool = False,
                 read_chunk_length: int = 1):
        self._consumer_group_name = consumer_group_name
        self._redis_url = redis_url
        self._active = False
        self._use_thread_executor = use_thread_executor
        self._read_chunk_length = read_chunk_length
        self.executor = None
        self.input_streams = tuple(input_streams)

    @property
    def supported_impl(self) -> Set[ConsumerImpl]:
        return set()  # pragma: nocover

    def __repr__(self):
        return f'<{self.__class__.__name__} input_streams={self.input_streams}>'

    @property
    def input_streams(self) -> Tuple[str, ...]:
        return tuple(self._input_streams)

    @input_streams.setter
    def input_streams(self, streams: Tuple[str, ...]):
        self._input_streams = tuple(streams)

    def register_consumer(self):
        redis_conn = redis.from_url(self._redis_url,
                                    encoding='utf-8',
                                    decode_responses=True)
        for stream in self.input_streams:
            try:
                logger.error(self._consumer_group_name)
                redis_conn.xgroup_create(stream,
                                         self._consumer_group_name,
                                         id='0',
                                         mkstream=True)
            except redis.ResponseError as e:
                if 'BUSYGROUP' not in str(e):
                    raise e  # pragma: nocover

    def stop(self, signum=None, frame=None):
        if not self._active:
            logger.warning(f'Killing {self}')
            sys.exit()
        else:
            logger.info(f'Terminating {self}')
            self.active = False

    def register_signals(self):
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value: bool):
        self._active = value

    @property
    def _xreadgroup_params(self) -> MutableMapping:
        consumer_name = secrets.token_urlsafe(CONSUMER_NAME_LENGTH)
        return {
            'streams': {s: '>'
                        for s in self.input_streams},
            'groupname': self._consumer_group_name,
            'consumername': consumer_name,
            'count': self._read_chunk_length,
            'block': 1000
        }


class SyncStreamConsumer(BaseStreamConsumer):
    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError(  # pragma: nocover
            f'Sync version of process_message for {type(self).__name__} not implemented.'
        )

    @property
    def supported_impl(self) -> Set[ConsumerImpl]:
        return super().supported_impl | {ConsumerImpl.SYNC}

    def stop(self, signum=None, frame=None):
        super().stop(signum, frame)
        if not self.executor:
            return
        self.executor.shutdown(wait=False, cancel_futures=False)
        if self.executor._work_queue.qsize():
            logger.warning(
                'Waiting for all futures to finish, use Ctrl + C to force exit.'
            )

    def run_sync(self):
        self.register_signals()
        redis_conn: redis.Redis = redis.from_url(self._redis_url,
                                                 encoding='utf-8',
                                                 decode_responses=True)
        self._active = True
        xreadgroup_params = self._xreadgroup_params
        if self._use_thread_executor:
            self.executor = futures.ThreadPoolExecutor(
                max_workers=self._read_chunk_length
            )  # TODO: Parametrize max_workers
        tasks: Set[futures.Future] = set()
        while self.active:
            response = redis_conn.xreadgroup(**xreadgroup_params)
            for stream, messages in response:
                for msg_id, payload in messages:
                    if self._use_thread_executor:
                        tasks.add(
                            self.executor.submit(self._handle_message_sync,
                                                 stream, msg_id, payload,
                                                 redis_conn))
                    else:
                        self._handle_message_sync(stream, msg_id, payload,
                                                  redis_conn)
                if self._use_thread_executor:
                    for num, task in enumerate(futures.as_completed(tasks)):
                        task.result()
                        if num > self._read_chunk_length / 2 or not self.active:
                            done, tasks = futures.wait(
                                tasks, return_when=asyncio.FIRST_COMPLETED)
                            xreadgroup_params[
                                'count'] = self._read_chunk_length - len(tasks)
                            break
        redis_conn.close()

    def _handle_message_sync(self, stream: str, msg_id: str,
                             payload: Dict[str, str], redis_conn: redis.Redis):
        with redis_conn.pipeline() as p:
            result = self.process_message_sync(stream,
                                               payload,
                                               redis_conn=redis_conn,
                                               pipeline=p)
            for out_stream, out_msg in result.items():
                p.xadd(out_stream, out_msg)
            p.xack(stream, self._consumer_group_name, msg_id)
            try:
                p.execute()
            except redis.WatchError:  # pragma: nocover
                redis_conn.xack(stream, self._consumer_group_name, msg_id)


class AsyncStreamConsumer(BaseStreamConsumer):
    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError(  # pragma: nocover
            f'Async version of process_message for {type(self).__name__} not implemented.'
        )

    @property
    def supported_impl(self) -> Set[ConsumerImpl]:
        return super().supported_impl | {ConsumerImpl.ASYNC}

    def stop(self, signum=None, frame=None):
        super().stop(signum, frame)
        if len(asyncio.all_tasks()):
            logger.info(
                'Waiting for all asyncio tasks to finish, use Ctrl + C to force exit.'
            )

    async def run_async(self):  # noqa: WPS217
        self.register_signals()
        redis_conn: aioredis.Redis = await aioredis.from_url(
            self._redis_url, encoding='utf-8', decode_responses=True)
        self.active = True
        xreadgroup_params = self._xreadgroup_params
        tasks: Set[asyncio.Future] = set()
        while self.active:
            response = await redis_conn.xreadgroup(**xreadgroup_params)
            for stream, messages in response:
                for msg_id, payload in messages:
                    tasks.add(
                        asyncio.create_task(
                            self._handle_message_async(stream, msg_id, payload,
                                                       redis_conn)))
            for num, task in enumerate(asyncio.as_completed(tasks)):
                await task
                if num > self._read_chunk_length / 2 or not self.active:
                    done, tasks = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED)
                    xreadgroup_params['count'] = self._read_chunk_length - len(
                        tasks)
                    break
        await redis_conn.close()

    async def _handle_message_async(self, stream: str, msg_id: str,
                                    payload: Dict[str, str],
                                    redis_conn: aioredis.Redis):
        async with redis_conn.pipeline() as p:
            result = await self.process_message_async(stream,
                                                      payload,
                                                      redis_conn=redis_conn,
                                                      pipeline=p)
            for out_stream, out_msg in result.items():
                p.xadd(out_stream, out_msg)  # type: ignore
            p.xack(stream, self._consumer_group_name, msg_id)
            try:
                await p.execute()
            except aioredis.WatchError:  # pragma: nocover
                await redis_conn.xack(stream, self._consumer_group_name,
                                      msg_id)


class GearsStreamConsumer(BaseStreamConsumer):
    @property
    def supported_impl(self) -> Set[ConsumerImpl]:
        return super().supported_impl | {ConsumerImpl.GEARS}

    def register_builder(self, pipeline: redis.client.Pipeline):
        raise NotImplementedError  # pragma: nocover
