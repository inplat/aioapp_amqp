import asyncio
import uuid
import pytest
from typing import Optional
import aioamqp.channel
import aioamqp.envelope
import aioamqp.properties
from aioapp.app import Application
from aioapp.error import PrepareError
from aioapp_amqp import Amqp, Channel, AmqpTracerConfig
from aioapp.misc import async_call
import aiozipkin.span as azs
import aiozipkin.aiohttp_helpers as azah


async def _start_amqp(app: Application, url: str,
                      channels,
                      connect_max_attempts=10,
                      connect_retry_delay=1.0) -> Amqp:
    amqp = Amqp(url, channels=channels,
                connect_max_attempts=connect_max_attempts,
                connect_retry_delay=connect_retry_delay)
    app.add('amqp', amqp)
    await app.run_prepare()
    return amqp


def _create_span(app) -> Optional[azs.SpanAbc]:
    if app.tracer:
        span = app.tracer.new_trace(sampled=True, debug=False)
        span.kind(azah.SERVER)
        span.name('test')
        return span


async def test_amqp(app, rabbitmq):
    messages = []
    msg_id = str(uuid.uuid4()).encode('UTF-8')
    fut = asyncio.Future(loop=app.loop)
    span = _create_span(app)
    checks = []

    class Pubchannel(Channel):
        name = 'pub'

        def __init__(self, exchange, queue):
            self.exchange = exchange
            self.queue = queue

        async def start(self):
            await self._safe_declare_exchange(self.exchange, 'direct',
                                              durable=False)
            await self._safe_declare_queue(self.queue, durable=False)
            await self.open()
            await self.channel.queue_bind(self.queue, self.exchange, '')

            res = await self._safe_declare_exchange(self.exchange, 'direct',
                                                    durable=True)
            if res is None:
                checks.append(True)
            res = await self._safe_declare_queue(self.queue, durable=True)
            if res is None:
                checks.append(True)

    class SubChannel(Channel):
        name = 'sub'

        def __init__(self, queue):
            self.queue = queue

        async def start(self):
            await self._safe_declare_queue(self.queue)
            await self.open()
            await self.consume(self.callback, self.queue)

        async def callback(self, ctx,
                           channel: aioamqp.channel.Channel, body: bytes,
                           envelope: aioamqp.envelope.Envelope,
                           properties: aioamqp.properties.Properties):
            await self.ack(ctx, envelope.delivery_tag)
            if ctx:
                ctx.annotate(body.decode('UTF-8'))
            if body == msg_id:
                messages.append((channel, body, envelope, properties))
                fut.set_result((channel, body, envelope, properties))

    amqp = await _start_amqp(app, rabbitmq, channels=[
        Pubchannel('test_ex', 'test_queue'),
        SubChannel('test_queue'),
    ])

    null_channel = amqp.channel(None)
    assert null_channel is None

    not_existing_channel = amqp.channel('blabla')
    assert not_existing_channel is None

    assert checks == [True, True]  # safe declare is ok

    await amqp.channel('pub').publish(span, b'hellow',
                                      'test_ex', '',
                                      tracer_config=AmqpTracerConfig())

    await amqp.channel('pub').publish(span, msg_id,
                                      'test_ex', '')
    await asyncio.wait_for(fut, timeout=3, loop=app.loop)
    await asyncio.sleep(.1, loop=app.loop)  # wait other messages(if bug)
    result = fut.result()
    assert result[1] == msg_id
    assert len(messages) == 1


async def test_start_without_channels(app, rabbitmq):
    amqp = await _start_amqp(app, rabbitmq, channels=None)

    null_channel = amqp.channel(None)
    assert null_channel is None

    not_existing_channel = amqp.channel('blabla')
    assert not_existing_channel is None


async def test_not_uniq_ch_names(app, rabbitmq):
    class Pubchannel(Channel):
        name = 'not_uniq'

    with pytest.raises(UserWarning):
        await _start_amqp(app, rabbitmq, channels=[
            Pubchannel(),
            Pubchannel(),
        ])


async def test_amqp_prepare_failure(app, unused_tcp_port):
    with pytest.raises(PrepareError):
        await _start_amqp(app,
                          'amqp://guets:guest@%s:%s/'
                          '' % ('127.0.0.1', unused_tcp_port),
                          channels=None,
                          connect_max_attempts=2,
                          connect_retry_delay=0.001
                          )


async def test_amqp_health_bad(app: Application, unused_tcp_port: int,
                               loop: asyncio.AbstractEventLoop) -> None:
    url = 'amqp://guest:guest@%s:%s/' % ('127.0.0.1', unused_tcp_port)

    amqp = Amqp(url)
    app.add('amqp', amqp)

    async def start():
        await app.run_prepare()
        await amqp.start()

    res = async_call(loop, start)
    await asyncio.sleep(1)

    result = await app.health()
    assert 'amqp' in result
    assert result['amqp'] is not None
    assert isinstance(result['amqp'], BaseException)

    if res['fut'] is not None:
        res['fut'].cancel()


async def test_amqp_health_ok(app: Application, rabbitmq: str,
                              loop: asyncio.AbstractEventLoop) -> None:
    amqp = Amqp(rabbitmq)
    app.add('amqp', amqp)

    async def start():
        await app.run_prepare()
        await amqp.start()

    res = async_call(loop, start)
    await asyncio.sleep(1)

    result = await app.health()
    assert 'amqp' in result
    assert result['amqp'] is None

    if res['fut'] is not None:
        res['fut'].cancel()
