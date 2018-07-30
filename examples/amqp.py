import os
import logging
import asyncio
import aioamqp.channel
import aioamqp.properties
import aioamqp.envelope
from typing import Optional
from aioapp.app import Application
from aioapp import config
from aioapp.tracer import Span
from aioapp_amqp import Amqp, Channel


class Config(config.Config):
    amqp_url: str
    _vars = {
        'amqp_url': {
            'type': str,
            'name': 'AMQP_URL',
            'descr': 'AMQP connection string in following format '
                     'amqp://user:passwd@host:{port}{vhost}',
            'default': 'amqp://guest:guest@localhost:5672/',
        },
    }


class ConsumerChannel(Channel):
    name = 'consumer'
    queue: Optional[str] = None

    async def start(self):
        await self.open()
        q = await self._safe_declare_queue('')
        self.queue = q['queue']
        await self.consume(self.msg, self.queue)

    async def msg(self, ctx: Span,
                  channel: aioamqp.channel.Channel,
                  body: bytes,
                  envelope: aioamqp.envelope.Envelope,
                  properties: aioamqp.properties.Properties) -> None:
        await self.ack(ctx, envelope.delivery_tag)
        print('received message', body)


class PubChannel(Channel):

    async def start(self):
        await self.open()
        with self.amqp.app.tracer.new_trace() as ctx:
            ctx.name('start_pub')
            print('sending message "hello"')
            await self.publish(ctx, b'hello', '',
                               self.amqp.channel('consumer').queue)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()

    cfg = Config(os.environ)

    app = Application(loop=loop)
    app.add(
        'srv',
        Amqp(cfg.amqp_url, [
            ConsumerChannel(),
            PubChannel(),
        ]),
        stop_after=[])
    app.run()
