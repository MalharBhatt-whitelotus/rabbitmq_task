import json
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t26 import RabbitmqConnectionTask26

class RabbitmqConsumerTask26:

    def __init__(self, url: str) -> None:
        self.rabbitmq = RabbitmqConnectionTask26(url)
    async def consume(self, message: IncomingMessage) -> None:
        try:
            body = json.loads(message.body.decode())
            corrupt = body.get("corrupt", True)
            retry_count = body.get("retry_count",0)
            if corrupt == True:
                raise Exception
            await message.ack()
        except Exception as exc:
            if retry_count  < 3:
                retry_count+= 1
                body["retry_count"] = retry_count
                await self.rabbitmq.retry_exchange.publish(
                    message=aio_pika.Message(body=json.dumps(body).encode()),
                    routing_key="retry_task26.key"
                )
                print("Message published to retry...")
                await message.ack()
            else:
                print("Maximu attempts reached...")
                await self.rabbitmq.dlq_exchange.publish(
                    message=aio_pika.Message(json.dumps(body).encode()),
                    routing_key="dlq_task26.key"
                )
                await message.ack()
                print("DlQ message published..")

    async def main(self):
        await self.rabbitmq.connect()
        await self.rabbitmq.queue.consume(self.consume)
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(
        RabbitmqConsumerTask26(
            "amqp://guest:guest@localhost:5672"
        ).main()
    )