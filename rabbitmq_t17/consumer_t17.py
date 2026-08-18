import json
import asyncio

from aio_pika import IncomingMessage

from rabbitmq_t17 import RabbitmqConnectionTask17

async def consume(message: IncomingMessage, message_ids: set):
    try:
        body = json.loads(message.body.decode())
        message_id = body.get("message_id", 0)
        await process_message(message, message_id, message_ids)
        await message.ack()
        print("Message acknowledged....")
    except Exception as exc:
        print(f"Something went wrong.. {exc}")

async def process_message( message: IncomingMessage, message_id: int, message_ids: set):
    print("Message is processing....")
    if message_id in message_ids:
        await message.ack()
        raise Exception(f"Duplicate messages not allowed...")
    message_ids.add(message_id)
    await asyncio.sleep(5)
    print("Message is processed...")

async def main():
    rabbitmq = RabbitmqConnectionTask17("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    async def callback(message: IncomingMessage):
        await consume(message=message, message_ids=rabbitmq.messages_ids, )
    await rabbitmq.queue.consume(callback=callback)
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())