import asyncio
from aio_pika import IncomingMessage

async def consume(message: IncomingMessage, consumer_name: str):
    print(f"[{consumer_name}] received...")
    print(f"message={message.body.decode()}")
    await asyncio.sleep(2)
    print(
        f"[{consumer_name}] finished "
        f"message={message.body.decode()}"
    )
    await message.ack()