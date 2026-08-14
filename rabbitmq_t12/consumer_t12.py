import json
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t12 import RabbitmqConnectionTask12

async def consume(message: IncomingMessage, exchange, retry_exchange, dlq_exchange):
    try:
        body = json.loads(message.body.decode())
        retry_count = body.get("retry_count", 0)
        print(f"Attempt: {retry_count}")
        await process_msg(body)
        await message.ack()
        print("Message acknowledged...")
    except Exception as exc:
        print(f"Something went wrong... {exc}")
        if retry_count < 3:
            print("Retrying....")
            retry_count += 1
            body["retry_count"] = retry_count
            new_message = aio_pika.Message(json.dumps(body).encode())
            print("Sendng to retry queue...")
            await retry_exchange.publish(new_message, routing_key="retry")
            print("Message sent for retry...")
            await message.ack() 
        else:
            print("Maximum attempt reached...")
            dlq_message = aio_pika.Message(json.dumps(body).encode())
            await dlq_exchange.publish(dlq_message, routing_key="failed")
            print("Message sent to DLQ....")
            await message.ack()

async def process_msg(message):
    print("Processing Message...")
    print(message)
    await asyncio.sleep(2)
    if message.get("file_id", 0) % 2 == 0:
        raise Exception("Failed to process message...")
    print("Message processed successfully.")

async def main():
    rabbitmq = RabbitmqConnectionTask12("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    main_queue = await rabbitmq.channel.declare_queue(name="main_queue", durable=True)
    await main_queue.bind(exchange=rabbitmq.exchange, routing_key="file.uploaded")
    async def callback(message: IncomingMessage):
        await consume(message=message, exchange=rabbitmq.exchange, retry_exchange=rabbitmq.retry_exchange, dlq_exchange=rabbitmq.dlq_exchange)
    await main_queue.consume(callback=callback, no_ack=False)
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())