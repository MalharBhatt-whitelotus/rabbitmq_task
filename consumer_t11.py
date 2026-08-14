import json
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t11 import RabbitmqConnectionTask11

async def consume(message: IncomingMessage, exchange, dlq_exhange):
    try:
        body = json.loads(message.body.decode())
        retry_count = body.get("retry_count", 0)
        retry_count += 1
        body["retry_count"] = retry_count
        print(f"Attempt: {retry_count}")
        await processing_msg(body)
        print("Message is processed.")
        await message.ack()
    except Exception as exc:
        print(f"Something went wrong. {exc}")
        if retry_count < 3:
            print("Retrying message processing ...")
            new_message = aio_pika.Message(
                body=json.dumps(body).encode("utf-8"), delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                )
            await exchange.publish(new_message, routing_key="file.uploaded")
            await message.ack()
        else:
            print("Maximum retries reached ... sending message to dlq..")
            dlq_message = aio_pika.Message(
                body=json.dumps(body).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await dlq_exhange.publish(dlq_message,routing_key="failed",)
            await message.ack()

async def processing_msg(message):
    print("Message is processing...")
    await asyncio.sleep(2)
    raise Exception("Failed to processed message")

async def main():
    rabbitmq = RabbitmqConnectionTask11("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    file_queue = await rabbitmq.channel.declare_queue(name="file_queue",durable=True,)
    await file_queue.bind(exchange=rabbitmq.exchange, routing_key="file.uploaded")
    async def callback(message: IncomingMessage):
        await consume(
            message=message, 
            exchange=rabbitmq.exchange, 
            dlq_exhange=rabbitmq.dlq_exchange
            )

    await file_queue.consume(callback=callback, no_ack=False)
    print("Consumer started...")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())