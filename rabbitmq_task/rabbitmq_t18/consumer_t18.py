import json
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t18 import RabbitmqConnectionTask18

async def consume(message: IncomingMessage, rabbitmq: RabbitmqConnectionTask18) -> None:
    try:
        body = json.loads(message.body.decode())
        message_id = body.get("message_id", 0)
        retry_count = body.get("retry_count", 0)
        print(f"Message id: {message_id}")
        print(f"Retry count: {retry_count}")
        await process_message(message_id=message_id, message_ids=rabbitmq.message_ids)
        await message.ack()
        print(body)
        print(f">>> Message {message_id} is acknowledged...")
    except Exception as exc:
        if retry_count < 3:
            print(F"Retrying message {message_id} processing...")
            retry_count += 1
            body["retry_count"] = retry_count
            new_retry_msg = aio_pika.Message(body=json.dumps(body).encode())
            try:
                await rabbitmq.retry_exchange.publish(new_retry_msg, routing_key="retry.task18")
                await message.ack()
                print(f"Message {message_id} is published to retry queue....")
            except Exception as exc:
                print(f"Something in retry queue went wrong...{exc}")
                raise
        else:
            print(f"Maximum retry counts for Message {message_id} reached...")
            print("Publishing messages to DLQ...")
            dlq_message = aio_pika.Message(body=json.dumps(body).encode())
            try:
                await rabbitmq.dlq_exchange.publish(dlq_message, routing_key="dlq.task18")
                await message.ack()
                print(F"Message {message_id} published to DLQ....")
            except Exception as exc:
                print(f"Something in dlq went wrong...{exc}")
                raise
        

async def process_message(message_id: int, message_ids: set) -> None:
    print(F"Message {message_id} is processing...")
    await asyncio.sleep(2)
    if message_id in message_ids:
        raise Exception("Duplicate messages not allowed..")
    message_ids.add(message_id)
    print(F"Message {message_id} is processed....")

async def main():
    rabbitmq = RabbitmqConnectionTask18("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    async def callback(message: IncomingMessage):
        await consume(message=message, rabbitmq=rabbitmq)
    print("Consumer is ready to consume messages...")
    await rabbitmq.queue.consume(callback=callback, no_ack=False)
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())