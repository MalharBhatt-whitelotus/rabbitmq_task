import json
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t19 import RabbitmqConnectionTask19


class RabbitmqConsumerTask19:


    def __init__(self, rabbitmq: RabbitmqConnectionTask19) -> None:
        self.rabbitmq = rabbitmq


    async def consume(self, message: IncomingMessage) -> None:
        try:
            body = json.loads(message.body.decode())
            message_id = body.get("message_id", 0)
            retry_count = body.get("retry_count", 0)
            print(f"Processing message :")
            print(f"Message ID: {message_id}")
            print(f"Retry Count: {retry_count}")
            await self.process_msg(message_id=message_id)
            await message.ack()
            print(f"Message {message_id} is acknowledged...")
        except:
            if retry_count < 3:
                print(f"Retrying Message {message_id} processing...")
                retry_count += 1
                body["retry_count"] = retry_count
                retry_msg= aio_pika.Message(
                    body=json.dumps(body).encode()
                )
                try:
                    await self.rabbitmq.retry_exchange.publish(
                        message=retry_msg,
                        routing_key="retry.task19",
                    )
                    await message.ack()
                    print(f"Message {message_id} published to retry exchange...")
                except Exception as exc:
                    print(f"Something went wrong with retry exchange...{exc}")
                    raise
            else:
                print(f"Maximum retry counts reached for {message_id}...")
                print(f"Publishing to DLQ...")
                dlq_msg = aio_pika.Message(
                    body=json.dumps(body).encode()
                )
                try:
                    await self.rabbitmq.dlq_exchange.publish(
                        message=dlq_msg,
                        routing_key="dlq.task19"
                    )
                    await message.ack()
                    print(f"Message {message_id} published to DLQ exchange...")
                except Exception as exc:
                    print(f"Something went wrong with DLQ exchange...{exc}")
                    raise


    async def process_msg(self, message_id: int) -> None:
        print(f"Message {message_id} is under process...")
        if message_id in self.rabbitmq.message_ids:
            raise Exception("Duplicates messages not allowed...")
        await asyncio.sleep(2)
        self.rabbitmq.message_ids.add(message_id)
        print(f"Message {message_id} is processed...")

    
async def main_consumer(cosumer_num: int) -> None:
    rabbitmq = RabbitmqConnectionTask19("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    consumer = RabbitmqConsumerTask19(rabbitmq=rabbitmq)
    try:
        await rabbitmq.main_queue.consume(consumer.consume, no_ack=False)
        print(f"Consumer {cosumer_num} ready for message consuming....")
        await asyncio.Future()
    except Exception as exc:
        print(f"Something went wrong with Consumer {cosumer_num}: {exc}")
        raise