import json
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t22 import RabbitmqConnectionTask22

class RabbitmqConsumerTask22:


    def __init__(self, url: str, consumer_num: int) -> None:
        self.rabbitmq = RabbitmqConnectionTask22(url)
        self.consumer_num = consumer_num


    async def consume(self, message: IncomingMessage) -> None:
        try:
            body = json.loads(message.body.decode())
            message_id = body.get("message_id", 0)
            retry_count = body.get("retry_count", 0)
            print(f"MessageID: {message_id}")
            print(f"Retry count: {retry_count}")
            await self.process_msg(message_id)
            await message.ack()
            print(f"Message {message_id} is acknowledged...")
        except Exception as exc:
            if retry_count < 3:
                print("Retrying the message processing...")
                routing_key = f"retry_{retry_count}_task22.key"
                retry_count += 1
                body["retry_count"] = retry_count
                retry_msg = aio_pika.Message(body=json.dumps(body).encode())
                try:
                    await self.rabbitmq.retry_exchange.publish(
                        message=retry_msg,
                        routing_key=routing_key,
                    )
                    await message.ack()
                    print(f"Message {message_id} is published to retry Queue with routing key = {routing_key}")
                except Exception as exc:
                    print(f"With Message {message_id}, Something went wrong while retrying...{exc}")
            else:
                print(f"Maximum attempts reached with message {message_id}...")
                print(f"Publishing message to DLQ...")
                dlq_msg = aio_pika.Message(body=json.dumps(body).encode())
                try:
                    await self.rabbitmq.dlq_exchange.publish(
                        message=dlq_msg,
                        routing_key="dlq_task22.key"
                    )
                    await message.ack()
                    print(f"Message {message_id} is published in DLQ...")
                except Exception as exc:
                    print(f"With message {message_id}, Something went wrong while publishing to DLQ...{exc}")
                    raise


    async def process_msg(self, message_id: int) -> None:
        try:
            print(f"Message {message_id} is processing...")
            if message_id in self.rabbitmq.message_ids:
                raise Exception(f"Duplicate message {message_id} not allowed...")
            await asyncio.sleep(2)
            self.rabbitmq.message_ids.add(message_id)
            print(f"Message {message_id} is processed...")
        except Exception as exc:
            print(f"With Message {message_id},Something went wrong while processing...{exc}")
            raise

    async def main(self):
            print(f"Consumer {self.consumer_num} is ready for message consumption...")
            await self.rabbitmq.connect()
            await self.rabbitmq.main_queue.consume(self.consume)
            await asyncio.Future()