import json
import signal
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t23 import RabbitmqConnectionTask23


class RabbitmqConsumerTask23:


    def __init__(self, url: str, consumer_number: int) -> None:
        self.rabbitmq = RabbitmqConnectionTask23(url=url)
        self.consumer_num = consumer_number
        self.consumer_tag = None


    async def consume(self, message: IncomingMessage) -> None:
        try:
            print(f"Message Received...")
            body = json.loads(message.body.decode())
            header = message.headers
            retry_count = header.get("retry_count", 0)
            message_id = header.get("message_id")
            timestamp = header.get("timestamp")
            print(f"Message ID: {message_id}")
            print(f"Retry count: {retry_count}")
            print(f"Timestamp: {timestamp}")
            await self.process_msg(message_id)
            print(body)
            await message.ack()
            print(f"Message {message_id} is acknowledged...")
        except Exception as exc:
            if retry_count < self.rabbitmq.attempts:
                print(f"Retrying Message {message_id} processing...")
                routing_key = f"retry_{retry_count}_task23.key"
                try:
                    print(f"Publishing message {message_id} to retry exchange...")
                    retry_msg=aio_pika.Message(
                        body=json.dumps(body).encode(),
                        headers= {
                            **header,
                            "retry_count": retry_count + 1,
                        },
                    )
                    await self.rabbitmq.retry_exchange.publish(
                        message=retry_msg,
                        routing_key=routing_key,
                    )
                    await message.ack()
                    print(f"Message {message_id} is published to Retry Exchange...")
                except Exception as exc:
                    print(f"With message {message_id}, Something went wrong while retrying...{exc}")
                    raise
            else:
                print(f"For message {message_id}, Maximum attempts reached...")
                print(f"Publishing message {message_id} to DLQ...")
                dlq_msg = aio_pika.Message(
                    body= json.dumps(body).encode(),
                    headers= {**header},
                )
                try:
                    await self.rabbitmq.dlq_exchange.publish(
                        message=dlq_msg,
                        routing_key="dlq_task23.key",
                    )
                    await message.ack()
                    print(f"Message {message_id} is published to DLQ...")
                except Exception as exc:
                    print(f"With message {message_id}, Something went wrong while publishing to DLQ...{exc}")
                    raise


    async def process_msg(self, message_id: int) -> None:
        try:
            print(f"Processing message {message_id}...")
            if message_id in self.rabbitmq.message_ids:
                raise Exception(f"Duplicates messages are not allowed: {message_id}")
            await asyncio.sleep(2)
            self.rabbitmq.message_ids.add(message_id)
            print(f"Message {message_id} is processed...")
        except Exception as exc:
            print(f"With Message {message_id}, Something went wrong while processing...{exc}")
            raise

    async def start_cosumer(self):
        print(f"Starting the consumer {self.consumer_num}...")
        self.consumer_tag = await self.rabbitmq.main_queue.consume(self.consume, no_ack=False)
        print(f"Consumer {self.consumer_num} is ready to consume...")

    async def stop_consumer(self):
        print(f"Stopping the consumer {self.consumer_num}...")
        if self.consumer_tag and self.rabbitmq.main_queue:
            await self.rabbitmq.main_queue.cancel(self.consumer_tag)
            print(f"Consumer {self.consumer_num} is stopped...")

    async def main(self):
        await self.rabbitmq.connect()
        await self.start_cosumer()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                stop_event.set,
            )
        print(f"Consumer_{self.consumer_num} application is running...")
        print(f"Press CTRL+C to shutdown...")
        try:
            await stop_event.wait()
        finally:
            print(f"Shutdown signal received for consumer {self.consumer_num}...")
            print(f"Shuting down consumer {self.consumer_num}..")
            await self.stop_consumer()
            print(f"Consumer {self.consumer_num} is shutdown...")
            print(f"Closing the connection for consumer {self.consumer_num}...")
            await self.rabbitmq.close()
            print(f"Connection closed for consumer {self.consumer_num}...")