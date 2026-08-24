import json
import signal
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t20 import RabbitmqConnectionTask20

class RabbitmqConsumerTask20:


    def __init__(self, consumer_number: int) -> None:
        self.rabbitmq = RabbitmqConnectionTask20(url="amqp://guest:guest@localhost:5672")
        self.consumer_number = consumer_number
        self.consumer_tag = None

        

    async def consume(self, message: IncomingMessage) -> None:
        try:
            print("Receiving Message...")
            body = json.loads(message.body.decode())
            message_id = body.get("message_id", 0)
            retry_count = body.get("retry_count", 0)
            print(f"Message ID: {message_id}")
            print(f"Retry Count: {retry_count}")
            await self.process_msg(message_id)
            await message.ack()
            print(f"Message {message_id} is acknowledged...")
        except Exception as exc:
            if retry_count < 3:
                print(f"Retry message {message_id} for processing...")
                retry_count += 1
                body["retry_count"] = retry_count
                retry_msg = aio_pika.Message(body=json.dumps(body).encode())
                try:
                    print(f"Publishing message {message_id} to retry exchange...")
                    await self.rabbitmq.retry_exchange.publish(message=retry_msg, routing_key="retry.task20")
                    await message.ack()
                    print(f"Message {message_id} is published to retry exchange...")
                except Exception as exc:
                    print(f"Something went wrong in retry publishing for message {message_id}: {exc}")
                    raise
            else:
                print(f"Maximum retry counts reached for message {message_id}...")
                print(f"Publishing message {message_id} to dlq exchange...")
                dlq_message = aio_pika.Message(json.dumps(body).encode())
                try:
                    await self.rabbitmq.dlq_exchange.publish(message=dlq_message, routing_key="dlq_task20.key")
                    await message.ack()
                    print(f"Message {message_id} is published to dlq exchange...")
                except Exception as exc:
                    print(f"Something went wrong with message {message_id} while publishing in dlq exchange...")
                    raise


    async def process_msg(self, message_id: int) -> None:
        try:
            print(f"Processing message {message_id}")
            if message_id in self.rabbitmq.message_ids:
                raise Exception(f"Duplicates messages for {message_id} are not allowed...")
            await asyncio.sleep(2)
            self.rabbitmq.message_ids.add(message_id)
            print(f"Message {message_id} is processed....")
        except Exception as exc:
            print(f"Somethinf went wrong while processing message {message_id}...")
            raise

    async def start_consumer(self) -> None:
        self.consumer_tag = await self.rabbitmq.main_queue.consume(self.consume)
        print(f"Consumer {self.consumer_number} is ready for consuming messages...")

    async def stop_consumer(self) -> None:
        print(f"Stopping Consumer {self.consumer_number}...")
        if self.consumer_tag and self.rabbitmq.main_queue:
            await self.rabbitmq.main_queue.cancel(self.consumer_tag)
        print(f"Consumer {self.consumer_number} is stopped...")

    async def main(self) -> None:
        await self.rabbitmq.connect()
        await self.start_consumer()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                stop_event.set,
            )
        print(f"Consumer {self.consumer_number} is running...")
        print(f"Press CTRL+C to shutdown...")
        try:
            await stop_event.wait()
        finally:
            print(f"Shutdown signal received for Consumer {self.consumer_number}..")
            await self.stop_consumer()
            print(f"Closing connection for consumer {self.consumer_number}")
            await self.rabbitmq.close()
            print(f"Shutdown for Consumer {self.consumer_number} completed...")