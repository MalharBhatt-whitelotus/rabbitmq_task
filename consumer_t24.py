import json
import signal
import asyncio
import aio_pika
from aio_pika import IncomingMessage

from rabbitmq_t24 import RabbitmqConnectionTask24


class RabbitmqConsumerTask24:


    def __init__(self, url: str, con_num: int) -> None:
        self.rabbitmq = RabbitmqConnectionTask24(url=url)
        self.con_num = con_num
        self.consumer_tag = None


    async def consume(self, message: IncomingMessage) -> None:
        try:
            print("Message Received...")
            body = json.loads(message.body.decode())
            header = message.headers
            message_id = header.get("message_id")
            retry_count = header.get("retry_count")
            timestamp = header.get("timestamp")
            correlation_id = message.correlation_id
            print(f"MessageId: {message_id}")
            print(f"CorrelationId: {correlation_id}")
            print(f"Retry Count: {retry_count}")
            print(f"Timestamp: {timestamp}")
            print(body)
            await self.process_msg(correlation_id, message_id)
            await message.ack()
            print(f"{correlation_id}:Message-{message_id} is acknowledged...")
        except Exception:
            if retry_count < self.rabbitmq.attempts:
                print(f"Retrying {correlation_id}:Message-{message_id} processing...")
                routing_key = f"retry_{retry_count}_task24.key"
                retry_msg = aio_pika.Message(
                    body=json.dumps(body).encode(),
                    headers={
                        **header,
                        "retry_count": retry_count + 1
                    },
                    correlation_id=correlation_id,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                )
                try:
                    print(f"Publishing {correlation_id}:Message-{message_id} to retry exchange...")
                    await self.rabbitmq.retry_exchange.publish(
                        message=retry_msg,
                        routing_key=routing_key,
                    )
                    await message.ack()
                    print(f"{correlation_id}:Message-{message_id} is published to retry exchange..")
                except Exception as exc:
                    print(f"With {correlation_id}:Message-{message_id} while retrying...exc: {exc}")
                    raise
            else:
                print(f"{correlation_id}:Message-{message_id} has reached maximum attempts...")
                dlq_msg = aio_pika.Message(
                    body=json.dumps(body).encode(),
                    headers={**header},
                    correlation_id=correlation_id,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                )
                try:
                    print(f"Publishing {correlation_id}:Message-{message_id} to DLQ exchange...")
                    await self.rabbitmq.dlq_exchange.publish(
                        message=dlq_msg,
                        routing_key="dlq_task24.key",
                    )
                    await message.ack()
                    print(f"{correlation_id}:Message-{message_id} is published to DLQ exchange...")
                except Exception as exc:
                    print(f"With {correlation_id}:Message-{message_id} while dlqing...exc: {exc}")
                    raise


    async def process_msg(self, correlation_id: str, message_id: int) -> None:
        try:
            print(f"Processing {correlation_id}:Message-{message_id}...")
            await asyncio.sleep(2)
            if message_id in self.rabbitmq.message_ids:
                raise Exception(f"Duplicate {correlation_id}:Message-{message_id} not allowed..")
            self.rabbitmq.message_ids.add(message_id)
            print(f"{correlation_id}:Message-{message_id} is processed...")
        except Exception as exc:
            print(f"With {correlation_id}:Message-{message_id} while processing...exc: {exc}")
            raise


    async def start_consumer(self) -> None:
        print(f"Starting the consumer {self.con_num}...")
        self.consumer_tag = await self.rabbitmq.main_queue.consume(self.consume)
        print(f"Consumer {self.con_num} is running...")
        print("Press CTRL+C to shutdown...")


    async def stop_consumer(self) -> None:
        print(f"Stoppin the consumer {self.con_num}...")
        if self.consumer_tag and self.rabbitmq.main_queue:
            await self.rabbitmq.main_queue.cancel(self.consumer_tag)
            print(f"Consumer {self.con_num} is stopped...")


    async def main(self) -> None:
        await self.rabbitmq.connect()
        await self.start_consumer()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        try:
            await stop_event.wait()
        finally:
            print("Shutdown signal received..")
            await self.stop_consumer()
            print(f"Stopping the connection for consumer {self.con_num}...")
            await self.rabbitmq.close()