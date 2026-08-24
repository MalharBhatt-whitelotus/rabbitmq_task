import json
import signal
import asyncio
from aio_pika import IncomingMessage

from rabbitmq_t27 import RabbitmqConnectionTask27
from publisher_t27 import RabbitmqPublisherTask27


class RabbitmqConsumerTask27:


    def __init__(self, url: str) -> None: 
        self.rabbitmq = RabbitmqConnectionTask27(url=url)
        self.retry_publisher = None
        self.dlq_publisher = None
        self.consumer_tag = None


    async def consumer(self, message: IncomingMessage) -> None:
        try:
            body = json.loads(message.body.decode())
            headers = message.headers
            retry_count = headers.get("retry_count", 0)
            message_id = headers.get("message_id")
            cor_id = message.correlation_id
            timestamp = message.timestamp
            print("\n=====================================")
            print("Message Received...")
            print(f"MessageId: {message_id}")
            print(f"Retry Count: {retry_count}")
            print(f"Cor_ID: {cor_id}")
            print(f"Time: {timestamp}")
            await self.process_msg(message_id, body)
            await message.ack()
            print("Message Acknowledged...")
            print("\n=====================================")
        except Exception:
            try:
                if retry_count < self.rabbitmq.attempts:
                    retry_msg = {
                        "body": body,
                        "headers": {
                            **headers,
                            "retry_count": retry_count + 1,
                        },
                    }
                    await self.retry_publisher.publish(
                        message=retry_msg,
                        routing_key=f"retry_{retry_count}_task27.key"
                    )
                    await message.ack()
                else:
                    dlq_msg = {
                        "body":{
                            "event": "file.uploaded",
                            "file_id": body.get("file_id"),
                            "retry_count": retry_count,
                            "failure_reason": "PDF processing failed",
                            "original_exchange": "main_exchange_task27",
                            "original_routing_key": "main_task27.key",
                            "body": body,
                            "correlation_id": cor_id,
                            "timestamp": str(timestamp),
                        },
                        "headers":{
                            **headers
                        }
                    }
                    await self.dlq_publisher.publish(
                        message=dlq_msg,
                        routing_key="dlq_task27.key",
                    )
                    await message.nack(requeue=False)
            except Exception as exc:
                print(f"Something went wrong...{exc}")
                raise


    async def process_msg(self, message_id: int, body: dict) -> None:
        try:
            print("Processing message")
            if message_id in self.rabbitmq.message_ids:
                raise Exception("Duplicate messages not allowed...")
            await asyncio.sleep(2)
            self.rabbitmq.message_ids.add(message_id)
            print(body)
            print("Message Processed...")
        except Exception:
            raise


    async def start_consumer(self) -> None:
        self.consumer_tag = await self.rabbitmq.main_queue.consume(self.consumer)


    async def stop_consumer(self) -> None:
        if self.consumer_tag and self.rabbitmq.main_queue:
            await self.rabbitmq.main_queue.cancel(self.consumer_tag)


    async def main(self):
        await self.rabbitmq.connect()
        self.retry_publisher = RabbitmqPublisherTask27(self.rabbitmq.retry_exchange)
        self.dlq_publisher = RabbitmqPublisherTask27(self.rabbitmq.dlq_exchange)
        await self.start_consumer()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        try:
            await stop_event.wait()
        finally:
            print("Shuting down the consumer...")
            await self.stop_consumer()
            await self.rabbitmq.close()