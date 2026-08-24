import json
import signal
import asyncio
import aio_pika
from aio_pika import IncomingMessage, Queue

from rabbitmq_t30 import RabbitmqConnectionTask30
from publisher_t30 import RabbitmqPublisherTask30


class RabbitmqConsumerTask30:


    def __init__(self, url: str, consumer_type: str) -> None:
        self.rabbitmq = RabbitmqConnectionTask30(url=url)
        self.consumer_type = consumer_type
        self.retry_publisher = None
        self.dlq_publisher = None
        self.consumer_tag = None
        self.retry_consumer_tags = []
        self.queues = {
            "file_upload": "file_upload_queue_task30",
            "file_process": "file_process_queue_task30",
            "file_embedding": "file_embedding_queue_task30",
        }
        self.next_phase = {
            "file_upload": "file_process",
            "file_process": "file_embedding",
            "file_embedding": None,
        }
        self.phase_routes = {
            "file_process": (
                "file_process_exchange_task30", "file_process_task30.key"
            ),
            "file_embedding": (
                "file_embedding_exchange_task30", "file_embedding_task30.key"
            )
        }


    async def consumer(self, message: IncomingMessage) -> None:
        try:
            print("\n===============================")
            print("Message Received...")
            body = json.loads(message.body.decode())
            file_id = body.get("file_id")
            headers = message.headers
            retry_count = headers.get("retry_count", 0)
            cor_id = message.correlation_id
            timestamp = message.timestamp
            print(f"File_ID: {file_id}")
            print(f"Retry Count: {retry_count}")
            print(f"Cor_ID: {cor_id}")
            print(f"Timestamp: {timestamp}")
            await self.process_msg(file_id)
            next_phase = self.next_phase.get(self.consumer_type)
            if next_phase:
                exchange_name, routing_key = self.phase_routes.get(next_phase)
                exchange = await self.rabbitmq.channel.get_exchange(exchange_name)
                next_msg = aio_pika.Message(
                    body=json.dumps(body).encode(),
                    headers={
                        **headers,
                        "event": next_phase,
                        "retry_count": 0,
                    },
                    correlation_id=cor_id,
                    timestamp=timestamp,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                )
                await exchange.publish(
                    message=next_msg,
                    routing_key=routing_key,
                )
                print(f"Message {file_id} published from {self.consumer_type} -> {next_phase}")
            await message.ack()
            print(f"Message {file_id} acknowledged by {self.consumer_type}")
        except Exception as exc:
            print(f"[{self.consumer_type}] "
                  f"Processing failed for file {file_id}: "
                  f"{type(exc).__name__}: {exc}"
                  )
            if retry_count < self.rabbitmq.attempts:
                print(f"Retrying {self.consumer_type}...")
                retry_msg = {
                    "body": body,
                    "headers": {
                        **headers,
                        "retry_count": retry_count + 1,
                        "original_exchange": message.exchange,
                        "original_routing_key": message.routing_key,
                    }
                }
                try:
                    await self.retry_publisher.publish(
                        message=retry_msg,
                        routing_key=f"retry_{retry_count}_task30.key"
                    )
                    await message.ack()
                    print(f"Message {file_id} published to retry exchange...")
                except Exception as r_exc:
                    print(f"With Message {file_id}, Something went wrong while retrying: {r_exc}")
                    raise
            else:
                print(f"For message {file_id}, Maximum attempts reached...")
                print(f"Publishing message {file_id} to DLQ exchange...")
                dlq_msg = {
                    "body": body,
                    "headers": {
                        **headers,
                    }
                }
                try:
                    await self.dlq_publisher.publish(
                        message=dlq_msg,
                        routing_key="dlq_task30.key",
                    )
                    await message.ack()
                    print(f"Message {file_id} published to DLQ exchange...")
                except Exception as dlq_exc:
                    print(f"With Message {file_id}, Something went wrong while DLQing: {dlq_exc}")
                    raise


    async def retry_consumer(self, message: IncomingMessage) -> None:
        try:
            print(f"\n========================")
            print("Retry message received...")
            body = json.loads(message.body.decode())
            headers = message.headers
            retry_count = headers.get("retry_count", 0)
            original_exchange = headers.get("original_exchange")
            original_routing_key = headers.get("original_routing_key")
            file_id = body.get("file_id")
            print(f"File_ID: {file_id}")
            print(f"Retry Count: {retry_count}")
            print(f"Original Exchange: {original_exchange}")
            print(f"Original Routing Key: {original_routing_key}")
            if not original_exchange:
                raise ValueError(
                    "Missing original_exchange"
                )
            if not original_routing_key:
                raise ValueError(
                    "Missing original_routing_key"
                )
            exchange = await self.rabbitmq.channel.get_exchange(original_exchange)
            retry_message = aio_pika.Message(
                body=json.dumps(body).encode(),
                headers=headers,
                correlation_id=message.correlation_id,
                timestamp=message.timestamp,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await exchange.publish(
                message=retry_message,
                routing_key=original_routing_key,
            )
            await message.ack()
            print(f"Message {file_id} returned to {original_exchange} -> {original_routing_key}")
        except Exception as exc:
            print(f"Retry consumer failed: {type(exc).__name__}: {exc}")
            raise


    async def dlq_consumer(self, message: IncomingMessage):
            try:
                body = json.loads(message.body.decode())
                headers = message.headers
                print("\n========== DEAD LETTER ==========")
                print(f"Event: {body.get('event')}")
                print(f"File ID: {body.get('file_id')}")
                print(f"Retry Count: {headers.get('retry_count')}")
                print(f"Failure Reason: {body.get('failure_reason')}")
                print(f"Original Exchange: {headers.get('original_exchange')}")
                print(f"Original Routing Key: {headers.get('original_routing_key')}")
                print(f"Timestamp: {message.timestamp}")
                print("=================================\n")
                await asyncio.sleep(3)
                await message.ack()
            except Exception as exc:
                print(f"Something went wrong...{exc}")
                raise


    async def process_msg(self, file_id: int) -> None:
        await asyncio.sleep(2)
        key = f"{self.consumer_type}.{file_id}"
        print(f"Redis key: {key}")
        result = await self.rabbitmq.message_ids.set(
            name=key,
            value="1",
            nx=True,
            ex=86400,
        )
        print(f"Redis SET result: {result}")
        exists = await self.rabbitmq.message_ids.exists(key)
        print(f"Redis key exists: {exists}")
        if not result:
            raise Exception(
                f"Duplicate file at {self.consumer_type} not allowed..."
            )
        

    async def start_consumer(self) -> None:
        if self.consumer_type == "retry":
            for i in range(self.rabbitmq.attempts):
                queue = await self.rabbitmq.channel.get_queue(
                        f"retry{i}_queue_task30"
                    )
                consumer_tag = await queue.consume(self.retry_consumer)
                self.retry_consumer_tags.append((queue, consumer_tag))
                print(f"Retry Consumer started for retry{i}...")
            return
        if self.consumer_type == "dlq":
            self.consumer_tag = await self.rabbitmq.dlq.consume(self.dlq_consumer)
            print("DLQ Consumer is started...")
            return
        queue = await self._get_queue()
        self.consumer_tag = await queue.consume(self.consumer)
        print(f"Consumer {self.consumer_type} is started...")


    async def stop_consumer(self) -> None:
        if self.consumer_type == "retry":
            for queue, consumer_tag in self.retry_consumer_tags:
                await queue.cancel(consumer_tag)
            self.retry_consumer_tags.clear()
            print("Retry consumers stopped...")
            return
        if self.consumer_type == "dlq" and self.rabbitmq.dlq:
            await self.rabbitmq.dlq.cancel(self.consumer_tag)
            print(f"Consumer {self.consumer_type} is stopped...")
            return
        queue = await self._get_queue()
        if self.consumer_tag and queue:
            await queue.cancel(self.consumer_tag)
            print(f"Consumer {self.consumer_type} is stopped...")


    async def _get_queue(self) -> Queue:
        if self.consumer_type not in self.queues:
            raise ValueError(f"Unknown Consumer: {self.consumer_type}")
        return await self.rabbitmq.channel.get_queue(self.queues.get(self.consumer_type))


    async def main(self) -> None:
        await self.rabbitmq.connect()
        self.retry_publisher = RabbitmqPublisherTask30(self.rabbitmq.retry_exchange)
        self.dlq_publisher = RabbitmqPublisherTask30(self.rabbitmq.dlq_exchange)
        print(f"Starting the consumer {self.consumer_type}...")
        await self.start_consumer()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        try:
            await stop_event.wait()
        finally:
            print(f"Shutdown signal received from {self.consumer_type}...")
            print(f"Shutting down the consumer {self.consumer_type}")
            await self.stop_consumer()
            print(f"Closing the connection for consumer {self.consumer_type}")
            await self.rabbitmq.close()