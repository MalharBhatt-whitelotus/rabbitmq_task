import json
import asyncio
import aio_pika

from rabbitmq_t10 import RabbitmqConnectionTask10


async def consume(
    message: aio_pika.IncomingMessage,
    exchange
):
    body = json.loads(message.body.decode())

    retry_attempt = body.get("retry_attempt", 0)

    try:
        print("\nProcessing message body")
        print(f"Attempt number: {retry_attempt + 1}")

        await process_msg(body)

        # Processing succeeded
        await message.ack()

        print("Message processed successfully and acknowledged")

    except Exception as exc:
        print(f"Processing message failed: {exc}")

        if retry_attempt < 3:

            body["retry_attempt"] = retry_attempt + 1

            await exchange.publish(
                aio_pika.Message(
                    json.dumps(body).encode("utf-8")
                ),
                routing_key="file.uploaded"
            )

            # Acknowledge original message
            await message.ack()

            print(
                f"Retry message published. "
                f"Retry count = {body['retry_attempt']}"
            )

        else:
            print("Maximum retry attempts reached.")

            # Don't retry anymore
            await message.ack()


async def process_msg(body):
    print("Processing...")
    await asyncio.sleep(2)
    # Deliberately fail for testing
    raise Exception("Something went wrong!")


async def main():

    rabbitmq = RabbitmqConnectionTask10(
        "amqp://guest:guest@localhost:5672"
    )

    await rabbitmq.connect()

    print("Declaring queue...")

    queue = await rabbitmq.channel.declare_queue(
        name="file_queue",
        durable=True
    )

    print("Queue declared...")

    await queue.bind(
        exchange=rabbitmq.exchange,
        routing_key="file.uploaded"
    )

    print("Queue bound...")

    async def callback(message):
        print("Message received!")
        await consume(message, rabbitmq.exchange)

    print("Starting consumer...")

    await queue.consume(
        callback,
        no_ack=False
    )

    print("Consumer started. Waiting for messages...")

    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())