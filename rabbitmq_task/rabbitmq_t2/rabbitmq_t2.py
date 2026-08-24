import aio_pika

class RabbitmqConnectionTask2:

    def __init__(self, url: str):
        self.url = url
        self.connection = None
        self.channel = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(url=self.url)
        self.channel = await self.connection.channel()
        print("Rabbitmq connected.")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("RabbitMQ connection closed.")