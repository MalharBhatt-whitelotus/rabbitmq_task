import aio_pika

class RabbitmqConnectionTask5:

    def __init__(self, url: str):
        self.url = url
        self.connection = None
        self.channel = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        print("Rabbitmq task5 connect")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq connection task5 closed.")