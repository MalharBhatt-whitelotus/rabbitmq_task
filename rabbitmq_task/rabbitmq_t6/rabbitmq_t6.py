import aio_pika

class RabbitmqConnectionTask6:

    def __init__(self, url):
        self.url = url 
        self.connection = None
        self.channel = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        print("Rabbitmq task6 connected..")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task6 closed..")