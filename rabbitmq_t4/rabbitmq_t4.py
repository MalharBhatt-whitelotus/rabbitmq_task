import aio_pika

class RabbitmqConnectionTask4:

    def __init__(self, url: str):
        self.url = url
        self.connection = None
        self.channel = None

    async def connect(self):

        self.connection = await aio_pika.connect(self.url)
        self.channel = await self.connection.channel()

        print("Rabbitmq task 4 connected.")

    async def close(self):
        
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task 4 closed.")