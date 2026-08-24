import asyncio

from consumer_t9 import RabbitmqConsumerTask9

consumer_uploaded = RabbitmqConsumerTask9("file.uploaded")

if __name__ == "__main__":
    asyncio.run(consumer_uploaded.main())