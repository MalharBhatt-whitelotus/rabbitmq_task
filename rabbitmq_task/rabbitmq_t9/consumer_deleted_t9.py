import asyncio

from consumer_t9 import RabbitmqConsumerTask9

consumer_deleted = RabbitmqConsumerTask9("file.deleted")

if __name__ == "__main__":
    asyncio.run(consumer_deleted.main())