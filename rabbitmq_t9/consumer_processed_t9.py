import asyncio

from consumer_t9 import RabbitmqConsumerTask9

consumer_processed = RabbitmqConsumerTask9("file.processed")

if __name__ == "__main__":
    asyncio.run(consumer_processed.main())