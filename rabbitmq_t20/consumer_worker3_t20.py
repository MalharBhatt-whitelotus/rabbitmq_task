import asyncio
from consumer_t20 import RabbitmqConsumerTask20
if __name__ == "__main__":
    asyncio.run(RabbitmqConsumerTask20(3).main())