import aioredis
async def get_redis_pool():
    return (await aioredis.from_url("redis://localhost", decode_responses=True))