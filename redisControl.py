import aioredis
async def get_redis_pool():
    return (await aioredis.from_url("redis://127.0.0.1", decode_responses=True))