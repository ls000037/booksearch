import sys
sys.path.append("..")
from sanic import Sanic
import aioredis
from sanic.response import text,json
from sanic_session import Session, AIORedisSessionInterface
from sanic_cors import CORS
from elasticsearch_dsl import Search
from elasticControl import es_con
import itertools


app=Sanic(name="redistest")

# CORS(app)

#初始化sanic session
# session = Session()
# @app.listener('before_server_start')
# async def server_init(app,loop):
    # redis = await aioredis.from_url("redis://localhost")
#     # init extensions fabrics
#     session.init_app(app, interface=AIORedisSessionInterface(app.redis))


#获取redis连接池
async def get_redis_pool():
    return (await aioredis.from_url("redis://localhost",decode_responses=True))
    # redis=await aioredis.from_url("redis://localhost",decode_responses=True)


@app.route("/redis")
async def test(request):
    # redis = await get_redis_pool()
    # status = await redis.get("status_" + '07040bd5145411ecb495005056bec555')
    return text('07040bd5145411ecb495005056bec5551')
    # print("status", status)

@app.route('/category', methods=["GET"])
async def publisherExport(requset):
    if requset.method == 'GET':
        # q0= Q('range', **{'create_time':{'gte':'0000-01-01T00:00:00','lte':'9999-12-31T23:59:59'}})

        search_context = Search(using=es_con, index="dwd_book_base_info")
        # s=search_context.sort('category',{"lines" : {"order" : "asc", "mode" : "avg"}})
        # s=search_context.sort({"store_pricing":{"order" : "asc", "mode" : "avg"}})
        s=search_context.sort("-store_pricing")
        try:
            results = s.execute()
            # results=s.scan()
            total = s.count()
        except Exception:
            results = []
            total = 0
        # for i in itertools.islice(results,0,5):
        page=1
        # print(results['hits']['hits'])
        for i in results['hits']['hits']:
        # for i in results:
            page += 1
            if page == 11:
                break
            print(i['_source']['store_pricing'])
            # print(i.book_name,i.store_pricing)
        # print(results,total)
        return text("111")


# if __name__ == '__main__':
app.run(host="0.0.0.0",port=9002,debug=True,workers=4)
# get_value()
