from sanic import Sanic
from api import api
from sanic_cors import CORS
import aioredis
from sanic.response import text
from auth import protected
from sanic_session import Session, AIORedisSessionInterface
import commons
from motor.motor_asyncio import AsyncIOMotorClient
from jinja2 import Environment, FileSystemLoader
import os
from sanic.response import html

app = Sanic(name="book")

app.config.SECRET = "KEEP_IT_SECRET_KEEP_IT_SAFE"
app.config.RESPONSE_TIMEOUT = 130
app.blueprint(api)
app.static("/", os.getcwd()+'/templates')
CORS(app)
# 初始化sanic session
session = Session()


@app.listener('before_server_start')
async def server_init(app, loop):
    # app.redis = await aioredis.from_url("redis://localhost")
    # # init extensions fabrics
    # session.init_app(app, interface=AIORedisSessionInterface(app.redis, expiry=3600))
    # 初始化mongodb
    commons.db = AsyncIOMotorClient("127.0.0.1", 27017)

    # 初始化jinja2
    template_path = os.path.join(os.getcwd(), 'templates')
    app.template_env = Environment(loader=FileSystemLoader(template_path), enable_async=True)

# @app.route("/redis")
# @protected
# async def test(request):
#     # redis = await get_redis_pool()
#     # data = await redis.get("username")
#
#     return text("111")

#接入前端静态文件
@app.route("/",methods=['GET'])
async def index(request):
    template = request.app.template_env.get_template('index.html')
    #异步模板渲染   同步为：content =  template.render()
    content = await template.render_async()
    return html(content)



# if __name__ == '__main__':
app.run(host="0.0.0.0", port=9001, debug=True, workers=4)
