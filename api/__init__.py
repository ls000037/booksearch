from sanic import Blueprint
from .search import search_bp
from .upload import upload_bp
from .login import login
from .download import download_bp
from .category import category_bp
from .dwt import dwt
from .opensearch import opensearch_bp

api = Blueprint.group(search_bp, upload_bp, login, download_bp, category_bp, dwt, opensearch_bp, url_prefix="/api")

# group = Blueprint.group(search, upload)


# 组蓝图中间件
# @group.middleware('response')
# async def group_middleware(request,response):
#     # print(request.ctx.user)
#     request.ctx.user = "wangsen"
#     print('common middleware applied for both bp1 and bp2')
