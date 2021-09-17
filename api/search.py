from sanic.response import json
from sanic import Blueprint
from searchEngine import get_results
from bookIndexs import search_index
from auth import protected

search_bp = Blueprint('search_bp')
index = search_index


# 全文检索 <page>为页码 <size>为每页数量
@search_bp.route('/search/<pages:str>', methods=['POST'])
@protected
async def search(requset, pages: str):
    if requset.method == 'POST':
        if not pages:
            pages = '1&10'
        page = pages.split('&')[0]
        size = pages.split('&')[1]
        # if not size:
        #     size = 10
        data = requset.json
        try:
            results, total, uuid = await get_results(data, index, int(page), int(size))
            # 测试
            # return json({"error": "take me"}, ensure_ascii=False)
            # results, total = get_results()
            return json({"code": 200, "msg": "查询成功", "data": results, "total": total, "uuid": uuid})
        except Exception as e:
            print("查询失败", e)
            return json({"code": 500, "msg": "查询失败", "err_info": e})
    return json({"code": 500, "msg": "请求方法不允许"}, ensure_ascii=False)


# #默认搜索路由 返回第一页 ，数量为10
@search_bp.route('/search', methods=['POST'])
@protected
async def search(requset):
    if requset.method == 'POST':
        page = 1
        size = 10
        data = requset.json
        try:
            results, total, uuid = await get_results(data, index, page, size)
            # 测试
            # return json({"error": "take me"}, ensure_ascii=False)
            # results, total = get_results()
            return json({"code": 200, "msg": "查询成功", "data": results, "total": total, "uuid": uuid})
        except Exception as e:
            print("查询失败", e)
            return json({"code": 500, "msg": "查询失败", "err_info": e})
    return json({"code": 500, "msg": "请求方法不允许"}, ensure_ascii=False)




# #查询所有结果
@search_bp.route('/search/all', methods=['POST'])
@protected
async def search(requset):
    if requset.method == 'POST':
        page = 0
        size = 0
        data = requset.json
        try:

            results, total, uuid = await get_results(data, index, page, size)
            # 测试
            # return json({"error": "take me"}, ensure_ascii=False)
            # results, total = get_results()
            return json({"code": 200, "msg": "查询成功", "data": results, "total": total, "uuid": uuid})
        except Exception as e:
            print("查询失败", e)
            return json({"code": 500, "msg": "查询失败", "err_info": e})
    return json({"code": 500, "msg": "请求方法不允许"}, ensure_ascii=False)
