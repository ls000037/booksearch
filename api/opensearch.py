from sanic import Blueprint
from sanic.response import json
from auth import protected
from elasticsearch_dsl import Q
from searchEngine import query_es_return_list
from bookIndexs import open_search_index
from redisControl import get_redis_pool
import uuid
import itertools
import json as js

opensearch_bp = Blueprint('opensearch_bp')


async def open_engine(data, page, size):


    book_map = {'1': "畅销书", '2': "新书", '3': "常销书", '4': "滞销书"}
    book_list = "book_list"
    try:
        q1 = Q("term", **{book_list: book_map[data[book_list]]})
    except Exception:
        q1 = []

    channel_map = {'1': "零售", '2': "实体店", '3': "网店"}
    channel_type = "channel_type"
    try:
        q2 = Q("term", **{channel_type: channel_map[data[channel_type]]})
    except Exception:
        q2 = []

    sale_time = "sale_time"
    if data[sale_time]["timerange"][0] == "":
        begin_date = '0000'
    else:
        begin_date = data[sale_time]["timerange"][0]
    if data[sale_time]["timerange"][1] == "":
        end_date = '9999'
    else:
        end_date = data[sale_time]["timerange"][1]
    q3 = Q('range', **{sale_time: {'gte': begin_date, 'lte': end_date}})
    if data[sale_time]["datekey"] == "month":
        q4 = Q("term", **{"sale_type": "月"})
    else:
        q4 = Q("term", **{"sale_type": "年"})
    q = q3 & q4
    if q1:
        q = q1 & q
    if q2:
        q = q & q2
    print(q)

    es_results, total = query_es_return_list(open_search_index, q)
    print(total)
    key_data = ["isbn", "book_name", "selling_price", "discount_rate", "author", "category",
                "publishing_house", "month_sales", "year_sales", "total_sales", "book_list", "channel_type",
                "sale_type",
                "sale_time"]
    results = []
    for i in itertools.islice(es_results, page * size - size, page * size):
        dict_data = {}
        for key in key_data:
            try:
                dict_data[key] = i[key]
                if i[key] == None or i[key] == -1 or i[key] == -0.1:
                    dict_data[key] = "未知"
            except Exception:
                dict_data[key] = "未知"

        results.append(dict_data)
    uuid_value=""
    if results:
        redis = await get_redis_pool()

        uuid_value = uuid.uuid1().hex
        uuid_str_redis = "searchid_" + uuid_value
        tmpdata = js.dumps(data)
        await redis.set(uuid_str_redis, tmpdata)
        await redis.expire(uuid_str_redis, 3600)
        await redis.close()
    return results, total, uuid_value


@opensearch_bp.route('/opensearch/<pages:str>', methods=['POST'])
@protected
async def opensearch(requset, pages: str):
    if requset.method == 'POST':
        # print("getttt")
        if not pages:
            pages = '1&10'
        page = pages.split('&')[0]
        size = pages.split('&')[1]
        if not size:
            size = 10
        data = requset.json

        try:
            #     pass
            results, total, uuid = await open_engine(data, int(page), int(size))
            # # 测试
            # # return json({"error": "take me"}, ensure_ascii=False)
            # # results, total = get_results()

            return json({"code": 200, "msg": "查询成功", "data": results, "total": total, "uuid": uuid})

        except Exception as e:
            print("查询失败", e)
            return json({"code": 500, "msg": "查询失败", "err_info": e})
    return json({"code": 500, "msg": "请求方法不允许"}, ensure_ascii=False)
