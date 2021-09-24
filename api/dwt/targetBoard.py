from sanic import Blueprint
from searchEngine import query_es_return_list
from sanic.response import json
from elasticsearch_dsl import Q
from auth import protected
from bookIndexs import dwt_channel, dwt_category, dwt_sales, dwt_copyright,dwt_copyright_info
import datetime

targetBoard_bp = Blueprint('targetBoard_bp')


# 获取渠道维度指标
@targetBoard_bp.route('/channel-dim-detail', methods=["POST"])
@protected
async def channelDim(requset):
    if requset.method == 'POST':
        source_map = {'0': '全部', '1': "正向选品", '2': "京东", '3': "天猫", '4': "当当", '5': "孔夫子", '6': '中图网'}
        data = requset.json
        source = source_map[data['channel']]
        type = data['type']
        # 匹配渠道
        q_source = Q("term", **{"channel": source})
        # 转换前端传来的下划线参数为字段所需格式
        if type == "premium_categorys":
            type = "premium-categorys"
        elif type == "premium_product":
            type = "premium-product"
        # 匹配指标类型
        q_type = Q("term", **{"type": type})
        # 查询创建时间为昨天的数据
        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        q_range = Q('range', **{"create_time": {'gte': yesterday, 'lte': yesterday}})
        try:
            results, total = query_es_return_list(dwt_channel, q_range & q_source & q_type)
            channel_detail = []
            for idata in results:
                channel_detail.append({"channel": idata.channel, "type": idata.type, "key": idata.key,
                                       "value": idata.value, "totalval": idata.totalval})
            return json({"code": 200, "msg": "查询成功", "data": channel_detail, "total": total})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询渠道维度指标错误"}, ensure_ascii=False)


# 获取销量趋势
@targetBoard_bp.route('/sales-trend', methods=["POST"])
@protected
async def salesTrend(requset):
    if requset.method == 'POST':
        source_map = {'1': "开卷数据", '2': "正向选品"}
        data = requset.json
        source = source_map[data['channel']]
        categorys = data['categorys']
        years = data['years']
        q_source = Q("term", **{"channel": source})
        month = data['months']
        q_month = Q("term", **{"month": month})

        q_category = []
        for category in categorys:
            if q_category:
                q_category = q_category | Q("term", **{"product_type": category})
            else:
                q_category = Q("term", **{"product_type": category})
        #年份多选构造
        q_year = []
        for year in years:
            if q_year:
                q_year = q_year | Q("term", **{"year": year})
            else:
                q_year = Q("term", **{"year": year})

        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        q_range = Q('range', **{"create_time": {'gte': yesterday, 'lte': yesterday}})
        try:
            results, total = query_es_return_list(dwt_sales, q_source & q_category & q_year & q_month & q_range)
            trends = []
            for idata in results:
                trends.append({"channel": idata.channel, "product_type": idata.product_type, "year": idata.year,
                               "value": idata.value})
            return json({"code": 200, "msg": "查询成功", "data": trends, "total": total})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)


# 获取业务员版权数量排名
@targetBoard_bp.route('/copyrights', methods=["POST"])
@protected
async def copyrights(requset):
    if requset.method == 'POST':
        data = requset.json
        year = data['year']
        month = data['month']
        q_year = Q("term", **{"year": year})
        q_month = Q("term", **{"month": month})
        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        q_range = Q('range', **{"create_time": {'gte': yesterday, 'lte': yesterday}})
        try:
            results, total = query_es_return_list(dwt_copyright, q_month & q_year & q_range)
            copyrights = []
            for idata in results:
                copyrights.append({"year": idata.year, "month": idata.month, "uploader": idata.copyright_person,
                                   "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num})
            return json({"code": 200, "msg": "查询成功", "data": copyrights, "total": total})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)

# 获取业务员版权数量排名
@targetBoard_bp.route('/copyrights-info', methods=["POST"])
@protected
async def copyrightsInfo(requset):
    if requset.method == 'POST':
        data = requset.json
        year = data['year']
        month = data['month']
        q_year = Q("term", **{"year": year})
        q_month = Q("term", **{"month": month})
        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        q_range = Q('range', **{"create_time": {'gte': yesterday, 'lte': yesterday}})
        try:
            results, total = query_es_return_list(dwt_copyright_info, q_month & q_year & q_range)
            copyrights = []
            for idata in results:
                copyrights.append({"year": idata.year, "month": idata.month,
                                   "total_copyright_num": idata.total_copyright_num,
                                   "total_shelf_num": idata.total_shelf_num, "total_sales": idata.total_sales})
            return json({"code": 200, "msg": "查询成功", "data": copyrights, "total": total})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)

# 获取类别明细
@targetBoard_bp.route('/categorys-detail', methods=["POST"])
@protected
async def categoryDetail(requset):
    if requset.method == 'POST':
        data = requset.json

        categorys = data['categorys']
        q_category = []
        for category in categorys:
            if q_category:
                q_category = q_category | Q("term", **{"product_type": category})
            else:
                q_category = Q("term", **{"product_type": category})

        publishers = data['publishers']
        q_publisher = []
        for publisher in publishers:
            if q_publisher:
                q_publisher = q_publisher | Q("term", **{"publishing_house": publisher})
            else:
                q_publisher = Q("term", **{"publishing_house": publisher})

        house_resources = data['house_resources']
        q0 = []
        if "-" in house_resources:

            amount = house_resources.split("-")
            # 匹配-100这种年限
            if amount[0] == "":
                if not q0:
                    q0 = Q("range", **{"publishing_house_resources": {'lt': amount[1]}})
                else:
                    q0 = Q("range", **{"publishing_house_resources": {'lt': amount[1]}}) | q0
            # 匹配500-这种年限
            elif amount[1] == "":

                if not q0:
                    q0 = Q("range", **{"publishing_house_resources": {'gt': amount[0]}})
                else:
                    q0 = Q("range", **{"publishing_house_resources": {'gt': amount[0]}}) | q0
            # 匹配100-300这种年限
            elif "" not in amount:

                if not q0:
                    q0 = Q("range", **{"publishing_house_resources": {'gte': amount[0], "lte": amount[1]}})
                else:
                    q0 = Q("range", **{"publishing_house_resources": {'gte': amount[0], "lte": amount[1]}}) | q0
        else:
            return json({"code": 500, "msg": "产品类目明细资源数传参错误"}, ensure_ascii=False)

        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        q_range = Q('range', **{"create_time": {'gte': yesterday, 'lte': yesterday}})
        try:
            results, total = query_es_return_list(dwt_category, q_category & q_publisher & q0 & q_range)
            categorys = []
            for idata in results:
                categorys.append({"year": idata.year, "month": idata.month, "copyright_person": idata.copyright_person,
                                  "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num,
                                  "total_copyright_num": idata.total_copyright_num,
                                  "total_shelf_num": idata.total_shelf_num, "total_sales": idata.total_sales})
            return json({"code": 200, "msg": "查询成功", "data": categorys, "total": total})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)
