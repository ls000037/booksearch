from sanic import Blueprint
from searchEngine import query_es_return_list
from sanic.response import json
from elasticsearch_dsl import Q
from auth import protected
from bookIndexs import dwt_channel, dwt_category, dwt_sales, dwt_copyright, dwt_copyright_info
import datetime

targetBoard_bp = Blueprint('targetBoard_bp')

# 查询创建时间为昨天的数据
today = datetime.date.today()
oneday = datetime.timedelta(days=1)
yesterday = today - oneday
q_range = Q('term', **{"create_time": yesterday})


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
        q_type = Q("term", **{"metric_type": type})
        q_range = Q('term', **{"create_time": "2021-09-26"})
        if data['type'] == "resources":
            # q_all = q_range & q_type
            q_all = q_range & q_type
        else:
            q_all = q_range & q_source & q_type
        try:
            results, total = query_es_return_list(dwt_channel, q_all)
            channel_detail = []
            for idata in results:
                channel_detail.append({"channel": idata.channel, "type": idata.metric_type, "key": idata.key,
                                       "value": idata.value, "totalval": idata.total_val})
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
        # month = data['months']
        # q_month = Q("term", **{"month": month})
        q_category = []
        for category in categorys:
            if q_category:
                q_category = q_category | Q("term", **{"product_type": category})
            else:
                q_category = Q("term", **{"product_type": category})
        # 年份多选构造
        q_year = []
        for year in years:
            if q_year:
                q_year = q_year | Q("term", **{"year": year})
            else:
                q_year = Q("term", **{"year": year})

        if categorys[0] == "all":
            q_all = q_source & q_year & q_range
        else:
            q_all = q_source & q_category & q_year & q_range

        try:
            results, total = query_es_return_list(dwt_sales, q_all)

            trends = []

            i = 0
            for idata in results:
                # 全部分类下的汇总数据
                if categorys[0] == "all":
                    # 如果为字典为空，则添加第一条数据
                    # if not temp_dict:
                    #
                    #     temp_dict = {"channel": idata.channel, "product_type": 'all', "year": idata.year,
                    #                  "month": idata.month,
                    #                  "value": idata.value, "index": i}
                    #     trends.append(temp_dict)
                    #
                    # else:
                    # 判断是否有可以汇总的数据
                    ikey = True
                    for itemp in trends:
                        if idata.channel == itemp["channel"] and idata.year == itemp["year"] and idata.month == \
                                itemp["month"]:
                            itemp['value'] = itemp['value'] + idata.value
                            trends[itemp['index']] = itemp
                            ikey = False
                            break
                    # 如果没有则新添加一条数据
                    if ikey:
                        temp_dict = {"channel": idata.channel, "product_type": 'all', "year": idata.year,
                                     "month": idata.month,
                                     "value": idata.value, "index": i}
                        i += 1
                        trends.append(temp_dict)


                # 指定类别的数据
                else:
                    trends.append({"channel": idata.channel, "product_type": idata.product_type, "year": idata.year,
                                   "month": idata.month,
                                   "value": idata.value})
            return json({"code": 200, "msg": "查询成功", "data": trends, "total": total})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)


# 获取销量趋势---类别信息
@targetBoard_bp.route('/categorys', methods=["GET"])
@protected
async def publishers(requset):
    if requset.method == 'GET':
        try:
            results, total = query_es_return_list(dwt_sales, q_range)
            productTypes = []
            try:
                for idata in results:
                    if idata.product_type not in productTypes:
                        productTypes.append(idata.product_type)
            except Exception:
                productTypes = []

            return json(
                {"code": 200, "msg": "查询成功", "data": {"product_type": productTypes}, "total": len(productTypes)})
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

        q_all = q_month & q_year & q_range
        if year == "all" and month == 'all':
            q_all = q_range
        if year == "all" and month != 'all':
            q_all = q_month & q_range
        if year != 'all' and month == 'all':
            q_all = q_year & q_range
        try:
            results, total = query_es_return_list(dwt_copyright, q_all)
            copyrights = []
            # temp_dict = {}
            i = 0
            for idata in results:
                temp_dict = {"year": idata.year, "month": idata.month, "copyright_person": idata.copyright_person,
                             "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                copyrights.append(temp_dict)
                # 如果为字典为空，则添加第一条数据
                # if not temp_dict:
                #     if year != 'all' and month != 'all':
                #         temp_dict = {"year": idata.year, "month": idata.month, "copyright_person": idata.copyright_person,
                #          "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                #     if year == "all" and month == 'all':
                #         temp_dict = {"year": "all", "month": "all",
                #                      "copyright_person": idata.copyright_person,
                #                      "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                #
                #     if year == "all" and month != 'all':
                #         temp_dict = {"year": "all", "month": month,
                #                      "copyright_person": idata.copyright_person,
                #                      "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                #     if year != 'all' and month == 'all':
                #         temp_dict = {"year": year, "month": "all",
                #                      "copyright_person": idata.copyright_person,
                #                      "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                #     copyrights.append(temp_dict)
                # else:
                # 判断是否有可以汇总的数据
                # ikey = True
                # for itemp in copyrights:
                #     if idata.copyright_person == itemp["copyright_person"]:
                #         itemp['copyright_num'] = itemp['copyright_num'] + idata.copyright_num
                #         itemp['shelf_num'] = itemp['shelf_num'] + idata.shelf_num
                #         copyrights[itemp['index']] = itemp
                #         ikey = False
                #         break
                # 如果没有则新添加一条数据
                # if ikey:
                #
                #     if year != 'all' and month != 'all':
                #         temp_dict = {"year": idata.year, "month": idata.month, "copyright_person": idata.copyright_person,
                #      "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                #     if year == "all" and month == 'all':
                #         temp_dict = {"year": "all", "month": "all",
                #                            "copyright_person": idata.copyright_person,
                #      "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                #     if year == "all" and month != 'all':
                #         temp_dict = {"year": "all", "month": month,
                #                            "copyright_person": idata.copyright_person,
                #                            "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                #     if year != 'all' and month == 'all':
                #         temp_dict = {"year": year, "month": "all",
                #                            "copyright_person": idata.copyright_person,
                #                            "copyright_num": idata.copyright_num, "shelf_num": idata.shelf_num, "index": i}
                #     i += 1
                #     copyrights.append(temp_dict)

            # 重组数据，以适应前端图表所需格式
            iperson = []
            icopyright = []
            ishelf = []
            for data in copyrights:
                iperson.append(data['copyright_person'])
                icopyright.append(data['copyright_num'])
                ishelf.append(data['shelf_num'])

            return json({"code": 200, "msg": "查询成功",
                         "data": {"year": year, "month": month, "iperson": iperson, "icopyright": icopyright,
                                  "ishelf": ishelf}, "total": len(copyrights)})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)


# 获取业务员版权数量排名--总量信息
@targetBoard_bp.route('/copyrights-info', methods=["POST"])
@protected
async def copyrightsInfo(requset):
    if requset.method == 'POST':
        data = requset.json
        year = data['year']
        month = data['month']
        q_year = Q("term", **{"year": year})
        q_month = Q("term", **{"month": month})

        q_all = q_month & q_year & q_range
        if year == "all" and month == 'all':
            q_all = q_range
        if year == "all" and month != 'all':
            q_all = q_month & q_range
        if year != 'all' and month == 'all':
            q_all = q_year & q_range

        try:
            results, total = query_es_return_list(dwt_copyright_info, q_all)
            copyrights = []
            total_copyright_num = 0
            total_shelf_num = 0
            total_sales = 0

            for idata in results:

                if year != 'all' and month != 'all':
                    copyrights.append({"year": idata.year, "month": idata.month,
                                       "total_copyright_num": idata.total_copyright_num,
                                       "total_shelf_num": idata.total_shelf_num, "total_sales": idata.total_sales})
                else:
                    total_copyright_num += idata.total_copyright_num
                    total_shelf_num += idata.total_shelf_num
                    total_sales += idata.total_sales
            if year == "all" and month == 'all':
                copyrights.append({"year": "all", "month": "all",
                                   "total_copyright_num": total_copyright_num,
                                   "total_shelf_num": total_shelf_num, "total_sales": total_sales})
            if year == "all" and month != 'all':
                copyrights.append({"year": "all", "month": month,
                                   "total_copyright_num": total_copyright_num,
                                   "total_shelf_num": total_shelf_num, "total_sales": total_sales})
            if year != 'all' and month == 'all':
                copyrights.append({"year": year, "month": "all",
                                   "total_copyright_num": total_copyright_num,
                                   "total_shelf_num": total_shelf_num, "total_sales": total_sales})

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
        channel = data['channel']
        q_channel = Q("term", **{"channel": channel})

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
                    q0 = Q("range", **{"publishing_house_resources": {'lt': int(amount[1])}})
                else:
                    q0 = Q("range", **{"publishing_house_resources": {'lt': int(amount[1])}}) | q0
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

        q_all = q0 & q_range & q_channel & q_category & q_publisher

        # 默认类别和出版社为全部
        # if data['categorys'][0] != "all":
        #     q_all = q_category & q_all
        # if data['publishers'][0] != "all":
        #     q_all = q_publisher & q_all

        # q_range = Q('range', **{"create_time": {'gte': yesterday, 'lte': yesterday}})
        try:
            results, total = query_es_return_list(dwt_category, q_all)
            categorys = []
            publishers = []
            productTypes = []
            for idata in results:
                categorys.append({"channel": idata.channel,
                                  "product_type": idata.product_type, "publishing_house": idata.publishing_house,
                                  "product_type_resources": idata.product_type_resources,
                                  "publishing_house_resources": idata.publishing_house_resources})
                if idata.publishing_house not in publishers:
                    publishers.append(idata.publishing_house)
                if idata.product_type not in productTypes:
                    productTypes.append(idata.product_type)
            return json({"code": 200, "msg": "查询成功",
                         "data": {"categorys": categorys[:100], "publishers": publishers[:100],
                                  "productTypes": productTypes[:100]}, "total": total})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)


# 获取产品类别明细--出版社信息
@targetBoard_bp.route('/publishers', methods=["GET"])
@protected
async def publishers(requset):
    if requset.method == 'GET':

        try:
            results, total = query_es_return_list(dwt_category, q_range)
            copyrights = []
            for idata in results:
                if idata.publishing_house not in copyrights:
                    copyrights.append(idata.publishing_house)

            return json(
                {"code": 200, "msg": "查询成功", "data": {"publishing_house": copyrights[:100]}, "total": len(copyrights)})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)


# 获取类别明细--类别信息
@targetBoard_bp.route('/categorys-item', methods=["GET"])
@protected
async def publishers(requset):
    if requset.method == 'GET':

        try:
            results, total = query_es_return_list(dwt_category, q_range)
            productTypes = []
            try:
                for idata in results:
                    if idata.product_type not in productTypes:
                        productTypes.append(idata.product_type)
            except Exception:
                productTypes = []

            return json(
                {"code": 200, "msg": "查询成功", "data": {"product_type": productTypes[:100]}, "total": len(productTypes)})
        except Exception as e:
            print(e)
            return json({"code": 500, "msg": "查询销量趋势指标错误"}, ensure_ascii=False)
