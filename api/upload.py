import time
from sanic.response import html, json
from sanic import Blueprint
import pandas as pd
import json as js
from bulkData import bulk_insert_data_to_es
from searchEngine import es_con
from auth import protected
from bookIndexs import search_index, on_index, sale_index
import datetime

# 计算在售书店数量的引入头文件
# from elasticsearch_dsl import MultiSearch, Search

upload_bp = Blueprint('upload_bp')

# 定义索引名称
# index = "book_base_info_test"
index = search_index


# 计算在售书店数量的搜索句柄（备用） 结合下面verifyer函数中的ISBN分支使用
# ms = MultiSearch(index='book_store',using=es_con)


# 存储文件
# async def savefile(filebody, upload_path):
#     with open(upload_path, 'wb') as fw:
#         fw.write(filebody)


# 解析上传的excel文件
async def readfile(body):
    # 判断sheet1
    try:
        try:
            pd_data = pd.read_excel(body, engine="openpyxl", sheet_name="正向选品数据模板")
        except Exception:
            pd_data = pd.read_excel(body, engine="openpyxl", sheet_name="正向选品数据")
    except Exception as e:
        print(e)
        return json({"code": 500, "msg": "解析失败，读取 sheet--正向选品数据 错误，检查sheet是否存在"}, ensure_ascii=False)

    # 判断sheet2
    try:
        try:
            on_pd_data = pd.read_excel(body, engine="openpyxl", sheet_name="上架产品数据模板")
        except Exception:
            on_pd_data = pd.read_excel(body, engine="openpyxl", sheet_name="上架产品数据")

    except Exception as e:
        print(e)
        return json({"code": 500, "msg": "解析失败，读取 sheet--上架产品数据 错误，检查sheet是否存在"}, ensure_ascii=False)

    # 判断sheet3
    try:
        try:
            sale_pd_data = pd.read_excel(body, engine="openpyxl", sheet_name="产品动销情况")
        except Exception:
            sale_pd_data = pd.read_excel(body, engine="openpyxl", sheet_name="产品动销情况模板")

    except Exception as e:
        print(e)
        return json({"code": 500, "msg": "解析失败，读取 sheet--产品动销情况 错误，检查sheet是否存在"}, ensure_ascii=False)

    try:

        on_pd_data.rename(
            columns={"渠道": "channel", "书号": "isbn", "书名": "book_name", "作者": "author", "出版社": "publishg_house",
                     "分类": "category", "版权业务员": "copyright_person", "上架时间": "shelf_time"}, inplace=True)
        sale_pd_data.rename(
            columns={"统计月份": "statistical_month", "分类": "category", "动销量": "sales"}, inplace=True)
        pd_data.rename(
            columns={"一级渠道": "first_channel", "二级渠道": "second_channel", "店铺": "store", "店铺评论数": "store_comments", \
                     "ISBN": "isbn", "图书名称（必填）": "book_name", "图书名称": "book_name", "分类": "category", "广告语": "slogan",
                     "图书简介": "book_description", "出版社（必填，不允许简写）": "publishing_house",
                     "出版时间（必填，格式:年-月-日）": "publishing_time", \
                     "印刷时间（必填，格式:年-月-日）": "printing_time", "版本（必填）": "edition", "印次（必填）": "impression",
                     "开本（必填，请下拉选择填写修改）": "format", "是否套装（必填，请下拉选择填写修改）": "is_suit", "图书目录": "catalogue",
                     "语种": "languages", "字数": "word_count", "书评论数": "book_comments", "定价（必填，最多俩位小数）": "store_pricing",
                     "定价": "store_pricing",
                     "售价（必填，最多俩位小数）": "selling_price", "出版社": "publishing_house",
                     "售价": "selling_price", \
                     "出版时间": "publishing_time", "印刷时间": "printing_time",
                     "版本": "edition",
                     "印次": "impression", "库存": "inventory", "销量": "sales", \
                     "作者": "author", "书品": "shuppites", "开本": "format", "是否套装": "is_suit",
                     "套装数量": "suits", "装帧": "binding_layout", \
                     "页数": "pages", "纸张": "papers", "上传人员": "uploader",
                     "获得版权月": "copyright_month", "多少店铺在售": "selling_stores",
                     "出版年限区间": "published_year_range", \
                     "出版年限积分": "published_year_integral", "评论数区间": "comments_range", "评论数积分": "comments_integral",
                     "溢价区间": "premium_range", \
                     "溢价积分": "premium_integral", "在售商家数区间": "selling_stores_range",
                     "在售商家数积分": "selling_stores_integral"}, inplace=True)

        # 去除图书目录字段
        try:
            pd_data.drop(['catalogue'], axis=1, inplace=True)
            # df.drop(['catalogue'], axis=1, inplace=True)产品动销情况
        except Exception:
            pass
        pd_data.dropna(how='all')
        on_pd_data.dropna(how='all')
        sale_pd_data.dropna(how='all')
        data = pd_data.to_json(orient='records', force_ascii=False)
        on_data = on_pd_data.to_json(orient='records', force_ascii=False)
        sale_data = sale_pd_data.to_json(orient='records', force_ascii=False)
        await bulk_insert_data_to_es(es_con, js.loads(data), index, "正向选品数据")
        await bulk_insert_data_to_es(es_con, js.loads(on_data), on_index, "上架产品数据")
        await bulk_insert_data_to_es(es_con, js.loads(sale_data), sale_index, "产品动销情况")
        return json({"code": 200, "msg": "解析成功"}, ensure_ascii=False)
    except Exception as e:
        print(e)
        return json({"code": 500, "msg": "解析失败，请检查内容格式是否规范"}, ensure_ascii=False)


# 子蓝图中间件
# 另外请求中间件：子蓝图中间件在组蓝图中间件之前执行
# 响应中间件：子蓝图中间件在组蓝图中间件之后执行
# @upload_bp.on_response  和下面的语句功能相同
# @upload_bp.middleware('response')
# async def run_before_handler(request,response):
#     time.sleep(5)

# 判断格式
async def open_verifyer(edata):
    # global ms
    count = 2
    for i in edata:

        for key in i:
            if "统计月份" in key or "上架时间" in key:
                if i[key]:
                    try:
                        time.strptime(i[key], "%Y-%m")
                        # time_new=time.strftime("%Y年%m月%d日", time_old)
                        # print(time_new)
                    except Exception as e:
                        return json(
                            {"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'格式需符合(eg. 2020-01)，请纠正后再上传!"},
                            ensure_ascii=False)
        count+=1

    return False

# 判断格式
async def verifyer(edata):
    # global ms
    count = 2
    for i in edata:

        for key in i:

            # 定价，售价 浮点数类型判断（必填项）
            if '定价' in key or "售价" in key:
                if i[key]:
                    if not isinstance(i[key], float):
                        if not isinstance(i[key], int):
                            return json(
                                {"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段不符合格式，请纠正后再上传!"},
                                ensure_ascii=False)
                    else:
                        # 判断小数点是否保留2位, 这里是让用户控制精度，还是自动换算精度？
                        float_num = len(str(i[key]).split('.')[1])
                        if float_num > 2:
                            return json(
                                {"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段小数点后最多两位，请纠正后再上传!"},
                                ensure_ascii=False)
                else:
                    return json(
                        {"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段为必填，当前为空值，请纠正后再上传!"},
                        ensure_ascii=False)

            # ISBN单独验证（加入mutisearch）
            elif "ISBN" in key:
                if i[key]:
                    # 判断是否为数字格式
                    try:
                        str(i[key])
                    except Exception:

                        # if not isinstance(str(i[key]), str):
                        return json({"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段不符合格式，请纠正后再上传!"},
                                    ensure_ascii=False)
                    # 利用每个isbn搜索书籍对应的书店数量信息，备用
                    # ms=ms.add(Search().filter('match_phrase', isbn=i["ISBN"]))
                # else:
                #     return json(
                #         {"code": 500, "msg": "第" + str(count) + "行（包括表头）数据，'" + key + "'字段为空值，请纠正后再上传!"},
                #         ensure_ascii=False)

            # # 整数类型判断（非必填项）
            # elif "版本" in key or "印次" in key:
            #     if i[key]:
            #         # 判断是否为数字格式
            #         if not isinstance(i[key], int):
            #             return json({"code": 500, "msg": "第" + str(count) + "行（包括表头）数据，'" + key + "'字段应为数字，请纠正后再上传!"},
            #                         ensure_ascii=False)
            # else:
            #     return json(
            #         {"code": 500, "msg": "第" + str(count) + "行（包括表头）数据，'" + key + "'字段为空值，请纠正后再上传!"},
            #         ensure_ascii=False)

            # 整数型判断（非必填项）
            elif "套装数量" in key or "页数" in key or "多少店铺在售" in key or "出版年限积分" in key or "评论数积分" in key or "溢价积分" in key \
                    or "在售店家积分" in key or "在售商家数积分" in key or "店铺评论数" in key or "销量" in key or "字数" in key or "书评论数" in key or "库存" in key:
                if i[key]:
                    # 判断是否为数字格式
                    if not isinstance(i[key], int):
                        return json({"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段填写数字类型，请纠正后再上传!"},
                                    ensure_ascii=False)

            # 字符串型判断（必填项）
            elif "图书名称" in key or "上传人员" in key:
                if i[key]:
                    # 判断是否为字符串格式
                    if not isinstance(i[key], str):
                        return json({"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段不符合格式，请纠正后再上传!"},
                                    ensure_ascii=False)
                else:
                    return json(
                        {"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段为必填，当前为空值，请纠正后再上传!"},
                        ensure_ascii=False)

            # 字符串型判断（非必填项）
            elif "开本" in key or "是否套装" in key or "出版社" in key or "一级渠道" in key or "二级渠道" in key or "店铺" in key or "分类" in key or "广告语" in key or "图书简介" in key \
                    or "图书目录" in key or "语种" in key or "作者" in key or "书品" in key or "装帧" in key or "纸张" in key \
                    or "出版年限区间" in key or "评论数区间" in key or "溢价区间" in key or "在售商家数区间" in key:
                if i[key]:
                    # 判断是否为字符串格式
                    # try:
                    #     str(i[key])
                    # except Exception:
                    if not isinstance(i[key], str):
                        return json(
                            {"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段不符合格式，请纠正后再上传!"},
                            ensure_ascii=False)

            # 出版时间印刷时间是否正确（非必填）
            elif "出版时间" in key or "印刷时间" in key:
                if i[key]:
                    # 判断日期格式是否正确
                    try:
                        try:
                            #     # timeArray = time.localtime(int(str(i[key])[:-3]))1
                            #
                            #     # print(time.strftime("%Y-%m-%d", timeArray))

                            time.strptime(str(int(i[key])), "%Y")

                        except Exception:
                            time.strptime(str(i[key]), "%Y-%m-%d")
                        #     time.strptime(i[key], "%Y/%m/%d")1

                        # time_new=time.strftime("%Y年%m月%d日", time_old)
                        # print(time_new)
                    except Exception as e:
                        return json(
                            {"code": 500,
                             "msg": "第" + str(count) + "行数据，'" + key + "'格式为单独年份 ，请纠正后再上传!"},
                            ensure_ascii=False)

                    # 此处判断可能多余 暂留
                    # if not isinstance(i[key], str):
                    #     # print(i[key])
                    #     return json({"code": 500, "msg": "第" + str(count) + "行（包括表头）数据，'" + key + "'字段不符合格式，请纠正后再上传!"},
                    #                 ensure_ascii=False)
                # else:
                #     return json(
                #         {"code": 500, "msg": "第" + str(count) + "行（包括表头）数据，'" + key + "'字段为空值，请纠正后再上传!"},
                #         ensure_ascii=False)

            # 获取版权月格式是否正确（必填）
            elif "获取版权月" in key:
                if i[key]:
                    try:
                        time.strptime(i[key], "%Y-%m")
                        # time_new=time.strftime("%Y年%m月%d日", time_old)
                        # print(time_new)
                    except Exception as e:
                        return json(
                            {"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'格式需符合(eg. 2020-01)，请纠正后再上传!"},
                            ensure_ascii=False)
                else:
                    return json(
                        {"code": 500, "msg": "第" + str(count) + "行数据，'" + key + "'字段为必填，当前为空值，请纠正后再上传!"},
                        ensure_ascii=False)

        count += 1

    return False


# 上传文件接口
@upload_bp.route('/upload', methods=['POST', "GET", 'OPTIONS'])
@protected
async def upload(requset):
    if requset.method == 'POST':
        f = requset.files.get("file")
        filename = f.name

        # 验证上传文件格式
        if filename.split('.')[1] not in ['xls', 'xlsx']:
            return json({"code": 500, "msg": "文件格式错误"}, ensure_ascii=False)

        # 构造储存路径和文件名
        # basepath = os.path.dirname(__file__)
        # nowtime = time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(time.time()))
        # filename = filename.split('.')[0] + "_" + nowtime + "." + filename.split('.')[1]
        # upload_path = os.path.join(basepath, '../static', filename)

        # 读取文件流
        try:
            ex_data = pd.read_excel(f.body, engine="openpyxl")

            # 判断是否确实必填列
            # cloumn=['图书名称',"图书名称（必填）",'定价','售价','定价（必填，最多俩位小数）',"售价（必填，最多俩位小数）","上传人员","获取版权月"]
            if ('图书名称' not in ex_data.columns) and ("图书名称（必填）" not in ex_data.columns):
                return json({"code": 500, "msg": "sheet--正向选品数据必填列为（图书名称，定价，售价，上传人员，获取版权月），请检查是否缺少"},
                            ensure_ascii=False)
            if ('定价' not in ex_data.columns) and ('定价（必填，最多俩位小数）' not in ex_data.columns):
                return json({"code": 500, "msg": "sheet--正向选品数据必填列为（图书名称，定价，售价，上传人员，获取版权月），请检查是否缺少"},
                            ensure_ascii=False)
            if ('售价' not in ex_data.columns) and ("售价（必填，最多俩位小数）" not in ex_data.columns):
                return json({"code": 500, "msg": "sheet--正向选品数据必填列为（图书名称，定价，售价，上传人员，获取版权月），请检查是否缺少"},
                            ensure_ascii=False)
            if ('上传人员' not in ex_data.columns) or ('获取版权月' not in ex_data.columns):
                return json({"code": 500, "msg": "sheet--正向选品数据必填列为（图书名称，定价，售价，上传人员，获取版权月），请检查是否缺少"},
                            ensure_ascii=False)

            edata = ex_data.to_json(orient='records', force_ascii=False)

        except Exception as e:
            return json({"code": 500, "msg": "读取文件失败，请检查原因"}, ensure_ascii=False)

        try:
            o_edata = pd.read_excel(f.body, engine="openpyxl", sheet_name="上架产品数据").to_json(orient='records', force_ascii=False)
        except Exception as e:
            return json({"code": 500, "msg": "读取 sheet--上架产品数据 失败，请检查格式是否正确"}, ensure_ascii=False)


        try:
            s_edata = pd.read_excel(f.body, engine="openpyxl", sheet_name="上架产品数据").to_json(orient='records', force_ascii=False)
        except Exception as e:
            return json({"code": 500, "msg": "读取 sheet--上架产品数据 失败，请检查格式是否正确"}, ensure_ascii=False)
        # print(js.loads(edata)) ,sheet_name='A

        # 验证格式是否正确，不正确返回重新上传
        verify = await verifyer(js.loads(edata))
        if verify:
            return verify

        on_verify = await open_verifyer(js.loads(o_edata))
        if on_verify:
            return on_verify

        sale_verify = await open_verifyer(js.loads(s_edata))
        if sale_verify:
            return sale_verify

        return await readfile(f.body)

        # 获取当前request的loop,添加后台非阻塞任务
        # myLoop = requset.app.loop
        # try:
        # myLoop.create_task(savefile(f.body, upload_path))
        # await myLoop.create_task(readfile(f.body))
        # return json({"msg": "上传成功"}, ensure_ascii=False)
        # except Exception as e:
        #     return json({"error": "解析文件出错，请检查列名和内容格式是否正确"}, ensure_ascii=False)

        # return redirect(requset.app.url_for('upload_bp.upload'))
    # html_path = os.path.join('../templates', filename)
    return html('''
                <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Title</title>
            </head>
            <body>
            <h1>文件上传</h1>
            <form action="" enctype="multipart/form-data" method="POST">
                <input type="file" name="file">
                <input type="submit" value="上传">
            </form>
            </body>
            </html>
            ''')
