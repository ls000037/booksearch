from sanic import Blueprint
import json as js
from redisControl import get_redis_pool
from sanic.response import json
from searchEngine import uni_search, query_es_return_list
from bookIndexs import search_index, open_search_index
from elasticsearch_dsl import Q
import os, time
import zipfile
from sanic.response import file_stream
import itertools
import pandas as pd
from auth import protected
download_bp = Blueprint('download_bp')


# 生成文件句柄，全路径，文件名
# def get_file():
#
#     file_name = str(int(time.time())) + '.csv'
#     # download_path = os.path.join(basepath, '../bookfiles/download', file_name)
#     download_path =os.path.join('/data/tmp',file_name)
#     # flow = open(download_path, 'a', newline='', encoding='utf-8-sig')
#     return download_path, file_name

def handle_doc(idata, index):
    if index == "ods_openbooks":
        key_data = ["isbn", "book_name", "selling_price", "discount_rate", "author", "category",
                    "publishing_house", "month_sales", "year_sales", "total_sales", "book_list", "channel_type",
                    "sale_type",
                    "sale_time"]
    else:
        key_data = ["store",
                    "store_comments", "isbn", "book_name", "category",
                    "slogan", "book_description", "languages" \
            , "word_count", "book_comments", "store_pricing", "selling_price",
                    "publishing_house", "publishing_time", "printing_time",
                    "edition", "impression" \
            , "inventory", "sales", "author", "shuppites", "format",
                    "is_suit", "suits", "binding_layout", "pages" \
            , "papers", "uploader", "selling_stores",
                    "published_year_range", "published_year_integral",
                    "comments_range", "comments_integral", \
                    "premium_range", "premium_integral", "selling_stores_range",
                    "selling_stores_integral", "create_type", "data_source", "total_integral", 'sales_month',
                    "shelf_time"]
    for key in key_data:
        try:
            idata[key]
        except Exception:
            idata[key] = None

    return idata


async def download_results(uuid, postdatas, index):
    mysearch = uni_search(postdatas)
    q1, q2, q3 = await mysearch.search()
    mylist = []
    for i in [q1, q2]:
        if i == []:
            continue
        mylist.append(i)
    blocklist = []
    if q3 != []:
        blocklist = [q3]

    q = Q('bool',
          must=[],
          should=mylist,
          must_not=blocklist
          )


    results, total = query_es_return_list(index, q)
    results = itertools.islice(results, 0, 50000)
    print(total)
    if total > 300000:
        return ([], total)
    # 如果查询结果为空，返回False
    if total == 0:
        return ([], total)

    # download_path, file_name = get_file()
    file_name = str(int(time.time())) + '.csv'
    # download_path = os.path.join(basepath, '../bookfiles/download', file_name)
    download_path = os.path.join('/data/tmp', file_name)
    # download_paths = []
    # download_paths.append([download_path, file_name])
    # csv_writer = csv.writer(flow)

    redis = await get_redis_pool()
    await redis.set("status_" + str(uuid), "generating")
    await redis.expire("status_" + str(uuid), 3600)

    df = pd.DataFrame((handle_doc(d.to_dict(), index) for d in results))

    #生成开卷数据
    if index =="ods_openbooks":
        try:
            df.drop(['acquisition_time'], axis=1, inplace=True)
        except Exception:
            pass
        try:
            df.drop(['acquisition_timestamp'], axis=1, inplace=True)
        except Exception:
            pass
        df.rename(
            columns={"isbn": "ISBN", "book_name": "书名", "selling_price": "售价", "discount_rate": "折扣", "author": "作者",
                     "category": "分类", "publishing_house": "出版社", "month_sales": "月销量", "year_sales": "年销量",
                     "total_sales": "累积销量", "book_list": "图书榜单", "channel_type": "渠道类型", "sale_type": "销售类型",
                     "sale_time": "销售时间"}, inplace=True)

    #生成全文检索数据
    else:
        try:
            df.drop(['data_source'], axis=1, inplace=True)

        except Exception:
            pass
        # 去除图书目录字段acquisition_timestamp
        try:
            df.drop(['catalogue'], axis=1, inplace=True)
        except Exception:
            pass
        df.rename(columns={"first_channel": "一级渠道", "second_channel": "二级渠道", "store": "店铺", "store_comments": "店铺评论数",
                           "isbn": "ISBN", "book_name": "图书名称", "category": "分类", "slogan": "广告语",
                           "book_description": "图书简介", "languages": "语种", "word_count": "字数", "book_comments": "书评论数",
                           "store_pricing": "定价", "selling_price": "售价", "publishing_house": "出版社",
                           "create_type": "创建类型",
                           "publishing_time": "出版时间", "printing_time": "印刷时间", "sales_month": "销售月份",
                           "shelf_time": "上架时间",
                           "edition": "版本", "impression": "印次", "inventory": "库存", "sales": "销量", "author": "作者",
                           "shuppites": "书品", "format": "开本", "is_suit": "是否套装", "suits": "套装数量",
                           "binding_layout": "装帧",
                           "pages": "页数", "papers": "纸张", "uploader": "上传人员", "selling_stores": "多少店铺在售",
                           "published_year_range": "出版年限区间", "published_year_integral": "出版年限积分",
                           "comments_range": "评论数区间",
                           "comments_integral": "评论数积分", "premium_range": "溢价区间", "premium_integral": "溢价积分",
                           "selling_stores_range": "在售商家数区间", "selling_stores_integral": "在售商家数积分",
                           "total_integral": "总积分",
                           "create_time": "创建时间", "update_time": "更新时间"}, inplace=True)
    # print(df)
    # i = 1
    #写入文件
    try:
        df.to_csv(download_path, index=False, encoding='utf-8-sig')

        # for res in results:
        #     # 分割文件
        #     # if (i % 50001) == 0:
        #     #     # csv_writer.writerow([res.id, res.isbn, res.name_of_shop, res.selling_selling_price, res.conditions, res.slogan])
        #     #     flow.close()
        #     #     flow, download_path, file_name = get_file()
        #     #     download_paths.append([download_path, file_name])
        #     #     csv_writer = csv.writer(flow)
        #     csv_writer.writerow(
        #         [res.first_channel, res.second_channel, res.store,
        #          res.store_comments, res.isbn, res.book_name,
        #          res.category,
        #          res.slogan, res.book_description, res.catalogue,
        #          res.languages \
        #             , res.word_count, res.book_comments, res.store_pricing,
        #          res.selling_price,
        #          res.publishing_house, res.publishing_time,
        #          res.printing_time,
        #          res.edition, res.impression \
        #             , res.inventory, res.sales, res.author,
        #          res.shuppites, res.format,
        #          res.is_suit, res.suits, res.binding_layout,
        #          res.pages \
        #             , res.papers, res.uploader, res.selling_stores,
        #          res.published_year_range,
        #          res.published_year_integral,
        #          res.comments_range, res.comments_integral, \
        #          res.premium_range, res.premium_integral,
        #          res.selling_stores_range,
        #          res.selling_stores_integral, res.data_source,
        #          res.create_time, res.update_time]
        #     )
        #     if i == 300000:
        #         flow.close()
        #         break
        #     i += 1

    except Exception:
        await redis.set("status_" + str(uuid), "generated")
        await redis.expire("status_" + str(uuid), 3600)
        await redis.close()
        return ([], total)

    # 如果条数过多生成压缩文件
    if total > 50000:

        zip_download_path = os.path.join('/data/tmp', str(int(time.time())) + '.zip')
        try:
            with zipfile.ZipFile(zip_download_path, 'w') as myzip:
                myzip.write(download_path, file_name, compress_type=zipfile.ZIP_DEFLATED)
        except Exception:
            pass
        await redis.set("status_" + str(uuid), "generated")
        await redis.expire("status_" + str(uuid), 3600)
        await redis.close()
        return (zip_download_path, total)

    await redis.set("status_" + str(uuid), "generated")
    await redis.expire("status_" + str(uuid), 3600)
    await redis.close()
    return (download_path, total)


# 导出全文检索查询文件接口
@download_bp.route('/download/<uuid>', methods=["GET"])
async def download(requset, uuid):
    if requset.method == 'GET':
        redis = await get_redis_pool()
        status = await redis.get("status_" + str(uuid))
        if status == "generating":
            await redis.close()
            return json({"code": 500, "msg": "正在导出请不要重复导出"}, ensure_ascii=False)
        data = await redis.get("searchid_" + uuid)
        await redis.close()

        # 获取导出数据所需的查询语句
        if data:
            searchdata = js.loads(data)
            # results, total = await download_results(searchdata, index)
            # return json(searchdata)
        else:
            return json({"code": 500, "msg": "导出错误或者已超时"}, ensure_ascii=False)

        # results, total = await download_results("weffew", index)

        try:
            file_path, total = await download_results(uuid, searchdata, search_index)
            # 超过30万判断
            if total > 300000:
                return json({"code": 500, "msg": "下载量超过30万，如需下载请联系工作人员！"}, ensure_ascii=False)
            # file_path="/data/published/app/py/booksearch/api/../static/download/1630546685.zip"
            # 如果查询结果为空 返回空
            if not file_path:
                return json({"code": 500, "msg": "结果为空"}, ensure_ascii=False)
            # 如果不为空，构造下载流
            if total > 50000:
                file_name = str(int(time.time())) + '.zip'
            else:
                file_name = str(int(time.time())) + '.csv'
            return await file_stream(
                file_path,
                # chunk_size=1024,
                mime_type="application/metalink4+xml",
                headers={
                    "Content-Disposition": 'Attachment; filename=' + file_name,
                    "Content-Type": "application/metalink4+xml",
                },
            )
        except Exception as e:
            print(e, "导出错误")
            return json({"code": 500, "msg": "导出错误或者已超时"}, ensure_ascii=False)
    return json({"code": 500, "msg": "请求方法不允许"}, ensure_ascii=False)


# 导出开卷查询文件接口
@download_bp.route('/obdownload/<uuid>', methods=["GET"])
async def obdownload(requset, uuid):
    if requset.method == 'GET':
        redis = await get_redis_pool()
        status = await redis.get("status_" + str(uuid))
        if status == "generating":
            await redis.close()
            return json({"code": 500, "msg": "正在导出请不要重复导出"}, ensure_ascii=False)
        data = await redis.get("searchid_" + uuid)
        await redis.close()

        # 获取导出数据所需的查询语句
        if data:
            searchdata = js.loads(data)
            # results, total = await download_results(searchdata, index)
            # return json(searchdata)
        else:
            return json({"code": 500, "msg": "导出错误或者已超时"}, ensure_ascii=False)

        # results, total = await download_results("weffew", index)

        try:
            file_path, total = await download_results(uuid, searchdata, open_search_index)
            # 超过30万判断
            if total > 300000:
                return json({"code": 500, "msg": "下载量超过30万，如需下载请联系工作人员！"}, ensure_ascii=False)
            # file_path="/data/published/app/py/booksearch/api/../static/download/1630546685.zip"
            # 如果查询结果为空 返回空
            if not file_path:
                return json({"code": 500, "msg": "结果为空"}, ensure_ascii=False)
            # 如果不为空，构造下载流
            if total > 50000:
                file_name = str(int(time.time())) + '.zip'
            else:
                file_name = str(int(time.time())) + '.csv'
            return await file_stream(
                file_path,
                # chunk_size=1024,
                mime_type="application/metalink4+xml",
                headers={
                    "Content-Disposition": 'Attachment; filename=' + file_name,
                    "Content-Type": "application/metalink4+xml",
                },
            )
        except Exception as e:
            print(e, "导出错误")
            return json({"code": 500, "msg": "导出错误或者已超时"}, ensure_ascii=False)
    return json({"code": 500, "msg": "请求方法不允许"}, ensure_ascii=False)


# #验证下载状态是否超时
@download_bp.route('/verify/<uuid:str>', methods=['GET'])
@protected
async def uuid_verify(requset, uuid: str):
    if requset.method == 'GET':
        redis = await get_redis_pool()
        data = await redis.get("searchid_" + str(uuid))
        await redis.close()
        if not data:
            return json({"code": 500, "msg": "下载状态超时，请重新检索"})
        return json({"code": 200, "msg": "下载状态正常"})
    return json({"code": 500, "msg": "请求方法不允许"}, ensure_ascii=False)

