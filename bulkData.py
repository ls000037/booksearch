from elasticsearch import helpers
import sys
from datetime import datetime
from sanic.response import json
from dateutil import tz
import time
tz_sh = tz.gettz('Asia/Shanghai')
# 通过file载入json数据
# def load_json_file(file):
#     data = []
#     if os.path.exists(file):
#         for line in open(file, 'r'):
#             data.append(js.loads(line))
#         return data
#     else:
#         return ("File does not exist")

# 按照bathc_size分割数据
# def get_list_by_chunk_size(original_list, batch_size):
#     # looping till length equals batch_size
#
#     for i in range(0, len(original_list), batch_size):
#         yield original_list[i:i + batch_size]
# 根据书品excel数据生成actions
def generate_actions(index, datas,sheet):
    dt = datetime.now(tz=tz_sh)

    #搜索每个书籍的在售书店数量，生成映射字典（备用）
    # responses = ms.execute()
    # book_sale_data={}
    # for response in responses:
    #     # print("Results for query %r." % response.search.query)
    #     if len(response):
    #         if response['hits']['hits'][0]['_source']['isbn']:
    #             book_sale_data[response['hits']['hits'][0]['_source']['isbn']]=len(response)

    for idata in datas:
        # print(idata.keys())
        # #遍历书籍在售字典book_sale_data，匹配书店数量信息 （备用）
        # if str(idata['isbn']) in book_sale_data.keys():
        #     idata['selling_stores']=book_sale_data[str(idata['isbn'])]
        #     print("got it", idata['selling_stores'])
        # #计算在售书店区间和积分
        # if idata['selling_stores']:
        #     sale_num=idata['selling_stores']
        #     if sale_num >5:
        #         idata['selling_stores_range']="5次以上"
        #         idata['selling_stores_integral']=25
        #     elif sale_num <=5 and sale_num >=3:
        #         idata['selling_stores_range']="3-5次"
        #         idata['selling_stores_integral']=20
        #     else:
        #         idata['selling_stores_range']="3次以下"
        #         idata['selling_stores_integral']=15

        # for i in ["store",
        #           "store_comments", "isbn", "book_name", "category",
        #           "slogan", "book_description", "languages" \
        #         , "word_count", "book_comments", "store_pricing", "selling_price",
        #           "publishing_house", "publishing_time", "printing_time",
        #           "edition", "impression" \
        #         , "inventory", "sales", "author", "shuppites", "format",
        #           "is_suit", "suits", "binding_layout", "pages" \
        #         , "papers", "uploader", "selling_stores",
        #           "published_year_range", "published_year_integral",
        #           "comments_range", "comments_integral", \
        #           "premium_range", "premium_integral", "selling_stores_range",
        #           "selling_stores_integral", "create_type", "data_source", "total_integral", 'sales_month',
        #           "shelf_time"]:
        #     try:
        #         idata[i]
        #     except Exception:
        #         idata[i] = None

        # #过滤特殊字符
        # if sheet == "上架产品数据模型":
        #     pass
        if sheet == "正向选品数据":
            try:
                if idata["author"]:
                    idata["author"] = idata["author"].replace('\n', ' ').replace('\r', '')
            except Exception:
                pass

            try:
                if idata["store"]:
                    idata["store"] = idata["store"].replace('\n', ' ').replace('\r', '')
            except Exception:
                pass

            idata["book_name"] = idata["book_name"].replace('\n', ' ').replace('\r', '')

            try:
                if idata["slogan"]:
                    idata["slogan"] = idata["slogan"].replace('\n', ' ').replace('\r', '')
            except Exception:
                pass

            try:
                if idata["book_description"]:
                    idata["book_description"] = idata["book_description"].replace('\n', ' ').replace('\r', '')
            except Exception:
                pass

            try:
                if idata["catalogue"]:
                    idata["catalogue"] = idata["catalogue"].replace('\n', ' ').replace('\r', '')
            except Exception:
                pass

            idata['first_channel'] = '正向选品'

            idata['second_channel'] = '正向选品'
            # 转换出版时间戳为正常格式
            try:

                if isinstance(idata['publishing_time'], int):

                    timestamp = int(str(idata['publishing_time'])[:-3])

                    timeArray = time.localtime(timestamp)
                    idata['publishing_time'] = time.strftime("%Y-%m", timeArray)
            except Exception as e:
                print(e)
            # 转换印刷时间戳为正常格式
            try:

                if isinstance(idata['printing_time'], int):

                    timestamp = int(str(idata['printing_time'])[:-3])

                    timeArray = time.localtime(timestamp)
                    idata['printing_time'] = time.strftime("%Y-%m", timeArray)
            except Exception as e:
                print(e)
            #转换获取出版月时间戳为正常格式
            try:

                if isinstance(idata['copyright_month'], int):
                    timestamp = int(str(idata['copyright_month'])[:-3])

                    timeArray = time.localtime(timestamp)
                    idata['copyright_month'] = time.strftime("%Y-%m", timeArray)
            except Exception as e:
                print(e)
      # 转换上架时间戳为正常格式
        try:

            if isinstance(idata['shelf_time'], int):
                print(idata['shelf_time'])
                timestamp = int(str(idata['shelf_time'])[:-3])

                timeArray = time.localtime(timestamp)
                idata['shelf_time'] = time.strftime("%Y-%m-%d", timeArray)
        except Exception as e:
            print(e)
        # 转换统计月份戳为正常格式
        try:

            if isinstance(idata['statistical_month'], int):
                print(idata['statistical_month'])
                timestamp = int(str(idata['statistical_month'])[:-3])

                timeArray = time.localtime(timestamp)
                idata['statistical_month'] = time.strftime("%Y-%m", timeArray)
        except Exception as e:
            print(e)

            # idata['publishing_time'] = idata['publishing_time'].replace("/", "-")
            # idata['data_source']= None

            # # 计算出版年限区间和积分
            # publish_time = datetime.strptime(idata['publishing_time'], "%Y-%m-%d")
            # pulish_year = (dt - publish_time).days / 365.0
            # # print(pulish_year)
            # if pulish_year > 5.0:
            #     idata['published_year_range'] = '5年以上'
            #     idata['published_year_integral'] = 20
            # elif pulish_year >= 2.0 and pulish_year <= 5.0:
            #     idata['published_year_range'] = '2-5年'
            #     idata['published_year_integral'] = 15
            # else:
            #     idata['published_year_range'] = '2年以下'
            #     idata['published_year_integral'] = 10
            #
            # # 计算评论数区间和积分
            # if idata['book_comments']:
            #     if idata['book_comments'] > 20:
            #         idata['comments_range'] = '20个以上'
            #         idata['comments_integral'] = 25
            #     elif idata['book_comments'] >= 10 & idata['book_comments'] <= 20:
            #         idata['comments_range'] = '10-20个'
            #         idata['comments_integral'] = 20
            #     else:
            #         idata['comments_range'] = '10个以下'
            #         idata['comments_integral'] = 15
            #
            # # 计算溢价区间和积分
            # premium = float('%.2f' % (idata['selling_price'] / idata['store_pricing'] - 1))
            # if premium > 0.5:
            #     idata['premium_range'] = '50%以上'
            #     idata['premium_integral'] = 30
            # elif premium >= 0.3 and premium <= 0.5:
            #     idata['premium_range'] = '30%-50%'
            #     idata['premium_integral'] = 25
            # else:
            #     idata['premium_range'] = '30%以下'
            #     idata['premium_integral'] = 20
            #
            # 数据类型
            idata["create_type"] = "import"

            idata["update_time"] = dt.strftime('%Y-%m-%dT%H:%M:%S%z')

        idata["create_time"] = dt.strftime('%Y-%m-%dT%H:%M:%S%z')
        action = {
            # '_op_type': 'index update create delete',
            # '_type': 'doc',
            "_index": index,
            "_source": {}
        }
            # print(idata)
        action["_source"] = idata
        yield action


async def bulk_insert_data_to_es(elasticsearch_connection, data, index,sheet):
    try:
        # batch_data为迭代器 可以用for来遍历 ，for循环的每一块为一个bulk_size的数据量，比如源数据有280条，bulk_size=100,batch_data就可以迭代3次
        # batch_data = get_list_by_chunk_size(data, bulk_size)
        # # print (batch_data)
        # for batch in batch_data:
        #     def generate_actions(index, batch):
        #         count = 0
        #         # actions = []
        #         while count <= len(batch) - 1:
        #             action = {
        #                 "_index": index,
        #                 "_source": {}
        #             }
        #             action["_source"] = batch[count]
        #             # actions.append(action)
        #             count = count + 1
        #             yield action
        # helpers.bulk(elasticsearch_connection, actions=generate_actions(index, batch))

        # 并发写入
        # helpers.parallel_bulk(elasticsearch_connection, actions=generate_actions(index, data))
        for success, info in helpers.parallel_bulk(elasticsearch_connection, actions=generate_actions(index, data,sheet)):
            if not success:
                print('文档插入失败信息: ', info)
                return json({"code":500,"msg": "文档插入ES失败,请检查原因"}, ensure_ascii=False)
        return json({"code":200,"msg": "解析文件成功"}, ensure_ascii=False)
    except Exception as e:
        e = sys.exc_info()
        print("bulk插入失败信息: ", e)
        return json({"code":500,"msg": "文档actions生成失败，请检查excel列名或内容是否规范"}, ensure_ascii=False)
