from elasticControl import es_con
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import json
import uuid
from redisControl import get_redis_pool
import hashlib
import itertools


# searchdata = {
#     "universe": [{"value": "", "logic": "&", "condition": "term"}],
#     "first_channel": [{"value": "正向选品", "logic": "&", "condition": "term"}],
#     "second_channel": [{"value": "正向选品", "logic": "&", "condition": "term"}],
#     "store": [{"value": "清香溢远", "logic": "&", "condition": "match"}],
#     "store_comments": [{"value": "", "logic": "&", "condition": "term"}],
#     "isbn": [{"value": "", "logic": "&", "condition": "term"}],
#     "book_name": [{"value": "技术", "logic": "!", "condition": "match"}],
#     "category": [{"value": "", "logic": "&", "condition": "term"}],
#     "slogan": [{"value": "", "logic": "&", "condition": "match"}],
#     "book_description": [{"value": "", "logic": "&", "condition": "match"}],
#     "catalogue": [{"value": "", "logic": "&", "condition": "match"}],
#     "languages": [{"value": "", "logic": "&", "condition": "term"}],
#     "word_count": [{"value": "", "logic": "&", "condition": "term"}],
#     "book_comments": [{"value": "", "logic": "&", "condition": "term"}],
#     "store_pricing": [{"value": "", "logic": "&", "condition": "term"}],
#     "selling_price": [{"value": "", "logic": "&", "condition": "term"}],
#     "publishing_house": [{"value": "人民交通出版社", "logic": "&", "condition": "match"}],
#     "publishing_time": [{"value": "2020-06-01", "logic": "&", "condition": "term"}],
#     "printing_time": [{"value": "2020-06-01", "logic": "&", "condition": "term"}],
#     "edition": [{"value": "", "logic": "&", "condition": "term"}],
#     "impression": [{"value": "", "logic": "&", "condition": "term"}],
#     "inventory": [{"value": "", "logic": "&", "condition": "term"}],
#     "sales": [{"value": "", "logic": "&", "condition": "term"}],
#     "author": [{"value": "", "logic": "&", "condition": "match"}],
#     "shuppites": [{"value": "", "logic": "&", "condition": "term"}],
#     "format": [{"value": "", "logic": "&", "condition": "term"}],
#     "is_suit": [{"value": "", "logic": "&", "condition": "term"}],
#     "suits": [{"value": "", "logic": "&", "condition": "term"}],
#     "binding_layout": [{"value": "", "logic": "&", "condition": "term"}],
#     "pages": [{"value": "", "logic": "&", "condition": "term"}],
#     "papers": [{"value": "", "logic": "&", "condition": "term"}],
#     "uploader": [{"value": "", "logic": "&", "condition": "term"}],
#     "selling_stores": [{"value": "", "logic": "&", "condition": "term"}],
#     "published_year_range": [{"value": "", "logic": "&", "condition": "term"}],
#     "published_year_integral": [{"value": "", "logic": "&", "condition": "term"}],
#     "comments_range": [{"value": "", "logic": "&", "condition": "term"}],
#     "comments_integral": [{"value": "", "logic": "&", "condition": "term"}],
#     "premium_range": [{"value": "", "logic": "&", "condition": "term"}],
#     "premium_integral": [{"value": "", "logic": "&", "condition": "term"}],
#     "selling_stores_range": [{"value": "", "logic": "&", "condition": "term"}],
#     "selling_stores_integral": [{"value": "", "logic": "&", "condition": "term"}],
#     "create_type": [{"value": "import or etl", "logic": "&", "condition": "term"}],
#     "data_source": [{"value": "", "logic": "&", "condition": "term"}],
#     "create_time": [{"value": "", "logic": "&", "condition": "term"}],

# }
def sort_es_return_list(index, query):
    search_context = Search(using=es_con, index=index)
    s = search_context.sort(query)
    try:
        response = s.execute()
        total =s.count()
        if response.success():
            # print(response['hits']['hits'])
            return response['hits']['hits'], total
        return [],0
    except Exception:
        return [],0

# esl检索函数
def query_es_return_list(index, query):
    search_context = Search(using=es_con, index=index)
    s = search_context.filter(query)
    # response = s.execute()
    # if response.success():
    #     return s.scan(), response['hits']['total']
    try:
        results = s.scan()
        total = s.count()
    except Exception:
        results = []
        total = 0
    return results, total


# 检索语句生成类
class uni_search:

    def __init__(self, postdatas):
        self.postdatas = postdatas
        self.q1 = []
        self.q2 = []
        self.q3 = []

    async def search(self):
        for elem in self.postdatas:
            # 常规字段检索语句构造
            if elem in ["store", "store_comments", "isbn", "book_name", "category","second_channel",
                        "slogan", "book_description",
                        "catalogue", "languages" \
                    , "word_count", "book_comments", "store_pricing", "selling_price", "publishing_house",
                        "edition", "impression" \
                    , "inventory", "sales", "author", "shuppites", "format", "is_suit", "suits", "binding_layout",
                        "pages" \
                    , "papers", "uploader", "selling_stores", "published_year_range", "published_year_integral",
                        "comments_range", "comments_integral", \
                        "premium_range", "premium_integral", "selling_stores_range", "selling_stores_integral",
                        "create_type", "data_source"]:
                datas = self.postdatas[elem]

                # 遍历前端传来的参数
                for data in datas:
                    if data['value']:
                        # 针对text keyword混合字段进行特殊语句构造term语句所用的key
                        if elem in ["store", "book_name", "publishing_house", "author"]:
                            ielem = elem + "__keyword"
                        else:
                            ielem = elem
                        # 判断数字型字段输入字符串检索引发的错误，遇到即跳过
                        if elem in ["store_comments", "word_count", "book_comments", "store_pricing", "selling_price",
                                    "inventory",
                                    "sales", \
                                    "suits", "pages", "selling_stores", "published_year_integral", "comments_integral", \
                                    "premium_integral", "selling_stores_integral", "total_integral"]:
                            try:
                                try:
                                    int(data['value'])
                                except Exception:
                                    float(data['value'])
                            except Exception:
                                continue
                        # 匹配或语句
                        if data['logic'] == "|":
                            #如果遇到多选列表查询，则构造嵌套的多选值或语句
                            if isinstance(data['value'], list):
                                q0 = []
                                for sdata in data['value']:

                                    if not q0:
                                        q0 = Q("term", **{elem: sdata})
                                    else:
                                        q0 = Q("term", **{elem: sdata}) | q0

                                if not self.q2:
                                    self.q2 = q0
                                else:
                                    self.q2 = q0 | self.q2
                            #如果遇到输入型查询，则构造独立单个语句拼接
                            else:
                                # 匹配模糊查询条件
                                if 'match' in data['condition']:
                                    if not self.q2:
                                        self.q2 = Q("match", **{elem: data['value']})
                                    else:
                                        self.q2 = Q("match", **{elem: data['value']}) | self.q2
                                else:
                                    if not self.q2:
                                        self.q2 = Q("term", **{ielem: data['value']})
                                    else:
                                        self.q2 = Q("term", **{ielem: data['value']}) | self.q2
                        # 匹配与条件
                        elif data['logic'] == "&":
                            # 如果遇到多选列表查询，则构造嵌套的多选值或语句
                            if isinstance(data['value'], list):
                                q0 = []
                                for sdata in data['value']:

                                    if not q0:
                                        q0 = Q("term", **{elem: sdata})
                                    else:
                                        q0 = Q("term", **{elem: sdata}) | q0

                                if not self.q1:
                                    self.q1 = q0
                                else:
                                    self.q1 = q0 & self.q1
                            # 如果遇到输入型查询，则构造独立单个语句拼接
                            else:

                                if 'match' in data['condition']:
                                    if not self.q1:
                                        self.q1 = Q("match", **{elem: data['value']})
                                    else:
                                        self.q1 = Q("match", **{elem: data['value']}) & self.q1
                                else:

                                    if not self.q1:
                                        self.q1 = Q("term", **{ielem: data['value']})
                                    else:
                                        self.q1 = Q("term", **{ielem: data['value']}) & self.q1
                        # 匹配非条件
                        elif data['logic'] == "!":
                            if isinstance(data['value'], list):

                                q0 = []
                                for sdata in data['value']:

                                    if not q0:
                                        q0 = Q("term", **{elem: sdata})
                                    else:
                                        q0 = Q("term", **{elem: sdata}) | q0

                                if not self.q3:
                                    self.q3 = q0
                                else:
                                    self.q3 = q0 | self.q3
                            else:
                                if 'match' in data['condition']:
                                    if not self.q3:
                                        self.q3 = Q("match", **{elem: data['value']})
                                    else:
                                        self.q3 = Q("match", **{elem: data['value']}) | self.q3

                                else:

                                    if not self.q3:
                                        self.q3 = Q("term", **{ielem: data['value']})
                                    else:
                                        self.q3 = Q("term", **{ielem: data['value']}) | self.q3
                        # 其他非法情况跳过
                        else:
                            pass


            # source字段的检索语句构造
            elif elem in ["first_channel"]:
                datas = self.postdatas[elem]

                for data in datas:
                    if data['value']:
                        if not self.q1:
                            self.q1 = Q("term", **{elem: data['value']})
                        else:
                            self.q1 = Q("term", **{elem: data['value']}) & self.q1
            # #多选情况语句构造
            # elif elem in ["second_channel"]:
            #
            #
            #     datas = self.postdatas[elem]
            #
            #     for data in datas:
            #         if data['value']:
            #
            #             if isinstance(data['value'], list):
            #
            #                 if len(data['value']) ==1:
            #                     print("gettt")
            #                     if not self.q1:
            #                         self.q1 = Q("term", **{elem: data['value'][0]})
            #                     else:
            #                         self.q1 = Q("term", **{elem: data['value'][0]}) & self.q1
            #                 else:
            #
            #                     print(data)
            #                     q0=[]
            #                     for sdata in data['value']:
            #                         print(sdata)
            #                         if not q0:
            #                             q0=Q("term", **{elem: sdata})
            #                         else:
            #                             q0= Q("term", **{elem: sdata}) | q0
            #
            #                     if not self.q1:
            #                         self.q1 = q0
            #                     else:
            #                         self.q1 = q0 & self.q1
            # 任意匹配字段的检索语句构造
            elif elem in ["universe"]:
                datas = self.postdatas[elem]
                for data in datas:
                    if data['value']:
                        # 如果输入的值是数字型则匹配全部字段，如果不是数字型则匹配部分字段
                        try:
                            try:
                                int(data['value'])
                            except Exception:
                                float(data['value'])
                            # match模糊查询所用的esl语句
                            match_seq = Q("multi_match", query=data['value'],
                                          fields=["first_channel", "second_channel", "store",
                                                  "store_comments", "isbn", "book_name", "category",
                                                  "slogan", "book_description", "catalogue", "languages" \
                                              , "word_count", "book_comments", "store_pricing", "selling_price",
                                                  "publishing_house", "publishing_time", "printing_time",
                                                  "edition", "impression" \
                                              , "inventory", "sales", "author", "shuppites", "format",
                                                  "is_suit", "suits", "binding_layout", "pages" \
                                              , "papers", "uploader", "selling_stores",
                                                  "published_year_range", "published_year_integral", "shelf_time",
                                                  "sales_month",
                                                  "comments_range", "comments_integral", \
                                                  "premium_range", "premium_integral", "selling_stores_range",
                                                  "selling_stores_integral", "create_type", "data_source",
                                                  "total_integral"])
                            # term精确查询所用的esl语句
                            term_seq = Q("term", first_channel=data['value']) | Q("term",
                                                                                  second_channel=data['value']) | Q(
                                "term", store__keyword=
                                data[
                                    'value']) | Q(
                                "term", store_comments=data['value']) | Q("term", isbn=data['value']) | Q(
                                "term",
                                book_name__keyword=
                                data[
                                    'value']) | Q(
                                "term", category=data['value']) | Q("term", slogan=data['value']) | Q(
                                "term",
                                book_description=
                                data[
                                    'value']) | Q(
                                "term", catalogue=data['value']) | Q("term", languages=data['value']) | Q(
                                "term",
                                word_count=
                                data[
                                    'value']) | Q(
                                "term", book_comments=data['value']) | Q("term", store_pricing=data['value']) | Q(
                                "term",
                                selling_price=data[
                                    'value']) | Q(
                                "term", publishing_house__keyword=data['value']) | Q("term",
                                                                                     publishing_time=data[
                                                                                         'value']) | Q(
                                "term", printing_time=data['value']) | Q("term", edition=data['value']) | Q(
                                "term",
                                impression=
                                data[
                                    'value']) | Q(
                                "term", inventory=data['value']) | Q("term", sales=data['value']) | Q("term",
                                                                                                      author__keyword=
                                                                                                      data[
                                                                                                          'value']) | Q(
                                "term", shuppites=data['value']) | Q("term", format=data['value']) | Q("term",
                                                                                                       is_suit=
                                                                                                       data[
                                                                                                           'value']) | Q(
                                "term", suits=data['value']) | Q("term", binding_layout=data['value']) | Q(
                                "term",
                                pages=data[
                                    'value']) | Q(
                                "term", papers=data['value']) | Q("term", uploader=data['value']) | Q("term",
                                                                                                      selling_stores=
                                                                                                      data[
                                                                                                          'value']) | Q(
                                "term", published_year_range=data['value']) | Q("term",
                                                                                published_year_integral=data[
                                                                                    'value']) | Q("term",
                                                                                                  comments_range=
                                                                                                  data[
                                                                                                      'value']) | Q(
                                "term", comments_integral=data['value']) | Q("term",
                                                                             premium_range=data['value']) | Q(
                                "term",
                                premium_integral=data[
                                    'value']) | Q(
                                "term", selling_stores_range=data['value']) | Q("term",
                                                                                selling_stores_integral=data[
                                                                                    'value']) | Q("term",
                                                                                                  create_type=
                                                                                                  data[
                                                                                                      'value']) | Q(
                                "term",
                                total_integral=
                                data[
                                    'value']) | Q("term",
                                                  shelf_time=
                                                  data[
                                                      'value']) | Q("term",
                                                                    sales_month=
                                                                    data[
                                                                        'value'])
                        # 输入的不是数字型，则剔除数字型字段，以免引发搜索错误
                        except Exception as e:
                            match_seq = Q("multi_match", query=data['value'],
                                          fields=["first_channel", "second_channel", "store",
                                                  "isbn", "book_name", "category",
                                                  "slogan", "book_description", "catalogue", "languages", \
                                                  "edition", "impression", \
                                                  "author", "shuppites", "format",
                                                  "is_suit", "binding_layout", \
                                                  "papers", "uploader",
                                                  "published_year_range",
                                                  "comments_range", \
                                                  "premium_range", "selling_stores_range",
                                                  "create_type", "data_source"])

                            term_seq = Q("term", first_channel=data['value']) | Q("term",
                                                                                  second_channel=data['value']) | Q(
                                "term",
                                store__keyword=
                                data[
                                    'value']) | Q("term", isbn=data['value']) | Q(
                                "term",
                                book_name__keyword=
                                data[
                                    'value']) | Q(
                                "term", category=data['value']) | Q("term", slogan=data['value']) | Q(
                                "term",
                                book_description=
                                data[
                                    'value']) | Q(
                                "term", catalogue=data['value']) | Q("term", languages=data['value']) | Q(
                                "term", publishing_house__keyword=data['value']) | Q("term", edition=data['value']) | Q(
                                "term",
                                impression=
                                data[
                                    'value']) | Q("term",
                                                  author__keyword=
                                                  data[
                                                      'value']) | Q(
                                "term", shuppites=data['value']) | Q("term", format=data['value']) | Q("term",
                                                                                                       is_suit=
                                                                                                       data[
                                                                                                           'value']) | Q(
                                "term", binding_layout=data['value']) | Q(
                                "term", papers=data['value']) | Q("term", uploader=data['value']) | Q(
                                "term", published_year_range=data['value']) | Q("term",
                                                                                comments_range=
                                                                                data[
                                                                                    'value']) | Q("term",
                                                                                                  premium_range=data[
                                                                                                      'value']) | Q(
                                "term", selling_stores_range=data['value']) | Q("term",
                                                                                create_type=
                                                                                data[
                                                                                    'value'])

                        # 匹配或字段
                        if data['logic'] == "|":
                            # 全局模糊搜索
                            if 'match' in data['condition']:
                                if not self.q2:
                                    self.q2 = match_seq
                                else:
                                    self.q2 = match_seq | self.q2
                            # 全局精确搜索
                            elif 'term' in data['condition']:
                                if not self.q2:
                                    self.q2 = term_seq
                                else:
                                    self.q2 = term_seq | self.q2
                        # 匹配与字段
                        elif data['logic'] == "&":
                            # 全局模糊搜索
                            if 'match' in data['condition']:

                                if not self.q1:
                                    self.q1 = match_seq

                                else:
                                    self.q1 = match_seq & self.q1
                            # 全局精确搜索
                            elif 'term' in data['condition']:

                                if not self.q1:
                                    self.q1 = term_seq
                                else:
                                    self.q1 = term_seq & self.q1
                        # 匹配非字段
                        elif data['logic'] == "!":
                            # 全局模糊搜索
                            if 'match' in data['condition']:

                                if not self.q3:
                                    self.q3 = match_seq
                                else:
                                    self.q3 = match_seq | self.q3
                            # 全局精确搜索
                            elif 'term' in data['condition']:

                                if not self.q3:
                                    self.q3 = term_seq
                                else:
                                    self.q3 = term_seq | self.q3
                        else:
                            if not self.q1:
                                self.q1 = match_seq
                            else:
                                self.q1 = match_seq & self.q1


            # 日期范围检索语句构造
            elif elem in ["create_time", "update_time", "publishing_time", "sales_month", "shelf_time",
                          "printing_time"]:
                datas = self.postdatas[elem]

                for data in datas:
                    # #根据前端传入得年份，扩大其搜索范围
                    # if elem in ['publishing_time'] and '-' in data['value'][0]:
                    #     data['value'][0] = data['value'][0] + '-01-01'
                    #     data['value'][1] = data['value'][1] + "-12-31"

                    if data['value'][0] =="":
                        begin_date='0000-01-01T00:00:00'
                    else:
                        begin_date =data['value'][0]
                    if data['value'][1] =="":
                        end_date='9999-12-31T23:59:59'
                    else:
                        end_date=data['value'][1]
                    if not self.q1:
                        self.q1 = Q('range', **{elem: {'gte': begin_date, 'lte': end_date}})
                    else:
                        self.q1 = Q('range', **{elem: {'gte': begin_date, 'lte': end_date}}) & self.q1

            # # 数字型检索语句构造
            elif elem in ["total_integral"]:
                datas = self.postdatas[elem]

                for data in datas:
                    if data['logic'] == "|":
                        if isinstance(data['value'], list):
                            q0 = []
                            for sdata in data['value']:
                                try:
                                    if "-" in sdata:
                                        ielem = elem
                                        year = sdata.split("-")
                                        # 匹配-2这种年限
                                        if year[0] == "":
                                            if not q0:
                                                q0 = Q("range", **{ielem: {'lt': year[1]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'lt': year[1]}}) | q0
                                        # 匹配5-这种年限
                                        elif year[1] == "":

                                            if not q0:
                                                q0 = Q("range", **{ielem: {'gt': year[0]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'gt': year[0]}}) | q0
                                        # 匹配2-5这种年限
                                        elif "" not in year:

                                            if not q0:
                                                q0 = Q("range", **{ielem: {'gte': year[0], "lte": year[1]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'gte': year[0], "lte": year[1]}}) | q0
                                except Exception:
                                    if not q0:
                                        q0 = Q("term", **{elem: sdata})
                                    else:
                                        q0 = Q("term", **{elem: sdata}) | q0
                            if not self.q2:
                                self.q2 = q0
                            else:
                                self.q2 = q0 | self.q2


                    elif data['logic'] == "&":
                        #判断前端传值是否为数组
                        if isinstance(data['value'], list):
                            q0 = []
                            for sdata in data['value']:

                                try:
                                    if "-" in sdata:

                                        ielem = elem
                                        year = sdata.split("-")
                                        # 匹配-2这种年限
                                        if year[0] == "":

                                            if not q0:
                                                q0 = Q("range", **{ielem: {'lt': year[1]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'lt': year[1]}}) | q0
                                        # 匹配5-这种年限
                                        elif year[1] == "":


                                            if not q0:
                                                q0 = Q("range", **{ielem: {'gt': year[0]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'gt': year[0]}}) | q0
                                        # 匹配2-5这种年限
                                        elif "" not in year:

                                            if not q0:
                                                q0 = Q("range", **{ielem: {'gte': year[0], "lte": year[1]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'gte': year[0], "lte": year[1]}}) | q0
                                                # 匹配错误参数情况
                                except Exception:

                                    if not q0:
                                        q0 = Q("term", **{elem: sdata})
                                    else:
                                        q0 = Q("term", **{elem: sdata}) | q0

                            if not self.q1:
                                self.q1 = q0
                            else:
                                self.q1 = q0 & self.q1
                        #不为数组，则匹配具体值

                    elif data['logic'] == "!":
                        if isinstance(data['value'], list):
                            q0 = []
                            for sdata in data['value']:
                                try:
                                    if "-" in sdata:
                                        ielem = elem
                                        year = sdata.split("-")
                                        # 匹配-2这种年限
                                        if year[0] == "":

                                            if not q0:
                                                q0 = Q("range", **{ielem: {'lt': year[1]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'lt': year[1]}}) | q0
                                        # 匹配5-这种年限
                                        elif year[1] == "":
                                            if not q0:
                                                q0 = Q("range", **{ielem: {'gt': year[0]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'gt': year[0]}}) | q0
                                        # 匹配2-5这种年限
                                        elif "" not in year:

                                            if not q0:
                                                q0 = Q("range", **{ielem: {'gte': year[0], "lte": year[1]}})
                                            else:
                                                q0 = Q("range", **{ielem: {'gte': year[0], "lte": year[1]}}) | q0

                                except Exception:
                                    if not q0:
                                        q0 = Q("term", **{elem: sdata})
                                    else:
                                        q0 = Q("term", **{elem: sdata}) | q0

                            if not self.q3:
                                self.q3 = q0
                            else:
                                self.q3 = q0 | self.q3

                    else:
                        pass
        # print("与语句", self.q1)
        # print("或语句", self.q2)
        # print("非语句", self.q3)
        return (self.q1, self.q2, self.q3)


async def get_results(postdatas, index, page, size):
    redis = await get_redis_pool()
    pg_result = []
    uuid_value = uuid.uuid1().hex
    uuid_str_redis = "searchid_" + uuid_value
    tmpdata = json.dumps(postdatas)

    # 如果是全部查询 判断是否存在缓存
    if size == 0:
        # #判断postdatas是否存在于redis的keys中
        search_uuid = hashlib.md5(tmpdata.encode(encoding='UTF-8')).hexdigest()
        cache = await redis.get("search_cache_" + search_uuid)

        # 如果存在则从redis取缓存的值
        if cache:
            pg_result = json.loads(cache)
            total = len(pg_result)
            return (pg_result, total, uuid_value)

    # 正常查询流程
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
    # print(q, index)
    # 将查询语句对应uuid存入redis
    await redis.set(uuid_str_redis, tmpdata)
    await redis.expire(uuid_str_redis, 3600)
    # await redis.close()

    results, total = query_es_return_list(index, q)

    key_data = ["first_channel", "second_channel", "isbn", "book_name", "category", "author",
                "publishing_house", "publishing_time", "sales", "store", "slogan", "book_comments", "store_pricing",
                "selling_price", "shuppites", "sales_month", "shelf_time"]
    # 查询所有结果
    if size == 0:

        for i in results:
            # 遍历判断文档是否覆盖到所有需要的key ,如果字段缺少或者字段值为空，则重新给字段赋值"未知"
            dict_data = {}
            for key in key_data:
                try:
                    dict_data[key] = i[key]
                    if i[key] == None or i[key] == -1 or i[key] == -0.1:
                        dict_data[key] = "未知"
                except Exception:
                    dict_data[key] = "未知"

            try:
                pg_result.append(dict_data)

                    # 全部字段
                    # {"first_channel": i.first_channel, "second_channel": i.second_channel, "store": i.store,
                    #  "store_comments": i.store_comments, "isbn": i.isbn, "book_name": i.book_name,
                    #  "category": i.category,
                    #  "slogan": i.slogan, "book_description": i.book_description,
                    #  "languages": i.languages \
                    #     , "word_count": i.word_count, "book_comments": i.book_comments, "store_pricing": i.store_pricing,
                    #  "selling_price": i.selling_price,
                    #  "publishing_house": i.publishing_house, "publishing_time": i.publishing_time,
                    #  "printing_time": i.printing_time,
                    #  "edition": i.edition, "impression": i.impression \
                    #     , "inventory": i.inventory, "sales": i.sales, "author": i.author,
                    #  "shuppites": i.shuppites, "format": i.format,
                    #  "is_suit": i.is_suit, "suits": i.suits, "binding_layout": i.binding_layout,
                    #  "pages": i.pages \
                    #     , "papers": i.papers, "uploader": i.uploader, "selling_stores": i.selling_stores,
                    #  "published_year_range": i.published_year_range,
                    #  "published_year_integral": i.published_year_integral,
                    #  "comments_range": i.comments_range, "comments_integral": i.comments_integral, \
                    #  "premium_range": i.premium_range, "premium_integral": i.premium_integral,
                    #  "selling_stores_range": i.selling_stores_range,
                    #  "selling_stores_integral": i.selling_stores_integral, "data_source": i.data_source,"total_integral":i.total_integral,
                    #  "sales_month":i.sales_month, "shelf_time":i.shelf_time,
                    #  "create_time": i.create_time, "update_time": i.update_time})

                    # 节约redis缓存空间，只取展示字段
                    # pg_result.append({"first_channel": i.first_channel, "second_channel": i.second_channel, "isbn": i.isbn,
                    #  "book_name": i.book_name, "category": i.category, "author": i.author,
                    #  "publishing_house": i.publishing_house, "publishing_time": i.publishing_time,
                    #  # "sales_month": i.sales_month,"shelf_time": i.shelf_time,
                    #  "sales": i.sales, "store": i.store,
                    #  "slogan": i.slogan, "book_comments": i.book_comments, "store_pricing": i.store_pricing,
                    #  "selling_price": i.selling_price,
                    #  "shuppites": i.shuppites
                    #
                    #  })
            except Exception as e:
                continue

        # 缓存查询结果到redis
        if total != 0:
            md5data = json.dumps(postdatas)
            uuid_trans = hashlib.md5(md5data.encode(encoding='UTF-8')).hexdigest()
            uuid_cache = "search_cache_" + uuid_trans
            await redis.set(uuid_cache, json.dumps(pg_result))
            await redis.expire(uuid_cache, 3600)

        await redis.close()
        return (pg_result, total, uuid_value)

    # 查询指定数量结果
    else:
        # nowpage = 1

        # for i in results:
        # # nowpage = 1
        # # 传统方法每页size条数据分页
        #     if (nowpage >= page * size - (size - 1)) & (nowpage <= page * size):
        #         dict_data = {}
        #         for key in key_data:
        #             try:
        #                 dict_data[key] = i[key]
        #                 if i[key] == None or i[key] == -1 or i[key] == -0.1:
        #                     dict_data[key] = "未知"
        #             except Exception:
        #                 dict_data[key] = "未知"
        #         try:
        #             pg_result.append(dict_data)
        #
        #         except Exception as e:
        #             print("查询结果数据append出错", e)
        #             continue
        #     nowpage += 1


        #利用迭代器切片分页
        for i in itertools.islice(results, page * size - size, page * size):

            #遍历判断文档是否覆盖到所有需要的key ,如果字段缺少或者字段值为空，则重新给字段赋值"未知"
            dict_data={}
            for key in key_data:
                try:
                    dict_data[key]=i[key]
                    if i[key] == None or i[key] == -1 or i[key] == -0.1:
                        dict_data[key] = "未知"
                except Exception:
                    dict_data[key]="未知"
            try:
                pg_result.append(dict_data)

            except Exception as e:
                print("查询结果数据append出错", e)
                continue

            # nowpage += 1
                # pg_result.append(
                #     # 仅展示字段
                #     {"first_channel": i.first_channel, "second_channel": i.second_channel, "isbn": i.isbn,
                #      "book_name": i.book_name, "category": i.category, "author": i.author,
                #      "publishing_house": i.publishing_house, "publishing_time": i.publishing_time,
                #      # "sales_month": i.sales_month, "shelf_time": i.shelf_time,
                #      "sales": i.sales, "store": i.store,
                #      "slogan": i.slogan, "book_comments": i.book_comments, "store_pricing": i.store_pricing,
                #      "selling_price": i.selling_price,
                #      "shuppites": i.shuppites
                #
                #      })
                # 备用全部字段
                # {"first_channel": i.first_channel, "second_channel": i.second_channel, "store": i.store,
                #  "store_comments": i.store_comments, "isbn": i.isbn, "book_name": i.book_name,
                #  "category": i.category,
                #  "slogan": i.slogan, "book_description": i.book_description,
                #  "languages": i.languages \
                #     , "word_count": i.word_count, "book_comments": i.book_comments,
                #  "store_pricing": i.store_pricing,
                #  "selling_price": i.selling_price,
                #  "publishing_house": i.publishing_house, "publishing_time": i.publishing_time,
                #  "printing_time": i.printing_time,
                #  "edition": i.edition, "impression": i.impression \
                #     , "inventory": i.inventory, "sales": i.sales, "author": i.author,
                #  "shuppites": i.shuppites, "format": i.format,
                #  "is_suit": i.is_suit, "suits": i.suits, "binding_layout": i.binding_layout,
                #  "pages": i.pages \
                #     , "papers": i.papers, "uploader": i.uploader, "selling_stores": i.selling_stores,
                #  "published_year_range": i.published_year_range,
                #  "published_year_integral": i.published_year_integral,
                #  "comments_range": i.comments_range, "comments_integral": i.comments_integral, \
                #  "premium_range": i.premium_range, "premium_integral": i.premium_integral,
                #  "selling_stores_range": i.selling_stores_range,
                #  "selling_stores_integral": i.selling_stores_integral, "data_source": i.data_source,
                #  "total_integral": i.total_integral,
                #  "sales_month": i.sales_month, "shelf_time": i.shelf_time,
                #  "create_time": i.create_time, "update_time": i.update_time})


        await redis.close()
        return (pg_result, total, uuid_value)
