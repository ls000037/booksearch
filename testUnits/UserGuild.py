from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Search
import bulkData
import pandas as pd
from elasticsearch_dsl import Q

es_con = Elasticsearch('http://121.5.199.139:9200', http_auth=('elastic', 'ls000037'))
# es_con.indices.delete(index="*")

# es_con.indices.create(index='demo-nessus', ignore=400)
# nessus_data = myes.load_json_file('nessus.json')
# myes.bulk_insert_data_to_es(es_con, nessus_data, "test-index")

# es_con.indices.create(index="demo-suricata", ignore=400)
# suricata_data = myes.load_json_file('suricata.json')
# myes.bulk_insert_data_to_es(es_con, suricata_data, "demo-suricata")

# search_context = Search(using=es_con, index='demo-*', doc_type='doc')

# s = search_context.query('query_string', query='src_ip:172.16.0.2 AND dest_port:80')
# response = s.execute()
# if response.success():
#     df = pd.DataFrame((d.to_dict() for d in s.scan()))
# print(df)
mapping2 = {
    "first_channel": {"type": "keyword"},
    "second_channel": {"type": "keyword"},
    "store": {"type": "text",
              "analyzer": "ik_max_word",
              "search_analyzer": "ik_max_word",
              "fields": {"keyword":{"type": "keyword",
                                    "ignore_above": 256}}},
    "store_comments": {"type": "long"},
    "isbn": {"type": "keyword"},
    "book_name":{"type": "text",
              "analyzer": "ik_max_word",
              "search_analyzer": "ik_max_word",
              "fields": {"keyword":{"type": "keyword",
                                    "ignore_above": 256}}},
    "category": {"type": "keyword"},
    "slogan": {"type": "text",
               "analyzer": "ik_max_word",
              "search_analyzer": "ik_max_word",
              "fields": {"keyword":{"type": "keyword",
                                    "ignore_above": 256}}},
    "book_description":{"type": "text",
              "analyzer": "ik_max_word",
              "search_analyzer": "ik_max_word",
              "fields": {"keyword":{"type": "keyword",
                                    "ignore_above": 256}}},
    "catalogue": {"type": "text",
              "analyzer": "ik_max_word",
              "search_analyzer": "ik_max_word",
              "fields": {"keyword":{"type": "keyword",
                                    "ignore_above": 256}}},
    "languages": {"type": "keyword"},
    "word_count": {"type": "long"},
    "book_comments": {"type": "long"},
    "store_pricing": {"type": "float"},
    "selling_price": {"type": "float"},
    "publishing_house": {"type": "text",
              "analyzer": "ik_max_word",
              "search_analyzer": "ik_max_word",
              "fields": {"keyword":{"type": "keyword",
                                    "ignore_above": 256}}},
    "publishing_time": {"type": "date"},
    "printing_time": {"type": "date"},
    "edition": {"type": "keyword"},
    "impression": {"type": "keyword"},
    "inventory": {"type": "long"},
    "sales": {"type": "long"},
    "author": {"type": "text",
              "analyzer": "ik_max_word",
              "search_analyzer": "ik_max_word",
              "fields": {"keyword":{"type": "keyword",
                                    "ignore_above": 256}}},
    "shuppites": {"type": "keyword"},
    "format": {"type": "keyword"},
    "is_suit": {"type": "keyword"},
    "suits": {"type": "long"},
    "binding_layout": {"type": "keyword"},
    "pages": {"type": "long"},
    "papers": {"type": "keyword"},
    "uploader": {"type": "keyword"},
    "selling_stores": {"type": "long"},
    "published_year_range": {"type": "keyword"},
    "published_year_integral": {"type": "long"},
    "comments_range": {"type": "keyword"},
    "comments_integral": {"type": "long"},
    "premium_range": {"type": "keyword"},
    "premium_integral": {"type": "long"},
    "selling_stores_range": {"type": "keyword"},
    "selling_stores_integral": {"type": "long"},
    "create_type": {"type": "keyword"},
    "create_time": {"type": "date"},
    "update_time": {"type": "date"}
}
mapping1 = {
    "properties": {
        "ISBN": {
            "type": "long"
        },
        "author": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        },
        "binding_layout": {
            "type": "keyword"
        },
        "book_comments": {
            "type": "long"
        },
        "book_description": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        },
        "book_name": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        },
        "catalogue": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        },
        "category": {
            "type": "keyword"
        },
        "comments_integral": {
            "type": "long"
        },
        "comments_range": {
            "type": "keyword"
        },
        "create_time": {
            "type": "date"
        },
        "create_type": {
            "type": "keyword"
        },
        "edition": {
            "type": "keyword"
        },
        "first_channel": {
            "type": "keyword"
        },
        "format": {
            "type": "keyword"
        },
        "impression": {
            "type": "keyword"
        },
        "inventory": {
            "type": "long"
        },
        "is_suit": {
            "type": "keyword"
        },
        "isbn": {
            "type": "keyword"
        },
        "languages": {
            "type": "keyword"
        },
        "pages": {
            "type": "long"
        },
        "papers": {
            "type": "keyword"
        },
        "uploader": {
            "type": "keyword"
        },
        "premium_integral": {
            "type": "long"
        },
        "premium_range": {
            "type": "keyword"
        },
        "store_pricing": {
            "type": "float"
        },
        "selling_price": {
            "type": "float"
        },
        "printing_time": {
            "type": "date"
        },
        "published_year_integral": {
            "type": "long"
        },
        "published_year_range": {
            "type": "keyword"
        },
        "publishing_house": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        },
        "publishing_time": {
            "type": "date"
        },
        "sales": {
            "type": "long"
        },
        "second_channel": {
            "type": "keyword"
        },
        "selling_stores": {
            "type": "long"
        },
        "selling_stores_integral": {
            "type": "long"
        },
        "selling_stores_range": {
            "type": "keyword"
        },
        "shuppites": {
            "type": "keyword"
        },
        "slogan": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        },
        "store": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        },
        "store_comments": {
            "type": "long"
        },
        "suits": {
            "type": "long"
        },
        "update_time": {
            "type": "date"
        },
        "word_count": {
            "type": "long"
        }
    }
}

mapping = {

    "properties": {
        "content": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        },
        "title": {
            "type": "text",
            "analyzer": "ik_max_word",
            "search_analyzer": "ik_max_word",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
        }
    }

}
# es_con.indices.delete(index="news")
# es_con.indices.create(index='news', ignore=400)
# result=es_con.indices.put_mapping(index="news", doc_type="politics",body=mapping, include_type_name=True)
# print (result)
#
datas = [
    {
        "title": "?????????????????????",
        "content": "??????????????????????????????????????????"},
    {
        "title": "????????????????????????",
        "content": "??????????????????????????????????????????"},
    {
        "title": "????????????????????????",
        "content": "????????????????????????????????????????????????1???????????????"},
    {
        "title": "?????????????????????????????????",
        "content": "???????????????????????????????????????????????? ???????????????"}
]


# # #
# for data in datas:
#     print(data)
#     es_con.index(index="news", doc_type="politics", body=data)
# ????????????
# result = es_con.search(index="news",doc_type="politics")
# print (result["hits"]["hits"])
#
# esl??????
def query_es_return_list(index, query):
    search_context = Search(using=es_con, index=index)
    s = search_context.filter(query)
    response = s.execute()
    if response.success():
        return response['hits']['hits'], response['hits']['total']


# dsl1='??????'

dsl = {
    'query':
        {'match':
             {'title': '?????????????????????'}
         }
}

# result = query_es_return_list("news",dsl1,"politics")
# print (result)
# ????????????
# result = es_con.search(index="news",doc_type="politics",body=dsl)
# results=result # es???????????????????????????

# match_phrase????????????   match????????????


# #??????q0???????????????q1
# q0=Q("match", title="??????")
# q1=Q("match", title='??????') | q0
#
# #??????q0??? ??? ?????????q2
# q0=~Q("match_phrase", title='??????')
# q2 = Q("match_phrase", content='??????') & q0

# ??????q0??? ??? ?????????q3
# q0=""
key = "title" + "__keyword"
# q0= Q("match",**{key:"????????????????????????"})
q0 = Q("term", title__keyword="?????????????????????")
print(q0)
q1 = Q("match", content="??????")
# ????????????
q4 = Q("multi_match", query='??????', fields=['title', 'content'])

mylist = [q0]
q = Q('bool',
      must=mylist,
      should=[],
      must_not=[]

      )
result, total = query_es_return_list("news", q)

# my_text="?????????????????????"
# results,total=query_es_return_list("news",MoreLikeThis(like=my_text, fields=['content', 'title']))
results = []
for i in result:
    results.append(i['_source'])
print(results, total['value'])

# ????????????
#  query = es.search(index='1485073708892',body=query_json,scroll='5m',size=100)
# results = query['hits']['hits'] # es???????????????????????????
#  total = query['hits']['total']  # es????????????????????????
#  scroll_id = query['_scroll_id'] # ??????????????????es????????????????????????
# # ??????????????????es????????????????????????
# total=total['value']
# for i in range(0, int(total/100)+1):
#      # scroll?????????????????????????????????
#      query_scroll = es_con.scroll(scroll_id=scroll_id,scroll='5m')['hits']['hits']
#      results += query_scroll
