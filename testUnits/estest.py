import json
import time
import itertools
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q
from elasticsearch_dsl import FacetedSearch, TermsFacet, DateHistogramFacet, NestedFacet
from elasticsearch_dsl import connections
from elasticsearch_dsl import MultiSearch, Search

es_con = Elasticsearch(['http:/192.172.9.198:8100', "http://192.172.9.199:8100", "http://192.172.9.200:8100"],
                       sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)


# es_con2 = Elasticsearch(["http://192.172.9.200:9200"])
# connections.configure(
#     default={'hosts': 'http:/192.172.9.198:9200'},
#     dev={
#         'hosts': ["http://192.172.9.199:9200","http://192.172.9.200:9200"],
#         'sniff_on_start': True
#     }
# )


def query_es_return_list(index, query):
    search_context = Search(using=es_con, index=index)
    s = search_context.filter(query)
    print(s)
    try:
        results=s.scan()
        total=s.count()
        print("dmmm")
    except Exception as e:
        print(e)
        results=[]
        total=0
    return results, total
    # try:
    #     response = s.execute()
    # except Exception:
    #     return ([],0)
    #
    # if response.success():
    #     return response['hits']['hits'],response['hits']['total']

# Bool(must_not=[Match(book_name='技术')], should=[Bool(should=[Match(store='清香溢远'), Match(publishing_house='人民交通出版社'), Term(second_channel='正向选品')])])
# # q4=Q("match", query='中国', fields=['title', 'content'])
key = "template_key"
q1 = Q("match", store_comments='2019')
q2= Q("match",store='清香溢远') & q1
q3=Q("match_all")
q0 = Q('range', **{'create_time':{'gte':'0000-01-01T00:00:00','lte':'9999-12-31T23:59:59'}})
mylist = [q0]
# time.strptime('2019', "%Y-%m-%d")
# q0 = Q("term", publishing_time="2019") | Q("match",store_comments="2019asdf")
q = Q('bool',
      must=[q0],
      should=[],
      must_not=[]
      )
print(q)
# NestedFacet('Dark', TermsFacet(field='canvas-workpad-template.template.name'))
results, total = query_es_return_list("dwd_book_base_info", q0)
# ms = MultiSearch(index='book_store',using=es_con)
#
# ms = ms.add(Search().filter('term', isbn__keyword="9787518409143"))
# ms = ms.add(Search().filter('term', tags='elasticsearch'))
# re=Search(using=es_con).filter('term', isbn__keyword="9787518409143")
# print(re.scan())
# for i in re.scan():
#     print (i.name_of_shop)
# responses = ms.execute()

# for response in responses:
#     print(response['hits']['hits'],response['hits']['total'])
# Bool(must_not=[Match(book_name='技术')], should=[Term(second_channel='正向选品'), Match(publishing_house='人民交通出版社'), Match(store='清香溢远')])
#     # print("Results for query %r." % response.search.query)
#     for hit in response:
#         print(hit.name_of_shop)
print(results,total)
# page=1
#
# for i in itertools.islice(results,1000000,1000010):
#     print(i.book_name)
#     page+=1
#     if page==1000000:
#         print("get")
#         break
    # for key in i:
    #     print(i['book_name'])
    # print ("111",i.publishing_house,i.store,i.publishing_time,i.create_time)

def get_results():
    frompage = 1
    topage = 2
    nowpage = 1
    pg_result = []
    for i in results:
        if (nowpage >= frompage) & (nowpage <= topage):
            #构造json
            pg_result.append({"event": i['event']})
        nowpage += 1
    print(pg_result,total)
    return (pg_result,total)
# get_results()


