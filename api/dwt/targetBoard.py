from sanic import Blueprint
from searchEngine import sort_es_return_list
from sanic.response import json
from elasticsearch_dsl import Q
from auth import protected
from sanic.response import text

targetBoard_bp = Blueprint('targetBoard_bp')


@targetBoard_bp.route('/publisher-export', methods=["GET"])
@protected
async def publisherExport(requset):
    if requset.method == 'GET':
        return text("111")

@targetBoard_bp.route('/categorys', methods=["GET"])
async def publisherExport(requset):
    if requset.method == 'GET':
        q0='category'
        results, total = sort_es_return_list("dwd_book_base_info", q0)
        categorys=[]
        for idata in results:
            categorys.append({"category":idata['_source']['category']})
            print(idata['_source']['category'])
        return json(categorys,ensure_ascii=False)

@targetBoard_bp.route('/sales-trend', methods=["GET"])
@protected
async def publisherExport(requset):
    if requset.method == 'GET':
        pass


@targetBoard_bp.route('/categorys-detail', methods=["GET"])
@protected
async def publisherExport(requset):
    if requset.method == 'GET':
        pass

@targetBoard_bp.route('/premium', methods=["GET"])
@protected
async def publisherExport(requset):
    if requset.method == 'GET':
        pass