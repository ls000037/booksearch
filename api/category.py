from sanic import Blueprint
from dbControl import db_con
from sanic.response import json
from auth import protected
category_bp = Blueprint('category_bp')


@category_bp.route('/category', methods=["GET"])
@protected
async def download(requset):
    if requset.method == 'GET':
        db = db_con()

        categorys=[]
        # 查询多个
        try:
            document = db.bookcategory.find({},{"category":1})
            for i in await document.to_list(length=100):
                categorys.append(i['category'])
            return json({"code":200,"msg":"获取书籍分类成功","data":categorys})
        except Exception as e:
            print(e)
            return json({"code":500,"msg":"获取书籍分类失败","err_info":e})

