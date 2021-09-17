import jwt
from sanic import Blueprint, text
import time
from auth import check_token
from sanic.response import json
from dbControl import db_con
login = Blueprint("login")
unit = 3600 * 24
test_unit = 30


# 用户登录获取token  此token不会过期 需要前端在用户登出时清除用户token
@login.post("/login")
async def book_login(request):
    # if check_token(request):
    #     return redirect(request.app.url_for('booksearch.test'))
    data = request.json

    if data:

        db=db_con()
        document = await db.userInfo.find_one({"username": data["username"],"password":data["password"]})
        if document:
            exp_time = int(time.time() + unit)
            token = jwt.encode({'exp': exp_time}, request.app.config.SECRET)
            return json({'code':200,'msg':"登录成功","token":token})

            # 配置session参考例子
            # token = jwt.encode({'cloud9': time.time()}, request.app.config.SECRET)
            # if not request.ctx.session.get('token'):
            #     request.ctx.session['token'] =token
            # # request.ctx.session['token']=token
            #     return text(token)
            # return text(request.ctx.session.get('token'))
        return json({'code': 500, 'msg': "登录失败"})


    return json({'code':500,'msg':"登录失败"})


@login.post("/register")
async def book_registe(request):
    data = request.json

    if data:
        username = data["username"]
        password = data["password"]
        try:
            phone = data['phone']
        except Exception:
            phone=""
        db=db_con()
        #查询单个
        document = await db.userInfo.find_one({"username": username})
        if document:
            return json({"msg":"用户名已经注册过"})
        else:
            result=await db.userInfo.insert_one({"username": username, "password": password,"phone":phone})
            if result.inserted_id:
                return text("注册成功")
        #查询多个
        # cursor = commons.db.userInfo.find({username: username})
        # for document in await cursor.to_list(length=100):
        #     print(document)

        # result=await commons.db.userInfo.insert_one({username:username,password:password})
        # print('result %s' % repr(result.inserted_id))
        return text("注册失败")
    return text("注册信息提交错误")
