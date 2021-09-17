from functools import wraps
import jwt
from sanic import text,json


def check_token(request):
    if not request.token:
        return False
    #后端算法验证token
    try:
        jwt.decode(
            request.token, request.app.config.SECRET, algorithms=["HS256"]
        )

    except jwt.exceptions.InvalidTokenError:
        return False

    else:
        return True

    # redis验证token
    # try:
    #     if request.ctx.session['token']==request.token:
    #         return True
    #     else:
    #         return False
    #
    # except Exception as e:
    #     return False


def protected(wrapped):
    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            is_authenticated = check_token(request)

            if is_authenticated:
                response = await f(request, *args, **kwargs)
                return response
            else:
                return json({"code":401,"msg":"You are unauthorized."})

        return decorated_function

    return decorator(wrapped)