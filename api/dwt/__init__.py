from sanic import Blueprint
from .targetBoard import targetBoard_bp

dwt = Blueprint.group(targetBoard_bp, url_prefix="/board")