import azure.functions as func

import json
import os

def APP_ENV_APITOKEN():
    return "MERCHANT_API_TOKEN"

def rx_not_found(msg:str = "Not Found") -> func.HttpResponse:
    return func.HttpResponse(status_code=404, body=msg)

def rx_bad_request(msg="bad request") -> func.HttpResponse:
    return func.HttpResponse(msg, status_code=400)

def rx_invalid_method() -> func.HttpResponse:
    return func.HttpResponse(status_code=405)

def rx_ok(msg="ok") -> func.HttpResponse:
    return func.HttpResponse(msg, status_code=200)

def rx_json(data: dict) -> func.HttpResponse:
    return func.HttpResponse(json.dumps(data), mimetype="application/json", status_code=200)

def rx_unauthorized() -> func.HttpResponse:
    return func.HttpResponse(status_code=401)

def is_post(req: func.HttpRequest) -> bool:
    return req.method == "POST"

def is_get(req: func.HttpRequest) -> bool:
    return req.method == "GET"

def is_post_or_get(req: func.HttpRequest) -> bool:
    return is_get(req) or is_post(req)

def is_authorized(client_token: str) -> bool:
    secure_token = os.environ[APP_ENV_APITOKEN()]
    return secure_token == client_token

def is_health_check(req: func.HttpRequest) -> bool:
    return "health" in req.params

def get_json_body(req: func.HttpRequest) -> dict:
    body = req.get_body().decode("utf-8")
    return json.loads(body)

def get_header(req: func.HttpRequest, header_name: str) -> str:
    return req.headers.get(header_name)

def get_headers(req: func.HttpRequest) -> dict:
    return dict(req.headers)