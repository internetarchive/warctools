CONTENT_TYPE="application/http"
REQUEST_TYPE="%s;msgtype=request"%CONTENT_TYPE
RESPONSE_TYPE="%s;msgtype=response"%CONTENT_TYPE
from hanzo.httptools.messaging import RequestMessage, ResponseMessage


__all__ = [
    "CONTENT_TYPE",
    "REQUEST_TYPE",
    "RESPONSE_TYPE",
    "RequestMessage",
    "ResponseMessage",
]
