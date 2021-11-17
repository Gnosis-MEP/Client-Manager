import os

from decouple import config

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

REDIS_ADDRESS = config('REDIS_ADDRESS', default='localhost')
REDIS_PORT = config('REDIS_PORT', default='6379')

TRACER_REPORTING_HOST = config('TRACER_REPORTING_HOST', default='localhost')
TRACER_REPORTING_PORT = config('TRACER_REPORTING_PORT', default='6831')


SERVICE_STREAM_KEY = config('SERVICE_STREAM_KEY')

SERVICE_CMD_KEY_LIST = []

PUB_EVENT_TYPE_QUERY_REGISTERED = config('PUB_EVENT_TYPE_QUERY_REGISTERED')


PUB_EVENT_LIST = [
    PUB_EVENT_TYPE_QUERY_REGISTERED,
]

for env_var in os.environ.keys():
    if env_var.startswith('LISTEN_EVENT_TYPE_'):
        locals()[env_var] = config(env_var)
        SERVICE_CMD_KEY_LIST.append(locals()[env_var])

SERVICE_DETAILS = None


LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
