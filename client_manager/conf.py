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


# def string_to_dict_cast(str_value):
#     if str_value == "":
#         return {}
#     final_dict = {}
#     for service_str in str_value.split(';'):
#         service_type, content_type_str_list = service_str.split(':')
#         final_dict[service_type] = {
#             'content_type': content_type_str_list.split(',')
#         }
#     return final_dict


# _DEFAULT_MOCKED_STR = "ObjectDetection:ObjectDetection,Person,Car;ColorDetection:ObjectColor,ColorDetection"

# MOCKED_SERVICE_REGISTRY = config('MOCKED_SERVICE_REGISTRY', cast=string_to_dict_cast,
#                                  default=_DEFAULT_MOCKED_STR)

LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
