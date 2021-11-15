import os

from decouple import config

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

REDIS_ADDRESS = config('REDIS_ADDRESS', default='localhost')
REDIS_PORT = config('REDIS_PORT', default='6379')

TRACER_REPORTING_HOST = config('TRACER_REPORTING_HOST', default='localhost')
TRACER_REPORTING_PORT = config('TRACER_REPORTING_PORT', default='6831')


SERVICE_REGISTRY_CMD_KEY = config('SERVICE_REGISTRY_CMD_KEY')

SERVICE_STREAM_KEY = config('SERVICE_STREAM_KEY')
SERVICE_CMD_KEY = config('SERVICE_CMD_KEY')

PREPROCESSOR_CMD_KEY = config('PREPROCESSOR_CMD_KEY')
EVENT_DISPATCHER_CMD_KEY = config('EVENT_DISPATCHER_CMD_KEY')
ADAPTATION_MONITOR_CMD_KEY = config('ADAPTATION_MONITOR_CMD_KEY')
WINDOW_MANAGER_CMD_KEY = config('WINDOW_MANAGER_CMD_KEY')
MATCHER_CMD_KEY = config('MATCHER_CMD_KEY')
FORWARDER_CMD_KEY = config('FORWARDER_CMD_KEY')

LISTEN_CMD_ENTITY_TYPE_PUBJOIN = config('LISTEN_CMD_ENTITY_TYPE_PUBJOIN')
LISTEN_CMD_ENTITY_TYPE_PUBJOIN = config('LISTEN_CMD_ENTITY_TYPE_PUBJOIN')

SERVICE_CMD_KEY_LIST = []
for env_var in os.environ.keys():
    if env_var.startswith('LISTEN_CMD_ENTITY_TYPE_'):
        locals()[env_var] = config(env_var)
        SERVICE_CMD_KEY_LIST.append(locals()[env_var])

SERVICE_DETAILS = None


def string_to_dict_cast(str_value):
    if str_value == "":
        return {}
    final_dict = {}
    for service_str in str_value.split(';'):
        service_type, content_type_str_list = service_str.split(':')
        final_dict[service_type] = {
            'content_type': content_type_str_list.split(',')
        }
    return final_dict


_DEFAULT_MOCKED_STR = "ObjectDetection:ObjectDetection,Person,Car;ColorDetection:ObjectColor,ColorDetection"

MOCKED_SERVICE_REGISTRY = config('MOCKED_SERVICE_REGISTRY', cast=string_to_dict_cast,
                                 default=_DEFAULT_MOCKED_STR)

LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
