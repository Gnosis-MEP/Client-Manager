#!/usr/bin/env python
from event_service_utils.streams.redis import RedisStreamFactory

from client_manager.service import ClientManager
from client_manager.service_registry import ServiceRegistry

from client_manager.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_REGISTRY_CMD_KEY,
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY_LIST,
    LOGGING_LEVEL,
    TRACER_REPORTING_HOST,
    TRACER_REPORTING_PORT,
    SERVICE_DETAILS,
)


def run_service():
    service_registry = ServiceRegistry()

    tracer_configs = {
        'reporting_host': TRACER_REPORTING_HOST,
        'reporting_port': TRACER_REPORTING_PORT,
    }
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service = ClientManager(
        service_stream_key=SERVICE_STREAM_KEY,
        service_cmd_key_list=SERVICE_CMD_KEY_LIST,
        service_registry_cmd_key=SERVICE_REGISTRY_CMD_KEY,
        service_details=SERVICE_DETAILS,
        stream_factory=stream_factory,
        service_registry=service_registry,
        logging_level=LOGGING_LEVEL,
        tracer_configs=tracer_configs
    )
    service.run()


def main():
    try:
        run_service()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
