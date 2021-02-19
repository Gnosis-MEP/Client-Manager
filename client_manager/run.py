#!/usr/bin/env python
from event_service_utils.streams.redis import RedisStreamFactory

from client_manager.service import ClientManager
from client_manager.mocked_service_registry import MockedRegistry

from client_manager.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
    PREPROCESSOR_CMD_KEY,
    EVENT_DISPATCHER_CMD_KEY,
    ADAPTATION_MONITOR_CMD_KEY,
    WINDOW_MANAGER_CMD_KEY,
    LOGGING_LEVEL,
    TRACER_REPORTING_HOST,
    TRACER_REPORTING_PORT,
    MOCKED_SERVICE_REGISTRY,
)


def run_service():
    mocked_registry = MockedRegistry(MOCKED_SERVICE_REGISTRY)

    tracer_configs = {
        'reporting_host': TRACER_REPORTING_HOST,
        'reporting_port': TRACER_REPORTING_PORT,
    }
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service = ClientManager(
        service_stream_key=SERVICE_STREAM_KEY,
        service_cmd_key=SERVICE_CMD_KEY,
        stream_factory=stream_factory,
        preprocessor_cmd_key=PREPROCESSOR_CMD_KEY,
        event_dispatcher_cmd_key=EVENT_DISPATCHER_CMD_KEY,
        adaptation_planner_cmd_key=ADAPTATION_MONITOR_CMD_KEY,
        window_manager_cmd_key=WINDOW_MANAGER_CMD_KEY,
        mocked_registry=mocked_registry,
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
