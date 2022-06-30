#!/usr/bin/env python
import time
import uuid
import json
from event_service_utils.streams.redis import RedisStreamFactory

from client_manager.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_msg(event_data):
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)

    import ipdb; ipdb.set_trace()
    pubJoin_cmd = stream_factory.create('PublisherCreated', stype='streamOnly')
    pubJoin_cmd.write_events(
        new_msg(
            {
                'publisher_id': 'pid',
                'source': 'psource',
                'meta': {
                    'resolution': '300x300',
                    'fps': '30',
                }
            }
        )
    )


    addworker_cmd = stream_factory.create('ServiceWorkerAnnounced', stype='streamOnly')
    addworker_cmd.write_events(
        new_msg(
            {
                'worker': {
                    'service_type': 'ObjectDetection',
                    'stream_key': 'objworker-key',
                    'queue_limit': 100,
                    'throughput': 10,
                    'accuracy': 0.1,
                    'energy_consumption': 100,
                }
            }
        )
    )

    addworker_cmd.write_events(
        new_msg(
            {
                'worker': {
                    'service_type': 'ObjectDetection',
                    'stream_key': 'objworker-key2',
                    'queue_limit': 100,
                    'throughput': 1,
                    'accuracy': 0.9,
                    'energy_consumption': 10,
                }
            }
        )
    )

    addworker_cmd.write_events(
        new_msg(
            {
                'worker': {
                    'service_type': 'ColorDetection',
                    'stream_key': 'clrworker-key',
                    'queue_limit': 100,
                    'throughput': 1,
                    'accuracy': 0.9,
                    'energy_consumption': 10,
                }
            }
        )
    )
    # import ipdb; ipdb.set_trace()
    time.sleep(1.5)


    query_text = """
    REGISTER QUERY my_first_query
    OUTPUT K_GRAPH_JSON
    CONTENT ObjectDetection, ColorDetection
    MATCH (c1:Car {color:'blue'}), (c2:Car {color:'white'})
    FROM pid
    WITHIN TUMBLING_COUNT_WINDOW(2)
    WITH_QOS latency = 'min'
    RETURN *
    """.strip()

    addQuery_cmd = stream_factory.create('QueryReceived', stype='streamOnly')
    addQuery_cmd.write_events(
        new_msg(
            {
                'subscriber_id': 'sid',
                'query': query_text
            }
        )
    )

    import ipdb; ipdb.set_trace()
    # events = workermon_stream.read_events()
    worker_stream = stream_factory.create('objworker-key', stype='streamOnly')
    for i in range(10):
        worker_stream.write_events(
            new_msg(
                {
                    'msg': 'a'
                }
            )
        )
    import ipdb; ipdb.set_trace()
    for i in range(100):
        worker_stream.write_events(
            new_msg(
                {
                    'msg': 'a'
                }
            )
        )


    import ipdb; ipdb.set_trace()
    delQuery_cmd = stream_factory.create('QueryDeletionRequested', stype='streamOnly')
    delQuery_cmd.write_events(
        new_msg(
            {
                'subscriber_id': 'sid',
                'query_name': 'my_first_query',
            }
        )
    )

    import ipdb; ipdb.set_trace()
    # service_stream = stream_factory.create(SERVICE_STREAM_KEY, stype='streamOnly')
    # send_data_msg(service_stream)


if __name__ == '__main__':
    main()
