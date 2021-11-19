#!/usr/bin/env python
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
                    'resolution': '300X300',
                    'fps': '30',
                }
            }
        )
    )


    import ipdb; ipdb.set_trace()

    query_text = """
    REGISTER QUERY my_first_query
    OUTPUT K_GRAPH_JSON
    CONTENT ObjectDetection, ColorDetection
    MATCH (c1:Car {color:'blue'}), (c2:Car {color:'white'})
    FROM pid
    WITHIN TUMBLING_COUNT_WINDOW(2)
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
