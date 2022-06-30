import copy
from unittest.mock import patch, MagicMock

from event_service_utils.tests.base_test_case import MockedEventDrivenServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from client_manager.service import ClientManager
from client_manager.service_registry import ServiceRegistry

from client_manager.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY_LIST,
    SERVICE_DETAILS,
    PUB_EVENT_LIST,
)


class TestClientManager(MockedEventDrivenServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key_list': SERVICE_CMD_KEY_LIST,
        'pub_event_list': PUB_EVENT_LIST,
        'service_details': SERVICE_DETAILS,
        'service_registry': ServiceRegistry(),
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = ClientManager

    MOCKED_CG_STREAM_DICT = {

    }
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        'cg-ClientManager': MOCKED_CG_STREAM_DICT,
    }

    SIMPLE_QUERY_TEXT = """
    REGISTER QUERY my_first_query
    OUTPUT K_GRAPH_JSON
    CONTENT ObjectDetection, ColorDetection
    MATCH (c1:Car {color:'blue'}), (c2:Car {color:'white'})
    FROM test
    WITHIN TUMBLING_COUNT_WINDOW(2)
    RETURN *
    """.strip()

    @patch('client_manager.service.ClientManager.process_event_type')
    def test_process_cmd_should_call_process_event_type(self, mocked_process_event_type):
        event_type = 'SomeEventType'
        unicode_event_type = event_type.encode('utf-8')
        event_data = {
            'id': 1,
            'action': event_type,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_event_type.__name__ = 'process_event_type'

        self.service.service_cmd.mocked_values_dict = {
            unicode_event_type: [msg_tuple]
        }
        self.service.process_cmd()
        self.assertTrue(mocked_process_event_type.called)
        self.service.process_event_type.assert_called_once_with(event_type=event_type, event_data=event_data, json_msg=msg_tuple[1])

    @patch('client_manager.service.ClientManager.process_query_received')
    def test_process_event_type_should_call_process_query_received_with_proper_parameters(self, mocked_q_created_event):
        event_data = {
            'id': 1,
            'subscriber_id': 'sub_id',
            'query': self.SIMPLE_QUERY_TEXT
        }
        event_type = 'QueryReceived'
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_q_created_event.assert_called_once_with(subscriber_id=event_data['subscriber_id'], query_text=event_data['query'])

    @patch('client_manager.service.ClientManager.process_query_deletion_requested')
    def test_process_event_type_should_call_process_query_deletion_requested_with_proper_parameters(self, mocked_del_query):
        event_data = {
            'id': 1,
            'subscriber_id': 'sub_id',
            'query_name': 'my_first_query'
        }
        event_type = 'QueryDeletionRequested'
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_del_query.assert_called_once_with(
            subscriber_id=event_data['subscriber_id'], query_name=event_data['query_name'])

    @patch('client_manager.service.ClientManager.process_publisher_created')
    def test_process_event_type_should_call_process_publisher_created_with_proper_parameters(self, mocked_pub_join):
        event_data = {
            'id': 1,
            'publisher_id': 'pub1',
            'source': 'rtmp://localhost/live/mystream',
            'meta': {
                'geolocation': '',
                'cctv': 'true',
                'color': 'true',
                'fps': '',
                'resolution': '',
                'color_channels': ''
            }
        }

        event_type = 'PublisherCreated'
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_pub_join.assert_called_once_with(
            publisher_id=event_data['publisher_id'],
            source=event_data['source'],
            meta=event_data['meta'],
        )

    @patch('client_manager.service.ClientManager.process_publisher_removed')
    def test_process_event_type_should_call_process_publisher_removed_with_proper_parameters(self, mocked_pub_leave):
        event_data = {
            'id': 1,
            'publisher_id': 'pub1'
        }

        event_type = 'PublisherRemoved'
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_pub_leave.assert_called_once_with(
            publisher_id=event_data['publisher_id'],
        )

    def test_create_query_id_properly_working(self):
        subscriber_id = 'sub_1'
        query_name = 'my incredible query'
        res = self.service.create_query_id(subscriber_id, query_name)
        expected = '6962607866718b3cbd13556162c95dd9'
        self.assertEqual(res, expected)

    @patch('client_manager.service.ClientManager.publish_query_created')
    @patch('client_manager.service.ClientManager.update_bufferstreams_from_new_query')
    @patch('client_manager.service.ClientManager.create_query_dict')
    def test_process_query_received_should_properly_include_query_into_datastructure(
            self, mocked_query_dict, mocked_buffer, mocked_pub):
        subscriber_id = 'sub1'
        query_id = 123
        service_chain = ['ObjectDetection', 'ColorDetection']
        registered_query = {
            'subscriber_id': subscriber_id,
            'query_id': query_id,
            'parsed_query': {
                'from': ['pub1'],
                'content': ['ObjectDetection', 'ColorDetection'],
                'window': {
                    'window_type': 'TUMBLING_COUNT_WINDOW',
                    'args': [2]
                }
                # 'cypher_query': query['cypher_query'],
            },
            'buffer_stream': {
                'publisher_id': 'publisher_id',
                'buffer_stream_key': 'buffer_stream_key',
                'source': 'source',
                'resolution': "300x900",
                'fps': "100",
            },
            'service_chain': service_chain,
        }

        self.service.mocked_registry = MagicMock()
        self.service.mocked_registry.get_service_function_chain_by_content_type_list = MagicMock(
            return_value=service_chain)

        mocked_query_dict.return_value = registered_query
        self.service.process_query_received(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

        mocked_query_dict.assert_called_once_with(subscriber_id, self.SIMPLE_QUERY_TEXT)
        mocked_buffer.assert_called_once_with(query=registered_query)
        self.assertIn(query_id, self.service.queries.keys())
        self.assertIn(registered_query, self.service.queries.values())


    @patch('client_manager.service.ClientManager.publish_query_created')
    @patch('client_manager.service.ClientManager.update_bufferstreams_from_new_query')
    @patch('client_manager.service.ClientManager.create_query_dict')
    def test_process_query_received_shouldnt_process_query_if_no_pub_registered(
            self, mocked_query_dict, mocked_buffer, mocked_pub):
        subscriber_id = 'sub1'
        query_id = 123

        mocked_query_dict.return_value = None
        self.service.process_query_received(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

        mocked_query_dict.assert_called_once_with(subscriber_id, self.SIMPLE_QUERY_TEXT)
        self.assertFalse(mocked_buffer.called)
        self.assertFalse(mocked_pub.called)

        self.assertNotIn(query_id, self.service.queries.keys())


    @patch('client_manager.service.ClientManager.publish_query_created')
    @patch('client_manager.service.ClientManager.update_bufferstreams_from_new_query')
    @patch('client_manager.service.ClientManager.create_query_dict')
    def test_process_query_received_shouldn_include_duplicated_query(
            self, mocked_query_dict, mocked_buffer, mocked_pub):
        subscriber_id = 'sub1'
        query_id = 123
        service_chain = ['ObjectDetection', 'ColorDetection']
        registered_query = {
            'subscriber_id': subscriber_id,
            'query_id': query_id,
            'parsed_query': {
                'from': ['pub1'],
                'content': ['ObjectDetection', 'ColorDetection'],
                'window': {
                    'window_type': 'TUMBLING_COUNT_WINDOW',
                    'args': [2]
                }
                # 'cypher_query': query['cypher_query'],
            },
            'buffer_stream': {
                'publisher_id': 'publisher_id',
                'buffer_stream_key': 'buffer_stream_key',
                'source': 'source',
                'resolution': "300x900",
                'fps': "100",
            },
            'service_chain': service_chain,
        }

        registered_query2 = copy.deepcopy(registered_query)
        registered_query2['duplicated'] = 'this query is dup'

        self.service.mocked_registry = MagicMock()
        self.service.mocked_registry.get_service_function_chain_by_content_type_list = MagicMock(
            return_value=service_chain)

        mocked_query_dict.return_value = registered_query
        self.service.process_query_received(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

        mocked_query_dict.return_value = registered_query2
        self.service.process_query_received(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

        self.assertIn(123, self.service.queries.keys())
        self.assertIn(registered_query, self.service.queries.values())
        self.assertNotIn(registered_query2, self.service.queries.values())


    @patch('client_manager.service.ClientManager.publish_query_removed')
    @patch('client_manager.service.ClientManager.update_bufferstreams_from_del_query')
    @patch('client_manager.service.ClientManager.create_query_id')
    def test_process_query_deletion_requested_should_properly_remove_query_into_datastructure(self, mocked_query_id, mocked_buffer, mocked_del_query):
        subscriber_id = 'sub1'
        query_name = 'some query'
        query_id = 123
        query_dict = {'id': query_id, 'etc': '...'}
        self.service.queries = {
            query_id: query_dict,
            456: {'other': '...'}
        }

        mocked_query_id.return_value = query_id
        self.service.process_query_deletion_requested(subscriber_id, query_name=query_name)

        mocked_query_id.assert_called_once_with(subscriber_id, query_name)
        mocked_buffer.assert_called_once_with(query_id)
        mocked_del_query.assert_called_once_with(query=query_dict)
        self.assertNotIn(123, self.service.queries.keys())
        self.assertNotIn(query_dict, self.service.queries.values())

        self.assertIn(456, self.service.queries.keys())
        self.assertIn({'other': '...'}, self.service.queries.values())

    @patch('client_manager.service.ClientManager.create_query_id')
    def test_process_query_deletion_requested_should_ignore_deleting_nonexising_query(self, mocked_query_id):
        subscriber_id = 'sub1'
        query_name = 'some query'
        query_id = 123
        query_dict = {'id': query_id, 'etc': '...'}
        self.service.queries = {
            456: {'other': '...'}
        }

        mocked_query_id.return_value = query_id
        self.service.process_query_deletion_requested(subscriber_id, query_name=query_name)

        mocked_query_id.assert_called_once_with(subscriber_id, query_name)
        self.assertNotIn(123, self.service.queries.keys())
        self.assertNotIn(query_dict, self.service.queries.values())

        self.assertIn(456, self.service.queries.keys())
        self.assertIn({'other': '...'}, self.service.queries.values())

    def test_process_publisher_created_should_properly_include_publisher_into_datastructure(self):

        publisher_id = 'pub1'
        source = 'http://etc.com',
        meta = {'fps': 30}
        publisher = {
            'id': publisher_id,
            'source': source,
            'meta': meta
        }
        self.service.process_publisher_created(publisher_id, source, meta)

        self.assertIn(publisher_id, self.service.publishers.keys())
        self.assertIn(publisher, self.service.publishers.values())

    def test_process_publisher_removed_should_properly_remove_publisher_into_datastructure(self):
        publisher_id = 'pub1'
        source = 'http://etc.com',
        meta = {'fps': 30}
        publisher = {
            'id': publisher_id,
            'source': source,
            'meta': meta
        }
        self.service.process_publisher_created(publisher_id, source, meta)
        self.service.process_publisher_removed(publisher_id)

        self.assertNotIn(publisher_id, self.service.publishers.keys())
        self.assertNotIn(publisher, self.service.publishers.values())

    def test_get_unique_buffer_hash(self):
        query_content = ['abc', 'dfg']
        publisher_id = 'pub_id1'
        resolution = "300x900"
        fps = "100"
        bufferstream_key = self.service.get_unique_buffer_hash(query_content, publisher_id, resolution, fps)
        excepted_key = '7a8cde7a97f51f561cda88d38df63caa'
        self.assertEqual(bufferstream_key, excepted_key)

    @patch('client_manager.service.ClientManager.get_unique_buffer_hash')
    def test_generate_query_bufferstream_dict_for_existing_pub(self, mocked_unique_buff):
        buffer_stream_key = 'bufferstream-key'
        mocked_unique_buff.return_value = buffer_stream_key

        pub_id = 'pub_id'
        subscriber_id = 'sub1'
        query_id = 123
        registered_query = {
            'subscriber_id': subscriber_id,
            'query_id': query_id,
            'parsed_query': {
                'name': 'my_first_query',
                'from': [pub_id],
                'content': ['ObjectDetection', 'ColorDetection'],
                'window': {
                    'window_type': 'TUMBLING_COUNT_WINDOW',
                    'args': [2]
                }
            },
        }

        self.service.publishers = {
            pub_id: {
                'meta': {
                    'fps': '30',
                    'resolution': '300x300',
                },
                'source': 'abc',
            }
        }

        expected_dict = {
            'publisher_id': pub_id,
            'buffer_stream_key': buffer_stream_key,
            'source': 'abc',
            'resolution': '300x300',
            'fps': '30',
        }

        ret = self.service.generate_query_bufferstream_dict(registered_query)

        self.assertDictEqual(ret, expected_dict)

    def test_generate_query_bufferstream_dict_for_non_existing_pub(self):
        pub_id = 'pub_id'
        subscriber_id = 'sub1'
        query_id = 123
        registered_query = {
            'subscriber_id': subscriber_id,
            'query_id': query_id,
            'parsed_query': {
                'name': 'my_first_query',
                'from': [pub_id],
                'content': ['ObjectDetection', 'ColorDetection'],
                'window': {
                    'window_type': 'TUMBLING_COUNT_WINDOW',
                    'args': [2]
                }
            },
        }

        self.service.publishers = {
        }

        ret = self.service.generate_query_bufferstream_dict(registered_query)

        self.assertEqual(ret, None)

    @patch('client_manager.service.ClientManager.generate_query_bufferstream_dict')
    @patch('client_manager.service.ClientManager.generate_query_service_chain')
    @patch('client_manager.service.ClientManager.create_query_id')
    def test_create_query_dict_parses_query_and_return_proper_dict(self, mocked_query_id, mocked_sc, mocked_buff_dict):
        subscriber_id = 'sub_1'
        query_id = '123'
        expected_query_name = 'my_first_query'
        mocked_query_id.return_value = query_id
        query = self.service.create_query_dict(subscriber_id, self.SIMPLE_QUERY_TEXT)
        mocked_query_id.assert_called_once_with(subscriber_id, expected_query_name)
        self.assertIn('subscriber_id', query.keys())
        self.assertIn('query_id', query.keys())
        self.assertIn('parsed_query', query.keys())
        self.assertIn('buffer_stream', query.keys())
        self.assertIn('service_chain', query.keys())

        self.assertEqual(query['query_id'], query_id)
        self.assertEqual(query['subscriber_id'], subscriber_id)
        self.assertEqual(query['parsed_query']['name'], expected_query_name)

    def test_update_bufferstreams_from_new_query_should_update_bufferstreams(self):
        subscriber_id = 'sub_1'
        query_id = '123'
        pub_id = 'pub1'
        self.service.publishers = {
            pub_id: {
                'meta': {
                    'fps': '30',
                    'resolution': '300x300',
                },
                'source': 'abc',
            }
        }
        registered_query = {
            'subscriber_id': subscriber_id,
            'query_id': query_id,
            'parsed_query': {
                'name': 'my_first_query',
                'from': [pub_id],
                'content': ['ObjectDetection', 'ColorDetection'],
                'window': {
                    'window_type': 'TUMBLING_COUNT_WINDOW',
                    'args': [2]
                }
            },
            'buffer_stream': {'buffer_stream_key': 'key'}
        }

        self.service.update_bufferstreams_from_new_query(registered_query)
        self.assertIn('key', self.service.buffer_hash_to_query_map)
        self.assertEqual(self.service.buffer_hash_to_query_map['key'], set({query_id}))

    def test_update_bufferstreams_from_del_query_should_update_bufferstreams(self):
        query_id = '123'
        bufferstream_key = 'bufferstream-key'
        self.service.buffer_hash_to_query_map = {bufferstream_key: set({query_id})}

        self.service.update_bufferstreams_from_del_query(query_id)
        self.assertDictEqual(
            self.service.buffer_hash_to_query_map,
            {}
        )

    def test_update_bufferstreams_from_del_query_shouldn_remove_bufferstream_if_not_empty(self):
        query_id = '123'
        bufferstream_key = 'bufferstream-key'
        self.service.buffer_hash_to_query_map = {bufferstream_key: set({query_id, 'query_2'})}

        self.service.update_bufferstreams_from_del_query(query_id)
        self.assertDictEqual(
            self.service.buffer_hash_to_query_map,
            {bufferstream_key: set({'query_2'})}
        )

    @patch('client_manager.service.ClientManager.process_service_worker_announced')
    def test_process_event_type_should_call_process_service_worker_announced_with_proper_parameters(self, mocked_p_sw):
        event_data = {
            'id': 1,
            'worker': {
                'service_type': 'SomeService',
                'stream_key': 'ss-data'
            }
        }

        event_type = 'ServiceWorkerAnnounced'
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_p_sw.assert_called_once_with(
            worker=event_data['worker'],
        )
