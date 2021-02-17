import json
from unittest.mock import patch

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from client_manager.service import ClientManager

from client_manager.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
)


class TestClientManager(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = ClientManager
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    SIMPLE_QUERY_TEXT = """
    REGISTER QUERY my_first_query
    OUTPUT K_GRAPH_JSON
    CONTENT ObjectDetection, ColorDetection
    MATCH (c1:Car {color:'blue'}) AND (c2:Car {color:'white'})
    FROM test
    WITHIN TUMBLING_COUNT_WINDOW(2)
    RETURN *
    """.strip()

    @patch('client_manager.service.ClientManager.process_action')
    def test_process_cmd_should_call_process_action(self, mocked_process_action):
        action = 'someAction'
        event_data = {
            'id': 1,
            'action': action,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_action.__name__ = 'process_action'

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_process_action.called)
        self.service.process_action.assert_called_once_with(action=action, event_data=event_data, json_msg=msg_tuple[1])

    @patch('client_manager.service.ClientManager.add_query_action')
    def test_process_action_should_call_add_query_with_proper_parameters(self, mocked_add_query):
        event_data = {
            'id': 1,
            'action': 'addQuery',
            'subscriber_id': 'sub_id',
            'query': self.SIMPLE_QUERY_TEXT
        }
        action = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_action(action, event_data, json_msg)
        # self.service.add_query_action.assert_called_once_with(action=action, event_data=event_data, json_msg=json_msg)
        mocked_add_query.assert_called_once_with(subscriber_id=event_data['subscriber_id'], query_text=event_data['query'])

    @patch('client_manager.service.ClientManager.del_query_action')
    def test_process_action_should_call_del_query_with_proper_parameters(self, mocked_del_query):
        event_data = {
            'id': 1,
            'action': 'delQuery',
            'subscriber_id': 'sub_id',
            'query_name': 'my_first_query'
        }
        action = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_action(action, event_data, json_msg)
        # self.service.add_query_action.assert_called_once_with(action=action, event_data=event_data, json_msg=json_msg)
        mocked_del_query.assert_called_once_with(
            subscriber_id=event_data['subscriber_id'], query_name=event_data['query_name'])

    @patch('client_manager.service.ClientManager.pub_join_action')
    def test_process_action_should_call_pub_join_with_proper_parameters(self, mocked_pub_join):
        event_data = {
            'id': 1,
            'action': 'pubJoin',
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

        action = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_action(action, event_data, json_msg)
        mocked_pub_join.assert_called_once_with(
            publisher_id=event_data['publisher_id'],
            source=event_data['source'],
            meta=event_data['meta'],
        )

    @patch('client_manager.service.ClientManager.pub_leave_action')
    def test_process_action_should_call_pub_leave_with_proper_parameters(self, mocked_pub_leave):
        event_data = {
            'id': 1,
            'action': 'pubLeave',
            'publisher_id': 'pub1'
        }

        action = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_action(action, event_data, json_msg)
        mocked_pub_leave.assert_called_once_with(
            publisher_id=event_data['publisher_id'],
        )

    def test_create_query_id_properly_working(self):
        subscriber_id = 'sub_1'
        query_name = 'my incredible query'
        res = self.service.create_query_id(subscriber_id, query_name)
        expected = '6962607866718b3cbd13556162c95dd9'
        self.assertEqual(res, expected)

    @patch('client_manager.service.ClientManager.create_query_dict')
    def test_add_query_should_properly_include_query_into_datastructure(self, mocked_query_dict):
        subscriber_id = 'sub1'
        query_id = 123
        query_dict = {
            'id': query_id,
            'from': 'test',
            'etc': '...'
        }
        mocked_query_dict.return_value = query_dict
        self.service.add_query_action(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

        mocked_query_dict.assert_called_once_with(subscriber_id, self.SIMPLE_QUERY_TEXT)
        self.assertIn(123, self.service.queries.keys())
        self.assertIn(query_dict, self.service.queries.values())

    @patch('client_manager.service.ClientManager.create_query_dict')
    def test_add_query_shouldn_include_duplicated_query(self, mocked_query_dict):
        subscriber_id = 'sub1'
        query_id = 123
        query_dict = {
            'id': query_id,
            'from': 'test',
            'etc': '...'
        }
        query_dict2 = {
            'id': query_id,
            'from': 'test',
            'other': '...'
        }
        mocked_query_dict.return_value = query_dict
        self.service.add_query_action(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

        mocked_query_dict.return_value = query_dict2
        self.service.add_query_action(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

        self.assertIn(123, self.service.queries.keys())
        self.assertIn(query_dict, self.service.queries.values())
        self.assertNotIn(query_dict2, self.service.queries.values())

    @patch('client_manager.service.ClientManager.create_query_id')
    def test_del_query_should_properly_remove_query_into_datastructure(self, mocked_query_id):
        subscriber_id = 'sub1'
        query_name = 'some query'
        query_id = 123
        query_dict = {'id': query_id, 'etc': '...'}
        self.service.queries = {
            query_id: query_dict,
            456: {'other': '...'}
        }

        mocked_query_id.return_value = query_id
        self.service.del_query_action(subscriber_id, query_name=query_name)

        mocked_query_id.assert_called_once_with(subscriber_id, query_name)
        self.assertNotIn(123, self.service.queries.keys())
        self.assertNotIn(query_dict, self.service.queries.values())

        self.assertIn(456, self.service.queries.keys())
        self.assertIn({'other': '...'}, self.service.queries.values())

    @patch('client_manager.service.ClientManager.create_query_id')
    def test_del_query_should_ignore_deleting_nonexising_query(self, mocked_query_id):
        subscriber_id = 'sub1'
        query_name = 'some query'
        query_id = 123
        query_dict = {'id': query_id, 'etc': '...'}
        self.service.queries = {
            456: {'other': '...'}
        }

        mocked_query_id.return_value = query_id
        self.service.del_query_action(subscriber_id, query_name=query_name)

        mocked_query_id.assert_called_once_with(subscriber_id, query_name)
        self.assertNotIn(123, self.service.queries.keys())
        self.assertNotIn(query_dict, self.service.queries.values())

        self.assertIn(456, self.service.queries.keys())
        self.assertIn({'other': '...'}, self.service.queries.values())

    def test_pub_join_should_properly_include_publisher_into_datastructure(self):

        publisher_id = 'pub1'
        source = 'http://etc.com',
        meta = {'fps': 30}
        publisher = {
            'id': publisher_id,
            'source': source,
            'meta': meta
        }
        self.service.pub_join_action(publisher_id, source, meta)

        self.assertIn(publisher_id, self.service.publishers.keys())
        self.assertIn(publisher, self.service.publishers.values())

    def test_pub_leave_should_properly_remove_publisher_into_datastructure(self):
        publisher_id = 'pub1'
        source = 'http://etc.com',
        meta = {'fps': 30}
        publisher = {
            'id': publisher_id,
            'source': source,
            'meta': meta
        }
        self.service.pub_join_action(publisher_id, source, meta)
        self.service.pub_leave_action(publisher_id)

        self.assertNotIn(publisher_id, self.service.publishers.keys())
        self.assertNotIn(publisher, self.service.publishers.values())

    @patch('client_manager.service.ClientManager.create_query_id')
    def test_create_query_dict_parses_query_and_return_proper_dict(self, mocked_query_id):
        subscriber_id = 'sub_1'
        query_id = '123'
        expected_query_name = 'my_first_query'
        mocked_query_id.return_value = query_id
        query = self.service.create_query_dict(subscriber_id, self.SIMPLE_QUERY_TEXT)
        mocked_query_id.assert_called_once_with(subscriber_id, expected_query_name)
        self.assertIn('subscriber_id', query.keys())
        self.assertIn('id', query.keys())
        self.assertIn('name', query.keys())

        self.assertEqual(query['id'], query_id)
        self.assertEqual(query['subscriber_id'], subscriber_id)
        self.assertEqual(query['name'], expected_query_name)

    @patch('client_manager.service.ClientManager.get_unique_buffer_hash')
    def test_update_bufferstreams_from_new_query_should_update_bufferstreams(self, mocked_unique_buff):
        query_id = '123'
        bufferstream_key = 'bufferstream-key'
        mocked_unique_buff.return_value = bufferstream_key
        self.service.publishers = {
            'pub1': {
                'meta': {
                    'fps': '30',
                    'resolution': '300x300',
                }
            }
        }
        query = {
            'id': query_id,
            'from': ['pub1']
        }

        self.service.update_bufferstreams_from_new_query(query)
        mocked_unique_buff.assert_called_once_with('pub1', '300x300', '30')
        self.assertIn(bufferstream_key, self.service.buffer_hash_to_query_map)
        self.assertEqual(self.service.buffer_hash_to_query_map[bufferstream_key], set({query_id}))

    @patch('client_manager.service.ClientManager.get_unique_buffer_hash')
    def test_update_bufferstreams_from_new_query_should_not_update_bufferstreams_if_no_pub(self, mocked_unique_buff):
        query_id = '123'
        bufferstream_key = 'bufferstream-key'
        mocked_unique_buff.return_value = bufferstream_key
        self.service.publishers = {
            'pub1': {
                'meta': {
                    'fps': '30',
                    'resolution': '300x300',
                }
            }
        }
        query = {
            'id': query_id,
            'from': ['pub2']
        }

        self.service.update_bufferstreams_from_new_query(query)
        self.assertFalse(mocked_unique_buff.called)
        self.assertNotIn(bufferstream_key, self.service.buffer_hash_to_query_map)
