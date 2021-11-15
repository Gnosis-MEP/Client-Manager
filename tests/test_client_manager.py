from unittest.mock import patch, MagicMock

from event_service_utils.tests.base_test_case import MockedEventDrivenServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from client_manager.service import ClientManager
from client_manager.mocked_service_registry import MockedRegistry

from client_manager.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY_LIST,
    PREPROCESSOR_CMD_KEY,
    EVENT_DISPATCHER_CMD_KEY,
    ADAPTATION_MONITOR_CMD_KEY,
    WINDOW_MANAGER_CMD_KEY,
    MATCHER_CMD_KEY,
    FORWARDER_CMD_KEY,
    SERVICE_DETAILS,
    SERVICE_REGISTRY_CMD_KEY
)


class TestClientManager(MockedEventDrivenServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key_list': SERVICE_CMD_KEY_LIST,
        'service_registry_cmd_key': SERVICE_REGISTRY_CMD_KEY,
        'service_details': SERVICE_DETAILS,
        'mocked_registry': MockedRegistry(),
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

    @patch('client_manager.service.ClientManager.process_event_type')
    def test_service_cmd_list_has_all_listened_event_types(self, mocked_process_event_type):
        expected_event_types = [
            'pubJoin',
            'addQuery',
            'delQuery',
            'pubLeave',
        ]
        self.assertEqual(
            self.service.service_cmd_key_list,
            expected_event_types
        )

    @patch('client_manager.service.ClientManager.add_query_action')
    def test_process_event_type_should_call_add_query_with_proper_parameters(self, mocked_add_query):
        event_data = {
            'id': 1,
            'action': 'addQuery',
            'subscriber_id': 'sub_id',
            'query': self.SIMPLE_QUERY_TEXT
        }
        event_type = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_add_query.assert_called_once_with(subscriber_id=event_data['subscriber_id'], query_text=event_data['query'])

    @patch('client_manager.service.ClientManager.del_query_action')
    def test_process_event_type_should_call_del_query_with_proper_parameters(self, mocked_del_query):
        event_data = {
            'id': 1,
            'action': 'delQuery',
            'subscriber_id': 'sub_id',
            'query_name': 'my_first_query'
        }
        event_type = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_del_query.assert_called_once_with(
            subscriber_id=event_data['subscriber_id'], query_name=event_data['query_name'])

    @patch('client_manager.service.ClientManager.pub_join_action')
    def test_process_event_type_should_call_pub_join_with_proper_parameters(self, mocked_pub_join):
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

        event_type = event_data['action']
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)
        mocked_pub_join.assert_called_once_with(
            publisher_id=event_data['publisher_id'],
            source=event_data['source'],
            meta=event_data['meta'],
        )

    @patch('client_manager.service.ClientManager.pub_leave_action')
    def test_process_event_type_should_call_pub_leave_with_proper_parameters(self, mocked_pub_leave):
        event_data = {
            'id': 1,
            'action': 'pubLeave',
            'publisher_id': 'pub1'
        }

        event_type = event_data['action']
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

    @patch('client_manager.service.ClientManager.publish_updated_controlflow')
    @patch('client_manager.service.ClientManager.send_query_matching_for_matcher')
    @patch('client_manager.service.ClientManager.send_query_window_for_window_manager')
    @patch('client_manager.service.ClientManager.update_bufferstreams_from_new_query')
    @patch('client_manager.service.ClientManager.create_query_dict')
    def test_add_query_should_properly_include_query_into_datastructure(
            self, mocked_query_dict, mocked_buffer, mocked_pub_ctrlflow, mocked_send_window, mocked_send_matcher):
        subscriber_id = 'sub1'
        query_id = 123
        query_dict = {
            'id': query_id,
            'from': ['test'],
            'content': ['ObjectDetection'],
            'etc': '...'
        }
        self.service.mocked_registry = MagicMock()
        self.service.mocked_registry.get_service_function_chain_by_content_type_list = MagicMock(
            return_value=['ObjectDetection'])

        mocked_query_dict.return_value = query_dict
        self.service.add_query_action(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

        mocked_query_dict.assert_called_once_with(subscriber_id, self.SIMPLE_QUERY_TEXT)
        mocked_buffer.assert_called_once_with(query=query_dict)
        mocked_pub_ctrlflow.assert_called_once_with(query=query_dict)
        mocked_send_window.assert_called_once_with(query=query_dict)
        mocked_send_matcher.assert_called_once_with(query=query_dict)
        self.assertIn(123, self.service.queries.keys())
        self.assertIn(query_dict, self.service.queries.values())

    # @patch('client_manager.service.ClientManager.create_query_dict')
    # def test_add_query_shouldn_include_duplicated_query(self, mocked_query_dict):
    #     subscriber_id = 'sub1'
    #     query_id = 123
    #     query_dict = {
    #         'id': query_id,
    #         'from': 'test',
    #         'content': ['ObjectDetection'],
    #         'window': 'window',
    #         'match': 'match',
    #         'where': 'where',
    #         'ret': 'ret',
    #         'etc': '...'
    #     }
    #     query_dict2 = {
    #         'id': query_id,
    #         'from': 'test',
    #         'content': ['ObjectDetection'],
    #         'window': 'window',
    #         'other': '...'
    #     }
    #     self.service.mocked_registry = MagicMock()
    #     self.service.mocked_registry.get_service_function_chain_by_content_type_list = MagicMock(
    #         return_value=['ObjectDetection'])

    #     mocked_query_dict.return_value = query_dict
    #     self.service.add_query_action(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

    #     mocked_query_dict.return_value = query_dict2
    #     self.service.add_query_action(subscriber_id, query_text=self.SIMPLE_QUERY_TEXT)

    #     self.assertIn(123, self.service.queries.keys())
    #     self.assertIn(query_dict, self.service.queries.values())
    #     self.assertNotIn(query_dict2, self.service.queries.values())

    # @patch('client_manager.service.ClientManager.update_bufferstreams_from_del_query')
    # @patch('client_manager.service.ClientManager.create_query_id')
    # def test_del_query_should_properly_remove_query_into_datastructure(self, mocked_query_id, mocked_buffer):
    #     subscriber_id = 'sub1'
    #     query_name = 'some query'
    #     query_id = 123
    #     query_dict = {'id': query_id, 'etc': '...'}
    #     self.service.queries = {
    #         query_id: query_dict,
    #         456: {'other': '...'}
    #     }

    #     mocked_query_id.return_value = query_id
    #     self.service.del_query_action(subscriber_id, query_name=query_name)

    #     mocked_query_id.assert_called_once_with(subscriber_id, query_name)
    #     mocked_buffer.assert_called_once_with(query_id)
    #     self.assertNotIn(123, self.service.queries.keys())
    #     self.assertNotIn(query_dict, self.service.queries.values())

    #     self.assertIn(456, self.service.queries.keys())
    #     self.assertIn({'other': '...'}, self.service.queries.values())

    # @patch('client_manager.service.ClientManager.create_query_id')
    # def test_del_query_should_ignore_deleting_nonexising_query(self, mocked_query_id):
    #     subscriber_id = 'sub1'
    #     query_name = 'some query'
    #     query_id = 123
    #     query_dict = {'id': query_id, 'etc': '...'}
    #     self.service.queries = {
    #         456: {'other': '...'}
    #     }

    #     mocked_query_id.return_value = query_id
    #     self.service.del_query_action(subscriber_id, query_name=query_name)

    #     mocked_query_id.assert_called_once_with(subscriber_id, query_name)
    #     self.assertNotIn(123, self.service.queries.keys())
    #     self.assertNotIn(query_dict, self.service.queries.values())

    #     self.assertIn(456, self.service.queries.keys())
    #     self.assertIn({'other': '...'}, self.service.queries.values())

    # def test_pub_join_should_properly_include_publisher_into_datastructure(self):

    #     publisher_id = 'pub1'
    #     source = 'http://etc.com',
    #     meta = {'fps': 30}
    #     publisher = {
    #         'id': publisher_id,
    #         'source': source,
    #         'meta': meta
    #     }
    #     self.service.pub_join_action(publisher_id, source, meta)

    #     self.assertIn(publisher_id, self.service.publishers.keys())
    #     self.assertIn(publisher, self.service.publishers.values())

    # def test_pub_leave_should_properly_remove_publisher_into_datastructure(self):
    #     publisher_id = 'pub1'
    #     source = 'http://etc.com',
    #     meta = {'fps': 30}
    #     publisher = {
    #         'id': publisher_id,
    #         'source': source,
    #         'meta': meta
    #     }
    #     self.service.pub_join_action(publisher_id, source, meta)
    #     self.service.pub_leave_action(publisher_id)

    #     self.assertNotIn(publisher_id, self.service.publishers.keys())
    #     self.assertNotIn(publisher, self.service.publishers.values())

    # @patch('client_manager.service.ClientManager.create_query_id')
    # def test_create_query_dict_parses_query_and_return_proper_dict(self, mocked_query_id):
    #     subscriber_id = 'sub_1'
    #     query_id = '123'
    #     expected_query_name = 'my_first_query'
    #     mocked_query_id.return_value = query_id
    #     query = self.service.create_query_dict(subscriber_id, self.SIMPLE_QUERY_TEXT)
    #     mocked_query_id.assert_called_once_with(subscriber_id, expected_query_name)
    #     self.assertIn('subscriber_id', query.keys())
    #     self.assertIn('id', query.keys())
    #     self.assertIn('name', query.keys())

    #     self.assertEqual(query['id'], query_id)
    #     self.assertEqual(query['subscriber_id'], subscriber_id)
    #     self.assertEqual(query['name'], expected_query_name)

    # def test_get_unique_buffer_hash(self):
    #     query_content = ['abc', 'dfg']
    #     publisher_id = 'pub_id1'
    #     resolution = "300x900"
    #     fps = "100"
    #     bufferstream_key = self.service.get_unique_buffer_hash(query_content, publisher_id, resolution, fps)
    #     excepted_key = '7a8cde7a97f51f561cda88d38df63caa'
    #     self.assertEqual(bufferstream_key, excepted_key)

    # @patch('client_manager.service.ClientManager.send_start_preprocessor_action')
    # @patch('client_manager.service.ClientManager.send_add_buffer_stream_key_to_event_dispatcher')
    # @patch('client_manager.service.ClientManager.get_unique_buffer_hash')
    # def test_update_bufferstreams_from_new_query_should_update_bufferstreams(
    #         self, mocked_unique_buff, mocked_send_b, mocked_send_p):
    #     query_id = '123'
    #     bufferstream_key = 'bufferstream-key'
    #     mocked_unique_buff.return_value = bufferstream_key
    #     self.service.publishers = {
    #         'pub1': {
    #             'meta': {
    #                 'fps': '30',
    #                 'resolution': '300x300',
    #             },
    #             'source': 'abc',
    #         }
    #     }
    #     query = {
    #         'id': query_id,
    #         'from': ['pub1'],
    #         'content': ['ObjectDetection', 'ColorDetection']
    #     }

    #     self.service.update_bufferstreams_from_new_query(query)
    #     mocked_send_b.assert_called_once_with('pub1', bufferstream_key)
    #     mocked_send_p.assert_called_once_with('pub1', 'abc', '300x300', '30', ['123'], 'bufferstream-key')
    #     self.assertIn(bufferstream_key, self.service.buffer_hash_to_query_map)
    #     self.assertEqual(self.service.buffer_hash_to_query_map[bufferstream_key], set({query_id}))

    # @patch('client_manager.service.ClientManager.send_start_preprocessor_action')
    # @patch('client_manager.service.ClientManager.send_add_buffer_stream_key_to_event_dispatcher')
    # @patch('client_manager.service.ClientManager.get_unique_buffer_hash')
    # def test_update_bufferstreams_from_new_query_should_not_update_bufferstreams_if_no_pub(
    #         self, mocked_unique_buff, mocked_send_b, mocked_send_p):
    #     query_id = '123'
    #     bufferstream_key = 'bufferstream-key'
    #     mocked_unique_buff.return_value = bufferstream_key
    #     self.service.publishers = {
    #         'pub1': {
    #             'meta': {
    #                 'fps': '30',
    #                 'resolution': '300x300',
    #             }
    #         }
    #     }
    #     query = {
    #         'id': query_id,
    #         'from': ['pub2']
    #     }

    #     self.service.update_bufferstreams_from_new_query(query)
    #     self.assertFalse(mocked_send_b.called)
    #     self.assertFalse(mocked_send_p.called)
    #     self.assertFalse(mocked_unique_buff.called)
    #     self.assertNotIn(bufferstream_key, self.service.buffer_hash_to_query_map)

    # @patch('client_manager.service.ClientManager.send_stop_preprocessor_action')
    # @patch('client_manager.service.ClientManager.send_del_buffer_stream_key_to_event_dispatcher')
    # def test_update_bufferstreams_from_del_query_should_update_bufferstreams(self, mocked_send_b, mocked_send_p):
    #     query_id = '123'
    #     bufferstream_key = 'bufferstream-key'
    #     self.service.buffer_hash_to_query_map = {bufferstream_key: set({query_id})}

    #     self.service.update_bufferstreams_from_del_query(query_id)
    #     self.assertDictEqual(
    #         self.service.buffer_hash_to_query_map,
    #         {}
    #     )
    #     mocked_send_b.assert_called_once_with(bufferstream_key)
    #     mocked_send_p.assert_called_once_with(bufferstream_key)

    # @patch('client_manager.service.ClientManager.send_stop_preprocessor_action')
    # @patch('client_manager.service.ClientManager.send_del_buffer_stream_key_to_event_dispatcher')
    # def test_update_bufferstreams_from_del_query_shouldn_remove_bufferstream_if_not_empty(
    #         self, mocked_send_b, mocked_send_p):
    #     query_id = '123'
    #     bufferstream_key = 'bufferstream-key'
    #     self.service.buffer_hash_to_query_map = {bufferstream_key: set({query_id, 'query_2'})}

    #     self.service.update_bufferstreams_from_del_query(query_id)
    #     self.assertDictEqual(
    #         self.service.buffer_hash_to_query_map,
    #         {bufferstream_key: set({'query_2'})}
    #     )

    #     self.assertFalse(mocked_send_b.called)
    #     self.assertFalse(mocked_send_p.called)

    # @patch('client_manager.service.ClientManager.service_based_random_event_id')
    # @patch('client_manager.service.ClientManager.write_event_with_trace')
    # def test_send_query_window_for_window_manager(self, mocked_write_event, mocked_rand_id):
    #     mocked_rand_id.return_value = 123
    #     query_id = '123'
    #     query = {
    #         'id': query_id,
    #         'from': ['pub1'],
    #         'content': ['ObjectDetection', 'ColorDetection'],
    #         'window': {
    #             'window_type': 'TUMBLING_COUNT_WINDOW',
    #             'args': [2]
    #         }
    #     }
    #     self.service.send_query_window_for_window_manager(query=query)
    #     mocked_write_event.assert_called_once_with(
    #         {
    #             'id': 123,
    #             'action': 'addQueryWindow',
    #             'window': query['window'],
    #             'query_id': query_id
    #         },
    #         self.service.window_manager_cmd
    #     )
