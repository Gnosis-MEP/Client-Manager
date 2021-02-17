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
    MATCH (c1:Car {color:'blue'} AND c2:Car {color:'white', })
    FROM *
    WITHIN TUMBLING_COUNT_WINDOW(2)
    RETURN *
    """

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
        mocked_add_query.assert_called_once_with(subscriber_id=event_data['subscriber_id'], query=event_data['query'])

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
