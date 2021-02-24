import hashlib
import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService
from event_service_utils.tracing.jaeger import init_tracer
from gnosis_epl.main import QueryParser


class ClientManager(BaseTracerService):
    def __init__(self,
                 service_stream_key, service_cmd_key,
                 stream_factory,
                 preprocessor_cmd_key,
                 event_dispatcher_cmd_key,
                 adaptation_planner_cmd_key,
                 window_manager_cmd_key,
                 matcher_cmd_key,
                 forwarder_cmd_key,
                 mocked_registry,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(ClientManager, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id']
        self.preprocessor_cmd = self.stream_factory.create(key=preprocessor_cmd_key, stype='streamOnly')
        self.event_dispatcher_cmd = self.stream_factory.create(key=event_dispatcher_cmd_key, stype='streamOnly')
        self.adaptation_planner_cmd = self.stream_factory.create(key=adaptation_planner_cmd_key, stype='streamOnly')
        self.matcher_cmd = self.stream_factory.create(key=matcher_cmd_key, stype='streamOnly')
        self.forwarder_cmd = self.stream_factory.create(key=forwarder_cmd_key, stype='streamOnly')
        self.window_manager_cmd = self.stream_factory.create(key=window_manager_cmd_key, stype='streamOnly')

        self.query_parser = QueryParser()

        self.queries = {}
        self.buffer_hash_to_query_map = {}
        self.publishers = {}

        self.mocked_registry = mocked_registry

    def send_start_preprocessor_action(self, publisher_id, source, resolution, fps, query_ids, buffer_stream_key):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'startPreprocessing',
            'publisher_id': publisher_id,
            'source': source,
            'resolution': resolution,
            'fps': fps,
            'buffer_stream_key': buffer_stream_key,
            'query_ids': query_ids
        }
        self.logger.info(f'Sending "startPreprocessing" action: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.preprocessor_cmd)

    def send_stop_preprocessor_action(self, buffer_stream_key):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'stopPreprocessing',
            'buffer_stream_key': buffer_stream_key
        }
        self.logger.info(f'Sending "stopPreprocessing" action: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.preprocessor_cmd)

    def send_add_buffer_stream_key_to_event_dispatcher(self, publisher_id, buffer_stream_key):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'addBufferStreamKey',
            'publisher_id': publisher_id,
            'buffer_stream_key': buffer_stream_key
        }
        self.logger.info(f'Sending "addBufferStreamKey" action: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.event_dispatcher_cmd)

    def send_del_buffer_stream_key_to_event_dispatcher(self, buffer_stream_key):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'delBufferStreamKey',
            'buffer_stream_key': buffer_stream_key
        }
        self.logger.info(f'Sending "delBufferStreamKey" action: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.event_dispatcher_cmd)

    def send_update_controlflow_for_adaptation_monitor(self, query):
        content_types = query['content']
        publisher_id = query['from'][0]
        service_function_chain = self.mocked_registry.get_service_function_chain_by_content_type_list(content_types)
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'updateControlFlow',
            'data_flow': service_function_chain,
            'qos_policies': {'accuracy': 'min', 'latency': 'max'},
            'query_id': query['id'],
            'publisher_id': publisher_id,
        }

        self.logger.info(f'Sending "updateControlFlow" action: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.adaptation_planner_cmd)

    def get_unique_buffer_hash(self, query_content, publisher_id, resolution, fps):
        keys_list = tuple(query_content) + tuple([publisher_id, resolution, fps])
        unhashed_key = '-'.join(keys_list)
        hash_key = hashlib.md5(unhashed_key.encode())
        return hash_key.hexdigest()

    def create_query_id(self, subscriber_id, query_name):
        key = f'{subscriber_id}_{query_name}'
        query_id = hashlib.md5(key.encode('utf-8')).hexdigest()
        return query_id

    def create_query_dict(self, subscriber_id, query_text):
        query = self.query_parser.parse(query_text)
        query_id = self.create_query_id(subscriber_id, query['name'])
        query['id'] = query_id
        query['subscriber_id'] = subscriber_id
        return query

    def update_bufferstreams_from_new_query(self, query):
        publisher_id = query['from'][0]
        publisher = self.publishers.get(publisher_id, None)
        if publisher is None:
            self.logger.info(f'Publisher id {publisher_id} not available. Will not process Query {query}')
            return
        source = publisher['source']
        resolution = publisher['meta']['resolution']
        fps = publisher['meta']['fps']
        query_content = query['content']
        buffer_hash = self.get_unique_buffer_hash(
            query_content, publisher_id, resolution, fps
        )
        buffer_query_set = self.buffer_hash_to_query_map.setdefault(buffer_hash, set())
        query_set_was_empty = len(buffer_query_set) == 0
        query_id = query['id']
        buffer_query_set.add(query_id)
        if query_set_was_empty:
            self.send_start_preprocessor_action(
                publisher_id, source, resolution, fps, list(buffer_query_set), buffer_hash)
            self.send_add_buffer_stream_key_to_event_dispatcher(publisher_id, buffer_hash)

    def send_query_matching_for_matcher(self, query):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'addQueryMatching',
            'query_id': query['id'],
            'match': query['match'],
            'optional_match': query.get('optional_match', ''),
            'where': query.get('where', ''),
            'ret': query['ret'],
        }

        self.logger.info(f'Sending "addQueryMatching" action: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.matcher_cmd)

    def send_query_window_for_window_manager(self, query):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'addQueryWindow',
            'query_id': query['id'],
            'window': query['window'],
        }

        self.logger.info(f'Sending "addQueryWindow" action: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.window_manager_cmd)

    def update_bufferstreams_from_del_query(self, query_id):
        buffer_to_remove = None
        for buffer_hash, query_set in self.buffer_hash_to_query_map.items():
            if query_id in query_set:
                query_set.remove(query_id)
                if len(query_set) == 0:
                    buffer_to_remove = buffer_hash
                    break
        if buffer_to_remove:
            del self.buffer_hash_to_query_map[buffer_to_remove]
            self.send_stop_preprocessor_action(buffer_to_remove)
            self.send_del_buffer_stream_key_to_event_dispatcher(buffer_to_remove)

    def add_query_action(self, subscriber_id, query_text):
        query = self.create_query_dict(subscriber_id, query_text)
        if query['id'] not in self.queries.keys():
            self.queries[query['id']] = query
            self.send_update_controlflow_for_adaptation_monitor(query=query)
            self.send_query_matching_for_matcher(query=query)
            self.send_query_window_for_window_manager(query=query)
            self.update_bufferstreams_from_new_query(query=query)
        else:
            self.logger.info('Ignoring duplicated query addition')

    def del_query_action(self, subscriber_id, query_name):
        query_id = self.create_query_id(subscriber_id, query_name)

        query = self.queries.pop(query_id, None)
        if query is None:
            self.logger.info('Ignoring removal of non-existing query')
        else:
            self.update_bufferstreams_from_del_query(query_id)

    def pub_join_action(self, publisher_id, source, meta):
        if publisher_id not in self.publishers.keys():
            self.publishers[publisher_id] = {
                'id': publisher_id,
                'source': source,
                'meta': meta,
            }
        else:
            self.logger.info('Ignoring duplicated publisher incluson')

    def pub_leave_action(self, publisher_id):
        publisher = self.publishers.pop(publisher_id, None)
        if publisher is None:
            self.logger.info('Ignoring removal of non-existing publisher')

    def add_service_worker_action(self, worker):
        service_type = worker['service_type']
        stream_key = worker['stream_key']
        service_dict = self.mocked_registry.available_services.setdefault(service_type, {'workers': {}})
        service_dict['workers'][stream_key] = worker

    def process_action(self, action, event_data, json_msg):
        if not super(ClientManager, self).process_action(action, event_data, json_msg):
            return False
        if action == 'addQuery':
            self.add_query_action(
                subscriber_id=event_data['subscriber_id'],
                query_text=event_data['query']
            )
        elif action == 'delQuery':
            self.del_query_action(
                subscriber_id=event_data['subscriber_id'],
                query_name=event_data['query_name']
            )
        elif action == 'pubJoin':
            self.pub_join_action(
                publisher_id=event_data['publisher_id'],
                source=event_data['source'],
                meta=event_data['meta']
            )
        elif action == 'pubLeave':
            self.pub_leave_action(
                publisher_id=event_data['publisher_id']
            )

        elif action == 'addServiceWorker':
            self.add_service_worker_action(event_data['worker'])

    def log_state(self):
        super(ClientManager, self).log_state()
        self._log_dict('Publishers', self.publishers)
        self._log_dict('Queries', self.queries)
        self._log_dict('Bufferstreams', self.buffer_hash_to_query_map)
        self._log_dict('Available Services', self.mocked_registry.available_services)

    def run(self):
        super(ClientManager, self).run()
        self.run_forever(self.process_cmd)
