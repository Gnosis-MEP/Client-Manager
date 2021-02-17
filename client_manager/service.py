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

        self.query_parser = QueryParser()

        self.queries = {}
        self.buffer_hash_to_query_map = {}
        self.publishers = {}

    # def send_event_to_somewhere(self, event_data):
    #     self.logger.debug(f'Sending event to somewhere: {event_data}')
    #     self.write_event_with_trace(event_data, self.somewhere_stream)

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(ClientManager, self).process_data_event(event_data, json_msg):
            return False
        # do something here
        pass

    def get_unique_buffer_hash(self, publisher_id, resolution, fps):
        unhashed_key = '-'.join([publisher_id, resolution, fps])
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
        buffer_hash = self.get_unique_buffer_hash(
            publisher_id, publisher['meta']['resolution'], publisher['meta']['fps']
        )
        buffer_query_set = self.buffer_hash_to_query_map.setdefault(buffer_hash, set())
        query_set_was_empty = len(buffer_query_set) == 0
        query_id = query['id']
        buffer_query_set.add(query_id)
        if query_set_was_empty:
            # self.send_start_preprocessor_action(
            #     publisher_id, source, resolution, fps, list(buffer_query_set), buffer_hash)
            # self.send_add_buffer_stream_key_to_event_dispatcher(publisher_id, buffer_hash)
            pass

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
            # self.send_stop_preprocessor_action(buffer_to_remove)
            # self.send_del_buffer_stream_key_to_event_dispatcher(buffer_hash)

    def add_query_action(self, subscriber_id, query_text):
        query = self.create_query_dict(subscriber_id, query_text)
        if query['id'] not in self.queries.keys():
            self.queries[query['id']] = query
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

    def log_state(self):
        super(ClientManager, self).log_state()
        self._log_dict('Publishers', self.publishers)
        self._log_dict('Queries', self.queries)
        self._log_dict('Bufferstreams', self.buffer_hash_to_query_map)

    def run(self):
        super(ClientManager, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
