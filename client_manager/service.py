import hashlib
import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer
from gnosis_epl.main import QueryParser


class ClientManager(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 service_registry_cmd_key, service_details,
                 stream_factory,
                 service_registry,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(ClientManager, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            service_registry_cmd_key=service_registry_cmd_key,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.pub_stream_registered_query_entity = self.stream_factory.create(key='RegisteredQuery', stype='streamOnly')

        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id']

        self.query_parser = QueryParser()

        self.queries = {}
        self.buffer_hash_to_query_map = {}
        self.publishers = {}

        self.service_registry = service_registry

    def publish_registered_query_entity_del(self, query):
        new_event_data = query.copy()
        new_event_data['id'] = self.service_based_random_event_id()
        new_event_data['deleted'] = True

        self.logger.info(f'Publishing "RegisteredQuery" deletion entity: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.pub_stream_registered_query_entity)

    def publish_registered_query_entity(self, query):
        new_event_data = query.copy()
        new_event_data['id'] = self.service_based_random_event_id()

        self.logger.info(f'Publishing "RegisteredQuery" entity: {new_event_data}')
        self.write_event_with_trace(new_event_data, self.pub_stream_registered_query_entity)

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
        publisher_id = query['from'][0]
        buffer_stream_dict = self.generate_query_bufferstream_dict(query)
        if buffer_stream_dict is None:
            self.logger.info(f'Publisher id {publisher_id} not available. Will not process Query {query}')
            return

        service_chain = self.generate_query_service_chain(query)
        registered_query = {
            'subscriber_id': query['subscriber_id'],
            'query_id': query_id,
            'parsed_query': {
                'from': query['from'],
                'content': query['content'],
                'window': query['window'],
                'qos_policies': query.get('qos_policies', {}),
                # 'cypher_query': query['cypher_query'],
            },
            'buffer_stream': buffer_stream_dict,
            'service_chain': service_chain,
        }
        return registered_query

    def generate_query_service_chain(self, query):
        content_types = query['content']
        service_function_chain = self.service_registry.get_service_function_chain_by_content_type_list(content_types)
        return service_function_chain

    def generate_query_bufferstream_dict(self, query):
        publisher_id = query['parsed_query']['from'][0]
        publisher = self.publishers.get(publisher_id, None)
        if publisher is None:
            return

        source = publisher['source']
        resolution = publisher['meta']['resolution']
        fps = publisher['meta']['fps']
        query_content = query['parsed_query']['content']
        buffer_stream_key = self.get_unique_buffer_hash(
            query_content, publisher_id, resolution, fps
        )
        return {
            'publisher_id': publisher_id,
            'buffer_stream_key': buffer_stream_key,
            'source': source,
            'resolution': resolution,
            'fps': fps,
        }

    def update_bufferstreams_from_new_query(self, query):
        buffer_stream_key = query['buffer_stream']['buffer_stream_key']
        buffer_query_set = self.buffer_hash_to_query_map.setdefault(buffer_stream_key, set())
        query_id = query['query_id']
        buffer_query_set.add(query_id)

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

    def add_query_action(self, subscriber_id, query_text):
        query = self.create_query_dict(subscriber_id, query_text)
        if query is not None:
            if query['query_id'] not in self.queries.keys():
                self.queries[query['query_id']] = query
                self.publish_registered_query_entity(query=query)
                self.update_bufferstreams_from_new_query(query=query)
            else:
                self.logger.info('Ignoring duplicated query addition')

    def del_query_action(self, subscriber_id, query_name):
        query_id = self.create_query_id(subscriber_id, query_name)

        query = self.queries.pop(query_id, None)
        if query is None:
            self.logger.info('Ignoring removal of non-existing query')
        else:
            self.publish_registered_query_entity_del(query=query)
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
        service_dict = self.service_registry.available_services.setdefault(service_type, {'workers': {}})
        service_dict['workers'][stream_key] = worker

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(ClientManager, self).process_event_type(event_type, event_data, json_msg):
            return False
        if event_type == 'addQuery':
            self.add_query_action(
                subscriber_id=event_data['subscriber_id'],
                query_text=event_data['query']
            )
        elif event_type == 'delQuery':
            self.del_query_action(
                subscriber_id=event_data['subscriber_id'],
                query_name=event_data['query_name']
            )
        elif event_type == 'pubJoin':
            self.pub_join_action(
                publisher_id=event_data['publisher_id'],
                source=event_data['source'],
                meta=event_data['meta']
            )
        elif event_type == 'pubLeave':
            self.pub_leave_action(
                publisher_id=event_data['publisher_id']
            )

        elif event_type == 'addServiceWorker':
            self.add_service_worker_action(event_data['worker'])

    def log_state(self):
        super(ClientManager, self).log_state()
        self._log_dict('Publishers', self.publishers)
        self._log_dict('Queries', self.queries)
        self._log_dict('Bufferstreams', self.buffer_hash_to_query_map)
        self._log_dict('Available Services', self.service_registry.available_services)

    def run(self):
        super(ClientManager, self).run()
        self.run_forever(self.process_cmd)
