import hashlib
import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer
from gnosis_epl.main import QueryParser


class ClientManager(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 service_registry,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(ClientManager, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )

        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']

        self.query_parser = QueryParser()

        self.queries = {}
        self.buffer_hash_to_query_map = {}
        self.publishers = {}

        self.service_registry = service_registry

    def publish_query_created(self, query):
        new_event_data = query.copy()
        new_event_data['id'] = self.service_based_random_event_id()
        self.publish_event_type_to_stream(event_type='QueryCreated', new_event_data=new_event_data)

    def publish_query_removed(self, query):
        new_event_data = query.copy()
        new_event_data['id'] = self.service_based_random_event_id()
        new_event_data['deleted'] = True
        self.publish_event_type_to_stream(event_type='QueryRemoved', new_event_data=new_event_data)

    def get_unique_buffer_hash(self, query_content, publisher_id, resolution, fps):
        keys_list = tuple(query_content) + tuple([publisher_id, resolution, fps])
        unhashed_key = '-'.join(keys_list)
        hash_key = hashlib.md5(unhashed_key.encode())
        return hash_key.hexdigest()

    def create_query_id(self, subscriber_id, query_name):
        key = f'{subscriber_id}_{query_name}'
        query_id = hashlib.md5(key.encode('utf-8')).hexdigest()
        return query_id

    def create_query_dict(self, query_received_event_id, subscriber_id, query_text):
        parsed_query = self.query_parser.parse(query_text)
        query_id = self.create_query_id(subscriber_id, parsed_query['name'])

        query = {
            'subscriber_id': subscriber_id,
            'query_id': query_id,
            'parsed_query': {
                'name': parsed_query['name'],
                'from': parsed_query['from'],
                'content': parsed_query['content'],
                'match': parsed_query['match'],
                'optional_match': parsed_query.get('optional_match', ''),
                'where': parsed_query.get('where', ''),
                'window': parsed_query['window'],
                'ret': parsed_query['ret'],
                'qos_policies': parsed_query.get('qos_policies', {}),
                # 'cypher_query': query['cypher_query'],
            },
            'query_received_event_id': query_received_event_id
        }
        publisher_id = query['parsed_query']['from'][0]
        buffer_stream_dict = self.generate_query_bufferstream_dict(query)
        if buffer_stream_dict is None:
            self.logger.info(f'Publisher id {publisher_id} not available. Will not process Query {query}')
            return

        query['buffer_stream'] = buffer_stream_dict
        query['service_chain'] = self.generate_query_service_chain(query)
        return query

    def generate_query_service_chain(self, query):
        content_types = query['parsed_query']['content']
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

    def process_query_received(self, query_received_event_id, subscriber_id, query_text):
        query = self.create_query_dict(query_received_event_id, subscriber_id, query_text)
        if query is not None:
            if query['query_id'] not in self.queries.keys():
                self.queries[query['query_id']] = query
                self.publish_query_created(query=query)
                self.update_bufferstreams_from_new_query(query=query)
            else:
                self.logger.info('Ignoring duplicated query addition')

    def process_query_deletion_requested(self, subscriber_id, query_name):
        query_id = self.create_query_id(subscriber_id, query_name)

        query = self.queries.pop(query_id, None)
        if query is None:
            self.logger.info('Ignoring removal of non-existing query')
        else:
            self.publish_query_removed(query=query)
            self.update_bufferstreams_from_del_query(query_id)

    def process_publisher_created(self, publisher_id, source, meta):
        if publisher_id not in self.publishers.keys():
            self.publishers[publisher_id] = {
                'id': publisher_id,
                'source': source,
                'meta': meta,
            }
        else:
            self.logger.info('Ignoring duplicated publisher incluson')

    def process_publisher_removed(self, publisher_id):
        publisher = self.publishers.pop(publisher_id, None)
        if publisher is None:
            self.logger.info('Ignoring removal of non-existing publisher')

    def process_service_worker_announced(self, worker):
        service_type = worker['service_type']
        stream_key = worker['stream_key']
        service_dict = self.service_registry.available_services.setdefault(service_type, {'workers': {}})
        service_dict['workers'][stream_key] = worker

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(ClientManager, self).process_event_type(event_type, event_data, json_msg):
            return False

        if event_type == 'QueryReceived':
            self.process_query_received(
                query_received_event_id=event_data['id'],
                subscriber_id=event_data['subscriber_id'],
                query_text=event_data['query'],
            )
        elif event_type == 'QueryDeletionRequested':
            self.process_query_deletion_requested(
                subscriber_id=event_data['subscriber_id'],
                query_name=event_data['query_name']
            )
        elif event_type == 'PublisherCreated':
            self.process_publisher_created(
                publisher_id=event_data['publisher_id'],
                source=event_data['source'],
                meta=event_data['meta']
            )
        elif event_type == 'PublisherRemoved':
            self.process_publisher_removed(
                publisher_id=event_data['publisher_id']
            )

        elif event_type == 'ServiceWorkerAnnounced':
            self.process_service_worker_announced(
                worker=event_data['worker']
            )

    def log_state(self):
        super(ClientManager, self).log_state()
        self._log_dict('Publishers', self.publishers)
        self._log_dict('Queries', self.queries)
        self._log_dict('Bufferstreams', self.buffer_hash_to_query_map)
        self._log_dict('Available Services', self.service_registry.available_services)

    def run(self):
        super(ClientManager, self).run()
        self.run_forever(self.process_cmd)
