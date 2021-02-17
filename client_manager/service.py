import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService
from event_service_utils.tracing.jaeger import init_tracer


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

    # def send_event_to_somewhere(self, event_data):
    #     self.logger.debug(f'Sending event to somewhere: {event_data}')
    #     self.write_event_with_trace(event_data, self.somewhere_stream)

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(ClientManager, self).process_data_event(event_data, json_msg):
            return False
        # do something here
        pass

    def add_query_action(self, subscriber_id, query):
        pass

    def del_query_action(self, subscriber_id, query_name):
        pass

    def pub_join_action(self, publisher_id, source, meta):
        pass

    def pub_leave_action(self, publisher_id):
        pass

    def process_action(self, action, event_data, json_msg):
        if not super(ClientManager, self).process_action(action, event_data, json_msg):
            return False
        if action == 'addQuery':
            self.add_query_action(
                subscriber_id=event_data['subscriber_id'],
                query=event_data['query']
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
        self.logger.info(f'My service name is: {self.name}')

    def run(self):
        super(ClientManager, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
