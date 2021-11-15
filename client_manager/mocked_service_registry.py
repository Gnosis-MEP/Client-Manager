class ServiceRegistry():

    def __init__(self):
        self.available_services = {}
        # self.available_services = {'ObjectDetection': [], 'ColorDetection': []}

    def get_service_function_chain_by_content_type_list(self, content_types):
        service_type_list = []
        for ct in content_types:
            # hack, just to make it work in a similar data type as it is right now
            if ct in self.available_services.keys():
                service_type_list.append(ct)
            # for service_type, service_data in self.available_services.items():
                # for worker_stream, worker in service_data.items():
                #     if ct in worker['content_types']:
                #         service_type_list.append(service_type)
                #         break
        return service_type_list
