class MockedRegistry():

    def __init__(self, services_dict):
        self.available_services = services_dict

    def get_service_function_chain_by_content_type_list(self, content_types):
        service_type_list = []
        for service_type, service_data in self.available_services.items():
            if any([ct in service_data['content_type'] for ct in content_types]):
                service_type_list.append(service_type)

        return service_type_list