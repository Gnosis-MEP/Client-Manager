class MockedRegistry():

    def __init__(self, services_dict):
        self.available_services = services_dict

    def get_services_type_by_content_type(self, content_type):
        service_type_list = []
        for service_type, service_data in self.available_services.items():
            if content_type in service_data['content_type']:
                service_type_list.append(service_type)

        return service_type_list
