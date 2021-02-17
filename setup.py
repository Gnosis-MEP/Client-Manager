from setuptools import setup

setup(
    name='client_manager',
    version='0.1',
    description='Service responsible for handling client related interactoions, such as with Publishers and Subscribers. It also manages any information related to entities from this interactions, such as queries, and it is responsible for providing internal representation of these entities to be used inside the system.',
    author='Felipe Arruda Pontes',
    author_email='felipe.arruda.pontes@insight-centre.org',
    packages=['client_manager'],
    zip_safe=False
)
