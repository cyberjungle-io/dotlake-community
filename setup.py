from setuptools import setup, find_packages

setup(
    name="dotlake-indexer",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pyyaml==6.0.1',
        'python-dotenv==1.0.0',
        'psycopg2-binary==2.9.9',
        'sqlalchemy==2.0.23',
        'substrate-interface==1.7.4',
        'websockets==11.0.3',
        'requests==2.31.0',
        'python-dateutil==2.8.2',
        'tenacity==8.2.3',
        'prometheus-client==0.19.0'
    ],
) 