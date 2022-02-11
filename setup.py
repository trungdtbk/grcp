import os
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name = 'grcp',
    version = '0.0.1',
    author = 'Trung Truong',
    author_email = 'trungdtbk@gmail.com',
    description = ("A control platform for programmable BGP routing."),
    license = 'BSD',
    long_description = read('README.rst'),
    classifiers = [
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Networking :: Routing :: BGP",
        "Programming Language :: Python",
        "License :: BSD License",
        ],
    entry_points = {
        'console_scripts': [
            'grcp=grcp.grcp:main',
            ],
        },
    packages=find_packages(exclude=['test']),
    install_requires=[
            'neo4j-driver',
            'twisted==22.1.0',
            'oslo.config',
            ]
    )
