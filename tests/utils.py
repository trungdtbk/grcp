import docker
import time, os, itertools
import ipaddress
import numpy as np

from random import randint
from numpy.random import choice

neo4j_name = 'test.grcp.neo4j'

def start_neo4j():
    print('----- Start neo4j on docker ------')
    docker_cli = docker.from_env()
    try:
        neo4j = docker_cli.containers.get(neo4j_name)
        neo4j.stop()
        neo4j.remove()
    except:
        pass

    neo4j = docker_cli.containers.run(
                image="neo4j:3.4", name=neo4j_name,
                ports={"7687": "7687", "7474": "7474"},
                environment={'NEO4J_AUTH': 'none'}, detach=True)
    for line in neo4j.logs(stream=True, follow=True):
        if 'Started' in str(line):
            print('neo4j started')
            time.sleep(5)
            break

def stop_neo4j():
    print('----- Stop neo4j docker ------')
    try:
        neo4j = docker_cli.containers.get(neo4j_name)
        neo4j.stop()
        neo4j.remove()
    except:
        pass

def random_ip():
    return '%d.%d.%d.%d' % (
            randint(0,255), randint(0, 255), randint(0, 255), randint(1, 250))

m = 1<<32
def random_prefix():
    prefix = randint(1, m)
    return str(ipaddress.ip_network(prefix, strict=False))

def random_as_path(max_length=5):
    as_path = []
    for _ in range(randint(0, max_length)):
        as_path.append(randint(1, 64999))
    return as_path

def random_weight():
    return int(np.random.uniform()*10)
