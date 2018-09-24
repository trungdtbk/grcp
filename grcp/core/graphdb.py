"""
This module provides interfaces to a Graph database. Cypher query language amd Meo4J
database are currently supported.

To use create an instance of Neo4J class (assume a database at the URI is already
running.

The method exec_query() can be used to run any Cypher queries. Methods including
create_node, delete_node, create_link, delete_link and query_paths are designed
specifically to use for a network topology management.

This module is used by the model module which provides an abstraction to BGP routing.
"""
import os
import time
import json
import logging

DEFAULT_DB_URI = os.environ.get('DB_URI', 'bolt://localhost:7687')
DEFAULT_DB_USER = os.environ.get('DB_USER', 'neo4j')
DEFAULT_DB_PASS = os.environ.get('DB_PASS', 'neo4j')

class GraphDB():

    SUPPORTED_LINK_ATTRS = [
            'aspath_len', 'delay', 'weight', 'bandwidth', 'loss', 'med', 'origin', 'cost']
    SUPPORTED_SORTS = ['asc', 'desc']


class Neo4J(GraphDB):
    """Represent a Neo4j database. An instance of this class to be used to run query against
    the database.
    """

    from neo4j.v1 import GraphDatabase
    from neo4j.v1.types.graph import Node, Relationship

    def __init__(self, db_uri=None, db_user=None, db_pass=None):
        uri = db_uri or DEFAULT_DB_URI
        username = db_user or DEFAULT_DB_USER
        password = db_pass or DEFAULT_DB_PASS
        self.driver = None
        for _ in range(120):
            try:
                self.driver = self.GraphDatabase.driver(uri, auth=(username, password))
            except:
                time.sleep(1)
        if not self.driver:
            print('Failed to connect to Neo4j')

    @classmethod
    def _dict_to_cypher(cls, d):
        """convert a dict into cypher-compliant string.
        Ex: {"label": "A", "cost": 2, "name": "R1"} -> '{:A {cost: 2, name:"R"}'
        """
        kind = d.pop('label', None)
        s = ''
        for k, v in d.items():
            if isinstance(v, str):
                s += '%s: "%s", ' % (k, v)
            else:
                s += '%s: %s, ' % (k, v)
        s = s[:-2]
        if s:
            s = '{ %s }' % s
        if kind:
            s = ':%s %s' % (kind, s)
        return s

    def exec_query(self, query, **params):
        """Run a Cypher query.
        Eg: exec_query("MATCH (n) WHERE n.name=$name RETURN n", name='R')
        """
        if not query:
            return []
        with self.driver.session() as session:
            return session.run(query, params)
        return []

    def create_constraint(self, kind, prop):
        """Make sure that each node with kind has unique property prop."""
        qry = 'CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE' % (kind, prop)
        self.exec_query(qry)

    def clear_db(self):
        """Clear everything from the database."""
        self.delete_node()

    def delete_node(self, label=None, filters=None):
        """Delete a node from the database. Leave out labels and filters to delete everything
        labels (list) to define kinds of the node
        filters (dict) a set of property names and values to match on.
        Ex:
        delete_node('Peer', {'name': 'R'}) to delete all Peer nodes with name = R
        delete_node(filters={'name': 'R'}) to delete all nodes with name = R
        """
        qry = ''
        kind = label or ''
        filter_str = ''
        if kind:
            kind = ':' + kind
        if filters:
            filter_str = self._dict_to_cypher(filters)
        qry = 'MATCH ( n {kind} {filter_str} ) DETACH DELETE n RETURN n;'
        records = self.exec_query(qry.format(kind=kind, filter_str=filter_str))
        return records

    def create_node(self, labels, properties={}):
        """Create a new node with labels as node kind and properties. If a 'uid' in
        the properties dict and a node with the same uid exist, it will be updated with
        the new properties.
        """
        if isinstance(labels, list):
            kind = ":".join(labels)
        else:
            kind = ':' % labels
        uid = self._dict_to_cypher({'uid': properties.pop('uid', -1)})
        qry = 'MERGE ( node:{kind} {uid} ) '\
              'ON MATCH SET node=$properties, node.uid=id(node) '\
              'ON CREATE SET node=$properties, node.uid=id(node) '\
              'RETURN id(node) AS uid'
        records = list(self.exec_query(
                        qry.format(kind=kind, uid=uid), properties=properties))
        if records:
            return records[0]['uid']
        return None

    def count_outgoing_edges(self, src, edge=None):
        """Count outgoing edges from this node. src is a dict describe the node, used
        for filter nodes. """
        if edge:
            edge = ':' + edge
        else:
            edge = ''
        src_str = self._dict_to_cypher(src)
        qry = 'MATCH (n {src})-[e {edge}]->() RETURN COUNT(e) as cnt'
        records = list(self.exec_query(qry.format(src=src_str, edge=edge)))
        if records:
            return records[0]['cnt']
        return None

    def create_link(self, label, src, dst, properties={}, create_dst=False):
        """create a link between a src Node and a dst Node. src node must exist.
        Argument create_dst can be used to turn on/off dst node condition.
        Args:
            src (dict): properties of src Node.
            dst (dict): properties of dst Node.
        """
        src_str = self._dict_to_cypher(src)
        dst_kind = dst.pop('label', '')
        dst_kinds = dst.pop('labels', [])
        dst_str = self._dict_to_cypher(dst)
        if create_dst:
            dst_kind = ': '.join(dst_kinds)
            if dst_kind:
                dst_kind = ':%s' % dst_kind
            qry = 'MATCH ( peer {src} ) '\
                  'MERGE (peer)-[link:{label}]->(prefix {dst_kind} {dst}) '\
                  'ON MATCH SET link = $properties, link.uid = id(link) '\
                  'ON CREATE SET link=$properties, link.uid=id(link), prefix.uid=id(prefix) '\
                  'RETURN link.uid as uid'
        else:
            if dst_kind:
                dst_kind = ':%s' % dst_kind
            qry = 'MATCH ( peer {src} ), ( prefix {dst_kind} {dst} ) '\
                  'MERGE (peer)-[link:{label}]->(prefix) '\
                  'SET link= $properties, link.uid = id(link) RETURN link.uid as uid'
        records = list(self.exec_query(
                qry.format(
                    src=src_str, dst=dst_str, label=label, dst_kind=dst_kind),
                properties=properties))
        if records:
            return records[0]['uid']
        return None

    def delete_link(self, label, src, dst):
        """Delete a link between a src Node and a dst Node. src and dst are dict that
        describe the Node (property name and value to filter nodes). label is the link type."""
        src_str = self._dict_to_cypher(src)
        dst_str = self._dict_to_cypher(dst)
        qry = 'MATCH (src {src}) -[link:{label}]->(dst {dst}) '\
              'DELETE link RETURN link'
        records = list(self.exec_query(qry.format(label=label, src=src_str, dst=dst_str)))
        if records:
            return records[0]
        return None


class RedisGraph(Neo4J):
    """To use with RedisGraph. Implementation not complete.
    """

    def __init__(self, node_kinds, host='localhost', port=6379):
        import redis
        import redisgraph
        self.node_kinds = node_kinds
        self.redis_conn = self.redis.Redis(host, port)
        self.graph = self.redisgraph.Graph('test', self.redis_conn)

    def exec_query(self, query, **params):
        result_set = self.graph.query(query).result_set
        var_names = set([name[0] for name in list(map(lambda x:x.decode('utf-8').split('.'), result_set[0]))])
        attr_names = set([name[1] for name in list(map(lambda x:x.decode('utf-8').split('.'), result_set[0]))])
        records = []
        for name in var_names:
            record = {}
            record[name] = []
