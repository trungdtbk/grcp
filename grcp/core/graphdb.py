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
    from neo4j.exceptions import ConstraintError

    def __init__(self, db_uri=None, db_user=None, db_pass=None):
        uri = db_uri or DEFAULT_DB_URI
        username = db_user or DEFAULT_DB_USER
        password = db_pass or DEFAULT_DB_PASS
        self.driver = None
        for _ in range(60):
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
        if d is None:
            return ''
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
        print(query)
        try:
            with self.driver.session() as session:
                return session.run(query, params)
        except self.ConstraintError:
            pass
        except Exception as e:
            raise e
        return []

    def create_constraint(self, kind, prop):
        """Make sure that each node with kind has unique property prop."""
        qry = 'CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE' % (kind, prop)
        self.exec_query(qry)

    def create_index(self, kind, prop):
        qry = 'CREATE INDEX ON :%s(%s)' % (kind, prop)
        self.exec_query(qry)

    def clear_db(self):
        """Clear everything from the database."""
        self.delete_node()

    def delete_node(self, kind=None, filter_str=''):
        """Delete a node from the database. Leave out labels and filters to delete everything
        labels (list) to define kinds of the node
        filters (dict) a set of property names and values to match on.
        Ex:
        delete_node('Peer', {'name': 'R'}) to delete all Peer nodes with name = R
        delete_node(filters={'name': 'R'}) to delete all nodes with name = R
        """
        label = ':' + kind if kind else ''
        if filter_str:
            filter_str = 'WHERE ' + filter_str
        qry = 'MATCH ( node {label} ) {filter_str} DETACH DELETE node RETURN count(node) AS count;'
        records = list(self.exec_query(qry.format(label=label, filter_str=filter_str)))
        if records:
            return records[0]['count']
        return None

    def create_node(self, match_dict, labels, properties={}):
        """Create a new node with labels as node kind and properties. If a 'uid' in
        the properties dict and a node with the same uid exist, it will be updated with
        the new properties.
        """
        if isinstance(labels, list):
            kind = ":".join(labels)
        else:
            kind = ':' % labels
        match = self._dict_to_cypher(match_dict)
        qry = 'MERGE ( node:{kind} {match} ) '\
              'ON MATCH SET node=$properties, node.uid=id(node) '\
              'ON CREATE SET node=$properties, node.uid=id(node) '\
              'RETURN node'
        qry = qry.format(kind=kind, match=match)
        records = list(self.exec_query(qry, properties=properties))
        if records:
            return records[0]['node']
        return None

    def update_node(self, match_dict, kind, properties={}):
        set_str = []
        for key, value in properties.items():
            if type(value) == str:
                set_str.append('node.%s="%s"' % (key, value))
            else:
                set_str.append('node.%s=%s' % (key, value))
        set_str = ', '.join(set_str)
        match = self._dict_to_cypher(match_dict)
        qry = 'MATCH ( node:{kind} {match} ) '\
              'SET {set_str} '\
              'RETURN node'
        qry = qry.format(kind=kind, match=match, set_str=set_str)
        records = list(self.exec_query(qry))
        if records:
            return records[0]['node']
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

    def create_link(self, kind, src, dst, properties={}, create_dst=False):
        """create a link between a src Node and a dst Node. src node must exist.
        Argument create_dst can be used to turn on/off dst node condition.
        Args:
            src (dict): properties of src Node.
            dst (dict): properties of dst Node.
        """
        src_match = self._dict_to_cypher(src)
        dst_match = self._dict_to_cypher(dst)
        set_str = []
        for key, value in properties.items():
            if type(value) == str:
                set_str.append('%s.%s="%s"' % (kind, key, value))
            else:
                set_str.append('%s.%s=%s' % (kind, key, value))
        set_str = ', '.join(set_str)
        if create_dst:
            set_str = set_str + ',' if set_str else set_str
            qry = 'MATCH ( src {src_match} ) '\
                  'MERGE ( src )-[{name}:{kind}]->( dst {dst_match} ) '\
                  'SET {set_str} '\
                  '(CASE {name}.uid WHEN NULL THEN {name} END).uid=id({name}), '\
                  '(CASE dst.uid WHEN NULL THEN dst END).uid=id(dst) '\
                  'RETURN src.uid AS src, dst.uid AS dst, {name}'
        else:
            qry = 'MATCH ( src {src_match} ), (dst {dst_match} ) '\
                  'MERGE ( src )-[{name}:{kind}]->( dst ) '\
                  'SET {set_str} '\
                  'RETURN src.uid AS src, dst.uid AS dst, {name}'
        qry = qry.format(src_match=src_match, dst_match=dst_match, name=kind, kind=kind, set_str=set_str)
        records = list(self.exec_query(qry))
        if records:
            return records[0]
        return None

    def update_link(self, name, linkid, kind=None, properties={}):
        kind = kind or ''
        if kind:
            kind = ':' + kind
        qry = 'MATCH ( src )-[{name} {kind}]->( dst) '\
              'WHERE id({name})=$linkid '\
              'SET {name}=$properties, {name}.uid=id({name}) '\
              'RETURN src.uid AS src, dst.uid AS dst, {name}'
        records = list(self.exec_query(qry.format(name=name, kind=kind), linkid=linkid, properties=properties))
        if records:
            return records[0]
        return None

    def delete_link(self, lid=None, label=None, src={}, dst={}):
        """Delete a link between a src Node and a dst Node. src and dst are dict that
        describe the Node (property name and value to filter nodes). label is the link type."""
        where_str = []
        for s, d in (('src', src), ('dst', dst)):
            if 'uid' in d:
                where_str.append('id(%s)=%s' % (s, d.pop('uid')))
        if lid:
            where_str.append('id(%s)=%s' % (label, lid))
        where_str = ' AND '.join(where_str)
        if where_str:
            where_str = 'WHERE ' + where_str
        if label is None:
            label = ''
        else:
            label = ':' + label
        qry = 'MATCH (src) -[{label}:{label}]->(dst) '\
              '{where} DELETE {label} RETURN src.uid AS src, dst.uid AS dst, {label}'
        qry = qry.format(label=label, src=src_str, dst=dst_str, where=where_str)
        return self.exec_query(qry)


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
