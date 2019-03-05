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
import time
import json

DEFAULT_DB_URI = 'bolt://localhost:7687'
DEFAULT_DB_USER = 'neo4j'
DEFAULT_DB_PASS = 'neo4j'

class GraphDB():

    SUPPORTED_SORTS = ['asc', 'desc']


class Neo4J(GraphDB):

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
            raise Exception('Failed to connect to Neo4j server')

    @staticmethod
    def _dict_to_match_str(d):
        """convert a dict into cypher-compliant string.
        Ex: {"label": "A", "cost": 2, "name": "R1"} -> '{:A {cost: 2, name:"R"}'
        """
        if not d:
            return ''
        kind = d.pop('label', None)
        match_str = []
        for k, v in d.items():
            if isinstance(v, str):
                match_str.append('%s: "%s"' % (k, v))
            else:
                match_str.append('%s: %s' % (k, v))
        match_str = ', '.join(match_str)
        match_str = '{ %s }' % match_str
        if kind:
            match_str = ':%s %s' % (kind, match_str)
        return match_str

    @staticmethod
    def _dict_to_set_str(name, d):
        """Turn a dict into Cypher SET command.
        Ex. _dict_to_set_str('node', {'delay': 1.0, 'state': 'up'}) return
            'node.delay=1.0, node.state="down"'
        """
        if not d:
            return ''
        set_str = []
        for key, value in d.items():
            if type(value) == str:
                set_str.append('%s.%s="%s"' % (name, key, value))
            else:
                set_str.append('%s.%s=%s' % (name, key, value))
        set_str = ', '.join(set_str)
        if set_str:
            set_str = 'SET ' + set_str
        return set_str

    def exec_query(self, query, **params):
        """Run a Cypher query."""
        if not query:
            return []
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

    def delete_node(self, kind=None, match={}):
        """Delete a node from the database. Leave out labels and filters to delete everything
        labels (list) to define kinds of the node
        filters (dict) a set of property names and values to match on.
        Ex:
        delete_node('Border', {'name': 'R'}) to delete all Border nodes with name = R
        delete_node(filters={'name': 'R'}) to delete all nodes with name = R
        """
        kind = ':' + kind if kind else ''
        where_str = self._dict_to_match_str(match)
        qry = 'MATCH (node {label} {filter_str}) DETACH DELETE node RETURN node'
        records = list(self.exec_query(qry.format(label=kind, filter_str=where_str)))
        return records

    def create_node(self, labels, match, properties={}):
        """Create a new node with labels as node kind and properties. If a 'uid' in
        the properties dict and a node with the same uid exist, it will be updated with
        the new properties.
        """
        if isinstance(labels, list):
            kind = ":".join(labels)
        else:
            kind = ':' % labels
        match = self._dict_to_match_str(match)
        qry = 'MERGE ( node:{kind} {match} ) '\
              'ON CREATE SET node=$properties '\
              'ON MATCH SET node=$properties '\
              'RETURN node'
        qry = qry.format(kind=kind, match=match)
        records = list(self.exec_query(qry, properties=properties))
        if records:
            return records[0]['node']
        return None

    def update_node(self, match, kind, properties={}):
        set_str = self._dict_to_set_str('node', properties)
        match = self._dict_to_match_str(match)
        qry = 'MATCH ( node:{kind} {match} ) '\
              '{set_str} '\
              'RETURN node'
        qry = qry.format(kind=kind, match=match, set_str=set_str)
        records = list(self.exec_query(qry))
        if records:
            return records[0]['node']
        return None

    def create_link(self, kind, src, dst, properties={}):
        """create a link between a src Node and a dst Node. src node must exist.
        Argument create_dst can be used to turn on/off dst node condition.

        :param src: match (dict) on src node
        :param dst: match (dict) on dst node
        :rtype: a link record
        """
        src_match = self._dict_to_match_str(src)
        dst_match = self._dict_to_match_str(dst)
        set_str = []
        set_str = self._dict_to_set_str(kind, properties)
        if set_str:
            set_str = ' ON CREATE %s \n ON MATCH %s ' % (set_str, set_str)
        qry = 'MATCH ( src {src_match} ), (dst {dst_match} ) '\
              'MERGE ( src )-[{name}:{kind}]->( dst ) '\
              '{set_str} RETURN src.uid AS src, dst.uid AS dst, {name}'
        qry = qry.format(src_match=src_match, dst_match=dst_match, name=kind, kind=kind, set_str=set_str)
        records = list(self.exec_query(qry))
        if records:
            return records[0]
        return None

    def update_link(self, kind, src, dst, properties={}):
        set_str = self._dict_to_set_str(kind, properties)
        src_match = self._dict_to_match_str(src)
        dst_match = self._dict_to_match_str(dst)
        qry = 'MATCH ( src {src_match} )-[{name}: {kind}]->( dst {dst_match} ) '\
              '{set_str} RETURN src.uid AS src, dst.uid AS dst, {name}'
        records = list(self.exec_query(
            qry.format(name=kind, kind=kind, set_str=set_str, src_match=src_match, dst_match=dst_match)))
        if records:
            return records[0]
        return None

    def delete_link(self, kind, match={}, src={}, dst={}):
        """Delete a link between a src Node and a dst Node. src and dst are dict that
        describe the Node (property name and value to filter nodes). label is the link type."""
        match_str = self._dict_to_match_str(match)
        src_match = self._dict_to_match_str(src)
        dst_match = self._dict_to_match_str(dst)
        qry = 'MATCH (src {src_match} ) -[{name}:{kind} {match}]->(dst {dst_match}) '\
              'DELETE {name} RETURN src.uid AS src, dst.uid AS dst, {name}'
        qry = qry.format(
                name=kind, kind=kind, match=match_str, src_match=src_match, dst_match=dst_match)
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
