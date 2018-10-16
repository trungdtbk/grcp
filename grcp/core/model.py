import copy
import inspect
import logging
import ipaddress

from . import graphdb
from . import query


_GID = 0

def auto_id():
    global _GID
    _GID += 1
    return _GID


class Property(object):
    """Base class for data object Property. A model can have many of these Property"""

    _code_name = None # used for creating Cypher query
    _name = None
    _indexed = True # whether the Property will be indexed in the graph
    _required = False
    _default = None
    _verbose_name = None

    def __init__(self, name=None, indexed=None, required=None,
                 default=None, verbose_name=None):
        if name is not None:
            self._name = name
        if indexed is not None:
            self._indexed = indexed
        if required is not None:
            self._required = required
        if default is not None:
            self._default = default
        if verbose_name is not None:
            self._verbose_name = verbose_name

    def __new__(cls, *args, **kwargs):
        if cls == Property:
            raise TypeError('Cannot instantiate Property, only subclass.')
        return super(Property, cls).__new__(cls)

    @classmethod
    def _validate(cls, value):
        """subclass should override this."""
        return value

    def _get_value(self, entity):
        """Get called when an instance trying to access its member which is an
        instance of the Property class, i.e. the __get__ method is overriden for this."""
        if self._name in entity._values:
            return entity._values[self._name]
        elif self._default is not None:
            return self._default
        return None

    def _set_value(self, entity, value):
        if value is not None:
            value = self._validate(value)
        if self._required and value is None:
            raise TypeError('%s requires a not-None value' % self.__class__.__name__)
        entity._values[self._name] = value

    def __get__(self, obj, objclass):
        """Override the default __get__ to change the _code_name as the variable name
        of the attribute of the instance whose attribute member is being access.
        Eg:
        class A():
            attr = SomeProperty(name='my_name')
        print(A.attr._code_name)
        # result: A.my_name
        We need this behaviour to generate Cypher queries on a model.
        """
        if objclass is not None:
            self._code_name = '.'.join((objclass.__name__, self._name))
        if obj is None:
            return self
        return self._get_value(obj)

    def copy(self):
        new = self.__class__(
                self._name, self._indexed, self._required, self._default, self._verbose_name)
        return new

    def _comparison(self, op, value):
        """get called when standard Python binary operator is used on a property to
        return a query.FilterNode.
        """
        if not self._indexed:
            raise TypeError('Cannot query on unindexed property %s' % self._name)
        value = self._validate(value)
        return query.FilterNode(self._code_name, op, value)

    def __hash__(self):
        return hash((self.name, self._value, self.required))

    def __eq__(self, value):
        return self._comparison('=', value)

    def __ne__(self, value):
        return self._comparison('<>', value)

    def __gt__(self, value):
        return self._comparison('>', value)

    def __ge__(self, value):
        return self._comparison('>=', value)

    def __lt__(self, value):
        return self._comparison('<', value)

    def __le__(self, value):
        return self._comparison('<=', value)

    def __neg__(self):
        """Return a descending order on this property."""
        return query.PropertyOrder(self._code_name, query.PropertyOrder.DESCENDING)

    def __pos__(self):
        return query.PropertyOrder(self._code_name)

    def __repr__(self):
        return '<%s name=%s required=%s indexed=%s verbose_name=%s>' % (
                self.__class__.__name__, self._name, self._required,
                self._indexed, self._verbose_name)


class StringProperty(Property):
    """A StringProperty accepts only string value."""
    @classmethod
    def _validate(cls, value):
        if not isinstance(value, str):
            raise TypeError(
                'StringProperty accepts only str value; received %r' % value)
        return value


class ListProperty(Property):
    """A ListProperty accepts only list typed value. Items in list can be scalar"""
    @classmethod
    def _validate(cls, value):
        if not isinstance(value, list):
            raise TypeError(
                'ListProperty accepts only list value; received %r' % value)
        return value

    def incl(self, item):
        """Return query.FilterInclude """
        return query.FilterInclude(self._code_name, 'IN', item)

    def excl(self, item):
        return query.FilterExclude(self._code_name, 'IN', item)

    def __neg__(self):
        """Return a descending order on this property."""
        return query.PropertyOrder(self._code_name, query.PropertyOrder.DESCENDING, func='length')

    def __pos__(self):
        return query.PropertyOrder(self._code_name, func='length')


class NumberProperty(Property):
    """exist for type checing."""
    pass


class IntegerProperty(NumberProperty):
    @classmethod
    def _validate(cls, value):
        if not isinstance(value, int):
            raise TypeError(
                'IntegerProperty accepts only int value; received %r' % value)
        return value


class UIDProperty(IntegerProperty):
    """Represent universal ID of nodes and links in the database.
    This UID is set automatically to the generated ID by Neo4J."""

    def __get__(self, obj, objclass):
        if objclass is not None:
            self._code_name = objclass.__name__
        if obj is None:
            return self
        return self._get_value(obj)

    def __eq__(self, value):
        return self._comparison('=', value)

    def __ne__(self, value):
        raise Exception('not supported')

    def __gt__(self, value):
        raise Exception('not supported')

    def __ge__(self, value):
        raise Exception('not supported')

    def __lt__(self, value):
        raise Exception('not supported')

    def __le__(self, value):
        raise Exception('not supported')

    def __neg__(self):
        raise Exception('not supported')

    def __pos__(self):
        raise Exception('not supported')

class FloatProperty(NumberProperty):
    @classmethod
    def _validate(cls, value):
        if not isinstance(value, (int, float)):
            raise TypeError(
                'FloatProperty accepts only float, int, or long  value; received %r' % value)
        return float(value)


class PositiveFloatProperty(FloatProperty):
    @classmethod
    def _validate(cls, value):
        value = super(PositiveFloatProperty, cls)._validate(value)
        if value < 0:
            raise TypeError('Must be a positive number. Received %r' % value)
        return value

class BandwidthProperty(PositiveFloatProperty):
    """Exist to map a property with a cypher function to get the bandwidth of a Path.
    path bandwidth = min(link bandwidth of all links in the path)
    """
    pass


class LossProperty(PositiveFloatProperty):
    """A loss is the sum of loss in all Links in the path."""
    pass

class WeightProperty(PositiveFloatProperty):
    """Represent an intradomain link weight."""
    pass


class LatencyProperty(PositiveFloatProperty):
    pass


class CostProperty(FloatProperty):
    """Represent cost for sending traffic unit over an inter-AS link.
    This is used to model AS relationship."""
    pass


class IPAddressProperty(Property):
    @classmethod
    def _validate(cls, value):
        try:
            address = ipaddress.ip_address(value)
            return str(address)
        except:
            raise TypeError('Wrong data type/format provided: %r' % value)


class PrefixProperty(Property):
    @classmethod
    def _validate(cls, value):
        try:
            prefix = ipaddress.ip_network(value)
            return str(prefix)
        except:
            raise TypeError('Wrong data format/type provided: %r' % value)


class Model(object):
    """A model represents an element in the network e.g. Router, Link and so on.
    Put _ before a property to exclude from being added to the database.
    """
    _gdb = None
    _values = None
    _properties = None

    uid = UIDProperty(name='uid', required=True, default=-1)

    def __new__(cls, *args, **kwargs):
        if cls in [Model, Node, Router, Edge, Link]:
            raise TypeError('Cannot create model %s, only subclass allowed' % cls)
        return super(Model, cls).__new__(cls)

    def __init__(self, *args, **kwargs):
        self._values = {}
        self._properties = {}
        for name, value in kwargs.items():
            setattr(self, name, value)
        for name, prop in inspect.getmembers(self.__class__):
            if isinstance(prop, Property):
                if name not in self._properties and prop._default is not None:
                    setattr(self, name, prop._default)
                if prop._required and name not in self._properties:
                    raise TypeError('Property %s is required' % name)

    @property
    def properties(self):
        """return a dict of Property"""
        return self._properties

    def __setattr__(self, name, value):
        """override the default setattr method so we can correctly set property for each instance."""
        if name.startswith('_'):
            self.__dict__[name] = value
            return
        prop = getattr(self.__class__, name)
        if not isinstance(prop, Property):
            raise TypeError('Cannot set non-property %s' % name)
        prop = prop.copy()
        prop._set_value(self, value)
        self._properties[name] = prop

    def _get_values(self):
        values = {}
        for name, prop in self._properties.items():
            if not isinstance(prop, Property):
                continue
            value = prop._get_value(self)
            if prop._required and value is None:
                raise TypeError('Property %s is required; but not set' % name)
            if value is not None:
                values[name] = value
        return values

    @classmethod
    def query(cls, *args, **kwargs):
        """Construct a Query instance for this model which can be used to fetch data from the db."""
        assert cls._gdb is not None, 'call Model.initialize to initialize the model first'
        early_filter = None
        kind = kwargs.pop('kind') if 'kind' in kwargs else cls
        if 'uid' in kwargs:
            early_filter = 'id(%s)=%s' % (kind.__name__, kwargs.pop('uid'))
        qry = query.Query(cls._gdb, kind=kind, early_filter=early_filter, **kwargs)
        return qry.filter(*args)

    def count_edges(self):
        src = {'label': self.__class__.__name__, 'uid': self._uid}
        return self._gdb.count_outgoing_edges(src=src)

    @classmethod
    def _class_name(cls):
        return cls.__class__.__name__

    @classmethod
    def _class_names(cls):
        """return all class names in the hierarchy (not 'object' and 'Model') to be used for Node labels."""
        return [str(cls_.__name__) for cls_ in inspect.getmro(cls)
                if cls_ not in [object, type, Model, Edge]]

    def get_match_dict(self):
        """override this if required in subclass."""
        return {}

    def put(self):
        raise NotImplemented

    def delete(self):
        filter_node = self.uid == self.uid._get_value(self)
        filter_str = filter_node.to_cypher()
        return self._gdb.delete_node(self._class_name(), filter_str) is not None

    @classmethod
    def entity_to_model(cls, entity, **kwargs):
        """turn a Neo4J object into a model instance."""
        properties = dict(entity)
        properties.update(kwargs)
        if isinstance(entity, cls._gdb.Node):
            modelclass = None
            for label in list(entity.labels):
                if label in _all_models:
                    modelclass = _all_models[label]
                    break
            if modelclass is not None:
                new = modelclass(**properties)
                return new
            else:
                raise Exception('model not found')
        elif isinstance(entity, cls._gdb.Relationship):
            modelclass = _all_models[entity.type]
            new = modelclass(**properties)
            return new
        return entity

    @classmethod
    def neo4j_to_model(cls, record):
        """Generate a model instance from a Cypher record."""
        if record and issubclass(cls, Node) or cls==Prefix:
            entity = record[cls.__name__]
            return cls.entity_to_model(entity)
        return None

    @classmethod
    def dict_to_cypher(cls, name, d):
        """ turn this dict d into a cypher string. name is variable used in cypher query
        """
        filters = []
        for k, v in d.items():
            if k == 'uid':
                k = 'id(%s)' % name
            else:
                k = '%s.%s' % (name, k)
            if type(v) == str:
                filters.append('%s="%s"' % (k, v))
            else:
                filters.append('%s=%s' % (k, v))
        return ' AND '.join(filters)

    def __str__(self):
        return '<%s %s>' % (self.__class__.__name__, self._get_values())
    __repr__ = __str__


class Node(Model):
    """Represent a routing node in graph."""
    name = StringProperty(name='name')

    @classmethod
    def nodes_with_no_edges(cls):
        """ Return all nodes that has no particular kind of edge."""
        records = cls._gdb.exec_query(
            'MATCH {name} WHERE ({name})--() RETRUN {name}'.format(name=cls.__name__))
        for record in records:
            yield cls.neo4j_to_model(record)

    @classmethod
    def count(cls):
        record = list(cls._gdb.exec_query('MATCH (n) RETURN COUNT(n) as count'))
        if record:
            return record[0]['count']

    @classmethod
    def node_by_id(cls, uid):
        nodes = list(cls.query(uid=uid).fetch(limit=1))
        if nodes:
            return nodes[0]
        return None

    def put(self):
        """Save to the database."""
        properties = self._get_values()
        match_dict = self.get_match_dict()
        if self.uid > 0:
            match_dict['uid'] = self.uid
        labels = self._class_names()
        record = self._gdb.create_node(match_dict, labels=labels, properties=properties)
        if record:
            return self.entity_to_model(record)
        return None

    @classmethod
    def get_or_create(cls, match_dict, properties):
        """Get node based on properties, create if not exist.
        properties (dict): ex {'name': 'R1'}
        """
        record = cls._gdb.create_node(match_dict=match_dict, labels=cls._class_names(), properties=properties)
        if record:
            return cls.entity_to_model(record)
        return None


class Router(Node):
    """A model represents a BGP Router."""
    state = StringProperty(name='state', default='down')

    @classmethod
    def get_by_router_id(cls, router_id):
        routers = list(cls.query(cls.router_id==router_id).fetch(limit=1))
        if routers:
            return routers[0]
        return None

    @classmethod
    def update(cls, router_id, state):
        match_dict = {'router_id': router_id}
        record = cls._gdb.update_node(match_dict, cls.__name__, {'state': state})
        if record:
            return cls.entity_to_model(record)


class BorderRouter(Router):
    """ Represent a border router of the ISP. """
    router_id = IPAddressProperty(name='router_id', required=True)
    local_as = IntegerProperty(name='local_as')

    def get_match_dict(self):
        return {'router_id': self.router_id, 'label': self.__class__.__name__}

    @classmethod
    def create_constraints(cls):
        cls._gdb.create_constraint(cls.__name__, cls.router_id._name)

    @classmethod
    def get_or_create(cls, router_id, name, state):
        return super(BorderRouter, cls).get_or_create(
                                        match_dict={'router_id': router_id},
                                        properties={
                                            'router_id': router_id,
                                            'state':state,
                                            'name': name})

class PeerRouter(Router):
    """Represent a router belonging to a neighbor."""
    peer_ip = IPAddressProperty(name='peer_ip', required=True)
    peer_as = IntegerProperty(name='peer_as', required=True)
    local_ip = IPAddressProperty(name='local_ip')
    local_as = IntegerProperty(name='local_as')

    def get_match_dict(self):
        return {'peer_ip': self.peer_ip, 'label': self.__class__.__name__}

    @classmethod
    def create_constraints(cls):
        cls._gdb.create_constraint(cls.__name__, cls.peer_ip._name)

    @classmethod
    def get_or_create(cls, peer_ip, peer_as, local_ip, local_as, state):
        properties = dict(
                peer_ip=peer_ip,
                peer_as=peer_as,
                local_ip=local_ip,
                local_as=local_as,
                state=state,
                )
        return super(PeerRouter, cls).get_or_create(
                                        match_dict={'peer_ip': peer_ip},
                                        properties=properties)


class Prefix(Node):
    """Represent a destination prefix."""
    prefix = PrefixProperty(name='prefix', required=True)

    def get_match_dict(self):
        return {'prefix': self.prefix}

    @classmethod
    def get(cls, prefix):
        prefixes = list(cls.query(cls.prefix==prefix).fetch(limit=1))
        if prefixes:
            return prefixes[0]
        return None

    @classmethod
    def create_constraints(cls):
        cls._gdb.create_constraint(cls.__name__, cls.prefix._name)

    @classmethod
    def get_or_create(cls, prefix):
        return super(Prefix, cls).get_or_create({'prefix': prefix}, {'prefix': prefix})


class Edge(Model):
    """Represent an edge in graph. Exist mainly to check type."""
    src = IntegerProperty('src', verbose_name='uid of src node', required=True)
    dst = IntegerProperty('dst', verbose_name='uid of dst node', required=True)
    state = StringProperty(name='state', default='down')
    name = StringProperty(name='name')

    @classmethod
    def count(cls):
        record = list(cls._gdb.exec_query(
                'MATCH (n)-[r : {kind}]->() RETURN COUNT(r) as count'.format(cls.__name__)))
        if record:
            return record[0]['count']

    def put(self):
        """Save to the database."""
        properties = self._get_values()
        uid = properties.pop('uid')
        src = { 'uid': properties.pop('src') }
        dst = { 'uid': properties.pop('dst') }
        label = self.__class__.__name__
        record = self._gdb.create_link(label=label, src=src, dst=dst,
                                       properties=properties)
        if record:
            return self.neo4j_to_model(record)
        return None

    @classmethod
    def get_or_create(cls, src_match, dst_match, properties, create_dst=False):
        for name, prop in inspect.getmembers(cls):
            if isinstance(prop, Property) and prop._default is not None:
                properties.setdefault(name, prop._default)
        record = cls._gdb.create_link(cls.__name__, src_match, dst_match, properties, create_dst)
        if record:
            return cls.neo4j_to_model(record)
        return None

    @classmethod
    def update(cls, uid, **attributes):
        record = cls._gdb.update_link(cls.__name__, uid, properties=attributes)
        if record:
            return neo4j_to_model(record)
        return None

    def delete(self):
        cls = self.__class__
        src = { 'uid': self.src }
        dst = { 'uid': self.dst }
        lid = self.uid
        label = cls.__name__
        return self._gdb.delete_link(lid=lid, label=label, src=src, dst=dst) is not None

    @classmethod
    def neo4j_to_model(cls, record):
        if record and issubclass(cls, Edge):
            entity = record[cls.__name__]
            modelclass = _all_models[entity.type]
            new = modelclass.entity_to_model(entity, src=record['src'], dst=record['dst'])
            return new

    @classmethod
    def query(cls, *args, **kwargs):
        """ There are three ways to query a link from the database:
        by uid of src and dst, by property of src and dst and by uid of the link
        1. query based on uid of src and dst
        Example:

        q = query(
                 src=dict(uid=1, prop1='a'),
                 dst=dict(uid=2))

        q = query(uid=123)

        q = query(Link.status=='up', uid=123)

        3. query based on link id
        """
        early_filter = []
        src_label = None
        dst_label = None
        if 'src' in kwargs:
            src = kwargs.pop('src')
            src_label = src.pop('label', None)
            early_filter.append(cls.dict_to_cypher('src', src))
        if 'dst' in kwargs:
            dst = kwargs.pop('dst')
            dst_label = dst.pop('label', None)
            early_filter.append(cls.dict_to_cypher('dst', dst))
        if 'uid' in kwargs:
            early_filter.append('id(%s)=%s' % (cls.__name__, kwargs.pop('uid')))
        early_filter = ' AND '.join([f for f in early_filter if f])
        kind = kwargs.pop('kind') if 'kind' in kwargs else cls
        qry = query.Query(
                cls._gdb, kind=kind, early_filter=early_filter,
                src_label=src_label, dst_label=dst_label, **kwargs)
        return qry.filter(*args)

class Route(Edge):
    """Represent a route."""
    local_pref = IntegerProperty('local_pref')
    aspath_len = IntegerProperty(name='aspath_len')
    as_path = ListProperty('as_path')
    origin = IntegerProperty('origin')
    med = IntegerProperty('med')
    communities = StringProperty('communities')
    origin_as = IntegerProperty('origin_as')
    prefix = StringProperty('prefix')
    nexthop = StringProperty('nexthop')

    @classmethod
    def get_or_create(cls, peer_ip, prefix, properties):
        src_match = {'peer_ip': peer_ip, 'label': PeerRouter.__name__}
        dst_match = {'prefix': prefix, 'label': Prefix.__name__}
        return super(Route, cls).get_or_create(src_match, dst_match, properties)


class Link(Edge):
    """Represent a link between two Nodes. A link is directional."""
    loss = LossProperty('loss', default=0)
    delay = LatencyProperty('delay', default=1)
    bandwidth = BandwidthProperty('bandwidth', default=1)
    utilization = PositiveFloatProperty('utilization', default=0)
    pathid = IntegerProperty(name='pathid')
    port = StringProperty(name='port')
    dp = StringProperty(name='dp')

    @classmethod
    def get_by_id(cls, uid):
        links = list(cls.query(uid=uid).fetch(limit=1))
        if links:
            return links[0]
        return None

class IntraLink(Link):
    weight = WeightProperty('weight')

    @classmethod
    def get_or_create(cls, src_router_id, dst_router_id, properties):
        src_match = {'router_id': src_router_id, 'label': BorderRouter.__name__}
        dst_match = {'router_id': dst_router_id, 'label': BorderRouter.__name__}
        return super(IntraLink, cls).get_or_create(src_match, dst_match, properties)


class InterIngress(Link):
    """Represent ingress link i.e from PeerRouter-->BorderRouter """
    @classmethod
    def get_or_create(cls, src_peer_ip, dst_router_ip, properties):
        src_match = {'peer_ip': src_peer_ip, 'label': PeerRouter.__name__}
        dst_match = {'router_id': dst_router_ip, 'label': BorderRouter.__name__}
        return super(InterIngress, cls).get_or_create(src_match, dst_match, properties)


class InterEgress(Link):
    """Represent egress link i.e from BorderRouter-->PeerRouter """
    cost = CostProperty('cost', verbose_name='transit cost')

    @classmethod
    def get_or_create(cls, src_router_ip, dst_peer_ip, properties):
        src_match = {'router_id': src_router_ip, 'label': BorderRouter.__name__}
        dst_match = {'peer_ip': dst_peer_ip, 'label': PeerRouter.__name__}
        return super(InterEgress, cls).get_or_create(src_match, dst_match, properties)


class PathProperty(type):
    """Use to override class attribute access.
    A path has following attributes:
    - intra_util: utilization of the intra link
    - inter_util: utilization of the egress link
    - capacity: minimum capacity of all links
    - lost: total loss of all links
    - delay: total delay of all links
    - cost: total cost of all links
    - weight: total weight of all links
    - as_path: as path
    """
    _SUPPORTED_PROPERTIES = {
            'intra_util': IntraLink.utilization,
            'intra_bw': IntraLink.bandwidth,
            'intra_loss': IntraLink.loss,
            'intra_delay': IntraLink.delay,
            'intra_weight': IntraLink.weight,
            'inter_bw': InterEgress.bandwidth,
            'inter_loss': InterEgress.loss,
            'inter_delay': InterEgress.delay,
            'inter_cost': InterEgress.cost,
            'inter_util': InterEgress.utilization,
            'route_aspath': Route.as_path,
            'route_pref': Route.local_pref,
            'route_med': Route.med,
            'src_uid': IntegerProperty,
            'dst_uid': IntegerProperty,
            'nodes': ListProperty,
            'links': ListProperty,
        }
    def __getattr__(cls, attr):
        if attr in cls._SUPPORTED_PROPERTIES:
            prop_cls = cls._SUPPORTED_PROPERTIES[attr]
            if isinstance(prop_cls, Property):
                prop_cls = prop_cls.__class__
            prop = prop_cls(name=attr)
            prop._code_name = 'Path.' + prop._name
            return prop


class Path(Edge, metaclass=PathProperty):
    """ Exist for queyring only."""
    def put(self):
        raise Exception('method not allowed')

    @classmethod
    def neo4j_to_model(cls, record):
        if record:
            path = dict(record[cls.__name__])
            return path
        return None

    @classmethod
    def query(cls, *args, router_id, prefix, for_peer=False):
        if for_peer:
            label = PeerRouter.__name__
            key_name = 'peer_ip'
        else:
            label = BorderRouter.__name__
            key_name = 'router_id'
        src_match = {'%s' % key_name: router_id, 'label': label}
        dst_match = dict(prefix=prefix, label=Prefix.__name__)
        return super(Path, cls).query(*args, src=src_match, dst=dst_match)


class Mapping(Edge):
    """Represent a RIB/FIB entry of a node. A mapping links a node to a prefix"""
    #nodes = ListProperty(name='nodes')
    #links = ListProperty(name='links')
    load = FloatProperty(name='load', default=0)
    prefix = StringProperty(name='prefix')
    ingress = StringProperty(name='ingress')
    egress = StringProperty(name='egress')
    nexthop = StringProperty(name='nexthop')
    neighbor = StringProperty(name='neighbor')
    pathid = IntegerProperty(name='pathid')

    def put(self):
        """Save to the database."""
        properties = self._get_values()
        properties.pop('uid')
        src = { 'uid': properties.pop('src') }
        dst = { 'uid': properties.pop('dst') }
        label = self.__class__.__name__
        record = self._gdb.create_link(label=label, src=src, dst=dst,
                                       properties=properties)
        if record:
            return self.neo4j_to_model(record)

    @classmethod
    def get_or_create(cls, src_node_id, prefix, properties, is_peer=False):
        if is_peer:
            src_match = {'peer_ip': src_node_id, 'label': PeerRouter.__name__}
        else:
            src_match = {'router_id': src_node_id, 'label': BorderRouter.__name__}
        dst_match = {'prefix': prefix, 'label': Prefix.__name__}
        return super(Mapping, cls).get_or_create(src_match, dst_match, properties)


def initialize():
    """Start the interface to graphdb. """
    Model._gdb = graphdb.Neo4J() # connect to a live Neo4J
    BorderRouter.create_constraints()
    PeerRouter.create_constraints()
    Prefix.create_constraints()

def warm_up_cache():
    Model._gdb.exec_query('MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN COUNT(n.uid) + COUNT(r.uid);')

def clear_db():
    Model._gdb.clear_db()


_all_models = {
        'Route': Route,
        'Path': Path,
        'IntraLink': IntraLink,
        'InterEgress': InterEgress,
        'InterIngress': InterIngress,
        'BorderRouter': BorderRouter,
        'PeerRouter': PeerRouter,
        'Prefix': Prefix,
        'Mapping': Mapping}
