"""
Models based on Labeled Graph Property for BGP routing.

"""
import copy
import inspect
import logging
import ipaddress
import uuid

from . import graphdb
from . import query


class Property(object):
    """Base class for data object Property. A model can have many of these Property"""

    _code_name = None # used for creating Cypher query
    _name = None
    _indexed = False # whether the Property will be indexed in the graph
    _required = False
    _default = None
    _verbose_name = None
    _type = None

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
            raise Exception('Instantiation of Property class is not allowed.')
        return super(Property, cls).__new__(cls)

    @classmethod
    def _validate(cls, value):
        """subclass should override this."""
        if value is None:
            return value
        try:
            value = cls._type(value)
        except:
            raise ValueError('%s accepts value typed %s; received: %r (%s)' % (
                             cls.__name__, cls._type, value, type(value)))
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
            raise ValueError('%s (%s) is required' % (self._name, self.__class__.__name__))
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
    __str__ = __repr__


class StringProperty(Property):
    """A StringProperty accepts only string value."""
    _type = str


class ListProperty(Property):
    """A ListProperty accepts only list typed value. Items in list can be scalar"""
    _type = list

    def incl(self, item):
        """Return query.FilterInclude """
        return query.FilterInclude(self._code_name, 'in', item)

    def excl(self, item):
        return query.FilterExclude(self._code_name, 'EXCL', item)

    def __neg__(self):
        """Return a descending order on this property."""
        return query.PropertyOrder(self._code_name, query.PropertyOrder.DESCENDING, func='length')

    def __pos__(self):
        return query.PropertyOrder(self._code_name, func='length')


class NumberProperty(Property):
    """exist for type checing."""
    pass


class IntegerProperty(NumberProperty):
    _type = int


class FloatProperty(NumberProperty):
    _type = float


class BandwidthProperty(FloatProperty):
    """Represent bandwidth (in bps) of a link."""
    pass


class LossProperty(FloatProperty):
    """A loss is the sum of loss in all Links in the path."""
    pass


class WeightProperty(FloatProperty):
    """Represent an IGP link weight."""
    pass


class LatencyProperty(FloatProperty):
    """Represent link latency (ms)."""
    pass


class CostProperty(FloatProperty):
    """Represent cost for sending traffic unit over an inter-AS link.
    This is used to model AS relationship."""
    pass


class PreferenceProperty(IntegerProperty):
    @classmethod
    def _validate(cls, value):
        value = super(PreferenceProperty, cls)._validate(value)
        if value < 0:
            raise ValueError('LocalPreference must be positive integer')
        return value


class MedProperty(IntegerProperty):
    pass


class OriginProperty(IntegerProperty):

    _CODES = {'incomplete': -1, 'igp': 0, 'egp': 1}

    @classmethod
    def _validate(cls, value):
        if isinstance(value, str) and value.lower() in cls._CODES:
            return cls._CODES[value.lower()]
        elif value in list(cls._CODES.values()):
            return value
        else:
            raise ValueError(
                'OriginProperty accepts value of integer or string:-1 (incomplete), '\
                '0 (igp), 1 (egp); received: %r' % value)


class IPAddressProperty(Property):
    @classmethod
    def _validate(cls, value):
        try:
            address = ipaddress.ip_address(value)
            return str(address)
        except:
            raise ValueError(
                '%s requires a valid IP address; received %r (%s)' % (cls._name, value, type(value)))


class PrefixProperty(Property):
    @classmethod
    def _validate(cls, value):
        try:
            prefix = ipaddress.ip_network(value)
            return str(prefix)
        except:
            raise ValueError(
                '%s requires a valid IP prefix; received %r (%s)' % (cls._name, value, type(value)))


class UIDProperty(Property):
    _type = uuid.UUID

    @classmethod
    def generate(cls):
        return str(uuid.uuid4())

    classmethod
    def _validate(cls, value):
        try:
            if isinstance(value, str):
                value = uuid.UUID(hex=value)
            elif isinstance(value, int):
                value = uuid.UUID(int=value)
            elif isinstance(value, bytes):
                value = uuid.UUID(bytes=value)
            if isinstance(value, uuid.UUID):
                return str(value)
        except:
            raise ValueError('%s requires a valid UUID; received %r' % (cls._name, value))


class Model(object):
    """A model represents an element in the network e.g. Router, Link and so on.
    When define a subclass put _ before a property to exclude from the database.
    """
    _gdb = None
    _values = None
    _properties = None
    _base_class = True

    uid = UIDProperty(name='uid', indexed=True, required=True)
    state = StringProperty(name='state', default='down')

    def __new__(cls, *args, **kwargs):
        if cls in [Model, Node, Router, Edge, Link, Path]:
            raise TypeError('Cannot create model %s, only subclass allowed.' % cls)
        return super(Model, cls).__new__(cls)

    def __init__(self, *args, **kwargs):
        self._values = {}
        self._properties = {}
        if 'uid' not in kwargs:
            kwargs['uid'] = UIDProperty.generate()
        for name, value in kwargs.items():
            setattr(self, name, value)
        for name, prop in inspect.getmembers(self.__class__):
            if isinstance(prop, Property):
                if name not in self._properties and prop._default is not None:
                    setattr(self, name, prop._default)
                if prop._required and name not in self._properties:
                    raise AttributeError('Property %s is required but not set.' % name)

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
            raise AttributeError('Attribute %s cannot be set.' % name)
        prop = prop.copy()
        prop._set_value(self, value)
        self._properties[name] = prop

    def _get_values(self):
        """Return a dict of all property names and their values."""
        values = {}
        for name, prop in self._properties.items():
            if not isinstance(prop, Property):
                continue
            value = prop._get_value(self)
            if prop._required and value is None:
                raise ValueError('%s is required but not set.' % name)
            if value is not None:
                values[name] = value
        return values

    @classmethod
    def default_values(cls):
        values = {}
        for name, prop in inspect.getmembers(cls):
            if isinstance(prop, Property) and prop._default is not None:
                values[name] = prop._default
        return values

    @classmethod
    def query(cls, *args, **kwargs):
        """Construct a Query instance to be used to fetch data from database."""
        if cls._gdb is None:
            raise RuntimeError('Intialize the model first. Require an accessible Neo4J')
        kind = kwargs.pop('kind') if 'kind' in kwargs else cls
        qry = query.Query(cls._gdb, kind=kind, **kwargs)
        return qry.filter(*args)

    def _cls_name(self):
        return self.__class__.__name__

    @classmethod
    def _cls_names(cls):
        """Return all class names in the hierarchy except base classes'."""
        names = [cl.__name__ for cl in inspect.getmro(cls)
                if issubclass(cl, Model) and not cl._base_class]
        return set(names)

    def match_dict(self):
        """override this if required in subclass."""
        return {}

    def put(self):
        raise NotImplemented

    @classmethod
    def get(cls, *args, **kwargs):
        raise NotImplemented

    def delete(self):
        raise NotImplemented

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
        if record and issubclass(cls, Model):
            entity = record[cls.__name__]
            return cls.entity_to_model(entity)
        return None

    @classmethod
    def dict_to_cypher(cls, name, d):
        """ turn this dict d into a cypher string. name is variable used in cypher query
        """
        filters = []
        for k, v in d.items():
            k = '%s.%s' % (name, k)
            if type(v) == str:
                filters.append('%s="%s"' % (k, v))
            else:
                filters.append('%s=%s' % (k, v))
        return ' AND '.join(filters)

    def __str__(self):
        return '<%s %s>' % (self.__class__.__name__, self._get_values())
    __repr__ = __str__

    @classmethod
    def create_constraints(cls):
        for cl in inspect.getmro(cls):
            if issubclass(cl, Model) and not cl._base_class:
                for name, prop in inspect.getmembers(cl):
                    if isinstance(prop, Property) and prop._indexed:
                        cls._gdb.create_constraint(cl.__name__, name)


class Node(Model):
    """Represent a routing node in graph."""
    name = StringProperty(name='name')

    @classmethod
    def nodes_by_degree(cls, degree=0, at_most=True):
        """ Return all nodes that has at most (or at least) degree number of edges."""
        if at_most:
            op = '<='
        else:
            op = '>='
        query = 'MATCH {name} WHERE ({name})-[r]-() WITH {name}, COUNT(r) as c'\
                'WHERE c {op} {degree} RETRUN {name}'.format(name=cls.__name__, degree=degree, op=op)
        records = cls._gdb.exec_query(query)
        for record in records:
            yield cls.neo4j_to_model(record)

    @classmethod
    def count(cls):
        """Return number of nodes of this class."""
        record = list(cls._gdb.exec_query('MATCH (n) RETURN COUNT(n) as count'))
        if record:
            return record[0]['count']

    @classmethod
    def node_by_id(cls, uid):
        """Return a node by UID."""
        early_filter = {'uid': str(uid)}
        nodes = list(cls.query(early_filter=early_filter).fetch(limit=1))
        if nodes:
            return nodes[0]
        return None

    def put(self):
        """Save to the database."""
        properties = self._get_values()
        match_dict = self.match_dict()
        labels = list(self._cls_names())
        if not labels:
            raise ValueError('No labels associated with this class %s' % self._cls_name())
        record = self._gdb.create_node(labels=labels, match=match_dict, properties=properties)
        if record:
            return self.entity_to_model(record)
        return None

    def delete(self):
        record = self._gdb.delete_node(kind=self.__class__.__name__, match={'uid': self.uid})
        return record is not None

    @classmethod
    def get_or_create(cls, match_dict, **kwargs):
        """Get node based on create if not exist.

        :param match_dict: properties and values to match on
        :param properties: dict of properties and values
        :rtype: An instance of this class
        """
        if 'uid' not in kwargs:
            kwargs['uid'] = UIDProperty.generate()
        for name, value in cls.default_values().items():
            kwargs.setdefault(name, value)
        record = cls._gdb.create_node(
                match=match_dict, labels=list(cls._cls_names()), properties=kwargs)
        if record:
            return cls.entity_to_model(record)
        return None


class Router(Node):
    """A model represents a BGP Router."""
    _base_class = False

    # for a Neighbor routerid is the IP of the neighbor which it uses to establish
    # BGP session with us
    # for a Border routerid has format dp_id@router_ident, ex. 2@router1
    routerid = StringProperty('routerid', indexed=True, required=True)
    # a (mpls) label is used to tunnel packet from on Border to this border
    # we're going to use label stack for mapping packet to the correct outport
    # in ingress router: [vid][label/pathid][ip data] --> egress
    # in egress router: pop label -> pop pathid, write metadata --> FIB table
    label = IntegerProperty('label')

    @classmethod
    def get(cls, routerid):
        routers = list(cls.query(cls.routerid==routerid).fetch(limit=1))
        if routers:
            return routers[0]
        return None

    @classmethod
    def update(cls, routerid, properties):
        """Update a Router. """
        assert type(properties) == dict
        match_dict = {'routerid': routerid, 'label': cls.__name__}
        record = cls._gdb.update_node(match_dict, cls.__name__, properties)
        if record:
            return cls.entity_to_model(record)


class Border(Router):
    """ Represent a border router of the ISP."""
    _base_class = False

    local_as = IntegerProperty(name='local_as')

    def match_dict(self):
        return {'routerid': self.routerid}

    @classmethod
    def get_or_create(cls, routerid, **kwargs):
        kwargs['routerid'] = routerid
        return super(Border, cls).get_or_create(match_dict={'routerid': routerid}, **kwargs)

class Neighbor(Router):
    """Represent a router belonging to a neighbor."""
    _base_class = False

    peer_ip = IPAddressProperty(name='peer_ip', required=True)
    peer_as = IntegerProperty(name='peer_as')
    local_ip = IPAddressProperty(name='local_ip')
    local_as = IntegerProperty(name='local_as')

    def match_dict(self):
        return {'peer_ip': self.peer_ip}

    @classmethod
    def get_or_create(cls, peer_ip, peer_as, local_ip=None, local_as=None, **kwargs):
        kwargs['routerid'] = peer_ip
        kwargs['peer_ip'] = peer_ip
        kwargs['peer_as'] = peer_as
        kwargs['local_as'] = local_as
        kwargs['local_ip'] = local_ip
        return super(Neighbor, cls).get_or_create(match_dict={'routerid': peer_ip}, **kwargs)


class Prefix(Node):
    """Represent a destination prefix."""
    _base_class = False

    prefix = PrefixProperty(name='prefix', required=True, indexed=True)

    def match_dict(self):
        return {'prefix': self.prefix}

    @classmethod
    def get(cls, prefix):
        prefixes = list(cls.query(cls.prefix==prefix).fetch(limit=1))
        if prefixes:
            return prefixes[0]
        return None

    @classmethod
    def get_or_create(cls, prefix, **kwargs):
        kwargs['prefix'] = prefix
        return super(Prefix, cls).get_or_create({'prefix': prefix}, **kwargs)


class Nexthop(Node):
    _base_class = False

    nexthop = IPAddressProperty('nexthop', required=True, indexed=True)
    dp = StringProperty('dp')
    port = StringProperty('port')
    vlan = StringProperty('vlan')

    @classmethod
    def get_or_create(cls, nexthop, **kwargs):
        kwargs['nexthop'] = nexthop
        return super(Nexthop, cls).get_or_create({'nexthop': nexthop}, **kwargs)

    @classmethod
    def get_and_delete(cls, nexthop):
        match = {'nexthop': nexthop}
        record = self._gdb.delete_node(kind=cls.__name__, match=match)
        if record:
            return cls.entity_to_model(record[0]['node'])
        return None

    def match_dict(self):
        return {'nexthop': self.nexthop}


class Edge(Model):
    """Represent an edge in graph. Exist mainly to check type."""
    src = UIDProperty('src', verbose_name='uid of src node', required=True)
    dst = UIDProperty('dst', verbose_name='uid of dst node', required=True)
    state = StringProperty(name='state', default='down')

    @classmethod
    def count(cls):
        record = list(cls._gdb.exec_query(
                'MATCH (n)-[r : {kind}]->() RETURN COUNT(r) as count'.format(cls.__name__)))
        if record:
            return record[0]['count']

    def put(self):
        """Save to the database."""
        properties = self._get_values()
        src = { 'uid': properties.pop('src') }
        dst = { 'uid': properties.pop('dst') }
        kind = self.__class__.__name__
        record = self._gdb.create_link(kind=kind, src=src, dst=dst,
                                       properties=properties)
        if record:
            return self.neo4j_to_model(record)
        return None

    @classmethod
    def get_or_create(cls, src_match, dst_match, **kwargs):
        for attr, value in list(kwargs.items()):
            value = getattr(cls, attr)._validate(value)
            kwargs.pop(attr)
            if value is not None:
                kwargs[attr] = value
        if 'uid' not in kwargs:
            kwargs['uid'] = UIDProperty.generate()
        record = cls._gdb.create_link(cls.__name__, src_match, dst_match, kwargs)
        if record:
            return cls.neo4j_to_model(record)
        return None

    @classmethod
    def update(cls, src_match, dst_match, **kwargs):
        record = cls._gdb.update_link(cls.__name__, src_match, dst_match, kwargs)
        if record:
            return cls.neo4j_to_model(record)
        return None

    def delete(self):
        src = { 'uid': self.src }
        dst = { 'uid': self.dst }
        label = self.__class__.__name__
        ret = self._gdb.delete_link(kind=self.__class__.__name__, src=src, dst=dst)
        return ret is not None

    @classmethod
    def get_and_delete(self, src_match, dst_match):
        record = cls._gdb.delete_link(kind=cls.__name__, src=src_match, dst=dst_match)
        if record:
            return cls.neo4j_to_model(record)
        return None

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
        src_label = None
        dst_label = None
        early_filter = []
        if 'src' in kwargs:
            src = kwargs.pop('src')
            src_label = src.pop('label', None)
            if src:
                early_filter.append(cls.dict_to_cypher('src', src))
        if 'dst' in kwargs:
            dst = kwargs.pop('dst')
            dst_label = dst.pop('label', None)
            if dst:
                early_filter.append(cls.dict_to_cypher('dst', dst))
        early_filter = ' AND '.join(early_filter)
        kind = kwargs.pop('kind') if 'kind' in kwargs else cls
        qry = query.Query(
                cls._gdb, kind=kind, src_label=src_label, dst_label=dst_label,
                early_filter=early_filter, **kwargs)
        return qry.filter(*args)


class Route(Edge):
    """Represent a route."""
    _base_class = False

    local_pref = PreferenceProperty('local_pref', default=100)
    as_path = ListProperty('as_path', default=[])
    origin = OriginProperty('origin', default=1)
    med = MedProperty('med', default=0)
    prefix = PrefixProperty('prefix')

    @classmethod
    def get_or_create(cls, neighbor, prefix, **properties):
        properties['prefix'] = prefix
        src_match = {'nexthop': neighbor, 'label': Nexthop.__name__}
        dst_match = {'prefix': prefix, 'label': Prefix.__name__}
        return super(Route, cls).get_or_create(src_match, dst_match, **properties)

    @classmethod
    def update(cls, nexthop, prefix, **properties):
        src_match = {'nexthop': nexthop, 'label': Nexthop.__name__}
        dst_match = {'prefix': prefix, 'label': Prefix.__name__}
        return super(Route, cls).update(src_match, dst_match, **properties)


class Session(Edge):
    """Represent a BGP session between a Border and a Neighbor."""
    _base_class = False

    @classmethod
    def get_or_create(cls, border, neighbor, **properties):
        src_match = {'routerid': border, 'label': Border.__name__}
        dst_match = {'routerid': neighbor, 'label': Neighbor.__name__}
        return super(Session, cls).get_or_create(src_match, dst_match, **properties)


class Link(Edge):
    """Represent a data-plane link between two Nodes. A link is directional."""
    _base_class = False

    loss = LossProperty('loss', default=0)
    delay = LatencyProperty('delay', default=1)
    bandwidth = BandwidthProperty('bandwidth', default=1)
    utilization = FloatProperty('utilization', default=0)
    dp_id = IntegerProperty(name='dp_id')
    port_name = StringProperty(name='port_name')
    port_no = IntegerProperty(name='port_no')
    vlan_vid = IntegerProperty(name='vlan_vid')

    @classmethod
    def get(cls, uid):
        links = list(cls.query(uid=uid).fetch(limit=1))
        if links:
            return links[0]
        return None

    @classmethod
    def update(cls, src_uid, dst_uid, **properties):
        src_match = {'uid': src_uid}
        dst_match = {'uid': dst_uid}
        return super(Link, cls).update(src_match, dst_match, **properties)


class IntraLink(Link):
    """Represent link Border --> Border."""
    weight = WeightProperty('weight')

    @classmethod
    def get_or_create(cls, border1, border2, **properties):
        src_match = {'routerid': border1, 'label': Border.__name__}
        dst_match = {'routerid': border2, 'label': Border.__name__}
        return super(IntraLink, cls).get_or_create(src_match, dst_match, **properties)


class InterIngress(Link):
    """Represent ingress link Nexthop --> Border."""
    @classmethod
    def get_or_create(cls, nexthop, border, **properties):
        src_match = {'nexthop': nexthop, 'label': Nexthop.__name__}
        dst_match = {'routerid': border, 'label': Border.__name__}
        return super(InterIngress, cls).get_or_create(src_match, dst_match, **properties)


class InterEgress(Link):
    """Represent egress link Border --> Nexthop."""
    cost = CostProperty('cost', verbose_name='transit cost')
    pathid = IntegerProperty(name='pathid', required=True)

    @classmethod
    def get_or_create(cls, border, nexthop, **properties):
        src_match = {'routerid': border, 'label': Border.__name__}
        dst_match = {'nexthop': nexthop, 'label': Nexthop.__name__}
        return super(InterEgress, cls).get_or_create(src_match, dst_match, **properties)


class Advertise(Edge):
    """Represent that a Neighbor has advertised a Nexthop."""
    _base_class = False


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
        raise Exception('Not allowed')

    @classmethod
    def neo4j_to_model(cls, record):
        if record:
            path = dict(record[cls.__name__])
            return path
        return None

    @classmethod
    def query(cls, routerid, prefix, for_peer=False):
        label = Neighbor.__name__ if for_peer else Border.__name__
        src_match = {'routerid': routerid, 'label': label}
        dst_match = {'prefix': prefix, 'label': Prefix.__name__}
        return super(Path, cls).query(src=src_match, dst=dst_match)


class Mapping(Edge):
    """Represent a RIB/FIB entry of a node. A mapping links a node to a prefix"""
    load = FloatProperty(name='load', default=0)
    prefix = PrefixProperty(name='prefix')
    ingress = StringProperty(name='ingress')
    egress = StringProperty(name='egress')
    neighbor = StringProperty(name='neighbor')
    pathid = StringProperty(name='pathid')

    def put(self):
        properties = self._get_values()
        src = { 'uid': properties.pop('src') }
        dst = { 'uid': properties.pop('dst') }
        label = self.__class__.__name__
        record = self._gdb.create_link(label=label, src=src, dst=dst,
                                       properties=properties)
        if record:
            return self.neo4j_to_model(record)

    @classmethod
    def get_or_create(cls, routerid, prefix, properties={}, for_peer=False):
        label = Neighbor.__name__ if for_peer else Border.__name__
        src_match = {'routerid': routerid, 'label': label}
        dst_match = {'prefix': prefix, 'label': Prefix.__name__}
        properties['prefix'] = prefix
        return super(Mapping, cls).get_or_create(src_match, dst_match, **properties)


def initialize(neo4j_uri=None, neo4j_user=None, neo4j_pass=None):
    """Start the interface to graphdb."""
    Model._gdb = graphdb.Neo4J(db_uri=neo4j_uri, db_user=neo4j_user, db_pass=neo4j_pass)
    Border.create_constraints()
    Neighbor.create_constraints()
    Prefix.create_constraints()
    Nexthop.create_constraints()

def warm_up():
    Model._gdb.exec_query('MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN COUNT(n.uid) + COUNT(r.uid);')

def clear():
    Model._gdb.clear_db()


_all_models = {
        'Route': Route,
        'Path': Path,
        'IntraLink': IntraLink,
        'InterEgress': InterEgress,
        'InterIngress': InterIngress,
        'Border': Border,
        'Neighbor': Neighbor,
        'Prefix': Prefix,
        'Nexthop': Nexthop,
        'Session': Session,
        'Advertise': Advertise,
        'Mapping': Mapping}
