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
                 default=None, verbose_name=None, ):
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

    def _validate(self, value):
        return value

    def _get_value(self, entity):
        """Get called when an instance trying to access its member which is an
        instance of the Property class, i.e. the __get__ method is overriden for this."""
        if self._name in entity._values:
            return entity._values[self._name]
        return None

    def _store_value(self, entity, value):
        entity._values[self._name] = value

    def _set_value(self, entity, value):
        if value is not None:
            value = self._validate(value)
        if self._required and value is None:
            raise TypeError('%s requires a not-None value' % self.__class__.__name__)
        self._store_value(entity, value)

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

    def _make_copy(self):
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
        return self._comparison('!=', value)

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
    def _validate(self, value):
        if not isinstance(value, str):
            raise TypeError(
                'StringProperty %s accepts only str value; received %r' % (self._name, value))
        return value


class ListProperty(Property):
    """A ListProperty accepts only list typed value. Items in list can be scalar"""
    def _validate(self, value):
        if not isinstance(value, list):
            raise TypeError(
                'ListProperty %s accepts only list value; received %r' % (self._name, value))
        return value

    def has(self, item):
        """Return query.FilterInclude """
        return query.FilterInclude(self._code_name, 'IN', item)
    HAS = has

    def __neg__(self):
        """Return a descending order on this property."""
        return query.PropertyOrder(self._code_name, query.PropertyOrder.DESCENDING, func='length')

    def __pos__(self):
        return query.PropertyOrder(self._code_name, func='length')


class NumberProperty(Property):
    """exist for type checing."""
    pass


class IntegerProperty(NumberProperty):
    def _validate(self, value):
        if not isinstance(value, int):
            raise TypeError(
                'IntegerProperty %s accepts only int value; received %r' % (self._name, value))
        return value


class FloatProperty(NumberProperty):
    def _validate(self, value):
        if not isinstance(value, (int, float)):
            raise TypeError(
                'FloatProperty %s accepts only float, int, or long  value; received %r' % (self._name, value))
        return float(value)


class BandwidthProperty(FloatProperty):
    """Exist to map a property with a cypher function to get the bandwidth of a Path.
    path bandwidth = min(link bandwidth of all links in the path)
    """
    pass


class LossProperty(FloatProperty):
    """A loss is the sum of loss in all Links in the path."""
    pass


class WeightProperty(FloatProperty):
    pass


class LatencyProperty(FloatProperty):
    pass


class CostProperty(FloatProperty):
    pass


class PrefixProperty(StringProperty):
    def _validate(self, value):
        try:
            prefix = ipaddress.ip_network(value)
            return str(prefix)
        except:
            raise TypeError(
                'PrefixProperty %s accepts only str or ipaddress.ip_network; received %r' % (self._name, value))


class StructuredProperty(Property):
    """A property which references to another model instance."""

    def __init__(self, modelclass, name=None, **kwargs):
        assert issubclass(modelclass, Model), 'StructuredProperty accepts only model.Node class'
        super(StructuredProperty, self).__init__(name=name, **kwargs)
        self._modelclass = modelclass

    def __getattr__(self, name):
        """This is overrided to construct code_name used for DB query.
        Ex:
            def Foo(Model):
                foo = StringProperty('foo')
            def Bar(Model):
                bar = StructuredProperty(Foo, 'foo')
        call Bar.bar.foo return an instance of StringProperty with _code_name = Foo.Foo.foo
        We need that name structure for Cypher query compatibility.
        """
        if hasattr(self._modelclass, name):
            attr = getattr(self._modelclass, name)
            attr._code_name = '.'.join((self._name, attr._name))
            return attr
        raise AttributeError('%s not found' % name)

    def _validate(self, value):
        if not isinstance(value, self._modelclass):
            raise TypeError('Property %s can only accepts instance of %s; received %r' % (
                             self._name, self._modelclass.__name__, value))
        return value

    def _make_copy(self):
        new = self.__class__(
                self._modelclass, self._name, indexed=self._indexed,
                required=self._required, default=self._default, verbose_name=self._verbose_name)
        return new


class Model(object):
    """A model represents an element in the network e.g. Router, Link and so on.
    Put _ before a property to exclude from being added to the database.
    """
    _gdb = graphdb.Neo4J()
    _values = None
    _properties = None
    _uid = -1

    def __new__(cls, *args, **kwargs):
        if cls in [Model, Node, Edge, Link, Neighbor]:
            raise TypeError('Cannot create model %s, only subclass allowed' % cls)
        return super(Model, cls).__new__(cls)

    @classmethod
    def _properties(cls):
        """return a dict of Property"""
        properties = {}
        properties = dict([
                (name, prop) for name, prop in \
                inspect.getmembers(cls) \
                if isinstance(prop, Property)])
        return properties

    def put(self):
        """Save to the database."""
        uid = None
        properties = self._get_values(self._properties)
        properties['uid'] = self._uid
        labels = self._class_names()
        record = self._gdb.create_node(labels=labels, properties=properties)
        if record:
            return self.entity_to_model(record)
        return None

    def __init__(self, *args, **kwargs):
        self._values = {}
        self._properties = {}
        for name, value in kwargs.items():
            prop = getattr(self.__class__, name)
            if not isinstance(prop, Property) and not name.startswith('_'):
                raise TypeError('Cannot set non-property %s' % name)
            setattr(self, name, value)

    def __setattr__(self, name, value):
        if name.startswith('_'):
            self.__dict__[name] = value
            return
        prop = getattr(self.__class__, name)
        if not isinstance(prop, Property):
            raise TypeError('Cannot set non-property %s' % name)
        prop = prop._make_copy()
        prop._set_value(self, value)
        self._properties[name] = prop

    @classmethod
    def query(cls, *args, **kwargs):
        """Construct a Query instance for this model which can be used to fetch data from the db."""
        if 'kind' in kwargs:
            kind = kwargs.pop('kind')
        else:
            kind = cls
        qry = query.Query(cls._gdb, kind=kind, **kwargs)
        qry = qry.filter(*args)
        return qry

    def _get_values(self, properties):
        values = {}
        for name, prop in properties.items():
            if not isinstance(prop, Property) or isinstance(prop, StructuredProperty):
                continue
            value = prop._get_value(self)
            if prop._required and value is None:
                raise TypeError('Property %s is required; but not set' % name)
            if value is not None:
                values[name] = value
        return values

    @classmethod
    def entity_to_model(cls, entity):
        """turn a Neo4J object into a model instance."""
        properties = dict(entity)
        uid = properties.pop('uid')
        if isinstance(entity, cls._gdb.Node):
            modelclass = None
            for label in list(entity.labels):
                if label in _all_models:
                    modelclass = _all_models[label]
                    break
            if modelclass is not None:
                new = modelclass(**properties)
                new._uid = uid
                return new
        elif isinstance(entity, cls._gdb.Relationship):
            modelclass = _all_models[entity.type]
            new = modelclass(**properties)
            new._uid = uid
            return new
        return entity

    @classmethod
    def neo4j_to_model(cls, record):
        """Generate a model instance from a Cypher record."""
        if record and issubclass(cls, Node):
            entity = record[cls.__name__]
            return cls.entity_to_model(entity)
        return None

    @classmethod
    def clear_db(cls):
        cls._gdb.clear_db()

    def _class_name(self):
        return self.__class__.__name__

    def _class_names(self):
        """return all class names in the hierarchy (not 'object' and 'Model') to be used for Node labels."""
        cls = self.__class__
        return [str(cls_.__name__) for cls_ in inspect.getmro(cls)
                if cls_ != object and cls_ != type and cls_ != Model]

    def __repr__(self):
        return '<%s %s>' % (
                self.__class__.__name__,
                ' '.join(['%s=%s' % (name, attr) for name, attr in self._properties.items()]))


class Node(Model):
    """Represent a node in graph."""
    name = StringProperty(name='name')
    location = StringProperty(name='location')

    def count_edges(self):
        src = {'label': self.__class__.__name__, 'uid': self._uid}
        return self._gdb.count_outgoing_edges(src=src)

    def delete(self):
        properties = {'uid': self._uid}
        self._gdb.delete_node([self._class_name()], properties)


class Router(Node):
    """A model represents an internal BGP Router."""
    router_id = StringProperty(name='router_id', verbose_name='router-id', required=True)
    router_ip = StringProperty(name='router_ip')
    router_as = IntegerProperty(name='local_as')
    state = StringProperty(name='state')

    @classmethod
    def create_constraints(cls):
        cls._gdb.create_constraint(cls.__name__, cls.router_id._name)
        cls._gdb.create_index(cls.__name__, 'router_id')
        cls._gdb.create_index(cls.__name__, 'uid')


class Neighbor(Node):
    """Represent a router belonging to a neighbor."""

    peer_as = IntegerProperty(name='peer_as', required=True)
    peer_ip = StringProperty(name='peer_ip', required=True)
    local_as = IntegerProperty(name='local_as')
    local_ip = StringProperty(name='local_ip')
    state = StringProperty(name='state')

    @classmethod
    def create_constraints(cls):
        cls._gdb.create_constraint(cls.__name__, cls.peer_ip._name)
        cls._gdb.create_index(cls.__name__, 'peer_ip')
        cls._gdb.create_index(cls.__name__, 'uid')


class Customer(Neighbor):
    """Represent a customer router."""
    pass


class Peer(Neighbor):
    """Represent a peer router."""
    pass


class Provider(Neighbor):
    """Represent a provider router."""
    pass


class Prefix(Model):
    """Represent a destination prefix."""
    prefix = PrefixProperty(name='prefix', required=True)

    @classmethod
    def create_constraints(cls):
        cls._gdb.create_constraint(cls.__name__, cls.prefix._name)
        cls._gdb.create_index(cls.__name__, 'uid')
        cls._gdb.create_index(cls.__name__, 'prefix')


class Edge(Model):
    """Represent an edge in graph. Exist mainly to check type."""
    name = StringProperty(name='name')

    def put(self, create_dst=False):
        """Save to the database."""
        uid = None
        properties = self._get_values(self._properties)
        if self._uid:
            properties['uid'] = self._uid
        src = { 'uid': self.src._uid }
        dst = self.dst._get_values(self.dst._properties)
        if self.dst._uid:
            dst['uid'] = self.dst._uid
        dst['labels'] = self.dst._class_names()
        label = self.__class__.__name__
        return self._gdb.create_link(label=label, src=src, dst=dst,
                                    properties=properties, create_dst=create_dst)

    def delete(self):
        properties = {'uid': self._uid}
        cls = self.__class__
        src = self.src._get_values(self.src._properties)
        dst = self.dst._get_values(self.dst._properties)
        label = cls.__name__
        return self._gdb.delete_link(name=label, label=label, src=src, dst=dst)

    @classmethod
    def neo4j_to_model(cls, record):
        if record and issubclass(cls, Edge):
            entity = record[cls.__name__]
            new = cls.entity_to_model(entity)
            new.src = Model.entity_to_model(record[cls.src._name])
            new.dst = Model.entity_to_model(record[cls.dst._name])
            return new


class Route(Edge):
    """Represent a route."""
    src = StructuredProperty(Neighbor, 'peer')
    dst = StructuredProperty(Prefix, name='prefix')
    local_pref = IntegerProperty('local_pref')
    aspath_len = IntegerProperty(name='aspath_len')
    as_path = ListProperty('as_path')
    origin = StringProperty('origin')
    med = IntegerProperty('med')
    communities = StringProperty('communities')
    origin_as = IntegerProperty('origin_as')


class Link(Edge):
    """Represent a link between two Nodes."""
    loss = LossProperty('loss')
    delay = LatencyProperty('delay')
    bandwidth = BandwidthProperty('bandwidth')
    utilization = FloatProperty('utilization')


class IntraLink(Link):
    src = StructuredProperty(Router, 'src')
    dst = StructuredProperty(Router, 'dst')
    weight = FloatProperty('weight')


class InterLink(Link):
    src = StructuredProperty(Router, 'src')
    dst = StructuredProperty(Neighbor, 'dst')
    cost = CostProperty('cost', verbose_name='transit cost')


class InterIngress(InterLink):
    """Represent ingress link i.e from Neighbor-->Router """
    pass


class InterEgress(InterLink):
    """Represent egress link i.e from Router-->Neighbor """
    pass


class Mapping(Edge):
    """Represent a RIB/FIB entry of a node. A mapping links a node to a prefix"""
    src = IntegerProperty(name='src_uid')
    dst = IntegerProperty(name='dst_uid')
    nodes = ListProperty(name='nodes')
    links = ListProperty(name='links')


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


class Path(Model, metaclass=PathProperty):

    def put(self):
        raise Exception('method not allowed')

    @classmethod
    def neo4j_to_model(cls, record):
        if record:
            path = dict(record[cls.__name__])
            return path
        return None


_all_models = {
        'Route': Route,
        'Path': Path,
        'IntraLink': IntraLink,
        'InterLink': InterLink,
        'Router': Router,
        'Customer': Customer,
        'Peer': Peer,
        'Provider': Provider,
        'Prefix': Prefix}
