"""A query interface to Cypher. Used to construct a Cypher query
"""
from . import model

ALLOWED_OPS = frozenset(['=', '<>', '<', '<=', '>', '>=', 'in'])

class NodeBase(object):
    """Base class for a filter"""
    def __eq__(self, other):
        raise NotImplemented

    def __ne__(self, other):
        eq = self.__eq__(other)
        if eq is not NotImplemented:
            eq = not eq
        return eq

    def to_cypher(self):
        return ''

    __str__ = to_cypher


class FilterNode(NodeBase):
    """ A filter """

    def __init__(self, name, opsymbol, value):
        """Constructor. Create a filter where opsymbol can be one of =, !=, >, >=,..
        """
        assert opsymbol in ALLOWED_OPS
        self._name = name
        self._opsymbol = opsymbol
        self._value = value

    def to_cypher(self):
        out = '{name} {op} {value}'
        value = '"%s"' % self._value if type(self._value)==str else self._value
        return out.format(name=self._name, op=self._opsymbol, value=value)


class ConjunctionNode(NodeBase):
    def __new__(cls, *nodes):
        for node in nodes:
            if not isinstance(node, NodeBase):
                raise TypeError(
                    'A filter node must be an instance of NodeBase; received: %r' % type(node))
        self = super(ConjunctionNode, cls).__new__(cls)
        self._nodes = nodes
        return self

    def to_cypher(self):
        s = ''
        for node in self._nodes:
            s += 'AND (%s) ' % node.to_cypher()
        s = s[3:]
        return s


class DisjunctionNode(NodeBase):

    def __new__(cls, *nodes):
        self = super(DisjunctionNode, cls).__new__(cls)
        self._nodes = nodes
        return self

    def to_cypher(self):
        s = ''
        for node in self._nodes:
            s += 'OR (%s) ' % node.to_cypher()
        s = s[2:]
        return s

    __repr__ = to_cypher


AND = ConjunctionNode
OR = DisjunctionNode


class FilterInclude(FilterNode):
    def to_cypher(self):
        out = '{value} {op} {name}'
        value = '"%s"' % self._value if type(self._value)==str else self._value
        return out.format(name=self._name, op=self._opsymbol, value=value)


class FilterExclude(FilterNode):
    def to_cypher(self):
        out = 'NOT {value} {op} {name}'
        value = '"%s"' % self._value if type(self._value)==str else self._value
        return out.format(name=self._name, op=self._opsymbol, value=value)


class PropertyOrder(NodeBase):
    ASCENDING = ''
    DESCENDING = 'DESC'

    def __init__(self, name, direction=None, func=None):
        self._name = name
        self._direction = direction or self.ASCENDING
        self._func = func

    def to_cypher(self):
        if self._func:
            s = '%s(%s) %s' % (self._func, self._name, self._direction)
        else:
            s = '%s %s' % (self._name, self._direction)
        return s


class Query(object):
    """Represent a Cypher query expression."""

    def __init__(self, gdb, kind=None, filters=None, orders=None,
                 src_label=None, dst_label=None, early_filter=None):
        """
        A query is a Cypher statement to be executed on a Cypher graph database.
        :param gdb: instance of grcp.core.Neo4j
        :param kind: model class
        """
        self.gdb = gdb
        self.kind = kind
        self.filters = filters
        self.orders = orders
        self.early_filter = early_filter
        self.src_label = src_label
        self.dst_label = dst_label

    def _to_cypher(self, limit=None, count=False):
        kind = self.kind.__name__
        filter_str = 'WHERE %s' % self.filters.to_cypher() if self.filters else ''
        if self.orders:
            sort_str = 'ORDER BY %s' % ', '.join([
                    order.to_cypher() for order in self.orders])
        else:
            sort_str = ''
        qry = ''
        if self.kind == model.Path:
            early_filter = ''
            if self.early_filter:
                early_filter = ' WHERE ' + self.early_filter
            qry = 'MATCH (src: {src_kind} {state:"up"}), (dst: {dst_kind} {state:"up"}), '\
                  '(src)-[session:{in_kind}*0..1 {state:"up"}]-(ingress:{ingress_kind} {state:"up"}), '\
                  '(ingress)-[intra:{intra_kind}*0..1 {state:"up"}]->(egress:{egress_kind} {state:"up"}), '\
                  '(egress)-[inter:{inter_kind} {state:"up"}]->(neigh:{neigh_kind} {state:"up"}), '\
                  '(neigh)-[route:{route_kind} {state:"up"}]->(dst) {early_filter} '\
                  'WITH src, dst, neigh, ingress, egress, inter, route, intra[0] as intra '\
                  'WITH {src: src, dst: dst, {intra}, {inter}, {route}, '\
                  'ingress: {id: ingress.routerid, vlan_vid: intra.vlan_vid, label: ingress.label, dp_id: ingress.dp_id}, '\
                  'egress: {id: egress.routerid, vlan_vid: inter.vlan_vid, label: egress.label, dp_id: egress.dp_id}, '\
                  'neighbor: {id: neigh.nexthop, pathid: inter.pathid} } '\
                  'AS {name} {where} RETURN {name} {sort}'

            qry = qry.replace('{early_filter}', early_filter)
            intra_props = []
            inter_props = []
            route_props = []
            for name, prop in model.Path._SUPPORTED_PROPERTIES.items():
                if 'intra' in name:
                    intra_props.append((name, prop))
                elif 'inter' in name:
                    inter_props.append((name, prop))
                elif 'route' in name:
                    route_props.append((name, prop))
            for rep, kind, props in [
                    ('{intra}', 'intra', intra_props),
                    ('{inter}', 'inter', inter_props),
                    ('{route}', 'route', route_props)]:
                prop_str = ','.join(['%s: %s.%s' % (name, kind, prop._name) for name, prop in props])
                qry = qry.replace(rep, prop_str)

            for (name, kind) in [
                    ('{src_kind}', self.src_label),
                    ('{dst_kind}', self.dst_label),
                    ('{ingress_kind}', model.Border.__name__),
                    ('{in_kind}', model.Session.__name__),
                    ('{intra_kind}', model.IntraLink.__name__),
                    ('{egress_kind}', model.Border.__name__),
                    ('{inter_kind}', model.InterEgress.__name__),
                    ('{neigh_kind}', model.Nexthop.__name__),
                    ('{route_kind}', model.Route.__name__),]:
                kind = kind or ''
                qry = qry.replace(name, kind)
            qry = qry.replace('{where}', filter_str)
            qry = qry.replace('{sort}', sort_str)
            qry = qry.replace('{name}', model.Path.__name__)

        elif issubclass(self.kind, model.Node):
            if filter_str and self.early_filter:
                filter_str = ' AND ' + self.early_filter
            elif self.early_filter:
                filter_str = ' WHERE ' + self.early_filter
            qry = 'MATCH ({name}:{kind}) {where} RETURN {name} {sort}'
            qry = qry.format(name=kind, kind=kind, where=filter_str, sort=sort_str)
        elif issubclass(self.kind, model.Edge):
            if filter_str and self.early_filter:
                filter_str = ' AND ' + self.early_filter
            elif self.early_filter:
                filter_str = ' WHERE ' + self.early_filter
            src_label = ':' + self.src_label if self.src_label else ''
            dst_label = ':' + self.dst_label if self.dst_label else ''
            link_kind = ':' + self.kind.__name__ if self.kind else ''
            qry = 'MATCH (src {src_label} )-[{name} {link_kind}]->(dst {dst_label} ) '\
                  '{where} RETURN src.uid AS src, dst.uid AS dst, {name} {sort}'
            qry = qry.format(
                    name=kind, link_kind=link_kind, where=filter_str, sort=sort_str,
                    src_label=src_label, dst_label=dst_label)
        else:
            raise TypeError('unkown query')
        if limit and qry:
            qry += ' LIMIT %d' % limit
        return qry

    def filter(self, *nodes):
        if not nodes:
            return self
        preds = []
        f = self.filters
        if f:
            preds.append(f)
        for node in nodes:
            if not isinstance(node, NodeBase):
                raise TypeError('Unknown filter instance; received: %r' % node)
            preds.append(node)
        if not preds:
            pred = None
        elif len(preds) == 1:
            pred = preds[0]
        else:
            pred = ConjunctionNode(*preds)
        return self.__class__(self.gdb, self.kind, filters=pred,
                              orders=self.orders, early_filter=self.early_filter,
                              src_label=self.src_label, dst_label=self.dst_label)

    def order(self, *nodes):
        if not nodes:
            return self
        if self.orders is None:
            self.orders = []
        if self.kind == model.Path:
            pass
        for node in nodes:
            if isinstance(node, PropertyOrder):
                self.orders.append(node)
            elif isinstance(node, model.ListProperty):
                self.orders.append(PropertyOrder(node._code_name, func='length'))
            elif isinstance(node, model.Property):
                self.orders.append(PropertyOrder(node._code_name))
        return self

    def fetch(self, limit=None):
        records = self.gdb.exec_query(self._to_cypher(limit))
        return [self.kind.neo4j_to_model(record) for record in records]

    def count(self):
        # TODO: to_cypher should return Cypher statement with COUNT
        qry = self._to_cypher(count=True)
        record = self.gdb.exec_query(qry)
        return len(list(record))
