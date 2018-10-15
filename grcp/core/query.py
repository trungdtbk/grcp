"""A query interface to Cypher. Used to construct a Cypher query
"""
from . import model

_OPS = frozenset(['=', '!=', '<', '<=', '>', '>=', 'in'])

class Node(object):
    """Base class for a filter"""
    def __eq__(self, other):
        raise NotImplemented

    def __ne__(self, other):
        eq = self.__eq__(other)
        if eq is not NotImplemented:
            eq = not eq
        return eq


class ParameterNode(Node):
    """A parameterized filter node."""

    def __new__(cls, prop, op, params):
        if not isinstance(prop, model.Property):
            raise TypeError('Expected a Property, got %r' % (prop,))
        if op not in _OPS:
            raise TypeError('Expected a valid operator, got %r' % (op,))


class FilterNode(Node):
    """ A filter """

    def __init__(self, name, opsymbol, value):
        """Constructor. Create a filter where opsymbol can be one of =, !=, >, >=,..
        """
        self._name = name
        self._opsymbol = opsymbol
        self._value = value

    def to_cypher(self):
        out = '{name} {op} {value}'
        if isinstance(self._value, str):
            out = '{name} {op} "{value}"'
        return out.format(name=self._name, op=self._opsymbol, value=self._value)

    def __repr__(self):
        return '<%s name=%s op=%s value=%s>' % (
                self.__class__.__name__, self._name, self._opsymbol, self._value)

class FilterByID(FilterNode):
    def to_cypher(self):
        out = 'id({name}) {op} {value}'
        return out.format(name=self._name, op=self._opsymbol, value=self._value)

class ConjunctionNode(Node):

    def __new__(cls, *nodes):
        for node in nodes:
            if not isinstance(node, Node):
                raise TypeError('Expect Node instances, got %r' % node)
        self = super(ConjunctionNode, cls).__new__(cls)
        self._nodes = nodes
        return self

    def to_cypher(self):
        s = ''
        for node in self._nodes:
            s += 'AND (%s) ' % node.to_cypher()
        s = s[3:]
        return s

    __repr__ = to_cypher


class DisjunctionNode(Node):

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
        if isinstance(self._value, str):
            out = '"{value}" {op} {name}'
        return out.format(name=self._name, op=self._opsymbol, value=self._value)

class FilterExclude(FilterNode):
    def to_cypher(self):
        out = 'NOT {value} {op} {name}'
        if isinstance(self._value, str):
            out = 'NOT "{value}" {op} {name}'
        return out.format(name=self._name, op=self._opsymbol, value=self._value)


class PropertyOrder(Node):
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
    """ For query expression """

    def __init__(self, gdb, kind=None, filters=None, orders=None, group_by=None,
                 src_label=None, dst_label=None, early_filter=None):
        """A Query
        Args:
            - mode (str): either 'node' or 'link'
            - kind (str): model kind
            - filter (list): list of FilterNode
            - orders (list): list of Properties
            - optimize (dict): use to set early WHERE to narrow down the search scope
        optimize = {'key': value}
        """
        self.gdb = gdb
        self.kind = kind
        self.filters = filters
        self.orders = orders
        self.group_by = group_by
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
            qry = 'MATCH (src:{src_kind}), (dst:{dst_kind}), (src)-[:{u_kind}*0..1]->(i:{b_kind}), '\
                  'p=shortestpath((i)-[:{i_kind}*0..{maxlen}]->(b:{b_kind})), '\
                  '(b)-[InterEgress:{e_kind}]->(neigh:{n_kind})-[Route:{r_kind}]->(dst) '\
                  'WHERE {early_filter} AND '\
                  'InterEgress.state="up" AND src.state="up" AND b.state="up" AND neigh.state="up" AND Route.state="up" '\
                  'WITH src, dst, neigh, i, b, InterEgress, Route, NODES(p) AS np, RELS(p) AS lp '\
                  'UNWIND CASE WHEN lp = [] THEN [null] ELSE FILTER(x IN lp '\
                  'WHERE type(x)="IntraLink" AND (x.state="up" OR x.state=NULL)) END AS IntraLink '\
                  'WITH {src: src, dst:dst, nodes:[x in np|x.uid]+[neigh.uid, dst.uid], '\
                  'links:[x in lp|x.uid]+[InterEgress.uid,Route.uid], '\
                  '{inter}, {intra}, {route}, neighbor: neigh.peer_ip, ingress: i.router_id, '\
                  'egress: b.router_id, pathid: InterEgress.pathid, nexthop: Route.nexthop } AS {name} {where} {group_by} '\
                  'RETURN {name} {sort}'
            early_filter = '( (neigh.peer_ip <> src.peer_ip AND EXISTS(src.peer_ip)) OR NOT EXISTS(src.peer_ip) )'
            if self.early_filter:
                early_filter = self.early_filter + ' AND ' + early_filter
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
                    ('{intra}', 'IntraLink', intra_props),
                    ('{inter}', 'InterEgress', inter_props),
                    ('{route}', 'Route', route_props)]:
                prop_str = ','.join(['%s: %s.%s' % (name, kind, prop._name) for name, prop in props])
                qry = qry.replace(rep, prop_str)

            for (name, kind) in [
                    ('{src_kind}', self.src_label),
                    ('{dst_kind}', self.dst_label),
                    ('{u_kind}', model.InterIngress.__name__),
                    ('{i_kind}', model.IntraLink.__name__),
                    ('{e_kind}', model.InterEgress.__name__),
                    ('{b_kind}', model.BorderRouter.__name__),
                    ('{n_kind}', model.PeerRouter.__name__),
                    ('{r_kind}', model.Route.__name__),]:
                kind = kind or ''
                qry = qry.replace(name, kind)
            qry = qry.replace('{where}', filter_str)
            qry = qry.replace('{sort}', sort_str)
            qry = qry.replace('{group_by}', '')
            qry = qry.replace('{name}', model.Path.__name__)
            qry = qry.replace('{maxlen}', str(1))
        elif issubclass(self.kind, model.Node):
            if filter_str and self.early_filter:
                filter_str += ' AND ' + self.early_filter
            elif self.early_filter:
                filter_str = 'WHERE ' + self.early_filter
            qry = 'MATCH ({name}:{kind}) {where} RETURN {name} {sort}'
            qry = qry.format(name=kind, kind=kind, where=filter_str, sort=sort_str)
        elif issubclass(self.kind, model.Edge):
            if filter_str and self.early_filter:
                filter_str += ' AND ' + self.early_filter
            elif self.early_filter:
                filter_str = 'WHERE ' + self.early_filter
            src_label = ':' + self.src_label if self.src_label else ''
            dst_label = ':' + self.dst_label if self.dst_label else ''
            if self.kind == model.Edge or self.kind == model.Link:
                link_kind = ''
            else:
                link_kind = ':' + kind
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
            if not isinstance(node, Node):
                raise TypeError('Node instance is required but got %r' % node)
            preds.append(node)
        if not preds:
            pred = None
        elif len(preds) == 1:
            pred = preds[0]
        else:
            pred = ConjunctionNode(*preds)
        return self.__class__(self.gdb, self.kind, filters=pred,
                              orders=self.orders, group_by=self.group_by,
                              src_label=self.src_label, dst_label=self.dst_label,
                              early_filter=self.early_filter)

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
        for record in records:
            yield self.kind.neo4j_to_model(record)

    def count(self):
        # TODO: to_cypher should return Cypher statement with COUNT
        qry = self._to_cypher(count=True)
        record = self.gdb.exec_query(qry)
        return len(list(record))
