import logging
import eventlet

from grcp.base import app_manager
from .controller import RouterController
from .event import EventBase
from . import model

logger = logging.getLogger('grcp.topo_manager')

class EventRouterUp(EventBase):
    pass


class EventRouterDown(EventBase):
    pass


class EventPeerUp(EventBase):
    pass


class EventPeerDown(EventBase):
    pass


class EventRouteAdd(EventBase):
    pass


class EventRouteDel(EventBase):
    pass


class EventLinkUp(EventBase):
    pass

class EventLinkDown(EventBase):
    pass

class EventLinkChange(EventBase):
    pass


class TopologyManager(app_manager.AppBase):

    def __init__(self):
        super(TopologyManager, self).__init__()
        self.name = 'topo_manager'
        model.Model.clear_db()
        model.Router.create_constraints()
        model.Neighbor.create_constraints()
        model.Prefix.create_constraints()

    def start(self):
        super(TopologyManager, self).start()
        self.controller = RouterController(self)
        return eventlet.spawn(self.controller)

    def send_msg(self, router_id, msg):
        logger.debug('send msg to %s' % router_id)
        self.controller.send_msg(router_id, msg)

    def router_up(self, router_id, **kwargs):
        logger.info('router up: %s' % router_id)
        router = model.Router(router_id=router_id, **kwargs)
        router = router.put()
        if router:
            ev = EventRouterUp(router)
            self.send_event_to_observers(ev)
        return router

    def router_down(self, router_id, **kwargs):
        logger.info('router down: %s' % router_id)
        router = model.Router(router_id=router_id, **kwargs)
        if router:
            ev = EventRouterDown(router)
            self.send_event_to_observers(ev)
        return router

    def create_link(self, src, dst, attributes={}):
        src_node = None
        dst_node = None
        if 'router_id' in src:
            src_node = self.get_router(src['router_id'])
        else:
            src_node = self.get_peer(src['peer_ip'])
        if 'router_id' in dst:
            dst_node = self.get_router(dst['router_id'])
        else:
            dst_node = self.get_peer(dst['peer_ip'])
        if src_node and dst_node:
            link = self._link_class(src, dst)(src=src_node._uid, dst=dst_node._uid, **attributes)
            return link.put()

    def _link_class(self, src, dst):
        link_model = None
        if 'router_id' in src and 'router_id' in dst:
            link_model = model.IntraLink
        elif 'router_id' in src and 'peer_ip' in dst:
            link_model = model.InterEgress
        elif 'peer_ip' in src and 'router_id' in dst:
            link_model = model.InterIngress
        return link_model

    def get_link(self, kind, src, dst):
        q = model.Edge.query(kind=self._link_class(src, dst), src=src, dst=dst)
        link = list(q.fetch())
        if link:
            return link[0]

    def link_state_change(self, src, dst, attributes={}):
        logger.info('link %s --> %s changed state to %s' % (src, dst, attributes.get('state')))
        if attributes.get('state') == 'up':
            ev_cls = EventLinkUp
        else:
            ev_cls = EventLinkDown
        link = self.create_link(src, dst, attributes)
        if link:
            ev = ev_cls(link)
            self.send_event_to_observers(ev)

    def get_nodes(self, prop_name=None, prop_value=None, kind=None, limit=None):
        if kind is None:
            kind = model.Node
        assert issubclass(kind, model.Model)
        query = kind.query()
        if prop_name and prop_value:
            query = query.filter(getattr(kind, prop_name)==prop_value)
        nodes = list(query.fetch(limit))
        return nodes

    def get_router(self, router_id):
        router = self.get_nodes('router_id', router_id, model.Router)
        if router:
            return router[0]
        return None

    def get_peer(self, peer_ip, kind=None):
        if kind is None:
            kind = model.Neighbor
        peer = self.get_nodes('peer_ip', peer_ip, kind)
        if peer:
            return peer[0]
        return None

    def get_prefix(self, prefix, put=False):
        pref = list(model.Prefix.query(model.Prefix.prefix==prefix).fetch(limit=1))
        if pref:
            return pref[0]
        else:
            pref = model.Prefix(prefix=prefix)
            return pref.put()
        return None

    def get_route(self, peer_ip, prefix):
        link = list(model.Route.query(src={'peer_ip': peer_ip}, dst={'prefix': prefix}).fetch())
        if link:
            return link[0]
        return None

    def _update_peer(self, peer_as, peer_ip, **kwargs):
        m = model.Peer
        peer_type = kwargs.pop('peer_type', None)
        if peer_type == 'customer':
            m = model.Customer
        elif peer_type == 'provider':
                m = model.Provider
        peer = self.get_peer(peer_ip)
        if not peer:
            peer = m(peer_as=peer_as, peer_ip=peer_ip, **kwargs)
        return peer.put()

    def peer_up(self, peer_as, peer_ip, **kwargs):
        logger.info('peer <as=%s, ip=%s> up' % (peer_as, peer_ip))
        kwargs['state'] = 'up'
        peer = self._update_peer(peer_as, peer_ip, **kwargs)
        if peer is not None:
            ev = EventPeerUp(peer)
            self.send_event_to_observers(ev)
        return peer

    def peer_down(self, peer_as, peer_ip):
        logger.info('peer <as=%s, ip=%s> down' % (peer_as, peer_ip))
        peer = self._update_peer(peer_as, peer_ip, state='down')
        if peer:
            ev = EventPeerDown(peer)
            self.send_event_to_observers(ev)
        return peer

    def route_add(self, peer_ip, prefix, **kwargs):
        peer = self.get_peer(peer_ip)
        dest = self.get_prefix(prefix, put=True)
        kwargs['prefix'] = prefix
        if peer and dest:
            route = model.Route(src=peer._uid, dst=dest._uid, **kwargs)
            route = route.put()
            if route:
                logger.info('added new route to %s by %s' % (prefix, peer_ip))
                ev = EventRouteAdd(route)
                self.send_event_to_observers(ev)
                return route
        logger.error('route to %s creation failed by %s' % (prefix, peer_ip))
        return

    def route_remove(self, peer_ip, prefix):
        peer = self.get_peer(peer_ip)
        pref = self.get_prefix(prefix)
        route = self.get_route(peer_ip, prefix)
        if route:
            route.delete()
            logger.info('deleted route to %s by %s' % (prefix, peer_ip))
            ev = EventRouteDel(route)
            self.send_event_to_observers(ev)
        if pref and not pref.count_edges():
            pref.delete()
            logger.info('removed a prefix %s' % (prefix, ))
        return route
