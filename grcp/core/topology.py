import logging
import signal
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

class EventLinkStateChange(EventBase):
    pass


class TopologyManager(app_manager.AppBase):

    def __init__(self):
        super(TopologyManager, self).__init__()
        self.name = 'topo_manager'
        model.initialize()
        self.clear()

    def clear(self):
        model.clear_db()

    def handle_signal(self, signum, _):
        if signum == signal.SIGHUP:
            logger.info('resetting topology')
            # TODO: uncomment this causes exception with neo4j bolt driver
            #self.reset()

    def start(self):
        super(TopologyManager, self).start()
        self.controller = RouterController(self)
        signal.signal(signal.SIGHUP, self.handle_signal)
        return eventlet.spawn(self.controller)

    def send_msg(self, router_id, msg):
        logger.debug('send msg to %s' % router_id)
        self.controller.send_msg(router_id, msg)

    def router_up(self, router_id, **kwargs):
        logger.info('router up: %s' % router_id)
        router = model.BorderRouter.get_or_create(
                router_id=router_id, state='up', name=kwargs.get('name', None))
        if router:
            logger.info('added a router to database: %s' % router_id)
            ev = EventRouterUp(router)
            self.send_event_to_observers(ev)
        return router

    def router_down(self, router_id):
        logger.info('router down: %s' % router_id)
        router = model.BorderRouter.update(router_id, state='down')
        if router:
            logger.info('marked a router router in database: %s' % router_id)
            ev = EventRouterDown(router)
            self.send_event_to_observers(ev)
        return router

    def peer_up(self, peer_ip, peer_as, local_ip, local_as):
        logger.info('peer <as=%s, ip=%s> up' % (peer_as, peer_ip))
        peer = model.PeerRouter.get_or_create(peer_ip, peer_as, local_ip, local_as, state='up')
        if peer:
            logger.debug('added a peer to database %s' % peer)
            ev = EventPeerUp(peer)
            self.send_event_to_observers(ev)
        return peer

    def peer_down(self, peer_ip):
        logger.info('peer %s down' % peer_ip)
        peer = model.PeerRouter.update(peer_ip, state='down')
        if peer:
            ev = EventPeerDown(peer)
            self.send_event_to_observers(ev)
        return peer

    def link_update(self, src, dst, **properties):
        logger.info('link %s --> %s changed state to %s' % (src, dst, properties.get('state')))
        link_model = None
        src_ip = None
        dst_ip = None
        if 'router_id' in src and 'router_id' in dst:
            link_model = model.IntraLink
            src_ip = src['router_id']
            dst_ip = dst['router_id']
        elif 'router_id' in src and 'peer_ip' in dst:
            link_model = model.InterEgress
            src_ip = src['router_id']
            dst_ip = dst['peer_ip']
        elif 'peer_ip' in src and 'router_id' in dst:
            link_model = model.InterIngress
            src_ip = src['peer_ip']
            dst_ip = dst['router_id']
        if link_model:
            link = link_model.get_or_create(src_ip, dst_ip, properties)
            if link:
                ev = EventLinkStateChange(link)
                self.send_event_to_observers(ev)
            else:
                print('failed to create link')
            return link
        else:
            print('no link model')

    def route_add(self, peer_ip, prefix, **kwargs):
        kwargs['prefix'] = prefix
        kwargs['state'] = 'up'
        prefix_ = model.Prefix(prefix=prefix).put()
        route = model.Route.get_or_create(peer_ip, prefix, kwargs)
        if route:
            logger.info('added new route to %s by %s' % (prefix, peer_ip))
            ev = EventRouteAdd(route)
            self.send_event_to_observers(ev)
            return route
        logger.error('route to %s creation failed by %s' % (prefix, peer_ip))
        return

    def route_remove(self, peer_ip, prefix):
        """ simply mark the Route relationship as down"""
        route = model.Route.get_or_create(peer_ip, prefix, {'state': 'down'})
        if route:
            logger.info('deleted route to %s by %s' % (prefix, peer_ip))
            ev = EventRouteDel(route)
            self.send_event_to_observers(ev)
        return route

    def link_update_by_id(self, linkid, **attributes):
        link = model.Edge.update(linkid, attributes)
        if link:
            ev = EventLinkStateChange(link)
            self.send_event_to_observers(ev)
        return link

    def get_nodes(self, prop_name=None, prop_value=None, kind=None, limit=None):
        if kind is None:
            kind = model.Node
        assert issubclass(kind, model.Model)
        query = kind.query()
        if prop_name and prop_value:
            query = query.filter(getattr(kind, prop_name)==prop_value)
        nodes = list(query.fetch(limit))
        return nodes

    def get_prefix(self, prefix, put=False):
        return model.Prefix.get_or_create(dict(prefix=prefix))

    def get_route(self, peer_ip, prefix):
        link = list(model.Route.query(
            src={'router_id': peer_ip, 'label': 'PeerRouter'},
            dst={'prefix': prefix, 'label': 'Prefix'}).fetch(limit=1))
        if link:
            return link[0]
        return None

    def create_mapping(self, src_id, prefix, path_info, for_peer=False):
        """ Create a path mapping between a src node (i.e Router or Neighbor) and a prefix. """
        if not path_info:
            return
        if for_peer:
            label = model.PeerRouter.__name__
        else:
            label = model.BorderRouter.__name__
        src = {'id': src_id, 'type': label}
        dst = {'id': prefix, 'type': model.Prefix.__name__}
        path = {
            'ingress': path_info['ingress'],
            'egress': path_info['egress'],
            'neighbor': path_info['neighbor'],
            'pathid': path_info['pathid'],
            'nexthop': path_info['nexthop']}
        mapping = model.Mapping.get_or_create(src_id, prefix, path, for_peer)
        return mapping

    def delete_mapping(self, src_node_id, prefix):
        """Delete a path mapping between a src_node_id and a prefix."""
        pass

