import eventlet
eventlet.monkey_patch()

import os
import logging
import collections

from grcp.app_manager import AppBase
from .controller import RouterController
from .stats import PrometheusQuery
from .event import EventBase
from . import model

logger = logging.getLogger('grcp.topo')

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


class EventLinkStatsChange(EventBase):
    pass


class TopologyManager(AppBase):

    def __init__(self):
        super(TopologyManager, self).__init__()
        self.name = 'topo_manager'
        self.prefixes = collections.defaultdict(set)
        self.nexthops = set()
        self.controller = None
        self.stats_collector = PrometheusQuery(self.link_stats_change_handler)
        model.initialize(
                neo4j_uri=os.environ.get('GRCP_DB_URI'),
                neo4j_user=os.environ.get('GRCP_DB_USER'),
                neo4j_pass=os.environ.get('GRCP_DB_PASS'))
        logger.info('model initialized')
        self.clear()

    def clear(self):
        model.clear()

    def start(self):
        super(TopologyManager, self).start()
        self.controller = RouterController(self)
        eventlet.spawn(self.stats_collector.run)
        return eventlet.spawn(self.controller)

    def link_stats_change_handler(self, link):
        ev = EventLinkStatsChange(link)
        self.send_event_to_observers(ev)

    def send_msg(self, router_id, msg):
        logger.debug('send msg to %s' % router_id)
        self.controller.send_msg(router_id, msg)

    def router_register(self, routerid, **kwargs):
        logger.debug('register router: %s' % routerid)
        if 'state' not in kwargs:
            kwargs['state'] = 'unknown'
        router = model.Border.get_or_create(routerid=routerid, **kwargs)
        if router:
            logger.info('added router to database: %s' % routerid)
            self.send_event_to_observers(EventRouterUp(router))
        return router

    def router_up(self, routerid):
        logger.debug('router up: %s' % routerid)
        router = model.Border.update(routerid, {'state': 'up'})
        if router:
            logger.info('updated router to database: %s' % routerid)
            self.send_event_to_observers(EventRouterUp(router))
        return router

    def router_down(self, routerid):
        logger.debug('router down: %s' % routerid)
        router = model.Border.update(routerid, properties={'state': 'down'})
        if router:
            logger.info('updated router in database: %s' % routerid)
            self.send_event_to_observers(EventRouterDown(router))
        return router

    def peer_up(self, peer_ip, peer_as, local_ip, local_as, state='up'):
        logger.debug('peer <as=%s, ip=%s> up' % (peer_as, peer_ip))
        peer = model.Neighbor.get_or_create(peer_ip, peer_as, local_ip, local_as,
                                            **{'state': state})
        session = model.Session.get_or_create(local_ip, peer_ip, **{'state': 'up'})
        if peer and session:
            logger.info('added peer to database: %s' % peer_ip)
            self.send_event_to_observers(EventPeerUp(peer))
            return peer
        return None

    def peer_down(self, peer_ip):
        logger.debug('peer down: %s' % peer_ip)
        peer = model.Neighbor.update(peer_ip, **{'state': 'down'})
        if peer:
            logger.info('updated peer in database: %s' % peer_ip)
            self.send_event_to_observers(EventPeerDown(peer))
        return peer

    def route_up(self, nexthop, prefix, local_pref=100, med=0, as_path=[], origin=0):
        if nexthop not in self.nexthops:
            self.create_nexthop(nexthop)
        if prefix not in self.prefixes:
            self.create_prefix(prefix)

        if nexthop in self.prefixes[prefix]:
            route = model.Route.update(nexthop, prefix, state='up')
        else:
            route = model.Route.get_or_create(
                    nexthop, prefix, state='up', med=med,
                    origin=origin, as_path=as_path,
                    local_pref=local_pref)
        if route:
            self.prefixes[prefix].add(nexthop)
            logger.info('added route to db: %s via %s' % (prefix, nexthop))
            self.send_event_to_observers(EventRouteAdd(route))
            return route
        logger.error('failed to create route in db: %s via %s' % (prefix, nexthop))

    def route_down(self, peer_ip, nexthop, prefix):
        """ simply mark the Route relationship as down"""
        route = model.Route.update(nexthop, prefix, **{'state': 'down'})
        if route:
            logger.info('updated route in db: %s via %s' % (prefix, nexthop))
            self.send_event_to_observers(EventRouteDel(route))
        return route

    def create_prefix(self, prefix):
        if prefix is None:
            return
        if prefix not in self.prefixes:
            return model.Prefix.get_or_create(prefix, state='up')

    def create_nexthop(self, nexthop):
        if nexthop is None:
            return
        if nexthop not in self.nexthops and model.Nexthop.get_or_create(nexthop, state='up'):
            self.nexthops.add(nexthop)

    def nexthop_up(self, routerid, nexthop, pathid, dp_id, vlan_vid, port_no, port_name):
        self.create_nexthop(nexthop)
        link = model.InterEgress.get_or_create(routerid, nexthop,
                **{'state': 'up', 'pathid': pathid, 'dp_id': dp_id, 'port_no': port_no,
                    'port_name': port_name, 'vlan_vid': vlan_vid})
        if link:
            logger.info('inter_egress link up: %s -> %s' % (routerid, nexthop))
            self.send_event_to_observers(EventLinkUp(link))

    def nexthop_down(self, routerid, nexthop):
        link = model.InterEgress.get_or_create(routerid, nexthop, **{'state': 'down'})
        if link:
            logger.info('inter_egress link down: %s -> %s' % (routerid, nexthop))
            self.send_event_to_observers(EventLinkDown(link))

    def intra_link_up(self, router1, router2, dp, port, vlan, **properties):
        link = model.IntraLink.get_or_create(router1, router2,
                **{'state': 'up', 'dp': dp, 'port': port, 'vlan': vlan})
        if link:
            logger.info('inra_link created in db: %s -> %s' % (router1, router2))
            self.send_event_to_observers(EventLinkUp(link))
        else:
            logger.error('failed to create/update intra_link %s -> %s' % (router1, router2))

    def intra_link_down(self, router1, router2):
        link = model.IntraLink.get_or_create(router1, router2, **{'state': 'down'})
        if link:
            logger.info('inra_link created in db: %s -> %s' % (router1, router2))
            self.send_event_to_observers(EventLinkDown(link))

    def create_mapping(self, routerid, prefix, path_info, for_peer=False):
        """ Create a path mapping between a src node (i.e Router or Neighbor) and a prefix.
        Example of a path_info:
        {
            'ingress': { 'id': 'router1', 'vlan_vid': 100, 'label': 10, 'dp_id': 1 }, # vlan connected to the egress
            'egress': { 'id': 'router2, 'vlan_vid': 200, 'label': 20, 'dp_id': 2 }, # vlan connected to the neighbor
            'neighbor': { 'id': nexthop, 'pathid': 1 },
        }
        """
        if not path_info:
            return
        path = {
                'ingress': path_info['ingress']['id'],
                'egress': path_info['egress']['id'],
                'neighbor': path_info['neighbor']['id'],
                'pathid': path_info['neighbor']['pathid'],
                'state': 'up'}
        mapping = model.Mapping.get_or_create(routerid, prefix, path, for_peer)
        if mapping:
            logger.info('created a mapping: %s (is peer: %s) -> %s' % (routerid, for_peer, prefix))
            ingress = str(path['ingress'])
            egress = str(path['egress'])
            nexthop = str(path['neighbor'])
            # send the mapping command to the ingress router
            self.send_msg(ingress, {
                        'command': 'add_mapping',
                        'routerid': routerid,
                        'prefix': prefix,
                        'nexthop': nexthop,
                        'egress': egress,
                        'pathid': path['pathid'],
                        'for_peer': for_peer})
            # send the mapping command to the egress router
            if ingress != egress:
                self.send_msg(egress, {
                        'command': 'add_tunnel',
                        'routerid': egress,
                        'pathid': path['pathid'],
                        'nexthop': nexthop})
        return mapping

    def delete_mapping(self, src_node_id, prefix):
        """Delete a path mapping between a src_node_id and a prefix."""
        pass
