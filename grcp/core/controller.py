import eventlet
eventlet.monkey_patch()

import traceback
import ipaddress
import logging
import json

from . import messenger
from grcp.cfg import CONF

logger = logging.getLogger('grcp.controller')


class RouterController(object):
    """This class handles communication between the controller and the fBGP module.
    """
    def __init__(self, handler):
        self.handler = handler
        self.messenger = messenger.MessengerServer(self.receive_msg, self.handle_disconnect)
        self.router_to_connection = {}
        self.incomming_queue = eventlet.Queue(512)
        self.outgoing_queue = eventlet.Queue(512)

    def _recv_loop(self, queue):
        while True:
            (clientid, msg) = queue.get()
            self._process_msg(clientid, msg)
            queue.task_done()

    def _send_loop(self, queue):
        while True:
            (conn_id, msg) = queue.get()
            self.messenger.send(conn_id, msg)
            queue.task_done()

    def __call__(self):
        logger.info('The server is listening on %s:%s' % (CONF.bind_host, CONF.bind_port))
        eventlet.spawn(self._send_loop, self.outgoing_queue)
        eventlet.spawn(self._recv_loop, self.incomming_queue)
        self.messenger.run_forever(CONF.bind_port)

    def _process_router_msg(self, conn_id, msg):
        msg_type = msg.get('msg_type')
        routerid = msg.get('routerid')
        if routerid is None:
            return
        attr = {}
        for name in ['state', 'name']:
            if name in msg:
                attr[name] = msg[name]
        if msg_type == 'router_up':
            if routerid in self.router_to_connection:
                self.handler.router_up(routerid)
            else:
                self.handler.router_register(routerid, **attr)
            self.router_to_connection[routerid] = conn_id
        elif msg_type == 'router_down':
            self.router_to_connection[routerid] = None
            self.handler.router_down(routerid)

    def _process_peer_msg(self, msg):
        msg_type = msg.get('msg_type')
        peer_ip = msg.get('peer_ip')
        local_ip = msg.get('local_ip')
        state = msg.get('state', 'down')
        if not (peer_ip and local_ip):
            return
        if msg_type == 'peer_up':
            peer_as = msg.get('peer_as')
            local_as = msg.get('local_as')
            self.handler.peer_up(
                peer_ip=peer_ip, peer_as=peer_as, local_ip=local_ip, local_as=local_as, state=state)
        else:
            self.handler.peer_down(peer_ip, local_ip)

    def _process_update_msg(self, msg):
        peer_ip = msg.get('peer_ip')
        prefix = msg.get('prefix')
        nexthop = msg.get('next_hop')
        if not (nexthop and prefix and peer_ip):
            return
        if msg.get('msg_type') == 'route_up':
            self.handler.route_up(
                    nexthop, prefix,
                    local_pref=msg.get('local_pref', 100),
                    as_path=msg.get('as_path', []),
                    med=msg.get('med', 100))
        else:
            self.handler.route_down(peer_ip, nexthop, prefix)

    def _process_nexthop_msg(self, msg):
        routerid = msg.get('routerid')
        nexthop = msg.get('nexthop')
        if not (routerid and nexthop):
            return
        if msg.get('msg_type') == 'nexthop_up':
            pathid = msg['pathid']
            dp_id = msg.get('dp_id')
            port_name = msg.get('port_name')
            port_no = msg.get('port_no')
            vlan_vid = msg.get('vlan_vid')
            self.handler.nexthop_up(routerid=routerid, nexthop=nexthop,
                                    pathid=pathid, dp_id=dp_id, port_no=port_no,
                                    port_name=port_name, vlan_vid=vlan_vid)
        else:
            self.handler.nexthop_down(routerid, nexthop)

    def _process_link_state_msg(self, msg):
        router1 = msg.get('src')
        router2 = msg.get('dst')
        if not (router1 and router2):
            return
        if msg.get('msg_type') == 'link_up':
            attrs = msg.get('attributes', {})
            if 'dp' in attrs and 'port' in attrs and 'vlan' in attrs:
                self.handler.intra_link_up(router1, router2, **attrs)
            else:
                self.logger.error('Received a mailformed link_up msg: %s' % msg)
        else:
            self.handler.intra_link_down(router1, router2)

    def _process_msg(self, conn_id, msg):
        logger.debug('processing msg %s from %s' % (msg, conn_id))
        try:
            msg = json.loads(msg)
            msg_type = msg.get('msg_type')
            if msg_type in ['route_up', 'route_down']:
                self._process_update_msg(msg)
            elif msg_type in ['router_up', 'router_down']:
                self._process_router_msg(conn_id, msg)
            elif msg_type in ['peer_up', 'peer_down']:
                self._process_peer_msg(msg)
            elif msg_type in ['link_up', 'link_down']:
                self._process_link_state_msg(msg)
            elif msg_type in ['nexthop_up', 'nexthop_down']:
                self._process_nexthop_msg(msg)
        except Exception as e:
            logger.error('error encountered when handling msg %s: %s' % (msg, e))
            traceback.print_exc()

    def receive_msg(self, conn_id, msg):
        #TODO: using queue doesn't work. don't understand why. it hanges up somewhere
        #self.incomming_queue.put((conn_id, msg))
        #eventlet.sleep(0) # this is really important, without it spawn won't work
        self._process_msg(conn_id, msg)

    def handle_disconnect(self, conn_id):
        # mark routers as down
        for router_id in list(self.router_to_connection.keys()):
            if self.router_to_connection[router_id] == conn_id:
                self.router_to_connection.pop(router_id)
                self.incomming_queue.put_nowait((
                    conn_id,
                    '{"msg_type": "router_down", "routerid": "%s"}' % router_id))

    def _get_connection_by_router_id(self, router_id):
        # TODO: race condition may occur
        conn_id = self.router_to_connection.get(router_id, None)
        return conn_id

    def send_msg(self, router_id, msg):
        """send a message to a specific router identified by router_id."""
        conn_id = self._get_connection_by_router_id(router_id)
        if conn_id:
            self.outgoing_queue.put_nowait((conn_id, msg))
