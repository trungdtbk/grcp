import traceback
import ipaddress
import logging
import eventlet
import json

eventlet.monkey_patch()

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
        self.outgoing_quque = eventlet.Queue(512)

    def _recv_loop(self):
        while True:
            clientid, msg = self.incomming_queue.get()
            self._process_msg(clientid, msg)

    def _send_loop(self):
        while True:
            conn_id, msg = self.outgoing_quque.get()
            self.messenger.send(conn_id, msg)

    def __call__(self):
        logger.info('The server is listening on %s:%s' % (CONF.bind_host, CONF.bind_port))
        eventlet.spawn(self._recv_loop)
        eventlet.spawn(self._send_loop)
        self.messenger.run_forever(CONF.bind_port)

    def _process_router_msg(self, conn_id, msg):
        msg_type = msg.get('msg_type')
        router_id = msg.get('router_id')
        if router_id is None:
            return
        if msg_type == 'router_up':
            router_ip = msg.get('router_ip')
            name = msg.get('name')
            self.router_to_connection[router_id] = conn_id
            self.handler.router_up(router_id, router_ip=router_ip, state='up', name=name)
        elif msg_type == 'router_down':
            self.router_to_connection.pop(conn_id, None)
            self.handler.router_down(router_id)

    def _process_peer_msg(self, msg):
        msg_type = msg.get('msg_type')
        peer_ip = msg.get('peer_ip')
        peer_as = msg.get('peer_as')
        if not peer_ip or not peer_as:
            return
        local_as = msg.get('local_as')
        local_ip = msg.get('local_ip')
        if msg_type == 'peer_up':
            self.handler.peer_up(peer_as, peer_ip, local_as=local_as, local_ip=local_ip)
        else:
            self.handler.peer_down(peer_as, peer_ip)

    def _process_update_msg(self, msg):
        peer_ip = msg.get('peer_ip')
        update = msg.get('update')
        if not peer_ip or not update:
            return
        if 'announce' in update and 'ipv4 unicast' in update['announce']:
            attr = update['attribute']
            for nexthop, nlris in update['announce']['ipv4 unicast'].items():
                for prefix in nlris:
                    prefix = prefix['nlri']
                    self.handler.route_add(
                            peer_ip, prefix,
                            local_pref=attr.get('local-preference', 100),
                            as_path=attr.get('as-path', []),
                            aspath_len=len(attr.get('as-path', [])),
                            origin=attr.get('origin', '-1'),
                            med=attr.get('med', 100),
                            nexthop=nexthop)
        if 'withdraw' in update and 'ipv4 unicast' in update['withdraw']:
            for prefix in update['withdraw']['ipv4 unicast']:
                prefix = prefix['nlri']
                self.handler.route_remove(peer_ip, prefix)

    def _process_msg(self, conn_id, msg):
        logger.debug('received: %s from %s' % (msg, conn_id))
        try:
            msg = json.loads(msg)
            msg_type = msg.get('msg_type')
            if msg_type == 'update':
                self._process_update_msg(msg)
            elif msg_type == 'router_up' or msg_type == 'router_down':
                self._process_router_msg(conn_id, msg)
            elif msg_type == 'peer_up' or msg_type == 'peer_down':
                self._process_peer_msg(msg)
            elif msg_type in ['link_up', 'link_down', 'link_change']:
                src = msg.get('src')
                dst = msg.get('dst')
                if not src or not dst:
                    return
                self.handler.link_state_change(src, dst, msg.get('attributes', {}))
        except:
            logger.error('error encountered when handling msg')
            traceback.print_exc()

    def receive_msg(self, conn_id, msg):
        self.incomming_queue.put_nowait((conn_id, msg))

    def handle_disconnect(self, conn_id):
        # mark routers as down
        for router_id in list(self.router_to_connection.keys()):
            if self.router_to_connection[router_id] == conn_id:
                self.router_to_connection.pop(router_id)
                self.incomming_queue.put_nowait((
                    conn_id,
                    '{"msg_type": "router_down", "router_id": "%s"}' % router_id))

    def _get_connection_by_router_id(self, router_id):
        # TODO: should use semaphore to avoid race condition
        conn_id = self.router_to_connection.get(router_id, None)
        return conn_id

    def send_msg(self, router_id, msg):
        """send a message to a specific router identified by router_id."""
        conn_id = self._get_connection_by_router_id(router_id)
        if conn_id:
            self.outgoing_quque.put_nowait((conn_id, msg))
