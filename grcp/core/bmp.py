
from oslo_config import cfg

if cfg.CONF.bmp:
    from yabmp.handler import BaseHandler
    from yabmp.service import prepare_service

PEER_DOWN_NOTIFICATION =2
PEER_UP_NOTIFICATION = 3
ROUTE_MONITORING = 0

class MyHandler(BaseHandler):

    def __init__(self, logger, handler):
        super(MyHandler, self).__init__()
        self.logger = logger
        self.handler = handler

    def init(self):
        pass

    def on_message_received(self, peer_host, peer_port, msg, msg_type):
        if msg_type not in [PEER_UP_NOTIFICATION, PEER_UP_NOTIFICATION, ROUTE_MONITORING]:
            return
        peer_header, data = msg
        peer = {}
        peer['peer_as'] = peer_header['as']
        peer['peer_ip'] = peer_header['addr']
        peer['bgp_id'] = peer_header['bgpID']
        if msg_type == ROUTE_MONITORING:
            # received a BGP update
            update_msg = data
            for prefix in update_msg['withdraw']:
                self.handler.remove_route(peer, prefix)
            for prefix in update_msg['nlri']:
                route = {}
                route['origin'] = update_msg['attr'][1]
                route['as_path'] = update_msg['attr'][2]
                route['nexthop'] = update_msg['attr'][3]
                route['med'] = update_msg['attr'][4]
                route['local_pref'] = update_msg['attr'][5]
                route['communities'] = update_msg['attr'][6]
                self.handler.update_route(peer, prefix, route)
        elif msg_type == PEER_UP_NOTIFICATION:
            reason = data
            self.handler.peer_down(peer, reason)
        elif msg_type == PEER_UP_NOTIFICATION:
            local_info = data
            local = {}
            local['local_ip'] = local_info['local_address']
            self.handler.peer_up(peer, local)

    def on_connection_lost(self, peer_host, peer_port):
        pass

    def on_connection_made(self, peer_host, peer_port):
        pass


def start_bmp(handler):
    prepare_service(handler=handler)



