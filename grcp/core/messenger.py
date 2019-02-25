""" Implmentation of the communication module to exchange messages between the
controller and the client routers.
"""
import eventlet
eventlet.monkey_patch(socket=True, time=True)

import os, json, logging

from twisted.internet import protocol, reactor
from twisted.protocols.basic import LineReceiver

logger = logging.getLogger('grcp.messenger')


class RouterControlProtocol(LineReceiver):
    """A protocol for communication with flow-based BGP routers."""

    delimiter = b'\n'

    def __init__(self, factory):
        self.factory = factory

    def connectionMade(self):
        conn_id = self.transport.getPeer()
        logger.debug('client connected: %r' % conn_id)
        self.factory.connection_to_protocol_instance[conn_id] = self

    def connectionLost(self, reason):
        conn_id = self.transport.getPeer()
        self.factory.connection_to_protocol_instance.pop(conn_id, None)
        self.factory.handle_connection_lost(conn_id)

    def lineReceived(self, data):
        logger.debug('received data from the wire: %r' % data)
        self.factory.receive(self.transport.getPeer(), data)

    def send(self, data):
        logger.debug('sent data to the wire: %r' % data)
        self.sendLine(data)


class MessengerServer(protocol.Factory):

    def __init__(self, handle_data_received, handle_connection_lost):
        self.connection_to_protocol_instance = {}
        self.handle_data_received = handle_data_received
        self.handle_connection_lost = handle_connection_lost

    def buildProtocol(self, addr):
        proto = RouterControlProtocol(self)
        self.connection_to_protocol_instance[addr] = proto
        return proto

    def receive(self, conn_id, data):
        if type(data) == bytes:
            data = data.decode('utf-8')
        self.handle_data_received(conn_id, data)

    def send(self, conn_id, data):
        proto = self.connection_to_protocol_instance.get(conn_id, None)
        if proto:
            try:
                if isinstance(data, dict):
                    msg = json.dumps(data).encode('utf-8')
                else:
                    msg = str(data).encode('utf-8')
                reactor.callFromThread(lambda: proto.send(msg))
                return True
            except Exception as e:
                logger.error('error encountered when sending %r to %r: %s' % (conn_id, data, e))
        else:
            logger.error('connection %s is disconnected' % conn_id)
        return False

    def run_forever(self, bind_port):
        reactor.listenTCP(bind_port, self)
        reactor.run()
