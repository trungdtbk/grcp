#!/usr/bin/env python
import unittest
import logging
import inspect
import time

from .utils import start_neo4j, stop_neo4j, random_ip, random_prefix

from grcp.core import model

start_neo4j()
model.initialize()

class ModelBasicTest(unittest.TestCase):
    """Test basic operation put/get/update on models."""

    def setUp(self):
        model.clear()

    def _node_model_test(self, modelclass):
        for i in range(2):
            if issubclass(modelclass, model.Prefix):
                node = modelclass(prefix=random_prefix())
            elif issubclass(modelclass, model.Nexthop):
                node = modelclass(nexthop=random_ip())
            elif issubclass(modelclass, model.Border):
                node = modelclass(routerid=random_ip())
            elif issubclass(modelclass, model.Neighbor):
                peer_ip = random_ip()
                node = modelclass(routerid=peer_ip, peer_ip=peer_ip, peer_as=1,
                                  local_as=2, local_ip=random_ip())
            else:
                continue
            node_ret = self.put_and_test(node)
            time.sleep(1)

    def put_and_test(self, entity):
        entity_ret = entity.put()
        self.assertTrue(entity_ret is not None)
        for name in entity._properties.keys():
            self.assertEqual(getattr(entity, name), getattr(entity_ret, name))
        return entity_ret

    def get_and_test(self, query, limit=None, expected=1):
        out = list(query.fetch(limit=limit))
        self.assertGreaterEqual(len(out), expected)
        if limit is None:
            return out
        elif limit == 1:
            return out[0]
        else:
            return out[:limit]

    def test_invalid_model(self):
        self.assertRaises(TypeError, model.Model)
        self.assertRaises(TypeError, model.Node)
        self.assertRaises(TypeError, model.Edge)
        self.assertRaises(TypeError, model.Path)
        self.assertRaises(TypeError, model.Link)
        self.assertRaises(TypeError, model.Router)

    def test_node_models(self):
        for m in [model.Border, model.Neighbor, model.Nexthop, model.Prefix]:
            self._node_model_test(m)
        self.assertEqual(len(list(model.Node.query(kind=model.Router).fetch())), 4)
        self.assertEqual(len(list(model.Node.query(kind=model.Nexthop).fetch())), 2)
        self.assertEqual(len(list(model.Node.query(kind=model.Prefix).fetch())), 2)

    def test_route_model(self):
        nexthop = self.put_and_test(model.Nexthop(nexthop='10.0.0.1'))
        prefix = self.put_and_test(model.Prefix(prefix='1.0.0.0/24'))
        attributes = dict(local_pref=100, origin=0, as_path=[1,2,3], med=100)
        route = self.exec_and_test(model.Route.get_or_create,
                                   neighbor='10.0.0.1', prefix='1.0.0.0/24',
                                   properties=attributes)
        self.verify_attributes(route, attributes)

    def test_intra_link_model(self):
        r1 = self.put_and_test(model.Border(routerid='1.1.1.1'))
        r2 = self.put_and_test(model.Border(routerid='2.2.2.2'))
        attributes = {
                'bandwidth': 100, 'loss': 1, 'delay': 10, 'state': 'up',
                'utilization': 0.8, 'dp': 'dp1', 'port': 'port1', 'vlan': 'vlan1'}
        link = model.IntraLink(src=r1.uid, dst=r2.uid, **attributes)
        link = self.put_and_test(link)
        link = self.get_and_test(model.Model.query(kind=model.IntraLink), limit=1)
        self.verify_attributes(link, attributes)

    def verify_attributes(self, entity, attributes):
        for attr, value in attributes.items():
            self.assertEqual(getattr(entity, attr), value)

    def exec_and_test(self, func, *args, **kwargs):
        """Run a function and test if the return is not None."""
        out = func(*args, **kwargs)
        self.assertTrue(out)
        return out

    def test_inter_ingress_model(self):
        nexthop = self.exec_and_test(model.Nexthop.get_or_create, nexthop='1.1.1.1')
        border = self.exec_and_test(model.Border.get_or_create, routerid='2.2.2.2')
        properties = {}
        link = self.exec_and_test(
                model.InterIngress.get_or_create,
                nexthop=nexthop.nexthop, border=border.routerid,
                properties=properties)

    def test_inter_egress_model(self):
        border = self.exec_and_test(model.Border.get_or_create, routerid='2.2.2.2')
        nexthop = self.exec_and_test(model.Nexthop.get_or_create, nexthop='1.1.1.1')
        properties = dict(cost=10, pathid=1, bandwidth=100, utilization=1, loss=1, delay=1,
                          dp='dp1', port='port1', vlan='vlan1')
        link = self.exec_and_test(
                model.InterEgress.get_or_create,
                border=border.routerid, nexthop=nexthop.nexthop, properties=properties)
        self.verify_attributes(link, properties)

    def test_session_model(self):
        border = self.put_and_test(model.Border(routerid='2.2.2.2'))
        neighbor = self.exec_and_test(
                model.Neighbor.get_or_create,
                peer_ip='1.1.1.1', peer_as=1, local_ip='2.2.2.2', local_as=2)
        link = self.put_and_test(model.Session(src=border.uid, dst=neighbor.uid))

    def test_path_query(self):
        border1 = self.exec_and_test(model.Border.get_or_create, routerid='1.1.1.1', properties={'state': 'up'})
        border2 = self.exec_and_test(model.Border.get_or_create, routerid='2.2.2.2', properties={'state': 'up'})
        peer1 = self.exec_and_test(model.Neighbor.get_or_create, peer_ip='3.3.3.3', peer_as=1, properties={'state': 'up'})
        peer2 = self.exec_and_test(model.Neighbor.get_or_create, peer_ip='4.4.4.4', peer_as=2, properties={'state': 'up'})
        nexthop1 = self.exec_and_test(model.Nexthop.get_or_create, nexthop='10.0.0.1', properties={'state': 'up'})
        nexthop2 = self.exec_and_test(model.Nexthop.get_or_create, nexthop='10.0.0.2', properties={'state': 'up'})
        prefix = self.exec_and_test(model.Prefix.get_or_create, prefix='1.0.0.0/24', properties={'state': 'up'})
        route1 = self.put_and_test(model.Route(
                        src=nexthop1.uid, dst=prefix.uid, local_pref=100, as_path=[1,2,3],
                        origin='igp', med=100, prefix=prefix.prefix, state='up'))
        route2 = self.put_and_test(model.Route(
                        src=nexthop2.uid, dst=prefix.uid, local_pref=100, as_path=[1,2,3],
                        origin='igp', med=100, prefix=prefix.prefix, state='up'))
        self.put_and_test(model.IntraLink(src=border1.uid, dst=border2.uid, state='up'))
        self.put_and_test(model.InterEgress(src=border2.uid, dst=nexthop1.uid, pathid=1, state='up', bandwidth=5))
        self.put_and_test(model.InterEgress(src=border2.uid, dst=nexthop2.uid, pathid=2, state='up'))
        self.put_and_test(model.Session(src=border1.uid, dst=peer1.uid, state='up'))
        self.put_and_test(model.Advertise(src=peer2.uid, dst=nexthop1.uid, state='up'))
        self.put_and_test(model.Advertise(src=peer2.uid, dst=nexthop2.uid, state='up'))

        query = model.Path.query(routerid='1.1.1.1', prefix='1.0.0.0/24').order(
                -model.Path.inter_bw, model.Path.route_pref, model.Path.route_aspath)
        paths = self.get_and_test(query, expected=2)
        self.assertGreater(paths[0]['inter_bw'], paths[1]['inter_bw'])
        self.get_and_test(query, expected=1, limit=1)
        query = query.filter(model.Path.inter_bw >= 5, model.Path.route_pref <= 100)
        self.get_and_test(query, expected=1)

    def test_model_link_delete(self):
        border1 = self.put_and_test(model.Border(routerid='1.1.1.1'))
        border2 = self.put_and_test(model.Border(routerid='2.2.2.2'))
        link = self.put_and_test(model.IntraLink(src=border1.uid, dst=border2.uid))
        self.assertTrue(link.delete())
        self.assertTrue(border1.delete())
        self.assertTrue(border2.delete())

if __name__ == '__main__':
    start_neo4j()
    model.initialize()
    unittest.main()
    stop_neo4j()

