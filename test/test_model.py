#!/usr/bin/env python
import unittest
import logging

from utils import start_neo4j, stop_neo4j

start_neo4j()
from grcp.core import model # must be imported after neo4j has started

class ModelTest(unittest.TestCase):

    def setUp(self):
        model.Model.clear_db()

    def _node_model_test(self, modelclass):
        nodes = []
        for i in range(2):
            name = 'N%i' % (i+1)
            node = modelclass(name=name)
            self.assertEqual(node.name, name)
            for attr, value in [
                    ('router_id', None), ('peer_as', 1), ('peer_ip', '10.0.0.1'),
                    ('local_ip', '10.0.0.254'), ('local_as', 2), ('prefix', '1.0.0.0/24')]:
                if value is None:
                    value = '{d}.{d}.{d}.{d}'.format(d=i+1)
                if hasattr(node, attr):
                    setattr(node, attr, value)
                    self.assertEqual(getattr(node, attr), value)
            nodes.append(node)
        for node in nodes:
            node.put()
        nodes = list(model.Model.query(kind=modelclass).fetch())
        self.assertEqual(len(nodes), 2)
        n1 = list(model.Model.query(kind=modelclass).filter(modelclass.name == 'N1').fetch())[0]
        self.assertTrue(isinstance(n1, modelclass))
        self.assertEqual(n1.name, 'N1')
        if hasattr(modelclass, 'router_id'):
            self.assertEqual(n1.router_id, '1.1.1.1')

    def put_and_test(self, entity):
        entity = entity.put()
        self.assertTrue(entity is not None)
        return entity

    def get_and_test(self, query, expected=1):
        out = list(query.fetch())
        self.assertGreaterEqual(len(out), expected)
        return out[0]

    def test_invalid_model(self):
        self.assertRaises(TypeError, model.Node)
        self.assertRaises(TypeError, model.Edge)
        self.assertRaises(TypeError, model.Neighbor)

    def test_all_node_models(self):
        for m in [model.Router, model.Customer, model.Peer, model.Provider]:
            self._node_model_test(m)
        self.assertEqual(len(list(model.Node.query().fetch())), 8) # should have 8 nodes

    def test_route_model(self):
        prefix = self.put_and_test(model.Prefix(prefix='1.0.0.0/24'))
        peer = self.put_and_test(model.Peer(peer_as=1, peer_ip='10.0.0.1'))
        route = model.Route(src=peer, dst=prefix, med=100, local_pref=100, origin='igp', as_path=[1, 2, 3, 4])
        self.put_and_test(route)
        route = self.get_and_test(model.Model.query(kind=model.Route))
        self.assertEqual(route.med, 100)
        self.assertEqual(route.local_pref, 100)
        self.assertEqual(route.src.peer_as, 1)
        self.assertEqual(route.src.peer_ip, '10.0.0.1')
        self.assertEqual(route.dst.prefix, '1.0.0.0/24')
        self.get_and_test(model.Model.query(
                kind=model.Route).filter(model.Route.med <= 100, model.Route.dst.prefix == '1.0.0.0/24'))

    def test_link_model(self):
        r1 = model.Router(name='R1', router_id='1.1.1.1')
        r2 = model.Router(name='R2', router_id='2.2.2.2')
        link = model.IntraLink(src=r1, dst=r2, loss=0.0, delay=5.0, bandwidth=10.0, weight=5.0)
        self.assertFalse(link.put() is not None)
        r1 = self.put_and_test(r1)
        r2 = self.put_and_test(r2)
        link = model.IntraLink(src=r1, dst=r2, loss=0.0, delay=5.0, bandwidth=10.0, weight=5.0)
        for _ in range(2):
            self.put_and_test(link)
        l = self.get_and_test(model.Model.query(kind=model.IntraLink))
        self.assertEqual(l.weight, 5.0)
        self.assertEqual(l.loss, 0.0)
        self.assertEqual(l.src.name, 'R1')
        self.assertEqual(l.dst.name, 'R2')
        link = self.get_and_test(model.Model.query(kind=model.IntraLink).filter(model.IntraLink.src.name == 'R1'))

    def test_path_query(self):
        first = None
        d = self.put_and_test(model.Prefix(prefix='1.0.0.0/24'))
        as_path = []
        routers = []
        for i in range(3):
            name = 'R%d' % (i+1)
            router_id = '{d}.{d}.{d}.{d}'.format(d=(i+1))
            r = self.put_and_test(model.Router(name=name, router_id=router_id))
            p = self.put_and_test(model.Peer(name='Peer%d' % (i+1), peer_as=(i+1), peer_ip=router_id))
            inter = self.put_and_test(model.InterEgress(src=r, dst=p, cost=(i+1), bandwidth=(i+1)))
            as_path.append(i+1)
            route = self.put_and_test(model.Route(src=p, dst=d, med=(i+1), local_pref=(i+1), as_path=as_path))
            if first is None:
                first = r
            else:
                name = '%s->%s' % (first.name, r.name)
                intra = self.put_and_test(model.IntraLink(src=first, dst=r, loss=1, weight=1, bandwidth=3, name=name))
            routers.append(r)
        second = routers[1]
        last = routers[-1]
        name = '%s->%s' % (second.name, last.name)
        intra = self.put_and_test(model.IntraLink(src=second, dst=last, loss=1, weight=1, bandwidth=3, name=name))
        q = model.Path.query().order(model.Path.inter_cost).order(-model.Path.inter_bw).filter(model.Path.route_med <= 2)
        path = self.get_and_test(q)
        self.assertEqual(path['inter_cost'], 1)
        q = model.Path.query().order(-model.Path.inter_cost).order(-model.Path.inter_bw)
        path = self.get_and_test(q)
        self.assertEqual(path['inter_cost'], 3)
        q = model.Path.query().order(-model.Path.route_aspath)
        path = self.get_and_test(q)
        self.assertEqual(path['route_aspath'], [1,2,3])

    def test_ordering(self):
        first = None
        routers = []
        for i in range(3):
            name = 'R%d' % (i+1)
            router_id = '{d}.{d}.{d}.{d}'.format(d=(i+1))
            r = self.put_and_test(model.Router(name=name, router_id=router_id))
            if first is None:
                first = r
            else:
                name = '%s->%s' % (first.name, r.name)
                l = self.put_and_test(model.IntraLink(src=first, dst=r, loss=1, weight=1, bandwidth=3, name=name))
            routers.append(r)
        second = routers[1]
        last = routers[-1]
        name = '%s->%s' % (second.name, last.name)
        l = self.put_and_test( model.IntraLink(src=second, dst=last, loss=2, weight=2, bandwidth=1, name=name))
        router = self.get_and_test(model.Model.query(kind=model.Router).order(+model.Router.name))
        self.assertEqual(router.name, 'R1')
        router = self.get_and_test(model.Model.query(kind=model.Router).order(-model.Router.name))
        self.assertEqual(router.name, 'R3')
        link = self.get_and_test(
                model.IntraLink.query().order(-model.IntraLink.bandwidth).order(
                    model.IntraLink.loss, model.IntraLink.delay), 2)
        self.assertEqual(link.bandwidth, 3)


if __name__ == '__main__':
    unittest.main()
    stop_neo4j()


