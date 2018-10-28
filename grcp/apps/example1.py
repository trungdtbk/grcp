
from grcp.core.topology import EventRouterUp, EventRouterDown, \
                               EventRouteAdd, EventRouteDel
from grcp.core import model
from grcp.app_manager import AppBase, get_topo_manager, listen_to_ev


class BgpExample1(AppBase):

    def __init__(self):
        super(BgpExample1, self).__init__()
        self.routers = {}
        self.topo = get_topo_manager()

    @listen_to_ev([EventRouterUp])
    def register_router(self, ev):
        router = ev.msg
        print('register router: %s' % router.routerid)
        self.routers[router.routerid] = router

    @listen_to_ev([EventRouteAdd])
    def calculate_route(self, ev):
        route = ev.msg
        prefix = route.prefix
        for routerid in self.routers.keys():
            qry = model.Path.query(routerid=routerid, prefix=prefix)
            qry = qry.order(model.Path.route_pref)
            paths = list(qry.fetch(limit=1))
            print(paths)
            if paths:
                self.topo.create_mapping(routerid=routerid, prefix=prefix, path_info=paths[0])
