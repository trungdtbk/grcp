"""
Query statistics from Gauge/Prometheus
"""
import requests
import time

from . import model

class PromStatsCollector():

    def __init__(self, handler, prom_host='127.0.0.1', prom_port=9090):
        self.handler = handler
        self.endpoint = 'http://%s:%s/api/v1/query' % (prom_host, prom_port)
        self.interval = 120 # seconds

    def run(self):
        print('runnning')
        while True:
            self.links_stats_update()
            time.sleep(self.interval)

    def links_stats_update(self):
        for link in list(model.InterEgress.query().fetch()):
            if link.port and link.dp:
                # query stats for this link from Prometheus
                self._link_stats_update(link)
        for link in list(model.IntraLink.query().fetch()):
            if link.port and link.dp:
                self._link_stats_update(link)

    def _link_stats_update(self, link):
        if not (link.dp and link.port and link.uid):
            return
        speed = self._link_curr_speed(link.dp, link.port)
        if speed:
            rate = self._link_tx_rate(link.dp, link.port)
            utilization = rate*8*100/speed
            if speed != link.bandwidth or utilization != link.utilization:
                link = model.Link.update(link.uid, bandwidth=speed, utilization=utilization)
                if link and self.handler:
                    print('link %s has state updated' % link)
                    self.handler(link)

    def _query(self, dp_name, port_name, stat_key, rate=True):
        query = '%s{job="gauge",dp_name="%s",port_name="%s"}' % (stat_key, dp_name, port_name)
        if rate:
            query = 'rate(%s[%dm])' % (query, self.interval/60)
        url = self.endpoint + '?query=%s' % query
        res = requests.get(url)
        if res.status_code == 200:
            result = res.json()
            if result['status'] == 'success' and result['data']['result']:
                timestamp, value = result['data']['result'][0]['value']
                try:
                    value = float(value) if '.' in value else int(value)
                except:
                    pass
                return value

    def _link_curr_speed(self, dp_name, port_name):
        return self._query(dp_name, port_name, 'of_port_curr_speed', False)

    def _link_tx_rate(self, dp_name, port_name):
        return self._query(dp_name, port_name, 'of_port_tx_bytes')
