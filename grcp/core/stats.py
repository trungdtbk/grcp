"""
Query statistics from Gauge/Prometheus
"""
import requests
import time
import logging

logger = logging.getLogger('grcp.stats')

from . import model

class PrometheusQuery():

    def __init__(self, handler, prom_host='127.0.0.1', prom_port=9090, interval=60):
        self.handler = handler
        self.endpoint = 'http://%s:%s/api/v1/query' % (prom_host, prom_port)
        self.interval = interval # in second

    def run(self):
        logger.info('PrometheusQuery started')
        while True:
            try:
                self.links_stats_update()
                time.sleep(self.interval)
            except Exception as e:
                logger.error('Error in querying stats: %s' % e)

    def links_stats_update(self):
        links = list(model.InterEgress.query().fetch()) + list(model.IntraLink.query().fetch())
        for link in links:
            logger.debug('Updating stats for link: %s' % link)
            self._link_stats_update(link)

    def _link_stats_update(self, link):
        if not (link.dp_id and link.port_no and link.uid):
            return
        speed = self._link_curr_speed(link.dp_id, link.port_no)
        rate = self._link_tx_rate(link.dp_id, link.port_no)
        if speed and rate:
            utilization = round(rate*8*100/speed, 3)
            if speed != link.bandwidth or utilization != link.utilization:
                link = link.update(link.src, link.dst, bandwidth=speed, utilization=utilization)
                if link and self.handler:
                    self.handler(link)

    def _query(self, dp_id, port_name, stat_key, rate=True):
        dp_id = hex(int(dp_id))
        query = '%s{job="gauge",dp_id="%s",port="%s"}' % (stat_key, dp_id, port_name)
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

    def _link_curr_speed(self, dp_id, port_name):
        return self._query(dp_id, port_name, 'of_port_curr_speed', False)

    def _link_tx_rate(self, dp_id, port_name):
        return self._query(dp_id, port_name, 'of_port_tx_bytes')
