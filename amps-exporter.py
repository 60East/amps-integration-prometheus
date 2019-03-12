from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY
import requests
import time


def get_value(amps, path):
    """
    This function extracts a value from a nested dictionary
    by following the path of the value.
    """
    current_value = amps
    for key in path.split('/')[1:]:
        current_value = current_value[key]

    return current_value


# create a custom collector
class AMPSCollector(object):
    def get_stats(self):
        """
        This method collects stats from AMPS at the moment of the scrape event from Prometheus.
        It can also handle all the required authentication / custom HTTP headers, if needed.
        """
        return requests.get('http://localhost:8085/amps.json').json()

    def collect(self):
        # load currents stats from AMPS first
        stats = self.get_stats()

        # update the metrics (suggested):

        # File system capacity (on all filesystems AMPS uses)
        for disk in get_value(stats, '/amps/host/disks'):
            yield GaugeMetricFamily(
                'amps_host_disk_' + disk['id'] + '_file_system_free_percent',
                'The amount of available storage on a disk.',
                value=disk['file_system_free_percent']
            )

        # Host memory capacity
        yield GaugeMetricFamily(
            'amps_host_memory_in_use',
            'The amount of memory currently in use.',
            value=get_value(stats, '/amps/host/memory/in_use')
        )

        # Network statistics (for each network and interface AMPS uses)
        for interface in get_value(stats, '/amps/host/network'):
            yield GaugeMetricFamily(
                'amps_host_network_' + interface['id'] + '_bytes_in',
                'Number of bytes received by the interface.',
                value=interface['bytes_in']
            )
            yield GaugeMetricFamily(
                'amps_host_network_' + interface['id'] + '_bytes_out',
                'Number of bytes transmitted by the interface.',
                value=interface['bytes_out']
            )

        # of connected clients
        yield GaugeMetricFamily(
            'amps_instance_clients',
            'Number of currently connected clients',
            value=len(get_value(stats, '/amps/instance/clients'))
        )

        # of subscriptions
        yield GaugeMetricFamily(
            'amps_instance_subscriptions',
            'Number of currently active subscriptions',
            value=len(get_value(stats, '/amps/instance/subscriptions'))
        )

        # other metrics to consider:
        # Replication seconds_behind (delta from sample to sample)
        # Volume / velocity counters for SOW topics, views, queues


# register the AMPS collector with the Prometheus client
REGISTRY.register(AMPSCollector())


if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)

    # keep the server running
    while True:
        time.sleep(10)
