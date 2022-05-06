from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY
import requests
import time


# Descriptions for every metric we collect below
METRIC_DOCS = {
    '/amps/host/memory': {
        'free': 'Amount of memory currently free',
        'in_use': 'Amount of memory in use',
        'swap_free': 'Amount of swap currently free',
        'swap_total': 'Total amount of swap'
    },
    '/amps/host/network': {
        'bytes_in': 'Total bytes in',
        'bytes_out': 'Total bytes out'
    },
    '/amps/host/disks': {
        'file_system_free_percent': 'The amount of available storage on a disk'
    },
    '/amps/host/cpus': {
        'iowait_percent': 'Amount of CPU time waiting on I/O',
        'idle_percent': 'Amount of CPU time idle'
    },
    '/amps/instance/processors': {
        'messages_received_per_sec': 'Incoming messages per second from a source',
        'denied_reads': 'Outgoing messages denied due to entitlement (on valid subscription).',
        'denied_writes': 'Incoming messages denied due to entitlement (publishes).',
        'last_active': 'Number of milliseconds since a processor was last active',
        'throttle_count': (
            'Number of times the processor had to wait to add a message to '
            'the processing pipeline due to the instance reaching capacity '
            'limits on the number of in-progress messages. '
            'This metric can indicate resource constraints on AMPS.'
        )
    },
    '/amps/instance/sow': {
        'inserts_per_sec': 'Number of new records added per second',
        'updates_per_sec': 'Number of records updated per second',
        'deletes_per_sec': 'Number of records deleted per second',
        'queries_per_sec': 'Number of queries of the topic per second',
        'insert_count': 'Total count of new records added to the topic',
        'delete_count': 'Total count of records removed from the topic',
        'update_count': 'Total count of updates to records in the topic'
    },
    '/amps/instance/views': {
        'queue_depth': 'Number of messages in the queue for the view to process',
    },
    '/amps/instance/queues': {
        'seconds_behind': 'Age of oldest unacknowledged message in the queue',
        'queue_depth': 'Number of messages in the queue',
        'transferred_in': 'Number of messages that have had ownership transferred to this instance',
        'transferred_out': (
            'Number of messages that this instance has previously owned, but'
            ' granted ownership to another instance'
        ),
        'owned': 'Number of messages currently owned'
    },
    '/amps/instance/replication': {
        'is_connected': 'Whether or not this destination is connected',
        'seconds_behind': (
            'Oldest message in the transaction log not yet sent'
            ' to the downstream instance. Note: this is not an estimate of the'
            ' time required to synchronize the downstream instance.'
        ),
        'messages_out_per_sec': 'Number of messages sent to the destination (per second)'
    },
    '/amps/instance/clients': {
        'transport_rx_queue': 'Number of bytes in the transport receive queue for this connection',
        'transport_tx_queue': 'Number of bytes in the transport send queue for this connection',
        'bytes_in_per_sec': 'Number of bytes per second received from the client',
        'bytes_out_per_sec': 'Number of bytes per second sent to the client',
        'queue_depth_out': 'Number of messages buffered in AMPS for the client',
        'queue_max_latency': 'Oldest message buffered in AMPS for the client'
    }
}


def get_stats(host):
    """
    This method collects stats from AMPS at the moment of the scrape event
    from Prometheus.  It can also handle all the required authentication
    or custom HTTP headers, if needed.
    """
    return requests.get(host + '/amps.json').json()


def get_value(amps, path):
    """
    This function extracts a value from a nested dictionary
    by following the path of the value.
    """
    current_value = amps
    for key in path.split('/')[1:]:
        current_value = current_value[key]

    return current_value


def generate_metric_group(stats, path, metrics, label_key='id', skip_ids=['all']):
    """
    This function generates a group of gauges per each metric collection.
    Example can be a gauge that tracks 'bytes_in' metric an array of network
    interfaces.

    If the metric is not of an array type, (say, /amps/host/memory/in_use) the
    gauge will track a single value from dictionary that contains this metric.
    """
    # get a list of metric group objects say, /amps/host/disks objects
    group_entries = get_value(stats, path)

    metric_name_base = path[1:].replace('/', '_') + '_'
    is_list = isinstance(group_entries, list)

    for metric in metrics:
        # create a gauge for each metric, ex.: /amps/host/network/{id}/bytes_in
        gauge = GaugeMetricFamily(
            metric_name_base + metric,
            METRIC_DOCS.get(path, {}).get(metric, 'No description available'),
            labels=[metric]
        )

        # populate the gauge
        if is_list:
            for entry in group_entries:
                entry_id = entry[label_key]

                # skip entries that we don't need, for example "all" aggregates
                if skip_ids and entry_id in skip_ids:
                    continue

                # adding a metric, say ['eth0'], value for 'bytes_in` key
                gauge.add_metric([entry_id], entry[metric])
        else:
            # adding a metric, say ['in_use'], value for 'in_use` key
            gauge.add_metric([metric], group_entries[metric])

        yield gauge


# create a custom collector
class AMPSCollector(object):
    def __init__(self, host):
        self.host = host

    def collect_host_metrics(self, stats):
        """
        Baseline Host Metrics (/amps/host)

        Typically, a monitoring system will capture, at a minimum, the
        following metrics about host-level performance. Since these related to
        the underlying system rather than AMPS itself, many sites already
        collect the equivalent of these statistics by default.

        This is not a complete list of statisics available for the host, but
        provides a starting point for developing your monitoring plan.
        """

        # Memory metrics
        yield from generate_metric_group(
            stats,
            path='/amps/host/memory',
            metrics=['free', 'in_use', 'swap_free', 'swap_total']
        )

        # Networking metrics (for each interface)
        yield from generate_metric_group(
            stats,
            path='/amps/host/network',
            metrics=['bytes_in', 'bytes_out']
        )

        # File system capacity metrics (for each partition)
        yield from generate_metric_group(
            stats,
            path='/amps/host/disks',
            metrics=['file_system_free_percent']
        )

        # CPU metrics (for each CPU core)
        yield from generate_metric_group(
            stats,
            path='/amps/host/cpus',
            metrics=['iowait_percent', 'idle_percent']
        )

    def collect_message_flow_metrics(self, stats):
        """
        Baseline Message Flow Metrics

        The following metrics monitor overall message flow to the instance.

        This is not a complete list of statisics available for message flow,
        but provides a starting point for developing your monitoring plan.
        """

        yield from generate_metric_group(
            stats,
            path='/amps/instance/processors',
            metrics=[
                'messages_received_per_sec',
                'denied_reads',
                'denied_writes',
                'last_active',
                'throttle_count'
            ]
        )

    def collect_sow_metrics(self, stats):
        """
        SOW Topic Traffic Metrics

        The following metrics monitor message flow for specific topics in the
        SOW (including Topics, Views, ConflatedTopics, and all replication
        models for Queues).

        Depending on your appliction, of course, a given metric may not be
        relevant. (For example, if an application only uses queues, then the
        “update” metrics would not be relevant, since a message can be added to
        the queue or removed from the queue, but cannot be modified while in
        the queue.)
        """

        yield from generate_metric_group(
            stats,
            path='/amps/instance/sow',
            metrics=[
                'inserts_per_sec',
                'updates_per_sec',
                'deletes_per_sec',
                'queries_per_sec',
                'insert_count',
                'delete_count',
                'update_count'
            ]
        )

    def collect_views_metrics(self, stats):
        """
        View-Specific Metrics

        If your application uses views, the following minimal metrics monitor
        traffic for a view. These should be monitored in addition to the
        general SOW topic metrics above.
        """

        yield from generate_metric_group(
            stats,
            path='/amps/instance/views',
            metrics=['queue_depth']
        )

    def collect_queue_metrics(self, stats):
        """
        Queue-Specific Metrics

        If your application uses queues, the following minimal metrics monitor
        traffic for a queue. These should be monitored in addition to the
        general SOW topic metrics above.
        """

        yield from generate_metric_group(
            stats,
            path='/amps/instance/queues',
            metrics=[
                'seconds_behind',
                'queue_depth',

                # watch these extra metrics in grafana for replicated queues
                'transferred_in',
                'transferred_out',
                'owned'
            ]
        )

    def collect_replication_metrics(self, stats):
        """
        Replication Destination Metrics

        The following metrics monitor traffic for a queue. These should be
        monitored in addition to the general topic metrics above.
        """

        yield from generate_metric_group(
            stats,
            path='/amps/instance/replication',
            metrics=['is_connected', 'seconds_behind', 'messages_out_per_sec']
        )

    def collect_application_connection_metrics(self, stats):
        """
        Application Connection Metrics

        The following metrics monitor network activity for a client connection.
        """

        yield from generate_metric_group(
            stats,
            path='/amps/instance/clients',
            metrics=[
                'transport_rx_queue',
                'transport_tx_queue',
                'bytes_in_per_sec',
                'bytes_out_per_sec',
                'queue_depth_out',
                'queue_max_latency'
            ]
        )

    def collect(self):
        # load currents stats from AMPS first
        stats = get_stats(self.host)

        # collect and yield groups of metrics
        yield from self.collect_host_metrics(stats)
        yield from self.collect_message_flow_metrics(stats)
        yield from self.collect_sow_metrics(stats)
        yield from self.collect_views_metrics(stats)
        yield from self.collect_queue_metrics(stats)
        yield from self.collect_replication_metrics(stats)
        yield from self.collect_application_connection_metrics(stats)


# register the AMPS collector with the Prometheus client
REGISTRY.register(AMPSCollector('http://localhost:8085'))


if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)

    # keep the server running
    while True:
        time.sleep(10)
