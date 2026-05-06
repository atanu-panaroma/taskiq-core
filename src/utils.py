import logging

from taskiq_nats import PullBasedJetStreamBroker
from src.config.redis import redis_client

logger = logging.getLogger(__name__)


async def log_nats_metrics(broker: PullBasedJetStreamBroker):
    """
    Fetches core JetStream stream and connection metrics from the NATS broker.
    """
    try:
        # 1. Stream Metrics
        js = broker.client.jetstream()
        stream_name = broker.stream_name

        stream_info = await js.stream_info(stream_name)
        state = stream_info.state

        # 2. Connection Metrics
        stats = broker.client.stats

        metrics_msg = (
            "\n=== 🌊 NATS JetStream Observability Metrics ===\n"
            f" Stream Name  : {stream_name}\n"
            f" Queue Storage: {state.messages} msgs | {state.bytes} bytes\n"
            f" Consumers    : {state.consumer_count}\n"
            "--- Connection Health ---\n"
            f" Throughput   : IN: {stats.get('in_msgs', 0)} msgs | OUT: {stats.get('out_msgs', 0)} msgs\n"
            f" Reconnects   : {stats.get('reconnects', 0)}\n"
            f" Async Errors : {stats.get('errors', 0)}\n"
            "==============================================="
        )
        logger.info(metrics_msg)
    except Exception as e:
        logger.error(f"Failed to fetch NATS metrics: {e}")


async def log_redis_cache_metrics():
    info = await redis_client.info("all")
    get_cmd_stats = info.get("latency_percentiles_usec_get")
    set_cmd_stats = info.get("latency_percentiles_usec_set")

    p99_get_cmd = get_cmd_stats.get("p99", "N/A")
    p50_get_cmd = get_cmd_stats.get("p50", "N/A")
    p99_set_cmd = set_cmd_stats.get("p99", "N/A")
    p50_set_cmd = set_cmd_stats.get("p50", "N/A")

    # Calculate Cache Hit Rate
    hits = int(info.get("keyspace_hits", 0))
    misses = int(info.get("keyspace_misses", 0))
    total_lookups = hits + misses
    hit_rate = (hits / total_lookups * 100) if total_lookups > 0 else 0.0

    metrics_msg = (
        "\n=== 📊 Redis Cache Observability Metrics ===\n"
        f" Efficiency : {hit_rate:.2f}% Hit Rate ({hits} hits, {misses} misses)\n"
        f" Memory     : {info.get('used_memory_human')} used (Peak: {info.get('used_memory_peak_human')})\n"
        f" Evictions  : {info.get('evicted_keys')} keys evicted due to memory limits\n"
        f" Clients    : {info.get('connected_clients')} active connections\n"
        f" Latency (microseconds) : GET p50 = {p50_get_cmd}, p99 = {p99_get_cmd} | SET p50 = {p50_set_cmd}, p99 = {p99_set_cmd}\n"
        f"=================================================\n"
    )
    return logger.info(metrics_msg)
