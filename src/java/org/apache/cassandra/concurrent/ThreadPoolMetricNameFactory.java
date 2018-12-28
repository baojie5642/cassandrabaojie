package apache.cassandra.concurrent;

import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.metrics.ThreadPoolMetrics;

class ThreadPoolMetricNameFactory implements MetricNameFactory
{
    private final String type;
    private final String path;
    private final String poolName;

    ThreadPoolMetricNameFactory(String type, String path, String poolName)
    {
        this.type = type;
        this.path = path;
        this.poolName = poolName;
    }

    public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
    {
        String groupName = ThreadPoolMetrics.class.getPackage().getName();
        StringBuilder mbeanName = new StringBuilder();
        mbeanName.append(groupName).append(":");
        mbeanName.append("type=").append(type);
        mbeanName.append(",path=").append(path);
        mbeanName.append(",scope=").append(poolName);
        mbeanName.append(",name=").append(metricName);

        return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, path + "." + poolName, mbeanName.toString());
    }
}

