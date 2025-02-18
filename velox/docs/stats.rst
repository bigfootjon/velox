==============
Runtime Metric
==============

Runtime metric is used to collect the metrics of important velox runtime events
for monitoring purpose. The collected metrics can provide insights into the
continuous availability and performance analysis of a Velox runtime system. For
instance, the collected data can help automatically generate alerts at an
outage. Velox provides a framework to collect the metrics which consists of
three steps:

**Define**: define the name and type for the metric through DEFINE_METRIC and
DEFINE_HISTOGRAM_METRIC macros. DEFINE_HISTOGRAM_METRIC is used for histogram
metric type and DEFINE_METRIC is used for the other types (see metric type
definition below). BaseStatsReporter provides methods for metric definition.
Register metrics during startup using registerVeloxMetrics() API.

**Record**: record the metric data point using RECORD_METRIC_VALUE and
RECORD_HISTOGRAM_METRIC_VALUE macros when the corresponding event happens.
BaseStatsReporter provides methods for metric recording.

**Export**: aggregates the collected data points based on the defined metrics,
and periodically exports to the backend monitoring service, such as ODS used by
Meta, Apache projects `OpenCensus <https://opencensus.io/>`_  and `Prometheus <https://prometheus.io/>`_ provided by OSS. A derived
implementation of BaseStatsReporter is required to integrate with a specific
monitoring service. The metric aggregation granularity and export interval are
also configured based on the actual used monitoring service.

**Count**: tracks the count of events, such as the number of query failures.

**Sum**: tracks the sum of event data point values, such as sum of query scan
read bytes.

**Avg**: tracks the average of event data point values, such as average of query
execution time.

**Rate**: tracks the sum of event data point values per second, such as the
number of shuffle requests per second.

**Histogram**: tracks the distribution of event data point values, such as query
execution time distribution. The histogram metric divides the entire data range
into a series of adjacent equal-sized intervals or buckets, and then count how
many data values fall into each bucket. DEFINE_HISTOGRAM_STAT specifies the data
range by min/max values, and the number of buckets. Any collected data value
less than min is counted in min bucket, and any one larger than max is counted
in max bucket. It also allows to specify the value percentiles to report for
monitoring. This allows BaseStatsReporter and the backend monitoring service to
optimize the aggregated data storage.

Memory Management
-----------------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - cache_shrink_count
     - Count
     - The number of times that in-memory data cache has been shrunk under
       memory pressure.
   * - cache_shrink_ms
     - Histogram
     - The distribution of cache shrink latency in range of [0, 100s] with 10
       buckets. It is configured to report the latency at P50, P90, P99, and
       P100 percentiles.
   * - memory_reclaim_exec_ms
     - Histogram
     - The distribution of memory reclaim execution time in range of [0, 600s]
       with 20 buckets. It is configured to report latency at P50, P90, P99, and
       P100 percentiles.
   * - memory_reclaim_bytes
     - Sum
     - The sum of reclaimed memory bytes.
   * - memory_reclaim_wait_ms
     - Histogram
     - The distribution of memory reclaim wait time in range of [0, 60s] with 10
       buckets. It is configured to report latency at P50, P90, P99, and P100
       percentiles.
   * - memory_reclaim_wait_timeout_count
     - Count
     - The number of times that the memory reclaim wait timeouts.
   * - memory_non_reclaimable_count
     - Count
     - The number of times that the memory reclaim fails because of
       non-reclaimable section which is an indicator that the memory reservation
       is not sufficient.

Spilling
--------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - spill_max_level_exceeded_count
     - Count
     - The number of times that a spillable operator hits the max spill level
       limit.

Hive Connector
--------------

.. list-table::
   :widths: 40 10 50
   :header-rows: 1

   * - Metric Name
     - Type
     - Description
   * - hive_file_handle_generate_latency_ms
     - Histogram
     - The distribution of hive file open latency in range of [0, 100s] with 10
       buckets. It is configured to report latency at P50, P90, P99, and P100
       percentiles.
