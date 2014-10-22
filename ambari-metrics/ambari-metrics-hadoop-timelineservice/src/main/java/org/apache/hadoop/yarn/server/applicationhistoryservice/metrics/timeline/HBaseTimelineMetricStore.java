/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetrics;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.conf.YarnConfig;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.PhoenixTransactSQL.Condition;

public class HBaseTimelineMetricStore extends AbstractService
  implements TimelineMetricStore {

  static final Log LOG = LogFactory.getLog(HBaseTimelineMetricStore.class);
  static final String HBASE_CONF = "hbase-site.xml";
  static final String DEFAULT_CHECKPOINT_LOCATION = System.getProperty("java.io.tmpdir");
  static final String AGGREGATOR_CHECKPOINT_FILE =
    "timeline-metrics-aggregator-checkpoint";
  static final String MINUTE_AGGREGATE_ROLLUP_CHECKPOINT_FILE =
    "timeline-metrics-minute-aggregator-checkpoint";
  static final String HOURLY_AGGREGATE_ROLLUP_CHECKPOINT_FILE =
    "timeline-metrics-hourly-aggregator-checkpoint";
  static final String HOURLY_ROLLUP_CHECKPOINT_FILE =
    "timeline-metrics-hourly-checkpoint";
  private PhoenixHBaseAccessor hBaseAccessor;

  /**
   * Construct the service.
   *
   */
  public HBaseTimelineMetricStore() {
    super(HBaseTimelineMetricStore.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    URL resUrl = getClass().getClassLoader().getResource(HBASE_CONF);
    LOG.info("Found hbase site configuration: " + resUrl);
    Configuration hbaseConf;
    if (resUrl != null) {
      hbaseConf = new Configuration(true);
      hbaseConf.addResource(resUrl.toURI().toURL());
      hBaseAccessor = new PhoenixHBaseAccessor(hbaseConf);
      hBaseAccessor.initMetricSchema();

      String checkpointLocation = FilenameUtils.concat(conf.get(
        YarnConfig.TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR,
        DEFAULT_CHECKPOINT_LOCATION), AGGREGATOR_CHECKPOINT_FILE);

      // Start the cluster aggregator
      TimelineMetricClusterAggregator clusterAggregator =
        new TimelineMetricClusterAggregator(hBaseAccessor, checkpointLocation);
      Thread aggregatorThread = new Thread(clusterAggregator);
      aggregatorThread.start();

      // Start the hourly cluster aggregator
      String clusterAggregatorHourlyCheckpoint = FilenameUtils.concat(conf.get(
        YarnConfig.TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR,
        DEFAULT_CHECKPOINT_LOCATION), HOURLY_AGGREGATE_ROLLUP_CHECKPOINT_FILE);

      TimelineMetricClusterAggregatorHourly clusterAggregatorHourly = new
        TimelineMetricClusterAggregatorHourly(hBaseAccessor,
        clusterAggregatorHourlyCheckpoint);
      Thread rollupAggregatorThread = new Thread(clusterAggregatorHourly);
      rollupAggregatorThread.start();

      // Start the 5 minute aggregator
      String minuteCheckpoint = FilenameUtils.concat(conf.get(
        YarnConfig.TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR,
        DEFAULT_CHECKPOINT_LOCATION), MINUTE_AGGREGATE_ROLLUP_CHECKPOINT_FILE);
      TimelineMetricAggregatorMinute minuteAggregator = new
        TimelineMetricAggregatorMinute(hBaseAccessor, minuteCheckpoint);

      Thread minuteAggregatorThread = new Thread(minuteAggregator);
      minuteAggregatorThread.start();

      // Start hourly host aggregator
      String hostAggregatorHourlyCheckpoint = FilenameUtils.concat(conf.get(
        YarnConfig.TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR,
        DEFAULT_CHECKPOINT_LOCATION), HOURLY_ROLLUP_CHECKPOINT_FILE);

      TimelineMetricAggregatorHourly aggregatorHourly = new
        TimelineMetricAggregatorHourly(hBaseAccessor, hostAggregatorHourlyCheckpoint);
      Thread aggregatorHourlyThread = new Thread(aggregatorHourly);
      aggregatorHourlyThread.start();

    } else {
      throw new IllegalStateException("Unable to initialize the metrics " +
        "subsystem. No hbase-site present in the classpath.");
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  @Override
  public TimelineMetrics getTimelineMetrics(List<String> metricNames,
      String hostname, String applicationId, String instanceId,
      Long startTime, Long endTime, Integer limit,
      boolean groupedByHosts) throws SQLException, IOException {

    Condition condition = new Condition(metricNames, hostname, applicationId,
      instanceId, startTime, endTime, limit, groupedByHosts);

    if (hostname == null) {
      return hBaseAccessor.getAggregateMetricRecords(condition);
    }

    return hBaseAccessor.getMetricRecords(condition);
  }

  @Override
  public TimelineMetric getTimelineMetric(String metricName, String hostname,
      String applicationId, String instanceId, Long startTime,
      Long endTime, Integer limit)
      throws SQLException, IOException {

    TimelineMetrics metrics = hBaseAccessor.getMetricRecords(
      new Condition(Collections.singletonList(metricName), hostname,
        applicationId, instanceId, startTime, endTime, limit, true)
    );

    TimelineMetric metric = new TimelineMetric();
    List<TimelineMetric> metricList = metrics.getMetrics();

    if (metricList != null && !metricList.isEmpty()) {
      metric.setMetricName(metricList.get(0).getMetricName());
      metric.setAppId(metricList.get(0).getAppId());
      metric.setInstanceId(metricList.get(0).getInstanceId());
      metric.setHostName(metricList.get(0).getHostName());
      // Assumption that metrics are ordered by start time
      metric.setStartTime(metricList.get(0).getStartTime());
      Map<Long, Double> metricRecords = new HashMap<Long, Double>();
      for (TimelineMetric timelineMetric : metricList) {
        metricRecords.putAll(timelineMetric.getMetricValues());
      }
      metric.setMetricValues(metricRecords);
    }

    return metric;
  }


  @Override
  public TimelinePutResponse putMetrics(TimelineMetrics metrics)
    throws SQLException, IOException {

    // Error indicated by the Sql exception
    TimelinePutResponse response = new TimelinePutResponse();

    hBaseAccessor.insertMetricRecords(metrics);

    return response;
  }
}
