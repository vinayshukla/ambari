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
package org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.PhoenixHBaseAccessor.getMetricClusterAggregateFromResultSet;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.PhoenixHBaseAccessor.getTimelineMetricClusterKeyFromResultSet;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.PhoenixTransactSQL.*;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.TimelineMetricClusterAggregator.TimelineClusterMetric;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.TimelineMetricConfiguration.*;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.TimelineMetricConfiguration
  .CLUSTER_AGGREGATOR_HOUR_CHECKPOINT_CUTOFF_MULTIPLIER;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.TimelineMetricConfiguration.CLUSTER_AGGREGATOR_HOUR_SLEEP_INTERVAL;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.TimelineMetricConfiguration.DEFAULT_CHECKPOINT_LOCATION;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.TimelineMetricConfiguration
  .TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR;

public class TimelineMetricClusterAggregatorHourly extends
  AbstractTimelineAggregator {
  private static final Log LOG = LogFactory.getLog
    (TimelineMetricClusterAggregatorHourly.class);
  private static final String CLUSTER_AGGREGATOR_HOURLY_CHECKPOINT_FILE =
    "timeline-metrics-cluster-aggregator-hourly-checkpoint";
  private final String checkpointLocation;
  private final long sleepIntervalMillis;
  private final Integer checkpointCutOffMultiplier;
  private long checkpointCutOffIntervalMillis;

  public TimelineMetricClusterAggregatorHourly(
    PhoenixHBaseAccessor hBaseAccessor, Configuration metricsConf) {
    super(hBaseAccessor, metricsConf);

    String checkpointDir = metricsConf.get(
      TIMELINE_METRICS_AGGREGATOR_CHECKPOINT_DIR, DEFAULT_CHECKPOINT_LOCATION);

    checkpointLocation = FilenameUtils.concat(checkpointDir,
      CLUSTER_AGGREGATOR_HOURLY_CHECKPOINT_FILE);

    sleepIntervalMillis = SECONDS.toMillis(metricsConf.getLong
      (CLUSTER_AGGREGATOR_HOUR_SLEEP_INTERVAL, 3600l));
    checkpointCutOffIntervalMillis = 7200000l;
    checkpointCutOffMultiplier = metricsConf.getInt
      (CLUSTER_AGGREGATOR_HOUR_CHECKPOINT_CUTOFF_MULTIPLIER, 2);
  }

  @Override
  protected String getCheckpointLocation() {
    return checkpointLocation;
  }

  @Override
  protected boolean doWork(long startTime, long endTime) {
    LOG.info("Start aggregation cycle @ " + new Date() + ", " +
      "startTime = " + new Date(startTime) + ", endTime = " + new Date
      (endTime));

    boolean success = true;
    Condition condition = prepareMetricQueryCondition(startTime, endTime);

    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      conn = hBaseAccessor.getConnection();
      stmt = prepareGetMetricsSqlStmt(conn, condition);

      ResultSet rs = stmt.executeQuery();
      Map<TimelineClusterMetric, MetricHostAggregate> hostAggregateMap =
        aggregateMetricsFromResultSet(rs);

      LOG.info("Saving " + hostAggregateMap.size() + " metric aggregates.");

      hBaseAccessor.saveClusterAggregateHourlyRecords(
        hostAggregateMap,
        METRICS_CLUSTER_AGGREGATE_HOURLY_TABLE_NAME);

    } catch (SQLException e) {
      LOG.error("Exception during aggregating metrics.", e);
      success = false;
    } catch (IOException e) {
      LOG.error("Exception during aggregating metrics.", e);
      success = false;
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          // Ignore
        }
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException sql) {
          // Ignore
        }
      }
    }

    LOG.info("End aggregation cycle @ " + new Date());
    return success;
  }

  // should rewrite from host agg to cluster agg
  //
  private Map<TimelineClusterMetric, MetricHostAggregate>
  aggregateMetricsFromResultSet(ResultSet rs) throws IOException, SQLException {

    TimelineClusterMetric existingMetric = null;
    MetricHostAggregate hostAggregate = null;
    Map<TimelineClusterMetric, MetricHostAggregate> hostAggregateMap =
      new HashMap<TimelineClusterMetric, MetricHostAggregate>();

    while (rs.next()) {
      TimelineClusterMetric currentMetric =
        getTimelineMetricClusterKeyFromResultSet(rs);
      MetricClusterAggregate currentHostAggregate =
        getMetricClusterAggregateFromResultSet(rs);

      if (existingMetric == null) {
        // First row
        existingMetric = currentMetric;
        hostAggregate = new MetricHostAggregate();
        hostAggregateMap.put(currentMetric, hostAggregate);
      }

      if (existingMetric.equalsExceptTime(currentMetric)) {
        // Recalculate totals with current metric
        updateAggregatesFromHost(hostAggregate, currentHostAggregate);

      } else {
        // Switched over to a new metric - save existing
        hostAggregate = new MetricHostAggregate();
        updateAggregatesFromHost(hostAggregate, currentHostAggregate);
        hostAggregateMap.put(currentMetric, hostAggregate);
        existingMetric = currentMetric;
      }

    }

    return hostAggregateMap;
  }

  private void updateAggregatesFromHost(
    MetricHostAggregate agg,
    MetricClusterAggregate currentClusterAggregate) {
    agg.updateMax(currentClusterAggregate.getMax());
    agg.updateMin(currentClusterAggregate.getMin());
    agg.updateSum(currentClusterAggregate.getSum());
    agg.updateNumberOfSamples(currentClusterAggregate.getNumberOfHosts());
  }

  private Condition prepareMetricQueryCondition(long startTime, long endTime) {
    Condition condition = new Condition
      (null, null, null, null, startTime, endTime, null, true);
    condition.setNoLimit();
    condition.setFetchSize(resultsetFetchSize);
    condition.setStatement(GET_CLUSTER_AGGREGATE_SQL);
    condition.addOrderByColumn("METRIC_NAME");
    condition.addOrderByColumn("APP_ID");
    condition.addOrderByColumn("INSTANCE_ID");
    condition.addOrderByColumn("SERVER_TIME");
    return condition;
  }

  @Override
  protected Long getSleepIntervalMillis() {
    return sleepIntervalMillis;
  }

  @Override
  protected Integer getCheckpointCutOffMultiplier() {
    return checkpointCutOffMultiplier;
  }

  @Override
  protected Long getCheckpointCutOffIntervalMillis() {
    return checkpointCutOffIntervalMillis;
  }

  @Override
  protected boolean isDisabled() {
    return metricsConf.getBoolean(CLUSTER_AGGREGATOR_HOUR_DISABLED, false);
  }


}
