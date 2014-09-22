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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.PhoenixTransactSQL.Condition;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.PhoenixTransactSQL.GET_METRIC_AGGREGATE_ONLY_SQL;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.PhoenixTransactSQL.METRICS_AGGREGATE_MINUTE_TABLE_NAME;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.PhoenixTransactSQL.METRICS_RECORD_CACHE_TABLE_NAME;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.PhoenixTransactSQL.METRICS_RECORD_TABLE_NAME;
import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics.timeline.PhoenixTransactSQL.prepareGetMetricsSqlStmt;

public class TimelineMetricAggregatorMinute extends AbstractTimelineAggregator {
  static final Long SLEEP_INTERVAL = 300000l; // 5 mins
  static final Long CHECKPOINT_CUT_OFF_INTERVAL = SLEEP_INTERVAL * 3;
  private static final Log LOG = LogFactory.getLog(TimelineMetricAggregatorMinute.class);

  public TimelineMetricAggregatorMinute(PhoenixHBaseAccessor hBaseAccessor,
                                        String checkpointLocation) {
    super(hBaseAccessor, checkpointLocation);
  }

  @Override
  protected boolean doWork(long startTime, long endTime) {
    LOG.info("Start aggregation cycle @ " + new Date() + ", " +
      "startTime = " + new Date(startTime) + ", endTime = " + new Date(endTime));

    boolean success = true;
    Condition condition = new Condition(null, null, null, null, startTime,
                                        endTime, null, true);
    condition.setNoLimit();
    condition.setFetchSize(RESULTSET_FETCH_SIZE);
    condition.setStatement(String.format(GET_METRIC_AGGREGATE_ONLY_SQL,
      METRICS_RECORD_CACHE_TABLE_NAME));
    condition.addOrderByColumn("METRIC_NAME");
    condition.addOrderByColumn("HOSTNAME");
    condition.addOrderByColumn("TIMESTAMP");

    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      conn = hBaseAccessor.getConnection();
      stmt = prepareGetMetricsSqlStmt(conn, condition);
      LOG.debug("Query issued @: " + new Date());
      ResultSet rs = stmt.executeQuery();
      LOG.debug("Query returned @: " + new Date());
      TimelineMetric existingMetric = null;
      MetricHostAggregate hostAggregate = null;

      Map<TimelineMetric, MetricHostAggregate> hostAggregateMap =
        new HashMap<TimelineMetric, MetricHostAggregate>();

      while (rs.next()) {
        TimelineMetric currentMetric =
          PhoenixHBaseAccessor.getTimelineMetricKeyFromResultSet(rs);
        MetricHostAggregate currentHostAggregate =
          PhoenixHBaseAccessor.getMetricHostAggregateFromResultSet(rs);

        if (existingMetric == null) {
          // First row
          existingMetric = currentMetric;
          hostAggregate = new MetricHostAggregate();
          hostAggregateMap.put(currentMetric, hostAggregate);
        }

        if (existingMetric.equalsExceptTime(currentMetric)) {
          // Recalculate totals with current metric
          hostAggregate.updateAggregates(currentHostAggregate);

        } else {
          // Switched over to a new metric - create new aggregate
          hostAggregate = new MetricHostAggregate();
          hostAggregate.updateAggregates(currentHostAggregate);
          hostAggregateMap.put(currentMetric, hostAggregate);
          existingMetric = currentMetric;
        }
      }

      LOG.info("Saving " + hostAggregateMap.size() + " metric aggregates.");

      hBaseAccessor.saveHostAggregateRecords(hostAggregateMap,
        METRICS_AGGREGATE_MINUTE_TABLE_NAME);

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

  @Override
  protected Long getSleepInterval() {
    return SLEEP_INTERVAL;
  }

  @Override
  protected Long getCheckpointCutOffInterval() {
    return CHECKPOINT_CUT_OFF_INTERVAL;
  }
}
