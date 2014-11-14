package org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline;

import org.junit.Test;

import static org.apache.hadoop.yarn.server.applicationhistoryservice.metrics
  .timeline.AbstractTimelineAggregator.MetricHostAggregate;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetricHostAggregate {

  @Test
  public void testCreateAggregate() throws Exception {
    // given
    MetricHostAggregate aggregate = createAggregate(3.0, 1.0, 2.0, 2);

    //then
    assertThat(aggregate.getSum()).isEqualTo(3.0);
    assertThat(aggregate.getMin()).isEqualTo(1.0);
    assertThat(aggregate.getMax()).isEqualTo(2.0);
    assertThat(aggregate.getAvg()).isEqualTo(3.0 / 2);
  }

  @Test
  public void testUpdateAggregates() throws Exception {
    // given
    MetricHostAggregate aggregate = createAggregate(3.0, 1.0, 2.0, 2);

    //when
    aggregate.updateAggregates(createAggregate(8.0, 0.5, 7.5, 2));
    aggregate.updateAggregates(createAggregate(1.0, 1.0, 1.0, 1));

    //then
    assertThat(aggregate.getSum()).isEqualTo(12.0);
    assertThat(aggregate.getMin()).isEqualTo(0.5);
    assertThat(aggregate.getMax()).isEqualTo(7.5);
    assertThat(aggregate.getAvg()).isEqualTo((3.0 + 8.0 + 1.0) / 5);
  }

  private MetricHostAggregate createAggregate
    (double sum, double min, double max, int samplesCount) {
    MetricHostAggregate aggregate = new MetricHostAggregate();
    aggregate.setSum(sum);
    aggregate.setMax(max);
    aggregate.setMin(min);
    aggregate.setDeviation(0.0);
    aggregate.setNumberOfSamples(samplesCount);
    return aggregate;
  }
}