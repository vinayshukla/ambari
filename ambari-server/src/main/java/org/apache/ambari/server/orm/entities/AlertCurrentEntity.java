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
package org.apache.ambari.server.orm.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.apache.ambari.server.state.MaintenanceState;

/**
 * The {@link AlertCurrentEntity} class represents the most recently received an
 * alert data for a given instance. This class always has an associated matching
 * {@link AlertHistoryEntity} that defines the actual data of the alert.
 * <p/>
 * There will only ever be a single entity for each given
 * {@link AlertDefinitionEntity}.
 */
@Entity
@Table(name = "alert_current")
@NamedQueries({
    @NamedQuery(name = "AlertCurrentEntity.findAll", query = "SELECT alert FROM AlertCurrentEntity alert"),
    @NamedQuery(name = "AlertCurrentEntity.findByService", query = "SELECT alert FROM AlertCurrentEntity alert JOIN alert.alertHistory history WHERE history.clusterId = :clusterId AND history.serviceName = :serviceName"),
    @NamedQuery(name = "AlertCurrentEntity.findByHost", query = "SELECT alert FROM AlertCurrentEntity alert JOIN alert.alertHistory history WHERE history.clusterId = :clusterId AND history.hostName = :hostName") })
public class AlertCurrentEntity {

  @Id
  @Column(name = "alert_id", nullable = false, updatable = false)
  private Long alertId;

  @Column(name = "latest_timestamp", nullable = false)
  private Long latestTimestamp;

  @Column(name = "maintenance_state", length = 255)
  @Enumerated(value = EnumType.STRING)
  private MaintenanceState maintenanceState;

  @Column(name = "original_timestamp", nullable = false)
  private Long originalTimestamp;

  /**
   * Bi-directional one-to-one association to {@link AlertHistoryEntity}
   */
  @OneToOne
  @JoinColumn(name = "alert_id", nullable = false, insertable = false, updatable = false)
  private AlertHistoryEntity alertHistory;

  /**
   * Constructor.
   */
  public AlertCurrentEntity() {
  }

  /**
   * Gets the unique ID for this current alert.
   * 
   * @return the ID (never {@code null}).
   */
  public Long getAlertId() {
    return alertId;
  }

  /**
   * Sets the unique ID for this current alert.
   * 
   * @param alertId
   *          the ID (not {@code null}).
   */
  public void setAlertId(Long alertId) {
    this.alertId = alertId;
  }

  /**
   * Gets the time, in millis, that the last instance of this alert state was
   * received.
   * 
   * @return the time of the most recently received alert data for this instance
   *         (never {@code null}).
   */
  public Long getLatestTimestamp() {
    return latestTimestamp;
  }

  /**
   * Sets the time, in millis, that the last instance of this alert state was
   * received.
   * 
   * @param latestTimestamp
   *          the time of the most recently received alert data for this
   *          instance (never {@code null}).
   */
  public void setLatestTimestamp(Long latestTimestamp) {
    this.latestTimestamp = latestTimestamp;
  }

  /**
   * Gets the current maintenance state for the alert.
   * 
   * @return the current maintenance state (never {@code null}).
   */
  public MaintenanceState getMaintenanceState() {
    return maintenanceState;
  }

  /**
   * Sets the current maintenance state for the alert.
   * 
   * @param maintenanceState
   *          the state to set (not {@code null}).
   */
  public void setMaintenanceState(MaintenanceState maintenanceState) {
    this.maintenanceState = maintenanceState;
  }

  /**
   * Gets the time, in milliseconds, when the alert was first received with the
   * current state.
   * 
   * @return the time of the first instance of this alert.
   */
  public Long getOriginalTimestamp() {
    return originalTimestamp;
  }

  /**
   * Sets the time, in milliseconds, when the alert was first received with the
   * current state.
   * 
   * @param originalTimestamp
   *          the time of the first instance of this alert (not {@code null}).
   */
  public void setOriginalTimestamp(Long originalTimestamp) {
    this.originalTimestamp = originalTimestamp;
  }

  /**
   * Gets the associated {@link AlertHistoryEntity} entry for this current alert
   * instance.
   * 
   * @return the most recently received history entry (never {@code null}).
   */
  public AlertHistoryEntity getAlertHistory() {
    return alertHistory;
  }

  /**
   * Gets the associated {@link AlertHistoryEntity} entry for this current alert
   * instance.
   * 
   * @param alertHistory
   *          the most recently received history entry (not {@code null}).
   */
  public void setAlertHistory(AlertHistoryEntity alertHistory) {
    this.alertHistory = alertHistory;
  }

  /**
   *
   */
  @Override
  public boolean equals(Object object) {
    if (this == object)
      return true;

    if (object == null || getClass() != object.getClass())
      return false;

    AlertCurrentEntity that = (AlertCurrentEntity) object;

    if (alertId != null ? !alertId.equals(that.alertId) : that.alertId != null)
      return false;

    return true;
  }

  /**
   *
   */
  @Override
  public int hashCode() {
    int result = null != alertId ? alertId.hashCode() : 0;
    return result;
  }
}