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

var App = require('app');
var stringUtils = require('utils/string_utils');

App.MainAdminStackAndUpgradeController = Em.Controller.extend(App.LocalStorage, {
  name: 'mainAdminStackAndUpgradeController',

  /**
   * @type {boolean}
   */
  isLoaded: false,

  /**
   * @type {object}
   * @default null
   */
  upgradeData: null,

  /**
   * @type {number}
   * @default null
   */
  upgradeId: null,

  /**
   * @type {string}
   * @default null
   */
  upgradeVersion: null,

  /**
   * @type {boolean}
   * @default false
   */
  isDowngrade: false,

  /**
   * version that currently applied to server
   * should be plain object, because stored to localStorage
   * @type {object|null}
   */
  currentVersion: null,

  /**
   * versions to which cluster could be upgraded
   * @type {Array}
   */
  targetVersions: [],

  /**
   * properties that stored to localStorage to resume wizard progress
   */
  wizardStorageProperties: ['upgradeId', 'upgradeVersion', 'currentVersion', 'isDowngrade'],

  /**
   * path to the mock json
   * @type {String}
   */
  mockRepoUrl: '/data/stack_versions/repo_versions_all.json',

  /**
   * api to get RepoVersions
   * @type {String}
   */
  realRepoUrl: function () {
    return App.get('apiPrefix') + App.get('stackVersionURL') +
      '/repository_versions?fields=*,operating_systems/*,operating_systems/repositories/*';
  }.property('App.stackVersionURL'),

  /**
   * path to the mock json
   * @type {String}
   */
  mockStackUrl: '/data/stack_versions/stack_version_all.json',

  /**
   * api to get ClusterStackVersions with repository_versions (use to init data load)
   * @type {String}
   */
  realStackUrl: function () {
    return App.get('apiPrefix') + '/clusters/' + App.get('clusterName') +
      '/stack_versions?fields=*,repository_versions/*,repository_versions/operating_systems/repositories/*';
  }.property('App.clusterName'),

  /**
   * api to get ClusterStackVersions without repository_versions (use to update data)
   * @type {String}
   */
  realUpdateUrl: function () {
    return App.get('apiPrefix') + '/clusters/' + App.get('clusterName') + '/stack_versions?fields=ClusterStackVersions/*';
  }.property('App.clusterName'),

  init: function () {
    this.initDBProperties();
  },

  /**
   * restore data from localStorage
   */
  initDBProperties: function () {
    this.get('wizardStorageProperties').forEach(function (property) {
      if (this.getDBProperty(property)) {
        this.set(property, this.getDBProperty(property));
      }
    }, this);
  },

  /**
   * load all data:
   * - upgrade data
   * - stack versions
   * - repo versions
   */
  load: function () {
    var dfd = $.Deferred();
    var self = this;

    this.loadUpgradeData(true).done(function() {
      self.loadStackVersionsToModel(true).done(function () {
        self.loadRepoVersionsToModel().done(function() {
          var currentVersion = App.StackVersion.find().findProperty('state', 'CURRENT');
          if (currentVersion) {
            self.set('currentVersion', {
              repository_version: currentVersion.get('repositoryVersion.repositoryVersion'),
              repository_name: currentVersion.get('repositoryVersion.displayName')
            });
          }
          dfd.resolve();
        });
      });
    });
    return dfd.promise();
  },

  /**
   * load upgrade tasks by upgrade id
   * @return {$.Deferred}
   * @param {boolean} onlyState
   */
  loadUpgradeData: function (onlyState) {
    var upgradeId = this.get('upgradeId');
    var deferred = $.Deferred();

    if (Em.isNone(upgradeId)) {
      deferred.resolve();
      console.log('Upgrade in INIT state');
    } else {
      App.ajax.send({
        name: (onlyState) ? 'admin.upgrade.state' : 'admin.upgrade.data',
        sender: this,
        data: {
          id: upgradeId
        },
        success: 'loadUpgradeDataSuccessCallback'
      }).then(deferred.resolve);
    }
    return deferred.promise();
  },

  /**
   * parse and push upgrade tasks to controller
   * @param data
   */
  loadUpgradeDataSuccessCallback: function (data) {
    App.set('upgradeState', data.Upgrade.request_status);
    this.setDBProperty('upgradeState', data.Upgrade.request_status);
    if (data.upgrade_groups) {
      this.updateUpgradeData(data);
    }
  },

  /**
   * update data of Upgrade
   * @param {object} newData
   */
  updateUpgradeData: function (newData) {
    var oldData = this.get('upgradeData'),
      groupsMap = {},
      itemsMap = {},
      tasksMap = {};

    if (Em.isNone(oldData)) {
      this.initUpgradeData(newData);
    } else {
      //create entities maps
      newData.upgrade_groups.forEach(function (newGroup) {
        groupsMap[newGroup.UpgradeGroup.group_id] = newGroup.UpgradeGroup;
        newGroup.upgrade_items.forEach(function (item) {
          itemsMap[item.UpgradeItem.stage_id] = item.UpgradeItem;
          item.tasks.forEach(function (task) {
            tasksMap[task.Tasks.id] = task.Tasks;
          });
        })
      });

      //update existed entities with new data
      oldData.upgradeGroups.forEach(function (oldGroup) {
        oldGroup.set('status', groupsMap[oldGroup.get('group_id')].status);
        oldGroup.set('progress_percent', groupsMap[oldGroup.get('group_id')].progress_percent);
        oldGroup.upgradeItems.forEach(function (item) {
          item.set('status', itemsMap[item.get('stage_id')].status);
          item.set('progress_percent', itemsMap[item.get('stage_id')].progress_percent);
          item.tasks.forEach(function (task) {
            task.set('status', tasksMap[task.get('id')].status);
          });
        })
      });
      oldData.set('Upgrade', newData.Upgrade);
    }
  },

  /**
   * change structure of Upgrade
   * In order to maintain nested views in template object should have direct link to its properties, for example
   * item.UpgradeItem.<properties> -> item.<properties>
   * @param {object} newData
   */
  initUpgradeData: function (newData) {
    var upgradeGroups = [];

    //wrap all entities into App.upgradeEntity
    newData.upgrade_groups.forEach(function (newGroup) {
      var oldGroup = App.upgradeEntity.create({type: 'GROUP'}, newGroup.UpgradeGroup);
      var upgradeItems = [];
      newGroup.upgrade_items.forEach(function (item) {
        var oldItem = App.upgradeEntity.create({type: 'ITEM'}, item.UpgradeItem);
        var tasks = [];
        item.tasks.forEach(function (task) {
          tasks.pushObject(App.upgradeEntity.create({type: 'TASK'}, task.Tasks));
        });
        oldItem.set('tasks', tasks);
        upgradeItems.pushObject(oldItem);
      });
      upgradeItems.reverse();
      oldGroup.set('upgradeItems', upgradeItems);
      upgradeGroups.pushObject(oldGroup);
    });
    upgradeGroups.reverse();
    this.set('upgradeData', Em.Object.create({
      upgradeGroups: upgradeGroups,
      Upgrade: newData.Upgrade
    }));
  },

  /**
   * downgrade confirmation popup
   * @param {object} event
   */
  confirmDowngrade: function (event) {
    var self = this;
    var currentVersion = this.get('currentVersion');
    return App.showConfirmationPopup(
      function() {
        self.downgrade.call(self, currentVersion, event);
      },
      Em.I18n.t('admin.stackUpgrade.downgrade.body').format(currentVersion.repository_name),
      null,
      Em.I18n.t('admin.stackUpgrade.dialog.downgrade.header').format(currentVersion.repository_name),
      Em.I18n.t('admin.stackUpgrade.downgrade.proceed')
    );
  },

  /**
   * make call to start downgrade process
   * @param {object} currentVersion
   * @param {object} event
   */
  downgrade: function (currentVersion, event) {
    this.abortUpgrade();
    App.ajax.send({
      name: 'admin.downgrade.start',
      sender: this,
      data: {
        value: currentVersion.repository_version,
        label: currentVersion.repository_name,
        isDowngrade: true
      },
      success: 'upgradeSuccessCallback'
    });
  },

  /**
   * abort upgrade (in order to start Downgrade)
   */
  abortUpgrade: function () {
    return App.ajax.send({
      name: 'admin.upgrade.abort',
      sender: this,
      data: {
        upgradeId: this.get('upgradeId')
      }
    });
  },

  /**
   * make call to start upgrade process and show popup with current progress
   * @param {object} version
   */
  upgrade: function (version) {
    App.ajax.send({
      name: 'admin.upgrade.start',
      sender: this,
      data: version,
      success: 'upgradeSuccessCallback'
    });
    this.setDBProperty('currentVersion', this.get('currentVersion'));
  },

  /**
   * success callback of <code>upgrade()</code>
   * @param {object} data
   */
  upgradeSuccessCallback: function (data, opt, params) {
    this.set('upgradeData', null);
    this.set('upgradeId', data.resources[0].Upgrade.request_id);
    this.set('upgradeVersion', params.label);
    this.set('isDowngrade', !!params.isDowngrade);
    this.setDBProperty('upgradeVersion', params.label);
    this.setDBProperty('upgradeId', data.resources[0].Upgrade.request_id);
    this.setDBProperty('upgradeState', 'PENDING');
    this.setDBProperty('isDowngrade', !!params.isDowngrade);
    App.set('upgradeState', 'PENDING');
    App.clusterStatus.setClusterStatus({
      wizardControllerName: this.get('name'),
      localdb: App.db.data
    });
    this.load();
    this.openUpgradeDialog();
  },

  /**
   * upgrade confirmation popup
   * @param {object} version
   * @return App.ModalPopup
   */
  confirmUpgrade: function (version) {
    var self = this;

    return App.showConfirmationPopup(
      function () {
        self.runPreUpgradeCheck.call(self, version);
      },
      Em.I18n.t('admin.stackUpgrade.upgrade.confirm.body').format(version.get('displayName')),
      null,
      Em.I18n.t('admin.stackUpgrade.dialog.header').format(version.get('displayName'))
    );
  },

  /**
   * send request for pre upgrade check
   * @param version
   */
  runPreUpgradeCheck: function(version) {
    var params = {
      value: version.get('repositoryVersion'),
      label: version.get('displayName')
    };

    if (App.get('supports.preUpgradeCheck')) {
      App.ajax.send({
        name: "admin.rolling_upgrade.pre_upgrade_check",
        sender: this,
        data: params,
        success: "runPreUpgradeCheckSuccess"
      });
    } else {
      this.upgrade(params);
    }
  },

  /**
   * success callback of <code>runPreUpgradeCheckSuccess()</code>
   * if there are some fails - it shows popup else run upgrade
   * @param data {object}
   * @param opt {object}
   * @param params {object}
   * @returns {App.ModalPopup|undefined}
   */
  runPreUpgradeCheckSuccess: function (data, opt, params) {
    if (data.items.someProperty('UpgradeChecks.status', "FAIL")) {
      var header = Em.I18n.t('popup.clusterCheck.Upgrade.header').format(params.label);
      var title = Em.I18n.t('popup.clusterCheck.Upgrade.title');
      var alert = Em.I18n.t('popup.clusterCheck.Upgrade.alert');
      App.showClusterCheckPopup(data, header, title, alert);
    } else {
      this.upgrade(params);
    }
  },

  /**
   * confirmation popup before install repository version
   */
  installRepoVersionConfirmation: function (repo) {
    var self = this;
    return App.showConfirmationPopup(function () {
        self.installRepoVersion(repo);
      },
      Em.I18n.t('admin.stackVersions.version.install.confirm').format(repo.get('displayName'))
    );
  },

  /**
   * sends request to install repoVersion to the cluster
   * and create clusterStackVersion resourse
   * @param {Em.Object} repo
   * @return {$.ajax}
   * @method installRepoVersion
   */
  installRepoVersion: function (repo) {
    var data = {
      ClusterStackVersions: {
        stack: repo.get('stackVersionType'),
        version: repo.get('stackVersionNumber'),
        repository_version: repo.get('repositoryVersion')
      },
      id: repo.get('id')
    };
    return App.ajax.send({
      name: 'admin.stack_version.install.repo_version',
      sender: this,
      data: data,
      success: 'installRepoVersionSuccess'
    });
  },

  /**
   * transform repo data into json for
   * saving changes to repository version
   * @param {Em.Object} repo
   * @returns {{operating_systems: Array}}
   */
  prepareRepoForSaving: function(repo) {
    var repoVersion = { "operating_systems": [] };

    repo.get('operatingSystems').forEach(function (os, k) {
      repoVersion.operating_systems.push({
        "OperatingSystems": {
          "os_type": os.get("osType")
        },
        "repositories": []
      });
      os.get('repositories').forEach(function (repository) {
        repoVersion.operating_systems[k].repositories.push({
          "Repositories": {
            "base_url": repository.get('baseUrl'),
            "repo_id": repository.get('repoId'),
            "repo_name": repository.get('repoName')
          }
        });
      });
    });
    return repoVersion;
  },

  /**
   * perform validation if <code>skip<code> is  false and run save if
   * validation successfull or run save without validation is <code>skip<code> is true
   * @param {Em.Object} repo
   * @param {boolean} skip
   * @returns {$.Deferred}
   */
  saveRepoOS: function (repo, skip) {
    var self = this;
    var deferred = $.Deferred();
    this.validateRepoVersions(repo, skip).done(function(data) {
      if (data.length > 0) {
        deferred.resolve(data);
      } else {
        var repoVersion = self.prepareRepoForSaving(repo);

        App.ajax.send({
          name: 'admin.stack_versions.edit.repo',
          sender: this,
          data: {
            stackName: App.get('currentStackName'),
            stackVersion: App.get('currentStackVersionNumber'),
            repoVersionId: repo.get('repoVersionId'),
            repoVersion: repoVersion
          }
        }).success(function() {
          deferred.resolve([]);
        });
      }
    });
    return deferred.promise();
  },

  /**
   * send request for validation for each repository
   * @param {Em.Object} repo
   * @param {boolean} skip
   * @returns {*}
   */
  validateRepoVersions: function(repo, skip) {
    var deferred = $.Deferred(),
      totalCalls = 0,
      invalidUrls = [];

    if (skip) {
      deferred.resolve(invalidUrls);
    } else {
      repo.get('operatingSystems').forEach(function (os) {
        if (os.get('isSelected')) {
          os.get('repositories').forEach(function (repo) {
            totalCalls++;
            App.ajax.send({
              name: 'admin.stack_versions.validate.repo',
              sender: this,
              data: {
                repo: repo,
                repoId: repo.get('repoId'),
                baseUrl: repo.get('baseUrl'),
                osType: os.get('osType'),
                stackName: App.get('currentStackName'),
                stackVersion: App.get('currentStackVersionNumber')
              }
            })
              .success(function () {
                totalCalls--;
                if (totalCalls === 0) deferred.resolve(invalidUrls);
              })
              .error(function () {
                repo.set('hasError', true);
                invalidUrls.push(repo);
                totalCalls--;
                if (totalCalls === 0) deferred.resolve(invalidUrls);
              });
          });
        } else {
          return deferred.resolve(invalidUrls);
        }
      });
    }
    return deferred.promise();
  },

  /**
   * success callback for <code>installRepoVersion()<code>
   * saves request id to the db
   * @param data
   * @param opt
   * @param params
   * @method installStackVersionSuccess
   */
  installRepoVersionSuccess: function (data, opt, params) {
    App.db.set('repoVersionInstall', 'id', [data.Requests.id]);
    App.clusterStatus.setClusterStatus({
      wizardControllerName: this.get('name'),
      localdb: App.db.data
    });
    App.RepositoryVersion.find(params.id).set('defaultStatus', 'INSTALLING');
  },

  /**
   * opens a popup with installations state per host
   * @param {Em.Object} version
   * @method showProgressPopup
   */
  showProgressPopup: function(version) {
    var popupTitle = Em.I18n.t('admin.stackVersions.details.install.hosts.popup.title').format(version.get('displayName'));
    var requestIds = App.get('testMode') ? [1] : App.db.get('repoVersionInstall', 'id');
    var hostProgressPopupController = App.router.get('highAvailabilityProgressPopupController');
    hostProgressPopupController.initPopup(popupTitle, requestIds, this);
  },

  /**
   * reset upgradeState to INIT when upgrade is COMPLETED
   * and clean auxiliary data
   */
  finish: function () {
    if (App.get('upgradeState') === 'COMPLETED') {
      this.setDBProperty('upgradeId', undefined);
      this.setDBProperty('upgradeState', 'INIT');
      this.setDBProperty('upgradeVersion', undefined);
      this.setDBProperty('currentVersion', undefined);
      this.setDBProperty('isDowngrade', undefined);
      App.clusterStatus.setClusterStatus({
        localdb: App.db.data
      });
      App.set('upgradeState', 'INIT');
    }
  }.observes('App.upgradeState'),

  /**
   * show dialog with tasks of upgrade
   * @return {App.ModalPopup}
   */
  openUpgradeDialog: function () {
    App.router.transitionTo('admin.stackUpgrade');
  },

  /**
   * returns url to get data for repoVersion or clusterStackVersion
   * @param {Boolean} stack true if load clusterStackVersion
   * @param {Boolean} fullLoad true if load all data
   * @returns {String}
   * @method getUrl
   */
  getUrl: function(stack, fullLoad) {
    if (App.get('testMode')) {
      return stack ? this.get('mockStackUrl') : this.get('mockRepoUrl')
    } else {
      if (fullLoad) {
        return stack ? this.get('realStackUrl') : this.get('realRepoUrl');
      } else {
        return this.get('realUpdateUrl');
      }
    }
  },

  /**
   * get stack versions from server and push it to model
   * @return {*}
   * @method loadStackVersionsToModel
   */
  loadStackVersionsToModel: function (fullLoad) {
    var dfd = $.Deferred();
    App.HttpClient.get(this.getUrl(true, fullLoad), App.stackVersionMapper, {
      complete: function () {
        dfd.resolve();
      }
    });
    return dfd.promise();
  },

  /**
   * get repo versions from server and push it to model
   * @return {*}
   * @params {Boolean} isUpdate - if true loads part of data that need to be updated
   * @method loadRepoVersionsToModel()
   */
  loadRepoVersionsToModel: function () {
    var dfd = $.Deferred();
    App.HttpClient.get(this.getUrl(false, true), App.repoVersionMapper, {
      complete: function () {
        dfd.resolve();
      }
    });
    return dfd.promise();
  },

  /**
   * set status to Upgrade item
   * @param item
   * @param status
   */
  setUpgradeItemStatus: function(item, status) {
    return App.ajax.send({
      name: 'admin.upgrade.upgradeItem.setState',
      sender: this,
      data: {
        upgradeId: item.get('request_id'),
        itemId: item.get('stage_id'),
        groupId: item.get('group_id'),
        status: status
      }
    }).done(function () {
      item.set('status', status);
    });
  },

  currentVersionObserver: function () {
    var versionNumber = this.get('currentVersion.repository_version');
    var currentVersionObject = App.RepositoryVersion.find().findProperty('status', 'CURRENT');
    var versionName = currentVersionObject && currentVersionObject.get('stackVersionType');
    App.set('isStormMetricsSupported', versionName != 'HDP' || stringUtils.compareVersions(versionNumber, '2.2.2') > -1 || !versionNumber);
  }.observes('currentVersion.repository_version')
});
