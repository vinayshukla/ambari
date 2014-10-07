"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from resource_management import *
from hdfs import hdfs
import service_mapping

class NameNode(Script):
  def install(self, env):
    if not check_windows_service_exists(service_mapping.namenode_win_service_name):
      self.install_packages(env)

    import params
    self.configure(env)
    namenode_format_marker = os.path.join(params.hadoop_conf_dir,"NN_FORMATTED")
    if not os.path.exists(namenode_format_marker):
      hadoop_cmd = "cmd /C %s" % (os.path.join(params.hadoop_home, "bin", "hadoop.cmd"))
      Execute("%s namenode -format" % (hadoop_cmd))
      open(namenode_format_marker, 'a').close()

  def start(self, env):
    self.configure(env)
    Service(service_mapping.namenode_win_service_name, action="start")

  def stop(self, env):
    Service(service_mapping.namenode_win_service_name, action="stop")

  def configure(self, env):
    import params
    env.set_params(params)
    hdfs("namenode")

  def status(self, env):
    check_windows_service_status(service_mapping.namenode_win_service_name)
    pass

  def decommission(self, env):
    import params

    env.set_params(params)
    hdfs_user = params.hdfs_user
    conf_dir = params.hadoop_conf_dir

    File(params.exclude_file_path,
         content=Template("exclude_hosts_list.j2"),
         owner=hdfs_user
    )

    if params.dfs_ha_enabled:
      # due to a bug in hdfs, refreshNodes will not run on both namenodes so we
      # need to execute each command scoped to a particular namenode
      nn_refresh_cmd = format('cmd /c hadoop dfsadmin -fs hdfs://{namenode_rpc} -refreshNodes')
    else:
      nn_refresh_cmd = format('cmd /c hadoop dfsadmin -refreshNodes')
    Execute(nn_refresh_cmd, user=hdfs_user)

if __name__ == "__main__":
  NameNode().execute()
