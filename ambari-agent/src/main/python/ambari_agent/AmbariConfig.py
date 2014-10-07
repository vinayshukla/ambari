#!/usr/bin/env python

'''
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
'''

import platform

import ConfigParser
import StringIO

SETUP_ACTION = "setup"
START_ACTION = "start"
STOP_ACTION = "stop"
RESET_ACTION = "reset"
STATUS_ACTION = "status"
DEBUG_ACTION = "debug"

IS_WINDOWS = platform.system() == "Windows"

if not IS_WINDOWS:
  from AgentConfig_linux import *
else:
  from AgentConfig_windows import *

config = ConfigParser.RawConfigParser()

s = StringIO.StringIO(content)
config.readfp(s)

class AmbariConfig:
  @staticmethod
  def getConfigFile():
    global configFile
    return configFile

  @staticmethod
  def getLogFile():
    global logfile
    return logfile

  @staticmethod
  def getOutFile():
    global outfile
    return outfile

  def getConfig(self):
    global config
    return config

  def getImports(self):
    global imports
    return imports

  def getRolesToClass(self):
    global rolesToClass
    return rolesToClass

  def getServiceStates(self):
    global serviceStates
    return serviceStates

  def getServicesToPidNames(self):
    global servicesToPidNames
    return servicesToPidNames

  def getPidPathesVars(self):
    global pidPathesVars
    return pidPathesVars


def setConfig(customConfig):
  global config
  config = customConfig


def main():
  print config

if __name__ == "__main__":
  main()
