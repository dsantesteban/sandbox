#!/usr/bin/env python
# Author: Diego Santesteban
# Description: Simple script to update hostnames in an Ambari cluster to match the current entries in /etc/hosts

# Instructions: 
# For automatic execution in a template, place this script in /root and create a softlink to it:
#    ln -s /root/ambari_server_hostname_change.py /root/ambari_server_hostname_change_link.py
#
# Then add the following to /etc/rc.local and create the template:
#    if [ -L /root/ambari_server_hostname_change_link.py ]; then
#      python /root/ambari_server_hostname_change.py
#      rm -f /root/ambari_server_hostname_change_link.py
#    fi

import json
import os
import shlex
import socket
import subprocess
import sys
import time
import unittest

# Uncomment this to use the script in an automated cluster setup
'''
# Wait 180 seconds before making changes to Ambari if we're running this as an automated process
if os.path.islink("%s/ambari_server_hostname_change_link.py" % os.path.dirname(os.path.realpath(sys.argv[0]))):
  time.sleep(180)
'''

# Get the current machine hostname. This hostname should be found in the /etc/hosts file
currentHost = os.uname()[1]

# These 2 values will be used in the while loop to add an additional 2 minutes waiting for the /etc/hsots
# to be udpated. If it still isn't updated in that time, exit.
upperTimeLoop = 24
loopCount = 0
smallSleep = 5

# Uncomment this to use the script in an automated cluster setup
'''
# Get the list of hosts from the /etc/hosts file and check for a determined length of time for when
# the currentHost is found in the file.
hosts = []
print "Checking if current host is found in /etc/hosts"
while currentHost not in hosts:
  time.sleep(smallSleep);
  hosts = []
  with open("/etc/hosts", "r") as hostsFile:
    fileText = hostsFile.read()

    for line in fileText.split("\n"):
      if "localhost" not in line and "rhn" not in line and len(line.split()) > 1:
        hosts.append(line.split()[1])
  loopCount += 1
  if loopCount > upperTimeLoop:
    print "Hostname not found in /etc/hosts. Script not continuing, hosts not updated"
    exit()
print hosts
'''

AMBARI_USER = "admin"
AMBARI_PASSWORD = "admin"
CLUSTER_NAME = "Ambari"
SERVER_HOST = "localhost"
SERVER_PORT = "8081"
SERVER_PROTOCOL = "http"
SSH_CMD = ["ssh", "-o", "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no"]

def getServerInfoFromConfig():
  with open("/etc/ambari-server/conf/ambari.properties", "r") as serverConfig:
    configEntries = serverConfig.read()

    port_set, ssl_port_set, ssl_in_use = False, False, False
    for line in configEntries.split("\n"):
      if port_set == True and ssl_port_set == True and ssl_in_use == True:
        break
      if "client.api.port" in line:
        port_set = True
        SERVER_PORT = str(line.split("=")[1]).strip()
        continue
      if "client.api.ssl.port" in line:
        ssl_port_set = True
        SERVER_SSL_PORT = str(line.split("=")[1]).strip()
        continue
      if "api.ssl" in line:
        if str(line.split("=")[1]).strip() == "true":
          ssl_in_use = True
          SERVER_PROTOCOL = "https"

def isAmbariServerRunning():
  proc = subprocess.Popen(["ambari-server", "status"], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
  response = proc.communicate()[0]

  if "Ambari Server running" in response:
    return True
  else:
    return False

def stopAmbariServer():
  print "Going to stop the Ambari Server"

  proc = subprocess.Popen(["ambari-server", "stop"], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
  response = proc.communicate()

  # Do a status check a few times to make sure the server is actually stopped
  numStatusChecks = 5
  for i in range(numStatusChecks):
    status = isAmbariServerRunning()
    if status == False:
      print "Successfully stopped the Ambari Server"
      print ""
      return 0
    else:
      time.sleep(10)

  print "Failed to stop the Ambari Server."
  print ""
  return 1

def startAmbariServer():
  print "Going to start the Ambari Server"

  proc = subprocess.Popen(["ambari-server", "start"], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
  response = proc.communicate()

  # Do a status check a few times to make sure the server is actually stopped
  numStatusChecks = 5
  for i in range(numStatusChecks):
    status = isAmbariServerRunning()
    if status == True:
      print "Successfully started the Ambari Server"
      print ""
      return 0
    else:
      time.sleep(10)

  print "Failed to start the Ambari Server."
  print ""
  return 1

def stopAmbariAgentOnHost(agentHostname):
  proc = subprocess.Popen(SSH_CMD + [agentHostname, "ambari-agent stop"], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
  response = proc.communicate()

def startAmbariAgentOnHost(agentHostname):
  proc = subprocess.Popen(SSH_CMD + [agentHostname, "ambari-agent start"], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
  response = proc.communicate()

def getClusterName():
  global CLUSTER_NAME
  cmd = "curl -u '%s:%s' -H 'X-Requested-By: ambari' -X GET %s://%s:%s/api/v1/clusters/" % \
        (AMBARI_USER, AMBARI_PASSWORD, SERVER_PROTOCOL, SERVER_HOST, SERVER_PORT)

  proc = subprocess.Popen(shlex.split(cmd.encode("ascii")), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
  response = proc.communicate()[0]

  clusterJson = json.loads(response)
  CLUSTER_NAME = clusterJson['items'][0]['Clusters']['cluster_name']

def updateServerHostnames():
  # Make sure the REST API is available
  status = isAmbariServerRunning()
  if status == False:
    rc = startAmbariServer()
    if rc == 1:
      print "Unable to start the Ambari Server and retrieve old hostnames"
      print "Exiting."
      exit(1)
  
  getClusterName()
  # Get the current ambari server hostnames
  cmd = "curl -u '%s:%s' -H 'X-Requested-By: ambari' -X GET %s://%s:%s/api/v1/clusters/%s/hosts" % \
        (AMBARI_USER, AMBARI_PASSWORD, SERVER_PROTOCOL, SERVER_HOST, SERVER_PORT, CLUSTER_NAME)

  response = subprocess.Popen(shlex.split(cmd.encode("ascii")), stdout = subprocess.PIPE, stderr = subprocess.PIPE).communicate()[0]

  curHostsJson = json.loads(response)
  oldHosts = []

  for item in curHostsJson['items']:
    oldHosts.append(item['Hosts']['host_name'].encode('utf8'))

  # Read hostnames from /etc/hosts
  hosts = []
  with open("/etc/hosts", "r") as hostsFile:
    fileText = hostsFile.read()

    for line in fileText.split("\n"):
      if "localhost" not in line and "rhn" not in line and len(line.split()) > 1:
        hosts.append(line.split()[1])

  def listsContainSameItems(list1, list2):
    for item in list1:
      if item not in list2:
        return False

    return True
  # End listsContainSameItems

  if listsContainSameItems(hosts, oldHosts) == True:
    print "No hostname change is needed. Exiting!"
    exit(0)

  # Create the dictionary for the old and new hostnames
  hostsDict = {}
  hostsDict[CLUSTER_NAME] = {}
  for oldHostname,  newHostname in zip(oldHosts, hosts):
    hostsDict[CLUSTER_NAME][oldHostname] = newHostname

  # Write the dictionary to a file
  ambariHostsFile = "/tmp/newAmbariHosts.json"
  with open(ambariHostsFile, "w") as hostsFile:
    hostsFile.write(json.dumps(hostsDict))

  stopAmbariServer()

  for agentHostname in hosts:
    stopAmbariAgentOnHost(agentHostname)

  time.sleep(30)

  proc = subprocess.Popen(["ambari-server", "update-host-names", ambariHostsFile], stdin=subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
  proc.stdin.write("y\n")
  proc.stdin.write("y\n")
  proc.stdin.write("y\n")
  proc.communicate()

  print "Successfully updated the Ambari Server hostnames"

  # Set all the ambari agents to use this new VM's hostname
  serverHostname = socket.gethostname()

  for agentHostname in hosts:
    proc = subprocess.Popen(SSH_CMD + [agentHostname, "ambari-agent reset %s" % serverHostname], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    response = proc.communicate()

    startAmbariAgentOnHost(agentHostname)

  startAmbariServer()

if __name__ == "__main__":
  if not isAmbariServerRunning():
    startAmbariServer()

  getServerInfoFromConfig()

  updateServerHostnames()
