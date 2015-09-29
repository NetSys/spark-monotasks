"""
Utilities to help with running experiments.
"""

import subprocess

# Copy a file from a given host through scp, throwing an exception if scp fails.
def scp_from(host, remote_file, local_file):
  subprocess.check_call(
    "scp -q -o StrictHostKeyChecking=no '%s:%s' '%s'" %
    (host, remote_file, local_file), shell=True)

def ssh_get_stdout(host, command):
  command = "source /root/.bash_profile; %s" % command
  ssh_command = ("ssh -t -o StrictHostKeyChecking=no %s '%s'" %
    (host, command))
  return subprocess.Popen(ssh_command, stdout=subprocess.PIPE, shell=True).communicate()[0]

