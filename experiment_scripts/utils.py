"""
Utilities to help with running experiments.
"""

import subprocess

# Copy a file from a given host through scp, throwing an exception if scp fails.
def scp_from(host, remote_file, local_file, identity_file=None):
  subprocess.check_call(
    "scp %s -q -o StrictHostKeyChecking=no '%s:%s' '%s'" %
    (get_identity_file_argument(identity_file), host, remote_file, local_file), shell=True)

# Run a command on the given host and return the standard output.
def ssh_get_stdout(host, command, identity_file=None):
  ssh_command = build_ssh_command(host, command, identity_file)
  return subprocess.Popen(ssh_command, stdout=subprocess.PIPE, shell=True).communicate()[0]

# Run a command on the given host and print the standard output.
def ssh_call(host, command, identity_file=None):
  subprocess.check_call(build_ssh_command(host, command, identity_file), shell=True)

def build_ssh_command(host, command, identity_file=None):
  command = "source /root/.bash_profile; %s" % command
  return "ssh %s -t -o StrictHostKeyChecking=no root@%s '%s'" % \
    (get_identity_file_argument(identity_file), host, command)

def get_identity_file_argument(identity_file):
  return "" if (identity_file is None) else "-i %s" % identity_file
