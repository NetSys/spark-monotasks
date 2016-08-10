import os
import subprocess
import threading
import time

def write_data_in_chunks(total_size):
  start_time = time.time()
  filename = "/mnt/test_{}_{}.txt".format(total_size, start_time)
  f = open(filename, "wb+")
  chunk = b'\xff'*4048
  for i in range(total_size / 4048):
    f.write(chunk)
  f.flush()
  os.fsync(f.fileno())
  f.close()
  write_end_time = time.time()
  elapsed_time = write_end_time - start_time
  print "  Written at rate ", float(total_size) / (megabyte_in_bytes * elapsed_time)

  subprocess.check_call("/root/spark-ec2/clear-cache.sh", shell=True)
  data = open(filename, "r").read(total_size)
  print "  Read at rate ", float(total_size) / (megabyte_in_bytes * (time.time() - write_end_time))

  return filename

def do_read(filename):
  f = open(filename, "r")
  f.read(total_size)
  f.close()

if __name__ == "__main__":
  megabyte_in_bytes = 1024 * 1024
  for total_size in [10 * megabyte_in_bytes, 100 * megabyte_in_bytes]:
    filenames = []
    print "Writing file with {} bytes to disk".format(total_size)
    for trial in range(1, 10):
      filenames.append(write_data_in_chunks(total_size))

    subprocess.check_call("/root/spark-ec2/clear-cache.sh", shell=True)

    # Read all of the files in parallel.
    threads = []
    start_time = time.time()
    for filename in filenames:
      thread = threading.Thread(target = lambda: do_read(filename))
      thread.start()
      threads.append(thread)

    for thread in threads:
      thread.join()

    elapsed_time = time.time() - start_time
    print " Parallel read at rate ", float(total_size) * 10 / (megabyte_in_bytes * elapsed_time)

    for filename in filenames:
      os.remove(filename)
