import os
import time

def write_data_in_chunks(total_size):
  filename = "/mnt/test_%s.txt" % total_size
  f = open(filename, "wb+")
  chunk = b'\xff'*4048
  for i in range(total_size / 4048):
    f.write(chunk)
  f.flush()
  os.fsync(f.fileno())
  f.close()
  os.remove(filename)

if __name__ == "__main__":
  megabyte_in_bytes = 1024 * 1024
  for total_size in [10 * megabyte_in_bytes, 100 * megabyte_in_bytes]:
    print "Writing file with %s bytes to disk" % total_size
    for trial in range(1, 10):
      start_time = time.time()
      write_data_in_chunks(total_size)
      elapsed_time = time.time() - start_time
      print "  Written at rate ", float(total_size) / (megabyte_in_bytes * elapsed_time)
