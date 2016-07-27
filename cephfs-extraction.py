#!/usr/bin/python
import rados
import sys
import struct
import os
import argparse
import re
import tarfile


default_conf = '/etc/ceph/ceph.conf'
metadata_pool = 'metadata'
debug = False
regex = None
a_size = None
log_file = None
is_max = False
timeout = 1


class CephConnMan:
  """Provides 'with' semantics for cluster connections"""
  def __init__(self, conffile, extra_conf={}):
    self.conffile = conffile
    self.extra_conf = extra_conf
  def __enter__(self):
    self.cluster = rados.Rados(conffile=self.conffile, conf=self.extra_conf)
    self.cluster.connect()
    return self.cluster
  def __exit__(self, type, value, traceback):
    self.cluster.shutdown()


class IoctxConnMan:
  """Provides 'with' semantics for Ioctx objects"""
  def __init__(self, cluster, pool):
    self.cluster = cluster
    self.pool = pool
  def __enter__(self):
    self.ioctx = self.cluster.open_ioctx(self.pool)
    return self.ioctx
  def __exit__(self, type, value, traceback):
    self.ioctx.close()


class DirMan:
  """emulates pushd/popd using 'with' semantics"""
  def __init__(self, new_dir):
    self.new_dir = new_dir
    self.old_dir = None
  def __enter__(self):
    self.old_dir = os.getcwd()
    if not os.path.exists(self.new_dir):
      os.mkdir(self.new_dir)
    os.chdir(self.new_dir)
  def __exit__(self, type, value, traceback):
    os.chdir(self.old_dir)

def list_dir(inode):
  """Lists all keys on an inode (doesn't handle pagination of the iterator)"""
  omap_keys = []
  with CephConnMan(default_conf) as cluster:
    with IoctxConnMan(cluster, metadata_pool) as ioctx:
      with rados.ReadOpCtx(ioctx) as read_op:
        i, ret = ioctx.get_omap_keys(read_op, '', (1<<31)-1)
        ioctx.operate_read_op(read_op, "{}.00000000".format(inode))
        for omap_kv in i:
          omap_keys.append(omap_kv[0])
  return omap_keys


def get_omap_val(inode, name):
  """Just extract the value for the key specified (doesn't handle missing keys)"""
  with CephConnMan(default_conf) as cluster:
    with IoctxConnMan(cluster, metadata_pool) as ioctx:
      with rados.ReadOpCtx(ioctx) as read_op:
        i, ret = ioctx.get_omap_vals_by_keys(read_op, (name,))
        ioctx.operate_read_op(read_op, "{}.00000000".format(inode))
        b = list(i)[0][1]
  return b


def get_pool_id(inode, name):
  """Extracts the pool number from the omap value for an item in a directory"""
  b = get_omap_val(inode, name)
  unpacked = struct.unpack("<L", b[84:88])
  return unpacked[0]


def is_dir(inode, name):
  """mostly correct way of identifying a directory"""
  pool_id = get_pool_id(inode, name)  # Pool id: will be zero if it is a directory
  if pool_id > 0:
    return False
  return True


def get_file_size(inode, name):
  """Extracts the expected file size from the directory entry"""
  b = get_omap_val(inode, name)
  unpacked = struct.unpack("<2L", b[88:96])  # Size stored in 2 longs
  return (unpacked[1]<<32) + unpacked[0]


def get_inode_from_omap_val(inode, name):
  """Extracts the inode from the directory entry"""
  b = get_omap_val(inode, name)
  unpacked = struct.unpack('<2L', b[15:23])  # inode stored in 2 longs
  return hex((unpacked[1]<<32) + unpacked[0])[2:]


def is_unextractable(inode):
  """Stats an object, with low timeouts to see if we can query it"""
  extra_conf = {
      'rados_osd_op_timeout': str(timeout),
      'rados_mon_op_timeout': str(timeout)
  }
  with CephConnMan(default_conf, extra_conf) as cluster:
    with IoctxConnMan(cluster, metadata_pool) as ioctx:
      try:
        ioctx.stat('{}.00000000'.format(inode))
      except rados.Error as ex:
        return True
  return False


def get_inode_from_path(path):
  """Recursively finds the inode of the path specified"""
  dirname = os.path.dirname(path)
  basename = os.path.basename(path)
  root_inode = "1"
  if dirname != '/':
    root_inode = get_inode_from_path(dirname)
  if '<- bad' in root_inode:
    return root_inode
  inode = get_inode_from_omap_val(root_inode, "{}_head".format(basename))
  if is_dir(root_inode, "{}_head".format(basename)) and is_unextractable(inode):
    return "{} <- bad".format(path)
  return inode


def extract_directory(inode, tar=None, tarcwd=None, log_fh=None):
  """Extracts files from an directory inode"""
  entries = list_dir(inode)
  for entry in entries:
    entry_inode = get_inode_from_omap_val(inode, entry)
    if is_dir(inode, entry):
      if not is_unextractable(entry_inode):
        # recurse into directory
        with DirMan(entry[:-5]):
          if debug:
            print "Entering directory: {}".format(os.getcwd())
          new_tarcwd = os.path.join(tarcwd, entry[:-5])
          extract_directory(entry_inode, tar, new_tarcwd, log_fh)
        if tar is not None:
          os.rmdir(entry[:-5])
      else:
        if log_fh is not None:
          log_fh.write('"timeout","{}","{}"\n'.format(entry_inode, os.path.join(tarcwd, entry[:-5])))
        print "Unable to extract {} as {} (timeout)".format(entry_inode, entry[:-5])
    else:
      entry_pool_id = get_pool_id(inode, entry)
      entry_size = get_file_size(inode, entry)
      # extract the files now
      extract_file(entry_inode, entry, entry_pool_id, entry_size, tar, tarcwd, log_fh)


def extract_file(inode, iname, pool_id, size, tar=None, tarcwd=None, log_fh=None):
  """extracts an object from the specified pool, sequentially until it is the right size"""
  seg_size = 4194304
  fname = iname[:-5]
  chunks = size / seg_size
  left_size = size
  if regex is not None and regex.match(fname) is None:
    if log_fh is not None:
      log_fh.write('"no-match","{}","{}","{}"\n'.format(inode, os.path.join(tarcwd, fname), size))
    if debug:
      print "Refusing to extract {}/{}, because it doesn't match {}".format(os.getcwd(), fname, regex.pattern)
    return
  if size is not None:
    if is_max and size > a_size:
      if log_fh is not None:
        log_fh.write('"too-large","{}","{}","{}"\n'.format(inode, os.path.join(tarcwd, fname), size))
      if debug:
        print "Refusing to extract {}, because it is larger than {} ({})".format(os.path.join(os.getcwd(), fname), a_size, size)
      return
    if not is_max and size <= a_size:
      if log_fh is not None:
        log_fh.write('"too-small","{}","{},"{}""\n'.format(inode, os.path.join(tarcwd, fname), size))
      if debug:
        print "Refusing to extract {}, because it is smaller than {} ({})".format(os.path.join(os.getcwd(), fname), a_size, size)
      return
  if debug:
    print "Extracting file {}/{}, size {}, inode {}".format(os.getcwd(), fname, size, inode)
  with open(fname, 'wb') as fh:
    for segment in range(0, chunks + 1):
      if left_size > seg_size:
        osize = seg_size
      else:
        osize = left_size
      oname = "{}.{}".format(inode, "{0:#0{1}x}".format(segment, 10)[2:])
      get_obj(oname, pool_id, osize, fh)
      left_size -= osize
      if left_size < 0:
        break
  if tar is not None:
    tar.add(fname, arcname=os.path.join(tarcwd, fname))
    os.remove(fname)


def get_obj(oname, pool_id, size, fh):
  """Reads an object out of ceph, appends it to the file specified"""
  with CephConnMan(default_conf) as cluster:
    pool_name = cluster.pool_reverse_lookup(pool_id)
    with IoctxConnMan(cluster, pool_name) as ioctx:
      try:
        osize = ioctx.stat(oname)[0]
      except rados.ObjectNotFound:
        print "{}: Object {} not found, setting object size to 0".format(cname, oname)
        osize = 0
      if osize != 0:
        contents = ioctx.read(oname, length=osize)  # read the length of the object.
      else:
        contents = bytes()
      if len(contents) < size:
        contents += ('\0' * (size - len(contents)))  # object could be sparse, compensate
      fh.write(contents)


def parse_size(ssize):
  if ssize[0] == '+':
    i_max = False
  else:
    i_max = True
  ssize = ssize[1:]
  if ssize[-1].upper() == 'T':
    ssize = int(ssize[:-1]) * 1<<40
  elif ssize[-1].upper() == 'G':
    ssize = int(ssize[:-1]) * 1<<30
  elif ssize[-1].upper() == 'M':
    ssize = int(ssize[:-1]) * 1<<20
  elif ssize[-1].upper() == 'K':
    ssize = int(ssize[:-1]) * 1<<10
  else:
    ssize = int(ssize)
  return (i_max, ssize)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Extract Data from CephFS')
  parser.add_argument('--metadata-pool', '-p', help="Metadata pool name", default='metadata')
  parser.add_argument('--conf', '-c', help="Path to ceph.conf", default='/etc/ceph/ceph.conf')
  parser.add_argument('--ls', action='store_true', help="List a directory")
  parser.add_argument('--debug', '-d', action='store_true', help="Print debug messages")
  group = parser.add_mutually_exclusive_group()
  group.add_argument('--path', help="the path to extract", default=None)
  group.add_argument('--inode', '-i', help="inode to extract", default=None)
  parser.add_argument('--regex', '-r', help="Regex to match against filenames for extraction")
  parser.add_argument('--file', '-f', help="The tar.gz file to extract all the files to")
  parser.add_argument('--size', '-s', help="(+|-)size of files to extract")
  parser.add_argument('--log-file', '-l', help="The file to log the extraction data to")
  parser.add_argument('--timeout', '-t', help="Timeout to determine if the mds inode is unextractable", type=int, default=1)
  namespace = parser.parse_args()
  default_conf = namespace.conf
  metadata_pool = namespace.metadata_pool
  debug = namespace.debug
  timeout = namespace.timeout
  if namespace.size is not None:
    if re.match('^[+-]\d+[TtGgMmKk]?$', namespace.size) is None:
      parser.error("--size must be in the format (+|-)$num$suff")
    is_max, a_size = parse_size(namespace.size)
  if namespace.regex is not None:
    regex = re.compile(namespace.regex)
  inode = None
  if namespace.path is not None:
    inode = get_inode_from_path(namespace.path)
  elif namespace.inode is not None:
    inode = namespace.inode
  if inode is None:
    parser.error("We need either a path or an inode specified")
  if namespace.ls:
    if '<- bad' in inode:
      print inode
    else:
      for line in list_dir(inode):
        print line[:-5]
  else:
    if namespace.log_file is None:
      if namespace.file is None:
        extract_directory(inode, tarcwd=os.path.basename(os.getcwd()))
      else:
        with tarfile.open(namespace.file, 'w:gz') as tar:
          extract_directory(inode, tar=tar, tarcwd=os.path.basename(namespace.file)[:-7])
    else:
      with open(namespace.log_file, 'a') as lfh:
        if namespace.file is None:
          extract_directory(inode, tarcwd=os.path.basename(os.getcwd()), log_fh=lfh)
        else:
          with tarfile.open(namespace.file, 'w:gz') as tar:
            extract_directory(inode, tar=tar, tarcwd=os.path.basename(namespace.file)[:-7], log_fh=lfh)
