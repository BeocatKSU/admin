#!/usr/bin/python
# Copyright (c) 2015, <name of copyright holder>
# Author: Tygart, Adam <mozestygart@gmail.com>
# 
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from __future__ import print_function
import os
import multiprocessing
import argparse
import time
import sys

walk_queue = multiprocessing.Queue()
file_queue = multiprocessing.Queue()
fix_queue = multiprocessing.Queue()

def fix_file(filename, verbosity):
    """Takes a filename that needs to be fixed, truncates it to a
    byte larger, then the correct number of bytes, and resets the
    mtime."""
    if verbosity > 0:
        print("Would fix {}".format(filename))
    stat = os.stat(filename)
    with open(filename, 'wb+') as f:
        f.truncate(stat.st_size + 1)
    with open(filename, 'wb+') as f:
        f.truncate(stat.st_size)
    os.utime(filename, (stat.st_atime, stat.st_mtime))


def check_file(filename, fix_queue, verbosity):
    """Takes a filename, reads the first 128 bytes, if all null
    assume the file needs fixing"""
    if verbosity > 2:
        print("Checking {}".format(filename))
    if os.stat(filename).st_size > 0:
        with open(filename, 'rb') as f:
            b = f.read(128)
        for byte in b:
            if byte != 0:
                break
        else:
            fix_queue.put(filename)


def find_files(directory, walk_queue, file_queue, verbosity):
    """Walks a directory, puts files on the file queue, directories
    on the walk_queue"""
    if verbosity > 1:
        print("Walking {}".format(directory))
    for item in os.listdir(directory):
        item = os.path.join(directory, item)
        if os.path.isdir(item):
            walk_queue.put(item)
        elif os.path.isfile(item):
            file_queue.put(item)

def check_file_thread(file_queue, fix_queue, verbosity):
    """Forever, check the file_queue"""
    while True:
        check_file(file_queue.get(), fix_queue, verbosity)

def fix_file_thread(fix_queue, verbosity):
    """Forever, check the fix_queue"""
    while True:
        fix_file(fix_queue.get(), verbosity)

def find_files_thread(walk_queue, file_queue, verbosity):
    """Forever, check the walk_queue"""
    while True:
        find_files(walk_queue.get(), walk_queue, file_queue, verbosity)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple script to walk a path and fix files on an EC CephFS pool hit by bug 12551')
    parser.add_argument("--verbose", "-v", action='count', default=0)
    parser.add_argument("--fix-threads", "-f", type=int, default=8, help="Threads to fix the files with")
    parser.add_argument("--check-threads", "-c", type=int, default=8, help="Threads to check the files with")
    parser.add_argument("--walk-threads", "-w", type=int, default=8, help="Threads to walk the tree with")
    parser.add_argument("path", nargs="+", help="Paths to walk, check and fix")
    args = parser.parse_args()

    fix_pool = multiprocessing.Pool(args.fix_threads, fix_file_thread, (fix_queue, args.verbose))
    check_pool = multiprocessing.Pool(args.check_threads, check_file_thread, (file_queue, fix_queue, args.verbose))
    find_pool = multiprocessing.Pool(args.walk_threads, find_files_thread, (walk_queue, file_queue, args.verbose))

    for p in args.path:
        walk_queue.put(p)

    while True:
        # avoid busy wait on the main thread
        time.sleep(1)
        if walk_queue.empty() and file_queue.empty() and fix_queue.empty():
            fix_pool.terminate()
            check_pool.terminate()
            find_pool.terminate()
            break
