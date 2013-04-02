#!/usr/bin/python
# Copyright (c) 2013, mozes@ksu.edu
# get_msr based on SpecificRegister from here: http://blog.rm-r-f.me/index.php?id=34
# Copyright (c) 2011, black@tmnhy.su aka unregistered
# License: GNU GPL v.2

# Toggle currently toggles on all cores, but turboboost seems to be processor specific in some cases
# This does no checking to be sure the processor supports turbo, or even
# if it is an intel proc. If it can't read the appropriate register, it
# will bail out before writing anything. I can't recommend using this on a
# processor that doesn't support turbo mode. I cannot be held
# responsible if it crashes your machine or does something even more
# dangerous.

import argparse
import os, struct, multiprocessing

parser = argparse.ArgumentParser(description='Toggles intel turboboost')
parser.add_argument('-p', type=str, default='ALL', help='What processor should this apply to')
parser.add_argument('-e', action='count', default=0, help='Enables turboboost')
parser.add_argument('-d', action='count', default=0, help='Disables turboboost')
parser.add_argument('-t', action='count', default=0, help='Toggles the current setting')
parser.add_argument('-s', action='count', default=0, help='Shows the current setting (Default)')

def get_msr(cpu, register):
    """
        reads the register from the cpu
    """
    try:
        fd = os.open('/dev/cpu/%s/msr' % cpu, os.O_RDONLY)
        os.lseek(fd, register, 0)
        data = os.read(fd,8)
        os.close(fd)
        return struct.unpack('=Q', data)[0]
    except Exception as e:
       print(e)
       return None

def write_msr(cpu, register, value):
    """
       writes the value to the register on that cpu
    """
    try:
        fd = os.open('/dev/cpu/%s/msr' % cpu, os.O_WRONLY)
        os.lseek(fd, register, 0)
        os.write(fd, struct.pack('=Q', value))
        os.close(fd)
        return True
    except Exception as e:
        print(e)
        return False

def turbo(proc, op):
        """
            Determines what to do with the proc, bit 38 in register 0x1a0 holds the turboboost settings 0 enabled, 1 disabled
        """
        origValue = get_msr(proc, 0x1a0)
        if origValue == None:
            return
        if op == 0 and (origValue >> 38) & 1 == 1:
            print("turbo off, didn't do anything", hex(origValue))
            return
        elif op == 1 and (origValue >> 38) & 1 == 0:
            print("turbo on, didn't do anything", hex(origValue))
            return
            return
        if op == -2 and (origValue >> 38) & 1 == 0:
            print("Turbo currently enabled on processor", proc)
            return
        elif op == -2 and (origValue >> 38) & 1 == 1:
            print("Turbo currently disabled on processor", proc)
            return
        write_msr(proc, 0x1a0, origValue ^ (1<<38))
        print("Toggled", hex(origValue))

if __name__ == '__main__':
    args = parser.parse_args()
    processors = []
    if args.p == 'ALL':
        for p in range(0, multiprocessing.cpu_count()):
            processors.append(p)
    else:
       processors.append(args.p)
    op = -2
    if args.e > 0:
        op = 1
    elif args.d > 0:
        op = 0
    elif args.t > 0:
        op = -1
    for proc in processors:
        turbo(proc, op)
