#!/bin/env python3

import subprocess
import argparse
import os 
from glob import glob as ls

parser = argparse.ArgumentParser(description = 'Submission script for hadd_wrapper')
parser.add_argument('--config', type=str, help='Which config', default=None)
parser.add_argument('--output_dir', type=str, help='Output top dir', default=None)
parser.add_argument('--extra_dir', type=str, help='Output lower level dir', default=None)
parser.add_argument('--dry_run', action='store_true', help='Tell fife_launch to do a dry_run')
parser.add_argument('--ls_cfg', action='store_true', help='ls cfg directory')
parser.add_argument('--listname', type=str, required=True)
parser.add_argument('--split', type=int, default=10)
parser.add_argument('--output_name', type=str, default='hadd_wrapper_test.root')
parser.add_argument('--lifetime', type=str, default='12h')

args = parser.parse_args()

if args.ls_cfg:
  print(ls('%s/*cfg'%os.getenv('NTUPLEPROD_CFG_PATH')))
  exit(0)

cmd = ['fife_launch', '-c']

if args.config:
  cmd += [args.config]
else:
  cmd += ['%s/hadd_wrapper.cfg'%os.getenv('NTUPLEPROD_CFG_PATH')]


if args.output_dir:
  cmd += ['-Oenv_pass.OUTPUT_DIR=%s'%args.output_dir]

if args.extra_dir:
  cmd += ['-Oenv_pass.EXTRA_DIR=%s'%args.extra_dir]

if args.dry_run:
  cmd += ['--dry_run']

cmd += ['-Oglobal.ntupleprod_version=%s'%os.getenv('NTUPLEPROD_VERSION')]

if len(args.listname.split('/')) < 2:
  print("Error. Must provide a valid listname.")
  exit(1)


bare_listname = args.listname.split('/')[-1]
cmd += ['-Oglobal.listname=%s'%bare_listname]
listpath = '/'.join(args.listname.split('/')[:-1])
cmd += ['-Oglobal.listloc=%s'%listpath]

cmd += ['-Oglobal.split=%i'%args.split]
cmd += ['-Oglobal.output_name=%s'%args.output_name]
cmd += ['-Osubmit.expected-lifetime=%s'%args.lifetime]

print(cmd)

subprocess.run(cmd)
