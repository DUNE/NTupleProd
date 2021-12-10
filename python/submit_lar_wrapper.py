#!/bin/env python3

import subprocess
import argparse
import os 
from glob import glob as ls

parser = argparse.ArgumentParser(description = 'Submission script for lar_wrapper')
parser.add_argument('--config', type=str, help='Which config', default=None)
parser.add_argument('--fcl', type=str, help='FCL file name', default="")
parser.add_argument('--output_dir', type=str, help='Output top dir', default=None)
parser.add_argument('--extra_dir', type=str, help='Output lower level dir', default=None)
parser.add_argument('--dataset', type=str, help='Which dataset', default=None)
parser.add_argument('--dry_run', action='store_true', help='Tell fife_launch to do a dry_run')
parser.add_argument('--ls_cfg', action='store_true', help='ls cfg directory')

args = parser.parse_args()

if args.ls_cfg:
  print(ls('%s/*cfg'%os.getenv('NTUPLEPROD_CFG_PATH')))
  exit(0)

cmd = ['fife_launch', '-c']

if args.config:
  cmd += [args.config]
else:
  cmd += ['%s/lar_wrapper.cfg'%os.getenv('NTUPLEPROD_CFG_PATH')]

if args.fcl == "":
  print('Error. Must supply a fcl file with --fcl')
  exit(1)

fcl_name = args.fcl.split('/')[-1]
cmd += ['-Oglobal.fcl_name=%s'%fcl_name]


if len(args.fcl.split('/')) > 1:

  path = '%s'%('/'.join(args.fcl.split('/')[:-1]))
  cmd += ['-Osubmit.f_0=dropbox://%s/%s'%(path, fcl_name)]
else:
  cmd += ['-Oexecutable.arg_2=%s'%fcl_name]

if args.dataset:
  cmd += ['-Osubmit.dataset=%s'%args.dataset]

if args.output_dir:
  cmd += ['-Oenv_pass.OUTPUT_DIR=%s'%args.output_dir]

if args.extra_dir:
  cmd += ['-Oenv_pass.EXTRA_DIR=%s'%args.extra_dir]

if args.dry_run:
  cmd += ['--dry_run']

cmd += ['-Oglobal.ntupleprod_version=%s'%os.getenv('NTUPLEPROD_VERSION')]


print(cmd)


subprocess.run(cmd)
