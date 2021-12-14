#!/bin/env python3

import subprocess
import argparse
import os 
from glob import glob as ls

parser = argparse.ArgumentParser(description = 'Submission script for lar_wrapper')
parser.add_argument('--config', type=str, help='Which config', default=None)
parser.add_argument('--fcl', type=str, help='FCL file name', default="")
parser.add_argument('--output_dir', type=str, help='Output top dir', default=None)
parser.add_argument('--extra_dir', type=str, help='Output lower level dir',
                    default=None)
parser.add_argument('--dataset', type=str, help='Which dataset', default=None)
parser.add_argument('--dry_run', action='store_true',
                    help='Tell fife_launch to do a dry_run')
parser.add_argument('--ls_cfg', action='store_true', help='ls cfg directory')

parser.add_argument('--nevents', type=int,
                    help='Override nevents within the lar_wrapper', default=50)
parser.add_argument('--n_files_per_job', type=int,
                    help='Override n_files_per_job within the lar_wrapper', default=5)
parser.add_argument('--pduneana_tar', type=str, default='',
                    help='Optional Protoduneana tarball to be set up before NTupleProd')

args = parser.parse_args()

##Just ls and exit
if args.ls_cfg:
  print(ls('%s/*cfg'%os.getenv('NTUPLEPROD_CFG_PATH')))
  exit(0)

##Begin fife_launch cmd 
cmd = ['fife_launch', '-c']

##Pick cfgs
if args.config:
  cmd += [args.config]
else:
  cmd += ['%s/lar_wrapper.cfg'%os.getenv('NTUPLEPROD_CFG_PATH')]

##Need a fcl
if args.fcl == "":
  print('Error. Must supply a fcl file with --fcl')
  exit(1)
fcl_name = args.fcl.split('/')[-1]
cmd += ['-Oglobal.fcl_name=%s'%fcl_name]


##Choose output dir
if len(args.fcl.split('/')) > 1:
  path = '%s'%('/'.join(args.fcl.split('/')[:-1]))
  cmd += ['-Osubmit.f_0=dropbox://%s/%s'%(path, fcl_name)]
else:
  cmd += ['-Oexecutable.arg_2=%s'%fcl_name]

##Dataset
if args.dataset:
  cmd += ['-Osubmit.dataset=%s'%args.dataset]

##Output
if args.output_dir:
  cmd += ['-Oenv_pass.OUTPUT_DIR=%s'%args.output_dir]
if args.extra_dir:
  cmd += ['-Oenv_pass.EXTRA_DIR=%s'%args.extra_dir]

##Tell fife_launch to only do a dry_run
if args.dry_run:
  cmd += ['--dry_run']

##Miscellanea
cmd += ['-Oglobal.ntupleprod_version=%s'%os.getenv('NTUPLEPROD_VERSION')]
cmd += ['-Oglobal.nevents=%i'%args.nevents]
cmd += ['-Osubmit.n_files_per_job=%i'%args.n_files_per_job]

print(cmd)
##Special commands for overriding some setup stuff
if not args.pduneana_tar == '':
  cmd += ['-Ojob_setup.setup_local=True',
          '-Osubmit.tar_file_name=%s'%args.pduneana_tar, 
          '-Ojob_setup.setup=-?',#hack to nullify the setup
          '-Ojob_setup.prescript=source `ups setup NTupleProd -v %(ntupleprod_version)s`'
         ]

subprocess.run(cmd)
