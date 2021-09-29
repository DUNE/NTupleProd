#!/bin/env python3

import subprocess
import argparse
import samweb_client
parser = argparse.ArgumentParser(description = 'Wrapper around hadd')
parser.add_argument('-r', type=str, help='List of root files to hadd', required=True)
parser.add_argument('-j', type=str, help='List of json files', default='')
parser.add_argument('-o', type=str, help='Name of output file', required=True)
parser.add_argument('--remote', type=int, help='Do remote?', default=0)
args = parser.parse_args()

with open(args.r, 'r') as f:
  root_files = [i.strip('\n') for i in f.readlines()]

if len(root_files) == 0:
  print('ERROR: Empty list of root files')
  exit(1)

if args.remote == 0:
  if args.j == '':
    print('ERROR: Need to provide json list when not doing remote')
  with open(args.j, 'r') as f:
    json_files = [i.strip('\n') for i in f.readlines()]

  if len(json_files) == 0:
    print('ERROR: Empty list of json files')
    exit(1)

if args.o in root_files:
  print('ERROR: output file name is in list of root files')

##if remote: get file access urls
if args.remote != 0:
  samweb = samweb_client.SAMWebClient(experiment='dune')
  new_root_files = []
  json_files = []
  for f in root_files:
    json_files.append(f.split('/')[-1])
    urls = samweb.getFileAccessUrls(f.split('/')[-1], 'xroot')
    if len(urls) == 0:
      print('ERROR: CANNOT FIND FILE', f.split('/')[-1])
      exit(1)
    new_root_files.append(urls[0])
  root_files = new_root_files


hadd_cmd = ['hadd', args.o] + root_files
proc = subprocess.run(hadd_cmd)
status = proc.returncode
print("Status:", status)

if status != 0:
  exit(status)

merge_cmd = ['python', 'mergeMeta.py', '-f', args.o]
if args.remote != 0:
  merge_cmd += ['-t', 'samweb']
merge_cmd += ['-j'] + json_files

proc = subprocess.run(merge_cmd)
status = proc.returncode
print("Status:", status)

exit(status)
