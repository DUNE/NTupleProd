#!/bin/env python3

import subprocess
import argparse
import samweb_client
parser = argparse.ArgumentParser(description = 'Wrapper around hadd')
parser.add_argument('-r', type=str, help='List of root files to hadd')
parser.add_argument('-j', type=str, help='List of json files', default='')
parser.add_argument('-o', type=str, help='Name of output file', required=True)
parser.add_argument('--usedb', type=int, help='Use db?', default=0)
parser.add_argument('--dataset', type=str, help='Dataset?', default='')
args = parser.parse_args()


##If doing remote, get the samweb client
if args.usedb != 0:
  samweb = samweb_client.SAMWebClient(experiment='dune')

##If local or if not providing a dataset, read in the file list
if args.usedb == 0 or (args.usedb != 0 and args.dataset == ''):
  with open(args.r, 'r') as f:
    root_files = [i.strip('\n') for i in f.readlines()]
##Get the files from the dataset
else:
  root_files = samweb.listFiles(defname=args.dataset)
  #print(root_files)

if len(root_files) == 0:
  print('ERROR: Empty list of root files')
  exit(1)

##Get the list of json files from the local list
if args.usedb == 0:
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
if args.usedb != 0:
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

##Call the hadd command
hadd_cmd = ['hadd', args.o] + root_files
proc = subprocess.run(hadd_cmd)
status = proc.returncode
print("Status:", status)

if status != 0:
  exit(status)

##Merge the metadata
merge_cmd = ['python', 'mergeMeta.py', '-f', args.o]
if args.usedb != 0:
  merge_cmd += ['-t', 'samweb']
else:
  merge_cmd += ['-t', 'local']
merge_cmd += ['-j'] + json_files

proc = subprocess.run(merge_cmd)
status = proc.returncode
print("Status:", status)

exit(status)
