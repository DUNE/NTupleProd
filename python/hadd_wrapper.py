#!/bin/env python3

import subprocess
import argparse
import os
import samweb_client
import mergeMeta
parser = argparse.ArgumentParser(description = 'Wrapper around hadd')
parser.add_argument('-r', type=str, help='List of root files to hadd')
parser.add_argument('-j', type=str, help='List of json files', default='')
parser.add_argument('-o', type=str, help='Name of output file', required=True)
parser.add_argument('--usedb', type=int, help='Use db?', default=0)
parser.add_argument('--dataset', type=str, help='Dataset?', default='')
parser.add_argument('-N', type=int, help='Split', default=1)
args = parser.parse_args()


#def hadd_func(i):
#  print("hadd", i)
#  hadd_cmd = ['hadd', 'temp%i.root'%i] + root_files[i::args.N]
#  proc = subprocess.run(hadd_cmd)
#  status = proc.returncode
#  if status != 0:
#    exit(status)


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

  a = 0
  for f in root_files:
    if not a%100: print('%f%%'%(100.*a/len(root_files)), end='\r')
    json_files.append(f.split('/')[-1])
    urls = samweb.getFileAccessUrls(f.split('/')[-1], 'xroot')
    if len(urls) == 0:
      print('ERROR: CANNOT FIND FILE', f.split('/')[-1])
      exit(1)
    new_root_files.append(urls[0])
    a += 1
  root_files = new_root_files



##Call the hadd command
temp_files = ['temp%i.root'%i for i in range(0, args.N)]
for i in range(0, args.N):
  print("hadd", i)
  #temp_list = root_files[i::args.N]
  hadd_cmd = ['hadd', 'temp%i.root'%i] + root_files[i::args.N]
  #temp_files.append('temp%i.root'%i)
  proc = subprocess.run(hadd_cmd)
  status = proc.returncode
  if status != 0:
    exit(status)

#hadd_cmd = ['hadd', args.o] + root_files
if args.N > 1:
  hadd_cmd = ['hadd', args.o] + temp_files
  proc = subprocess.run(hadd_cmd)
  status = proc.returncode
  print("Status:", status)
  
  if status != 0:
    exit(status)

  for f in temp_files:
    print('Removing', f)
    os.remove(f)
else:
  os.rename('temp0.root', args.o)

##Merge the metadata
#merge_cmd = ['python', 'mergeMeta.py', '-f', args.o]
#if args.usedb != 0:
#  merge_cmd += ['-t', 'samweb']
#else:
#  merge_cmd += ['-t', 'local']
#merge_cmd += ['-j'] + json_files


status = mergeMeta.run_merge(filename=args.o, jsonlist=json_files, merge_type=('local' if args.usedb == 0 else 'samweb'))

#proc = subprocess.run(merge_cmd)
#status = proc.returncode
print("Status:", status)

exit(status)
