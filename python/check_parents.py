#!/bin/env python3
import json
import sys
import samweb_client
import argparse
import multiprocessing as mp

def check_file(f, swc, parents, bad_files):
  md = swc.getMetadata(f) 
  #print(len(md['parents']))
  for p in md['parents']:
    if p['file_name'] in parents:
      print("Warning: Found duplicate parent", p['file_name'], "Ntuple", f)
    parents.append(p['file_name'])
    #print(len(parents))

  urls = swc.getFileAccessUrls(f, 'xroot')
  if len(urls) == 0:
    print('ERROR: CANNOT FIND FILE', f)
    bad_files.append(f)

def check_files(files, parents, bad_files):
  samweb = samweb_client.SAMWebClient(experiment='dune')
  a = 0
  for f in files:
    if not a % 100: print('%i/%i'%(a, len(files)))
    check_file(f, samweb, parents, bad_files)
    a += 1
  print(len(parents))

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description = 'Wrapper around check')
  parser.add_argument('-r', type=str, help='List of root files to hadd')
  parser.add_argument('-n', type=int, help='')
  args = parser.parse_args()

  with open(args.r, 'r') as f:
    lines = [l.strip('\n') for l in f.readlines()]
    files = [l.split('/')[-1] for l in lines]
  
  split_files = [files[i::args.n] for i in range(args.n)]
  split_parents = [] #[[] for i in range(args.n)]
  with mp.Manager() as manager:
    parents = manager.list()
    bad_files = manager.list()
    for sf in split_files:
      split_parents.append([])
      print(len(sf))

    n = [
        mp.Process(target=check_files,
                   args=(split_files[i],
                   parents, bad_files))
        for i in range(args.n)
    ]
    
    for p in n:
      p.start()
    for p in n:
      p.join()

    print()
    print('All parents:', len(parents))
    print('Set parents:', len(set(parents)))

    print('Bad files')
    for bf in bad_files:
      print(bf)
