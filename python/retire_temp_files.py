#!/bin/env python3

import samweb_client
import argparse
import multiprocessing as mp


def retire_files(files):
  samweb = samweb_client.SAMWebClient(experiment='dune')
  n = len(files)
  a = 0
  for f in files:
    if not a%100: print(a, "/", n)
    samweb.retireFile(f)
    a += 1

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Script to retire temporary files')
  parser.add_argument('-u', type=str, help='Username', default='calcuttj')
  parser.add_argument('-t', type=str, help='data tier')
  parser.add_argument('--dry_run', action='store_true')
  parser.add_argument('-d', type=str,
                      help='Add check for if the file is a child of a file matching this dataset)',
                      default='')
  parser.add_argument('-l', action='store_true')
  parser.add_argument('--procs', type=int, help='Number of processes', default=1)
  args = parser.parse_args()
  
  samweb = samweb_client.SAMWebClient(experiment='dune')
  query = "user %s and data_tier %s"%(args.u, args.t)
  if not args.d=='':
    query += " and ischildof:(defname:%s)"%args.d
  files = samweb.listFiles(query)
  
  if args.l:
    for f in files: print(f)
  print('Found %i files matching query'%len(files))
  
  print('N procs:', args.procs)
  split_files = [files[i::args.procs] for i in range(args.procs)]
  for sf in split_files:
    print(len(sf))

  if args.dry_run:
    print("Dry Run: Exiting")
    exit()

  #procs = [mp.Process(target=retire_files, args=(split_files[i], samweb)) for i in range(args.procs)]
  procs = [mp.Process(target=retire_files, args=(split_files[i], )) for i in range(args.procs)]


  for p in procs: 
    p.start()
  for p in procs:
    p.join()
  
  #n = len(files)
  #a = 0
  #for f in files:
  #  if not a%100: print(a, "/", n, end='\r')
  #  samweb.retireFile(f)
  #  a += 1
