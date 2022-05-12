#!/bin/env python3
import json
import argparse


parser = argparse.ArgumentParser(description = 'Run patcher')
parser.add_argument('-i', type=str, help='Input')
parser.add_argument('-o', type=str, help='Output')

args = parser.parse_args()

with open(args.i, 'r') as mdf:
  md = json.load(mdf)


runslist = []

a = 0
for run in md['runs']:
  print(run)
  if run in runslist:
    #print('Found duplicate. Skipping')
    continue
  runslist.append(run)

meta = md
meta['runs'] = runslist

with open(args.o, 'w') as f:
  json.dump(meta, f, indent=2,separators=(',',': '))
