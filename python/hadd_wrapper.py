import subprocess
import argparse
parser = argparse.ArgumentParser(description = 'Wrapper around hadd')
parser.add_argument('-r', type=str, help='List of root files to hadd', required=True)
parser.add_argument('-j', type=str, help='List of json files', required=True)
parser.add_argument('-o', type=str, help='Name of output file', required=True)
args = parser.parse_args()

with open(args.r, 'r') as f:
  root_files = [i.strip('\n') for i in f.readlines()]

if len(root_files) == 0:
  print('ERROR: Empty list of root files')
  exit(1)

with open(args.j, 'r') as f:
  json_files = [i.strip('\n') for i in f.readlines()]

if len(json_files) == 0:
  print('ERROR: Empty list of json files')
  exit(1)


if args.o in root_files:
  print('ERROR: output file name is in list of root files')

hadd_cmd = ['hadd', args.o] + root_files
proc = subprocess.run(hadd_cmd)
status = proc.returncode
print("Status:", status)

if status != 0:
  exit(status)

merge_cmd = ['python', 'mergeMeta.py', '-f', args.o, '-j'] + json_files
proc = subprocess.run(merge_cmd)
status = proc.returncode
print("Status:", status)

exit(status)
