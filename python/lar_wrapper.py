#!/bin/env python3

import os
import subprocess
import argparse
from mergeMeta import *
import samweb_client
import json
import ROOT as RT
import Loginator

from glob import glob as ls

def searchFCLPath(fcl_file):
  split_path = os.environ['FHICL_FILE_PATH'].replace(':', ' ')
  cmd = 'whereis -S %s -f %s'%(split_path, fcl_file)
  proc = subprocess.run(cmd.split(), capture_output=True)
  #Need stderr check
  return proc.stdout

def searchFCLPath2(fcl_file):
  split_path = os.environ['FHICL_FILE_PATH'].split(':')
  print(split_path)

  for p in set(split_path):
    if len(ls("%s/%s"%(p, fcl_file))) > 0:
      print('found', p)
      return p
  return '' 

def getFCLPath(fcl_file):
  if fcl_file[0] == '.':
    print('this location')
    return os.environ['PWD'] 
  elif fcl_file[0] == '/':
    print('full path')
    return '/'.join(fcl_file.split('/')[:-1])
  else:
    results = searchFCLPath2(fcl_file)
    print(results)
    return results

###Method to fill in some necessary info from the art-level parents
def fillMeta(rootname, jsonname, status, options):
  mopts = {}
  maker = mergeMeta(mopts)
  if status == 0 and os.path.exists(options["jsonName"]):
    print("found ", options["jsonName"])
    os.rename(options["jsonName"], jsonname)
    os.rename(options["rootName"], rootname)
    maker.fillInFromParents(jsonname, jsonname.replace(".json", "_filled.json"))
    with open(jsonname.replace(".json", "_filled.json"), 'r') as f:
      the_md = json.load(f)
    the_md['file_name'] = rootname
    the_md['file_size'] = os.path.getsize(rootname)
    fcl_path = getFCLPath(options['fcl'])
    the_md['DUNE.fcl_path'] = fcl_path
    the_md['DUNE.fcl_name'] = options['fcl']
    the_md['DUNE.fcl_version_tag'] = the_md['application']['version']

    the_md['info.memory'] = ram
    the_md['info.cpusec'] = cpu
    the_md['info.wallsec'] = wall
    the_md['info.physicsgroup'] = 'dunepdhd'

    if options['fix_count']:
      print('Fixing count')
      fCount = RT.TFile(rootname, 'open')
      the_md['event_count'] = fCount.Get('pduneana/beamana').GetEntries()
      fCount.Close()

    with open(jsonname.replace(".json", "_filled.json"), 'w') as f:
      json.dump(the_md, f, indent=2, separators=(',', ': '))


##Set up arguments
##A lot of these are the same from what fife_wrap passes to lar
parser = argparse.ArgumentParser(description = 'Wrapper around lar')
parser.add_argument('--sam-web-uri', type=str, help='Samweb Project URL',
                    required=True)
parser.add_argument('--sam-process-id', type=str, help='Consumer ID',
                    required=True)
parser.add_argument('--sam-application-family', type=str, help='App Family',
                    required=True)
#parser.add_argument('--sam-application-version', type=str, help='App Version',
#                    required=True)
parser.add_argument('-c', type=str, help='FCL file', required=True)
parser.add_argument('-n', type=int, help='N events', default=10)
parser.add_argument('-j', type=str, help='JSON filename produced by module',
                    default='ana_hist.root.json')
parser.add_argument('--rootname', type=str, help='', required=True)
parser.add_argument('--fix_count', action='store_true')

args = parser.parse_args()
json_name = args.j
#fixed = open(json_name.replace("_temp", ""), 'w')


##Build larsoft command
lar_cmd = ["lar", "-c%s" % args.c, "-n%i" % args.n,
           "-T", "pduneana.root",
           "--sam-web-uri=%s" % args.sam_web_uri,
           "--sam-process-id=%s" % args.sam_process_id,
           "--sam-application-family=%s" % args.sam_application_family,
           "--sam-application-version=%s" % os.getenv("PROTODUNEANA_VERSION")]

           #"--sam-application-version=%s" % args.sam_application_version]

##Call larsoft command
logfile = open('temp.out', 'w')
#errfile = open('temp.err', 'w')
proc = subprocess.run(lar_cmd, stdout=logfile)
status = proc.returncode
print("Status:", status)
logfile.close()
cpu = -1.
ram = -1.
wall = -1.
with open('temp.out', 'r') as f:
  for l in f:
    print(l)
    if 'TimeReport' in l and 'CPU' in l:
      cpu = float(l.split()[3])
      wall = float(l.split()[6])
    if 'MemReport' in l and 'VmHWM' in l:
      ram = float(l.split()[6])
print ("Got to before loginator")

logparser = Loginator("temp.out")
logparser.readme()
logparser.addinfo(logparser.getinfo())
logparser.addsaminfo()
list = logparser.writeme()
print ("things to send to elasticsearch",list)


if status != 0:
  exit(status)

##Now merge the metadata
opts = {
  "fcl": args.c,
  "n_consumers": 1,
  "appFamily": "protoduneana",
  "appName":  "pdspana",
  "appVersion": os.getenv("PROTODUNEANA_VERSION"),
  "process_description": "testing sam access",
  "MaxFiles": 2,
  "jsonName": "ana_hist.root.json",
  "rootName": "pduneana.root",
  "runType": "protodune-sp",
  "dataTier": "storage-testing",
  "dataStream": "physics",
  "fix_count": args.fix_count,
  "cpu": cpu,
  "ram": ram,
  "wall": wall
}


##FIll in info from the parents
fillMeta(args.rootname, json_name, status, opts)

samweb = samweb_client.SAMWebClient(experiment='dune')

#Declare file sam so it can be hadded later
with open(json_name.replace(".json", "_filled.json"), 'r') as md_file:
  samweb.declareFile(mdfile=md_file)

exit()
