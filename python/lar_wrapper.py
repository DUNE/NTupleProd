#!/bin/env python3

import os
import subprocess
import argparse
from mergeMeta import *

def fillMeta(rootname, jsonname, status, options):
  mopts = {}
  maker = mergeMeta(mopts)
  if status == 0 and os.path.exists(options["jsonName"]):
    print("found ", options["jsonName"])
    os.rename(options["jsonName"], jsonname)
    os.rename(options["rootName"], rootname)
    maker.fillInFromParents(jsonname, jsonname.replace(".json", "filled.json"))

def mergeTheMeta(rootname, jsonname, status, options):
  mopts = {}
  maker = mergeMeta(mopts)
  maker.setSourceLocal()

  if status == 0 and os.path.exists(options["jsonName"]):
    print("found ", options["jsonName"])
    os.rename(options["jsonName"], jsonname)
    os.rename(options["rootName"], rootname)
    json_file = open(jsonname,'r')
    if not json_file:
      raise IOError('Unable to open json file %s.' % jsonname)
    else:
      md = json.load(json_file)
      json_file.close()
      #set things we need to make certain don't inherit from parents
      app_info = {
        "family": options["appFamily"],
        "name": options["appName"],
        "version": options["appVersion"]
      }

      externals = {
        "file_name": rootname,
        "data_tier": options["dataTier"],
        "file_size": os.path.getsize(rootname),
        "data_stream": options["dataStream"],
        "start_time": md["start_time"],
        "end_time": md["end_time"],
        "art.returnstatus": status,
        "application": app_info
      }
      
      # identify the parents so you can merge
      #parents = md["parents"]
      ##plist = [(p["file_name"]) for p in parents]
      #plist = [jsonname]

      #status = maker.checkmerge(plist)

      #if status:
      #  newmd = maker.concatenate(plist, externals)
      #  print (newmd)
      #  return newmd
      #else:
      #  print (" could not merge the inputs, sorry")
      #  return None

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

args = parser.parse_args()
json_name = args.j
fixed = open(json_name.replace("_temp", ""), 'w')

##Build larsoft command
lar_cmd = ["lar", "-c%s" % args.c, "-n%i" % args.n,
           "-T", "pduneana.root",
           "--sam-web-uri=%s" % args.sam_web_uri,
           "--sam-process-id=%s" % args.sam_process_id,
           "--sam-application-family=%s" % args.sam_application_family,
           "--sam-application-version=%s" % os.getenv("PROTODUNEANA_VERSION")]

           #"--sam-application-version=%s" % args.sam_application_version]

##Call larsoft command
proc = subprocess.run(lar_cmd)
status = proc.returncode
print("Status:", status)

if status != 0:
  exit(status)

##Now merge the metadata
opts = {
  "fcl": "test.fcl",
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
  "dataStream": "physics"
}

fillMeta(args.rootname, json_name, status, opts)
#if os.path.exists(opts["jsonName"]):
#  themd = mergeTheMeta(args.rootname, json_name, status, opts)
#  if themd:
#    json.dump(themd, fixed, indent=2, separators=(',', ': '))
#    print("wrote json. exiting lar_wrapper")
#    exit() #Will send out 0 by default
#  else:
#    print("could not merge meta")
#    exit(1)
#      
#else:
#  print("no json or failure")
#  exit(1)

exit()
