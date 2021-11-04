# script to do short tests of sam projects interactively

import os,sys,time,string,datetime,socket
import samweb_client
import json
import ifdh
import subprocess
import argparse
from mergeMeta import *

parser = argparse.ArgumentParser(description='Test Sam Project')
parser.add_argument("-c", type=str, help="Name of fcl file", default="test.fcl")
parser.add_argument("-n", type=int, help="n events", default=20)
parser.add_argument('-w', type=int, help='Use wrapper?', default=0)
parser.add_argument('-f', type=int, help='N files?', default=3)
args = parser.parse_args()

 
# test setup to be fixed up later with real args
e = "unknown"
statuscodes ={}
opts = {}
opts["fcl"]="test.fcl"
opts["n_consumers"]=1
opts["appFamily"] = "protoduneana"
opts["appName"]= "pdspana"
opts["appVersion"]=os.getenv("PROTODUNEANA_VERSION")
opts["process_description"]="testing sam access"
opts["MaxFiles"]=args.f
opts["jsonName"]="ana_hist.root.json"
opts["rootName"]="pduneana.root"
opts["runType"]="protodune-sp"
opts["dataTier"]="storage-testing"
opts["dataStream"]="physics"

# need this
samweb = samweb_client.SAMWebClient(experiment='dune')

def mytime():
  return datetime.datetime.now().strftime("%Y-%m-%d-%H%M.%S")

def test():
  
  #larargs = ["-c./test.fcl"]+["-n100"]
  larargs = ["-c" + args.c]+["-n%i"%args.n]
  project_name = "schellma-1GeVMC-test"
  samExample(project_name,larargs)
  

# merge the meta based on the parents in the json file produced by Art.

def mergeTheMeta(rootname,jsonname,status,options):
  mopts = {}
  maker = mergeMeta(mopts)
  maker.source = "samweb"
  if status == 0 and os.path.exists(options["jsonName"]):
    print ("found ",options["jsonName"])
    os.rename(options["jsonName"],jsonname)
    os.rename(options["rootName"],rootname)
    json_file = open(jsonname,'r')
    if not json_file:
        raise IOError('Unable to open json file %s.' % jsonname)
    else:
      md = json.load(json_file)
      json_file.close()
      externals = {}
      #set things we need to make certain don't inherit from parents
      externals["file_name"]=rootname
      externals["application"]={"family": options["appFamily"],"name": options["appName"],"version": options["appVersion"]}
      externals["data_tier"]=options["dataTier"]
      externals["file_size"]=os.path.getsize(rootname)
      externals["data_stream"]=options["dataStream"]
      externals["start_time"]=md["start_time"]
      externals["end_time"]=md["end_time"]
      externals["art.returnstatus"] = status
      
      # identify the parents so you can merge
      parents = md["parents"]
      plist = []
      for p in parents:
        plist += [(p["file_name"])]
      status = maker.checkmerge(plist)
      if status:
        newmd = maker.concatenate(plist,externals)
        print (newmd)
        return newmd
      else:
        print (" could not merge the inputs, sorry")
        return None
  
def process_sam(project_url,project_name,consumer_id,larargs):
  
  larcommand = ["lar"]+larargs
  #larcommand += ["--sam-inherit-run-type"]
  larcommand += ["--sam-web-uri=%s"%project_url]
  larcommand += ["--sam-process-id=%s"%consumer_id]
  larcommand += ["--sam-application-family=%s"%opts["appFamily"]]
  larcommand += ["--sam-application-version=%s"%opts["appVersion"]]
  #arcommand += ["--sam-application-name=pdspana"]
  filename = "protoduneana_%s_%s"%(project_name,consumer_id)
  rootname = filename+".root"
  logfile = open(filename+".out",'w')
  errfile = open(filename+".err",'w')
  jsonname =filename+"_temp.json"
  print ("SamExample:",mytime(),"try to launch",larcommand,consumer_id)
  #start_time = timeform(datetime.datetime.now())


  if (args.w == 0):
    fixed = open(jsonname.replace("_temp",""),'w')
    print('Not using wrapper')
    ret = subprocess.run(larcommand,stdout=logfile,stderr=errfile)
    status = ret.returncode
    print ("lar returned:",status)
    # now make better metadata
    if os.path.exists(opts["jsonName"]):
      themd = mergeTheMeta(rootname,jsonname,status,opts)

      if themd != None:
        json.dump(themd,fixed, indent=2,separators=(',',': '))
        return 0
      else:
        return 1
        
    else:
      print ("no json or failure",status)
      return status
    print ("subprocess returns:", status)
    return 0

  else:
    print('Using wrapper')
    wrap_cmd = ["python", "lar_wrapper.py"] + larargs
    wrap_cmd += ["--sam-web-uri=%s" % project_url,
                 "--sam-process-id=%s" % consumer_id,
                 "--sam-application-family=%s" % opts["appFamily"],
                 #"--sam-application-version=%s" % opts["appVersion"],
                 "-j", jsonname,
                 "--rootname", rootname,
                 "--fix_count"]
    ret = subprocess.run(wrap_cmd, stdout=logfile, stderr=errfile)
    return ret.returncode

  #end_time = timeform(datetime.datetime.now())
  
def startProject(def_name):
  project_name = def_name+"_"+mytime()
  val = samweb.startProject(project_name,def_name)
  print ("startProject",val)
  return val


def samExample(def_name,larargs):

  print ("SamExample:",mytime(), "Getting files from Sam definition with name:",def_name)
  
  info = startProject(def_name)
  
  project_name = info["project"]
  project_url = info["projectURL"]

  print ("Got SAM project url:",project_url)
 
  consumer_id = 0
  
  n_consumers = opts["n_consumers"]

  for nc in range(0,n_consumers):  # do several consumers, each with MaxFiles files
  
    
    consumer_id = samweb.startProcess(project_url, opts["appFamily"], opts["appName"], opts["appVersion"], node=socket.gethostname(), description=opts["process_description"], maxFiles=opts["MaxFiles"], schemas="root")
    
    process_url = samweb.makeProcessUrl(project_url, consumer_id)
    print ("SamExample:",mytime(),"Got SAM consumer id:",consumer_id, process_url)
    consumerok = True
    
    # do the actual processing
    
    status = process_sam(project_url,project_name,consumer_id,larargs)
    
    consumerok = (status==0)
    
    print (" larsoft returned status", status)
 
    #if we got this far then we trust the project so mark it as completed
    try:
        if consumerok:
          print ("SamExample:",mytime(), " set consumer ", consumer_id, "ok")
          samweb.stopProcess(process_url )
         
        else:
          print ("SamExample:",mytime(), " set consumer ", consumer_id, "bad")
          samweb.setProcessStatus('bad',process_url )
         
    except Exception:
        print ("SamExample:",mytime()," can't even set to bad as consumer status failed",e)
      
  samweb.stopProject(project_url)
  out = samweb.projectSummaryText(project_url)
  print (out)

test()
