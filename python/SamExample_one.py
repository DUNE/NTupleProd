# script to do short tests of sam projects interactively

import os,sys,time,string,datetime,socket
import samweb_client
import json
import ifdh
import subprocess
import argparse

parser = argparse.ArgumentParser(description='Test Sam Project')

 
# test setup to be fixed up later with real args
e = "unknown"
statuscodes ={}
opts = {}
opts["fcl"]="test.fcl"
opts["n_consumers"]=2
opts["appFamily"] = "protoduneana"
opts["appName"]= "pdspana"
opts["appVersion"]=os.getenv("PROTODUNEANA_VERSION")
opts["process_description"]="testing sam access"
opts["MaxFiles"]=3
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
  
  larargs = ["-c./test.fcl"]+["-n100"]
  project_name = "PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1"
  samExample(project_name,larargs)
  
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
  ret = subprocess.run(larcommand,stdout=logfile,stderr=errfile)
  status = ret.returncode
  print ("lar returned:",status)
  # now make better metadata
  if status == 0 and os.path.exists(opts["jsonName"]):
    print ("found ",opts["jsonName"])
    os.rename(opts["jsonName"],jsonname)
    os.rename(opts["rootName"],rootname)
    json_file = open(jsonname,'r')
    fixed = open(jsonname.replace("_temp","_fixed"),'w')
    if not json_file:
        raise IOError('Unable to open json file %s.' % jsonname)
    else:
      md = json.load(json_file)
      #patch the application as the art option doesn't do anything and doesn't do name
      md["file_name"]=rootname
      md["application"]={"family": opts["appFamily"],"name": opts["appName"],"version": opts["appVersion"]}
      md["data_tier"]=opts["dataTier"]
      md["file_size"]=os.path.getsize(rootname)
      md["data_stream"]=opts["dataStream"]
      # patch the runs with the runtype as the art option doesn't work
      runs = md["runs"]
      newruns = []
      for run in runs:
        run[2] = opts["runType"]
        newruns.append(run)
      print ("run fix",newruns)
      md["runs"]=newruns
      json.dump(md,fixed, indent=2,separators=(',',': '))
  else:
    print ("no json or failure",status)
  print ("subprocess returns:", status)
  return 0
  
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
