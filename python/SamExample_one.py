# needs to kno-
# project_name
# opts["sam_perfile"] # of files
import os,sys,time,string,datetime,socket
import samweb_client
import ifdh
import subprocess
 
# test setup to be fixed up later with real args
e = "unknown"
statuscodes ={}
opts = {}
opts["n_consumers"]=2
opts["appFamily"] = "protoduneana"
opts["appName"]= "test"
opts["process_description"]="testing sam access"
opts["MaxFiles"]=2
opts["jsonName"]="ana_hist.root.json"

# need this
samweb = samweb_client.SAMWebClient(experiment='dune')

def mytime():
  return datetime.datetime.now().strftime("%Y-%m-%d-%H%M.%S")

def test():
  larargs = ["-c./test.fcl"]+["-n100"]
  project_name = "PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1"
  samExample(project_name,larargs)
  
#def process(filelist,larargs):
#  if len(filelist)< 1:
#    return -1
#  larcommand = ["lar"]+[larargs]
#  files = filelist.split(" ")
#  filename = os.path.basename(files[0]).replace(".root","")+"_tuple"
#  #larcommand += ["-o %s.root"%filename]
#
#  larcommand += files
#  logname = open(filename+".out",'w')
#  errname = open(filename+".err",'w')
#  jsonname = open(filename+".json",'w')
#  print ("try to launch",larcommand,filename)
#  ret = subprocess.run(larcommand,stdout=logname,stderr=errname)
#  os.path.rename(opts["jsonName"],jsonname)
#  print ("subprocess returns:", ret)
#  return 0
  
def process_sam(project_url,project_name,consumer_id,larargs):
  
  larcommand = ["lar"]+larargs
  
  larcommand += ["--sam-web-uri=%s"%project_url]
  larcommand += ["--sam-process-id=%s"%consumer_id]
  larcommand += ["--sam-application-family=%s"%opts["appFamily"]]
  larcommand += ["--sam-application-version=%s"%os.getenv("PROTODUNEANA_VERSION")]
  #arcommand += ["--sam-application-name=pdspana"]
  filename = "protoduneana_%s_%s"%(project_name,consumer_id)
  logfile = open(filename+".out",'w')
  errfile = open(filename+".err",'w')
  jsonname =filename+".json"
  print ("try to launch",larcommand,consumer_id)
  ret = subprocess.run(larcommand,stdout=logfile,stderr=errfile)
  if ret == 0 and os.path.exists(opts["jsonName"]):
    os.rename(opts["jsonName"],jsonname)
  print ("subprocess returns:", ret)
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
  
#  try:
#    project_url = ifdh_handle.findProject(  project_name, "" )
#  except Exception:
#    print ("SamExample:",mytime(),"findProject exception ", e)
#    sys.exit(1)

  print ("Got SAM project url:",project_url)
 
  consumer_id = 0
  
  n_consumers = opts["n_consumers"]

  # to get n files per outputfile, we need to make n consumers, get the file handles and then pass those to the Gaudi job

  
  #get files from SAM until it has no more

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
          print ("SamExample:",mytime(), " set consumer ", consumer_id, "done")
          samweb.setProcessStatus('finished',process_url )
          #ifdh_handle.setStatus(project_url, consumer_id, "completed")
        else:
          print ("SamExample:",mytime(), " set consumer ", consumer_id, "bad")
          samweb.setProcessStatus('bad',process_url )
          #ifdh_handle.setStatus(project_url, consumer_id, "bad")
    except Exception:
        print ("SamExample:",mytime()," can't even set to bad as consumer status failed",e)
      
  samweb.stopProject(project_url)
  out = samweb.projectSummaryText(project_url)
  print (out)

test()
