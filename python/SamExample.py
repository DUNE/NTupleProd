# needs to kno-
# project_name
# opts["sam_perfile"] # of files
import os,sys,time,string,datetime,socket
import samweb_client
import ifdh
import subprocess

e = "unknown"
statuscodes ={}
opts = {}
opts["sam_perfile"]=2
opts["appFamily"] = "test"
opts["appName"]= "test"
opts["appVersion"] = os.getenv("DUNETPC_VERSION")
opts["process_description"]="testing sam access"
opts["MaxFiles"]=6
samweb = samweb_client.SAMWebClient(experiment='dune')

def mytime():
  return datetime.datetime.now().strftime("%Y-%m-%d-%H%M.%S")

def test():
  larargs = "-c./test.fcl"
  project_name = "PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1"
  samExample(project_name,larargs)
  
def process(filelist,larargs):
  if len(filelist)< 1:
    return -1
  larcommand = ["lar",larargs]
  files = filelist.split(" ")
  filename = os.path.basename(files[0]).replace(".root","")+"_tuple"
  larcommand += files
  logname = open(filename+".out",'w')
  errname = open(filename+".err",'w')
  print ("try to launch",larcommand,filename)
  ret = subprocess.run(larcommand,stdout=logname,stderr=errname)
  print ("subprocess returns:", ret)
  return 0
  
def startProject(def_name):
  project_name = def_name+"_"+mytime()
  val = samweb.startProject(project_name,def_name)
  print ("startProject",val)
  return val


def samExample(def_name,larargs):

  print (mytime(), "Getting files from Sam definition with name:",def_name)
  
  info = startProject(def_name)
  
  project_name = info["project"]
  project_url = info["projectURL"]
  
#  try:
#    project_url = ifdh_handle.findProject(  project_name, "" )
#  except Exception:
#    print (mytime(),"findProject exception ", e)
#    sys.exit(1)

  print ("Got SAM project url:",project_url)
 
  consumer_id = 0
  perfile = opts["sam_perfile"]

  # to get n files per outputfile, we need to make n consumers, get the file handles and then pass those to the Gaudi job

  stillfiles = True
  
  ntotal = 0

  #get files from SAM until it has no more

  consumer_id = samweb.startProcess(project_url, opts["appFamily"], opts["appName"], opts["appVersion"], node=socket.gethostname(), description=opts["process_description"], maxFiles=opts["MaxFiles"], schemas="root")
    #consumer_id = ifdh_handle.establishProcess(project_url,"ana",os.getenv("DUNE_RELEASE"), socket.gethostname(),os.getenv("GRID_USER"),"root-tuple")
  process_url = samweb.makeProcessUrl(project_url, consumer_id)
  print (mytime(),"Got SAM consumer id:",consumer_id, process_url)
  consumerok = True
  
  while stillfiles and ntotal < opts["MaxFiles"]:
    ntotal += 1
    print ("stillfiles loop",ntotal)
    nfiles = 0
    inputfiles = ""
    input_urls = []

    
    #try to get the next chunk of input files
    input_url = ""
    
    while  nfiles < opts["sam_perfile"]:
      print (" try to get a file",nfiles)
      input_url = ""
      try:
        next = samweb.getNextFile(process_url)
        print ("next file info ",next)
        input_url = next['url']
        filename=next["filename"]
        print (mytime(),"  Got next input_url from ifdh: ", input_url)
        samweb.releaseFile(process_url, filename, status="ok")
      except samweb_client.exceptions.NoMoreFiles:
        print ("ran out of files")
        break
      except Exception:
        print ("something bad happened in nextfile")
        break
#      except Exception:
#        print (mytime()," getNextFile failed ",e)
#        consumerok = False
#        stillfiles = False
#        samweb.setProcessStatus('bad',process_url )
#        #ifdh_handle.setStatus(project_url, consumer_id, "bad")
#        break

      if input_url == "":
         print (mytime(),"   SAM project says there are no more files.  Quitting...")
         stillfiles = False
         break
         
# got a file location
      input_urls.append(input_url)
      
      print(" got file ",input_url)
#      try:
#        inputfile = input_url
#        inputfilename = os.path.basename(inputfile)
#        print(" got file ",inputfile)
#        if inputfile == "":
#          print (mytime(),"   No input file delivered, ifdh should have raised an exception " ,input_url)
#          stillfiles= False
#          consumerok = False
#          samweb.setProcessStatus('bad',process_url )
#          #ifdh_handle.setStatus(project_url, consumer_id, "bad")
#          break
#        print (mytime(),"  Fetched input:",inputfilename)
#
#      except Exception:
#
#      #todo can we just continue?
#
#        stillfiles = False
#        consumerok = False
#        samweb.setProcessStatus('bad',process_url )
#        #ifdh_handle.setStatus(project_url, consumer_id, "bad")
#        raise
#        break
        
      nfiles = nfiles + 1
      inputfiles += input_url + " "
    
      # end of loop to gather list of files
       
    print (" have a bunch of input files ", inputfiles)
    if len(inputfiles) > 0:
      status = process(inputfiles,larargs)
    else:
      print ("ran out of files")
      stillfiles = False
    # mark all files as bad if processing failed.
#    if status != 0:
#      for file in input_urls:
#        samweb.releaseFile(process_url, filename, status="bad")
#        print ("release bad",file)
#        #samweb.setFileStatus(project_url, consumer_id, urllib.quote(file), 'skipped' )
#      consumerok = False
#    else:
#      for file in input_urls:
#        filename = os.path.basename(file)
#        print ("release ok",file)
#        samweb.releaseFile(process_url, filename, status="ok")
#
#      consumerok = False
    
    
    print (mytime(),"end of loop to get files from consumer:  ",inputfiles)
    
  # end loop over all files
  if not consumerok:
    try:
      print (mytime()," consumer not ok ", consumer_id, " try to set bad")
      samweb.setProcessStatus('bad',process_url )
     # ifdh_handle.setStatus(project_url, consumer_id, "bad")
    except Exception:
      print (mytime()," can't even set to bad as consumer status failed",e)
      raise
      

      #try to process the input files  we just got handles for
    
      
      


# return status for the whole list
 
#    if status not in statuscodes:
#      statuscodes[status] = 0
#    statuscodes[status] += 1
#
#    if status != 0:
#      print (mytime()," there was an error, stop looping ")
#
   

# clean up

 

  #if we got this far then we trust the project so mark it as completed
    try:
      if consumerok:
        print (mytime(), " set consumer ", consumer_id, "done")
        samweb.setProcessStatus('finished',process_url )
        #ifdh_handle.setStatus(project_url, consumer_id, "completed")
      else:
        print (mytime(), " set consumer ", consumer_id, "bad")
        samweb.setProcessStatus('bad',process_url )
        #ifdh_handle.setStatus(project_url, consumer_id, "bad")
    except Exception:
      print (mytime()," can't even set to bad as consumer status failed",e)
      
  samweb.stopProject(project_url)

test()
