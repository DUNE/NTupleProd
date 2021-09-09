# needs to kno-
# project_name
# opts["sam_perfile"] # of files
import os,sys,time,string,datetime,socket
import samweb_client
import ifdh

e = "unknown"

opts = {}
opts["sam_perfile"]=4
opts["appFamily"] = "test"
opts["appName"]= "test"
opts["appVersion"] = os.getenv("DUNETPC_VERSION")
opts["process_description"]="testing sam access"
opts["MaxFiles"]=10000
samweb = samweb_client.SAMWebClient(experiment='dune')

def mytime():
  return datetime.datetime.now().strftime("%Y-%m-%d-%H%M.%S")

def test():
  larargs = "-c test.fcl  "
  project_name = "PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1"
  samExample(project_name,larargs)
  
def process(filelist,larargs):
  larcommand = "lar %s -s %s"%(larargs, filelist)
  print ("for now a dummy processing",larcommand)
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
  project_uri = info["projectURL"]
  
#  try:
#    project_uri = ifdh_handle.findProject(  project_name, "" )
#  except Exception:
#    print (mytime(),"findProject exception ", e)
#    sys.exit(1)

  print ("Got SAM project uri:",project_uri)
  nfileid = 0
  consumerid = 0
  perfile = opts["sam_perfile"]

  # to get n files per outputfile, we need to make n consumers, get the file handles and then pass those to the Gaudi job

  stillfiles = True

  #get files from SAM until it has no more

  consumer_id = samweb.startProcess(project_uri, opts["appFamily"], opts["appName"], opts["appVersion"], node=socket.gethostname(), description=opts["process_description"], maxFiles=opts["MaxFiles"], schemas="root")
    #consumer_id = ifdh_handle.establishProcess(project_uri,"ana",os.getenv("DUNE_RELEASE"), socket.gethostname(),os.getenv("GRID_USER"),"root-tuple")
  process_url = samweb.makeProcessUrl(project_uri, consumer_id)
  print (mytime(),"Got SAM consumer id:",consumer_id, process_url)
  consumerok = True
  
  while stillfiles:

    print ("stillfiles loop")
    nfiles = 0
    inputfiles = ""
    input_uris = []

    
    #try to get the next chunk of input files
    input_uri = ""
    
    while  nfiles < perfile:
      print (" try to get a file",nfiles)
      try:
        #input_uri = ifdh_handle.getNextFile( project_uri, consumer_id )
        input_uri = samweb.getNextFile(process_url)['url']
        print (mytime(),"  Got input_uri from ifdh: ", input_uri)
      except Exception:
        print (mytime()," getNextFile failed ",e)
        consumerok = False
        stillfiles = False
        samweb.setProcessStatus('bad',process_url )
        #ifdh_handle.setStatus(project_uri, consumer_id, "bad")
        break

      if input_uri == "":
         print (mytime(),"   SAM project says there are no more files.  Quitting...")
         stillfiles = False
         break
         
# got a file location
      input_uris.append(input_uri)
      try:
        inputfile = input_uri
        inputfilename = os.path.basename(inputfile)
        print(" got file ",inputfile)
        if inputfile == "":
          print (mytime(),"   No input file delivered, ifdh should have raised an exception " ,input_uri)
          stillfiles= False
          consumerok = False
          samweb.setProcessStatus('bad',process_url )
          #ifdh_handle.setStatus(project_uri, consumer_id, "bad")
          break
        print (mytime(),"  Fetched input:",inputfilename)
  
      except Exception:

      #todo can we just continue?
        
        stillfiles = False
        consumerok = False
        samweb.setProcessStatus('bad',process_url )
        #ifdh_handle.setStatus(project_uri, consumer_id, "bad")
        raise
        break
        
      nfiles = nfiles + 1
      inputfiles += " "+inputfile
    
      # end of loop to gather list of files
       
    
    status = process(inputfiles,larargs)
    
    # mark all files as bad if processing failed.
    if status != 0:
      for file in input_uris:
        samweb.setFileStatus(project_uri, consumer_id, urllib.quote(file), 'skipped' )
      
      
    
    

    print (mytime(),"end of loop to get files from consumer: INPUTLIST ",inputfiles)

    if not consumerok:
      try:
        print (mytime()," consumer not ok ", consumer_id, " try to set bad")
        samweb.setProcessStatus('failed',process_url )
       # ifdh_handle.setStatus(project_uri, consumer_id, "bad")
      except Exception:
        print (mytime()," can't even set to bad as consumer status failed",e)
        raise
        break

      #try to process the input files  we just got handles for
    
      
      


# return status for the whole list
 
      if status not in statuscodes:
        statuscodes[status] = 0
      statuscodes[status] += 1

      if status != 0:
        print (mytime()," there was an error, stop looping ")
        break
   

# clean up

 

  #if we got this far then we trust the project so mark it as completed
    try:
      if consumerok:
        print (mytime(), " set consumer ", consumer_id, "done")
        samweb.setProcessStatus('finished',process_url )
        #ifdh_handle.setStatus(project_uri, consumer_id, "completed")
      else:
        print (mytime(), " set consumer ", consumer_id, "bad")
        samweb.setProcessStatus('bad',process_url )
        #ifdh_handle.setStatus(project_uri, consumer_id, "bad")
    except Exception:
      print (mytime()," can't even set to bad as consumer status failed",e)

test()
