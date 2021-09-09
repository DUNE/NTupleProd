# needs to kno-
# project_name
# opts["sam_perfile"] # of files
import os,sys,time,string,datetime
import samweb_client
import ifdh

e = "unknown"


samweb = samweb_client.SAMWebClient(experiment='dune')

def mytime():
  return datetime.datetime.now().strftime("%Y-%m-%d-%H%M.%S")

def test():
  larargs = "-c test.fcl -n2"
  project_name = "PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1"
  samExample(project_name,larargs)
  
def process(file,larargs):
  larcommand = "lar %s -i%s"%(larargs, file)
  print ("for now a dummy processing",larcommand)
  
def startProject(def_name):
  project_name = def_name+"_"+mytime()
  val = samweb.startProject(project_name,def_name)
  print ("startProject",val)
  return val


def samExample(def_name,larargs):

  print (mytime(), "Getting files from Sam definition with name:",def_name)
  
  info = startProject(def_name)
  
  try:
    project_uri = ifdh_handle.findProject(  project_name, "" )
  except Exception:
    print (mytime(),"findProject exception ", e)
    sys.exit(1)

  print ("Got SAM project uri:",project_uri)
  nfileid = 0
  consumerid = 0
  perfile = opts["sam_perfile"]

  # to get n files per outputfile, we need to make n consumers, get the file handles and then pass those to the Gaudi job

  stillfiles = True

  #get files from SAM until it has no more

  while stillfiles:

    print ("stillfiles loop")

    nfiles = 0
    inputlist = []

    try:
      consumer_id = ifdh_handle.establishProcess(project_uri,"ana",os.getenv("DUNE_RELEASE"), socket.gethostname(),os.getenv("GRID_USER"),"root-tuple")
      print (mytime(),"Got SAM consumer id:",consumer_id)
    except Exception:
      print (mytime()," could not get a consumer ",e)
      break
    print (mytime(),"consumer ids", consumerid)
    #try to get the next input file
    input_uri = ""
    consumerok = True
    while  nfiles < perfile:
      try:
        input_uri = ifdh_handle.getNextFile( project_uri, consumer_id )
        print (mytime(),"  Got input_uri from ifdh: ", input_uri)
      except Exception:
        print (mytime()," getNextFile failed ",e)
        consumerok = False
        stillfiles = False
        ifdh_handle.setStatus(project_uri, consumer_id, "bad")

        break

      if input_uri == "":
         print (mytime(),"   SAM project says there are no more files.  Quitting...")
         stillfiles = False
         break
      try:
        inputfile = ifdh_handle.fetchInput(input_uri)

        if inputfile == "":
          print (mytime(),"   No input file delivered, ifdh should have raised an exception " ,input_uri)
          stillfiles= False
          consumerok = False
          ifdh_handle.setStatus(project_uri, consumer_id, "bad")
          break
        print (mytime(),"  Fetched input:",inputfile," space left is ",get_fs_freespace(inputfile))

      except Exception:

      #todo can we just continue?
        print (mytime(),"fetchInput ifdh error:", e, " quitting big time")
        try:
          ifdh_handle.updateFileStatus(project_uri, consumer_id, urllib.quote(input_uri), 'skipped' )
        except Exception:
          print (mytime()," can't even set it to skipped as file status failed",e)
        stillfiles = False
        consumerok = False
        ifdh_handle.setStatus(project_uri, consumer_id, "bad")
        raise
        break
      if os.path.exists(inputfile):
          try:
            status = process(inputfile,larargs)
          except Exception:
            print ("processing returned",status)
            ifdh_handle.updateFileStatus(project_uri, consumer_id, urllib.quote(input_uri), 'failed' )
            raise
            break
          try:
            ifdh_handle.updateFileStatus(project_uri, consumer_id, urllib.quote(input_uri), 'consumed' )
          except Exception:
            print (mytime()," can't even set it to skipped as file status failed",e)
            raise
            break
          nfiles = nfiles + 1
      else:
          print (mytime(),"SAM lied - this file was not delivered, process what we have but then bail")
          try:
            ifdh_handle.updateFileStatus(project_uri, consumer_id, urllib.quote( input_uri), 'skipped' )
          except Exception:
            print (mytime()," can't even set it to skipped as file status failed",e)
          stillfiles = False
          consumerok = False
          try:
            ifdh_handle.setStatus(project_uri, consumer_id, "bad")
          except Exception:
            print (mytime()," can't even set to bad as consumer status failed",e)
          raise
          break


    print (mytime(),"end of loop to get files from consumer: INPUTLIST ",inputlist)

    if not consumerok:
      try:
        print (mytime()," consumer not ok ", consumer_id, " try to set bad")
        ifdh_handle.setStatus(project_uri, consumer_id, "bad")
      except Exception:
        print (mytime()," can't even set to bad as consumer status failed",e)
      raise
      break

      #try to process the input files  we just got handles for
    try:
      print (mytime(),"     Processing files:",inputlist)
      n_tried += len(inputlist)

      #todo record status separately for each tool
      status = 0
      input_files = inputlist

      # run it
      
      


# return status for the whole list
      if(HACK):
        status = 0

      if status not in statuscodes:
        statuscodes[status] = 0
      statuscodes[status] += 1

      if status != 0:
        print (mytime()," there was an error, stop looping ")
        break
    except Exception:
      print (mytime()," problem someplace in real processing ", e)
      raise
      break

# clean up

    for files in inputlist:
      print (mytime(),"try to remove", files)
      if files.find("/local")> -1:
        print (mytime(),"removing input file after output) produced",files)
        os.remove(files)
        print (mytime(),"removed ",files, "remaining space is ", get_fs_freespace(files))

  #if we got this far then we trust the project so mark it as completed
    try:
      if consumerok:
        print (mytime(), " set consumer ", consumer_id, "complete")
        ifdh_handle.setStatus(project_uri, consumer_id, "completed")
      else:
        print (mytime(), " set consumer ", consumer_id, "bad")
        ifdh_handle.setStatus(project_uri, consumer_id, "bad")
    except Exception:
      print (mytime()," can't even set to bad as consumer status failed",e)
      raise

test()
