#!/bin/env python
#################################################
# For running MINERvA Gaudi jobs
#-----------------------
# contains helper classes:
#   OutputHandler - to transfer chunks of output
#   JobRunner     - to process one file
#-----------------------------------------
# Brian G Tice, 2012-2013
# tice.physics@gmail.com
# modified by H. Schellman for sam control

#  Note that this currently has to check filesize on the bluearc due to copy timing issue in metadata generation
#  10-7-2013
#############################################


# have to hack the status code to true to avoid bad code in v10r6p12

print " set up globals"

HACK=False

import sys,os,re,subprocess,socket,datetime,time,socket,string
from optparse import OptionParser
import samweb_client
import urllib
from samweb_client import utility
import json
import mergeMeta

DEBUG = True  #print debug info

debug = False

  # define the client we are talking to

samweb = samweb_client.SAMWebClient(experiment='minerva')



###################
#get an ifdh handle
###################
if not os.environ.has_key("IFDH_BASE_URI"):
  os.environ["IFDH_BASE_URI"] = "http://samweb.fnal.gov:8480/sam/minerva/api"


print " before ifdh setup"


import ifdh

print " after ifdh setup"

   
#ifdh needs the globus commands to be in the path.  it should already be there, but it's not.

if not os.environ.has_key("GLOBUS_PATH"):
  print "No GLOBUS_PATH variable.  Unable to get transfer using gftp.  Hopefully this is an interactive processing."
else:
  os.environ["PATH"] = os.environ["GLOBUS_PATH"] + "/bin:" + os.environ["PATH"]
 
ifdh_handle = ifdh.ifdh(os.environ["IFDH_BASE_URI"])

POSSIBLE_DIRS = ["ANA","DST","HIST","LOG","OPTS","POT","META"]

##########################
#
# OutputHandler
#
##########################
class OutputHandler:

  """Class to organize the transfer of files from condor to final location on disk"""

  def __init__(self, condor_dirs = None):
    self.dirMap = {}
    self.outputMap = {}

    if condor_dirs:
      for condor_dir in condor_dirs:
        self.addDir(condor_dir)


  def addDir(self, dirname):
    """Add the CONDOR_DIR_<dirname> to CONDOR_DEST_DIR_<dirname> relation to the directory map."""
    #look for both the CONDOR_DIR and CONDOR_DEST_DIR
    srcdir  = os.getenv("CONDOR_DIR_%s" % dirname)
    destdir = os.getenv("CONDOR_DEST_DIR_%s" % dirname)

    if DEBUG:
      print "addDir ", srcdir,destdir

    #if they both exist add it to the map
    if srcdir and destdir:
      self.dirMap[srcdir] = destdir

  def getDir(self, dirname):
    """Return CONDOR_DIR_<dirname> if it is in the directory map.  Return None if not in map."""
    srcdir = os.getenv("CONDOR_DIR_%s"%dirname)
    if srcdir in self.dirMap.keys():
      return srcdir
    else:
      return None

  def getDestDir(self, dirname):
    """Return CONDOR_DEST_DIR_<dirname> if it is in the directory map.  Return None if not in map."""
    destdir = os.getenv("CONDOR_DEST_DIR_%s"%dirname)
    if destdir in self.dirMap.values():
      return destdir
    else:
      return None


  def takeSnapshot(self, ignoreFiles = [] ):
    """Record all of the files that exist in the source directories.  Do not include files in the ignore list."""
    if DEBUG:
      print "OutputHandler: Taking snapshot of output files..."
    for src,dest in self.dirMap.iteritems():
      #no need to copy if the output and input are the same place
      if src == dest:
        continue
      listed = os.listdir(src)
      files = []
      for x in listed:
        file = os.path.join( src, x )
        if os.path.isfile( file ) and file not in ignoreFiles:
          if DEBUG:
            print "  ==> adding file",file
          files.append(file)
      self.outputMap[dest] = files

  def getSnapshotFiles(self):
    """Return a list of all files in the snapshot"""
    allfiles = []
    for dest,files in self.outputMap.iteritems():
      allfiles.extend( files )
    return allfiles

  def copySnapshot(self):

    print " copySnapshot called"
    """Copy files in the snapshot to their destinations"""
    #todo store return values and do exception handling
    #note: ifdh won't work for interactive jobs, but interactive jobs won't have an outputMap so it's OK
    #do them all in one shot to reduce lock requests
    if DEBUG:
      print "OutputHandler: Copy snapshot of output files to destinations..."
    cp_args = []
    for dest,files in self.outputMap.iteritems():
      for file in files:
        destfile = os.path.join( dest, os.path.basename(file) )
        cp_args.append(file)
        cp_args.append(destfile)
        cp_args.append(";")
        if DEBUG:
          print "    ==> src:",file,"---> dest",destfile
    if len(cp_args) > 0:
      cp_args.pop() #get rid of the last semi-colon
      if os.environ["FORCE_IFDH"] and os.environ["FORCE_IFDH"][0] == 'd':
        cp_args.insert(0, "--force=d")

      print "cp args ",cp_args
      try:
        ifdh_handle.cp( cp_args )
      except Exception, e:
        print "ifdh cp error",e
    return 0

  def purgeSnapshot(self):
    """Remove all the files in a snapshot"""
    #todo store return values and do exception handling
    if DEBUG:
      print "OutputHandler: Purge snapshot of local output files..."
    files = self.getSnapshotFiles()
    for f in files:
      if DEBUG:
        print "    ==> unlinking file:",f
      os.unlink(f)
    return 0

  def clearSnapshot(self):
    """Clear the files in the snapshot"""
    self.outputMap = {}

  def transferSnapshot(self):
    """Copy the snapshot of output files to the destination then remove them from the source directories"""
    cpRval = self.copySnapshot()
    purgeRval = self.purgeSnapshot()
    return cpRval + 10*purgeRval






def get_fs_freespace(pathname):
#"Get the free space of the filesystem containing pathname"
    dir = os.path.dirname(pathname)
    
    if os.path.exists(dir):
      stat= os.statvfs(dir)
    # use f_bfree for superuser, or f_bavail if filesystem
    # has reserved space for superuser
      return stat.f_bfree*stat.f_bsize
    else:
      return -1


########## metadata helpers ##############





############################################
## Function that forms merged metadata
## for a list of files
############################################

def dumpList(list):
    for item in list:
        print item, list[item]
        


def outputfiles(optsfile):

  files = {}
  output_types = {"DST":"DSTWriterAlg.OutputFile",
                  "HIST":"HistogramPersistencySvc.OutputFile",
                  "ANA":"ToolSvc.AnaTupleManager.TupleOutput",
                  "POT":"POTCounterAlg.POTTool.Output"}

  for line in open(optsfile):
    for type in output_types:
      key = output_types[type]
      if line.find(key) > -1 :
        fields = line.split("=")
        if(len(fields)>1 and not type in files):
          filename = string.strip(fields[1].replace(";","").replace('"',''))
          files[type]=filename
          print "found",type,filename
  print files
  return files
  
                  

#########################################################
#
# JobRunner
#
#########################################################
class JobRunner:
  """Class for running Gaudi jobs on the grid.  It stores the master information for the whole run and will process one an input file from subrun."""
  def __init__(self, opts):
    #refer to the OptionParser help below for information on these variables
    self.master_opts = opts["master_opts"]
    self.make_meta   = opts["make_meta"]
    self.input_files = opts["input_files"]
    if self.input_files:
      self.input_files = self.input_files.split(',')
    self.current_output = {}
    self.current_starttime = 0
    self.project_name = opts["project_name"]
    self.watch_log    = opts["watch_log"]
    self.outtag  = opts["outtag"][1:]  # strip off leading "_"
    self.executable   = os.getenv("SYSTEMTESTSROOT")+"/" + os.getenv("CMTCONFIG") + "/SystemTestsApp.exe"
    self.meta_cmd     = "%s %s" % ( os.getenv("PRODUCTIONSCRIPTSROOT")+"/rawdata_scripts/sampy.sh", os.getenv("PRODUCTIONSCRIPTSROOT")+"/data_scripts/MakeOfflineMeta.py")

    self.n_locks = 0 #keep track of the number of cpn locks used

    #for copying output
    self.output_handler_map = {}
    self.all_known_outputs = []


  def addToOutputHandlers( self, name, outputHandler ):
    """Add to the map of  -> output file handler."""
    #take a snapshot with this output map
    #be sure to ignore previously know output files, just in case the previous transfer isn't complete
    outputHandler.takeSnapshot( self.all_known_outputs )
    self.all_known_outputs.extend( outputHandler.getSnapshotFiles() )

    #add the hander to the map
    self.output_handler_map[name] = outputHandler

  def transferOutput( self, name = None ):
    """Transfer an existing output handler.  If no output handler name is given.  Take a snapshot now and transfer it."""
    if name:
      if name in self.output_handler_map.keys():
        return self.output_handler_map[name].transferSnapshot()
      else:
        raise Exception( "You requested that I transfer an output handler '%s', but it does not exist." % ( name ) )

    outputHandler = OutputHandler( POSSIBLE_DIRS )
    outputHandler.takeSnapshot()
    return outputHandler.transferSnapshot()


  def process( self, inputfile = None, ana_tool=None ):
    """Given an input file create a contrete instance of an options file and run the job.  If no input files given process them all."""
    #normally we run on a file which is copied to the local CONDOR_DIR_INPUT.  Sometimes we just want to trust the full filename given
    infiles = self.input_files
    metaMerger  = mergeMeta.mergeMeta({}) # make a metadata merger with default options
    if inputfile:
      infiles = [ inputfile ]

    #files will be in CONDOR_DIR_INPUT
    if not self.project_name:
     infiles = [ os.path.join(os.getenv("CONDOR_DIR_INPUT"), os.path.basename(infile) ) for infile in infiles ]
     if DEBUG:
       print "input files are ",infiles

    #remove repeate slashes
    infiles = [ re.sub( r'^/+(.*)', r'/\1', infile ) for infile in infiles ]

    #will look something like <subrun>-<subrun>-...
    subruns = []
    runs = []
    for infile in infiles:
      m = re.search( r'_(\d{8})_(\d{4})_', os.path.basename(infile) )
      if m:
        strsub = "%d.%d" % (string.atoi(m.group(1)),string.atoi(m.group(2)))
        subruns.append(strsub)

        print infile, strsub
      else:
        raise Exception( "Could not get subrun from file.  Something must be wrong.  The file is:%s" % (infile) )

    if opts["sam"] and len(subruns) > 0:
      subrun_replacer = "Subruns_"+subruns[0]
    else:
      subrun_replacer = "Subruns_"+"-".join(subruns)
    if ana_tool:
      subrun_replacer += "_" + ana_tool

    print "subrun_replacer is ",subrun_replacer


    outputHandler = OutputHandler( POSSIBLE_DIRS )

    # Create an options file using the master as a template
    optsfiledir = outputHandler.getDir("OPTS")
    if not optsfiledir:
      raise Exception("Could not create options file for this process because I couldn't find CONDOR_DIR_OPTS")

    optstemplate = self.master_opts

    # I don't understand regular expressions and the simple substitution was not working so I coded it in python.  HMS
    
    if ana_tool:
      if(opts["sam"]):
#        optstemplate = re.sub( r'(_SAM_\s_)', r'\1%s_' % ana_tool, optstemplate )
         index_sam = string.find(optstemplate,"SAM_")
         #print index_sam
         for index  in range(index_sam+len("SAM_"),len(optstemplate)):
           ch = optstemplate[index]
           #print index,ch
           if ch != "_":
             continue
           optstemplate = optstemplate[0:index]+"_"+ana_tool+"_"+optstemplate[index+1:]
           print "new template", optstemplate
           break
      else:
        optstemplate = re.sub( r'(_Run_\d{8}_)', r'\1%s_' % ana_tool, optstemplate )
      if DEBUG:
        print "    Ana Master options file:",optstemplate


    optsfile = os.path.join( optsfiledir, os.path.basename( optstemplate ).replace("Master",subrun_replacer) )
    print "new optsfile ",optsfile
    optsnew = open(optsfile, "w")

    
    for line in open( optstemplate ):
      #add input files
      if re.match( r'(^EventSelector\.Input).*', line ):
        line = ""
        for infile in infiles:
          line += """EventSelector.Input += {"DATAFILE='PFN:%s'  TYP='POOL_ROOTTREE' OPT='READ'" };\n"""  % infile

      #replace the unique piece of the output filenames
      line = re.sub( r'(REPLACE_SUBRUN)', subrun_replacer, line )

      optsnew.write( line )
    optsnew.close()

    self.current_output = outputfiles(optsfile)
    logfiledir     = outputHandler.getDir( "LOG" )
    if not logfiledir:
      raise Exception("Could not create log file for this process because I couldn't find CONDOR_DIR_LOG")
    logfile        = os.path.join( logfiledir, os.path.basename(optsfile).replace(".opts",".log") )

    #we may want to copy the logfile to bluarc with some frequency
    logfiledestdir = outputHandler.getDestDir( "LOG" )
    logfiledest    = os.path.join( logfiledestdir, os.path.basename(logfile) )
    print "        Logfile:",logfiledest

    starttime = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
  
    timerStart = datetime.datetime.now()
    self.current_starttime=timerStart
    fLogfile = open(logfile,"w")

    exec_cmd = [self.executable,optsfile]

    if DEBUG:
      print "       Executing:\n\t",exec_cmd
    if(len(infiles) < 1):
      fLogfile.write( " Seem to be no input files for this job, sometimes happens in sam - probably not a problem")
      fLogfile.close()
      ret=0
      return ret
    
    proc = subprocess.Popen( exec_cmd, shell=False, stdout=fLogfile, stderr=fLogfile )

    ret = None
    if self.watch_log:
      #if you want to watch the logfile...
      check_time = 30         # check to see if the job is still running every X seconds
      n_checks = 0            # keep track of how many checks you've done
      n_checks_for_copy = 2*60*3 # copy the logfile every X checks ( 2 check/minute * 60 minutes/hr * 3hrs/copy)
      while True:
        ret = proc.poll()
        if ret is None:
          time.sleep( check_time )
          n_checks += 1
          if n_checks % n_checks_for_copy == 0:
            if DEBUG:
              print "  I checked %d times at %s second intervals.  Now I copy the logfile." % ( n_checks, check_time )
            self.n_locks+=1
            try:
              ifdh_handle.cp( [logfile, logfiledest] )
            except Exception,e:
              print "ifdh cp error for logfile"
            
        else:
          break
    else:
      #if you don't need the logfile, then just sit and wait
      if DEBUG:
        print "  Do not watch the logfile.  Wait for Gaudi to finish then copy to bluearc."
      ret = proc.wait()
      

#######################
# todo: figure out how to make metadata out of these
    if self.make_meta:
      metafiledir     = outputHandler.getDir( "META" )
      print "meta data will go to directory",metafiledir, "and then to ",os.getenv("CONDOR_DEST_DIR_META")
      if not metafiledir:
        raise Exception("Could not create metadata file for this process because I couldn't find CONDOR_DIR_META")


      output_tiers_data = {"DST":"analyzed-dst",
                       "HIST":"analyzed-hist",
                       "ANA":"analyzed-tuple",
                       "POT":"analyzed-pot"}
      output_types_data = {"DST":"root",
                       "HIST":"root",
                       "ANA":"root",
                       "POT":"root"}

      print "   Making meta data..."

      print " outputs ",self.current_output
      if ret != 0:
        print "    Non meta-data will be made, because file was not processed without error."
      else:
        for type,recofile in self.current_output.iteritems():
          # yikes - the opts file gives me an environmental - just use what they really meant
          base = os.path.basename(recofile)
          path = os.path.dirname(recofile)
          newpath = outputHandler.getDir(type)
          if newpath == None:
            print " no output directory defined for file ",recofile
            continue
          recofile = os.path.join(outputHandler.getDir(type),base)
          if not os.path.exists(recofile):
            print recofile," did not exist, try output destination"
            recofile = os.path.join(outputHandler.getDestDir( type ),base)
            if not os.path.exists(recofile):
              print recofile, " did not exist there either"
              continue

          print "      Make meta data for output file ",recofile
          newmeta = {}
          newmeta["file_name"] = os.path.basename(recofile)
          newmeta["file_format"] = output_types_data[type]
          newmeta["start_time"] = mergeMeta.timeform(self.current_starttime)
          newmeta["end_time"] = mergeMeta.timeform(datetime.datetime.now())
          newmeta["application"]= {"family":"reconstructed","name":"ana","version":os.getenv("MINERVA_RELEASE")}
          newmeta["data_tier"] = output_tiers_data[type]
          
          newmeta["crc" ] =   utility.fileEnstoreChecksum(recofile)
          newmeta["file_size"] = os.path.getsize(recofile)

          outmeta = metaMerger.concatenate(infiles,newmeta)
          if outmeta == "Failure":
            print "concatenation failed "
            break
          try:
            command = "awk '/GatesUsed/ {print $4}' %s" % logfile
            print command
            nevents = os.popen(command).readline()
          
            print "events",nevents,outmeta["event_count"]
          
            outmeta["event_count"] = string.atoi(nevents)
          except:
            print "unable to get the number of events - use number of input events"
          if(self.outtag):
            outmeta["ID.outtag"] = self.outtag

          if DEBUG:
            for key in outmeta:
              print key, outmeta[key]
          samweb.validateFileMetadata(outmeta)
          metafilename       = os.path.join( metafiledir, newmeta["file_name"]+".json" )
          fLogfile.write("about to write metadata "+metafilename)
          metafile = open(metafilename,'w')
          json.dump(outmeta,metafile, sort_keys=True, indent=4,separators=(',',': '))

          metafile.close()
#          metafiledestdir = outputHandler.getDestDir( "META" )
#          metafiledest    = os.path.join( metafiledestdir, os.path.basename(metafilename) )

#          fLogfile.write( "Brute force copy of the metadata" )
          
#          try:
#            ifdh_handle.cp( [metafile, metafiledest] )
#          except:
#            print "Copy of metadata failed"

#---- now can close the logfile
        
    stoptime = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    timerStop = datetime.datetime.now()
    procTime = timerStop - timerStart
    fLogfile.write("Processing Time (s): %d\n" % procTime.seconds )
    fLogfile.write("Exit Status: %d\n" % ret )
    fLogfile.close()
    print "        Processing Time (s): %d" % procTime.seconds
    print "        Gaudi Exit Code    : %d" % ret
   
  #          cmd = " ".join( [self.meta_cmd, filetype, self.process, self.executable, optsfile, inputfile, recofile, starttime, stoptime] )
#          os.system( cmd )
#######################

    return ret

def mytime():
  return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

###################################################
# MAIN SCRIPT - process the subruns of this run
##############################
#
# GaudiRunProcessor.py --opts <master opts> --make_meta --input <input1,input2>  --project <project url> --watch_log
#    master opts    = template options file for the run (required)
#    make_meta      = after processing a file successfully make SAM metadata
#    intput(s)      = comma separated list of filenames (or basenames) to process, which will be found in CONDOR_DIR_INPUT
#                   input filenames are only needed if SAM will not be giving input
#    project url    = a SAM project url which will be used to get the input file#todo will get this from environmental variable from DAG
#    watch_log      = when processing on the condor system, I will copy the logfile every 30 seconds so you can watch the progress
#
#  Note: this expects condor style directory maps whether running on condor or not.
#        if running interactively, then set these environmental variables to fake it:
#        CONDOR_DIR_ANA     (AnaTuple files)
#        CONDOR_DIR_HIST    (histograms)
#        CONDOR_DIR_DST     (DST file)
#        CONDOR_DIR_POT     (POT files)
#        CONDOR_DIR_LOG     (log files)
#        CONDOR_DIR_OPTS    (opts files)
#     and if making SAM meta data ...
#        CONDOR_DIR_POOL_META    (pool meta)
#        CONDOR_DIR_DST_META     (dst meta)
#        CONDOR_DIR_HIST_META    (hist meta)
#        CONDOR_DIR_META
#
#     
###################################################


print "beginning of execution script"

location = socket.getfqdn()
print "*"*60
print "Running from host:",location
print "Time :",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print "Command :", sys.argv
print "*"*60
totalTimerStart = datetime.datetime.now()

if DEBUG:
  print "look at the environment"

  myenvs = list(os.environ.keys())

  for key in myenvs:
    print "%s=%s" % (key,os.getenv(key))
  
parser = OptionParser()
parser.add_option("--opts","--master_opts", dest="master_opts", default=None, help="template options file for the run (required)")
parser.add_option("--ana_tool", dest="ana_tools", action="append", help="Analysis tool to process.  Can use argument multiple times.")
parser.add_option("--make_meta", dest="make_meta", default=True, action="store_true", help="Make SAM metadata.")
parser.add_option("--input", "--input_files", dest="input_files", default=None, help="comma separated list of filenames")
parser.add_option("--project", "--project_name", dest="project_name", default=None, help="SAM project name used to get the input files")
parser.add_option("--watch_log", dest="watch_log", default=True, action="store_true", help="I will copy the logfile from the worker node to bluearc every 10 minutes")
parser.add_option("--sam_output_per_file",dest="sam_perfile",default=5,type="int",help="number of input files/output file in sam mode")
parser.add_option("--sam", default = False, action="store_true", help="Input information will come from sam")
parser.add_option("--outtag", dest="outtag", default="", type="string", help="output tag to go in sam")
(opts,args) = parser.parse_args()
opts = vars(opts)

if not opts["master_opts"]:
  print "ERROR: Master options file not supplied.  See help."
  parser.parse_args("--help".split())

if opts["input_files"] and opts["project_name"]:
  print "ERROR: You cannot supply both filenames and a SAM project name.  See help."
  parser.parse_args("--help".split())

# default to $SAM_PROJECT_NAME...
if not opts["input_files"] and not opts["project_name"]:
  if os.environ.has_key("SAM_PROJECT_NAME"):
    opts["project_name"] = os.environ["SAM_PROJECT_NAME"]
  else:
    print "ERROR: You must supply filenames or a SAM project name so I know which input files to process.  See help."
    parser.parse_args("--help".split())


if not opts["sam"]:
   print "sam is false so setting sam related flags to False"
   opts["make_meta"] = False
   opts["sam_perfile"] = 0

job = JobRunner(opts)

n_tried = 0
statuscodes = {}     #map from status code to number of times it was seen

if not job.input_files:
  print mytime(), "Getting files from Sam project with name:",job.project_name

  try:
    project_uri = ifdh_handle.findProject(  job.project_name, "" )
  except Exception,e:
    print mytime(),"findProject exception ", e
    sys.exit(1)

  print "Got SAM project uri:",project_uri
  nfileid = 0
  consumerid = 0
  perfile = opts["sam_perfile"]

  # to get n files per outputfile, we need to make n consumers, get the file handles and then pass those to the Gaudi job


  stillfiles = True

  #get files from SAM until it has no more

  while stillfiles:

    print "stillfiles loop"

    nfiles = 0
    inputlist = []

    try:
      consumer_id = ifdh_handle.establishProcess(project_uri,"ana",os.getenv("MINERVA_RELEASE"), socket.gethostname(),os.getenv("GRID_USER"),"reconstructed")
      print mytime(),"Got SAM consumer id:",consumer_id
    except Exception, e:
      print mytime()," could not get a consumer ",e
      break
    print mytime(),"consumer ids", consumerid

    #try to get the next input file
    input_uri = ""
    consumerok = True
    while  nfiles < perfile:
      try:        
        input_uri = ifdh_handle.getNextFile( project_uri, consumer_id )
        print mytime(),"  Got input_uri from ifdh: ", input_uri
      except Exception, e:
        print mytime()," getNextFile failed ",e
        consumerok = False
        stillfiles = False
        ifdh_handle.setStatus(project_uri, consumer_id, "bad")

        break
      
      if input_uri == "":
         print mytime(),"   SAM project says there are no more files.  Quitting..."
         stillfiles = False
         break
      try:
        inputfile = ifdh_handle.fetchInput(input_uri)

        if inputfile == "":
          print mytime(),"   No input file delivered, ifdh should have raised an exception " ,input_uri
          stillfiles= False
          consumerok = False
          ifdh_handle.setStatus(project_uri, consumer_id, "bad")
          break
        print mytime(),"  Fetched input:",inputfile," space left is ",get_fs_freespace(inputfile)

      except Exception, e:
      
      #todo can we just continue?
        print mytime(),"fetchInput ifdh error:", e, " quitting big time"
        try:
          ifdh_handle.updateFileStatus(project_uri, consumer_id, urllib.quote(input_uri), 'skipped' )
        except Exception, e:
          print mytime()," can't even set it to skipped as file status failed",e
        stillfiles = False
        consumerok = False
        ifdh_handle.setStatus(project_uri, consumer_id, "bad")
        raise
        break
      if os.path.exists(inputfile):
          inputlist.append( inputfile)
          try:
            ifdh_handle.updateFileStatus(project_uri, consumer_id, urllib.quote(input_uri), 'consumed' )
          except Exception,e:
            print mytime()," can't even set it to skipped as file status failed",e
            raise
            break
          nfiles = nfiles + 1
      else:
          print mytime(),"SAM lied - this file was not delivered, process what we have but then bail"
          try:
            ifdh_handle.updateFileStatus(project_uri, consumer_id, urllib.quote( input_uri), 'skipped' )
          except Exception, e:
            print mytime()," can't even set it to skipped as file status failed",e
          stillfiles = False
          consumerok = False
          try:
            ifdh_handle.setStatus(project_uri, consumer_id, "bad")
          except Exception, e:
            print mytime()," can't even set to bad as consumer status failed",e
          raise
          break


    print mytime(),"end of loop to get files from consumer: INPUTLIST ",inputlist

    if not consumerok:
      try:
        print mytime()," consumer not ok ", consumer_id, " try to set bad"
        ifdh_handle.setStatus(project_uri, consumer_id, "bad")
      except Exception, e:
        print mytime()," can't even set to bad as consumer status failed",e
      raise  
      break
    
      #try to process the input files  we just got handles for
    try:
      print mytime(),"     Processing files:",inputlist
      n_tried += len(inputlist)

      #todo record status separately for each tool
      status = 0
      job.input_files = inputlist
      
      if len( opts["ana_tools"] ) == 0:
        status = job.process()
      else:
        for ana_tool in opts["ana_tools"]:
          print "-"*40
          print mytime(),"    Running analysis:",ana_tool

          newStatus = job.process( ana_tool = ana_tool )
          status = status or newStatus
          print mytime(),"    Analysis returned:",newStatus
          

# return status for the whole list
      if(HACK):
        status = 0
         
      if status not in statuscodes:
        statuscodes[status] = 0
      statuscodes[status] += 1
      
      if status != 0:
        print mytime()," there was an error, stop looping "
        break
    except Exception, e:
      print mytime()," problem someplace in real processing ", e
      raise
      break

# clean up

    for files in inputlist:
      print mytime(),"try to remove", files
      if files.find("/local")> -1:
        print mytime(),"removing input file after output produced",files
        os.remove(files)
        print mytime(),"removed ",files, "remaining space is ", get_fs_freespace(files)
        
  #if we got this far then we trust the project so mark it as completed
    try:
      if consumerok:
        print mytime(), " set consumer ", consumer_id, "complete"
        ifdh_handle.setStatus(project_uri, consumer_id, "completed")
      else:
        print mytime(), " set consumer ", consumer_id, "bad"
        ifdh_handle.setStatus(project_uri, consumer_id, "bad")
    except Exception, e:
      print mytime()," can't even set to bad as consumer status failed",e
      raise
  

else:  #process all user supplied files at once

  print mytime(),"Processing all input files:", job.input_files
   
  try: #try to process the file
    n_tried += 1

    status = 0
    if len( opts["ana_tools"] ) == 0:
      status = job.process( )
    else:
      for ana_tool in opts["ana_tools"]:
        print "-"*40
        print mytime(),"    Running analysis:",ana_tool
        newStatus = job.process( ana_tool=ana_tool )
        status = status or newStatus
        print mytime(),"    Analysis returned:",newStatus

    if status not in statuscodes:
      statuscodes[status] = 0
    statuscodes[status] += 1
  except Exception, e:
    print mytime(),"    Unable to process files. Exception was:\n\t:",e
    raise

  print "="*40
  print mytime(),"All done with user supplied input files.  Try to get more"

totalTimerStop = datetime.datetime.now()
totaProcTime   = totalTimerStop - totalTimerStart

print "*"*60
print "End Time :",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print "Processing Time (s) :",totaProcTime.seconds
print "Number of jobs attempted:",n_tried
print "Number of CPN locks used:",job.n_locks
print "Summary of status codes:"
print "   Status Code, Number of Times Seen"
for k,v in statuscodes.iteritems():
  print "    %d, %d" % (k,v)
print "*"*60

