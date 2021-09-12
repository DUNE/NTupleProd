########## metadata helper ##############

# this provides a meta data merger class which given a defined list of external information about a file and a list of input files, will produce metadata for the output.

import os,sys,time,datetime
import samweb_client
from samweb_client import utility
import json

samweb = samweb_client.SAMWebClient(experiment='minerva')
DEBUG=False


#-------utilities ------#

def dumpList(list):
    for item in list:
      print item, list[item]


def timeform(now):
  timeFormat = "%Y-%m-%d_%H:%M:%S"
  nowtime = now.strftime(timeFormat)
  nowTstamp= time.strptime(nowtime,timeFormat)
  return int(time.mktime(nowTstamp))


class mergeMeta():
  #""" Base class for making metadata for a file based on parents"""
  def __init__(self, opts):
    
    self.opts = opts #this is a dictionary containing the option=>value pairs given at the command line
    self.samweb = samweb_client.SAMWebClient(experiment='minerva')
    self.externals = ["file_name","file_format","start_time","end_time","crc","file_size","application","data_tier"]
    self.consistent = ["file_type","file_format","data_tier","group","data_stream" ]
    self.ignore = ["checksum","create_date","Offline.options","first_event",
"Offline.optionspath",
"SupDigits.options",
"RawDigits.options",
"parents",
"Offline.machine",
"file_id",
"lum_block_ranges",
"last_event","RawDigits.optionspath","Online.triggertype"]



############################################
## Function that forms merged metadata
## for a list of files
############################################


  def checkmerge(self,list):
    
  
    checks = {}
    for tag in self.consistent:
      checks[tag] = []
    
    try:
      for file in list:
        filename = os.path.basename(file)
        if DEBUG:
          print filename
        try:
          thismeta = self.samweb.getMetadata(filename)
        except samweb_client.exceptions.Error, x:
          print " can't get sam metadata ",filename,x
          return False
      
        if DEBUG:
          dumpList(thismeta)
        
        # here to find the must not mix ones
        for tag in self.consistent:
          if tag not in thismeta:
              checks[tag].append("missing")  # if it ain't there, it aint there.
          if thismeta[tag] not in checks[tag]:
            checks[tag].append(thismeta[tag])

    except Exception, x:
      print " error checking parents",x
      return False
  
  
    for tag in self.consistent:
            if(len(checks[tag]) != 1):
              print "tag ", tag, " has problem ",checks[tag]
              return False
    
    return True

  
  def concatenate(self,list,externals):

    # here are things that are unique to the output and must be supplied externally
    
    for tag in self.externals:
        if not tag in externals:
            print "must supply ",tag," before we can merge"
            sys.exit(2)
    newmeta = externals       
    firstevent = 999999999999L
    lastevent = -999L
    runlist = []
    eventcount=0
    parentage = []
    if(len(list) < 1):
        return []
    lumlist = []
    starttime = ""
    endtime=""

    # here are things that are internals and must be consistent
    
    checks = {}
    mix = {}
    other = {}
    for tag in self.consistent:
        checks[tag] = []
    
    try:
        for file in list:
            filename = os.path.basename(file)
            if DEBUG:
              print filename
            try:
              thismeta = self.samweb.getMetadata(filename)
            except samweb_client.exceptions.Error, x:
              print " can't get sam metadata ",filename,x
              sys.exit(1)
            
            for tag in thismeta:
                if DEBUG:
                    print " check tag ", tag
                if tag not in self.consistent and tag not in self.externals and tag not in mix:
                    if DEBUG:
                        print " found a new parameter to worry about", tag
                    mix[tag]=[thismeta[tag]]
            if DEBUG:
                dumpList(thismeta)
                      
            # here to find the must not mix ones
            for tag in self.consistent:
                if thismeta[tag] not in checks[tag]:
                    checks[tag].append(thismeta[tag])
            # here to find others
            for tag in mix:
                if tag in thismeta:
                    if thismeta[tag] not in mix[tag]:
                        mix[tag].append(thismeta[tag])
                        if DEBUG:
                            print "tag",tag," has", len(mix[tag]), "mixes"
            # get info from the parent files

            try:
                if thismeta["first_event"] <= firstevent:
                    firstevent = thismeta["first_event"]
                if thismeta["last_event"] >= lastevent:
                    lastevent = thismeta["last_event"]
                eventcount = eventcount + thismeta["event_count"]
            except:
                print "something in event count, firstevent, lastevent is missing"
            # is this already in the runlist 
            runlist =runlist + thismeta["runs"]
            if DEBUG: 
                print thismeta["runs"], runlist
            parentage = parentage+ [{'file_name':filename,'file_id':thismeta['file_id']}]
            if(not thismeta.has_key("lum_block_ranges")):
                thismeta["lum_block_ranges"] = [[firstevent,lastevent]]
            lumlist = lumlist + thismeta["lum_block_ranges"]
    except Exception, e:
        print "WARNING: error in merge of metadata:",e
        return "Failure"

    newJsonData={}
    # full metadata is available

    # first check that things are not getting mixed up, these are things that should carry through.

    for tag in self.consistent:
        if(len(checks[tag]) != 1):
            print "tag ", tag, " has problem ",checks[tag]
            sys.exit(1)
        else:
            newJsonData[tag] = checks[tag][0]

            

# inherit from parents if all are consistent

    for tag in mix:
        if tag in self.ignore:
            continue
        if tag == "runs":
            continue
        if tag == "event_count":
            continue
        if len(mix[tag]) == 1:
           newJsonData[tag]=mix[tag][0]
        if len(mix[tag]) > 1:
            print "don't write out mixed tags ", tag
            #newJsonData[tag] = "mixed"
    
        


    # overwrite with the externals if they are there
    
    for tag in externals:
        newJsonData[tag] = externals[tag]
  
    #if no event count was provided from externals, use the input files
    if("event_count" not in newJsonData or newJsonData["event_count"] == -1):
          newJsonData["event_count"] = eventcount
          
    # set these from the parents
    if(firstevent!=-1 and lastevent !=-1):
        newJsonData["first_event"]=firstevent
        newJsonData["last_event"]=lastevent
        newJsonData["runs"]=runlist
        newJsonData["parents"]=parentage
        newJsonData["lum_block_ranges"]=lumlist

    #events/lumblock info is missing
    else:
        newJsonData["first_event"]=firstevent
        newJsonData["last_event"]=lastevent
        newJsonData["event_count"]=eventcount
        newJsonData["runs"]=runlist
        newJsonData["parents"]=parentage

    if newJsonData["data_stream"]=="mc":
        newJsonData["first_event"]=firstevent
        newJsonData["last_event"]=lastevent
        newJsonData["event_count"]=eventcount
        newJsonData["runs"]=runlist
        newJsonData["parents"]=parentage
        newJsonData["lum_block_ranges"]=lumlist
 
    if(DEBUG):

        print "-------------------\n"
        dumpList(newJsonData)
    try:
        self.samweb.validateFileMetadata(newJsonData)
    except Exception, x:
        print " metadata validation faild - write it out anyways", x
    return newJsonData




if __name__ == "__main__":
  
  opts = {}
  
  filename = "test.root"
  data_tier = "analyzed-dst"
  
  maker = mergeMeta(opts)
  
  inputfiles=[   "MV_00003129_0010_numil_v09_1104302155_RecoData_v10r6.root"
    ,"MV_00003130_0020_numib_v09_1105012301_RecoData_v10r6.root"
    ,"MV_00003123_0061_numib_v09_1104281245_RecoData_v10r6.root"
    ,"MV_00003129_0056_numil_v09_1105011431_RecoData_v10r6.root"
    ,"MV_00003124_0061_numib_v09_1104291036_RecoData_v10r6.root"
    ,"MV_00003122_0028_numil_v09_1104262332_RecoData_v10r6.root"
    ,"MV_00003130_0019_numil_v09_1105012247_RecoData_v10r6.root"
    ]


  inputfiles =[
    "MV_00002061_0002_numib_v05_1004032132_RawDigits_raw2_numibeam.root",
"MV_00002061_0003_numip_v05_1004032135_RawDigits_raw2_numibeam.root",
"MV_00002061_0007_numib_v05_1004040218_RawDigits_raw2_numibeam.root",
"MV_00002061_0015_numib_v05_1004041159_RawDigits_raw2_numibeam.root",
"MV_00002061_0009_numib_v05_1004040452_RawDigits_raw2_numibeam.root",
"MV_00002061_0011_numib_v05_1004040729_RawDigits_raw2_numibeam.root",
"MV_00002061_0014_numib_v05_1004041042_RawDigits_raw2_numibeam.root",
"MV_00002061_0008_numib_v05_1004040334_RawDigits_raw2_numibeam.root",
"MV_00002061_0013_numip_v05_1004041003_RawDigits_raw2_numibeam.root",
"MV_00002061_0019_numib_v05_1004041412_RawDigits_raw6_numibeam.root",
"MV_00002061_0004_numib_v05_1004032218_RawDigits_raw2_numibeam.root",
"MV_00002061_0005_numib_v05_1004032343_RawDigits_raw2_numibeam.root",
"MV_00002061_0006_numib_v05_1004040102_RawDigits_raw2_numibeam.root",
"MV_00002061_0010_numib_v05_1004040608_RawDigits_raw2_numibeam.root",
"MV_00002061_0012_numib_v05_1004040847_RawDigits_raw2_numibeam.root",
"MV_00002061_0018_numip_v05_1004041330_RawDigits_raw2_numibeam.root"]
  inputfiles.sort()
    
  externals = {"file_name":filename,
              "file_format":"root",
              "start_time":timeform(datetime.datetime.now()),
              "end_time":timeform(datetime.datetime.now()),
              "crc":utility.fileEnstoreChecksum(filename),
              "file_size":os.path.getsize(filename),
              "application":{"family":"reconstructed","name":"ana","version":os.getenv("MINERVA_RELEASE")},
              "data_tier":data_tier,
              "event_count":-1}
              
  if DEBUG:
    print externals
  test = maker.checkmerge(inputfiles)
  if test:
    meta = maker.concatenate(inputfiles,externals)
  f = open(filename+".json",'w')
  
  




