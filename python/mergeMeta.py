########## metadata helper ##############

# this provides a meta data merger class which given a defined list of external information about a file and a list of input files, will produce metadata for the output.

import os,sys,time,datetime
import samweb_client
from samweb_client import utility
import json

samweb = samweb_client.SAMWebClient(experiment='dune')
DEBUG=True


#-------utilities ------#

def dumpList(list):
    for item in list:
      print (item, list[item])


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
    self.externals = ["file_name","file_format","start_time","end_time","file_size"]
    self.consistent = ["file_type","file_format","data_tier","group","data_stream" ]
    self.ignore = ["checksum","create_date","Offline.options","first_event",
"parents",
"Offline.machine",
"last_event","RawDigits.optionspath","Online.triggertype"]



############################################
## Function that forms merged metadata
## for a list of files
############################################


  def checkmerge(self,list):
    
  
    checks = {}
    for tag in self.consistent:
      checks[tag] = []
    fail = True
  
    for file in list:
      filename = os.path.basename(file)
      if not os.path.exists(file):
        print (" can't find file",file,"quitting")
        break
      if DEBUG:
        print (" looking at: ", filename)
   
      metafile = open(file,'r')
      thismeta = json.load(metafile)
      print (thismeta)
      if DEBUG:
        dumpList(thismeta)
      
      # here to find the must not mix ones
      for tag in self.consistent:
        if tag not in thismeta:
            checks[tag].append("missing")  # if it ain't there, it aint there.
        if thismeta[tag] not in checks[tag]:
          checks[tag].append(thismeta[tag])
      
    fail = False
      
  
    if fail:
      return False
      
    for tag in self.consistent:
            if(len(checks[tag]) != 1):
              print ("tag ", tag, " has problem ",checks[tag])
              return False
    
    return True

  
  def concatenate(self,list,externals):

    # here are things that are unique to the output and must be supplied externally
    
    for tag in self.externals:
        if not tag in externals:
            print ("must supply ",tag," before we can merge")
            sys.exit(2)
    newmeta = externals       
    firstevent = 999999999999
    lastevent = -999
    runlist = []
    eventcount=0
    parentage = []
    if(len(list) < 1):
        return []
   
    starttime = ""
    endtime=""

    # here are things that are internals and must be consistent
    
    checks = {}
    mix = {}
    other = {}
    for tag in self.consistent:
        checks[tag] = []
    
 
    for file in list:
      filename = os.path.basename(file)
      if not os.path.exists(file):
          print (" can't find file",file,"quitting")
          break
      if DEBUG:
        print (" looking at: ", filename)
        
      metafile = open(file,'r')
      thismeta = json.load(metafile)
      print (thismeta)
      if DEBUG:
        dumpList(thismeta)
            
      for tag in thismeta:
          if DEBUG:
              print (" check tag ", tag)
          if tag not in self.consistent and tag not in self.externals and tag not in mix:
              if DEBUG:
                  print (" found a new parameter to worry about", tag)
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
                      print ("tag",tag," has", len(mix[tag]), "mixes")
      # get info from the parent files

      try:
          if thismeta["first_event"] <= firstevent:
              firstevent = thismeta["first_event"]
          if thismeta["last_event"] >= lastevent:
              lastevent = thismeta["last_event"]
          eventcount = eventcount + thismeta["event_count"]
      except:
          print ("something in event count, firstevent, lastevent is missing")
      # is this already in the runlist
      runlist =runlist + thismeta["runs"]
      if DEBUG:
          print (thismeta["runs"], runlist)
      parentage += thismeta["parents"]
      
    
    newJsonData={}
    # full metadata is available

    # first check that things are not getting mixed up, these are things that should carry through.

    for tag in self.consistent:
        if(len(checks[tag]) != 1):
            print ("tag ", tag, " has problem ",checks[tag])
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
            print ("don't write out mixed tags ",tag)
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
        
 
    if(DEBUG):

        print ("-------------------\n")
        dumpList(newJsonData)
    try:
        self.samweb.validateFileMetadata(newJsonData)
    except Exception:
        print (" metadata validation failed - write it out anyways")
    return newJsonData




if __name__ == "__main__":
  
  opts = {}
  
  filename = "new.root"
  
  
  maker = mergeMeta(opts)
  
  inputfiles=[
    "protoduneana_PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1_2021-09-11-1857.18_16723900_fixed.json",
    "protoduneana_PDSPProd4a_MC_1GeV_reco1_sce_datadriven_v1_2021-09-11-1857.18_16723932_fixed.json"]


  inputfiles.sort()
    
  externals = {"file_name":filename,
              "file_format":"root",
              "start_time":timeform(datetime.datetime.now()),
              "end_time":timeform(datetime.datetime.now()),
              "file_size":os.path.getsize(filename),
              "event_count":-1
  }
              
  if DEBUG:
    print (externals)
  test = maker.checkmerge(inputfiles)
  if test:
    meta = maker.concatenate(inputfiles,externals)
    print(meta)
  f = open(filename+".json",'w')
  json.dump(meta,f, indent=2,separators=(',',': '))
  
  




