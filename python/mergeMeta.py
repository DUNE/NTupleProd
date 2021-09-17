########## metadata helper ##############

# this provides a meta data merger class which given a defined list of external information about a file and a list of input files, will produce metadata for the output.

import os,sys,time,datetime
import samweb_client
from samweb_client import utility
import json
import argparse

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
    self.externals = ["file_name", "file_format", "start_time", "end_time", "file_size"]
    self.consistent = ["file_type", "file_format", "data_tier", "group", "data_stream" ]
    self.ignore = ["checksum", "create_date", "Offline.options", "first_event", 
                   "parents", 
                   "Offline.machine", 
                   "last_event", "RawDigits.optionspath", "Online.triggertype"]
    self.input_filenames = []
    self.is_sorted = False

############################################
## Function that forms merged metadata
## for a list of files
############################################


  def checkmerge(self, file_list):
    
  
    checks = {}
    for tag in self.consistent:
      checks[tag] = []
    fail = True
  
    for f in file_list:
      filename = os.path.basename(f)
      if not os.path.exists(f):
        print (" can't find file", f, "quitting")
        break
      if DEBUG:
        print (" looking at: ", filename)
   
      metafile = open(f, 'r')
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
      if self.opts["DEBUG"]:
        print (" looking at: ", filename)
        
      metafile = open(file,'r')
      thismeta = json.load(metafile)
      print (thismeta)
      if self.opts["DEBUG"]:
        dumpList(thismeta)
            
      for tag in thismeta:
          if self.opts["DEBUG"]:
              print (" check tag ", tag)
          if tag not in self.consistent and tag not in self.externals and tag not in mix:
              if self.opts["DEBUG"]:
                  print (" found a new parameter to worry about", tag)
              mix[tag]=[thismeta[tag]]
      if self.opts["DEBUG"]:
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
                  if self.opts["DEBUG"]:
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
      if self.opts["DEBUG"]:
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
        
 
    if(self.opts["DEBUG"]):

        print ("-------------------\n")
        dumpList(newJsonData)
    try:
        self.samweb.validateFileMetadata(newJsonData)
    except Exception:
        print (" metadata validation failed - write it out anyways")
    return newJsonData


  ## Update internal list of files
  ## Flip sort flag to tell the class later
  def add_files(self, files):
    self.input_filenames += files
    self.is_sorted = False
  
  ## Sort the files
  def sort_files(self):
    self.input_filenames.sort()
    self.is_sorted = True

  def write_metadata(self, merged_file, meta):
    with open(merged_file + '.json', 'w') as f:
      json.dump(meta, f, indent=2, separators=(',', ': '))
    

if __name__ == "__main__":
  
  parser = argparse.ArgumentParser(description='Test merge meta')
  parser.add_argument('--files', help="List of files", nargs='+', default = [])
  parser.add_argument('-m', help='Name of merged file', type = str,
                      default = 'new.root')
  parser.add_argument('--debug', help = 'Turn on/off debug', type = int, default = 0)
  args = parser.parse_args()

  opts = {
    "DEBUG": bool(args.debug)
  }
  
  filename = args.m
  
  maker = mergeMeta(opts)
  #maker.add_files(args.files)
  #maker.sort_files()

  
  inputfiles = args.files
  inputfiles.sort()
    
  externals = {"file_name": filename,
              "file_format": "root",
              "start_time": timeform(datetime.datetime.now()),
              "end_time": timeform(datetime.datetime.now()),
              "file_size": os.path.getsize(filename),
              "event_count": -1
  }
              
  if args.debug:
    print (externals)
  test = maker.checkmerge(inputfiles)
  if test:
    meta = maker.concatenate(inputfiles, externals)
    print(meta)

  #Write file
  #f = open(filename+".json",'w')
  #json.dump(meta,f, indent=2,separators=(',',': '))
  maker.write_metadata(filename, meta)
  
