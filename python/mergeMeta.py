########## metadata helper ##############

# this provides a meta data merger class which given a defined list of external information about a file and a list of input files, will produce metadata for the output.

# H Schellman, Sept. 13, 2021

import os,sys,time,datetime
import samweb_client
from samweb_client import utility
import json
import argparse
from statistics import mean

samweb = samweb_client.SAMWebClient(experiment='dune')


#-------utilities ------#

def dumpList(the_list):
    for item in the_list:
      print (item, the_list[item])


def timeform(now):
  timeFormat = "%Y-%m-%d_%H:%M:%S"
  nowtime = now.strftime(timeFormat)
  nowTstamp= time.strptime(nowtime,timeFormat)
  return int(time.mktime(nowTstamp))

 


class mergeMeta():
  #""" Base class for making metadata for a file based on parents"""
  def __init__(self, opts):
    
    self.opts = opts #this is a dictionary containing the option=>value pairs given at the command line
    self.samweb = samweb_client.SAMWebClient(experiment='dune')
    self.externals = ["file_name","start_time","end_time","file_size"]
    #self.consistent = ["file_type","file_format","data_tier","group","data_stream" ]
    self.consistent = ["file_type","file_format","data_tier","group", 'application']
    self.ignore = ["checksum","create_date","Offline.options","first_event","parents","Offline.machine","last_event"]
    self.source = "samweb" # alternative = local
    self.special = ['info.wallsec', 'info.memory', 'info.cpusec', 'DUNE.fcl_name']
    self.debug = False

############################################
## Function that forms merged metadata
## for a list of files
############################################
  def checkmerge(self, the_list):
    checks = {}
    for tag in self.consistent:
      checks[tag] = []
    fail = True
  
    ##Look through the file list and get the metadata for each
    a = 0
    for f in the_list:
      if not a%100: print('%i/%i'%(a, len(the_list)), end='\r')
      a += 1
      filename = os.path.basename(f)
      if self.source == "local":
        if not os.path.exists(f):
          #print(" can't find file", f, "quitting")
          break
        if self.debug:
          print(" looking at: ", filename)
     
        with open(f, 'r') as metafile:
          thismeta = json.load(metafile)
      else:
        thismeta = samweb.getMetadata(filename)
      #print(thismeta)
      if self.debug:
        dumpList(thismeta)
      
      # here to find the must not mix ones
      # Loop over the tags we've defined as consistent
      # and check that it's in the file metadata
      for tag in self.consistent:
        if tag not in thismeta:
            checks[tag].append("missing")  # if it ain't there, it aint there.
        ## Then, add the value from this metadata field to the check list 
        if thismeta[tag] not in checks[tag]:
          checks[tag].append(thismeta[tag])
      
    fail = False
    if fail:
      return False
      
    #Checks that all files have the same value for this field
    for tag in self.consistent:
      if (len(checks[tag]) != 1):
        #print ("tag ", tag, " has problem ",checks[tag])
        return False
    
    return True

  
  def concatenate(self, the_list, externals, user=''):
    # here are things that are unique to the output and must be supplied externally
    for tag in self.externals:
      if not tag in externals:
        print ("must supply", tag, "before we can merge")
        sys.exit(2)
    newmeta = externals       
    firstevent = 999999999999
    lastevent = -999
    runlist = []
    eventcount = 0
    parentage = []
    if(len(the_list) < 1):
      return []
   
    starttime = ""
    endtime = ""

    # here are things that are internals and must be consistent
    checks = {}
    mix = {}
    other = {}
    for tag in self.consistent:
      checks[tag] = []
 
    # loop over files in the list
    a = 0
    special_md = {}
    for f in the_list:
      if not a%100: print('%i/%i'%(a, len(the_list)), end='\r')
      #print('%i/%i'%(a, len(the_list)))
      a += 1
      filename = os.path.basename(f)
      #get the metadata for each file
      if self.source == "local":
        if not os.path.exists(f):
          print(" can't find file", f, "quitting")
          break
        if self.debug:
          print(" looking at:", filename)
          
        with open(f, 'r') as metafile:
          thismeta = json.load(metafile)
      else:
        thismeta = samweb.getMetadata(filename)
      #print (thismeta)
      if self.debug:
        dumpList(thismeta)
            
      #Loop over tags in the metadata
      for tag in thismeta:
        if self.debug:
          print (" check tag ", tag)
          ##Check if it's a new field
        if (tag not in self.consistent and
            tag not in self.externals and tag not in mix):
          if self.debug:
            print (" found a new parameter to worry about", tag)
          if tag in self.special:
            #print('special', tag)
            self.getSpecialMD(tag, thismeta[tag], special_md)
          else:
            mix[tag]=[thismeta[tag]]
      if self.debug:
        dumpList(thismeta)
      #print ("meta is", thismeta)
      #print ("mix is", mix)

      #Loop over the tags that must be consistent
      #and add the fields to the checklist
      for tag in self.consistent:
        if thismeta[tag] not in checks[tag]:
          checks[tag].append(thismeta[tag])

      #See how many mixed fields are here
      for tag in mix:
        if tag in thismeta:
          if thismeta[tag] not in mix[tag]:
            mix[tag].append(thismeta[tag])
            if self.debug:
              print ("tag",tag," has", len(mix[tag]), "mixes")

      #Get the first and last events and the count
      try:
        if thismeta["first_event"] <= firstevent:
          firstevent = thismeta["first_event"]
        if thismeta["last_event"] >= lastevent:
          lastevent = thismeta["last_event"]
        eventcount = eventcount + thismeta["event_count"]
      except:
        print ("something in event count, firstevent, lastevent is missing")
      # is this already in the runlist
      runlist = runlist + thismeta["runs"]
      if self.debug:
        print (thismeta["runs"], runlist)
      # Get the list of parents
      parentage += thismeta["parents"]
      
    
    #Start building the new metadata 
    newJsonData={}

    #For the must-be-consistent tags,
    #first check that things are not getting mixed up,
    #these are things that should carry through.
    for tag in self.consistent:
      if(len(checks[tag]) != 1):
        print ("tag ", tag, " has problem ",checks[tag])
        sys.exit(1)
      else:
        newJsonData[tag] = checks[tag][0]

    for tag in mix:
      #ignore certain ones
      if tag in self.ignore or tag == 'runs' or tag == 'event_count':
        continue
      if len(mix[tag]) == 1:
        newJsonData[tag] = mix[tag][0]
      elif len(mix[tag]) > 1:
        print ("don't write out mixed tags", tag)
        #print (mix[tag])

    self.finishSpecialMD(special_md)

    # overwrite with the externals if they are there
    for tag in externals:
      newJsonData[tag] = externals[tag]
    for tag in special_md:
      newJsonData[tag] = special_md[tag]
  
    #if no event count was provided from externals, use the input files
    if("event_count" not in newJsonData or newJsonData["event_count"] == -1):
      newJsonData["event_count"] = eventcount
          
    # set these from the parents
    if(firstevent!=-1 and lastevent !=-1):
      newJsonData["first_event"] = firstevent
      newJsonData["last_event"] = lastevent
      newJsonData["runs"] = runlist
      newJsonData["parents"] = parentage

    #events/lumblock info is missing
    else:
        newJsonData["first_event"] = firstevent
        newJsonData["last_event"] = lastevent
        newJsonData["event_count"] = eventcount
        newJsonData["runs"] = runlist
        newJsonData["parents"] = parentage

    if newJsonData["data_stream"] == "mc":
        newJsonData["first_event"] = firstevent
        newJsonData["last_event"] = lastevent
        newJsonData["event_count"] = eventcount
        newJsonData["runs"] = runlist
        newJsonData["parents"] = parentage
        
 
    if(self.debug):
      print ("-------------------\n")
      dumpList(newJsonData)
    if user != '': newJsonData['user'] = user
    self.samweb.validateFileMetadata(newJsonData)
    #try:
    #  self.samweb.validateFileMetadata(newJsonData)
    #except Exception:
    #  print (" metadata validation failed - write it out anyways")
    return newJsonData
   
  def setDebug(self, debug=True):
    self.debug = debug 
  def setSourceLocal(self):
    self.source = "local"
  def setSourceSamweb(self):
    self.source = "samweb"


  ##Method to grab some info parents
  def fillInFromParents(self, json_filename, new_json_filename):
    meta_file = open(json_filename, 'r')
    this_meta = json.load(meta_file)
    parents = [i['file_name'] for i in this_meta['parents']]
    parent_metas = [samweb.getMetadata(f) for f in parents]


    ##skip these fields
    skip = ['file_id', 'create_date', 'user', 'file_size', 'checksum',
            'content_status', 'file_type', 'file_format', 'group', 'data_tier',
            'application', 'event_count', 'first_event', 'last_event',
            'start_time', 'end_time', 'art.file_format_era',
            'art.file_format_version', 'art.first_event', 'art.last_event',
            'art.process_name', 'DUNE.requestid', 'runs',
            'parents', 'file_name', 'data_stream']

    all_fields = {}
    for pm in parent_metas:
      for t in pm:
        if t in skip: continue 
        if t not in all_fields: all_fields[t] = []
        all_fields[t] += [pm[t]]
    new_meta = {}
    for t, l in all_fields.items():
      #print(t, l)

      if len(set(l)) > 1: print("ERROR")
      else: new_meta[t] = l[0]

    filled_meta = this_meta
    for t, m in new_meta.items():
      if t in filled_meta:
        print("ERROR")
        break
      filled_meta[t] = m 

    ##Patch the run type and data_stream
    print("Patching run type")
    new_runs = []
    for r in filled_meta['runs']:
      #print(r)
      new_runs.append(r)
      new_runs[-1][2] = 'protodune-sp' 
    filled_meta['runs'] = new_runs

    filled_meta['data_stream'] = 'physics'

    with open(new_json_filename , 'w') as f:
      json.dump(filled_meta, f, indent=2, separators=(',', ': '))

  def getSpecialMD(self, tag, val, special_md):
    if tag in ['info.wallsec', 'info.cpusec']:
      if tag not in special_md.keys():
        special_md[tag] = 0.
      special_md[tag] += val
    elif tag == 'info.memory':
      if tag not in special_md.keys():
        special_md[tag] = []
      special_md[tag].append(val)
    elif tag == 'DUNE.fcl_name':
      special_md[tag] = val.split('/')[-1]

  def finishSpecialMD(self, special_md):
    if 'info.memory' in special_md.keys():
      special_md['info.memory'] = mean(special_md['info.memory'])

def run_merge(filename, jsonlist, merge_type, do_sort=0, user=''):
  opts = {}
  maker = mergeMeta(opts)
  if merge_type == 'local':
    maker.setSourceLocal()
  elif merge_type == 'samweb':
    maker.setSourceSamweb()
  else:
    print('error: mergeMeta -t provided is not local or samweb', merge_type)
    return 1

  inputfiles = jsonlist 
  #print (inputfiles)

  if (do_sort != 0):
    inputfiles.sort()
    
  #app_info = {
  #  "family": "protoduneana",
  #  "name": "pdspana",
  #  "version": os.getenv("PROTODUNEANA_VERSION")
  #} 
  externals = {"file_name": filename,
               #"application": app_info,
               "data_tier": "root-tuple",
               "file_size": os.path.getsize(filename),
               "data_stream": "physics",
               "file_format": "root",
               "start_time": timeform(datetime.datetime.now()),
               "end_time": timeform(datetime.datetime.now()),
               "event_count": -1,
              }
              
  DEBUG = 0
  if DEBUG:
    print (externals)
  #test = maker.checkmerge(inputfiles)
  #print ("merge status",test)
  #if test:
  print ("concatenate")
  meta = maker.concatenate(inputfiles,externals, user=user)
  print ("done")
  #print(meta)

  f = open(filename+".json",'w')
  json.dump(meta,f, indent=2,separators=(',',': '))

  return 0
 

if __name__ == "__main__":
  
  parser = argparse.ArgumentParser(description='Merge Meta')
  parser.add_argument("-f", type=str, help="Name of merged root file", default="new.root")
  parser.add_argument('-j', help='List of json files', nargs='+', default=[])
  parser.add_argument('-s', help='Do Sort?', default=1, type=int)
  parser.add_argument('-t', help='local or samweb', type=str, default='samweb')
  parser.add_argument('-u', help='Patch user to specified. Leave empty to not patch', type=str, default='')
  args = parser.parse_args()

  run_merge(filename=args.f, jsonlist=args.j, do_sort=args.s, merge_type=args.t, user=args.u)
