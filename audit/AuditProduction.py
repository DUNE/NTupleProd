# adapted from MINERvA sam audit script

# arguments are
# type, campaign, dataset

import os,time,sys,datetime, glob, fnmatch,string,subprocess, json

import samweb_client
samweb = samweb_client.SAMWebClient(experiment='dune')


#############################################
## Main Function
#############################################
if len(sys.argv)<4:
  print (" args are <data/mc> <campaign> <dataset>")
  sys.exit(1)
datatype = sys.argv[1]
campaign = sys.argv[2]
scampaign = campaign.replace("XX","%")
dataset = sys.argv[3]

DEBUG = False

# this can be extended for MC production by adding "mc" in types and putting in the right stuff
types = {}
types["data"] = ["raw","reco"]
types["mc"] = ["raw","reco"]
tiers = {}

# these give shorter names for the tables

tiers["data"]= {"raw":"raw","reco":"full-reconstructed","detsim":"detsim"}
tiers["mc"]= {"reco":"full-reconstructed","raw":"detector-simulated"}


datatype = sys.argv[1]
if datatype not in ["data","mc"]:
  print (" need data or mc ")
  sys.exit(1)

# runrange can actually be a single number

outname = "audit_%s_%s.txt"%(campaign,dataset)
outfile = open(outname,'w')
#runrange = sys.argv[2]
#runs = runrange.split("-")
#runmin = 0
#runmax = 0
#if len(runs)==1:
#  runmin = int(runs)
#  runmax = int(runs)
#else:
#  runmin = int(runs[0])
#  runmax = int(runs[1])
#

runs = []
count = 0
files = samweb.listFiles("defname:%s"%dataset)
for f in files:
  if DEBUG and count > 10:
    break
  md = samweb.getMetadata(f)
  #print (md)
  fruns = md["runs"]
  for r in fruns:
    therun = r[0]
    if therun not in runs:
      runs.append(therun)
      count +=1
      if DEBUG:
        print ("found a run",therun)
      

print (runs)
print ("audit as of ",datetime.datetime.now())
count = 0
#print "Running over these sets", runsets
tots = {}
tots["raw"] = 0.0
tots["reco"] = 0.0

type = "data"

runs.sort()
for myrun in runs:
    sizeper = {}
    answer = {}
    tapeanswer = {}
    for type in types[datatype]:
      
        mycampaign = campaign
        quality = " "
        # make a sam query - can add quality cuts later
#        tapequery =  " run_number %6s and run_type protodune-sp and data_tier %s and data_stream in (physics) "%(myrun,tiers[datatype][type])
        if type == "data":
          tapequery =  " run_number %s and run_type protodune-sp and data_tier %s and data_quality.online_good_run_list 1 and data_stream physics "%(myrun,tiers[datatype][type])
        else:
          tapequery =  " run_number %s and run_type protodune-sp and data_tier %s "%(myrun,tiers[datatype][type])
        if tiers[datatype][type] != "raw":
          tapequery += " and DUNE.campaign "+scampaign
        
        # this checks to see if the file has a location or not
        
        query = tapequery+" and availability:anylocation "
        if DEBUG:
          print ("query = ",query)
        
        # just count the files...
        fileinfo = len(samweb.listFiles(query))
        tapeinfo = len(samweb.listFiles(tapequery))
       
        summary = samweb.listFilesSummary(query)
        if DEBUG:
          print (summary)
          #print summary
        if summary["total_event_count"] != None:
          nevents = summary["total_event_count"]
          size = summary["total_file_size"]
        else:
          nevents = 0
          size = 0
          
    
        sizeper[type] = 0
        if nevents > 0:
          sizeper[type] = size/nevents/1000000.
        answer[type] = nevents
        tapeanswer[type] = tapeinfo
  
    status = ""
    tapestatus = []
    for key in types[datatype]:
        if tapeanswer[key] != answer[key]:
            tapestatus.append(key)
    answerline = ""
    for key in types[datatype]:
      
        answerline += "%s %6s\t" % (key,answer[key])
    if answer["reco"] == None or answer["reco"] == "None":
       answer["reco"] = 0
    if answer["reco"] != None and answer["raw"] > 0:
    
      percent = float(answer["reco"])/float(answer["raw"]/100.)
      formatted = "%% %5.2f\t size per event %5.2fM"%(percent,sizeper["raw"])
      out = "%s\t%s\t%s"% (myrun,answerline, formatted)
      print (out)
      outfile.write(out+"\n")
      tots["raw"]=tots["raw"]+float(answer["raw"])
      tots["reco"]+=float(answer["reco"])
out =  "%d\t%d\t%f" %(tots["raw"], tots["reco"], tots["reco"]/tots["raw"])
print (out)
outfile.write(out+"\n")
