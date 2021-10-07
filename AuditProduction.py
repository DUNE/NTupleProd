# adapted from MINERvA sam audit script

# arguments are

# python AuditProduction.py <data/mc> <runmin-runmax> [version]

import os,time,sys,datetime, glob, fnmatch,string,subprocess, json

import samweb_client
samweb = samweb_client.SAMWebClient(experiment='dune')


#############################################
## Main Function
#############################################


DEBUG = True

# this can be extended for MC production by adding "mc" in types and putting in the right stuff
types = {}
types["data"] = ["raw","reco"]
tiers = {}

# these give shorter names for the tables

tiers["data"]= {"raw":"raw","reco":"full-reconstructed"}
#{"decoded":"decoded-raw","hits":"hit-reconstructed","raw":"raw","full":"full-reconstructed"}

# you can specify a code version

version = "v08_27_XX"
sversion = "v08_27_%"
datatype = "data"

#if len(sys.argv) < 2:
#    print " need to provide <mc/data> <run-range=a-b> [version] "
#    sys.exit(1)

#datatype = sys.argv[1]
#if datatype not in ["data","mc"]:
#  print " need data or mc "
#  sys.exit(1)

# runrange can actually be a single number

outname = "audit_PDPSProd2.txt"
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

    
if len(sys.argv) > 3:
    version = sys.argv[3]

# holdover from minerva - sorry

runsets=samweb.listDefinitions()

print "audit as of ",datetime.datetime.now()
count = 0
#print "Running over these sets", runsets
tots = {}
tots["raw"] = 0.0
tots["reco"] = 0.0

type = "data"
runsets.sort()
for sets in runsets:
  #print sets
    if not( "protodune-sp_runset") in sets:
      continue
    if not "v0" in sets:
      continue
    if not version in sets:
      continue
    myrun = sets.split("_")[2]
#print sets, myrun
    if sets != "protodune-sp_runset_"+myrun+"_reco_"+version+"_v0":
      continue
    sizeper = {}
    answer = {}
    tapeanswer = {}
    sum = {}
    if count%10 == 0:
        out =  "-------------------------------------------------"
        print out
        outfile.write(out+"\n")
    count +=1
    for type in types[datatype]:
      
        myversion = version
        quality = " "
        # make a sam query - can add quality cuts later
#        tapequery =  " run_number %6s and run_type protodune-sp and data_tier %s and data_stream in (physics) "%(myrun,tiers[datatype][type])
        tapequery =  " run_number %s and run_type protodune-sp and data_tier %s and data_quality.online_good_run_list 1 and data_stream physics "%(myrun,tiers[datatype][type])
        if tiers[datatype][type] != "raw":
          tapequery += "and version "+sversion
        
        # this checks to see if the file has a location or not
        
        query = tapequery+" and availability:anylocation "

        
        # just count the files...
        fileinfo = len(samweb.listFiles(query))
        tapeinfo = len(samweb.listFiles(tapequery))
       
        summary = samweb.listFilesSummary(query)
          #print summary
        nevents = summary["total_event_count"]
        size = summary["total_file_size"]
    
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
      print out
      outfile.write(out+"\n")
      tots["raw"]=tots["raw"]+float(answer["raw"])
      tots["reco"]+=float(answer["reco"])
out =  "%d\t%d\t%f" %(tots["raw"], tots["reco"], tots["reco"]/tots["raw"])
print out
outfile.write(out+"\n")
