"""! @brief Art logfile parser """
##
# @mainpage Loginator.py
#
# @section description_main Description
# A program for parsing art logs to put information into DUNE job monitoring.
#
#
# Copyright (c) 2022 Heidi Schellman, Oregon State University
##
# @file Loginator.py

import string,time,datetime,json,os,sys
import samweb_client
from metacat.webapi import MetaCatClient
import string,datetime,dateutil
from datetime import date,timezone,datetime
from dateutil import parser


DEBUG=False

class Loginator:
    
    def __init__(self,logname):
        if not os.path.exists(logname):
            print ("no such file exists, quitting",logname)
            sys.exit(1)
        self.logname = logname
        self.logfile = open(logname,'r')
        self.outobject ={}
        self.info = self.getinfo()
        self.tags = ["Opened input file", "Closed input file","Peak resident set size usage (VmHWM)"]
        self.template = {
            "source_rse":None,  #
            "user":None,  # (who'��s request is this)
            "job_id":None, # (jobsubXXX03@fnal.gov)
            "timestamp_for_start":None,  #
            "timestamp_for_end":None,  #
            "duration":None,  # (difference between end and start)
            "file_size":None,  #
            "application_family":None,  #
            "application_name":None,  #
            "application_version":None,  #
            "final_state":None,  # (what happened?)
            "cpu_site":None,  # (e.g. FNAL":None,  # RAL)
            "project_name":None, #(wkf request_id?)"
            "file_name":None,  # (including the metacat namespace)
            "data_tier":None,  # (from metacat)
            "data_stream":None,
            "run_type":None,
            "job_node":None,  # (name within the site)
            "job_site":None,  # (name of the site)
            "country":None,  # (nationality of the site)
            "campaign":None,  # (DUNE campaign)
            "delivery_method":None, #(stream/copy)
            "workflow_method":None,
            "access_method":None, #(samweb/dd)
            "path":None,
            "namespace":None,
            "real_memory":None,
            "project_id":None,
            "delivery_method":None
        }

## return the first tag or None in a line
    def findme(self,line):
        for tag in self.tags:
            if tag in line:
                if DEBUG: print (tag,line)
                return tag
        return None

## get system info for the full job
    def getinfo(self):
        info = {}
        # get a bunch of system thingies.
        info["application_version"]=os.getenv("DUNESW_VERSION")
        info["user"]=os.getenv("GRID_USER")
        info["job_node"] = os.getenv("HOST")
        info["job_site"] = os.getenv("GLIDEIN_DUNESite")
        #info["POMSINFO"] = os.getenv("poms_data")  # need to parse this further
        return info

## read in the log file and parse it, add the info
    def readme(self):
        object = {}
        for line in self.logfile:
            tag = self.findme(line)
            if DEBUG: print (tag,line)
            if tag == None:
                continue
            if "file" in tag:
                data = line.split(tag)
                filefull = data[1].strip().replace('"','')
                timestamp = data[0].strip()
                filename = os.path.basename(filefull).strip()
                filepath = os.path.dirname(filefull).strip()
                if "Opened" in tag and not filename in object.keys():
                    object[filename] = self.template
                    object[filename]["timestamp_for_start"] = timestamp
                    start = timestamp
                    object[filename]["path"]=filepath
                    object[filename]["file_name"] = filename
                    print ("filepath",filepath)
                    if "root" in filepath[0:10]:
                        print ("I am root")
                        tmp = filepath.split("//")
                        object[filename]["source_rse"] = tmp[1]
                        object[filename]["deliver_method"] = "xroot"
                    for thing in self.info:
                        object[filename][thing] = self.info[thing]
                    object[filename]["final_state"] = "Opened"
                if "Closed" in tag:
                    object[filename]["timestamp_for_end"] = timestamp
                    object[filename]["duration"]=self.duration(start,timestamp)
                    object[filename]["final_state"] = "Closed"
                continue
            if "size usage" in tag:
                data = line.split(":")
                for thing in object:
                    object[thing]["real_memory"]=data[1].strip()
        self.outobject=object

    def addinfo(self,info):
        for s in info:
            if s in self.outobject:
                print (" replacing",s, self.outobject[s],self.info[s])
            else:
                for f in self.outobject:
                    self.outobject[f][s] = info[s]
                    print ("adding",s,info[s])

    def addsaminfo(self):
        samweb = samweb_client.SAMWebClient(experiment='dune')
        for f in self.outobject:
            print ("f ",f)
            meta = samweb.getMetadata(f)
            self.outobject[f]["namespace"]="samweb"
            self.outobject[f]["access_method"]="samweb"
            for item in ["data_tier","file_type","data_stream","group","file_size"]:
                self.outobject[f][item]=meta[item]
            for run in meta["runs"]:
                self.outobject[f]["run_type"] = run[2]
                break

    def addmetacatinfo(self,namespace):
        os.environ["METACAT_SERVER_URL"]="https://metacat.fnal.gov:9443/dune_meta_demo/app"
        mc_client = MetaCatClient('https://metacat.fnal.gov:9443/dune_meta_demo/app')
        for f in self.outobject:
            meta = mc_client.get_file(name=f,namespace=namespace)
            print ("metacat answer",f,meta.keys())
            self.outobject[f]["access_method"]="metacat"
            for item in ["data_tier","file_type","data_stream","group","run_type"]:
                if "core."+item in meta["metadata"].keys():
                    self.outobject[f][item]=meta["metadata"]["core."+item]
                else:
                    print ("no", item, "in ",list(meta["metadata"].keys()))
            self.outobject[f]["file_size"]=meta["size"]
            self.outobject[f]["namespace"]=namespace



    def metacatinfo(self,namespace,filename):
        print ("do something here")


    def writeme(self):
        result = []
        for thing in self.outobject:
            outname = thing+".process.json"
            outfile = open(outname,'w')
            json.dump(self.outobject[thing],outfile,indent=4)
            outfile.close()
            result.append(outname)
        return result

    def human2number(self,stamp):
        #15-Nov-2022 17:24:41 CST https://docs.python.org/3/library/time.html#time.strftime
        format = "%d-%b-%Y %H:%M:%S"
        # python no longer accepts time zones.  We only want the different but need to correct for DT
        thetime  = datetime.strptime(stamp[:-4],format)
        epoch = datetime.utcfromtimestamp(0)
        if "DT" in stamp:
            stamp += 3600
        return (thetime-epoch).total_seconds()

    def duration(self,start,end):
        t0 = self.human2number(start)
        t1 = self.human2number(end)
        return t1-t0

def envScraper():
    env = os.environ
    if "apple" in env["CLANGXX"]:
        f = open("bigenv.txt")
        env = {}
        for a in f.readlines():
            line = a.split("=")
            env[line[0]] = line[1]
    digest = {}
    for k in env.keys():
        if "SETUP_" in k:
            it = env[k].split(" ")
            digest[k] = {"Product":it[0],"Version":it[1]}
    return digest


def test():
    parse = Loginator(sys.argv[1])
    print ("looking at",sys.argv[1])
    parse.readme()
    parse.addinfo(parse.getinfo())
   # parse.addsaminfo()
    parse.addmetacatinfo("pdsp_mc_reco")
    parse.writeme()


if __name__ == '__main__':
    test()
