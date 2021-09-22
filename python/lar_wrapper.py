import subprocess
import argparse

parser = argparse.ArgumentParser(description = 'Wrapper around lar')
parser.add_argument('--sam-web-uri', type=str, help = 'Samweb Project URL', required=True)
parser.add_argument('--sam-process-id', type=str, help = 'Consumer ID', required=True)
parser.add_argument('--sam-application-family', type=str, help = 'App Family', required=True)
parser.add_argument('--sam-application-version', type=str, help = 'App Version', required=True)
parser.add_argument('-c', type=str, help = 'FCL file', required=True)
parser.add_argument('-n', type=int, help = 'N events', default=10)

args = parser.parse_args()

lar_cmd =  ["lar", "-c%s"%args.c, "-n%i"%args.n]
lar_cmd += ["--sam-web-uri=%s"%args.sam_web_uri]
lar_cmd += ["--sam-process-id=%s"%args.sam_process_id]
lar_cmd += ["--sam-application-family=%s"%args.sam_application_family]
lar_cmd += ["--sam-application-version=%s"%args.sam_application_version]

#print(lar_cmd)

proc = subprocess.run(lar_cmd)
status = proc.returncode
print("Status:", status)
exit(status)
