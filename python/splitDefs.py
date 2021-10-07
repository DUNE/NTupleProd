import samweb_client
import os
import argparse
import time
import datetime
from math import ceil

def timeform(now):
  timeFormat = "%Y-%m-%d_%H:%M:%S"
  nowtime = now.strftime(timeFormat)
  nowTstamp= time.strptime(nowtime,timeFormat)
  return int(time.mktime(nowTstamp))

def splitByN(samweb, defname, n, name):
  total_files = samweb.countFiles(defname=defname)
  print('Dataset %s has %d total files'%(defname, total_files))

  n_sets = int(total_files / n)
  if total_files % n > 0: n_sets += 1
  print('Splitting into %d sets with at most %d files'%(n_sets, n))

  for i in range(0, n_sets):

    #If total_files not a multiple of n, last set will be shorter
    this_name = name + '_%d_%d'%(i*n,
                                 ((i+1)*n if i != n_sets-1 else total_files) - 1)
    dimensions = 'defname: %s with limit %d with offset %d'%(defname, n, i*n)
    print(this_name, dimensions)
    samweb.createDefinition(defname=this_name, dims=dimensions)

def splitBySize(samweb, defname, size, name):
  files = samweb.listFiles(defname=defname)

  total_files = len(files)

  print('Finding size of %s with %d files'%(defname, total_files))

  # Get the size of 1000 files to estimate total
  size_of_1000 = 0.
  a = 0
  for f in files:
    if a == 1000: break

    if not a%100: print(a, end='\r')
    size_of_1000 += float(samweb.getMetadata(filenameorid=f)['file_size'])
    a += 1

  print('Size of 1000 files: %f'%size_of_1000)

  #convert to GB
  estimated_size = (size_of_1000*total_files/1000.) * 1.e-9
  print('Estimated size of dataset: %.2f GB'%estimated_size)

  #make sure the dataset is big enough
  if estimated_size < size:
    print('Error: requested chunk size is larger than total definition size.',
          'Exiting')
    exit(1)

  #Get the number of sets to split into
  n_sets = ceil(estimated_size / size)
  print('Splitting into', n_sets)

  n_split = int(total_files / n_sets)
  print(n_split, ' per set')
  #splitByN(samweb, defname=defname, n=n_split, name=name)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Split definition into chunks')
  parser.add_argument("-d", type=str, help="Name of definition", required=True)
  parser.add_argument('--name', type=str,
                      help='Base name of new definitions. Leave blank to use the input def and timestamp',
                      default='')
  parser.add_argument("-n", type=int, help="Max n files per chunk", default=-1)
  parser.add_argument('-s', type=int, help='Size per chunk', default=-1)
  args = parser.parse_args()
  
  ##Form the new name
  new_name = ''
  if args.name == '':
    print('Using input dataset as new definition base')
    new_name = args.d
    if not os.environ['USER'] in args.d:
      print('Adding username to start and appending timestamp')
      new_name = '%s_%s'%(os.environ['USER'], args.d)
  elif os.environ['USER'] not in args.name:
    print('Warning: adding username to new definition base')
    new_name = '%s_%s'%(os.environ['USER'], args.name)
    print('New dataset name: %s'%new_name)
  else:
    new_name = args.name
  new_name += '_' + str(timeform(datetime.datetime.now()))
  print('added timestamp to name: %s'%new_name)


  ## instantiate samweb client
  samweb = samweb_client.SAMWebClient(experiment='dune')

  #Check what type of splitting to do
  if args.s != -1 and args.n != -1:
    print('Must supply either -s or -n')
  
  elif args.n != -1:
    splitByN(samweb, args.d, args.n, new_name)
  else:
    splitBySize(samweb, args.d, args.s, new_name)   
  
