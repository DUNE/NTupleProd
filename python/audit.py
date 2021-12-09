#!/bin/env python3

import samweb_client
import argparse
import sys

parser = argparse.ArgumentParser(description = 'Auditing script')
parser.add_argument('-d', '--dataset', type=str, help='Dataset to check',
                    required=True)
parser.add_argument('-v', '--version', type=str, help='protoduneana version',
                    default=None)
parser.add_argument('-u', '--user', type=str, help='User', default=None)
parser.add_argument('--new-dataset', type=str,
                    help='Which dataset was this added to?', default=None)

make_note = "Create a definition with the supplied name."
make_note += "Note: Name must contain 'makeup'"
parser.add_argument('--make', type=str,
                    help='Create a definition with the supplied name',
                    default=None)
args = parser.parse_args()

if args.make:
  if 'makeup' not in args.make:
    print("FATAL ERROR: definition name supplied to --make must contain 'makeup'")
    print("Supplied: %s"%args.make)
    exit()

samweb = samweb_client.SAMWebClient(experiment='dune')

not_parent = "data_tier root-tuple-virtual"
if args.user:
  not_parent += " and user %s"%args.user
if args.new_dataset:
  not_parent += ' and defname:%s'%args.new_dataset

query = "defname:%s and not isparentof:(%s)"%(args.dataset, not_parent)

print(query)

files = samweb.listFiles(query)
print("Got %i files"%len(files))


if args.make:
  print("Supplied definition %s to --make"%args.make)
  do_exit = False

  try:
    a = samweb.descDefinition(args.make)
    #print(a)

    prompt = "%s exists. Do you want to delete this defnition"%args.make
    prompt += " and recreate it with %i files (y/n)?"%len(files)
    val = input(prompt)
    val = val.lower().strip()
    if val == 'y':
      samweb.deleteDefinition(args.make)
      print("Deleted definition %s"%args.make)
    else:
      print("Will not delete definition %s. Exiting"%args.make)
      do_exit = True

  except BaseException:
    print("%s does not exist."%args.make,
          "Will create this definition")

  if do_exit: exit()
  print("Creating definition '%s'"%args.make,
        "From query '%s'"%query)

  samweb.createDefinition(defname=args.make, dims=query)
  print("Created defintion")
