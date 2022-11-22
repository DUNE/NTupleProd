#!/bin/env python3
import samweb_client as swc
import argparse

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'Wrapper around check')
  parser.add_argument('-d', '-datasets', type=str, nargs='+',
                      help='Parent dataset(s)')
  parser.add_argument('-u', '--user', type=str, default='calcuttj')
  parser.add_argument('-o', type=str, required=True, help='Output string')
  args = parser.parse_args()

  samweb = swc.SAMWebClient(experiment='dune')

  print(args)
  all_datasets = 'defname:' + ' or defname:'.join(args.d)
  print(all_datasets)
  query = f'data_tier root-tuple-virtual and user {args.user} and ischildof:({all_datasets})'
  print(query)

  files = samweb.listFiles(dimensions=query)
  print(f'Got {len(files)} files')


  files = [f+'\n' for f in files]
  with open(args.o, 'w') as f:
    f.writelines(files)
