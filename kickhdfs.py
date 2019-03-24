#!/usr/bin/env python
# coding = utf-8

import subprocess
import argparse
import os
import json

'''
the default config
{
    "default" : "---------"
}
'''

'''read the hdfs config from file'''
def readHDFSConfig():
    configPath = os.path.expanduser('~') + os.sep + '.kickhdfs' +  os.sep + 'config.json'

    with open(configPath, 'r') as f:
        config = json.loads(f.read())
        f.close()

        return config

def runHDFSCommand(command):
    subprocess.call(command, shell=True)

def parseGetCommand(hdfsUrl, path):
    if hdfsUrl is None or path is None:
        raise Exception('the HDFS url or get file path can not be None')

    command = 'hdfs dfs -get {0}{1}'.format(hdfsUrl, path)

    runHDFSCommand(command)

def parseRmCommand(hdfsUrl, path):
    if hdfsUrl is None or path is None:
        raise Exception('the HDFS url or delete file path can not be None')

    command = 'hdfs dfs -rm {0}{1}'.format(hdfsUrl, path)

    runHDFSCommand(command)

def parseRmrCommand(hdfsUrl, path):
    if hdfsUrl is None or path is None:
        raise Exception('the HDFS url or delete file path can not be None')

    command = 'hdfs dfs -rmr {0}{1}'.format(hdfsUrl, path)

    runHDFSCommand(command)

def parseLsCommand(hdfsUrl, path):
    if hdfsUrl is None or path is None:
        raise Exception('the HDFS url or LL path can not be None')

    command = 'hdfs dfs -ls {0}{1}'.format(hdfsUrl, path)

    runHDFSCommand(command)

def parsePutCommand(hdfsUrl, source, target):
    if hdfsUrl is None or source is None or target is None:
        raise Exception('the HDFS url, source path, target path can not be None')

    command = 'hdfs dfs -put {0} {1}{2}'.format(source, hdfsUrl, target)

    runHDFSCommand(command)

def callHDFS():
    hdfsConfig = readHDFSConfig()

    if hdfsConfig is None or type(hdfsConfig).__name__ != 'dict':
        raise Exception('the config is empty you must config the HDFS alis in ~/.kickhdfs/config.json')


    parser = argparse.ArgumentParser()

    parser.add_argument('-hdfs',
                        '--hdfs',
                        nargs=1,
                        default='default',
                        required=False,
                        help='the HDFS url alis, if not set it will use the default HDFS site, you can set it in file: ~/.kickhdfs/config.json')

    parser.add_argument('-g',
                        '--get',
                        nargs=1,
                        required=False,
                        help='get file from the HDFS')

    parser.add_argument('-r',
                        '--rm',
                        nargs=1,
                        required=False,
                        help='remove the file from the HDFS')

    parser.add_argument('-rr',
                        '--rmr',
                        nargs=1,
                        required=False,
                        help='remove the file from the HDFS recursive')

    parser.add_argument('-l',
                        '--ls',
                        '--ll',
                        nargs=1,
                        required=False,
                        help='show the file in HDFS')

    parser.add_argument('-p',
                        '--put',
                        nargs=2,
                        required=False,
                        help='put the file to HDFS')

    args = parser.parse_args()

    if args.hdfs is None:
        raise Exception('the --HDFS site must be set')

    if args.hdfs not in hdfsConfig:
        raise Exception('the --HDFS site can not find in ~/.kickhdfs/config.json, you must set a HDFS site alias in this file')

    hdfsUrl = hdfsConfig[args.hdfs]

    '''get command'''
    if args.get is not None:
        parseGetCommand(hdfsUrl, args.get[0])
    elif args.rm is not None:
        parseRmCommand(hdfsUrl, args.rm[0])
    elif args.rmr is not None:
        parseRmrCommand(hdfsUrl, args.rmr[0])
    elif args.ls is not None:
        parseLsCommand(hdfsUrl, args.ls[0])
    elif args.put is not None:
        parsePutCommand(hdfsUrl, args.put[0], args.put[1])
    else:
        raise Exception('one of -g/--get, -r/--rm, -l/--ls, -p/--put must be set, use -h to get help')

if '__main__' == __name__:
    callHDFS()