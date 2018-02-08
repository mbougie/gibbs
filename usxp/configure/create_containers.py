from sqlalchemy import create_engine
import numpy as np, sys, os, errno
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc')
import general as gen 
import pandas as pd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import fnmatch
import psycopg2

import pprint
import json


'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



###################  create classes ######################################################




def run(route, instance):

    # create the directories and gdbs for this instance
    instance_directory = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\{}\\{}'.format(route,instance)
    gen.createDirectory(instance_directory)
    
    
    for stage in ['core','post','vectors','plots']:
        subdir = '{}\\{}'.format(instance_directory, stage)
        #create subdir
        gen.createDirectory(subdir)
        
        if stage == 'core' or stage == 'post':
            #create geodatabse
            if stage == 'post':
                arcpy.CreateFileGDB_management(subdir, "{}_{}.gdb".format('ytc',instance))
            else:
                arcpy.CreateFileGDB_management(subdir, "{}_{}.gdb".format(stage,instance))



if __name__ == '__main__':
    run(route, instance)
