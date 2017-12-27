from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
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
import general as gen 
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



def getSeriesParams(series):
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = """SELECT * FROM series.series FULL JOIN series.core USING(series) LEFT OUTER JOIN series.routes USING(route) FULL JOIN series.post USING(series) WHERE series = '{}'""".format(series)
    print 'query:-getSeries-', query
    df = pd.read_sql_query(query, con=engine)
    # print df
    for index, row in df.iterrows():
        print row
        return row


def getObjectValues(row):
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = "SELECT * FROM series.objects where object = '{}'".format(row)
    print 'query:-getPaths-', query
    df = pd.read_sql_query(query, con=engine)
    # print df
    # return df
    for index, row in df.iterrows():
        # print row
        return row



def getGDBpath(wc):
    for root, dirnames, filenames in os.walk("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\"):
        for dirnames in fnmatch.filter(dirnames, '*{}*.gdb'.format(wc)):
            print dirnames
            gdbmatches = os.path.join(root, dirnames)
    print gdbmatches
    # return json.dumps(gdbmatches)
    return gdbmatches


def insertGDBpaths(subpath, gdb):
    #get gdb list
    gdblist = getGDBpath(subpath)
    gdbstring = ','.join(gdblist)
    print 'sdsd', gdbstring

    #insert list into table
    query = "UPDATE series.objects SET gdb_path='{}' WHERE gdb='{}'".format(gdbstring, gdb);
    print query
    
    gen.commitQuery(query)
    



def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\template.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template



class ProcessingObject(object):

    def __init__(self, kernel):
        
        ##get the template json
        self.data = getJSONfile()
        



        #call the methods in order to modifiy the tempalte json
        self.updateKernel = self.updateKernel(kernel)
        self.updatePreObject = self.updatePreObject(kernel)
        self.updateRefineObject = self.updateRefineObject()
        self.updateCoreObject = self.updateCoreObject(kernel)
        
        #export modified object to json file
        self.exportObject = self.exportObject()
    


    def updateKernel(self, kernel):
        #add kenel object to json 
        self.data['globals']=kernel['globals']

        print 'fdfdfd', self.data
        #add datarange to kernel object
        self.data['globals']['datarange'] = '{}to{}'.format(str(self.data['globals']['years'][0]), str(self.data['globals']['years'][-1]))

 

    def updatePreObject(self, kernel):
        ##define attributes
        self.data['pre']['traj']['version'] = kernel['pre']['version']['traj']
        self.data['pre']['traj']['gdb'] = getGDBpath('{}_traj'.format(self.data['pre']['traj']['version']))
        self.data['pre']['traj']['filename'] = '_'.join(['traj', self.data['pre']['traj']['version'], 'cdl'+self.data['globals']['res'], 'b', self.data['globals']['datarange']])
        self.data['pre']['traj']['lookup'] = '_'.join(['traj', self.data['globals']['datarange'], 'lookup'])

        self.data['pre']['traj_rfnd']['version'] = kernel['pre']['version']['traj_rfnd']
        self.data['pre']['traj_rfnd']['gdb'] = getGDBpath('{}_traj_rfnd'.format(self.data['pre']['traj_rfnd']['version']))
        self.data['pre']['traj_rfnd']['filename'] = '_'.join([self.data['pre']['traj']['filename'],'rfnd',self.data['pre']['traj_rfnd']['version']])




    def updateRefineObject(self):
        ##define attributes
        self.data['refine']['gdb'] = getGDBpath('refine')
        # self.data['pre']['traj']['gdb'] = getGDBpath('{}_traj'.format(self.data['pre']['traj']['version']))
        # self.data['pre']['traj']['filename'] = '_'.join(['traj',self.data['pre']['traj']['version'],'cdl'+self.data['globals']['res'],'b',self.data['globals']['datarange']])

 

    def updateCoreObject(self, kernel):
        ##define attributes
        self.data['core']['gdb'] = getGDBpath('core')
        self.data['core']['filter'] = kernel['core']['filter']
        self.data['core']['route'] = kernel['core']['route']
        self.data['core']['mmu'] = kernel['core']['mmu']
        self.defineRoute = self.defineRoute()

    
    def defineRoute(self):
        self.data['core']['filename'] = self.createFileNames()

                                                
    def createFileNames(self):
        file_dict = {}
        if self.data['core']['route'] == 'r2':
            file_dict['majorityfilter']='_'.join((self.data['globals']['series'],self.data['pre']['traj_rfnd']['filename'],self.data['core']['filter']))
            file_dict['createMTR']='_'.join((file_dict['majorityfilter'],'mtr'))
            file_dict['createMMU']='_'.join((file_dict['createMTR'],'mmu'+self.data['core']['mmu']))
        return file_dict






    def exportObject(self):
        with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json', 'w') as outfile:
            json.dump(self.data, outfile, indent=4)








###########  create instance of class ################################################

pre = ProcessingObject(
    {
        'globals':{
            'series':'s21',
            'res':'30',
            'years':range(2008,2013),
            'years_conv':range(2009,2013)
        },
        'pre':{'version':{'traj':'v3', 'traj_rfnd':'v2'}},
        'refine':{'join_operator':'or', 'dev_mask':'dev122to124', 'nlcd_masks':['2001','2006','2011']},
        'core':{'filter':'n8h', "route":"r2", 'mmu':'5'}
    }
)

