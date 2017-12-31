from sqlalchemy import create_engine
import numpy as np, sys, os
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






def getArbitraryCropValue(table, years, croptype):
    #def this is a sub function for createMask_nlcdtraj()
    #this is to return the arbitrary value assocaited with full crop over entire years span
    cropdict = {'crop':'1', 'noncrop':'0'}

    lc_series = cropdict[croptype] * len(years)
    print croptype, lc_series
    columnList = ','.join(str(e) for e in lc_series)
    print 'columnlist', columnList

    cur = conn.cursor()
   
    cur.execute("SELECT \"Value\" FROM pre."+table+" Where traj_array = '{" + columnList + "}' ")

    # fetch all rows from table
    rows = cur.fetchall()
    print 'arbitrary value', rows[0][0]
    return rows[0][0]



class ProcessingObject(object):

    def __init__(self, kernel):
        
        ##get the template json
        self.data = getJSONfile()
        

        #call the methods in order to modifiy the tempalte json
        self.updateKernel = self.updateKernel(kernel)
        self.updatePreObject = self.updatePreObject(kernel)
        self.updateRefineObject = self.updateRefineObject(kernel)
        self.updateCoreObject = self.updateCoreObject(kernel)
        
        #export modified object to json file
        self.exportObject = self.exportObject()
    


    def updateKernel(self, kernel):
        #add kenel object to json 
        self.data['global']=kernel['global']

        print 'fdfdfd', self.data
        #add datarange to kernel object
        self.data['global']['datarange'] = '{}to{}'.format(str(self.data['global']['years'][0]), str(self.data['global']['years'][-1]))


 



    def updatePreObject(self, kernel):
        ##define attributes
        self.data['pre']['traj']['version'] = kernel['pre']['version']['traj']
        self.data['pre']['traj']['gdb'] = getGDBpath('{}_traj'.format(self.data['pre']['traj']['version']))
        self.data['pre']['traj']['filename'] = '_'.join(['traj', self.data['pre']['traj']['version'], 'cdl'+self.data['global']['res'], 'b', self.data['global']['datarange']])
        self.data['pre']['traj']['lookup'] = 'traj_{}_lookup'.format(self.data['global']['datarange'])
        self.data['pre']['traj']['path']  = '\\'.join([self.data['pre']['traj']['gdb'], self.data['pre']['traj']['filename']]) 

        self.data['pre']['traj_rfnd']['version'] = kernel['pre']['version']['traj_rfnd']
        self.data['pre']['traj_rfnd']['gdb'] = getGDBpath('{}_traj_rfnd'.format(self.data['pre']['traj_rfnd']['version']))
        self.data['pre']['traj_rfnd']['filename'] = '_'.join([self.data['pre']['traj']['filename'],'rfnd', self.data['pre']['traj_rfnd']['version']])
        self.data['pre']['traj_rfnd']['path']  = '\\'.join([self.data['pre']['traj_rfnd']['gdb'], self.data['pre']['traj_rfnd']['filename']])  





    def updateRefineObject(self, kernel):
        ##define attributes
        self.data['refine']['version'] = kernel['refine']['version']
        self.data['refine']['gdb'] = getGDBpath('{}_masks'.format(self.data['refine']['version']))
        

        self.data['refine']['mask_nlcd']['filename'] = '{}_mask_nlcd_{}'.format(self.data['refine']['version'], self.data['global']['datarange'])
        self.data['refine']['mask_nlcd']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_nlcd']['filename']])
        self.data['refine']['mask_nlcd']['arbitrary'] = getArbitraryCropValue(self.data['pre']['traj']['filename'], self.data['global']['years'], 'crop')
        self.data['refine']['mask_nlcd']['years_nlcd'] = kernel['refine']['years_nlcd']
        self.data['refine']['mask_nlcd']['operator'] = kernel['refine']['operator']
        
        
        self.data['refine']['mask_dev_alfalfa_fallow']['filename'] = '{}_mask_dev_alfalfa_fallow_{}'.format(self.data['refine']['version'], self.data['global']['datarange'])
        self.data['refine']['mask_dev_alfalfa_fallow']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_dev_alfalfa_fallow']['filename']])
        self.data['refine']['mask_dev_alfalfa_fallow']['arbitrary'] = getArbitraryCropValue(self.data['pre']['traj']['filename'], self.data['global']['years'], 'noncrop')
        


 
    #####   core functions  ################################################################################
    def updateCoreObject(self, kernel):
        ##define attributes
        self.data['core']['gdb'] = getGDBpath('core')
        self.data['core']['filter'] = kernel['core']['filter']
        self.data['core']['route'] = kernel['core']['route']
        self.data['core']['rg'] = kernel['core']['rg']
        self.data['core']['mmu'] = kernel['core']['mmu']
        self.defineRoute = self.defineRoute()

    
    def defineRoute(self):
        self.data['core']['filename'] = self.createCoreFileNames()
        self.data['core']['path'] = self.createCorePaths()
        self.data['core']['function'] = self.createCoreFunctionArguments()

                                                
    def createCoreFileNames(self):
        file_dict = {}
        if self.data['core']['route'] == 'r2':
            file_dict['filter']='_'.join((self.data['global']['series'], self.data['pre']['traj_rfnd']['filename'], self.data['core']['filter']))
            file_dict['mtr']='_'.join((file_dict['filter'],'mtr'))
            file_dict['rg']='{}_{}_rgmask{}'.format(file_dict['mtr'], self.data['core']['rg'], self.data['core']['mmu'])
            file_dict['mmu']='{}_mmu{}'.format(file_dict['mtr'], self.data['core']['mmu'])
        return file_dict

    def createCorePaths(self):
        path_dict = {}
        if self.data['core']['route'] == 'r2':
            path_dict['filter']='\\'.join([self.data['core']['gdb'], self.data['core']['filename']['filter']])
            path_dict['mtr']='\\'.join([self.data['core']['gdb'], self.data['core']['filename']['mtr']])
            path_dict['mmu']='\\'.join([self.data['core']['gdb'], self.data['core']['filename']['mmu']])
        return path_dict

# s14_traj_cdl30_b_2008to2016_rfnd_n8h_mtr_8w_rgmask5

    def createCoreFunctionArguments(self):
        fct_dict = {}
        if self.data['core']['route'] == 'r2':
            fct_dict['majorityFilter']={'input':self.data['pre']['traj_rfnd']['path'], 'output':self.data['core']['filename']['filter']}
            fct_dict['createMTR']={'input':self.data['core']['filename']['filter'], 'output':self.data['core']['filename']['mtr']}
            fct_dict['parallel_rg']={'input':self.data['core']['filename']['mtr'], 'output':self.data['core']['filename']['rg']}
            fct_dict['createMMU']={'input':self.data['core']['filename']['mtr'], 'output':self.data['core']['filename']['mmu']}
        return fct_dict

    #####   core functions end  ################################################################################




    def exportObject(self):
        with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\test\\series_test4.json', 'w') as outfile:
            json.dump(self.data, outfile, indent=4)




###########  create instance of class ################################################

pre = ProcessingObject(
    {
        'global':{
            'series':'s15',
            'res':'30',
            'years':range(2008,2013),
            'years_conv':range(2009,2013)
        },
        'pre':{'version':{'traj':'v3', 'traj_rfnd':'v2'}},
        'refine':{'version':'v2', 'operator':'or', 'years_nlcd':[2001,2006]},
        'core':{'filter':'n8h','route':'r2', 'rg':'8w', 'mmu':'5'}
    }
)

