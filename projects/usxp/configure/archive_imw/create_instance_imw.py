from sqlalchemy import create_engine
import numpy as np, sys, os
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules')
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


# def getInfo(directory):
#     return directory


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
            # print dirnames
            gdbmatches = os.path.join(root, dirnames)
    # print gdbmatches
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
    


def getTemplatefile(directory):
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\instances\\template.json'.format(directory)) as json_data:
        template = json.load(json_data)
        return template


def getKernelfile(args_list):
    print 'args_list----------', args_list
    directory = args_list[0]
    print 'directory-------------', directory

    if directory == 'series':
        filename = args_list[1]

        with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\instances\\{}\\{}.json'.format(directory, filename)) as json_data:
            template = json.load(json_data)
            return template, directory, filename
    
    elif directory == 'routes':
        route = args_list[1]
        filename = args_list[2]

        with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\instances\\{}\\{}\\{}.json'.format(directory, route, filename)) as json_data:
            template = json.load(json_data)
            return template, directory, filename      




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

    def __init__(self, kernel, tmw, version):
        print("____________________start new object______________________________________________")
        #### instantiaote the template object so it can be modified ####
        self.data = getTemplatefile()

        ##need to split into versions because later methods refernce actual datasets ( i.e. getArbitraryCropValue() )
        if version == 'initial':
            self.updateKernel = self.updateKernel(kernel, tmw, version)
            self.updatePreObject = self.updatePreObject(kernel)
            self.exportObject = self.exportObject()
            # print 'initial version so quiting now with small current_instance'
            # return

        elif version == 'final':
            self.updateKernel = self.updateKernel(kernel, tmw, version)
            self.updatePreObject = self.updatePreObject(kernel)
            self.updateRefineObject = self.updateRefineObject(kernel)
            self.updateCoreObject = self.updateCoreObject(kernel)
            self.updatePostObject_YTC = self.updatePostObject_YTC(kernel)
            self.updatePostObject_YFC = self.updatePostObject_YFC(kernel)
            self.exportObject = self.exportObject()


        # elif version == 'instance':  
        #     print 'instance'
        #     ### once the trajectory datasets are created they can now be referenced by these methods!
            
        #     # self.updateCoreObject = self.updateCoreObject(kernel)
        #     # self.updatePostObject_YTC = self.updatePostObject_YTC(kernel)
        #     # self.updatePostObject_YFC = self.updatePostObject_YFC(kernel)
        #     # self.updateVectorsObject = self.updateVectorsObject(kernel)
            
        #     # #export modified object to json file
        #     # self.exportObject = self.exportObject()
    





    def updateKernel(self, kernel, tmw, version):
        #add the kernel object to the data object
        self.data['global']=kernel['global']
        self.data['global']['instance'] = kernel['global']['instance']
        self.data['global']['version'] = version

        for cy, years in tmw.iteritems():
            self.data['global']['years'] = years
            self.data['global']['years_conv'] = cy
            self.data['global']['datarange'] = '{}to{}'.format(str(years[0]), str(years[-1]))


 



    def updatePreObject(self, kernel):
        print '###############    updatePreObject()   #############################'

        ###update traj
        self.data['pre']['traj']['version'] = kernel['pre']['version']['traj']
        self.data['pre']['traj']['gdb'] = getGDBpath('{}_traj'.format(self.data['pre']['traj']['version']))
        self.data['pre']['traj']['filename'] = '_'.join([self.data['pre']['traj']['version'], 'traj', 'cdl'+self.data['global']['res'], 'b', self.data['global']['datarange']])
        self.data['pre']['traj']['path']  = '\\'.join([self.data['pre']['traj']['gdb'], self.data['pre']['traj']['filename']])
        if self.data['global']['years_conv'] == 2009: 
            self.data['pre']['traj']['lookup'] = '{}_lookup_2009'.format(self.data['pre']['traj']['version'])
        else:
            self.data['pre']['traj']['lookup'] = '{}_lookup'.format(self.data['pre']['traj']['version'])

        ### update traj rfnd
        self.data['pre']['traj_rfnd']['version'] = kernel['refine']['version']
        self.data['pre']['traj_rfnd']['gdb'] = getGDBpath('{}_traj_rfnd'.format(self.data['pre']['traj_rfnd']['version']))
        self.data['pre']['traj_rfnd']['filename'] = '_'.join([self.data['pre']['traj']['filename'],'rfnd', self.data['pre']['traj_rfnd']['version']])
        self.data['pre']['traj_rfnd']['path']  = '\\'.join([self.data['pre']['traj_rfnd']['gdb'], self.data['pre']['traj_rfnd']['filename']])  





    def updateRefineObject(self, kernel):
        print '###############    updateRefineObject()   #############################'
        self.data['refine']['version'] = kernel['refine']['version']
        self.data['refine']['gdb'] = getGDBpath('{}_masks'.format(self.data['refine']['version']))
        self.data['refine']['arbitrary_crop'] = getArbitraryCropValue(self.data['pre']['traj']['filename'], self.data['global']['years'], 'crop')
        self.data['refine']['arbitrary_noncrop'] = getArbitraryCropValue(self.data['pre']['traj']['filename'], self.data['global']['years'], 'noncrop')


        ## NLCD mask  ################################
        self.data['refine']['mask_nlcd']['years_nlcd'] = kernel['refine']['years_nlcd']
        self.data['refine']['mask_nlcd']['operator'] = kernel['refine']['operator']
        self.data['refine']['mask_nlcd']['filename'] = '{}_mask_nlcd_{}'.format(self.data['refine']['version'], self.data['global']['datarange'])
        self.data['refine']['mask_nlcd']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_nlcd']['filename']])
        
        ## developement,alfalfa and fallow mask  ################################
        self.data['refine']['mask_dev_alfalfa_fallow']['filename'] = '{}_mask_dev_alfalfa_fallow_{}'.format(self.data['refine']['version'], self.data['global']['datarange'])
        self.data['refine']['mask_dev_alfalfa_fallow']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_dev_alfalfa_fallow']['filename']])

        ## 2007 cdl  ################################
        if self.data['global']['years_conv'] == 2009: 
            self.data['refine']['mask_2007']['filename'] = '{}_mask_2007_{}'.format(self.data['refine']['version'], self.data['global']['datarange'])
            self.data['refine']['mask_2007']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_2007']['filename']])




    def updateCoreObject(self, kernel):
        print '###############    updateCoreObject()   #############################'
        ## transfer over the arguments from the kernel to the current data instance
        self.data['core']['gdb'] = getGDBpath('core_{}'.format(self.data['global']['instance']))
        self.data['core']['filter'] = kernel['core']['filter']
        self.data['core']['route'] = kernel['core']['route']
        self.data['core']['rg'] = kernel['core']['rg']
        self.data['core']['mmu'] = kernel['core']['mmu']
       

        if self.data['core']['route'] == 'r1':
            self.data['core']['filename'] = '{}_{}_mtr_{}_{}_mmu{}'.format(self.data['global']['instance'], self.data['pre']['traj_rfnd']['filename'], self.data['core']['filter'], self.data['core']['rg'], self.data['core']['mmu'])
            self.data['core']['path'] = '\\'.join([self.data['core']['gdb'], self.data['core']['filename']])
            print "self.data['core']['path']----------------------", self.data['core']['path']

        elif self.data['core']['route'] == 'r2':
            self.data['core']['filename'] = '{}_{}_{}_mtr_{}_mmu{}'.format(self.data['global']['instance'], self.data['pre']['traj_rfnd']['filename'], self.data['core']['filter'], self.data['core']['rg'], self.data['core']['mmu'])
            self.data['core']['path'] = '\\'.join([self.data['core']['gdb'], self.data['core']['filename']])
            print "self.data['core']['path']----------------------", self.data['core']['path']

        elif self.data['core']['route'] == 'r3':
            self.data['core']['filename'] = '{}_{}_{}_{}_mmu{}_mtr'.format(self.data['global']['instance'], self.data['pre']['traj_rfnd']['filename'], self.data['core']['filter'], self.data['core']['rg'], self.data['core']['mmu'])
            self.data['core']['path'] = '\\'.join([self.data['core']['gdb'], self.data['core']['filename']])
            print "self.data['core']['path']----------------------", self.data['core']['path']




    def updatePostObject_YTC(self, kernel):
        print '###############    updatePostObject_YTC()   #############################'
        def getvalues():
            ytc_dict = {}

            ytc_dict['gdb'] = getGDBpath('ytc_{}'.format(self.data['global']['instance']))
            ytc_dict['filename'] = '{}_ytc{}_{}_mmu{}'.format(self.data['global']['instance'], self.data['global']['res'], self.data['global']['datarange'], str(self.data['core']['mmu']))
            ytc_dict['path']  = '\\'.join([ytc_dict['gdb'], ytc_dict['filename']]) 
            ytc_dict['fc'] = self.createCDLdict('ytc', 'fc', self.data['global']['years_conv'])
            ytc_dict['bfc'] = self.createCDLdict('ytc', 'bfc', self.data['global']['years_conv'])
            
            return ytc_dict


        self.data['post']['ytc'] = getvalues()
    



    def updatePostObject_YFC(self, kernel):
        print '###############    updatePostObject_YFC()   #############################'
        def getvalues():
            yfc_dict = {}

            yfc_dict['gdb'] = getGDBpath('yfc_{}'.format(self.data['global']['instance']))
            yfc_dict['filename'] = '{}_yfc{}_{}_mmu{}'.format(self.data['global']['instance'], self.data['global']['res'], self.data['global']['datarange'], str(self.data['core']['mmu']))
            yfc_dict['path']  = '\\'.join([yfc_dict['gdb'], yfc_dict['filename']]) 
            yfc_dict['fnc'] = self.createCDLdict('yfc', 'fnc', self.data['global']['years_conv'])
            yfc_dict['bfnc'] = self.createCDLdict('yfc', 'bfnc', self.data['global']['years_conv'])
            
            return yfc_dict


        self.data['post']['yfc'] = getvalues()



    def createCDLdict(self, type, subtype, years):

        def getCDLpathsDict(years):
            #this is an aux function for attachCDL() function to get correct cdl for the attachCDL() function
            dict = {}
            for year in years:
                if subtype == 'fc' or subtype == 'fnc':
                    cdl_file = '{}{}cdl30_{}'.format(getGDBpath('cdl'), '\\', str(year))
                    dict[str(year)]=cdl_file
                elif subtype == 'bfc' or subtype == 'bfnc':
                    cdl_file = '{}{}cdl30_{}'.format(getGDBpath('cdl'), '\\', str(year-1))
                    dict[str(year)]=cdl_file
            return dict

        definedfilename = '{}_{}{}_{}_mmu{}_{}'.format(self.data['global']['instance'], type, self.data['global']['res'], self.data['global']['datarange'], str(self.data['core']['mmu']), subtype)
        definedpath = '{}\\{}'.format(getGDBpath('{}_{}'.format(type, self.data['global']['instance'])), definedfilename)
        dictpath={"cdlpaths":getCDLpathsDict(years),"filename":definedfilename, "path":definedpath}
        print dictpath
        return dictpath






    ####  create ########################################################################################
    def updateVectorsObject(self, kernel):
        print '###############    updateVectorsObject()   #############################'
        self.data['vectors'] = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\{}\\{}\\vectors".format(self.data['core']['route'], self.data['global']['instance'])




     #####   export the data objects  ################################################################################
    def exportObject(self):
        with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\current_instance.json', 'w') as outfile:
            json.dump(self.data, outfile, indent=4)




###########  create instance of class ################################################
def run(kernel, tmw, version):

    ProcessingObject(kernel, tmw, version)
if __name__ == '__main__':
    run(kernel, tmw, version)

