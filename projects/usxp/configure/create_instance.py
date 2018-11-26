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
import psycopg2.extras
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
        for dirnames in fnmatch.filter(dirnames, '*{}*'.format(wc)):
            # print dirnames
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


def getArbitraryYFC(table,lookup):
    query = ''' SELECT
                    yfc, "Value"
                FROM pre.{0} INNER JOIN pre.{1}
                USING(traj_array) 
                WHERE "Count" IN (SELECT
                                    max("Count")
                                  FROM pre.{0} INNER JOIN pre.{1}
                                    USING(traj_array) WHERE yfc > 2008 and potential_yfc=0
                                   GROUP BY yfc)
                GROUP BY "Value",yfc
                ORDER BY yfc'''.format(table,lookup)


    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    print rows
    print type(rows)
    d = {}
    for row in rows:
        print row[0]
        print row[1]

        d[row[0]] = row[1]

    print d
    return d



def getArbitraryYFC_list(table,lookup):
    query = ''' SELECT
                    yfc, "Value"
                FROM pre.{0} INNER JOIN pre.{1}
                USING(traj_array) 
                WHERE "Count" IN (SELECT
                                    max("Count")
                                  FROM pre.{0} INNER JOIN pre.{1}
                                    USING(traj_array) WHERE yfc > 2008 and potential_yfc=0
                                   GROUP BY yfc)
                GROUP BY "Value",yfc
                ORDER BY yfc'''.format(table,lookup)


    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    print rows
    return rows









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



def getArbitraryMTR5(table, lookup):
    cur = conn.cursor()
   
    cur.execute("SELECT \"Value\" FROM pre.{} INNER JOIN pre.{} USING(traj_array) WHERE mtr=5 ORDER BY \"Count\" desc limit 1".format(table, lookup))

    # fetch all rows from table
    rows = cur.fetchall()
    print 'arbitrary value', rows[0][0]
    return rows[0][0]


def getPixelsPerTile(raster, vector):
    value = raster/vector
    return value



class ProcessingObject(object):

    def __init__(self, args, version):
        kernel = args[0]
        print 'kernel', kernel
        directory = args[1]
        instance = args[2]
        print instance

        ##get the template json
        self.data = getTemplatefile(directory)
        print self.data


        ##need to split into versions because later methods refernce actual datasets ( i.e. getArbitraryCropValue() )
        if version == 'initial':
            self.updateKernel = self.updateKernel(kernel, instance, version)
            self.updatePreObject = self.updatePreObject(kernel)
            self.exportObject = self.exportObject(directory)
            # print 'initial version so quiting now with small current_instance'
            # return

        elif version == 'add_yfc' or version == 'final':
            self.updateKernel = self.updateKernel(kernel, instance, version)
            self.updateTiles = self.updateTiles(kernel)
            self.updatePreObject = self.updatePreObject(kernel)
            self.updateRefineObject = self.updateRefineObject(kernel)
            self.updateCoreObject = self.updateCoreObject(kernel)
            self.updatePostObject_YTC = self.updatePostObject_YTC(kernel)
            self.updatePostObject_YFC = self.updatePostObject_YFC(kernel)
            self.exportObject = self.exportObject(directory)

            # #call the methods in order to modifiy the tempalte json
            # self.updateKernel = self.updateKernel(kernel, instance)
            # self.updatePreObject = self.updatePreObject(kernel)
            # # self.updateRefineObject = self.updateRefineObject(kernel)
            # self.updateCoreObject = self.updateCoreObject(kernel)
            # self.updatePostObject_YTC = self.updatePostObject_YTC(kernel)
            # self.updatePostObject_YFC = self.updatePostObject_YFC(kernel)
            # self.updateVectorsObject = self.updateVectorsObject(kernel)
            
            # #export modified object to json file
            # self.exportObject = self.exportObject(directory)
    


    def updateKernel(self, kernel, instance, version):
        print '###############    updateKernel()   #############################'
        self.data['global']=kernel['global']

        self.data['global']['instance'] = kernel['global']['instance']
        self.data['global']['years'] = range(kernel['global']['years'][0], kernel['global']['years'][1]+1)
        self.data['global']['years_conv'] = range(kernel['global']['conv_years'][0], kernel['global']['conv_years'][1]+1)
        self.data['global']['datarange'] = '{}to{}'.format(str(self.data['global']['years'][0]), str(self.data['global']['years'][-1]))
        self.data['global']['version'] = version



    def updateTiles(self, kernel):
        print '###############    updateShapefiles()   #############################'
        cdl = self.data['ancillary']['rasters']['cdl']
        nlcd = self.data['ancillary']['rasters']['nlcd']

        fishnet_cdl_7_7 = self.data['ancillary']['vectors']['fishnet_cdl_7_7']
        fishnet_cdl_49_7 = self.data['ancillary']['vectors']['fishnet_cdl_49_7']
        fishnet_nlcd_6_19 = self.data['ancillary']['vectors']['fishnet_nlcd_6_19']


        self.data['ancillary']['tiles']["fishnet_cdl_7_7"]["cls"]=getPixelsPerTile( cdl["cls"], fishnet_cdl_7_7["cls"] )
        self.data['ancillary']['tiles']["fishnet_cdl_7_7"]["rws"]=getPixelsPerTile( cdl["rws"], fishnet_cdl_7_7["rws"] )

        self.data['ancillary']['tiles']["fishnet_cdl_49_7"]["cls"]=getPixelsPerTile( cdl["cls"], fishnet_cdl_49_7["cls"] )
        self.data['ancillary']['tiles']["fishnet_cdl_49_7"]["rws"]=getPixelsPerTile( cdl["rws"], fishnet_cdl_49_7["rws"] )

        self.data['ancillary']['tiles']["fishnet_nlcd_6_19"]["cls"]=getPixelsPerTile( nlcd["cls"], fishnet_nlcd_6_19["cls"] )
        self.data['ancillary']['tiles']["fishnet_nlcd_6_19"]["rws"]=getPixelsPerTile( nlcd["rws"], fishnet_nlcd_6_19["rws"] )


        # self.data['ancillary']['tiles']["fishnet_cdl_7_7"]["cls"]=45
        # self.data['ancillary']['tiles']["fishnet_cdl_7_7"]["rws"]=4522





        # ###update traj
        # self.data['pre']['traj']['version'] = kernel['pre']['version']['traj']
        # self.data['pre']['traj']['gdb'] = getGDBpath('{}_traj.gdb'.format(self.data['pre']['traj']['version']))
        # print "self.data['pre']['traj']['gdb']", self.data['pre']['traj']['gdb']
        # self.data['pre']['traj']['filename'] = '_'.join([self.data['pre']['traj']['version'], 'traj', 'cdl'+self.data['global']['res'], 'b', self.data['global']['datarange']])
        # self.data['pre']['traj']['path']  = '\\'.join([self.data['pre']['traj']['gdb'], self.data['pre']['traj']['filename']]) 
        # self.data['pre']['traj']['lookup_version'] = kernel['pre']['version']['lookup']
        # self.data['pre']['traj']['lookup_name'] = '{}_traj_lookup_{}_{}'.format(self.data['pre']['traj']['version'], self.data['global']['datarange'], kernel['pre']['version']['lookup'])





    def updatePreObject(self, kernel):
        print '###############    updatePreObject()   #############################'
        
        ###update traj
        self.data['pre']['traj']['version'] = kernel['pre']['version']['traj']
        self.data['pre']['traj']['gdb'] = getGDBpath('{}_traj.gdb'.format(self.data['pre']['traj']['version']))
        print "self.data['pre']['traj']['gdb']", self.data['pre']['traj']['gdb']
        self.data['pre']['traj']['filename'] = '_'.join([self.data['pre']['traj']['version'], 'traj', 'cdl'+self.data['global']['res'], 'b', self.data['global']['datarange']])
        self.data['pre']['traj']['path']  = '\\'.join([self.data['pre']['traj']['gdb'], self.data['pre']['traj']['filename']]) 
        self.data['pre']['traj']['lookup_version'] = kernel['pre']['version']['lookup']
        self.data['pre']['traj']['lookup_name'] = '{}_traj_lookup_{}_{}'.format(self.data['pre']['traj']['version'], self.data['global']['datarange'], kernel['pre']['version']['lookup'])




        # ### update traj rfnd
        self.data['pre']['traj_rfnd']['version'] = kernel['refine']['version']
        self.data['pre']['traj_rfnd']['gdb'] = getGDBpath('{}_traj_rfnd'.format(self.data['pre']['traj_rfnd']['version']))


         # ### update traj rfnd
        self.data['pre']['traj_yfc']['filename'] = '_'.join([self.data['pre']['traj']['filename'],'yfc', self.data['pre']['traj_rfnd']['version']])
        self.data['pre']['traj_yfc']['path']  = '\\'.join([self.data['pre']['traj_rfnd']['gdb'], self.data['pre']['traj_yfc']['filename']]) 
        

        self.data['pre']['traj_rfnd']['filename'] = '_'.join([self.data['pre']['traj']['filename'],'rfnd', self.data['pre']['traj_rfnd']['version']])
        self.data['pre']['traj_rfnd']['path']  = '\\'.join([self.data['pre']['traj_rfnd']['gdb'], self.data['pre']['traj_rfnd']['filename']])  





    def updateRefineObject(self, kernel):
        print '###############    updateRefineObject()   #############################'
        self.data['refine']['version'] = kernel['refine']['version']
        # self.data['refine']['gdb'] = getGDBpath('{}_masks'.format(self.data['refine']['version']))
        self.data['refine']['gdb'] = getGDBpath('{}_masks'.format(self.data['pre']['traj']['filename']))
        # self.data['refine']['fp_gdb'] = getGDBpath('masks_fp')
        self.data['refine']['arbitrary_crop'] = getArbitraryCropValue(self.data['pre']['traj']['filename'], self.data['global']['years'], 'crop')
        self.data['refine']['arbitrary_noncrop'] = getArbitraryCropValue(self.data['pre']['traj']['filename'], self.data['global']['years'], 'noncrop')
        self.data['refine']['dict_arb_yfc'] = getArbitraryYFC(self.data['pre']['traj']['filename'], self.data['pre']['traj']['lookup_name'])
        self.data['refine']['list_arb_yfc'] = getArbitraryYFC_list(self.data['pre']['traj']['filename'], self.data['pre']['traj']['lookup_name'])
        self.data['refine']['arbitrary_mtr5'] = getArbitraryMTR5(self.data['pre']['traj']['filename'], self.data['pre']['traj']['lookup_name'])


        

        #############################  false negative masks #################################################################################
        # self.data['refine']['mask_nlcd']['arbitrary_expand'] = getArbitraryCropValue(self.data['pre']['traj']['filename'], self.data['global']['years'], 'crop')
        # self.data['refine']['mask_nlcd']['arbitrary_abandon'] = 1
        


        # self.data['refine']['mask_fn_yfc_nlcd']['filename'] = '{}_mask_fn_yfc_nlcd'.format(self.data['refine']['version'])
        self.data['refine']['mask_fn_yfc_nlcd_mtr1']['filename'] = 'mask_fn_yfc_nlcd_mtr1'
        self.data['refine']['mask_fn_yfc_nlcd_mtr1']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fn_yfc_nlcd_mtr1']['filename']])

        # self.data['refine']['mask_fn_yfc_61']['filename'] = '{}_mask_fn_yfc_61'.format(self.data['refine']['version'])
        self.data['refine']['mask_fn_yfc_61']['filename'] = 'mask_fn_yfc_61'
        self.data['refine']['mask_fn_yfc_61']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fn_yfc_61']['filename']])
        self.data['refine']['mask_fn_yfc_61_preview']['filename'] = 'mask_fn_yfc_61_preview'
        self.data['refine']['mask_fn_yfc_61_preview']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fn_yfc_61_preview']['filename']])

        # self.data['refine']['mask_nlcd_yfc']['filename'] = '{}_mask_nlcd_{}_{}_yfc'.format(self.data['refine']['version'], 'and'.join(str(e) for e in self.data['refine']['mask_nlcd']['years_nlcd']), self.data['global']['datarange'])
        # self.data['refine']['mask_nlcd_yfc']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_nlcd_yfc']['filename']])



        ############################  false postive masks  ##############################################################
        self.data['refine']['mask_fp_nlcd']['years_nlcd'] = kernel['refine']['years_nlcd']
        self.data['refine']['mask_fp_nlcd']['operator'] = kernel['refine']['operator']
        # self.data['refine']['mask_fp_nlcd']['filename'] = '{}_mask_fp_nlcd_{}_{}'.format(self.data['refine']['version'], 'and'.join(str(e) for e in self.data['refine']['mask_fp_nlcd']['years_nlcd']), self.data['global']['datarange'])
        self.data['refine']['mask_fp_nlcd_yfc']['filename'] = 'mask_fp_nlcd_yfc'
        self.data['refine']['mask_fp_nlcd_yfc']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fp_nlcd_yfc']['filename']])

        self.data['refine']['mask_fp_nlcd_ytc']['filename'] = 'mask_fp_nlcd_ytc'
        self.data['refine']['mask_fp_nlcd_ytc']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fp_nlcd_ytc']['filename']])



        # self.data['refine']['mask_fp_ytc']['filename'] = '{}_masks_fp_ytc_{}'.format(self.data['refine']['version'], self.data['global']['datarange'])
        self.data['refine']['mask_fp_ytc']['filename'] = 'masks_fp_ytc'
        self.data['refine']['mask_fp_ytc']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fp_ytc']['filename']])
        self.data['refine']['mask_fp_ytc_preview']['filename'] = 'masks_fp_ytc_preview'
        self.data['refine']['mask_fp_ytc_preview']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fp_ytc_preview']['filename']])

        # self.data['refine']['mask_fp_yfc']['filename'] = '{}_masks_fp_yfc_{}'.format(self.data['refine']['version'], self.data['global']['datarange'])
        self.data['refine']['mask_fp_yfc']['filename'] = 'masks_fp_yfc'
        self.data['refine']['mask_fp_yfc']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fp_yfc']['filename']])
        self.data['refine']['mask_fp_yfc_preview']['filename'] = 'masks_fp_yfc_preview'
        self.data['refine']['mask_fp_yfc_preview']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fp_yfc_preview']['filename']])

        


        # self.data['refine']['mask_fp_2007']['filename'] = '{}_mask_fp_2007_{}'.format(self.data['refine']['version'], self.data['global']['datarange'])
        self.data['refine']['mask_fp_2007']['filename'] = 'mask_fp_2007'
        self.data['refine']['mask_fp_2007']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fp_2007']['filename']])

        # self.data['refine']['mask_fp_yfc_potential']['filename'] = '{}_mask_fp_yfc_potential'.format(self.data['refine']['version'])
        self.data['refine']['mask_fp_yfc_potential']['filename'] = 'mask_fp_yfc_potential'
        self.data['refine']['mask_fp_yfc_potential']['path'] = '\\'.join([self.data['refine']['gdb'], self.data['refine']['mask_fp_yfc_potential']['filename']])

        # self.data['refine']['mask_dev_alfalfa_fallow']['arbitrary'] = getArbitraryCropValue(self.data['pre']['traj']['filename'], self.data['global']['years'], 'noncrop')
        


 
    #####   core functions  ################################################################################
    def updateCoreObject(self, kernel):
        print '###############    updateCoreObject()   #############################'
        
        self.data['core']['gdb'] = getGDBpath('core_{}'.format(self.data['global']['instance']))
        print "self.data['core']['gdb']-----------------------", self.data['core']['gdb']
        self.data['core']['filter'] = kernel['core']['filter']
        self.data['core']['route'] = kernel['core']['route']
        self.data['core']['rg'] = kernel['core']['rg']
        self.data['core']['mmu'] = kernel['core']['mmu']
        # self.data['core']['lookup'] = '{}_traj_lookup_{}_{}'.format(self.data['pre']['traj']['version'], self.data['global']['datarange'], kernel['core']['lookup_version'])

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




    #####   post functions  ################################################################################
    

    def updatePostObject_YTC(self, kernel):
        print '###############    updatePostObject_YTC()   #############################'
        def getvalues():
            ytc_dict = {}

            ytc_dict['gdb'] = getGDBpath('ytc_{}'.format(self.data['global']['instance']))
            ytc_dict['filename'] = '{}_ytc{}_{}_mmu{}'.format(self.data['global']['instance'], self.data['global']['res'], self.data['global']['datarange'], str(self.data['core']['mmu']))
            ytc_dict['path']  = '\\'.join([ytc_dict['gdb'], ytc_dict['filename']]) 
            ytc_dict['fc'] = self.createCDLdict('ytc', 'fc', self.data['global']['years_conv'])
            ytc_dict['bfc'] = self.createCDLdict('ytc', 'bfc', self.data['global']['years_conv'])
            ytc_dict['zonal_hist_path_ytc'] = '\\'.join([ytc_dict['gdb'],'{}_zonal_hist_table_ytc'.format(self.data['global']['instance'])])
            ytc_dict['zonal_hist_path_bfc'] = '\\'.join([ytc_dict['gdb'],'{}_zonal_hist_table_bfc'.format(self.data['global']['instance'])])
            ytc_dict['zonal_hist_path_fc'] = '\\'.join([ytc_dict['gdb'],'{}_zonal_hist_table_fc'.format(self.data['global']['instance'])])


            
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
            
            ##### sub-optimal  #####################################################################
            yfc_dict['fnc_61_filename'] = '{}_yfc_fnc_61'.format(self.data['global']['instance'])
            yfc_dict['fnc_61_path'] = '\\'.join([yfc_dict['gdb'], yfc_dict['fnc_61_filename']]) 
            ######################################################################################

            yfc_dict['bfnc'] = self.createCDLdict('yfc', 'bfnc', self.data['global']['years_conv'])
            yfc_dict['zonal_hist_path_yfc'] = '\\'.join([yfc_dict['gdb'],'{}_zonal_hist_table_yfc'.format(self.data['global']['instance'])])

            
            return yfc_dict


        self.data['post']['yfc'] = getvalues()



    def createCDLdict(self, type, subtype, years):

        def getCDLpathsDict(years):
            #this is an aux function for attachCDL() function to get correct cdl for the attachCDL() function
            dict = {}
            for year in years:
                if subtype == 'fc' or subtype == 'fnc':
                    cdl_file = '{}{}cdl30_{}'.format('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb', '\\', str(year))
                    dict[str(year)]=cdl_file
                elif subtype == 'bfc' or subtype == 'bfnc':
                    cdl_file = '{}{}cdl30_{}'.format('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb', '\\', str(year-1))
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
        # self.data['refine']['version'] = kernel['refine']['version']
        self.data['vectors'] = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\{}\\{}\\vectors".format(self.data['core']['route'], self.data['global']['instance'])




     #####   export objects  ################################################################################
    def exportObject(self, directory):
        with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\instances\\current_instance.json'.format(directory), 'w') as outfile:
            json.dump(self.data, outfile, indent=4)




###########  create instance of class ################################################
def run(arg_list, version):

    ProcessingObject(getKernelfile(arg_list), version)



if __name__ == '__main__':
    run(arg_list, version)




