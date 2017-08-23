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
import psycopg2
import general as gen 


'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"

###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path


#################### class to create yxc object  ####################################################
class ConversionObject:

    def __init__(self, gdb_args_in, subtype, mmu_dataset, res, conversionyears):
        self.name = 'ytc'
        self.subtype = subtype
        self.conversionyears = range(conversionyears[0], conversionyears[1] + 1)
        self.res = res
        print self.conversionyears
        self.mmu_gdb=defineGDBpath(gdb_args_in)
        self.mmu_Raster=Raster(self.mmu_gdb + mmu_dataset)
        

        if self.name == 'ytc':
            self.mtr = '3'
        elif self.name == 'yfc':
            self.mtr = '4'
    
    #function for to get correct cdl for the attachCDL() function
    def getAssociatedCDL(self, year):
        if self.subtype == 'bfc' or  self.subtype == 'bfnc':
            # subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ self.res + '_' + str(year - 1)
            return cdl_file

        elif self.subtype == 'fc' or  self.subtype == 'fnc':
            # subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ self.res + '_' + str(year)
            return cdl_file

        







def addColorMap(inraster,template):
    ##Add Colormap
    ##Usage: AddColormap_management in_raster {in_template_raster} {input_CLR_file}

    try:
        import arcpy
        # arcpy.env.workspace = r'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'
        
        ##Assign colormap using template image
        arcpy.AddColormap_management(inraster, "#", template)
        

    except:
        print "Add Colormap example failed."
        print arcpy.GetMessages()


def createYearbinaries(gdb_args_in):
    #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #copy trajectory raster so it can be modified iteritively
    traj_years = 'ytc_years8to12'
    # arcpy.CopyRaster_management(defineGDBpath(['pre', 'trajectories']) + 'traj_cdl_b', traj_years)
    
    #Connect to postgres database to get values from traj dataset 
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query('select * from pre.traj_cdl30_b_8to12 as a JOIN pre.traj_cdl30_b_8to12_lookup as b ON a.traj_array = b.traj_array WHERE b.'+yxc.name+' IS NOT NULL',con=engine)
    print 'df--',df
    
    # loop through rows in the dataframe
    for index, row in df.iterrows():
        #get the arbitrary value assigned to the specific trajectory
        value=str(row['Value'])
        print 'value: ', value

        #cy is acronym for conversion year
        cy = str(row[yxc.name])
        print 'cy:', cy
        
        # allow raster to be overwritten
        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput
    
        #establish the condition
        cond = "Value = " + value
        print 'cond: ', cond
        
        # set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
        OutRas = Con(traj_years, cy, traj_years, cond)
   
        OutRas.save(traj_years)



def clipByMMUmask(gdb_args_in):
    #DESCRIPTION: clip the year mosiac raster by the mmu raster in the core geodatabase to only get the patches > 5 acres

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #get the years raster from geodatabase
    traj_years=yxc.name+'_years'

    #create output file 
    output = traj_years + '_' + yxc.mmu
    print 'output: ', output
    
    #condition value is dependent of yxc (i.e. ytc = 3 and yfc = 4)
    cond = "Value <> " + yxc.mtr
    print 'cond: ', cond
    
    # set mmu raster to null where not equal to value and then attached the values fron traj_years tp these [value] patches
    outSetNull = SetNull(yxc.mmu_Raster, traj_years,  cond)
    
    #Save the output 
    outSetNull.save(output)



def removeArbitraryValues(gdb_args_in):
    #DESCRIPTION: remove the arbitrary values from the 'yfc_years_'+mmu dataset

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)

    # allow raster to be overwritten
    # arcpy.env.overwriteOutput = True
    # print "overwrite on? ", arcpy.env.overwriteOutput
    
    #get raster from geodatabse
    raster_input = 'ytc30_years8to12'
    raster_output = 'ytc30_years8to12_clean'

    cond = "Value < 2009" 
    print 'cond: ', cond
        
    # set mmu raster to null where value is less 2013 (i.e. get rid of attribute values)
    outSetNull = SetNull(raster_input, raster_input,  cond)
    
    #Save the output 
    outSetNull.save(raster_output)



def attachCDL(gdb_args_in):
    # DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    # arcpy.env.rasterStatistics = 'STATISTICS'
    # # Set the cell size environment using a raster dataset.
    # arcpy.env.cellSize = "ytc_years_traj_cdl_b_n8h_mtr_8w_msk23_nbl"

    # # Set Snap Raster environment
    # arcpy.env.snapRaster = "ytc_years_traj_cdl_b_n8h_mtr_8w_msk23_nbl"

    # # #copy raster
    # arcpy.CopyRaster_management("ytc_years_traj_cdl_b_n8h_mtr_8w_msk23_nbl", "ytc_years_traj_cdl_b_n8h_mtr_8w_msk23_nbl_"+yxc.subtype)
    
    wc = '*'+yxc.subtype
    print wc

    for raster in arcpy.ListDatasets(wc, "Raster"): 
      
        for year in  yxc.conversionyears:
            print 'raster: ', raster
            print 'year: ', year

            # allow raster to be overwritten
            arcpy.env.overwriteOutput = True
            print "overwrite on? ", arcpy.env.overwriteOutput
        
            #establish the condition
            cond = "Value = " + str(year)
            print 'cond: ', cond

            cdl_file= yxc.getAssociatedCDL(year)
            print 'associated cdl file: ', cdl_file
            
            # # set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
            OutRas = Con(raster, cdl_file, raster, cond)
       
            OutRas.save(raster)



################ Instantiate the class to create yxc object  ########################
yxc = ConversionObject(
      ['refinement','refinement_current'],
      'bfc',
      'traj_cdl30_b_8to12_mtr',
      '30',
      ## conversion years!!---NOT the range of years in the analysis! 
      [2009,2012]
      )

################ call functions  #####################################################
# createYearbinaries(['refinement','refinement_current'])
# removeArbitraryValues(['refinement','refinement_current'])
attachCDL(['refinement','refinement_current'])




###### NOTE FOR COPY RASTER """""""""""""""""""""
## 1 test snap 
## 2 test import stats from other image??
## 3 try other creating stats from multiple methods






