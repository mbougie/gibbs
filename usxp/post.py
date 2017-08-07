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
# import general as gen 


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

    def __init__(self, name, subtype, conversionyears):
        self.name = name
        self.subtype = subtype
        self.conversionyears = range(conversionyears[0], conversionyears[1] + 1)
        self.mmu_gdb=defineGDBpath(['core','mmu'])
        self.mmu='traj_rfnd_n8h_mtr_8w_msk23_nbl'
        self.mmu_Raster=Raster(self.mmu_gdb + self.mmu)
        

        if self.name == 'ytc':
            self.mtr = '3'
        elif self.name == 'yfc':
            self.mtr = '4'
    
    #function for to get correct cdl for the attachCDL() function
    def getAssociatedCDL(self, year):
        if self.subtype == 'bfc' or  self.subtype == 'bfnc':
            # subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl_'+ str(year - 1)
            return cdl_file

        elif self.subtype == 'fc' or  self.subtype == 'fnc':
            # subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl_'+ str(year)
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
    
    #NEED TO COPY TAJ RASTER AND RENAME IT THIS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    traj_years = yxc.name+"_years"

    #create an empty raster so it can be modified later
    # createEmptyRaster(gdb_args_in, traj_years)
    
    #Connect to postgres database to get values from traj dataset 
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query('select * from pre.traj_cdl_b as a JOIN pre.traj_cdl_b_lookup as b ON a.traj_array = b.traj_array WHERE b.'+yxc.name+' IS NOT NULL',con=engine)
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
    traj_years=Raster(yxc.name+'_years')

    #create output file 
    output = traj_years + '_' + yxc.mmu
    print 'output: ', output
    
    #condition value is dependent of yxc (i.e. ytc = 3 and yfc = 4)
    cond = "Value <> " + yxc.mtr
    print 'cond: ', cond
    
    # set mmu raster to null where not equal to value and then attached the values fron traj_years tp these [value] patches
    outSetNull = SetNull(mmu_Raster, traj_years,  cond)
    
    #Save the output 
    outSetNull.save(output)



def removeArbitraryValues(gdb_args_in):
    #DESCRIPTION: remove the arbitrary values from the 'yfc_years_'+mmu dataset

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #get raster from geodatabse
    raster_input = Raster(yxc.name + '_years_' + yxc.mmu)

    #create output file 
    raster_output = raster_input+'_mask'
    print 'output: ', raster_output
        
    # set mmu raster to null where value is less 2013 (i.e. get rid of attribute values)
    outSetNull = SetNull(raster_input, raster_input,  yxc.conversionyears[0])
    
    #Save the output 
    outSetNull.save(raster_output)



def attachCDL(gdb_args_in):
    #DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    arcpy.env.workspace=defineGDBpath(gdb_args_in)

    #make a copy of raster  NOTE:DOESNT WORK YET!!!
    # arcpy.CopyRaster_management("ytc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl", "ytc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_"+yxc.subtype)
    wc = '*'+yxc.subtype

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





# def nibble(gdb_args_in, filename):
#     #DESCRIPTION:The Nibble tool allows selected areas of a raster to be assigned the value of their nearest neighbor.  In our case there are gaps in the conversion patch that we fill 
#     #Note: Cells in the input raster containing NoData are not nibbled. To nibble NoData, first convert it to another value (see ndTo1mask(wc) function above)

#     #define gdb workspace
#     arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
#     #declare variables but dont intialize them
#     in_raster = 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_'+filename
#     print 'in_raster: ', in_raster

#     in_mask_raster = 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_mask'
#     print 'in_mask_raster: ', in_mask_raster

#     #define output
#     output = in_raster+'_fnl'
#     print 'output: ', output

#     ###  Execute Nibble  #####################
#     nibbleOut = Nibble(in_raster, in_mask_raster, "DATA_ONLY")
#     ###  Save the output  ################### 
#     nibbleOut.save(output)





################ call functions  ##################################
#create base datasets
# createYearbinaries('yfc', ['post','yfc'])
# clipByMMUmask('b', ['post',yxc])
# ndTo1mask('b', ['post',yxc])
# nibble('b', ['post',yxc])

# croplist=['fnc','bfnc']:
# for crp in croplist:
# attachCDL(['post',yxc], 'fnc', conversion_years, 0)
#     nibble('b', ['post',yxc])




# 

##############  call functions  #####################################################
# createYearbinaries(['post',yxc])
# clipByMMUmask(['post',yxc])
# removeArbitraryValues(['post',yxc], "Value < 2013")
# nibble('b', ['post',yxc])




# attachCDL(['post',yxc],0)
# attachCDL(['post',yxc],0)
# mosiacRasters('fnc', ['post',yxc])
# clipByMMUmask('fnc', ['post',yxc])
# ndTo1mask('fnc', ['post',yxc])
# nibble('bfnc', ['post',yxc])



# attachCDL(['post',yxc],'bfc', conversion_years, 0)






# mosiacRasters('bfnc', ['post',yxc])
# clipByMMUmask('bfnc', ['post',yxc])
# ndTo1mask('bfnc', ['post',yxc])
# nibble('bfnc', ['post',yxc])



################ Instantiate the class to create yxc object  ########################
yxc = ConversionObject(
      'ytc',
      'bfc', 
      [2013,2015]
      )

################ call functions  #####################################################
# createYearbinaries(['post',yxc.name])
# clipByMMUmask(['post',yxc.name])
# removeArbitraryValues(['post',yxc.name])
attachCDL(['post',yxc.name])




