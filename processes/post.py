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


'''######## DEFINE THESE EACH TIME ##########'''
#NOTE: need to declare if want to process ytc or yfc
yxc = 'yfc'

#the associated mtr value qwith the yxc
yxc_mtr = {'ytc':'3', 'yfc':'4'}

#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")



###################  declare functions  #######################################################
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 


##  datasets from core process ##########################################################
mmu_gdb=defineGDBpath(['core','mmu'])
mmu='traj_rfnd_n8h_mtr_8w_msk23_nbl'
mmu_Raster=Raster(mmu_gdb + mmu)




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


def createYearbinaries(typ, gdb_args_in):

    #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #define file names
    traj = defineGDBpath(['pre','trajectories'])+"traj_b"
    traj_years = typ+"_years"

    #create an empty raster so it can be modified later
    # createEmptyRaster(gdb_args_in, traj_years)
    
    #Connect to postgres database to get values from traj dataset 
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query('select * from pre.traj_b as a JOIN pre.traj_b_lookup as b ON a.traj_array = b.traj_array WHERE b.'+typ+' IS NOT NULL',con=engine)
    print 'df--',df
    
    # loop through rows in the dataframe
    for index, row in df.iterrows():
        #get the arbitrary value assigned to the specific trajectory
        value=str(row['Value'])
        print 'value: ', value

        #cy is acronym for conversion year
        cy = str(row[typ])
        print 'cy:', cy
        
        # allow raster to be overwritten
        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput
    
        #establish the condition
        cond = "Value = " + value
        print 'cond: ', cond
        
        # set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
        OutRas = Con(traj_years, cy, traj_years, cond)
        # set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
        # outSetNull = SetNull(traj, cy, cond)
        OutRas.save(traj_years)





def clipByMMUmask(gdb_args_in):
    #DESCRIPTION: clip the year mosiac raster by the mmu raster to only get the patches > 5 acres

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #create wildcard to subset processes want to work with
    traj_years=yxc+'_years'

    #create output file 
    output = traj_years+'_'+mmu
    print 'output: ', output
    
    #condition value is dependent of yxc (i.e. ytc = 3 and yfc = 4)
    cond = "Value <> " + yxc_mtr[yxc]
    print 'cond: ', cond
    
    # set mmu raster to null where not equal to value and then attached the values fron traj_years tp these [value] patches
    outSetNull = SetNull(mmu_Raster, traj_years,  cond)
    
    #Save the output 
    outSetNull.save(output)




def attachCDL(gdb_args_in, yr_reduction):
    # NEED TO AUTOMATE!!!!!

    #DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    arcpy.env.workspace=defineGDBpath(gdb_args_in)


    raster_reference = Raster('yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl')
    # raster_input = 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_clean_bfnc'
        
    #     #loop through
    # cursor = arcpy.SearchCursor(raster_reference)
    # for row in cursor:
    # year = row.getValue('Value')
    # print "outside of if:", year
    # # if year >= 2013:
        
    # print 'year:', year

    #allow the mosaic raster to be overwritten
    # arcpy.env.overwriteOutput = True
    # print "overwrite on? ", arcpy.env.overwriteOutput 
    outraster='yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_bfnc'
        
    # # #######  GET APPROPRIATE CDL BY YEAR  #############
    # cdl = defineGDBpath(['ancillary','cdl'])+'cdl_'+str(year)
    # print "cdl raster with the appropriate year:", cdl

    # #establish the condition
    # year_mod=year+yr_reduction
    # cond = "Value = "+str(year_mod)
    # print 'cond: ', cond
    cdl2012=Raster(defineGDBpath(['ancillary','cdl'])+'cdl_2012')
    cdl2013=Raster(defineGDBpath(['ancillary','cdl'])+'cdl_2013')
    cdl2014=Raster(defineGDBpath(['ancillary','cdl'])+'cdl_2014')
    cdl2015=Raster(defineGDBpath(['ancillary','cdl'])+'cdl_2015')

    # if raster_reference equals 2013 apply cdl value of that year ELSE if false check if raster_reference equals 2014 or 2015 and apply the corresponding cdl year
    OutRas = Con((raster_reference == 2013),cdl2012, Con((raster_reference == 2014),cdl2013, Con((raster_reference == 2015),cdl2014, Con((raster_reference < 2013),raster_reference))))
    # OutRas = Con((raster_reference == 2013),cdl2013, Con((raster_reference == 2014),cdl2014, Con((raster_reference == 2015),cdl2015, Con((raster_reference < 2013),raster_reference))))

    # # Save the output 
    OutRas.save(outraster)


def createMaskRaster(gdb_args_in):
    #DESCRIPTION: remove the arbitrary values from the 'yfc_years_'+mmu dataset

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #create wildcard to subset processes want to work with
    raster_input = 'yfc_years_'+mmu

    #create output file 
    raster_output = raster_input+'_mask'
    print 'output: ', raster_output
    
    #condition value is dependent of yxc (i.e. ytc = 3 and yfc = 4)
    cond = "Value < 2013"
    print 'cond: ', cond
    
    # set mmu raster to null where value is less 2013 to get rid of attribute values
    outSetNull = SetNull(raster_input, raster_input,  cond)
    
    #Save the output 
    outSetNull.save(raster_output)





def nibble(gdb_args_in, filename):
    #DESCRIPTION:The Nibble tool allows selected areas of a raster to be assigned the value of their nearest neighbor.  In our case there are gaps in the conversion patch that we fill 
    #Note: Cells in the input raster containing NoData are not nibbled. To nibble NoData, first convert it to another value (see ndTo1mask(wc) function above)

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #declare variables but dont intialize them
    in_raster = 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_'+filename
    print 'in_raster: ', in_raster

    in_mask_raster = 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_mask'
    print 'in_mask_raster: ', in_mask_raster

    #define output
    output = in_raster+'_fnl'
    print 'output: ', output

    ###  Execute Nibble  #####################
    nibbleOut = Nibble(in_raster, in_mask_raster, "DATA_ONLY")
    ###  Save the output  ################### 
    nibbleOut.save(output)





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






##############  call functions  #####################################################
# createYearbinaries(yxc, ['post',yxc])
# clipByMMUmask(['post',yxc])
createMaskRaster(['post',yxc])
# nibble('b', ['post',yxc])




# attachCDL(['post',yxc],0)
# mosiacRasters('fnc', ['post',yxc])
# clipByMMUmask('fnc', ['post',yxc])
# ndTo1mask('fnc', ['post',yxc])
nibble('bfnc', ['post',yxc])



# attachCDL(['post',yxc],'bfc', conversion_years, 0)






# mosiacRasters('bfnc', ['post',yxc])
# clipByMMUmask('bfnc', ['post',yxc])
# ndTo1mask('bfnc', ['post',yxc])
# nibble('bfnc', ['post',yxc])





