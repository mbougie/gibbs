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

conversion_years = ['2013','2014','2015']

#import extension
arcpy.CheckOutExtension("Spatial")


# print arcpy.env.overwriteOutput 

# arcpy.env.overwriteOutput = True

# print arcpy.env.overwriteOutput 

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

    # copy raster
    arcpy.CopyRaster_management(traj, traj_years)
    
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    # df = pd.read_sql_query("select \"Value\",new_value from refinement.traj_"+degree_lc+" as a JOIN refinement.traj_lookup as b ON a.traj_array = b.traj_array WHERE b.name='"+degree_lc+"'",con=engine)
    df = pd.read_sql_query('select * from pre.traj_b as a JOIN pre.traj_b_lookup as b ON a.traj_array = b.traj_array WHERE b.'+typ+' IS NOT NULL',con=engine)
    print 'df--',df
    
    # loop through rowes in the dataframe
    for index, row in df.iterrows():
        # value=str(int(row['Value'])) 
        value=row['Value']
        print 'value: ', value

        #cy is acrronym for conversion year
        # cy=str(int(row[typ]))
        cy=row[typ]
        print 'cy:', cy
        
        # allow raster to be overwritten
        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput
    
        #establish the condtion
        cond = "Value <> "+value
        print 'cond: ', cond
        
        # set everthing not equal to the unique trajectory value to null label this abitray value equal to cy
        outSetNull = SetNull(traj, cy, cond)
        outSetNull.save(traj_years)




def mosiacRasters(wc, gdb_args_in):
    #DESCRIPTION:merge the seperate binary year rasters into on raster so to represent the entire conversion/abandon dataset
    
    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #create list to store all raster that meet condition below
    files_list = []
    for raster in arcpy.ListDatasets("*"+wc, "Raster"): 
        print raster
        files_list.append(raster)
    
    #create output file 
    output = yxc+'_'+wc+'_mosaic'
    print 'output: ', output
    
    #perform cellstatistics function to mosiac rasters
    outCellStatistics = CellStatistics([files_list[0],files_list[1],files_list[2]], "SUM", "DATA")

    # Save the output 
    outCellStatistics.save(output)




def clipByMMUmask(wc, gdb_args_in):
    #DESCRIPTION: clip the year mosiac raster by the mmu raster to only get the patches > 5 acres

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #create wildcard to subset processes want to work with
    wc='*_'+wc+'_mosaic'
    print 'wc: ', wc
    
    #loop through rasters in gdb that match cond.
    for raster in arcpy.ListDatasets(wc, "Raster"): 
        print 'raster: ',raster
        
        #create output file 
        output = raster+'_'+mmu
        print 'output: ', output
        
        #condition value is dependent of yxc (i.e. ytc = 3 and yfc = 4)
        cond = "Value <> " + yxc_mtr[yxc]
        print 'cond: ', cond
        
        # perform setNull function to convert raster to null except where mtr value = cond
        outSetNull = SetNull(mmu_Raster, raster,  cond)
        
        #Save the output 
        outSetNull.save(output)


def ndTo1mask(wc, gdb_args_in):
    #DESCRIPTION: Function converts nondata values to value 1. Output raster used as an input mask for nibble() function.

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
      
    #create wildcard to subset processes want to work with
    wc='*_'+wc+'_mosaic_'+mmu
    print 'wc: ', wc

    #loop through rasters in gdb that match cond.
    for raster in arcpy.ListDatasets(wc, "Raster"): 
        print 'raster: ',raster

        #create output file 
        output = raster+'_ndTo1'
        print 'output: ', output
        
        #perform CON function to............................
        OutRas=Con((IsNull(raster)) & (mmu_Raster == int(yxc_mtr[yxc])), 1,raster)
        
        #Save the output 
        OutRas.save(output)





# def attachCDL(gdb_args_in,typ,yr_reduction):
#     #DESCRIPTION:attach the appropriate cdl value to each year binary dataset
#     arcpy.env.workspace=defineGDBpath(gdb_args_in)

#     # arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/processes/post/'+typ[0]+'.gdb'
#     for raster in arcpy.ListDatasets("*_b", "Raster"): 
#         print 'raster: ', raster

#         #######  GET YEAR  #############################
#         #acronym: fnf=file name fragments
#         fnf=(os.path.splitext(raster)[0]).split("_")
#         # print 'fnf:', fnf
#         year = int(fnf[1]) - yr_reduction
#         print 'cdl year to reference: ', year
        
#         #######  DEFINE OUT RASTER  #####################
#         output = raster + '_'+typ
#         print 'output: ', output

        
#         #######  GET APPROPRIATE CDL BY YEAR  #############
#         cdl = defineGDBpath(['ancillary','cdl'])+'cdl_'+str(year)
#         print "cdl raster with the appropriate year: ", cdl


#         cond = "Value <> "+str(fnf[1][2:])
#         print 'cond: ', cond

#         OutRas=Con(raster, Raster(cdl), raster, cond)

#         # # Save the output 
#         OutRas.save(output)



def attachCDL(gdb_args_in,typ,years, yr_reduction):


    
   

    #DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    arcpy.env.workspace=defineGDBpath(gdb_args_in)

    #get mosiac raster
    for raster in arcpy.ListDatasets("*_b_mosaic_traj_rfnd_n8h_mtr_8w_msk23_nbl", "Raster"): 
        print 'raster: ', raster
        


        for year in conversion_years:
            print year
            print 'cdl year to reference: ', year

            #allow the mosaic raster to be overwritten
            arcpy.env.overwriteOutput = True
            print "overwrite on? ", arcpy.env.overwriteOutput 
            
            # #######  GET APPROPRIATE CDL BY YEAR  #############
            cdl = defineGDBpath(['ancillary','cdl'])+'cdl_'+year
            print "cdl raster with the appropriate year: ", cdl

            #establish the condition
            cond = "Value = "+year
            print 'cond: ', cond
            
            #if mosiac raster equals year get the corresponding year of cdl and replace year value with corresponding cdl value
            OutRas=Con(raster, Raster(cdl), raster, cond)

            # # Save the output 
            OutRas.save(raster)








def nibble(wc, gdb_args_in):
    #DESCRIPTION:The Nibble tool allows selected areas of a raster to be assigned the value of their nearest neighbor.  In our case there are gaps in the conversion patch that we fill 
    #Note: Cells in the input raster containing NoData are not nibbled. To nibble NoData, first convert it to another value (see ndTo1mask(wc) function above)

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    #declare variables but dont intialize them
    clipByMMU = None
    ndTo1 = None
    
    #initialize the above variables giving it the name of the raster with specific condtion
    for raster in arcpy.ListDatasets('*'+'_'+wc+'*_ndTo1', "Raster"): 
        ndTo1=raster

    for raster in arcpy.ListDatasets('*'+'_'+wc+'*_nbl', "Raster"): 
        clipByMMU=raster

    #define output
    output = clipByMMU+'_fnl'
    print 'output: ', output

    ###  Execute Nibble  #####################
    nibbleOut = Nibble(ndTo1, clipByMMU, "DATA_ONLY")

    ###  Save the output  ################### 
    nibbleOut.save(output)



##############  call functions  #####################################################
createYearbinaries('yfc', ['post','yfc'])
# mosiacRasters('b', ['post',yxc])
# clipByMMUmask('b', ['post',yxc])
# ndTo1mask('b', ['post',yxc])
# nibble('b', ['post',yxc])




# attachCDL(['post',yxc],'fnc',0)
# mosiacRasters('fnc', ['post',yxc])
# clipByMMUmask('fnc', ['post',yxc])
# ndTo1mask('fnc', ['post',yxc])
# nibble('fnc', ['post',yxc])



# attachCDL(['post',yxc],'bfc', conversion_years, 0)






# mosiacRasters('bfnc', ['post',yxc])
# clipByMMUmask('bfnc', ['post',yxc])
# ndTo1mask('bfnc', ['post',yxc])
# nibble('bfnc', ['post',yxc])



