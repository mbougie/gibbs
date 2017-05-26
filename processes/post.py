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


def createYearbinaries(typ, gdb_args_out):
    #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
    arcpy.env.workspace=defineGDBpath(['pre','trajectories'])
    
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    # df = pd.read_sql_query("select \"Value\",new_value from refinement.traj_"+degree_lc+" as a JOIN refinement.traj_lookup as b ON a.traj_array = b.traj_array WHERE b.name='"+degree_lc+"'",con=engine)
    df = pd.read_sql_query('select * from pre.traj_b as a JOIN pre.traj_b_lookup as b ON a.traj_array = b.traj_array WHERE '+typ+' IS NOT NULL',con=engine)
    print 'df--',df

    for index, row in df.iterrows():
        value=str(int(row['Value'])) 
        print 'value: ', value
        #cy=conversion year
        cy=str(int(row[typ]))
        print 'cy:', type(cy)
        if cy not in {'2012','2016'}:

            out_raster=typ+"_"+cy+"_b"

            output= defineGDBpath(gdb_args_out)+out_raster
            print 'output: ', output

            # Get trajectories layer
            inRas = Raster('traj_b')

       
            cond = "Value <> "+value
            outSetNull = SetNull(inRas, cy[2:], cond)
            outSetNull.save(output)



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

        cond = "Value <> " + yxc_mtr[yxc]
        print 'cond: ', cond
        
        #perform setNull function to convert raster to null except where mtr value = yxc_mtr[yxc]
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
        OutRas=Con((IsNull(raster)) & (mmu_Raster == int(yxc_mtr[yxc]), 1,raster)
        
        #Save the output 
        OutRas.save(output)





def attachCDL(typ,yr_reduction):
    #DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    arcpy.env.workspace=defineGDBpath(['post','ytc'])

    # arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/processes/post/'+typ[0]+'.gdb'
    for raster in arcpy.ListDatasets("*_b", "Raster"): 
        print 'raster: ', raster

        #######  GET YEAR  #############################
        #acronym: fnf=file name fragments
        fnf=(os.path.splitext(raster)[0]).split("_")
        # print 'fnf:', fnf
        year = int(fnf[1]) - yr_reduction
        print 'cdl year to reference: ', year
        
        #######  DEFINE OUT RASTER  #####################
        output = raster + '_'+typ
        print 'output: ', output

        
        #######  GET APPROPRIATE CDL BY YEAR  #############
        cdl = 'D:/cdl/'+str(year)+"_30m_cdls.img"
        print "cdl raster with the appropriate year: ", cdl


        cond = "Value <> "+str(fnf[1][2:])
        print 'cond: ', cond

        OutRas=Con(raster, Raster(cdl), raster, cond)

        # # Save the output 
        OutRas.save(output)








def nibble(wc, gdb_args_in):
    #DESCRIPTION:The Nibble tool allows selected areas of a raster to be assigned the value of their nearest neighbor.  In our case there are gaps a conversion patch that we fill 
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
# createYearbinaries('yfc', ['post','yfc'])
mosiacRasters('b', ['post',yxc])
clipByMMUmask('b', ['post',yxc])
ndTo1mask('b', ['post',yxc])
nibble('b', ['post',yxc])




# attachCDL('fc',0)
# mosiacRasters('fc')
# mask('fc','clipByMMU')
# mask('fc','ndTo1')
# nibble('fc')


# attachCDL('bfc',1)
# mosiacRasters('bfc')
# mask('bfc','clipByMMU')
# mask('bfc','ndTo1')
# nibble('bfc')







# {createBinaries,cellStats,mask_clipByMMU,mask_ndTo1}
# {attachCDL,cellStats,mask_clipByMMU}