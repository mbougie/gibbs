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




arcpy.CheckOutExtension("Spatial")
case=['bougie','gibbs']


###################  declare functions  #######################################################
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/processes/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
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



def createYTCbinaries(typ):
    arcpy.env.workspace=defineGDBpath(['pre','pre'])
    # arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/processes/pre/pre.gdb'
    #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/core')
    df = pd.read_sql_query('select * from pre.traj WHERE '+typ+' IS NOT NULL',con=engine)
    print 'df--',df

    for index, row in df.iterrows():
        value=str(int(row['Value'])) 
        print 'value: ', value
        #cy=conversion year
        cy=str(int(row[typ]))
        print 'cy:', type(cy)
        if cy not in {'2012','2016'}:

            out_raster=typ+"_"+cy+"_b"

            # output= 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/processes/post/'+typ+'.gdb/'+typ+"_"+cy+"_b"

            output= defineGDBpath(['post','ytc'])+out_raster
            print 'output: ', output

            # Get trajectories layer
            inRas = Raster('traj')

       
            cond = "Value <> "+value
            outSetNull = SetNull(inRas, cy[2:], cond)
            outSetNull.save(output)






def attachCDL(typ,yr_reduction):
    arcpy.env.workspace=defineGDBpath(arg_list)

    # arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/processes/post/'+typ[0]+'.gdb'
    for raster in arcpy.ListDatasets("*_b", "Raster"): 
        print raster

        #######  GET YEAR  #############################
        #acronym: fnf=file name fragments
        fnf=(os.path.splitext(raster)[0]).split("_")
        print 'fnf:', fnf
        year = int(fnf[1]) - yr_reduction
        print 'cdl year to reference: ', year
        
        #######  DEFINE OUT RASTER  #####################
        x=(os.path.splitext(raster)[0]).split(".")
        output = x[0] + '_'+typ[1]
        print 'output: ', output

        
        #######  GET APPROPRIATE CDL BY YEAR  #############
        cdl = 'D:/cdl/'+str(year)+"_30m_cdls.img"
        print "cdl raster with the appropriate year", cdl


        cond = "Value <> "+str(fnf[1])
        print cond

        OutRas=Con(raster, Raster(cdl), raster, cond)

        # # Save the output 
        OutRas.save(output)

        #NEED TO FIX CRASHING PYHTON POSSIBLY ADD ENVIRONMENT?????????????????????????????????????????
        addColorMap(output,'C:/Users/bougie/Desktop/'+rootDir+'/colormaps/cdl.clr')





def mosiacRasters(wc):
    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(['post','ytc'])
    
    #create list to store all raster that meet condition below
    files_list = []
    for raster in arcpy.ListDatasets("*"+wc, "Raster"): 
        print raster
        files_list.append(raster)
    
    #create output file 
    output='ytc_'+wc+'_mosaic'
    print 'output: ', output
    
    #perform cellstatistics function to mosiac rasters
    outCellStatistics = CellStatistics([files_list[0],files_list[1],files_list[2]], "SUM", "DATA")

    # Save the output 
    outCellStatistics.save(output)




def mask(wc,masktype):
    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(['post','ytc'])
    
    if masktype == 'clipByMMU':
        
        #create wildcard to subset processes want to work with
        wc='*_'+wc+'_mosaic'
        print 'wc: ', wc
        
        #loop through rasters in gdb that match cond.
        for raster in arcpy.ListDatasets(wc, "Raster"): 
            print 'raster: ',raster
            
            #create output file 
            output = raster+'_'+mmu
            print 'output: ', output
            
            #perform setNull function to convert raster to null except where mtr value = 3
            outSetNull = SetNull(mmu_Raster, raster,  "Value <> 3")
            
            #Save the output 
            outSetNull.save(output)


    
    elif masktype == 'ndTo1':
       
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
            OutRas=Con((IsNull(raster)) & (mmu_Raster == 3), 1,raster)
            
            #Save the output 
            OutRas.save(output)



def nibble(typ,mskSize):
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/processes/post/'+typ[0]+'.gdb'
    raster = Raster('ytc_'+typ[1]+'_mosaic_traj_n8h_mtr_8w_msk'+mskSize+'_nbl_ndTo1')
    print 'raster: ', raster
    
    wc='ytc_'+typ[1]+'_mosaic_traj_n8h_mtr_8w_msk'+mskSize+'_nbl'
    print 'wc: ', wc

    for mask in arcpy.ListDatasets(wc, "Raster"): 
        print 'mask: ', mask

        output = mask+'_fnl'
        print 'output: ', output

        ###  Execute Nibble  #####################
        nibbleOut = Nibble(raster, mask, "DATA_ONLY")

        ###  Save the output  ################### 
        nibbleOut.save(output)

        addColorMap(output,'C:/Users/bougie/Desktop/'+rootDir+'/colormaps/mmu.clr')





##############  call functions  #####################################################
# createYTCbinaries('ytc')
# mosiacRasters('b')
# mask('b','clipByMMU')
# mask('b','ndTo1')
# nibble(['ytc','b'],'23')



# attachCDL(['ytc','fc'],0)
# mosiacRasters('fc')
# mask('fc','clipByMMU')
# mask('fc','ndTo1')
# nibble(['ytc','fc'],'23')


# attachCDL(['ytc','bfc'],1)
# mosiacRasters('bfc')
# mask('bfc','clipByMMU')
# mask('bfc','ndTo1')
# nibble(['ytc','bfc'],'23')







# {createBinaries,cellStats,mask_clipByMMU,mask_ndTo1}
# {attachCDL,cellStats,mask_clipByMMU}