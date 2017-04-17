from sqlalchemy import create_engine
import numpy as np, sys, os
from osgeo import gdal
from osgeo.gdalconst import *
# from pandas import read_sql_query
import pandas as pd
# import tables
import collections
from collections import namedtuple
import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2



rootDir='gibbs'
production_type='production'


engine = create_engine('postgresql://postgres:postgres@localhost:5432/pre')
df = pd.read_csv('C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/pre/traj.csv')
df.to_sql('trajectories', engine, schema='mtr')

# ###################  set up environment  #####################################

# rootDir='gibbs'
# production_type='production'
# arcpy.CheckOutExtension("Spatial")
# env.scratchWorkspace ="C:/Users/bougie/Documents/ArcGIS/scratch.gdb"




# mmu_gdb='C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/core/mmu.gdb/'
# mmu='traj_n8h_mtr_8w_msk45_nbl'
# mmu_Raster=Raster(mmu_gdb + mmu)




# def addColorMap(inraster,template):
#     ##Add Colormap
#     ##Usage: AddColormap_management in_raster {in_template_raster} {input_CLR_file}

#     try:
#         import arcpy
#         # arcpy.env.workspace = r'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'
        
#         ##Assign colormap using template image
#         arcpy.AddColormap_management(inraster, "#", template)
        

#     except:
#         print "Add Colormap example failed."
#         print arcpy.GetMessages()



# def createBinaries(typ,proc):
#     print proc
#     arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/pre/pre.gdb'
#     #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
#     engine = create_engine('postgresql://postgres:postgres@localhost:5432/pre')
#     df = pd.read_sql_query('select * from mtr.trajectories WHERE '+proc[0]+' IS NOT NULL',con=engine)
#     print 'df--',df

#     for index, row in df.iterrows():
#         value=str(int(row['Value'])) 
#         #cy=conversion year
#         cy=str(int(row[proc[0]]))
#         print type(cy)
#         if cy not in {'2012','2016'}:

            
#             output= 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/post/'+typ[0]+'.gdb/'+proc[0]+"_"+cy+"_b"
#             print output

#             #Get trajectories layer
#             inRas = Raster('traj')

#             print value

#             cond = "Value <> "+value
#             outSetNull = SetNull(inRas, cy[2:], cond)
#             outSetNull.save(output)






# def attachCDL(typ,params):
#     arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/post/'+typ[0]+'.gdb'
#     for raster in arcpy.ListDatasets("*_b", "Raster"): 
#         print raster

#         #######  GET YEAR  #############################
#         #acronym: fnf=file name fragments
#         fnf=(os.path.splitext(raster)[0]).split("_")
#         print 'fnf:', fnf
#         year = int(fnf[1]) - int(params[0])
#         print year
        
#         #######  DEFINE OUT RASTER  #####################
#         x=(os.path.splitext(raster)[0]).split(".")
#         output = x[0] + '_'+typ[1]
#         print 'output: ', output

        
#         #######  GET APPROPRIATE CDL BY YEAR  #############
#         cdl = 'D:/gibbs/'+production_type+'/rasters/pre/cdl/'+str(year)+"_30m_cdls.img"
#         print cdl


#         cond = "Value <> "+str(fnf[1])
#         print cond

#         OutRas=Con(raster, cdl, raster, cond)

#         # # Save the output 
#         OutRas.save(output)

#         #NEED TO FIX CRASHING PYHTON POSSIBLY ADD ENVIRONMENT?????????????????????????????????????????
#         addColorMap(output,'C:/Users/bougie/Desktop/'+rootDir+'/colormaps/cdl.clr')





# def cellStats(typ,params):
#     arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/post/'+typ[0]+'.gdb'
#     print 'params', params
#     wc=typ[1]
#     print wc


#     files_list = []
#     for raster in arcpy.ListDatasets("*"+wc, "Raster"): 
#         print raster
#         files_list.append(raster)

#     output='ytc_'+wc+'_mosaic'
#     print 'output: ', output
    
#     # Save the output 
#     outCellStatistics = CellStatistics([files_list[0],files_list[1],files_list[2]], "SUM", "DATA")
#     outCellStatistics.save(output)




# def mask(typ,params):
#     # params[x]:
#     # 0=mask name
#     # 1=wildcard
#     # 2
#     arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/post/'+typ[0]+'.gdb'
    
#     if params[0] == 'clipByMMU':
#         #create wildcard to subset rasters want to work with
#         wc='*_'+typ[1]+'_mosaic'
#         print 'wc: ', wc

#         for raster in arcpy.ListDatasets(wc, "Raster"): 
#             print 'raster: ',raster

#             output = raster+'_'+mmu
#             print 'output: ', output

#             outSetNull = SetNull(mmu_Raster, raster,  "Value <> 3")
            
#             #Save the output 
#             outSetNull.save(output)


    
#     elif params[0] == 'ndTo1':
#         #create wildcard to subset rasters want to work with
#         wc='*_'+typ[1]+'_mosaic_'+mmu
#         print 'wc: ', wc

#         for raster in arcpy.ListDatasets(wc, "Raster"): 
#             print 'raster: ',raster

#             # input_Raster=Raster(raster)
#             output = raster+'_ndTo1'
#             print 'output: ', output
            
#             OutRas=Con((IsNull(raster)) & (mmu_Raster == 3), 1,raster)
            
#             #Save the output 
#             OutRas.save(output)



# def nibble(typ,params):
#     arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/post/'+typ[0]+'.gdb'
#     raster = Raster('ytc_fc_mosaic_traj_n8h_mtr_8w_msk23_nbl_ndTo1')
    
#     wc='ytc_fc_mosaic_traj_n8h_mtr_8w_msk23_nbl'

#     for mask in arcpy.ListDatasets(wc, "Raster"): 
#         print 'mask: ', mask

#         output = mask+'_fnl'
#         print 'outRaster: ', outRaster

#         ###  Execute Nibble  #####################
#         nibbleOut = Nibble(raster, mask, "DATA_ONLY")

#         ###  Save the output  ################### 
#         nibbleOut.save(output)

#         addColorMap(output,'C:/Users/bougie/Desktop/'+rootDir+'/colormaps/mmu.clr')



# ######################################################################################


# def fire_all(func_list,typ,params):
#     for f in func_list:
#         f(typ,params)


    
# def runit():

#     fct_list = {'createBinaries':[createBinaries],
#                 'attachCDL': [attachCDL],
#                 'cellStats': [cellStats],
#                 'mask':[mask],
#                 'mask_clipByMMU':[mask],
#                 'mask_prepNibble':[mask],
#                 'mask_ndTo1':[mask],
#                 'nibble':[nibble]
#                }


#     engine = create_engine('postgresql://postgres:postgres@localhost:5432/metadata')
#     df = pd.read_sql_query('SELECT * FROM routes_prod.routes_post ORDER BY serial',con=engine)
    
#     routes=df['routes']
#     for i, route in enumerate(routes):
#         print i
#         print route
#         for step in route:
#             print 'step-----------------', step
         
#             params=df[step][i]
#             print 'params: ', params
#             typ=df['type'][i]
#             print typ
#             fire_all(fct_list[step],typ,params)
    



# ##############  call functions  #####################################################
# runit()

# # {createBinaries,cellStats,mask_clipByMMU,mask_ndTo1}
# # {attachCDL,cellStats,mask_clipByMMU}