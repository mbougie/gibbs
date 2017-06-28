from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
# from pandas import read_sql_query
import pandas as pd
# import tables
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2


import multiprocessing

from multiprocessing import Process
from multiprocessing import Process, freeze_support












arcpy.CheckOutExtension("Spatial")
case=['bougie','gibbs']


###################  declare functions  #######################################################
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 



def majorityFilter(gdb_args_in, dataset, gdb_args_out):
    arcpy.env.workspace = defineGDBpath(gdb_args_in)

    filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    for k, v in filter_combos.iteritems():
        print k,v
        for raster in arcpy.ListDatasets(dataset, "Raster"): 
            print 'raster: ', raster
    
            raster_out=raster+'_'+k
            
            # Execute MajorityFilter
            outMajFilt = MajorityFilter(raster, v[0], v[1])
            
            output = defineGDBpath(gdb_args_out)+raster_out
            print 'output: ',output
            
            #save processed raster to new file
            outMajFilt.save(output)




def focalStats(index,dir_in,dir_out):

    #NOT CONVERTED TO GDB YET!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print dir_in,dir_out
    ds = DirStructure(dir_in, dir_out)

    filter_combos = {'k3':[3, 3, "CELL"],'k5':[5, 5, "CELL"]}
    for k, v in filter_combos.iteritems():
        print k,v
        os.chdir(ds.dir_in)
        for file in glob.glob("*.img"):
            #fnf=file name fragments
            fnf=(os.path.splitext(file)[0]).split(".")
            
            #create file structure
            fs = FileStructure(fnf[0]+'_'+k, '.img')

            neighborhood = NbrRectangle(v[0], v[1], v[2])

            # Check out the ArcGIS Spatial Analyst extension license
            arcpy.CheckOutExtension("Spatial")

            # Execute FocalStatistics
            outFocalStatistics = FocalStatistics(ds.dir_in+file, neighborhood, "MAJORITY")

            output = ds.dir_out+fs.file_out

            # Save the output 
            outFocalStatistics.save(output)

            # addColorMap(output,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/filter_and_mmu.clr')




# majorityFilter(['sensitivity_analysis','pre'],'traj_initial',['sensitivity_analysis','filter'])


















def majorityFilter_try(dataset):
    arcpy.env.workspace = defineGDBpath(['split','try'])

    filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    for k, v in filter_combos.iteritems():
        print k,v
        for raster in arcpy.ListDatasets(dataset, "Raster"): 
            print 'raster: ', raster
    
            raster_out=raster+'_'+k
            
            # Execute MajorityFilter
            outMajFilt = MajorityFilter(raster, v[0], v[1])
            
            output = defineGDBpath(['split','output15'])+raster_out
            print 'output: ',output
            
            #save processed raster to new file
            outMajFilt.save(output)

# def main():


oids = ['try1', 'try2', 'try3']

#     pool = multiprocessing.Pool()

#     result_tables = pool.map(majorityFilter_try, oids)

#     # Synchronize the main process with the job processes to ensure proper cleanup.
#     pool.close()
#     pool.join()

#     if __name__ == '__main__':
#         freeze_support()
#         Process(target=majorityFilter_try).start()
# main()





def f(x):
    return x*x

if __name__ == '__main__':
    p = Process(target=majorityFilter_try, args=(oids,))
    p.start()
    p.join()
