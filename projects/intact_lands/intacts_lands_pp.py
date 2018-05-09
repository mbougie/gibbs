import sys
import os
#import modules from other folders
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import arcpy
from arcpy import env
from arcpy.sa import *
import glob

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
# import general as gen
import json
import fnmatch
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import multiprocessing

sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen




arcpy.CheckOutExtension("Spatial")


try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 


def createReclassifyList():
    cur = conn.cursor()

    query = " SELECT value,b FROM misc.lookup_cdl WHERE b='1' "
    print 'query:', query

    cur.execute(query)
    #create empty list
    fulllist=[[0,0,"NODATA"]]

    ### fetch all rows from table
    rows = cur.fetchall()
    # print rows
    # print 'number of records in lookup table', len(rows)
    return rows





def execute_task(args):
    in_extentDict, covertype = args

    fc_count = in_extentDict[0]
    
    procExt = in_extentDict[1]
    # print procExt
    XMin = procExt[0]
    YMin = procExt[1]
    XMax = procExt[2]
    YMax = procExt[3]

    #set environments
    #The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
    arcpy.env.snapRaster = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj\\v4\\v4_traj.gdb\\v4_traj_cdl30_b_2008to2017"
    arcpy.env.cellsize = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj\\v4\\v4_traj.gdb\\v4_traj_cdl30_b_2008to2017"
    arcpy.env.outputCoordinateSystem = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj\\v4\\v4_traj.gdb\\v4_traj_cdl30_b_2008to2017"
    arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

    cls = 21973
    rws = 13789

    outData = np.zeros((13789, 21973), dtype=np.int)
    
    cdl_2016 = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2016'
    cdl_2017 = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017'
    InRas1=Raster('D:\\projects\\intact_land\\years\\2015.gdb\\clu_2015_crop_c_raster_rc1')


    def getReclassedRasters(covertype):
        #####NOTE &/OR compliment each other---exhuastive!!
        if covertype=='noncrop':
            reclass_2016 = Reclassify(Raster(cdl_2016), "Value", RemapValue([[176,1],[152,1],[195,1]]), "NODATA")
            reclass_2017 = Reclassify(Raster(cdl_2017), "Value", RemapValue([[176,1],[152,1],[195,1]]), "NODATA")
            #####  AND Operator  ###########################
            outCon = Con(IsNull(InRas1), InRas1, Con((reclass_2016==1) & (reclass_2017==1),1))
            return outCon
        elif covertype=='crop':
            ####NOTE: Need to reclass NULL to zero or else will ignore pixel !!!!!!!!!
            reclass_2016 = Reclassify(Raster(cdl_2016), "Value", RemapRange(createReclassifyList()), "NODATA")
            reclass_2016_0 =  Con(IsNull(reclass_2016),0,reclass_2016)
            reclass_2017 = Reclassify(Raster(cdl_2017), "Value", RemapRange(createReclassifyList()), "NODATA")
            reclass_2017_0 =  Con(IsNull(reclass_2017),0,reclass_2017)
            #####  OR Operator  ###########################
            outCon = Con(IsNull(InRas1), InRas1, Con((reclass_2016_0==1) | (reclass_2017_0==1),1))
            return outCon

    outCon=getReclassedRasters(covertype)

    #clear out the extent for next time
    arcpy.ClearEnvironment("extent")

    outname = "tile_" + str(fc_count) +'.tif'

    outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

    ###Save the output 
    outCon.save(outpath)






def mosiacRasters(covertype):
    ######Description: mosiac tiles together into a new raster
    tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
    print 'tilelist:', tilelist 

    #### need to wrap these paths with Raster() fct or complains about the paths being a string
    inTraj=Raster("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj\\v4\\v4_traj.gdb\\v4_traj_cdl30_b_2008to2017")
    
    gdb='D:\\projects\\intact_land\\abandoned.gdb'

    def getFileName(covertype):
        if covertype=='noncrop':
            return "abandoned_nd"
        elif covertype=='crop':
            return "crop_nd"

    filename = getFileName(covertype)
    print 'filename:-----------------------------', filename
    
    ######mosiac tiles together into a new raster
    arcpy.MosaicToNewRaster_management(tilelist, gdb, filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

    #######Overwrite the existing attribute table file
    arcpy.BuildRasterAttributeTable_management(gdb+'\\'+filename, "Overwrite")

    ########Overwrite pyramids
    gen.buildPyramids(gdb+'\\'+filename)





def run(covertype):

    tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
    for tile in tiles:
        os.remove(tile)

    #get extents of individual features and add it to a dictionary
    extDict = {}

    for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\fishnet_nd', ["oid","SHAPE@"]):
        atlas_stco = row[0]
        print atlas_stco
        extent_curr = row[1].extent
        ls = []
        ls.append(extent_curr.XMin)
        ls.append(extent_curr.YMin)
        ls.append(extent_curr.XMax)
        ls.append(extent_curr.YMax)
        extDict[atlas_stco] = ls

    print 'extDict', extDict
    print'extDict.items',  extDict.items()

    ######create a process and pass dictionary of extent to execute task
    pool = Pool(processes=5)
    pool.map(execute_task, [(ed, covertype) for ed in extDict.items()])
    pool.close()
    pool.join
    
    mosiacRasters(covertype)
    

if __name__ == '__main__':
    run('noncrop')


