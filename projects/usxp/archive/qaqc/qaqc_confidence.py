import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen
import json
import re


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

subtype = 'bfc'


##get the current instance
data = gen.getJSONfile()
print data


def createReclassifyList():
  engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
  query = " SELECT \"Value\", ytc from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc IS NOT NULL AND version = '{}' ".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup'], data['pre']['traj']['lookup_version'])
  # print 'query:', query
  df = pd.read_sql_query(query, con=engine)
  print df
  fulllist=[[0,0,"NODATA"]]
  for index, row in df.iterrows():
      templist=[]
      value=row['Value'] 
      mtr=row['ytc']  
      templist.append(int(value))
      templist.append(int(mtr))
      fulllist.append(templist)
  print 'fulllist: ', fulllist
  return fulllist


  
def execute_task(in_extentDict):
  yxc = {'ytc':3, 'yfc':4}

  fc_count = in_extentDict[0]
  # print fc_count
  procExt = in_extentDict[1]
  # print procExt
  XMin = procExt[0]
  YMin = procExt[1]
  XMax = procExt[2]
  YMax = procExt[3]

  path_traj_rfnd = data['pre']['traj_rfnd']['path']
  path_mtr = Raster(data['core']['path'])

  #set environments
  arcpy.env.snapRaster = path_mtr
  arcpy.env.cellsize = path_mtr
  arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)
  
  # reclassify raster
  raster_yxc = Reclassify(Raster(path_traj_rfnd), "Value", RemapRange(createReclassifyList()), "NODATA")
  
  #create raster mask to get rid of anyhting not pertaining to year
  raster_mask = Con((path_mtr == yxc['ytc']) & (raster_yxc >= 2008), raster_yxc)

  # for year, cdlpath in data['post']['ytc'][subtype]['cdlpaths'].iteritems():
  # Set the current workspace
  arcpy.env.workspace = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\conf.gdb"

  # Get and print a list of GRIDs from the workspace
  # for year, cdlpath in data['post']['ytc'][subtype]['cdlpaths'].iteritems():

  rasters = arcpy.ListRasters("*", "GRID")
  
  for raster in rasters:
    print(raster)

   
    #extact the year from the filename
    year_int = re.search('National_Confidence_(.*)_30m_cdls', raster)
    year = year_int.group(1)

    # allow raster to be overwritten
    arcpy.env.overwriteOutput = True
    print "overwrite on? ", arcpy.env.overwriteOutput

    #establish the condition
    cond = "Value = " + year
    print 'cond: ', cond
    
    # overwrite the raster_mask dataset
    raster_mask = Con(raster_mask, raster, raster_mask, cond)


  raster_mmu = Con((path_mtr == yxc['ytc']) & (IsNull(raster_mask)), 255, raster_mask)

  raster_nibble = arcpy.sa.Nibble(raster_mmu, raster_mask, "DATA_ONLY")

  outname = "tile_" + str(fc_count) +'.tif'

  outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

  arcpy.ClearEnvironment("extent")

  raster_nibble.save(outpath)
        



def mosiacRasters():
  ######Description: mosiac tiles together into a new raster
  tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
  print 'tilelist:', tilelist 

  #### need to wrap these paths with Raster() fct or complains about the paths being a string
  inTraj=Raster(data['pre']['traj']['path'])

  filename = data['post']['ytc'][subtype]['filename']
  print 'filename:', filename
  
  ######mosiac tiles together into a new raster
  arcpy.MosaicToNewRaster_management(tilelist, data['post']['ytc']['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

  #Overwrite the existing attribute table file
  arcpy.BuildRasterAttributeTable_management(data['post']['ytc'][subtype]['path'], "Overwrite")

  # Overwrite pyramids
  gen.buildPyramids(data['post']['ytc'][subtype]['path'])





if __name__ == '__main__':

  #####  remove a files in tiles directory
  tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*")
  for tile in tiles:
    os.remove(tile)

  #get extents of individual features and add it to a dictionary
  extDict = {}
  count = 1 

  for row in arcpy.da.SearchCursor(data['ancillary']['vector']['shapefiles']['counties_subset'], ["SHAPE@"]):
    extent_curr = row[0].extent
    ls = []
    ls.append(extent_curr.XMin)
    ls.append(extent_curr.YMin)
    ls.append(extent_curr.XMax)
    ls.append(extent_curr.YMax)
    extDict[count] = ls
    count+=1
    
  print 'extDict', extDict
  print'extDict.items',  extDict.items()

  #######create a process and pass dictionary of extent to execute task
  # pool = Pool(processes=1)
  pool = Pool(processes=cpu_count())
  pool.map(execute_task, extDict.items())
  pool.close()
  pool.join

  # mosiacRasters()