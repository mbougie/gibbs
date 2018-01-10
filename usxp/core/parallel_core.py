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


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 




def getJSONfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\config\\current_instance.json') as json_data:
        template = json.load(json_data)
        # print(template)
        # print type(template)
        return template



data = getJSONfile()
print data


# def createMMUmaskTiles():
#     root_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
#     rasterlist = glob.glob(root_in+"*.tif")
#     print rasterlist

#     for raster in rasterlist:
#         print raster

#         output = raster.replace('.', '_mask.')
#         print output

#         # for count in masks_list:
#         cond = "Count < " + str(gen.getPixelCount(str(data['global']['res']), int(data['core']['mmu'])))
#         print 'cond: ',cond

#         outSetNull = SetNull(raster, 1, cond)

#         # Save the output 
#         outSetNull.save(output)




def createReclassifyList():
    #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = " SELECT \"Value\", mtr from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array AND version = '{}' ".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup'], data['pre']['traj']['lookup_version'])
    # print 'query:', query
    df = pd.read_sql_query(query, con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        value=row['Value'] 
        mtr=row['mtr']  
        templist.append(int(value))
        templist.append(int(mtr))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist

 

reclassArray = createReclassifyList() 
print 'reclassArray:', reclassArray





def execute_task(in_extentDict):
	#########  Execute Nibble  #####################
	filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
	filter_key = data['core']['filter']

	rg_combos = {'8w':["EIGHT", "WITHIN"]}
	rg_instance = rg_combos['8w']

	# for count in masks_list:
	cond = "Count < " + str(gen.getPixelCount(str(data['global']['res']), int(data['core']['mmu'])))
	print 'cond: ',cond


	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	route = 'r2'

	if route == 'r2':
		raster_filter = MajorityFilter(Raster(data['pre']['traj_rfnd']['path']), filter_combos[filter_key][0], filter_combos[filter_key][1])
		raster_mtr = Reclassify(raster_filter, "Value", RemapRange(reclassArray), "NODATA")
		raster_rg = RegionGroup(raster_mtr, rg_instance[0], rg_instance[1],"NO_LINK")
		raster_mask = SetNull(raster_rg, 1, cond)
		raster_nbl = arcpy.sa.Nibble(raster_mtr, raster_mask, "DATA_ONLY")

		#clear out the extent for next time
		arcpy.ClearEnvironment("extent")

		outname = "tile_" + str(fc_count) +'.tif'

		outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

		raster_nbl.save(outpath)








# def execute_task(in_extentDict):
# 	yxc = {'ytc':3, 'yfc':4}


# 	fc_count = in_extentDict[0]
# 	# print fc_count
# 	procExt = in_extentDict[1]
# 	# print procExt
# 	XMin = procExt[0]
# 	YMin = procExt[1]
# 	XMax = procExt[2]
# 	YMax = procExt[3]

# 	path_traj_rfnd = data['pre']['traj_rfnd']['path']
# 	print 'path_traj_rfnd:', path_traj_rfnd

# 	path_mtr = Raster(data['core']['path']['mmu'])

# 	#set environments
# 	arcpy.env.snapRaster = path_mtr
# 	arcpy.env.cellsize = path_mtr
# 	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

# 	###  Execute the three functions  #####################
# 	raster_yxc = Reclassify(Raster(path_traj_rfnd), "Value", RemapRange(createReclassifyList()), "NODATA")

# 	raster_mask = Con((path_mtr == yxc['ytc']) & (raster_yxc >= 2008), raster_yxc)

# 	raster_mmu = Con((path_mtr == yxc['ytc']) & (IsNull(raster_mask)), yxc['ytc'], Con((path_mtr == yxc['ytc']) & (raster_mask >= 2008), raster_mask))

# 	raster_nibble = arcpy.sa.Nibble(raster_mmu, raster_mask, "DATA_ONLY")

# 	#clear out the extent for next time
# 	arcpy.ClearEnvironment("extent")

# 	# print fc_count
# 	outname = "tile_" + str(fc_count) +'.tif'

# 	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

# 	raster_nibble.save(outpath)




def mosiacRasters():
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['core']['function']['parallel_mmu']['output'].replace(data['core']['gdb']+'\\', '')
	print 'filename:', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['core']['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['core']['function']['parallel_mmu']['output'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['core']['function']['parallel_mmu']['output'])





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
	pool = Pool(processes=8)
	# pool = Pool(processes=cpu_count())
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join

	mosiacRasters()