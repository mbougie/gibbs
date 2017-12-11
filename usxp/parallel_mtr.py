import arcpy
from arcpy import env
from arcpy.sa import *
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
import os
import glob
import sys
import time
import logging
import multiprocessing
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import general as gen

arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")

#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


#######  define raster and mask  ####################

class ProcessingObject(object):

    def __init__(self, series, res, mmu, years, subtype, gdb_parent, parent_seq, gdb_child, child_seq):
    	self.series = series
        self.res = str(res)
        self.mmu = mmu
        self.years =years 
        self.subtype = subtype
        self.datarange = str(self.years[0])+'to'+str(self.years[1])
        self.traj = self.series+'_traj_cdl'+self.res+'_b_'+self.datarange+'_rfnd'
       

        self.gdb_parent = defineGDBpath(gdb_parent)
        self.raster_parent = self.traj+parent_seq
        self.path_parent = self.gdb_parent + self.raster_parent
       

        self.gdb_child = defineGDBpath(gdb_child)
        self.raster_child = self.traj + child_seq
        self.path_child = self.gdb_child + self.raster_child
        
        self.out_fishnet = defineGDBpath(['ancillary', 'shapefiles']) + 'counties_subset'
        self.pixel_type = "32_BIT_UNSIGNED"
        self.dir_tiles = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
        self.reclasslist = self.createReclassifyList()





    def createReclassifyList(self):
        # this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value
		engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
		query = 'SELECT "Value", mtr from pre.traj_cdl' + self.res + '_b_' + self.datarange + ' as a JOIN pre.traj_' + self.datarange + '_lookup as b ON a.traj_array = b.traj_array'
		print 'query:', query
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




	  
def execute_task(args):
	in_extentDict, prg = args

	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	 #The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	arcpy.env.snapRaster = prg.path_parent
	arcpy.env.cellsize = prg.path_parent
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	###  Execute Region Group  #####################
	
	ras_out = Reclassify(Raster(prg.path_parent), "Value", RemapRange(prg.reclasslist), "NODATA")

	#clear out the extent for next time
	arcpy.ClearEnvironment("extent")
    
    # print fc_count
	outname = "tile_" + str(fc_count) +'.tif'

	#create Directory

	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	ras_out.save(outpath)




def mosiacRasters(prg):
	# root_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\tiles\\'
	# roo
	tilelist = glob.glob(prg.dir_tiles+"*.tif")
	print 'mtr tilelist:', tilelist  

	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, prg.gdb_child, prg.raster_child, Raster(prg.path_parent).spatialReference, prg.pixel_type, prg.res, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(prg.path_child, "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(prg.path_child)



def run(series, res, mmu, years, subtype, gdb_parent, parent_seq, gdb_child, child_seq):  
	#instantiate the class inside run() function
	prg = ProcessingObject(series, res, mmu, years, subtype, gdb_parent, parent_seq, gdb_child, child_seq)

	# need to create a unique fishnet for each dataset
	#create_fishnet()

	#####  remove a files in tiles directory
	tiles = glob.glob(prg.dir_tiles+"*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(prg.out_fishnet, ["SHAPE@"]):
		extent_curr = row[0].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[count] = ls
		count+=1
    
	# print 'extDict', extDict
	# print'extDict.items()',  extDict.items()

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=cpu_count())
	pool.map(execute_task, [(ed, prg) for ed in extDict.items()])
	pool.close()
	pool.join

	mosiacRasters(prg)
    