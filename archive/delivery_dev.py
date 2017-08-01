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




try:
    conn = psycopg2.connect("dbname='delivery' user='postgres' host='localhost' password='postgres'")
except:
    print "I am unable to connect to the database"



rootDir='gibbs'
production_type='production'
arcpy.CheckOutExtension("Spatial")

gdb='b_dev.gdb'

# env.scratchWorkspace ="C:/Users/bougie/Documents/ArcGIS/scratch.gdb"



def question_a1():
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/core/mmu.gdb'
	wc='traj_n8h_mtr_8w_msk45_nbl'
	print 'wc: ', wc

	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		output = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/a.gdb/ytc'
		print 'output: ', output

		outSetNull = SetNull(raster, 1, "Value <> 3")

		#Save the output 
		outSetNull.save(output)



####NOTE: RESET THE YEARS ARRAY !!!!!!!!!!!!!!!!!!!!!!!
def question_a4():
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/post/ytc.gdb'
	wc='ytc_b_mosaic_traj_n8h_mtr_8w_msk45_nbl_fnl'
	print 'wc: ', wc


	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = wc

		# Set Snap Raster environment
		arcpy.env.snapRaster = wc

		lcc='C:/Users/bougie/Desktop/'+rootDir+'/usxp/ancillary.gdb/lcc_100m_reproject'

		# years=['13', '14', '15']
		years=['13']
		for year in years:
			output = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/a.gdb/ytc_'+year+'_lcc'
			print 'output: ', output

			cond="Value <> " + year
			print 'cond', cond

			outSetNull = SetNull(raster, Raster(lcc), cond)

			#Save the output 
			outSetNull.save(output)









########  Part B Questions  #################################################




def createGSconv(wc,fn):
	# arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb

	# print 'wc: ', wc

	# for raster in arcpy.ListDatasets(wc, "Raster"): 

	raster = Raster(wc)

	print 'raster: ',raster

	# Set the cell size environment using a raster dataset.
	arcpy.env.cellSize = raster

	# Set Snap Raster environment
	arcpy.env.snapRaster = raster

	output = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb+'/'+fn
	print 'output: ', output

	cond='Value = 37 OR Value =62 OR Value = 64 OR Value =152 OR Value =171 OR Value =181 OR Value =176'
	print 'cond', cond

	attExtract = ExtractByAttributes(raster, cond) 

	# Save the output 
	attExtract.save(output)





def createGSconvByYearLCC(wc,yr_dset,years):

	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb

	print 'wc: ', wc

	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster

        #set up the 2 datasets that will be used in the Con() function below
		# yr_dset='C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb+'/'+fname
		lcc='C:/Users/bougie/Desktop/'+rootDir+'/usxp/ancillary.gdb/lcc_100m_reproject'

		
		for year in years:
			output = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb+'/gsConv_'+year+'_lcc'
			print 'output: ', output

			# using 3 rasters in this condtion. only select pixels of year x, if true get the lcc value for that pixel if fale set to null!!
			OutRas=Con((Raster(yr_dset) == int(year)), Raster(lcc),(SetNull(raster, raster,  "Value > 8")))
			# OutRas=Con((Raster(yr_dset) == int(year)), Raster(lcc),raster)

			#Save the output 
			OutRas.save(output)

	     



def tabArea():
	# Set environment settings
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb
    
    #select all the datasets with "_lcc" at the end of name
	wc = "*_lcc"

	# Set local variables
	for raster in arcpy.ListDatasets(wc, "Raster"): 
		print 'raster: ', raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster


		inZoneData = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/ancillary.gdb/counties'
		zoneField = "atlas_stco"
		inClassData = raster
		classField = "Value"
		outTable = inClassData + 'ta'

		#get the resolution of each raster to get the coorect size to process
		res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")
		print 'res: ', res
		processingCellSize = res

		# Check out the ArcGIS Spatial Analyst extension license
		arcpy.CheckOutExtension("Spatial")

		# Execute TabulateArea
		TabulateArea(inZoneData, zoneField, inClassData, classField, outTable,processingCellSize)




def createTables():
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb

	for table in arcpy.ListTables(): 
		print 'table: ', table
		yo = str(table)
		print yo
		print type(yo[0])
		y='this_is_a_string'
		fnf=(os.path.splitext(str(table))[0]).split("_")
		print fnf


        #define variables
		schema = fnf[0]
		print 'schema: ', schema
		year = fnf[1]
		tablename = schema.lower()+'.'+str(table)
		print 'tablename', tablename
		columnname = str(table)[:-3]
		print columnname

		cur = conn.cursor()
		query='CREATE TABLE '+tablename+'(atlas_stco text, ' + columnname + '12 integer, ' + columnname + '34 integer, '+columnname+ '56 integer, '+columnname+ '78 integer)'
		# query='CREATE TABLE '+tablename+'(atlas_stco text, ' + columnname + '12 integer, ' + columnname + '34 integer, '+columnname+ '56 integer)'
		print query
		cur.execute(query)
		conn.commit()

		with arcpy.da.SearchCursor(table, "*") as cur:
			for row in cur:
				print row
				print row[2]
                if year = 
				atlas_stco = row[1]
				lcc12 = str((row[2]+row[3])*(0.000247105))
				lcc34 = str((row[3]+row[4])*(0.000247105))
				lcc56 = str((row[5]+row[6])*(0.000247105))
				# lcc78 = row[7]+row[8]

				cur = conn.cursor()
				query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + lcc12 + " , " + lcc34 + " , " + lcc56  + ")"
				print query
				cur.execute(query)
				conn.commit()
      




#####  call functions  ###################
createTables()



