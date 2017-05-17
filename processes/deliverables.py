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




# try:
#     conn = psycopg2.connect("dbname='delivery' user='postgres' host='localhost' password='postgres'")
# except:
#     print "I am unable to connect to the database"



arcpy.CheckOutExtension("Spatial")
case=['bougie','gibbs']


###################  declare functions  #######################################################
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/processes/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 

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




def createGSconvBylcc(wc):

	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb

	print 'wc: ', wc

	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster

		lcc='C:/Users/bougie/Desktop/'+rootDir+'/usxp/ancillary.gdb/lcc_100m_reproject'

		output = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb+'/'+wc+'_lcc'
		print 'output: ', output

		cond = "Value IS NULL"
        print cond

        OutRas=Con(raster, raster, Raster(lcc), cond)

        # # Save the output 
        OutRas.save(output)




def createGSconvByYearANDlcc(wc,yr_dset,years):

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

	     



def tabArea(wc):
	# Set environment settings
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb
    
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
		outTable = inClassData + '_ta'

		#get the resolution of each raster to get the coorect size to process
		res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")
		print 'res: ', res
		processingCellSize = res

		# Check out the ArcGIS Spatial Analyst extension license
		arcpy.CheckOutExtension("Spatial")

		# Execute TabulateArea
		TabulateArea(inZoneData, zoneField, inClassData, classField, outTable,processingCellSize)




def createTables_ta1(wc):
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb

	for table in arcpy.ListTables(wc): 
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
		columnname = str(table)[:-4]
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

				atlas_stco = row[1]
				lcc12 = str((row[3]+row[4])*(0.000247105))
				lcc34 = str((row[5]+row[6])*(0.000247105))
				lcc56 = str((row[7]+row[8])*(0.000247105))
				lcc78 = str((row[9]+row[10])*(0.000247105))
			

				cur = conn.cursor()
				query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + lcc12 + " , " + lcc34 + " , " + lcc56+ " , " + lcc78 + ")"
				print query
				cur.execute(query)
				conn.commit()
      


def createTables_ta2(wc,year):
	arcpy.env.workspace = 'C:/Users/bougie/Desktop/'+rootDir+'/usxp/'+gdb

	for table in arcpy.ListTables(wc): 
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
		# year = fnf[1]
		# print year
		tablename = 'totalcrop.totalcrop_'+year+'_ta2'
		print 'tablename', tablename
		columnname = 'totalcrop_'+year
		print columnname

		cur = conn.cursor()
		query='CREATE TABLE '+tablename+'(atlas_stco text, ' + columnname + ' integer)'
		
		print query
		cur.execute(query)
		conn.commit()

       
        if year == '2012':
			with arcpy.da.SearchCursor(table, "*") as cur:
				for row in cur:
                    #OBJECTID, ATLAS_STCO, VALUE_1, VALUE_2, VALUE_3, VALUE_4, VALUE_5

					atlas_stco = row[1]
					print (row[3]+row[6])*(0.000247105)
				

					count = str((row[3]+row[6])*(0.000247105))
			
					cur = conn.cursor()
					query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + count + ")"
					print query
					cur.execute(query)
					conn.commit()

        elif year == '2015':
			with arcpy.da.SearchCursor(table, "*") as cur:
				for row in cur:
					#OBJECTID, ATLAS_STCO, VALUE_1, VALUE_2, VALUE_3, VALUE_4, VALUE_5

					atlas_stco = row[1]
					print ((row[3]+row[6])+(row[4]-row[5]))*(0.000247105)


					count = str(((row[3]+row[6])+(row[4]-row[5]))*(0.000247105))

					cur = conn.cursor()
					query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + count + ")"
					print query
					cur.execute(query)
					conn.commit()




      





###########  subset bfc datasets to grass and shrub  ################################################
# createGSconv('C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/post/ytc.gdb/ytc_bfc_mosaic_traj_n8h_mtr_8w_msk45_nbl_fnl','gsConv_new')
# createGSconv('C:/Users/bougie/Desktop/'+rootDir+'/usxp/ancillary.gdb/class_before_crop','gsConv_old')
# createGSconv('D:/gibbs/production/rasters/pre/cdl/2012_30m_cdls.img','gs2012')

###########  subset the gsConv datasets by year and then attach lcc value  ###########################
# createGSconvByYearANDlcc('gsConv_new','C:/Users/bougie/Desktop/'+rootDir+'/'+production_type+'/rasters/post/ytc.gdb/ytc_b_mosaic_traj_n8h_mtr_8w_msk45_nbl_fnl',['2013','2014','2015'])
# createGSconvByYearANDlcc('gsConv_old','D:/gibbs/control/raster/2008_2012_data/ytc_ff2.tif',['2009','2010','2011','2012'])
# createGSconvBylcc('gs2012')

###########  Execute TabulateArea  ###################################################################
# tabArea("gs2012_lcc")

###########  create/populate tables in postgres  ###################################################################
# createTables_ta1('*_lcc_ta1')
# createTables_ta2('*2015_ta2','2015')











