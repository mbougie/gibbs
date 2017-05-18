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
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



arcpy.CheckOutExtension("Spatial")
case=['Bougie','Gibbs']


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




def createGSconv(gdb_path,wc,outfile):
	#DESCRIPTION: subset bfc datasets to grass and shrub
	arcpy.env.workspace = defineGDBpath(gdb_path)

	for raster in arcpy.ListDatasets(wc, "Raster"): 
		
		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster

		output = defineGDBpath(['deliverables','deliverables'])+outfile
		print 'output: ', output

		cond='Value = 37 OR Value =62 OR Value = 64 OR Value =152 OR Value =171 OR Value =181 OR Value =176'
		print 'cond', cond

		attExtract = ExtractByAttributes(raster, cond) 

		# Save the output 
		attExtract.save(output)




def createGSconvBylcc(wc):
	# DESCRIPTION: replace the gsconv value with lcc value

	arcpy.env.workspace = defineGDBpath(['deliverables','deliverables'])

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




def createGSconvByYearANDlcc(wc,years):

	arcpy.env.workspace = defineGDBpath(['deliverables','deliverables_refined'])

	print 'wc: ', wc

	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster

        #set up the 2 datasets that will be used in the Con() function below
		yr_dset=defineGDBpath(['deliverables','xp_update_refined'])+'ytc'
		lcc=defineGDBpath(['ancillary','other'])+'LCC_100m'

		
		for year in years:
			output = 'gsConv_'+year+'_lcc'
			print 'output: ', output

			print year[2:]

			# using 3 rasters in this condtion. only select pixels of year x, if true get the lcc value for that pixel if fale set to null!!
			OutRas=Con((Raster(yr_dset) == int(year[2:])), Raster(lcc),(SetNull(raster, raster,  "Value > 8")))
			# OutRas=Con((Raster(yr_dset) == int(year)), Raster(lcc),raster)

			#Save the output 
			OutRas.save(output)

	     



def tabAreaByCounty(wc):
	# Set environment settings
	arcpy.env.workspace = defineGDBpath(['deliverables','deliverables_refined'])
    
	# Set local variables
	for raster in arcpy.ListDatasets(wc, "Raster"): 
		print 'raster: ', raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster


		inZoneData = defineGDBpath(['ancillary','shapefiles'])+'counties'
		zoneField = "atlas_stco"
		inClassData = raster
		classField = "Value"
		outTable = inClassData + '_counties'

		#get the resolution of each raster to get the coorect size to process
		res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")
		print 'res: ', res
		processingCellSize = res

		# Check out the ArcGIS Spatial Analyst extension license
		arcpy.CheckOutExtension("Spatial")

		# Execute TabulateArea
		TabulateArea(inZoneData, zoneField, inClassData, classField, outTable,processingCellSize)


def createPGTables_lcc(wc):
	arcpy.env.workspace = defineGDBpath(['deliverables','deliverables_refined'])

	for table in arcpy.ListTables(wc): 
		print 'table: ', table
	    
	    #define table and column names
		tablename = 'deliverables.'+table
		print 'tablename', tablename
		columnname = table.replace('_counties','')
		print columnname

		cur = conn.cursor()
		query='CREATE TABLE '+tablename+'(atlas_stco text, ' + columnname + '12 integer, ' + columnname + '34 integer, '+columnname+ '56 integer, '+columnname+ '78 integer)'

		print query
		cur.execute(query)
		conn.commit()
     
        #loop through each row and get the value for specified columns
		rows = arcpy.SearchCursor(table)
		for row in rows:

			atlas_stco = row.getValue("ATLAS_STCO")
			lcc12 = str((row.getValue('VALUE_1')+row.getValue('VALUE_2'))*(0.000247105))
			lcc34 = str((row.getValue('VALUE_3')+row.getValue('VALUE_4'))*(0.000247105))
			lcc56 = str((row.getValue('VALUE_5')+row.getValue('VALUE_6'))*(0.000247105))
			lcc78 = str((row.getValue('VALUE_7')+row.getValue('VALUE_8'))*(0.000247105))
			

			cur = conn.cursor()
			query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + lcc12 + " , " + lcc34 + " , " + lcc56+ " , " + lcc78 + ")"
			print query
			cur.execute(query)
			conn.commit()
      

def createPGTables_mtr(year):
	arcpy.env.workspace = defineGDBpath(['pre','binaries'])
    wc='*'+year+'*'
	for table in arcpy.ListTables(wc): 
		print 'table: ', table

		#define table and column names
		tablename = 'deliverables.totalcrop_'+year+'_counties'
		print 'tablename: ', tablename
		columnname = 'totalcrop_'+year
		print 'columnname: ', columnname
        
        #execute quesry to postgres db
		cur = conn.cursor()
		query='CREATE TABLE '+tablename+'(atlas_stco text, ' + columnname + ' integer)'
		print 'query: ', query
		cur.execute(query)
		conn.commit()


        #loop through each row and get the value for specified columns
		rows = arcpy.SearchCursor(table)
		for row in rows:

			atlas_stco=row.getValue("ATLAS_STCO")
			count = getCount(year,row)

			cur = conn.cursor()
			query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + count + ")"
			print query
			cur.execute(query)
			conn.commit()


       
   #      if year == '2012':
			# with arcpy.da.SearchCursor(table, "*") as cur:
			# 	for row in cur:
   #                  #OBJECTID, ATLAS_STCO, VALUE_1, VALUE_2, VALUE_3, VALUE_4, VALUE_5

			# 		atlas_stco = row[1]
			# 		print (row[3]+row[6])*(0.000247105)
				

			# 		count = str((row[3]+row[6])*(0.000247105))
			
			# 		cur = conn.cursor()
			# 		query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + count + ")"
			# 		print query
			# 		cur.execute(query)
			# 		conn.commit()

   #      elif year == '2015':
			# with arcpy.da.SearchCursor(table, "*") as cur:
			# 	for row in cur:
			# 		#OBJECTID, ATLAS_STCO, VALUE_1, VALUE_2, VALUE_3, VALUE_4, VALUE_5

			# 		atlas_stco = row[1]
			# 		print ((row[3]+row[6])+(row[4]-row[5]))*(0.000247105)


			# 		count = getCount(year,row)

			# 		cur = conn.cursor()
			# 		query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + count + ")"
			# 		print query
			# 		cur.execute(query)
			# 		conn.commit()


def getCount(year):
	if year == '2012':
		# count = str((row[3]+row[6])*(0.000247105))
		count = str((row.getValue()+row.getValue())*(0.000247105))
		return count

	elif year == '2015':
		# count = str(((row[3]+row[6])+(row[4]-row[5]))*(0.000247105))
		count = str(((row.getValue()+row.getValue())+(row.getValue()-row.getValue()))*(0.000247105))
		return count





###########  subset bfc datasets to grass and shrub  ################################################
# createGSconv(['post','xp_update_refined'], 'bfc', 'gsConv_new')
# createGSconv(['ancillary','data_2008_2012'], 'class_before_crop', 'gsConv_old')
# createGSconv(['ancillary','cdl'], '2012_30m_cdls', 'gs2012')
# erase this if above code works!!!!!!!       createGSconv('D:/gibbs/production/rasters/pre/cdl/2012_30m_cdls.img','gs2012')

###########  subset the gsConv datasets by year and then attach lcc value  ###########################
# createGSconvByYearANDlcc('gsConv_new',['2013','2014','2015'])
# createGSconvByYearANDlcc('gsConv_old','D:/gibbs/control/raster/2008_2012_data/ytc_ff2.tif',['2009','2010','2011','2012'])
# createGSconvBylcc('gs2012')

###########  Execute TabulateArea  ###################################################################
# tabAreaByCounty("*lcc")

###########  create/populate tables in postgres  ###################################################################
createPGTables_lcc('*_lcc_counties')

naive_years=['2012','2015']
for year in naive_years:
	createPGTables_mtr(year)











