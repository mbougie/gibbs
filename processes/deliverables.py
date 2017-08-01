from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import collections
from collections import namedtuple

import arcpy
from arcpy import env
from arcpy.sa import *
# import glob
import psycopg2




try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



arcpy.CheckOutExtension("Spatial")



#########  global variables   ################
#acccounts for different machines having different cases in path
case=['Bougie','Gibbs']

#conversion coefficent: 1 square meter = 0.000247105 acres
conv_coef=0.000247105

#function to establish path to geodatabase
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 






########  Part A Questions  #################################################
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
	arcpy.env.workspace = defineGDBpath(['deliverables','xp_update_refined'])

	wc='ytc'
	print 'wc: ', wc

	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = wc

		# Set Snap Raster environment
		arcpy.env.snapRaster = wc

		lcc=defineGDBpath(['ancillary','misc'])+'LCC_100m'

		years=['13', '14', '15']
		for year in years:
			output = defineGDBpath(['deliverables','xp_update_refined'])+'ytc_'+year+'_lcc'
			print 'output: ', output

			cond="Value = " + year
			print 'cond', cond

			outSetNull = SetNull(raster, Raster(lcc), cond)

			#Save the output 
			outSetNull.save(output)






















########  Part B Questions  #################################################
def createGSdataset(gdb_path,wc,outfile):
	#DESCRIPTION: subset bfc datasets to grass and shrub
	arcpy.env.workspace = defineGDBpath(gdb_path)

	for raster in arcpy.ListDatasets(wc, "Raster"): 
		
		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster

		output = defineGDBpath(['deliverables','deliverables_refined'])+outfile
		print 'output: ', output

		cond='Value = 37 OR Value = 62 OR Value = 64 OR Value = 152 OR Value = 171 OR Value = 181 OR Value = 176'
		print 'cond', cond

		attExtract = ExtractByAttributes(raster, cond) 

		# Save the output 
		attExtract.save(output)




def createGSBylcc(wc):
	# DESCRIPTION: replace the gsconv value with lcc value

	arcpy.env.workspace =  defineGDBpath(['deliverables','deliverables_refined'])

	print 'wc: ', wc

	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster
        
        #get the lcc dataset
		lcc=defineGDBpath(['ancillary','misc'])+'LCC_100m'

		output = raster+'_lcc'
		print 'output: ', output

		cond = "Value IS NULL"
        print cond
        
        #if values is null keep it null else replace all values of gs_2012 with lcc
        OutRas=Con(raster, raster, Raster(lcc), cond)

        # # Save the output 
        OutRas.save(output)





def clip2mtr():
	# DESCRIPTION: replace the gsconv value with lcc value

	arcpy.env.workspace =  defineGDBpath(['ancillary','data_2008_2012'])

	for raster in arcpy.ListDatasets('Multitemporal_Results_FF2', "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster
        
        #get the lcc dataset
		reference_raster=defineGDBpath(['deliverables','deliverables_refined'])+'gs_2012_lcc'

		output = reference_raster+'_mtr'
		print 'output: ', output
        
        #want to set all crop associated values to null.  Want to keep 1 = cropland and 4 = conversion from crop to noncrop
        cond = 'Value = 2 OR Value = 3 OR Value = 5'
        print 'cond: ', cond

        OutRas=SetNull(raster, Raster(reference_raster),  cond)
        # # Save the output 
        OutRas.save(output)



def createGSconvByYearANDlcc(wc, gdb_path, filename, years):

	arcpy.env.workspace = defineGDBpath(['deliverables','deliverables_refined'])

	print 'wc: ', wc

	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster

        #set up the 2 datasets that will be used in the Con() function below
		yr_dset=Raster(defineGDBpath(gdb_path)+filename)
		lcc=Raster(defineGDBpath(['ancillary','misc'])+'LCC_100m')

		
		for year in years:
			output = 'gsconv_'+year+'_lcc'
			print 'output: ', output

			print year[2:]


			cond = "Value <> " + year[2:]
			print cond

			# # using 3 rasters in this condtion. only select pixels of year x, if true get the lcc value for that pixel if fale set to null!!
			OutRas=Con((yr_dset == int(year[2:])) & raster, lcc,(SetNull(raster,raster)))

			#Save the output 
			OutRas.save(output)


	     
def tabAreaByCounty(gdb_path_env, wc, gdb_path_out):
	#description: tabulate the values of the raster by county 

	# Set environment settings
	arcpy.env.workspace = defineGDBpath(gdb_path_env)
    
	# Set local variables
	for raster in arcpy.ListDatasets('*'+wc+'*', "Raster"): 
		print 'raster: ', raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster

        #populate the parameters for tabluateArea() function:
		inZoneData = defineGDBpath(['ancillary','shapefiles'])+'counties'
		zoneField = "atlas_stco"
		inClassData = raster
		classField = "Value"
		outTable = defineGDBpath(gdb_path_out)+raster + '_counties'
		print 'outTable: ', outTable

		#get the resolution of each raster to get the correct size to process
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
		query='CREATE TABLE '+tablename+'(atlas_stco text, ' + columnname + '12 double precision, ' + columnname + '34 double precision, '+columnname+ '56 double precision, '+columnname+ '78 double precision)'

		print query
		cur.execute(query)
		conn.commit()
     
        #loop through each row and get the value for specified columns
		rows = arcpy.SearchCursor(table)
		for row in rows:

			atlas_stco = row.getValue("ATLAS_STCO")
			lcc12 = str((row.getValue('VALUE_1')+row.getValue('VALUE_2'))*(conv_coef))
			lcc34 = str((row.getValue('VALUE_3')+row.getValue('VALUE_4'))*(conv_coef))
			lcc56 = str((row.getValue('VALUE_5')+row.getValue('VALUE_6'))*(conv_coef))
			lcc78 = str((row.getValue('VALUE_7')+row.getValue('VALUE_8'))*(conv_coef))
			

			cur = conn.cursor()
			query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + lcc12 + " , " + lcc34 + " , " + lcc56+ " , " + lcc78 + ")"
			print query
			cur.execute(query)
			conn.commit()
      

def createPGTables_totalcrop(filename,year):
	#description: create a postgres table that calculates the different mtr comboninations depending on year parameter

	table = defineGDBpath(['deliverables','deliverables_refined'])+filename
	print 'table: ', table

	#define table and column names
	tablename = 'deliverables.totalcrop_'+year+'_counties'
	print 'tablename: ', tablename
	columnname = 'totalcrop_'+year
	print 'columnname: ', columnname
    
    #create table in postgres database
	cur = conn.cursor()
	query='CREATE TABLE '+tablename+'(atlas_stco text, ' + columnname + ' double precision)'
	print 'query: ', query
	cur.execute(query)
	conn.commit()


    #populate newly created table. Loop through each row and get the value for specified columns
	rows = arcpy.SearchCursor(table)
	for row in rows:

		atlas_stco=row.getValue("ATLAS_STCO")
		count = getCount(year,row)

		cur = conn.cursor()
		query="INSERT INTO "+tablename+" VALUES ('" + atlas_stco + "' , " + count + ")"
		print query
		cur.execute(query)
		conn.commit()


def getCount(year,row):
	#aux function for createTables_mtr()

	if year == '2012':
		count = str((row.getValue('VALUE_2')+row.getValue('VALUE_5'))*(conv_coef))
		return count

	elif year == '2015':
		count = str(((row.getValue('VALUE_2')+row.getValue('VALUE_5'))+(row.getValue('VALUE_3')-row.getValue('VALUE_4')))*(conv_coef))
		return count



###################  CALL FUNCTIONS  #######################################################
'''###################  SECTION A QUESTIONS  #########################################################################################################################'''
# question_a4()



'''###################  SECTION B QUESTIONS  #########################################################################################################################'''

'''description: subset bfc and old mtr datasets for grassland and shrubland  ===============================================
description: add it here 
'''


'''
[ Q1 ] ===============================================================================
purpose: Count acres by suitability(lcc 1-8) of (MTR1 + MTR3) where 2012 CDL value = shrubland or grassland
product: a table that contains lcc1-8
'''

'''
function: createGS_2012()
description: extract all shrub or grass from cdl_2012
output dataset: gs_2012 
'''
# createGSdataset(['ancillary','cdl'], '*2012*', 'gs_2012')

'''
function: createGSBylcc
description: replace the cdl value with lcc value
output dataset: gs_2012 
'''
# createGSBylcc('gs_2012')



'''
function: clip2mtr()
description: subset the gs_2012_lcc dataset by mtr
output dataset: gs_2012_lcc_mtr 
'''
# clip2mtr()


'''
function: tabAreaByCounty()
description: tabulate the values of the gsConv_gs_2012_lcc_mtr by county 
output dataset: gs_2012_lcc_mtr_counties 
'''
# tabAreaByCounty(['deliverables','deliverables_refined'], 'gs_2012_lcc_mtr', ['deliverables','deliverables_refined'])
# tabAreaByCounty(['deliverables','deliverables_refined'], 'gs_2012_lcc', ['deliverables','deliverables_refined'])
'''
function: createPGTables_mtr()
description: create a postgres table that contains lcc combinations
output postgres table: gs_2012_lcc_mtr_counties
''' 
# createPGTables_lcc('gs_2012_lcc_mtr_counties')
# createPGTables_lcc('gs_2012_lcc_counties')


'''
[ Q2 and Q3 ] ===============================================================================
purpose: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  
''' 


'''
function: createGSconv  ------------------------------------
description: subset input raster by grassland and shrubland
output datasets: gsConv_[x]
'''
# createGSdataset(['deliverables','xp_update_refined'], 'bfc', 'gsconv_new')
# createGSdataset(['ancillary','xp_initial'], 'bfc_v1', 'gsconv_old')



'''
function: createGSconvByYearANDlcc()---------------------------------------
description: subset the gsConv datasets by year and then attach lcc value
output datasets: gsConv_[year]_lcc
'''
# createGSconvByYearANDlcc('gsconv_old',['ancillary','xp_initial'], 'ytc_v1', ['2009','2010','2011','2012'])
# createGSconvByYearANDlcc('gsconv_new', ['deliverables','xp_update_refined'], 'ytc', ['2013', '2014', '2015'])


'''
,'deliverables_refined.gdb'function: tabAreaByCount()-------------------------------------------------
description: tabulate the area per county of each gsConv_[year]_lcc dataset
output datasets: gsConv_[year]_lcc_counties
'''
# tabAreaByCounty(['deliverables','deliverables_refined'],"*gsconv_20*",['deliverables','deliverables_refined'])


'''
function: createPGTables_lcc()---------------------------------------------
description: create and populate "gsConv_[year]_lcc_counties" tables in postgres
output postgres tables: gsConv_[year]_lcc_counties
'''
# createPGTables_lcc('gsconv*_lcc_counties')





'''
[ Q4 and Q5 ] ======================================================================
purpose: Calculate the totalcrop via naive change for years 2012 and 2015 
'''

'''
tabAreaByCounty(wc)
function: tabAreaByCounty() -------------------------------------------
description: tabulate the area per county for both mtr2012 and mtr2015
output tables: [mtr]_counties
'''
# tabAreaByCounty(['ancillary','xp_initial'], 'mtr_v1', ['deliverables','deliverables_refined'])
# tabAreaByCounty(['deliverables','xp_update_refined'], 'mtr', ['deliverables','deliverables_refined'])

'''
function: createPGTables_mtr() -------------------------------------------
description: create a postgres table that calculates the different mtr comboninations depending on year parameter
output postgres tables: totalcrop_[year]_counties
'''
# createPGTables_totalcrop('mtr_v1_counties','2012')
# createPGTables_totalcrop('mtr_counties','2015')




'''
[final ===========================================================================
create table deliverables.final as

SELECT
  s.atlas_name as state,
  c.atlas_stco,
  c.atlas_name as county,
  ts.gs_2012_lcc12 as suitability_lcc12,
  ts.gs_2012_lcc34 as suitability_lcc34,
  ts.gs_2012_lcc56 as suitability_lcc56,
  ts.gs_2012_lcc78 as suitability_lcc78,
  gs9.gsconv_2009_lcc12, 
  gs9.gsconv_2009_lcc34, 
  gs9.gsconv_2009_lcc56, 
  gs9.gsconv_2009_lcc78, 
  gs10.gsconv_2010_lcc12, 
  gs10.gsconv_2010_lcc34, 
  gs10.gsconv_2010_lcc56, 
  gs10.gsconv_2010_lcc78,
  gs11.gsconv_2011_lcc12, 
  gs11.gsconv_2011_lcc34, 
  gs11.gsconv_2011_lcc56, 
  gs11.gsconv_2011_lcc78,
  gs12.gsconv_2012_lcc12, 
  gs12.gsconv_2012_lcc34, 
  gs12.gsconv_2012_lcc56, 
  gs12.gsconv_2012_lcc78,
  gs13.gsconv_2013_lcc12, 
  gs13.gsconv_2013_lcc34, 
  gs13.gsconv_2013_lcc56, 
  gs13.gsconv_2013_lcc78,
  gs14.gsconv_2014_lcc12, 
  gs14.gsconv_2014_lcc34, 
  gs14.gsconv_2014_lcc56, 
  gs14.gsconv_2014_lcc78,
  gs15.gsconv_2015_lcc12, 
  gs15.gsconv_2015_lcc34, 
  gs15.gsconv_2015_lcc56, 
  gs15.gsconv_2015_lcc78,
  tc12.totalcrop_2012, 
  tc15.totalcrop_2015
   
FROM
  spatial.states as s JOIN spatial.counties as c ON s.atlas_st = c.atlas_st
  FULL JOIN deliverables.gsconv_2009_lcc_counties as gs9 ON gs9.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.gsconv_2010_lcc_counties as gs10 ON gs10.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.gsconv_2011_lcc_counties as gs11 ON gs11.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.gsconv_2012_lcc_counties as gs12 ON gs12.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.gsconv_2013_lcc_counties as gs13 ON gs13.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.gsconv_2014_lcc_counties as gs14 ON gs14.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.gsconv_2015_lcc_counties as gs15 ON gs15.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.totalcrop_2012_counties as tc12 ON tc12.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.totalcrop_2015_counties as tc15 ON tc15.atlas_stco = c.atlas_stco
  FULL JOIN deliverables.gs_2012_lcc_counties as ts ON ts.atlas_stco = c.atlas_stco


ORDER BY atlas_stco


'''



def createGSBylcc(wc):
	# DESCRIPTION: replace the gsconv value with lcc value

	arcpy.env.workspace =  defineGDBpath(['deliverables','xp_update_refined'])

	print 'wc: ', wc

	for raster in arcpy.ListDatasets(wc, "Raster"): 

		print 'raster: ',raster

		# Set the cell size environment using a raster dataset.
		arcpy.env.cellSize = raster

		# Set Snap Raster environment
		arcpy.env.snapRaster = raster
        
        #get the lcc dataset
		lcc=defineGDBpath(['ancillary','misc'])+'LCC_100m'

		output =defineGDBpath(['deliverables','deliverables_refined']) + raster+'_lcc'
		print 'output: ', output

		cond = "Value <> 3"
        print cond
        
        #if values is null keep it null else replace all values of gs_2012 with lcc
        OutRas=SetNull(raster, Raster(lcc), cond)
       
        # # Save the output 
        OutRas.save(output)




# createGSBylcc('mtr')