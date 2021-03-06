import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import webcolors as wc
import palettable
# import matplotlib as mpl

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
# import replace_61_w_hard_crop


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 






def createReclassifyList():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	query = " SELECT mukey, niccdcd from suitability.muaggatt WHERE niccdcd <> '0' "
	# print 'query:', query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[int(row[0]), int(row[1])]
	    # initial=row[0] 
	    # new=row[1]  
	    # templist.append(int(initial))
	    # templist.append(int(new))
	    fulllist.append(templist)
	# print 'fulllist: ', fulllist
	return fulllist


  
# def execute_task(in_extentDict):
def execute_task(args):
	in_extentDict, reclass_list, in_raster = args

	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	cdl30_2017=Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017')
	arcpy.env.snapRaster = cdl30_2017
	# arcpy.env.cellsize = 30
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)


	raster_reclassed = Reclassify(Raster(in_raster), "Value", RemapRange(reclass_list), "NODATA")
	nbr = NbrRectangle(3, 3, "CELL")
	outBlockStat = BlockStatistics(raster_reclassed, nbr, "MAJORITY", "DATA")
	raster_reclassed=None
	outAggreg = Aggregate(outBlockStat, 3, "MAXIMUM", "TRUNCATE", "DATA")
	outBlockStat=None


	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("D:/projects/usxp/deliverables/maps/suitability/", r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	outAggreg.save(outpath)
	outAggreg=None

	outpath=None






def mosiacRasters():
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("D:/projects/usxp/deliverables/maps/suitability/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017')

	filename = 'gssurgo_nicc_30m'
	print 'filename:', filename

	gdb = 'D:\\projects\\usxp\\deliverables\\maps\\suitability\\suitability.gdb'
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, gdb, filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management('{0}\\{1}'.format(gdb, filename), "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids('{0}\\{1}'.format(gdb, filename))






def addGDBTable2postgres():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
    
    # # path to the table you want to import into postgres
    # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'
    raster = 'D:\\projects\\usxp\\deliverables\\maps\\suitability\\suitability.gdb\\suitability'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(raster)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(raster,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df

    schema = 'suitability'
    tablename = 'suitability'


    # df[].sum(axis=1)
    
    # # # # use pandas method to import table into psotgres
    # df.to_sql(tablename, engine, schema=schema)
    
    # # # #add trajectory field to table
    # addAcresField(schema, tablename, 30)




def addAcresField(schema, tablename, res):

	try:
		conn = psycopg2.connect("dbname= 'usxp_deliverables' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
	except:
		print "I am unable to connect to the database"
	#this is a sub function for addGDBTable2postgres()

	cur = conn.cursor()

	####DDL: add column to hold arrays
	query = 'ALTER TABLE {}.{} ADD COLUMN acres bigint'.format(schema, tablename)
	print query
	cur.execute(query)

	#####DML: insert values into new array column
	cur.execute("UPDATE {0}.{1} SET acres=count*{2}".format(schema, tablename, gen.getPixelConversion2Acres(res) ))
	conn.commit() 





def getPGtable():
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

    df = pd.read_sql_query("select * from suitability.colormap_suitability WHERE hex <> 'grey'",con=engine)

    # print df

    return df







def createColorMap():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

	yo=palettable.colorbrewer.diverging.RdYlGn_11.colors
	print yo
	yo = yo[::-1]
	print yo
	print tuple(yo)
	print type(yo)

	hi = np.array(yo)
	print type(hi)


	good_objects = [True, False, True, False, True, True, True, True, False, True, True]

	meow = hi[good_objects]

	# for n in meow:
	# 	print tuple(n)

	df = getPGtable()
	pd.concat([df,pd.DataFrame(columns=list('e'))])
	print 'df----------',df
	for index, row in df.iterrows():
		print 'mewo', meow[index]

 		fulllist = np.append([row['class']], meow[index])

		df.at[index, 'e'] = ' '.join(fulllist)

		print df

		# print 'class', row['class']
		# rgb_string = ' '.join(meow[index])
		# print rgb_string
	# 	print index
	# 	print row['class'], row['hex'], row['colormap']
	# 	hi = yo[index]
	# 	print 'hi', hi
	# 	mi = map(str, hi)
	# 	mimi = [str(row['class'])] + mi
	# 	print 'yo map', mimi
	# 	rgb_list = [str(row['class']), str(wc.hex_to_rgb(str(row['hex']))[0]), str(wc.hex_to_rgb(str(row['hex']))[1]), str(wc.hex_to_rgb(str(row['hex']))[2])]
	# 	# rgb_list = [str(row['class'])]
	# 	print rgb_list

	# 	rgb_string = ' '.join(mimi)
	# # 	print rgb_string

	# # 	print df.at[index, 'colormap'] 
		# df.at[index, 'e'] = rgb_string

	# # print type(df)
	df = df['e']
	# print 'fdfdf', df
	# #  # df.to_sql(name='colormap_suitability', con=engine, schema='suitability', index=False, if_exists='replace')


	df.to_csv(path='C:\\Users\\Bougie\\Desktop\\misc\\colormaps\\suitability_RdYlGn_12_mod.clr')




def conversionRGB2HEX():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	df = pd.read_csv(filepath_or_buffer='C:\\Users\\Bougie\\Desktop\\misc\\colormaps\\suitability_RdYlGn_8.clr', delimiter=' ', header=None)
	print df
	df.drop(df.columns[0],axis=1,inplace=True)
	yo = list(df.itertuples(index=False, name=None))

	df = getPGtable()
	for index, row in df.iterrows():
		print index
		print yo[index]
		print wc.rgb_to_hex(yo[index])
		print row['class'], row['hex']

		df.at[index, 'hex'] = wc.rgb_to_hex(yo[index])

		print 'fdfdf', df
		df.to_sql(name='colormap_suitability', con=engine, schema='suitability', index=False, if_exists='replace')









def conversionHEX2RGB_2(cba):

	print cba

	cba = map(str, cba)
	print cba

	for color in cba:
		###convert elements in list from interger to string
		color = map(str, color)
		print ' '.join(color)






if __name__ == '__main__':
	print ('this is the main function')
	# run()




	# createColorMap()
	conversionRGB2HEX()
	# conversionHEX2RGB_2(palettable.colorbrewer.diverging.RdYlGn_8.colors)


	






