import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from numpy import copy
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


def addGDBTable2postgres(gdb, pgdb, schema, inraster):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))


    arcpy.env.workspace = gdb
    
    # # path to the table you want to import into postgres
    # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(inraster)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(inraster,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df

    # total=df['count'].sum()

    # print df.describe() 

    # hist = df.hist(bins=3)
    
    # # # use pandas method to import table into psotgres
    df.to_sql(inraster, engine, schema=schema)
    
    # # #add trajectory field to table
    # addAcresField('counts_yxc', data['post'][yxc]['filename'], yxc, 30, total)




def appyValuesToMTR(schema, inraster, reclassed_raster):
	arcpy.env.workspace = 'D:\\projects\\usxp\\series\\s35\\maps\\{0}\\{0}.gdb'.format(schema)

	cond = "Value <> 3" 
	outraster = SetNull('D:\\projects\\usxp\\series\\s35\\s35.gdb\\s35_mtr', inraster, cond)

	outraster.save(reclassed_raster)

	gen.buildPyramids(reclassed_raster)




# def blockStats(schema, inraster, cellsize):
# 	arcpy.env.workspace = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{0}.gdb'.format(schema)
	
# 	nbr = NbrRectangle(cellsize, cellsize, "CELL")
# 	outBlockStat = BlockStatistics(inraster, nbr, "MAJORITY", "DATA")
# 	outAggreg = Aggregate(outBlockStat, cellsize, "MAXIMUM", "TRUNCATE", "DATA")
# 	outBlockStat=None

# 	output_raster = "{}_bs3km".format(inraster)
# 	outAggreg.save(output_raster)
# 	gen.buildPyramids(output_raster)



def aggregateFct(in_raster, mapname, cellsize, agg_type, bs_label):
	arcpy.env.workspace = 'D:\\projects\\usxp\\series\\s35\\maps\\{0}\\{0}.gdb'.format(mapname)
	
	out_agg = Aggregate(in_raster=in_raster, cell_factor=cellsize, aggregation_type=agg_type, extent_handling="TRUNCATE", ignore_nodata="DATA")

	output_raster = "{}_{}_{}".format('s35_slope_null', bs_label, agg_type)

	out_agg.save(output_raster)
	# gen.buildPyramids(output_raster)







def weightedMedian(data, weights):
    """
    Args:
      data (list or numpy.array): data
      weights (list or numpy.array): weights
    """
    data, weights = np.array(data).squeeze(), np.array(weights).squeeze()
    s_data, s_weights = map(np.array, zip(*sorted(zip(data, weights))))
    midpoint = 0.5 * sum(s_weights)
    if any(weights > midpoint):
        w_median = (data[weights == np.max(weights)])[0]
    else:
        cs_weights = np.cumsum(s_weights)
        idx = np.where(cs_weights <= midpoint)[0][-1]
        if cs_weights[idx] == midpoint:
            w_median = np.mean(s_data[idx:idx+2])
        else:
            w_median = s_data[idx+1]
    return w_median





def weighted_quantile(values, quantiles, sample_weight=None, 
                      values_sorted=False, old_style=False):
    """ Very close to numpy.percentile, but supports weights.
    NOTE: quantiles should be in [0, 1]!
    :param values: numpy.array with data
    :param quantiles: array-like with many quantiles needed
    :param sample_weight: array-like of the same length as `array`
    :param values_sorted: bool, if True, then will avoid sorting of
        initial array
    :param old_style: if True, will correct output to be consistent
        with numpy.percentile.
    :return: numpy.array with computed quantiles.
    """
    values = np.array(values)
    quantiles = np.array(quantiles)
    if sample_weight is None:
        sample_weight = np.ones(len(values))
    sample_weight = np.array(sample_weight)
    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), \
        'quantiles should be in [0, 1]'

    if not values_sorted:
        sorter = np.argsort(values)
        values = values[sorter]
        sample_weight = sample_weight[sorter]

    weighted_quantiles = np.cumsum(sample_weight) - 0.5 * sample_weight
    if old_style:
        # To be convenient with numpy.percentile
        weighted_quantiles -= weighted_quantiles[0]
        weighted_quantiles /= weighted_quantiles[-1]
    else:
        weighted_quantiles /= np.sum(sample_weight)
    return np.interp(quantiles, weighted_quantiles, values)




def weight_array(ar, weights):
     zipped = zip(ar, weights)
     weighted = []
     for i in zipped:
         for j in range(i[1]):
             weighted.append(i[0])
     return weighted





 

def getStats(gdb, table):
	currentobject = '{}//{}'.format(gdb,table)
	# Execute AddField twice for two new fields
	fields = [f.name for f in arcpy.ListFields(currentobject)]

	# converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(currentobject,fields)
	print arr

	# # convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)

	df.columns = map(str.lower, df.columns)
	print 'df-----------------------', df

	df['value'] = df['value'].astype(int)
	df['count'] = df['count'].astype(int)

	# # using formula
	# wm_formula = (df['value']*df['count']).sum()/df['count'].sum()
	# print('wm_formula:', wm_formula)
	 
	# # using numpy average() method
	# wm_numpy = np.average(df['value'], weights=df['count'])
	# print('wm_numpy:', wm_numpy)


	   #  average = numpy.average(values, weights=weights)
	# # Fast and numerically precise:
	# variance = numpy.average((df['value']-average)**2, weights=df['count'])

	"""
	Return the weighted average and standard deviation.

	values, weights -- Numpy ndarrays with the same shape.
	"""
	weighted_mean = np.average(df['value'], weights=df['count'])
	print('weighted_mean:', weighted_mean)

	weighted_variance = np.average((df['value']-weighted_mean)**2, weights=df['count'])
	print ('weighted_sd:', math.sqrt(weighted_variance))






# )	median = np.median(df['value'], weights=df['count'])
# 	print('median:', median






	weighted_median = weightedMedian(data=df['value'], weights=df['count'])
	print('weighted 0.5 quantile (median):', weighted_median)

	print('weighted 0.95 quantile:', 15)









	yo = weighted_quantile(values=df['value'], quantiles=[0.0, 0.5, 0.95], sample_weight=df['count'], values_sorted=False, old_style=False)
	print('weighted 0.5 quantile (median)---yo:', yo)







	hi = np.percentile(weight_array(ar=df['value'], weights=df['count']), 5)
	print('5', hi)
	hi = np.percentile(weight_array(ar=df['value'], weights=df['count']), 50)
	print('50', hi)
	hi = np.percentile(weight_array(ar=df['value'], weights=df['count']), 95)
	print('95', hi)

#############################################################################################
####################  parrell processing code ###############################################
#############################################################################################




def createReclassifyList(query):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	print query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[]
	for index, row in df.iterrows():
	    templist=[int(row[0]), int(row[1])]
	    fulllist.append(templist)
	# print fulllist
	return fulllist




###NOTE STILL HAVE TO DEAL WITH YFC IN QUERY BELOW  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def createReclassifyList_v2(conn, query):
	cur = conn.cursor()

	cur.execute(query)

	# fetch all rows from table
	rows = cur.fetchall()
	# print rows
	# print 'number of records in lookup table', len(rows)
	return rows






def createReclassifyDict(query):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	print query
	df = pd.read_sql_query(query, con=engine)
	print df
	d = {255:255}
	for index, row in df.iterrows():
		d[int(row[0])] = float(row[1])
	# print d
	return d




# def execute_task(in_extentDict):
def execute_task(args):
	in_extentDict, reclass_list, in_raster, schema = args

	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]
	
	#set environments
	cdl30_2017=Raster('E:\\data\\gSSURGO\\gSSURGO_CONUS_10m.gdb\\MapunitRaster_30m')
	arcpy.env.snapRaster = cdl30_2017
	# arcpy.env.cellsize = 30
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	#NOTE: need to reference mukey_ssurgo column in the raster and NOT value otherwise weird things occur with reclass function!
	raster_reclassed = Reclassify(Raster(in_raster), "Value", RemapRange(reclass_list), "NODATA")
	
	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("D:/projects/usxp/series/s35/maps/{}/".format(schema), r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	raster_reclassed.save(outpath)
	raster_reclassed=None

	outpath=None





	# def execute_task(in_extentDict):
def execute_task_v2(args):
	in_extentDict, reclass_list, in_raster, schema, cls, rws = args

	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]
	
	#set environments
	# cdl30_2017=Raster('E:\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017')
	cdl30_2017=Raster(in_raster)

		#set environments
	arcpy.env.snapRaster = cdl30_2017
	arcpy.env.cellsize = cdl30_2017
	arcpy.env.outputCoordinateSystem = cdl30_2017	
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	a = arcpy.RasterToNumPyArray(in_raster=Raster(in_raster), lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls)
	
	outData = np.vectorize(reclass_list.get)(a)

	outname = "tile_" + str(fc_count) +'.tif'

	outpath = os.path.join("D:/projects/usxp/series/s35/maps/{}/".format(schema), r"tiles", outname)

	arcpy.ClearEnvironment("extent")

	myRaster = arcpy.NumPyArrayToRaster(outData, lower_left_corner=arcpy.Point(XMin, YMin), x_cell_size=30, y_cell_size=30, value_to_nodata=255)

	outData = None

	myRaster.save(outpath)

	myRaster = None













def mosiacRasters(schema):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("D:/projects/usxp/series/s35/maps/{}/tiles/*.tif".format(schema))
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster('E:\\data\\gSSURGO\\gSSURGO_CONUS_10m.gdb\\MapunitRaster_30m')

	filename = 'gssurgo_{}_30m'.format(schema)
	print 'filename:', filename

	gdb = 'D:\\projects\\usxp\\series\\s35\\maps\\{0}\\{0}.gdb'.format(schema)
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, gdb, filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management('{0}\\{1}'.format(gdb, filename), "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids('{0}\\{1}'.format(gdb, filename))













def run(schema, newtable):
	print ('run method-------------------')

	# conn = psycopg2.connect("dbname='usxp_deliverables' user='mbougie' host='144.92.235.105' password='Mend0ta!'")

	# print ('------------------------------------------------this is the run function------------------------------------------------------------------')
	# ##add the table to postgres
	# # addGDBTable2postgres_hydric(gdb='D:\\projects\\ksu\\control\\gSSURGO\\gSSURGO_CONUS_10m.gdb', pg_db='usxp_deliverables', schema=schema, table=table, fields=['mukey']+fieldlist)

	# tiles = glob.glob("D:/projects/usxp/series/s35/maps/{}/tiles/*".format(schema))
	# for tile in tiles:
	# 	os.remove(tile)


	# # in_raster = 'D:\\projects\\usxp\\series\\s35\\maps\\slope\\slope.gdb\\MapunitRaster_30m'
	# in_raster = 'D:\\projects\\usxp\\series\\s35\\maps\\slope\\slope.gdb\\MapunitRaster_30m_null_to_255'

	# # query="SELECT mukey, slopegradwta FROM slope.muaggatt_v2 WHERE slopegradwta IS NOT NULL"
	# query="SELECT mukey, slopegradwta FROM slope.muaggatt"
	# # reclass_list = createReclassifyList_v2(conn, query)
	# reclass_list = createReclassifyDict(query)
	# print reclass_list[2566847]

	# fishnet = 'fishnet_ggsurgo_71_5'

	# cls = 2169
	# rws = 19351

	# #get extents of individual features and add it to a dictionary
	# extDict = {}
	# # for row in arcpy.da.SearchCursor('E:\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\{}'.format(fishnet), ["oid","SHAPE@"]):
	# for row in arcpy.da.SearchCursor('D:\\projects\\usxp\\series\\s35\\maps\\slope\\slope.gdb\\{}'.format(fishnet), ["oid","SHAPE@"]):
	# 	atlas_stco = row[0]
	# 	print atlas_stco
	# 	extent_curr = row[1].extent
	# 	ls = []
	# 	ls.append(extent_curr.XMin)
	# 	ls.append(extent_curr.YMin)
	# 	ls.append(extent_curr.XMax)
	# 	ls.append(extent_curr.YMax)
	# 	extDict[atlas_stco] = ls

	# print 'extDict', extDict
	# print'extDict.items',  extDict.items()

	# # execute_task_v2(extDict.items, reclass_list, in_raster, schema, cls, rws, **kwargs)
	# #######create a process and pass dictionary of extent to execute task
	# pool = Pool(processes=6)
	# pool.map(execute_task_v2, [(ed, reclass_list, in_raster, schema, cls, rws) for ed in extDict.items()])
	# pool.close()
	# pool.join

	# conn.close ()

	# mosiacRasters(schema)


	# # ## setnull function to reclass mtr3 with nicc values!!!
	# setRasterNull(schema, newtable)


	# ####note: this function isnt working so had to do it this in the gui
	# blockStats(schema, newtable, 100)











if __name__ == '__main__':
	print ('----------------this is the main function----------------------------------')

	######  define parameters  ###################################
	schema='slope'
	inraster='gssurgo_slopegradwta_30m_null'
	reclassed_raster='s35_slope_null'



	## clip slope raster to the mtr3 dataset ########
	# appyValuesToMTR(schema, inraster, reclassed_raster)

	### setblockstats!!!
	# aggregateFct(in_raster='s35_slope_null', mapname='slope', cellsize=100, agg_type='mean', bs_label='bs3km')



	### export raster attribute table to postgres
	# addGDBTable2postgres(reclassed_raster)


	dataset = 'hydric'
	typecrop = 'newcrop'
	##### get weighted average of slope ########################
	getStats(gdb='I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\{0}\\{0}.gdb'.format(dataset), table='s35_{0}_{1}'.format(dataset,typecrop))


	# run(schema='slope', newtable='s35_slope')







# a = np.array([[1,2,3],[7,7,7],[3,2,4]])
# my_dict = {1:23, 2:34, 3:36, 4:45, 5:11}
# b = np.vectorize(my_dict.get)(a)
# print(b)








	################  sandbox #######################################################
	# addGDBTable2postgres(gdb='D:\\projects\\usxp\\series\\s35\\maps\\hydric\\hydric.gdb', pgdb='usxp_deliverables', schema='hydric', inraster='s35_hydric_newcrop')