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
	arcpy.env.workspace = gdb
	# Execute AddField twice for two new fields
	fields = [f.name for f in arcpy.ListFields(table)]

	# converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(table,fields)
	print arr

	# # convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)

	df.columns = map(str.lower, df.columns)
	print 'df-----------------------', df

	df['value'] = df['value'].astype(int)

	df = df.loc[(df['value']>0) & (df['value'] <100)]
	df['count'] = df['count'].astype(int)




	df.to_csv('C:\\Users\\Bougie\\Desktop\\current_stats.csv')

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

























if __name__ == '__main__':
	print ('----------------this is the main function----------------------------------')

	gdb = 'I:\\d_drive\\projects\\usxp\\series\\s35\\maps\\conversion\\conversion.gdb'
	table = 's35_loss_perc_w_agg9km_int'
	##### get weighted average of slope ########################
	getStats(gdb=gdb, table=table)






