import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import fiona
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

arcpy.CheckOutExtension("Spatial")


def aggregateFlow(dict_aggregate):
	###block stats
	print('---- running aggregateFlow ------')

	print('---- block stats ------')
	nbr = NbrAnnulus(dict_aggregate['cellsize'], dict_aggregate['cellsize'], "CELL")
	outBlockStat = BlockStatistics(in_raster=dict_aggregate['intraster'], neighborhood=nbr, statistics_type="MAJORITY", ignore_nodata="DATA")
	outBlockStat.save(dict_aggregate['outraster'])



	# print('---- aggregate ------')
	# ###aggregate function
	# outAggreg = Aggregate(in_raster=outBlockStat, cell_factor=dict_aggregate['cellsize'], aggregation_type="MEDIAN", extent_handling="EXPAND", ignore_nodata="DATA")
	# outAggreg.save(dict_aggregate['outraster'])




def main(dict_copyraster, dict_aggregate):

	###convert to smallest pixel-type it can
	print('---- running CopyRaster_management ------')
	# arcpy.CopyRaster_management(in_raster=dict_copyraster['inraster'], out_rasterdataset=dict_copyraster['outraster'], pixel_type=dict_copyraster['pixel_type'], format=dict_copyraster['format'])

	####create the aggregated dataset
	aggregateFlow(dict_aggregate)





####create the dictionary of parameters for reformatting raster
dict_copyraster={
			'inraster':'D:\\intactland\\intact_compare\\intact_compare.gdb\\intact_compare_region',
			'outraster':'D:\\intactland\\intact_compare\\tiffs\\intact_compare_region_4bit_t2.tif',
			'pixel_type':'4_BIT',
			'format':'TIFF'
			}


####create the dictionary of parameters for first block stats and thenaggregation of raster
dict_aggregate={
			'intraster':'D:\\intactland\\intact_compare\\tiffs\\intact_compare_region_4bit_t2.tif',
			'outraster':'D:\\intactland\\intact_compare\\tiffs\\intact_compare_region_4bit_bs300m_t3.tif',
			'cellsize':10
			}


main(dict_copyraster, dict_aggregate)





