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
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general as gen
import json
import general_deliverables as gen_dev





def processingCluster(inraster, outraster, reclasslist):
	#####  reclass  ####################################################
	## Reclassify (in_raster, reclass_field, remap, {missing_values})
	outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
	print 'finished outReclass-------------------'


	######  block stats  ###############################################
	##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})

	nbr = NbrRectangle(100, 100, "CELL")
	outBlockStat = BlockStatistics(outReclass, nbr, "SUM", "DATA")
	print 'finished block stats------------------'




	######  resample  ###############################################
	##Resample_management (in_raster, out_raster, {cell_size}, {resampling_type})

	arcpy.Resample_management(outBlockStat, outraster, "3000 3000", "NEAREST")








def run():
	inraster = Raster('D:\\projects\\usxp\\deliverables\\s25.gdb\\s25_bfc')

	yxc_dict = {'forest':[[63,1],[141,1],[142,1],[143,1]], 'wetland':[[83,1],[87,1],[190,1],[195,1]], 'grassland':[[37,1],[62,1],[171,1],[176,1],[181,1]], 'shrubland':[[64,1],[65,1],[131,1],[152,1]]}

	for key, value in yxc_dict.iteritems():
		print key
		print value

		outraster = 'D:\\projects\\usxp\\deliverables\\xtocropland\\xtocropland.gdb\\s25_{}_bs200_rs3km'.format(key)
		
		processingCluster(inraster, outraster, value)



run()











