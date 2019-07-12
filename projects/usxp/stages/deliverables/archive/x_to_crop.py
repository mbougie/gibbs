# import arcpy
# from arcpy import env
# from arcpy.sa import *
# import multiprocessing
# from sqlalchemy import create_engine
# import pandas as pd
# import psycopg2
# import os
# import glob
# import sys
# import time
# import logging
# from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
# # sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# # import general as gen
# import json
# import general_deliverables as gen_dev





# def processingCluster(inraster, outraster, reclasslist):
# 	#####  reclass  ####################################################
# 	## Reclassify (in_raster, reclass_field, remap, {missing_values})
# 	outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
# 	print 'finished outReclass'


# 	######  block stats  ###############################################
# 	##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})

# 	nbr = NbrRectangle(100, 100, "CELL")
# 	outBlockStat = BlockStatistics(outReclass, nbr, "SUM", "DATA")
# 	print 'finished block stats'




# 	######  resample  ###############################################
# 	##Resample_management (in_raster, out_raster, {cell_size}, {resampling_type})

# 	arcpy.Resample_management(outBlockStat, outraster, "3000 3000", "NEAREST")











# def styleRaster(inraster):
# 	##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
# 	arcpy.AddField_management(in_table=inFeatures, field_name='percent', field_type='FLOAT')

# 	cur = arcpy.UpdateCursor(inraster)

# 	for row in cur:
# 		row.setValue('percent', ((float(row.getValue('Value'))/10000)*100))
# 		cur.updateRow(row)

















# def run():
# 	inraster = Raster('D:\\projects\\usxp\\deliverables\\s25.gdb\\s25_bfc')

# 	yxc_dict = {'forest':[[63,1],[141,1],[142,1],[143,1]], 'wetland':[[83,1],[87,1],[190,1],[195,1]], 'grassland':[[37,1],[62,1],[171,1],[176,1],[181,1]], 'shrubland':[[64,1],[65,1],[131,1],[152,1]]}

# 	for key, value in yxc_dict.iteritems():
# 		print key
# 		print value

# 		outraster = 'D:\\projects\\usxp\\deliverables\\xtocropland\\xtocropland.gdb\\s25_{}_bs200_rs3km'.format(key)
		
# 		# processingCluster(inraster, outraster, value)


# 		##add column to raster attribute table
# 		styleRaster(outraster)

# run()







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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
# import general_deliverables as gen_dev





def processingCluster(instance, inraster, outraster, reclasslist):
	#####  reclass  ####################################################
	## Reclassify (in_raster, reclass_field, remap, {missing_values})

	outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
	print 'finished outReclass-------------------'


	######  block stats  ###############################################
	##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})

	# for key, value in instance['scale'].iteritems():

	# 	nbr = NbrRectangle(value, value, "CELL")
	# 	outBlockStat = BlockStatistics(outReclass, nbr, "SUM", "DATA")
	# 	print 'finished block stats.............'
	# 	outBlockStat.save(outraster)


	# 	addField(outraster, value)

		
	# 	# addfield_percentbin()


	# 	gen.buildPyramids_new(outraster, 'NEAREST')





def addField(raster, value):
	normalizer = value*value
	##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
	arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

	cur = arcpy.UpdateCursor(raster)

	for row in cur:
		row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
		cur.updateRow(row)




# def addfield_percentbins(raster, value):
# 	normalizer = value*value
# 	##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
# 	arcpy.AddField_management(in_table=raster, field_name='percent_bin', field_type='Short')

# 	cur = arcpy.UpdateCursor(raster)

	

# 	for row in cur:
# 		row.setValue('percent_bin', ((float(row.getValue('Value'))/normalizer)*100))
# 		cur.updateRow(row)




def main(instance):
	inraster=Raster('D:\\projects\\usxp\\deliverables\\{0}\\{0}.gdb\\{0}_bfc'.format(instance['series']))

	for key, reclasslist in instance['reclass'].iteritems():
		for scale in instance['scale'].keys():
			outraster = 'D:\\projects\\usxp\\deliverables\\maps\\x_to_crop\\x_to_crop.gdb\\{0}_{1}_{2}'.format(instance['series'], scale, key)

			print outraster

			processingCluster(instance, inraster, outraster, reclasslist)








scale_dict = {'3km':100, '6km':200, '9km':300}


instance = {'series':'s35', 'scale':{'3km':100}, 'reclass':{'forest':[[63,1],[141,1],[142,1],[143,1]], 'wetland':[[83,1],[87,1],[190,1],[195,1]], 'grassland':[[37,1],[62,1],[171,1],[176,1],[181,1]], 'shrubland':[[64,1],[65,1],[131,1],[152,1]]} }
print instance

main(instance)




















