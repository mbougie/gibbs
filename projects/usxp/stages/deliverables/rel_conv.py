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
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
# import general as gen
# import json
# # import general_deliverables as gen_dev





# def processingCluster(instance, inraster, outraster):
# 	#####  reclass  ####################################################
# 	## Reclassify (in_raster, reclass_field, remap, {missing_values})

# 	mtr2 = Reclassify(inraster, "Value", RemapValue(instance['reclass']['mtr2']), "NODATA")
# 	mtr2_raster = outraster+'_mtr2'
# 	# mtr3.save(mtr2_raster)
# 	mtr3 = Reclassify(inraster, "Value", RemapValue(instance['reclass']['mtr3']), "NODATA")
# 	mtr3_raster = outraster+'_mtr3'
# 	# mtr3.save(mtr3_raster)
# 	print 'finished outReclass-------------------'





#     # net_raster = mtr4 - mtr3

#     # net_raster = Minus(Raster(mtr4_raster), Raster(mtr3_raster))

# 	rel_conv_raster = Divide(Raster(mtr3_raster),Plus(Raster(mtr3_raster), Raster(mtr2_raster)))
# 	print 'finished raster math------------------'

# 	rel_conv_raster.save(outraster)


# 	# ######  block stats  ###############################################
# 	##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})

# 	# for key, value in instance['scale'].iteritems():

# 	# 	nbr = NbrRectangle(value, value, "CELL")
# 	# 	outBlockStat = BlockStatistics(rel_conv_raster, nbr, "SUM", "DATA")
# 	# 	print 'finished block stats.............'
# 	# 	outBlockStat.save(outraster)


# 	# 	addField(outraster, value)


# 	# 	gen.buildPyramids_new(outraster, 'NEAREST')



# 	gen.buildPyramids_new(rel_conv_raster, 'NEAREST')




# def addField(raster, value):
# 	normalizer = value*value
# 	##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
# 	arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

# 	cur = arcpy.UpdateCursor(raster)

# 	for row in cur:
# 		row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
# 		cur.updateRow(row)






# def main(instance):
# 	inraster=Raster('D:\\projects\\usxp\\deliverables\\{0}\\{0}.gdb\\{0}_mtr'.format(instance['series']))
# 	print 'inraster', inraster

# 	# for key, reclasslist in instance['reclass'].iteritems():
# 	for scale in instance['scale'].keys():
# 		outraster = 'D:\\projects\\usxp\\deliverables\\{0}\\maps\\rel_conv\\rel_conv.gdb\\{0}_{1}'.format(instance['series'], scale)

# 		print outraster
# 		print 'outraster', outraster

# 		processingCluster(instance, inraster, outraster)






# instance = {'scale':{'3km':100}, 'series':'s26', 'reclass':{'mtr2':[[2,1]], 'mtr3':[[3,1]]} }
# main(instance)













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

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")





def processingCluster(instance, inraster, outraster):
    test = {}

    for mtr, reclasslist in instance['reclass'].iteritems():
    	print 'current mtr:', mtr
        reclass_raster = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
        print 'finished reclass_raster.............'
        reclass_raster_to_zero = Con(IsNull(reclass_raster), 0, reclass_raster)
        print 'reclass_raster_to_zero.............'
        for key, value in instance['scale'].iteritems():

			nbr = NbrRectangle(value, value, "CELL")
			outBlockStat = BlockStatistics(reclass_raster_to_zero, nbr, "SUM", "DATA")
			print 'finished block stats.............'


			outBlockStat.save(outraster+'_'+mtr)

			addField(outraster, value)   

			gen.buildPyramids_new(outraster, 'NEAREST')

			# test[mtr] = outBlockStat

			print 'test', test


    # print 'test', test

    # net_raster = Divide(test['mtr3'],(Plus(test['mtr3'], test['mtr2'])))

    # net_raster.save(outraster)

    # addField(outraster, value)   

    # gen.buildPyramids_new(outraster, 'NEAREST')






def addField(raster, value):
    normalizer = value*value
    ##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
    arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

    cur = arcpy.UpdateCursor(raster)

    for row in cur:
        row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
        cur.updateRow(row)







def main(instance):
	inraster=Raster('D:\\projects\\usxp\\deliverables\\{0}\\{0}.gdb\\{0}_mtr'.format(instance['series']))
	print 'inraster', inraster

	# for key, reclasslist in instance['reclass'].iteritems():
	for scale in instance['scale'].keys():
		outraster = 'D:\\projects\\usxp\\deliverables\\{0}\\maps\\rel_conv\\rel_conv.gdb\\{0}_{1}_relcov'.format(instance['series'], scale)

		print outraster
		print 'outraster', outraster

		processingCluster(instance, inraster, outraster)






def getAcres(group_counts):
    for key, value in group_counts.iteritems():
        print key, gen.getAcres(pixel_count=value, resolution=30)



##########  call functions  ######################################################################

# instance = {'scale':{'3km':100}, 'series':'s26', 'reclass':{'mtr2':[[2,1]], 'mtr3':[[3,1]]} }
# main(instance)
# getAcres(group_counts = {'forest':514410000, 'grassland':1770500000, 'shrubland':538360000, 'wetland':404270000})















