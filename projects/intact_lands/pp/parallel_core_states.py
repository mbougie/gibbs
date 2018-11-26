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


#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "100%"
arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 



def execute_task(args):
	# in_extentDict, data, traj_list = args
	in_extentDict = args


	st_abbrev = in_extentDict[0]
	print st_abbrev
	procExt = in_extentDict[1]
	print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

	yo='D:\\projects\\intact_land\\intact\\refine\\archive\\mask\\misc.gdb\\fishnet_region'

	#######  clip ##########################################################################################
	rails = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\road\\roads.gdb\\region_rails_buff25m'
	roads = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\road\\roads.gdb\\region_roads_buff25m'

	cdl_urban = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\urban\\urban.gdb\\region_cdl_2015_dev_5mmu'
	urban_500k = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\urban\\urban.gdb\\region_urban_500k'

	water = 'D:\\projects\\intact_land\\intact\\refine\\layers\\water\\water.gdb\\region_tiger_water'

	main = 'D:\\projects\\intact_land\\intact\\main\\years\\2015.gdb\\clu_2015_noncrop_c'
	merged = 'D:\\projects\\intact_land\\intact\\refine\\archive\\mask\\final.gdb\\region_merged_masks_t2'

	output = 'D:\\projects\\intact_land\\intact\\refine\\final\\merged_{}.shp'.format(st_abbrev)
	# inputslist = [rails, roads, cdl_urban, urban_500k, water]	

	# for inputs in inputslist:

	current_layer='layer_{}'.format(st_abbrev)




	 # "\"FIELD\" = \'121\'"

	# Make a layer from the feature class
	arcpy.MakeFeatureLayer_management(in_features=main, out_layer=current_layer)


	# arcpy.SelectLayerByAttribute_management("cities_lyr", "SUBSET_SELECTION", "POPULATION > 10000")

  	arcpy.SelectLayerByAttribute_management (in_layer_or_view=current_layer, selection_type="SUBSET_SELECTION", where_clause="oid_yo={}".format(st_abbrev))
	# # Select all cities that overlap the chihuahua polygon
	# arcpy.SelectLayerByLocation_management("cities_lyr", "INTERSECT", "c:/data/mexico.gdb/chihuahua", "", "NEW_SELECTION")

	# # Within the selection (done above) further select only those cities that have a population >10,000
	# arcpy.SelectLayerByAttribute_management(st_abbrev, "SUBSET_SELECTION", "POPULATION > 10000")

	# # Write the selected features to a new featureclass
	# arcpy.CopyFeatures_management("cities_lyr", "c:/data/mexico.gdb/chihuahua_10000plus")


	arcpy.Clip_analysis(in_features=main, clip_features=current_layer, out_feature_class=output)



	#######  MERGE ##########################################################################################
	# rails = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\road\\roads.gdb\\region_rails_buff25m'
	# roads = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\road\\roads.gdb\\region_roads_buff25m'

	# cdl_urban = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\urban\\urban.gdb\\region_cdl_2015_dev_5mmu'
	# urban_500k = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\urban\\urban.gdb\\region_urban_500k'

	# water = 'D:\\projects\\intact_land\\intact\\refine\\layers\\water\\water.gdb\\region_tiger_water'

	# inputs = [rails, roads, cdl_urban, urban_500k, water]

	# rails = roads = cdl_urban = urban_500k = water = None

	
	
	# # arcpy.Merge_management(inputs, output)
	# output = None



	# #######  ERASE ##########################################################################################
	# in_features ='D:\\projects\\intact_land\\intact\\main\\years\\2015.gdb\\clu_2015_noncrop_c'
	# # erase_features = 'D:\\projects\\intact_land\\intact\\refine\mask\\final.gdb\\region_merged_masks_t2'
	# out_feature_class = 'D:\\projects\\intact_land\\intact\\refine\\final\\clu_2015_noncrop_c_w_masks_{}'.format(str(st_abbrev))

	

	# arcpy.Erase_analysis(in_features=in_features, erase_features=output, out_feature_class=out_feature_class)







def mosiacRasters(data):
	######Description: mosiac tiles together into a new raster
	tilelist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*.tif")
	print 'tilelist:', tilelist 

	#### need to wrap these paths with Raster() fct or complains about the paths being a string
	inTraj=Raster(data['pre']['traj']['path'])

	filename = data['core']['filename']
	print 'filename:-----------------------------', filename
	
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(tilelist, data['core']['gdb'], filename, inTraj.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")

	#Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(data['core']['path'], "Overwrite")

	# Overwrite pyramids
	gen.buildPyramids(data['core']['path'])







def run():

	# tiles = glob.glob("C:/Users/Bougie/Desktop/Gibbs/data/tiles/*")
	# for tile in tiles:
	# 	os.remove(tile)

	# traj_list = createReclassifyList(data)

	#get extents of individual features and add it to a dictionary
	extDict = {}

	for row in arcpy.da.SearchCursor('D:\\projects\\intact_land\\intact\\refine\\archive\\mask\\misc.gdb\\fishnet_region', ["oid","SHAPE@"]):
		atlas_stco = row[0]
		print atlas_stco
		extent_curr = row[1].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[atlas_stco] = ls

	print 'extDict', extDict
	print'extDict.items',  extDict.items()

	#####create a process and pass dictionary of extent to execute task
	pool = Pool(processes=7)
	pool.map(execute_task, [(ed) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(data)


if __name__ == '__main__':
	print('running the __main__ fct......')
	# run()




def byState():

	for row in arcpy.da.SearchCursor('D:\\projects\\intact_land\\intact\\refine\\archive\\mask\\misc.gdb\\states_region', ["st_abbrev","SHAPE@"]):
		st_abbrev = row[0]

		if st_abbrev != 'MN':

			state_gdb_path = 'D:\\projects\\intact_land\\intact\\refine\\states'
			yo='D:\\projects\\intact_land\\intact\\refine\\archive\\mask\\misc.gdb\\states_region'
			
			##create geodatabase for each state
			# arcpy.CreateFileGDB_management(out_folder_path=state_gdb_path, out_name="{}.gdb".format(st_abbrev))


			#######  Define dataset paths ##########################################################################################
			
			main = 'D:\\projects\\intact_land\\intact\\main\\years\\2015.gdb\\clu_2015_noncrop_c'
			output_main = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_main'.format(st_abbrev)


			intactlands_buff25_buff50_fips = 'D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50_fips'
			output_intactlands_buff25_buff50_fips = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_intactlands_buff25_buff50_fips'.format(st_abbrev)

			pad_acea_region_mngmt_dissolved = 'D:\\projects\\intact_land\\pad\\pad.gdb\\pad_acea_region_mngmt_dissolved'
			output_pad_acea_region_mngmt_dissolved = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_pad_acea_region_mngmt_dissolved'.format(st_abbrev)
	

			roads = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\transport\\transport.gdb\\region_roads'
			output_roads = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_roads'.format(st_abbrev)
			output_roads_buff25m = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_roads_buff25m'.format(st_abbrev)

			rails_buff25m = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\transport\\transport.gdb\\region_rails_buff25m_dissolved'
			output_rails_buff25m = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_rails_rails_buff25m_dissolved'.format(st_abbrev)

			urban_cdl = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\urban\\urban.gdb\\region_cdl_2015_dev_5mmu_dissolved'
			output_urban_cdl = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_cdl_2015_dev_5mmu_dissolved'.format(st_abbrev)
			
			urban_500k = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\urban\\urban.gdb\\region_urban_500k_dissolved'
			output_urban_500k = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_urban_500k_dissolved'.format(st_abbrev)

			water = 'D:\\projects\\intact_land\\intact\\refine\\layers\\water\\water.gdb\\region_tiger_water_dissolved'
			output_water = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_tiger_water_dissolved'.format(st_abbrev)


			output_merge = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_merge'.format(st_abbrev)
			output_erase = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_erase'.format(st_abbrev)


			output_union = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_union'.format(st_abbrev)
			

			####### Make a layer from the feature class and subset it by field value
			current_layer='layer_{}'.format(st_abbrev)
			arcpy.MakeFeatureLayer_management(in_features=yo, out_layer=current_layer)
			print current_layer
			arcpy.SelectLayerByAttribute_management (current_layer, "NEW_SELECTION", "st_abbrev='{0}'".format(st_abbrev))





			#####  clip datasets  ##########################################################################################
			# arcpy.Clip_analysis(in_features=main, clip_features=current_layer, out_feature_class=output_main)
			# arcpy.Clip_analysis(in_features=roads, clip_features=current_layer, out_feature_class=output_roads)
			# arcpy.Clip_analysis(in_features=rails_buff25m, clip_features=current_layer, out_feature_class=output_rails_buff25m)
			# arcpy.Clip_analysis(in_features=urban_cdl, clip_features=current_layer, out_feature_class=output_urban_cdl)
			# arcpy.Clip_analysis(in_features=urban_500k, clip_features=current_layer, out_feature_class=output_urban_500k)
			# arcpy.Clip_analysis(in_features=water, clip_features=current_layer, out_feature_class=output_water)



	        #####  add buffers to roads and rails datasets  ##############################
			# arcpy.Buffer_analysis(in_features=output_roads, out_feature_class=output_roads_buff25m, buffer_distance_or_field=25, dissolve_option='LIST', dissolve_field='MTFCC')



			# # Merge_management (inputs, output, {field_mappings})
			# arcpy.Merge_management(inputs=[output_roads_buff25m, output_rails_buff25m, output_urban_cdl, output_urban_500k, output_water], output=output_merge)



			#####union all datasets
			# arcpy.Union_analysis(in_features=[main, output_roads_buff25m, output_rails_buff25m, output_urban_cdl, output_urban_500k, output_water], out_feature_class=output_union)

			## USe erase function to remove remove the ....  ################
			# arcpy.Erase_analysis(in_features=output_main, erase_features=output_merge, out_feature_class=output_erase)






			arcpy.Clip_analysis(in_features=intactlands_buff25_buff50_fips, clip_features=current_layer, out_feature_class=output_intactlands_buff25_buff50_fips)
			arcpy.Clip_analysis(in_features=pad_acea_region_mngmt_dissolved, clip_features=current_layer, out_feature_class=output_pad_acea_region_mngmt_dissolved)
			output_union_final = 'D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_output_intactlands_buff25_buff50_union'.format(st_abbrev)
			arcpy.Union_analysis(in_features=[output_intactlands_buff25_buff50_fips, output_pad_acea_region_mngmt_dissolved], out_feature_class=output_union_final)














###  entire region  ##################################
byState()

