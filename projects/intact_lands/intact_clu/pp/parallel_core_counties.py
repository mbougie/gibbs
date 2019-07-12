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
# arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



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

	list_union = []

	for row in arcpy.da.SearchCursor('D:\\projects\\intact_land\\intact\\refine\\archive\\mask\\misc.gdb\\states_region', ["st_abbrev","SHAPE@"]):
		st_abbrev = row[0]



		if st_abbrev == 'MN':

			countylist = [row[0] for row in arcpy.da.SearchCursor('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties', ["atlas_stco","SHAPE@"], where_clause="st_abbrev='{}'".format(st_abbrev))]
			print countylist




			for county in countylist:

				print county

				state_gdb_path = 'D:\\projects\\intact_land\\intact\\refine\\states\\{}'.format(st_abbrev)
				county_gdb = '{0}\\county_{1}.gdb'.format(state_gdb_path, county)
				print county_gdb
				counties_fc='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb\\counties'
				
				#####create geodatabase for each state
				# if arcpy.Exists(county_gdb):
				# 	print "gdb exists so continue to next gdb"
				# 	continue

				# print '--------------should only processing if gdb does NOT exist---------------------------------------------'

				# arcpy.CreateFileGDB_management(out_folder_path=state_gdb_path, out_name="county_{}.gdb".format(county))


				# #######  Define dataset paths ##########################################################################################
				
				main = 'D:\\projects\\intact_land\\intact\\main\\years\\2015.gdb\\clu_2015_noncrop_c'
				print 'main', main
				output_main = '{0}\\county_{1}.gdb\\county_{1}_main'.format(state_gdb_path, county)


				rails_buff25m = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\transport\\transport.gdb\\region_rails_buff25m_dissolved'
				output_rails_buff25m = '{0}\\county_{1}.gdb\\county_{1}_rails_rails_buff25m_dissolved'.format(state_gdb_path, county)
				
				roads = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\transport\\transport.gdb\\region_roads'
				output_roads = '{0}\\county_{1}.gdb\\county_{1}_roads'.format(state_gdb_path, county)
				
				roads_buff25m = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\transport\\transport.gdb\\region_roads_buff25m'
				output_roads_buff25m = '{0}\\county_{1}.gdb\\county_{1}_roads_buff25m'.format(state_gdb_path, county)

				urban_cdl = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\urban\\urban.gdb\\region_cdl_2015_dev_5mmu_dissolved'
				output_urban_cdl = '{0}\\county_{1}.gdb\\county_{1}_cdl_2015_dev_5mmu_dissolved'.format(state_gdb_path, county)
				
				urban_500k = 'D:\\projects\\intact_land\\intact\\refine\\layers\\development\\urban\\urban.gdb\\region_urban_500k_dissolved'
				output_urban_500k = '{0}\\county_{1}.gdb\\county_{1}_urban_500k_dissolved'.format(state_gdb_path, county)

				water = 'D:\\projects\\intact_land\\intact\\refine\\layers\\water\\water.gdb\\region_tiger_water_dissolved'
				output_water = '{0}\\county_{1}.gdb\\county_{1}_tiger_water_dissolved'.format(state_gdb_path, county)


				# output_merge = '{0}\\county_{1}.gdb\\county_{1}_merge'.format(state_gdb_path, county)
				output_erase = '{0}\\county_{1}.gdb\\county_{1}_erase'.format(state_gdb_path, county)
				output_union = '{0}\\county_{1}.gdb\\county_{1}_union'.format(state_gdb_path, county)
				output_union_buff = '{0}\\county_{1}.gdb\\county_{1}_union_buff{2}'.format(state_gdb_path, county, '25')
				print output_union_buff
				list_union.append(output_union_buff)


				merge = '{0}.gdb\\{1}_merge'.format(state_gdb_path, st_abbrev)
				print 'merge', merge
				output_merge = '{0}\\county_{1}.gdb\\county_{1}_merge'.format(state_gdb_path, county)
				

				####### Make a layer from the feature class and subset it by field value
				current_layer='layer_{}'.format(county)
				arcpy.MakeFeatureLayer_management(in_features=counties_fc, out_layer=current_layer)
				print current_layer
				arcpy.SelectLayerByAttribute_management (current_layer, "NEW_SELECTION", "atlas_stco='{0}'".format(county))





				####  clip datasets  ##########################################################################################
				arcpy.Clip_analysis(in_features=main, clip_features=current_layer, out_feature_class=output_main)
				arcpy.Clip_analysis(in_features=roads, clip_features=current_layer, out_feature_class=output_roads)
				arcpy.Clip_analysis(in_features=rails_buff25m, clip_features=current_layer, out_feature_class=output_rails_buff25m)
				arcpy.Clip_analysis(in_features=urban_cdl, clip_features=current_layer, out_feature_class=output_urban_cdl)
				arcpy.Clip_analysis(in_features=urban_500k, clip_features=current_layer, out_feature_class=output_urban_500k)
				arcpy.Clip_analysis(in_features=water, clip_features=current_layer, out_feature_class=output_water)



		        ####  add buffers to roads and rails datasets  ##############################
				arcpy.Buffer_analysis(in_features=output_roads, out_feature_class=output_roads_buff25m, buffer_distance_or_field=25, dissolve_option='LIST', dissolve_field='MTFCC')



				# ####### DONT UNCOMMENT THIS NOT FINISHED CODE
				# final_layer='layer_buff25m_{}'.format(county)
				# arcpy.MakeFeatureLayer_management(in_features=output_union, out_layer=final_layer)
				# print final_layer
				# arcpy.SelectLayerByAttribute_management (final_layer, "NEW_SELECTION", "OBJECTID=1")
				# arcpy.FeatureClassToFeatureClass_conversion(in_features=final_layer, out_path='{0}\\county_{1}.gdb'.format(state_gdb_path, county), out_name='county_{}_union_buff25'.format(county))




				#####  union all datasets
				arcpy.Union_analysis(in_features=[output_main, output_roads_buff25m, output_rails_buff25m, output_urban_cdl, output_urban_500k, output_water], out_feature_class=output_union)
				

				# #####  Merge_management (inputs, output, {field_mappings})
				# arcpy.Merge_management(inputs=[output_rails_buff25m, output_urban_cdl, output_urban_500k, output_water], output=output_merge)

				# #####  USe erase function to remove remove the ....  ################
				# arcpy.Erase_analysis(in_features=output_main, erase_features=output_merge, out_feature_class=output_erase)




				####### Make a layer from the feature class and subset it by field value
				final_layer='layer_union_{}'.format(county)
				arcpy.MakeFeatureLayer_management(in_features=output_union, out_layer=final_layer)
				print final_layer
				arcpy.SelectLayerByAttribute_management (final_layer, "NEW_SELECTION", "OBJECTID=1")

				arcpy.FeatureClassToFeatureClass_conversion(in_features=final_layer, out_path='{0}\\county_{1}.gdb'.format(state_gdb_path, county), out_name='county_{}_union_buff25'.format(county))




			### create a list of all union datasets to Merge union datasets together ##############################
			print 'list_union', list_union
			print st_abbrev
			arcpy.Merge_management(inputs=list_union, output='D:\\projects\\intact_land\\intact\\refine\\states\\{0}.gdb\\{0}_union_buff25'.format(st_abbrev))


    
def createFinalProduct():   
	list_states_paths = [
						 'D:\\projects\\intact_land\\intact\\refine\\states\\IA.gdb\\IA_erase',
						 'D:\\projects\\intact_land\\intact\\refine\\states\\MN.gdb\\MN_union_buff25',
						 'D:\\projects\\intact_land\\intact\\refine\\states\\MT.gdb\\MT_union_buff25',
						 'D:\\projects\\intact_land\\intact\\refine\\states\\ND.gdb\\ND_union_buff25',
						 'D:\\projects\\intact_land\\intact\\refine\\states\\NE.gdb\\NE_erase',
						 'D:\\projects\\intact_land\\intact\\refine\\states\\SD.gdb\\SD_erase',
						 'D:\\projects\\intact_land\\intact\\refine\\states\\WY.gdb\\WY_erase'
						]
	arcpy.Merge_management(inputs=list_states_paths, output='D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25')





def SubsetColumns():
	featureclass = "D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50"
	field_names = [f.name for f in arcpy.ListFields(featureclass)]
	print field_names
	fields_remove = [e for e in field_names if e not in ('atlas_stco', 'Shape_Area', 'Shape_Length', 'Shape', 'OBJECTID')]
	print fields_remove
	arcpy.CopyFeatures_management("D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50", "D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50_fips")
	arcpy.DeleteField_management("D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50_fips", fields_remove)



def copy_with_fields(in_fc, out_fc, keep_fields, where=''):
    """
    Required:
        in_fc -- input feature class
        out_fc -- output feature class
        keep_fields -- names of fields to keep in output

    Optional:
        where -- optional where clause to filter records
    """
    fmap = arcpy.FieldMappings()
    fmap.addTable(in_fc)

    # get all fields
    fields = {f.name: f for f in arcpy.ListFields(in_fc)}

    # clean up field map
    for fname, fld in fields.iteritems():
        # if fld.type not in ('OID', 'Geometry') and 'shape' not in fname.lower():
        if fname not in keep_fields:
            fmap.removeFieldMap(fmap.findFieldMapIndex(fname))

    # # copy features
    # path, name = os.path.split(out_fc)
    # arcpy.conversion.FeatureClassToFeatureClass(in_fc, path, name, where, fmap)
    # return out_fc



if __name__ == '__main__':

    # fc = r'D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50'
    # new = r'D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50_fips_t3'
    # fields = ['atlas_stco', 'Shape_Area', 'Shape_Length', 'Shape', 'OBJECTID'] #only these fields and other required fields will be included
    # copy_with_fields(fc, new, fields)
    


    # SubsetColumns()













				#######    JUNK  ###########################################################

				# print output_union
				# fields = arcpy.ListFields(output_union)
				# for field in fields:
				# 	print field
				# 	print field.name
				# yo = arcpy.da.SearchCursor(output_union, ["OBJECTID"], where_clause="OBJECTID=1")
				# print max(yo)




				# print output_main
				# yo = arcpy.da.SearchCursor(output_union, ["Shape_Length"])
				# this = max(yo)[0]
				# print 'this', this
				# hi = arcpy.da.SearchCursor(output_union, ["Shape_Length"],  where_clause="MTFCC='' AND MTFCC_1 ='' AND MTFCC_12='' AND Shape_Length={}".format(this))
				# for row in hi:
				# 	print row


			



if __name__ == '__main__':

    # fc = r'D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50'
    # new = r'D:\\projects\\intact_land\\intact\\refine\\final\\final.gdb\\intactlands_buff25_buff50_fips_t2'
    # fields = ['Match_addr', 'PROPERTY_O', 'OWNER_ADDR'] #only these fields and other required fields will be included
    # copy_with_fields(fc, new, fields)
    SubsetColumns()




###  entire region  ##################################
# byState()
# createFinalProduct()
# SubsetColumns()

