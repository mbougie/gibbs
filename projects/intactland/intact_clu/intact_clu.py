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



def reclass(inraster, outraster):
    reclasslist = [[63,1],[141,1],[142,1],[143,1],[83,2],[87,2],[190,2],[195,2],[37,3],[62,3],[171,3],[176,3],[181,3],[64,4],[65,4],[131,4],[152,4]]
    outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
    outReclass.save(outraster)







#### description ##########################################
####1)attach cdl_2015 and LCC datasets to intact_clu_2015
####2)create zonal stats for both of these new datasets

##establish workspace evironment
gdb = 'D:\\intactland\\intact_clu\\final\\current.gdb'
arcpy.env.workspace = gdb

#### establish environmental parameters
reference_raster = Raster('intactland_15_refined')
arcpy.env.snapRaster = reference_raster
arcpy.env.cellsize = reference_raster
arcpy.env.outputCoordinateSystem = reference_raster
arcpy.env.extent = reference_raster.extent


#### reference datasets #######################
states = 'D:\\intactland\\general.gdb\\states'
counties = 'D:\\intactland\\general.gdb\\counties'

#### create a dataset referncing cdl15 and lcc
# rasterdict = {'intact_clu':
# 					{'outraster':'D:\\intactland\\intact_clu\\final\\current.gdb\\intactland_15_refined'}
# 				'lcc':
# 					{'inraster':'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\suitability\\suitability.gdb\\gssurgo_nicc_30m',
# 					 'outraster':'intactland_15_refined_lcc'
# 					},
# 			  'cdl15':
# 					{'inraster':'I:\\e_drive\\data\\cdl\\cdl.gdb\\cdl30_2015',
# 					 'outraster':'intactland_15_refined_cdl15'
# 					}
# 			}



rasterdict = {'cdl_broad':
				   {'inraster':'intactland_15_refined_cdl15',
				    'outraster':'intactland_15_refined_cdl15_broad'
				   }
			 }



#####label what this is and put it into a function!!!!!!!!!!!!!!!!!!
# for key, value in rasterdict.iteritems():
# 	print key

# 	outraster = value['outraster']
# 	print outraster

# 	if key in ['lcc', 'cdl15']:
# 		print 'not intact_clu.....so setNull'
# 		inraster = value['inraster']
# 		print inraster
# 		#########  set null #############################
# 		outSetNull = SetNull (in_conditional_raster=reference_raster, in_false_raster_or_constant=Raster(inraster), where_clause="Value=0")
# 		outSetNull.save(outraster)
# 	elif key == 'cdl_broad':
# 		inraster = value['inraster']
# 		print inraster
# 		####group the broad cdl classes into broad classes
# 		reclass(inraster, outraster)
    


	#########  zonal histogram  #############################
	####NOTE: can't perform this in arcpy because table is returned with no LABEL column --- need to create unique values versus stretched
	#### maybe adding a color_map to this before hand will resolve this and then can run in arcpy???


	#### create zonal histogram dataset by state and county --- PERFORMED IN GUI
	# ZonalHistogram(in_zone_data=states, zone_field='atlas_st', in_value_raster=Raster(outraster), out_table='{}_hist_states'.format(outraster))
	
	# #### export zonal histogram tables to postgres
	# gen.addGDBTable2postgres_histo_states(gdb=gdb, pgdb='intactland', schema='intact_clu', table='{}_hist_states'.format(outraster))

	# 	#### create zonal histogram dataset by state and county --- PERFORMED IN GUI
	# ZonalHistogram(in_zone_data=counties, zone_field='atlas_stco', in_value_raster=Raster(outraster), out_table='{}_hist_counties'.format(outraster))
	
	# #### export zonal histogram tables to postgres
	# gen.addGDBTable2postgres_histo_counties(gdb=gdb, pgdb='intactland', schema='intact_clu', table='{}_hist_counties'.format(outraster))


##############################################################################################
#######--------------- combined dataset (dataset that contains ALL classes that we are concerned with for the 7 state reion) ----------------
################################################################################################

#### combined crop_15, non_crop, mask_final, pad to create a dataset of all classes to define the 7 state region
##Note: performed this in GUI

###input dataets for combine function
# crop = 'D:\\intactland\\intact_clu\\main\\years\\2015_initial.gdb\\clu_2015_crop_c_b'
# non_crop = 'intactland_15_refined'
# masks = 'D:\\intactland\\intact_clu\\refine\\masks\\merged\\merged.gdb\\mask_main'
# pad = 'pad_30m_b'

###output dataset from combine function
# outraster_combine = "combined"

##### run the combine function ###########################
# outCombine = Combine([crop,non_crop, masks, pad])
# outCombine.save(outraster_combine)

#### export raster attribute table to be used as a lookup table
# gen.addGDBTable2postgres_raster(gdb=gdb, pgdb='intactland', schema='intact_clu', intable=outraster_combine, outtable=outraster_combine)

# # #### create zonal histogram dataset by state
# ZonalHistogram(in_zone_data=states, zone_field='atlas_st', in_value_raster=Raster(outraster_combine), out_table='{}_hist_states'.format(outraster_combine))

# # #### export zonal histogram tables to postgres
# gen.addGDBTable2postgres_histo_states(gdb=gdb, pgdb='intactland', schema='intact_clu', table='{}_hist_states'.format(outraster_combine))







#######################################################################################
##### attach pad to intactland_15_refined_cdl15_broad and then export to postgres
##########################################################################################

###combine intactland_15_refined_cdl15_broad with pad (did this in the GUI)

#### export raster attribute table to be used as a lookup table
gen.addGDBTable2postgres_raster(gdb=gdb, pgdb='intactland', schema='intact_clu', intable='intactland_15_refined_cdl15_broad_pad', outtable='intactland_15_refined_cdl15_broad_pad')

#### create zonal histogram dataset by state
# ZonalHistogram(in_zone_data=states, zone_field='atlas_st', in_value_raster=Raster('intactland_15_refined_cdl15_broad_pad'), out_table='{}_hist_states'.format('intactland_15_refined_cdl15_broad_pad'))

# #### export zonal histogram tables to postgres
# gen.addGDBTable2postgres_histo_states(gdb=gdb, pgdb='intactland', schema='intact_clu', table='{}_hist_states'.format('intactland_15_refined_cdl15_broad_pad'))















