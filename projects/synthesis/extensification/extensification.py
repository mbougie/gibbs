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


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 



def importCSV(csv, pgdb, schema, table):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{0}'.format(pgdb))


	# chunksize = 100000
	# results = pd.read_csv(filepath_or_buffer='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\rfs_intensification.csv', chunksize=chunksize)
	df = pd.read_csv(filepath_or_buffer=csv)

	print df
	
	df.to_sql(name=table, con=engine, schema=schema)
	# for df in results:
	# 	print df
	# 	df.to_sql(name='rfs_intensification', con=engine, schema='synthesis', if_exists='append', chunksize=chunksize)





def convertPGtoFC(gdb, pgdb, schema, table, geom_type, epsg):
    command = 'ogr2ogr -f "FileGDB" -t_srs EPSG:{5} -progress -update {0} PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {2}.{3}" -nln {3} -nlt {4}'.format(gdb, pgdb, schema, table, geom_type, epsg)
    
    os.system(command)



def convertFCtoPG(gdb, pgdb, schema, table, out_table, epsg):
    command = 'ogr2ogr -f "PostgreSQL" PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" {0} -nlt PROMOTE_TO_MULTI -nln {2}.{4} {3} -progress --config PG_USE_COPY YES'.format(gdb, pgdb, schema, table, out_table)
    
    os.system(command)

    # gen.alterGeomSRID(pgdb, schema, table, epsg)


def addGDBTable2postgres_histo(currentobject):
    print 'addGDBTable2postgres_histo..................................................'
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ### remove column
    del df['OBJECTID']

    # ##perform a psuedo pivot table
    # df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_st", value_name="count")
    df=pd.melt(df, id_vars=["LABEL"], value_name="count")

    df.columns = map(str.lower, df.columns)

    # ####add column 
    df['acres'] = df['count']*gen.getPixelConversion2Acres(30)

    tablename = currentobject.split('\\')[-1]
    print 'tablename', tablename

    # print df

    df.to_sql(tablename, engine, schema='synthesis_extensification')

    # MergeWithGeom(df, tablename, eu, eu_col)


def createReclassifyList(columnlist):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	query='SELECT {0},{1} FROM synthesis_extensification.extensification_mlra'.format(columnlist[0], columnlist[1])
	print query
	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[int(row[0]), (int(row[1]*1000000))]
	    fulllist.append(templist)
	print fulllist
	return fulllist



def reclassRaster(schema, gdb, inraster, conv_type, columnlist):
	gdb = 'D:\\projects\\usxp\\deliverables\\maps\\{0}\\{1}.gdb'.format(schema, gdb)
	arcpy.env.workspace = gdb

	###reclass the pixels with the stems_acres coumn by county value from the rasster above
	reclassed_1 = Reclassify(inraster, "Value", RemapRange(createReclassifyList(columnlist)), "NODATA")
	reclassed_1.save("mlra_county_regions_rc_{}_xmillion".format(conv_type))



# def final():
# 	XMin = -2356095
# 	YMin = 276915
# 	rws = 96523
# 	cls = 153811
# 	# outData = numpy.zeros((rows,cols), numpy.int16)
# 	# outData = np.zeros((rws, cls), dtype=np.uint16)
    
#     ### create numpy arrays for input datasets nlcds and traj
# 	# nlcds = {
# 	# 		1992:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_1992', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
# 	# 		2001:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_2001', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
# 	# 		2006:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_2006', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
# 	# 		2011:arcpy.RasterToNumPyArray(in_raster='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb\\nlcd30_2011', lower_left_corner = arcpy.Point(XMin,YMin), nrows = rws, ncols = cls),
# 	# 		}


# 	carbon=Raster('D:\\projects\\usxp\\deliverables\\maps\\synthesis\\extensification\\s35_Carbon.gdb\\s35_Carbon_x10_MgCpixel')
# 	arr_traj = arcpy.RasterToNumPyArray(in_raster=carbon, lower_left_corner = arcpy.Point(XMin,YMin), nrows = cls, ncols = rws)

# 	# yo=arr_traj/10





engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')


def  getDFfromPG(schema, table, columnlist):
	columnlist_string = ','.join(columnlist)
	# engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	query='SELECT {2} FROM {0}.{1}'.format(schema, table, columnlist_string)
	# print query
	df = pd.read_sql_query(query, con=engine)
	# print df

	return df


# rows_list = []
# for row in input_rows:

#         dict1 = {}
#         # get input row in dictionary format
#         # key = col_name
#         dict1.update(blah..) 

#         rows_list.append(dict1)

# df = pd.DataFrame(rows_list)

# >gapminder_2002 = gapminder[gapminder.year == 2002]

def getGrossNetValues(schema, intable, outtable, columnlist):

	df=getDFfromPG(schema, intable)

	rows_list = []

	for column in columnlist:
		if column in ['acres_change_rot_cc_awa', 'acres_change_rot_oo_awa', 'acres_change_rot_co_awa']:
			##erase the previous dictionary
			data_dict = {}
			neg_values=df[column].apply(lambda x: x if x < 0 else 0).mean()
			print neg_values

			pos_values=df[column].apply(lambda x: x if x > 0 else 0).mean()
			print pos_values

            ###bad code FIX!!!!!!!!!!!!!
			###data_dict = {'columnname': column, 'increase': pos_values, 'decrease': neg_values, 'net': neg_values + pos_values, 'qaqc': df[column].mean()}

			rows_list.append(data_dict)
		else:
	        ##erase the previous dictionary
			data_dict = {}
			neg_values=df[column].apply(lambda x: x if x < 0 else 0).sum()
			print neg_values

			pos_values=df[column].apply(lambda x: x if x > 0 else 0).sum()
			print pos_values


			data_dict = {'columnname': column, 'increase': pos_values, 'decrease': neg_values, 'net': neg_values + pos_values, 'qaqc': df[column].sum()}

			rows_list.append(data_dict)



	print rows_list

	df2 = pd.DataFrame(rows_list)
	print df2

	df2 = df2[['columnname', 'increase','decrease', 'net', 'qaqc']]
	df2.to_sql(outtable, engine, schema=schema)






def  getDFfromPG(schema, intable):
	# columnlist_string = ','.join(columnlist)
	# engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')
	query='SELECT * FROM {}.{}'.format(schema, intable)
	# print query
	df = pd.read_sql_query(query, con=engine)

	print df

	return df

	# df2 = pd.melt(df, var_name="dataset", value_name="value")

	# print df2

	# return df2








def mergeAcrTablesDF(wc):
	arcpy.env.workspace = "D:\\projects\\usxp\\deliverables\\maps\\synthesis\\synthesis_extensification.gdb"

	# Get and print a list of tables
	tables = arcpy.ListTables(wc)
	for table in tables:
	    print(table)

	    # Execute AddField twice for two new fields
	    fields = [f.name for f in arcpy.ListFields(table)]

	    # converts a table to NumPy structured array.
	    arr = arcpy.da.TableToNumPyArray(table,fields)
	    print arr


	    #### convert numpy array to pandas dataframe
	    df = pd.DataFrame(data=arr)

	    df.columns = map(str.lower, df.columns)

	    # tablename = table.split('_')[-1]
	    # print 'tablename:', tablename

	    # # print df

	    df.to_sql(table, engine, schema='synthesis_extensification')










#############################################################################################
###### call functions ##################################################################
#################################################################################################
os. chdir("I:\\d_drive\\projects\\synthesis\\s35\\extensification\\csv")
#### preliminary steps ---- import Eric's and Nathan's csv datasets into postgres ########################################
# importCSV(csv='C:\\Users\\Bougie\\Box\\NWF-RFS project\\Shared Data\\AgroIBIS_results\\Extensification\\20190505\\countyStats02.csv', pgdb='synthesis', schema='extensification_agroibis', table='agroibis_20190505')
importCSV(csv='extensification_county_regions.csv', pgdb='synthesis', schema='extensification_mlra', table='extensification_county_regions')
importCSV(csv='extensification_regions.csv', pgdb='synthesis', schema='extensification_mlra', table='extensification_regions')




####convert the main agroibis table into a feature class so R code can reference it
# convertPGtoFC(gdb='D:\\projects\\synthesis\\s35\\extensification\\extensification.gdb', pgdb='synthesis', schema='extensification_agroibis', table='agroibis_counties', geom_type='MultiPolygon', epsg='4326')
# convertPGtoFC(gdb='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\synthesis.gdb', pgdb='usxp_deliverables', schema='synthesis', table='counties_5070_lrrgroup', geom_type='MultiPolygon')
# convertPGtoFC(gdb='D:\\projects\\synthesis\\s35\\extensification\\extensification.gdb', pgdb='usxp_deliverables', schema='synthesis_extensification', table='extensification_mlra', geom_type='MultiPolygon', epsg='4326')





# convertFCtoPG(gdb='D:\\projects\\usxp\\deliverables\\maps\\synthesis\\synthesis.gdb', pgdb='usxp_deliverables', schema='synthesis', table='counties_5070_lrrgroup_dissolved', out_table='counties_5070_lrrgroup_dissolved', epsg=5070)
# convertFCtoPG(gdb='D:\\data\\WBD_National_GDB\\wbd.gdb', pgdb='usxp', schema='spatial', table='WBDHU10', out_table='WBDHU10', epsg=4269)
# convertFCtoPG(gdb='D:\\data\\mlra\\mlra.gdb', pgdb='usxp_deliverables', schema='synthesis', table='mlra_t5', out_table='mlra', epsg=4269)



####  Seth's carbon datasets  ###########################################################################################
# create 
# addGDBTable2postgres_histo('D:\\projects\\usxp\\deliverables\\maps\\synthesis\\synthesis_extensification.gdb\\extensification_counties_hist_from_vector')
# createReclassifyList()
# reclassRaster(schema='synthesis', gdb='synthesis_extensification', inraster='counties_5070_lrrgroup_dissolved_raster', conv_type='expand', columnlist=['unique_id','perc_expand_rfs'])
# reclassRaster(schema='synthesis', gdb='synthesis_extensification', inraster='counties_5070_lrrgroup_dissolved_raster', conv_type='abandon', columnlist=['unique_id','perc_abandon_rfs'])

# NOTE:  this is the gui calculation to derive carbon_emissions_rfs
# Float((Float(Float("s35_Carbon_x10_MgCpixel")/Float(10)))*(Float(Float("mlra_county_regions_rc_xmillion")/Float(1000000))))

# Float((Float(Float("s35_Abandon_Carbon_x10_MgCpixell")/Float(10)))*(Float(Float("mlra_county_regions_rc_abandon_xmillion")/Float(1000000))))


# D:\projects\usxp\deliverables\maps\synthesis\synthesis_extensification.gdb\carbon_sequester_rfs













###  synthesis_extensification  #####################################################
####define objects###################



###  synthesis_intensification  #####################################################






columnlist_extensification_mlra=[
						'expand_from_pasture',
						'abandon_to_pasture',
						'net_pasture',
						'expand_from_crp',
						'abandon_to_crp',
						'net_crp',
						'expand_from_either',
						'abandon_to_either',
						'net_either',
						'perc_expand_rfs',
						'perc_abandon_rfs']



# extensification_mlra = {'schema':'synthesis_extensification', 'intable':'extensification_mlra', 'outtable':'extensification_mlra_national', 'columnlist':columnlist_extensification_mlra}
# getGrossNetValues(schema=extensification_mlra['schema'], intable=extensification_mlra['intable'], outtable=extensification_mlra['outtable'], columnlist=extensification_mlra['columnlist'])




columnlist_extensification_carbon=[
						'e_mg_c',
						'e_gigagrams_co2e',
						's_mg_c',
						's_gigagrams_co2e']

# extensification_carbon = {'schema':'synthesis_extensification', 'intable':'carbon_rfs_counties_v2', 'outtable':'carbon_rfs_counties_v2_national', 'columnlist':columnlist_extensification_carbon}



columnlist_extensification_agroibis=[
						'et_exp_imp_rfs',
						'gal_et_exp_imp_rfs',
						'et_aban_imp_rfs',
						'gal_et_aban_imp_rfs',
						'irr_exp_imp_rfs',
						'gal_irr_exp_imp_rfs',
						'irr_aban_imp_rfs',
						'gal_irr_aban_imp_rfs']






# extensification_agroibis = {'schema':'synthesis_extensification', 'intable':'agroibis', 'outtable':'agroibis_national_v3', 'columnlist':columnlist_extensification_agroibis}

# getGrossNetValues(schema=extensification_agroibis['schema'], intable=extensification_agroibis['intable'], outtable=extensification_agroibis['outtable'], columnlist=extensification_agroibis['columnlist'])








# getGrossNetValues(schema=extensification_carbon['schema'], intable=extensification_carbon['intable'], outtable=extensification_carbon['outtable'], columnlist=extensification_carbon['columnlist'])

columnlist_intensification=[
			  'acres_change_rot_cc',
			  'acres_change_rot_oo',
              'acres_change_rot_co',
			  'acres_change_rot_cc_awa',
			  'acres_change_rot_oo_awa',
			  'acres_change_rot_co_awa',
			  'hectares_change_rot_cc_napp',
			  'tons_co2e_change_rot_cc_n2o_mean',
			  'tons_co2e_change_rot_cc_n2o_p025',
			  'tons_co2e_change_rot_cc_n2o_p975',
			  'hectares_change_rot_oo_napp',
			  'tons_co2e_change_rot_oo_n2o_mean',
			  'tons_co2e_change_rot_oo_n2o_p025',
			  'tons_co2e_change_rot_oo_n2o_p975',
			  'hectares_change_rot_co_napp',
			  'tons_co2e_change_rot_co_n2o_mean',
			  'tons_co2e_change_rot_co_n2o_p025',
			  'tons_co2e_change_rot_co_n2o_p975',
			  'hectares_change_total_napp',
			  'hectares_change_total_n2o_mean',
			  'tons_co2e_change_total_n2o_mean',
			  'tons_co2e_change_total_n2o_p025',
			  'tons_co2e_change_total_n2o_p975',]






# intensification = {'schema':'synthesis_intensification', 'intable':'rfs_intensification_results_counties', 'outtable':'rfs_intensification_results_counties_national', 'columnlist':columnlist_intensification}
# getGrossNetValues(schema=intensification['schema'], intable=intensification['intable'], outtable=intensification['outtable'], columnlist=intensification['columnlist'])





















# getDFfromPG()

# mergeAcrTablesDF("carbon*")





