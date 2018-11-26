# Import system modules
import sys 
# sys.path.append("C:\Users\Bougie\Desktop\Gibbs\scripts\usxp\\misc")
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os, subprocess
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import fnmatch





arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 



def get(query):
    ###sub-function for reclassRaster
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')
    print 'query:', query
    df = pd.read_sql_query(query, con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        templist.append(row['initial'])
        templist.append(row['grouped'])
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist




def createZonalStats(gdb, in_zone_data, zone_field, in_value_raster, out_table, ignore_nodata, statistics_type):
    print("createZonalStats().............")
    
    env.workspace = 'D:\\projects\\lem\\matt\\gdbases\\{}.gdb'.format(gdb)

    # Execute ZonalStatistics
    ZonalStatisticsAsTable(in_zone_data, zone_field, in_value_raster, out_table, ignore_nodata, statistics_type)


def findNeighbors(gdb, in_feature, out_table, in_fields):
    print("findNeighbors().............")

    env.workspace = 'D:\\projects\\lem\\matt\\gdbases\\{}.gdb'.format(gdb)

    arcpy.PolygonNeighbors_analysis(in_features=in_feature, out_table=out_table, in_fields=in_fields, area_overlap='AREA_OVERLAP', both_sides='BOTH_SIDES', out_linear_units='FEET', out_area_units='ACRES')



def reclassRaster(inraster, outraster, query):
    raster_reclass = Reclassify((inraster), "Value", RemapRange(createReclassifyList(query)), "NODATA")
    raster_reclass.save(outraster)



def createReclassifyList(query):
    ###sub-function for reclassRaster
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')
    print 'query:', query
    df = pd.read_sql_query(query, con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        templist.append(row[0])
        templist.append(row[1])
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist





def addGDBTable2postgres(gdb, schema, table):
    print("addGDBTable2postgres().............")
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')
    
    # # path to the table you want to import into postgres
    input_table = 'D:\\projects\\lem\\matt\\gdbases\\{}.gdb\\{}'.format(gdb, table)

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input_table)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input_table,fields)
    print arr


    print 'Split the array in 20 equal-sized subarrays:' 
    split_array = np.array_split(arr,10) 

    for array in split_array:
    
        # # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=array)

        df.columns = map(str.lower, df.columns)
        print 'df-----------------------', df
        
        df.to_sql(table, con=engine, schema=schema, if_exists='append')










def changeTableFormat(gdb, schema, in_table):
    print("changeTableFormat().............")
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')
    
    # # path to the table you want to import into postgres
    input_table = 'D:\\projects\\lem\\matt\\gdbases\\{}.gdb\\{}'.format(gdb, in_table)

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input_table)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input_table,fields)
    print arr

    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    

    #r## remove column
    # del df['OBJECTID']
    # print df


    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["OBJECTID"],var_name="geoid", value_name="count")


    print df

    df.columns = map(str.lower, df.columns)

    df['value']=df['objectid'] - 1
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df['geoid'] = df['geoid'].map(lambda x: x.strip('geoid_'))

    print df


    # #### add columns to table ########################
    # df['series'] = data['global']['instance']
    # df['yxc'] = yxc
    # df['acres'] = gen.getAcres(df['count'], 30)
    # df['year'] = year

    # #### join tables to get the state abreviation #########
    # df = pd.merge(df, pd.read_sql_query('SELECT atlas_st,st_abbrev FROM spatial.states;',con=engine), on='atlas_st')
    # print df

    # return df
    df.to_sql(in_table, engine, schema=schema)
    

# command = 'ogr2ogr -f "PostgreSQL" PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" D:\\projects\\lem\\lem.gdb -nlt PROMOTE_TO_MULTI -nln blocks.us_blck_grp_2016_mainland_5070 us_blck_grp_2016_mainland_5070 -progress --config PG_USE_COPY YES'
# epsg_102003 = 'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic",GEOGCS["GCS_North_American_1983",DATUM["North_American_Datum_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["longitude_of_center",-96],PARAMETER["Standard_Parallel_1",29.5],PARAMETER["Standard_Parallel_2",45.5],PARAMETER["latitude_of_center",37.5],UNIT["Meter",1],AUTHORITY["EPSG","102003"]]'
# epsg_102003 = '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs '
def convertFCtoPG(gdb, pgdb, schema, table, epsg):
    command = 'ogr2ogr -f "PostgreSQL" PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" D:\\projects\\lem\\matt\\gdbases\\{0}.gdb -nlt PROMOTE_TO_MULTI -nln {1}.{2} {2} -progress --config PG_USE_COPY YES'.format(gdb, schema, table)

    os.system(command)

    gen.alterGeomSRID(pgdb, schema, table, epsg)


def convertPGtoFC(gdb, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -update D:\\projects\\lem\\matt\\gdbases\\{0}.gdb -progress PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {1}.{2}" -nln {2} -nlt MULTIPOLYGON'.format(gdb, schema, table)
    os.system(command)


def getNullPolygons(gdb, schema, in_table, out_table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -update D:\\projects\\lem\\matt\\gdbases\\{0}.gdb -progress PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT a.geoid, majority, wkb_geometry AS geom FROM v1_wisc.block_group AS a JOIN {1}.{2} AS b USING(geoid) WHERE majority IN (11,21)" -nln {3} -nlt MULTIPOLYGON'.format(gdb, schema, in_table, out_table)
    os.system(command)

# "PG:\"host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem\""

def convertPGtoJSON(version, db, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "geojson" -progress D:\\projects\\lem\\matt\\deliverables\\{0}\\{3}.json PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {2}.{3}" -t_srs EPSG:4152'.format(version, db, schema, table) 
    os.system(command)








#######BLOCK-GROUP#################################################
############### call functions ##############################
# reclassRaster(inraster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", outraster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt_rc", query="SELECT initial, grouped FROM nwalt_lookup WHERE grouped IS NOT NULL")
# reclassRaster(inraster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", outraster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt_11_21", query="SELECT initial, initial FROM nwalt_lookup WHERE initial IN (11,21)")
# convertFCtoPG('v1', 'lem', 'v1', 'block_group', 102003)

# createZonalStats(gdb="v1_wisc", in_zone_data="block_group", zone_field="geoid", in_value_raster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", out_table="block_group_zonal_maj_v1_0", ignore_nodata="DATA", statistics_type="MAJORITY")
# addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_group_zonal_maj_v1_0")

# findNeighbors(gdb='v1_wisc', in_feature='block_group', out_table='block_group_neighbors_v1_0', in_fields="geoid")
# addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_group_neighbors_v1_0")

# changeTableFormat(gdb="block", schema="block", table="block_wisc_zonal_hist")
# convertPGtoFC(gdb='block', schema='block', table='block_wisc_102003')

##### export from postgres #####################################
# convertPGtoFC(gdb='v1', schema='v1', table='block_group_view')
# convertPGtoJSON()













#######BLOCK#################################################
############### call functions ##############################
# reclassRaster(inraster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", outraster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt_rc", query="SELECT initial, grouped FROM nwalt_lookup WHERE grouped IS NOT NULL")
# reclassRaster(inraster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", outraster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt_11_21", query="SELECT initial, initial FROM nwalt_lookup WHERE initial IN (11,21)")

# convertFCtoPG('v1', 'lem', 'v1', 'block', 102003)

# createZonalStats(gdb="v1_wisc", in_zone_data="block", zone_field="geoid", in_value_raster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", out_table="block_zonal_maj_v1_0", ignore_nodata="DATA", statistics_type="MAJORITY")
# addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_zonal_maj_v1_0")

# findNeighbors(gdb='v1_wisc', in_feature='block', out_table='block_neighbors_v1_0', in_fields="geoid")
# addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_neighbors_v1_0")

# changeTableFormat(gdb="block", schema="block", table="block_wisc_zonal_hist")
# convertPGtoFC(gdb='block', schema='block', table='block_wisc_102003')





##### general functions (stateless)  #####################################################################
# convertFCtoPG('v1_wisc', 'lem', 'v1_wisc', 'block_group', 102003)
# convertFCtoPG('v1_wisc', 'lem', 'v1_wisc', 'block', 102003)

# findNeighbors(gdb='v1_wisc', in_feature='block_group', out_table='block_group_neighbors', in_fields="geoid")
# addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_group_neighbors")

# findNeighbors(gdb='v1_wisc', in_feature='block', out_table='block_neighbors_v1_0', in_fields="geoid")
# addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_neighbors_v1_0")





def main():
    print 'main() function............................'
    #####  V1_0 parameters (statefull) ####################################################################################
  

    ##_________Create hybrid dataset_____________________________________________________________________
    # createZonalStats(gdb="v1_wisc", in_zone_data="block_group", zone_field="geoid", in_value_raster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", out_table="block_group_zonal_maj_v1_0", ignore_nodata="DATA", statistics_type="MAJORITY")
    # addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_group_zonal_maj_v1_0")

    # getNullPolygons(gdb="v1_wisc", schema="v1_wisc", in_table="block_group_zonal_maj_v1_0", out_table="block_group_null_v1_0")
    #####need to have zonal hist function here!!!!!!
    # changeTableFormat(gdb="v1_wisc", schema="v1_wisc", in_table="block_group_zonal_hist_v1_0")



    # findNeighbors(gdb='v1_wisc', in_feature='block_group', out_table='block_group_neighbors_v1_0', in_fields="geoid")
    # addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_group_neighbors_v1_0")

    # createZonalStats(gdb="v1_wisc", in_zone_data="block", zone_field="geoid", in_value_raster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", out_table="block_zonal_maj_v1_0", ignore_nodata="DATA", statistics_type="MAJORITY")
    # addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_zonal_maj_v1_0")







    #####  V1_1 parameters (statefull) ####################################################################################
    # createZonalStats(gdb="v1_wisc", in_zone_data="block_group", zone_field="geoid", in_value_raster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", out_table="block_group_zonal_maj_v1_1_1", ignore_nodata="DATA", statistics_type="MAJORITY")
    # addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_group_zonal_maj_v1_1_1")
    # convertPGtoFC(gdb='v1_wisc', schema='v1_wisc', table='block_group_v1_1_2_no_neighbors_view')



    # createZonalStats(gdb="v1_wisc", in_zone_data="block", zone_field="geoid", in_value_raster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt_rc", out_table="block_zonal_maj_v1_1_1", ignore_nodata="DATA", statistics_type="MAJORITY")
    # addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_zonal_maj_v1_1_1")
    # convertPGtoFC(gdb='v1_wisc', schema='v1_wisc', table='block_v1_2_1_no_neighbors_view')
    # findNeighbors(gdb='v1_wisc', in_feature='block', out_table='block_neighbors', in_fields="geoid")
    # addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_neighbors")





    #####  V1_1 parameters (statefull) ####################################################################################
    # createZonalStats(gdb="v1_wisc", in_zone_data="block_group", zone_field="geoid", in_value_raster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt", out_table="block_group_zonal_maj_v1_1_1", ignore_nodata="DATA", statistics_type="MAJORITY")
    # addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_group_zonal_maj_v1_1_1")
    # convertPGtoFC(gdb='v1_wisc', schema='v1_wisc', table='block_group_v1_1_2_no_neighbors_view')



    # createZonalStats(gdb="v1_wisc", in_zone_data="block", zone_field="geoid", in_value_raster="D:\\projects\\lem\\matt\\gdbases\\rasters.gdb\\nwalt_rc", out_table="block_zonal_maj_v1_1_1", ignore_nodata="DATA", statistics_type="MAJORITY")
    # addGDBTable2postgres(gdb="v1_wisc", schema="v1_wisc", table="block_zonal_maj_v1_1_1")
    # convertPGtoFC(gdb='v1_wisc', schema='v1_wisc', table='block_v1_2_1_no_neighbors_view')


    #####  V1_2_1 parameters (statefull) ####################################################################################
    ###note using a hybrid approach with the v1_0 and v1_1_1 so only need to reference the stored query in postgres
    # convertPGtoFC(gdb='v1_wisc', schema='v1_wisc', table='block_group_v1_2_1')
    convertPGtoJSON('v1', 'lem', 'v1_wisc', 'block_group_v1_2_1')

    # convertPGtoFC(gdb='v1_wisc', schema='v1_wisc', table='block_v1_2_1')
    convertPGtoJSON('v1', 'lem', 'v1_wisc', 'block_v1_2_1')








###call the main function#####################################################################
# main()



##### export from postgres #####################################
convertPGtoFC(gdb='v1_wisc_qaqc', schema='v1_wisc_qaqc', table='block_group_null')
# convertPGtoJSON()




















# topojson -o D:\projects\lem\deliverables\output.json D:\projects\lem\deliverables\us_blck_grp_nonformat.json



# geo2topo D:\projects\lem\deliverables\us_blck_grp_nonformat.json > D:\projects\lem\deliverables\output.topojson
# ogr2ogr -f GeoJSON units.json ne_10m_admin_0_countries_lakes.shp





# ogr2ogr -f "PostgreSQL" PG:"dbname=ksu_v3 user=mbougie host=144.92.235.105 password=Mend0ta! ACTIVE_SCHEMA=yans_roy" E:\clu_yo.gdb -overwrite -progress --config PG_USE_COPY YES

# ogr2ogr -f "PostgreSQL" PG:"dbname=ksu_v3 user=mbougie host=144.92.235.105 password=Mend0ta! ACTIVE_SCHEMA=yan_roy" D:\projects\ksu\v2\main\yan_roy.gdb -nlt PROMOTE_TO_MULTI -nln yan_roy.yans_roy_5070_erase_singleparts_026_samples_plan_b_4152 yans_roy_5070_erase_singleparts_026_samples_plan_b_4152 -progress --config PG_USE_COPY YES



# ogr2ogr -f "PostgreSQL" PG:"dbname=ksu_v3 user=mbougie host=144.92.235.105 password=Mend0ta!" D:\projects\ksu\v2\main\clu.gdb -nlt PROMOTE_TO_MULTI -nln clu.clu2008county_5070_polygon -progress --config PG_USE_COPY YES



# ogr2ogr -f "PostgreSQL" PG:"dbname=ksu_v3 user=mbougie host=144.92.235.105 password=Mend0ta!" E:\\ksu\\ksu.gdb -nlt PROMOTE_TO_MULTI -nln merged.ksu_polygon ksu_polygons_initial -progress -t_srs EPSG:4152 --config PG_USE_COPY YES 


# ######################################
# ogr2ogr -f "PostgreSQL" PG:"dbname=ksu_v3 user=postgres host=localhost port=5433 password=postgres" E:\\ksu\\merged.gdb -nlt POINT -nln merged.ksu_samples_counties_huc8_mlra_statsgo ksu_samples_counties_huc8_mlra_statsgo -progress --config PG_USE_COPY YES

# ogr2ogr -f "PostgreSQL" PG:"dbname=ksu_v4 user=mbougie host=144.92.235.105 password=Mend0ta!" E:\\ksu\\ksu.gdb -nlt POINT -nln merged.ksu_polygons_5070_samples_states_counties_huc8_statsgo_mlra ksu_polygons_5070_samples_states_counties_huc8_statsgo_mlra -progress -t_srs EPSG:4152 --config PG_USE_COPY YES
# ######################################




# ogr2ogr -f "PostgreSQL" 
#     PG:"host=localhost port=5432 dbname=SampleNY user=postgres" 
#     NYPluto/Pluto.gdb 
#     -overwrite -progress --config PG_USE_COPY YES



















# ogr2ogr -f "FileGDB" -update E:\\ksu\\v4\\ksu_v4.gdb -progress PG:"dbname=ksu_v4 user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM merged.ksu_samples_final" -nln ksu_samples -t_srs EPSG:4152 -nlt POINT 

# ogr2ogr -f "FileGDB" -update E:\\ksu\\merged3.gdb -progress PG:"dbname=ksu_v3 user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT oid,geom FROM merged.ksu_samples" -nln ksu_samples_bb -t_srs EPSG:5070 -nlt POINT 


# ogr2ogr -f "FileGDB" -update E:\\ksu\\merged3.gdb -progress PG:"dbname=ksu_v3 user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM yans_roy.yans_roy_raster_attrib_4152_states_within_final" -nln yans_roy_raster_attrib_4152_states_within_final -t_srs EPSG:5070 -nlt POINT 




# ogr2ogr -f "FileGDB" -update E:\\ksu\\merged3.gdb -progress PG:"dbname=ksu_v3 user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM clu.clu_raster_attrib_4152_states_within_final" -nln clu_raster_attrib_4152_states_within_final -t_srs EPSG:5070 -nlt POINT 








# ogr2ogr -f "topojson" -progress D:\\projects\\lem\\matt\\json\\yo_topo.json PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM blocks.us_blck_grp_2016_mainland_5070_maj_states_wi" -t_srs EPSG:4326'

