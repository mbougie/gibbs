# Import system modules
import sys 
# sys.path.append("C:\Users\Bougie\Desktop\Gibbs\scripts\usxp\\misc")
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os, subprocess
import fiona
import geopandas
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import fnmatch
import io
import StringIO



arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 



try:
    conn = psycopg2.connect("dbname='lem' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




# def get(query):
#     ###sub-function for reclassRaster
#     engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')
#     print 'query:', query
#     df = pd.read_sql_query(query, con=engine)
#     print df
#     fulllist=[[0,0,"NODATA"]]
#     for index, row in df.iterrows():
#         templist=[]
#         templist.append(row['initial'])
#         templist.append(row['grouped'])
#         fulllist.append(templist)
#     print 'fulllist: ', fulllist
#     return fulllist




# def createZonalStats(in_zone_data, zone_field, in_value_raster, out_table, ignore_nodata, statistics_type):
#     print("createZonalStats().............")
    
#     # env.workspace = 'D:\\projects\\lem\\matt\\gdbases\\{}.gdb'.format(gdb)

#     # out_table_path = 'D:\\projects\\lem\\matt\\gdbases\\{}.gdb\\{}'.format(gdb)

#     # Execute ZonalStatistics
#     ZonalStatisticsAsTable(in_zone_data, zone_field, in_value_raster, out_table, ignore_nodata, statistics_type)


def findNeighbors(in_feature, out_table, in_fields):
    print("findNeighbors().............")

    # env.workspace = 'D:\\projects\\lem\\matt\\gdbases\\{}.gdb'.format(gdb)

    arcpy.PolygonNeighbors_analysis(in_features=in_feature, out_table=out_table, in_fields=in_fields, area_overlap='AREA_OVERLAP', both_sides='BOTH_SIDES', out_linear_units='FEET', out_area_units='ACRES')


############# reclassify raster #####################################################


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
    input_table = '{}.gdb\\{}'.format(gdb, table)

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




def addGDBTable2postgres_io(gdb, schema, table):

    print("addGDBTable2postgres().............")
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')

    # # path to the table you want to import into postgres
    input_table = '{}.gdb\\{}'.format(gdb, table)

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input_table)]

    print fields

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input_table,fields)
    print arr


    # # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)
    print 'df-----------------------', df


    df.head(0).to_sql(table, engine, schema=schema, index=False)#truncates the table


    print df
    # df.fillna(0)
    df = df.fillna(0)
    print df



    conn = engine.raw_connection()

    cur = conn.cursor()

    # output = io.BytesIO
    output = StringIO.StringIO()

    # df.to_csv(output, sep=',', header=False, index_col=0, null='')

    df.to_csv(output, sep='\t', header=False, index=False, null=0)

    output.seek(0)

    contents = output.getvalue()

    cur.copy_from(output, '{0}.{1}'.format(schema, table)) # null values become ''

    conn.commit()









####what is this ????#############################
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


def convertFCtoPG(gdb, pgdb, schema, geomtype, table, epsg):
    command = 'ogr2ogr -f "PostgreSQL" PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" {0} -nlt PROMOTE_TO_MULTI -nln {1}.{2} {2} -progress --config PG_USE_COPY YES'.format(gdb, schema, table)
    
    os.system(command)

    gen.alterGeomSRID(pgdb, schema, table, geomtype, epsg)


def convertPGtoFC(gdb, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update {0}.gdb PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {1}.{2}" -nln {2} -nlt MULTIPOLYGON'.format(gdb, schema, table)
    print command
    os.system(command)


def convertPGtoFC_test(gdb, schema, table, query):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update {0}.gdb PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "" -nln {2} -nlt MULTIPOLYGON'.format(gdb, schema, table, query)
    print command
    os.system(command)


# def getNullPolygons(gdb, schema, in_table, out_table):
#     # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
#     command = 'ogr2ogr -f "FileGDB" -update {0}.gdb -progress PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT a.geoid, majority, wkb_geometry AS geom FROM v1_wisc.block_group AS a JOIN {1}.{2} AS b USING(geoid) WHERE majority IN (11,21)" -nln {3} -nlt MULTIPOLYGON'.format(gdb, schema, in_table, out_table)
#     os.system(command)

# "PG:\"host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem\""

def convertPGtoJSON(version, db, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "geojson" -progress I:\\d_drive\\projects\\lem\\projects\\main\\deliverables\\version\\v3\\{0}\\{3}.json PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {2}.{3}" -t_srs EPSG:4152'.format(version, db, schema, table) 
    print 'command:', command
    os.system(command)







def main(gdb, version, scale, rc_version, levellist):
    print 'main() function............................'
    
    for level in levellist:
        
        print('---------   {}   -----------------------'.format(level))
        #### export the raw census feature class into postgres
        convertFCtoPG(gdb=gdb, pgdb='lem', schema=version, table=level, geomtype='MULTIPOLYGON', epsg=102003)
       
        ##### run the arcgis arcpy.PolygonNeighbors_analysis to get the neighbors of each feature in featureclass
        arcpy.PolygonNeighbors_analysis(in_features='{0}\\{1}'.format(gdb,level), out_table='{0}.gdb\\{1}_neighbors'.format(version,level), in_fields="geoid")
        addGDBTable2postgres_io(gdb=version, schema=version, table="{}_neighbors".format(level))

        ####### create majority zonal stats with raw NWALT raster #####################################################
        ZonalStatisticsAsTable(in_zone_data='{0}\\{1}'.format(gdb,level), zone_field="geoid", in_value_raster="rasters.gdb\\nwalt_{0}m".format(scale), out_table='{0}.gdb\\{2}_zonal_maj_nwalt_{1}m'.format(version, scale, level), ignore_nodata="DATA", statistics_type="MAJORITY")
        addGDBTable2postgres_io(gdb=version, schema=version, table="{0}_zonal_maj_nwalt_{1}m".format(level, scale))

        ###### create zonal stats with BIOMES raster  #####################################################
        ZonalStatisticsAsTable(in_zone_data='{0}\\{1}'.format(gdb,level), zone_field="geoid", in_value_raster="rasters.gdb\\biomes_vegtype_conus_exp10_{}m".format(scale), out_table='{0}.gdb\\{2}_zonal_maj_biomes_{1}m'.format(version, scale, level), ignore_nodata="DATA", statistics_type="MAJORITY")
        addGDBTable2postgres_io(gdb=version, schema=version, table="{0}_zonal_maj_biomes_{1}m".format(level, scale))


        ####### create majority zonal stats with reclassed NWALT raster  #####################################################
        ZonalStatisticsAsTable(in_zone_data='{0}\\{1}'.format(gdb,level), zone_field="geoid", in_value_raster="rasters.gdb\\nwalt_{0}m_rc_v2".format(scale), out_table='{0}.gdb\\{2}_zonal_maj_nwalt_rc_{1}m'.format(version, scale, level), ignore_nodata="DATA", statistics_type="MAJORITY")
        addGDBTable2postgres_io(gdb=version, schema=version, table="{0}_zonal_maj_nwalt_rc_{1}_{2}m".format(level, rc_version, scale))




        ####run refinement to fill polygons that need to be filled#####

        #---------NEED to do this in pg_admin III-----------------------




        ###execute .sql test file############################Still nedd to write!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # convertPGtoFC(gdb=version, schema=version, table='{0}_v3_main_refinement_view'.format(level))






def refine(gdb, version, scale, rc_version, levellist):
    print 'refine() function............................'
    
    for level in levellist:
        
        print('---------   {}   -----------------------'.format(level))

        in_zone_data = '{0}_refine_nwalt21_and_null'.format(level)
        

        #### convert the refine postgres view into a table
        # convertPGtoFC(gdb=version, schema=version, table=in_zone_data)

        ############################################################

        # ####### create majority zonal stats with reclassed NWALT raster  #####################################################
        ZonalStatisticsAsTable(in_zone_data="{0}.gdb\\{1}".format(version, in_zone_data), zone_field="geoid", in_value_raster="rasters.gdb\\nwalt_{0}m_rc_v2".format(scale), out_table='{0}.gdb\\{2}_zonal_maj_nwalt_rc_v2_{1}m'.format(version, scale, level), ignore_nodata="DATA", statistics_type="MAJORITY")
        addGDBTable2postgres_io(gdb=version, schema=version, table="{0}_zonal_maj_nwalt_rc_v2_{1}m".format(level, scale))

        # # ###### create zonal stats with biomes raster  #####################################################
        # # ######note: only creating this dataset because some polygons might be too small for all rasters ---i.e. across the board for all zonal stats they are null
        ZonalStatisticsAsTable(in_zone_data="{0}.gdb\\{1}".format(version, in_zone_data), zone_field="geoid", in_value_raster="rasters.gdb\\biomes_vegtype_conus_exp10_{}m".format(scale), out_table='{0}.gdb\\{2}_zonal_maj_biomes_{1}m'.format(version, scale, level), ignore_nodata="DATA", statistics_type="MAJORITY")
        addGDBTable2postgres_io(gdb=version, schema=version, table="{0}_zonal_maj_biomes_{1}m".format(level, scale))




# def getDeliverables(version, levellist):
#     print 'deliverables() function............................'
    
#     for level in levellist:
#         print('---------   {}   -----------------------'.format(level))

#         convertPGtoJSON(version, db, schema=, table=level)
#         convertPGtoJSON(version, 'lem', version, level)







def addIndexField():
    #this is a sub function for addGDBTable2postgres()
    cur = conn.cursor()

    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE v3_2.block_v3_main ADD COLUMN index serial;');
    
    conn.commit()
    print "Records created successfully";
    conn.close()






##############################################################################################
###call function#####################################################################
##############################################################################################

###  define the working directory  ###########################################
os.chdir('I:\\d_drive\\projects\\lem\\data\\gdbases\\main')

#####  prep functions (relcassify rasters) ################################################################
# arcpy.env.workspace = 'I:\\d_drive\\projects\\lem\\data\\gdbases\\main\\rasters.gdb'
# reclassRaster(inraster="nwalt_60m", outraster="nwalt_60m_rc_v2", query="SELECT initial, reclass_v2 FROM public.nwalt_lookup WHERE reclass_v2 IS NOT NULL")
# reclassRaster(inraster="nwalt_30m", outraster="nwalt_30m_rc_v2", query="SELECT initial, reclass_v2 FROM public.nwalt_lookup WHERE reclass_v2 IS NOT NULL")
# reclassRaster(inraster="nwalt_10m", outraster="nwalt_10m_rc_v2", query="SELECT initial, reclass_v2 FROM public.nwalt_lookup WHERE reclass_v2 IS NOT NULL")


####  run the main function  ############################################
# main(gdb='census_features.gdb', version='v3_3', scale='60', rc_version='v2', levellist=['county', 'tract', 'block_group'])


### run the refinement functions #############################
# refine(gdb='v3_3.gdb', version='v3_3', scale='10', rc_version='v2', levellist=['block'])
# refine(gdb='v3_3.gdb', version='v3_3', scale='10', rc_version='v2', levellist=['block_group'])
 
###  create the deliverables products: feature class and json dataset  ############################
# convertPGtoJSON(version='v3_3', db='lem', schema='v3_3_final', table='county_final')
# convertPGtoJSON(version='v3_3', db='lem', schema='v3_3_final', table='tract_final')
convertPGtoJSON(version='v3_3', db='lem', schema='v3_3_final', table='block_final')
# getDeliverables('v3_3', ['block_group'])
# getDeliverables('v3_3', ['block'])




################### QAQC ###################################
# convertPGtoFC(gdb='qaqc', schema='v3_3_qaqc', table='v3_3_block_main_nwalt_11_21_60_null')

































############# sandbox #########################################






# def convertPGtoFC(gdb, schema, table):
#     # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
#     command = 'ogr2ogr -f "FileGDB" -progress -update {0}.gdb PG:"dbname=ksu_v4 user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {1}.{2}" -nln {2} -nlt POINT'.format(gdb, schema, table)
#     print command
#     os.system(command)



# convertPGtoFC(gdb='I:\\temp\\rda_unique_id_attributes', schema='merged', table='rda_unique_id_attributes')












# try:
#     conn = psycopg2.connect("dbname='lem' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
# except:
#     print "I am unable to connect to the database"





# with open("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\lem\\sql\\v1\\v2.block_group_v2_table.sql", 'r') as file:

#     query = '''{}'''.format(file.read())

#     cur = conn.cursor()
#     cur.execute(query);



# convertPGtoFC(gdb='v3_1.gdb', schema='v3_1', table='test_negihnbor_v5')


# addIndexField()
# convertPGtoFC(gdb='v3_2', schema='v3_2', table='block_mod_t1')




































