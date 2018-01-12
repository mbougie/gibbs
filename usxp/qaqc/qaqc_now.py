from sqlalchemy import create_engine
import numpy as np, sys, os
import fnmatch
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen




'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"




def getGDBpath(wc):
    for root, dirnames, filenames in os.walk("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\"):
        for dirnames in fnmatch.filter(dirnames, '*{}*.gdb'.format(wc)):
            print dirnames
            gdbmatches = os.path.join(root, dirnames)
    print gdbmatches
    # return json.dumps(gdbmatches)
    return gdbmatches




data = gen.getJSONfile()
print data




def main_setUpGDB():

    states =  gen.getStatesField("st_abbrev")
    for state in states:
        print state
        # arcpy.CreateFileGDB_management(rootpath+'qaqc', state.lower()+".gdb")
        years = [2014,2015,2016]
        for year in years:
            importRasterToGDB(state,year)





def importRasterToGDB(state,year):

  matches = []
  for root, dirnames, filenames in os.walk('E:\\data\\CDL_confidence\\{}'.format(str(year))):
    for filename in fnmatch.filter(filenames, '*_'+state+'_*.img'):
      print 'filename:', filename[:-4]
      matches.append(os.path.join(root, filename))
      print matches
      arcpy.CheckOutExtension("Spatial")


      ######Use try/except to skip the states that dont have all years
      try:
          arcpy.CopyRaster_management(matches[0],defineGDBpath(['qaqc',state])+filename[:-4],"DEFAULTS","0","","","","8_BIT_UNSIGNED")
      except:
          pass










def main_createFC():

    states = gen.getStatesField("st_abbrev")
    for state in states:
        if state == 'IA':
            print state
            pgTableToFC(state)
            years = [2010,2011,2012,2013]
            for year in years:
                zonalstatsTable(state,year)






### run zonal stats
def zonalstatsTable(state,year):
    arcpy.env.workspace = defineGDBpath(['qaqc',state.lower()])

    #define arguments
    sf = state.lower()
    zone_field = "LINK"
    in_value_raster = 'cdl_30m_r_'+state+'_'+str(year)+'_albers_confidence'
    out_table = sf+'_'+str(year)+'_conf'

    # Check out the ArcGIS Spatial Analyst extension license
    arcpy.CheckOutExtension("Spatial")

    # Execute ZonalStatisticsAsTable
    ZonalStatisticsAsTable(sf, zone_field, in_value_raster, out_table, "DATA", "ALL")

    gen.addGDBTable2postgres(['qaqc',state.lower()],'*_'+str(year)+'_*','qaqc')
    






def pgTableToFC(state):
    sf = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/qaqc/sf/'+state.lower()+'.shp'
    sf_acea = sf.replace(".shp", "_acea")

    command = 'pgsql2shp -f "'+sf+'" -h 144.92.235.105 -u mbougie -P Mend0ta! usxp "SELECT combo_bfc_fc_ytc_spatial.gid, combo_bfc_fc_ytc_spatial.gid::text as link, combo_bfc_fc_ytc.year, combo_bfc_fc_ytc_spatial.geom FROM qaqc.combo_bfc_fc_ytc, qaqc.combo_bfc_fc_ytc_spatial,spatial.states WHERE combo_bfc_fc_ytc_spatial.gridcode = combo_bfc_fc_ytc.value and st_within(combo_bfc_fc_ytc_spatial.geom, states.geom) and st_abbrev = \''+state+'\' "'
    print command
    os.system(command)

    ### reproject shapefile to acea  ---still need
    print 'define reprojection'
    sr = arcpy.SpatialReference(5070)
    print sr
    arcpy.DefineProjection_management(sf, sr)

    print 'reprojection'
    arcpy.Project_management(sf, defineGDBpath(['qaqc',state])+state.lower(), "PROJCS['Albers_Conical_Equal_Area',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['false_easting',0.0],PARAMETER['false_northing',0.0],PARAMETER['central_meridian',-96.0],PARAMETER['standard_parallel_1',29.5],PARAMETER['standard_parallel_2',45.5],PARAMETER['latitude_of_origin',23.0],UNIT['Meters',1.0]]", "", "PROJCS['NAD_1983_UTM_Zone_15N',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-93.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", "NO_PRESERVE_SHAPE", "", "NO_VERTICAL")









def testit():
    cur = conn.cursor()


    query = """Create Table qaqc.ia_merged as 
              SELECT 
              combo_bfc_fc.bfc, 
              combo_bfc_fc.fc, 
              combo_bfc_fc_ytc.value, 
              combo_bfc_fc_ytc.year,
              st_area(combo_bfc_fc_ytc_spatial.geom) * 0.000247105 as acres,
              ia_2010_conf.mean as conf_2010,
              ia_2011_conf.mean as conf_2011,
              ia_2012_conf.mean as conf_2012,
              ia_2013_conf.mean as conf_2013
            FROM 
              qaqc.combo_bfc_fc, 
              qaqc.combo_bfc_fc_ytc, 
              qaqc.combo_bfc_fc_ytc_spatial, 
              qaqc.ia_2010_conf,
              qaqc.ia_2011_conf,
              qaqc.ia_2012_conf,
              qaqc.ia_2013_conf
            WHERE 
              combo_bfc_fc.value = combo_bfc_fc_ytc.conv_traj AND
              combo_bfc_fc_ytc.value = combo_bfc_fc_ytc_spatial.gridcode AND
              combo_bfc_fc_ytc_spatial.gid::text = ia_2010_conf.link AND
              ia_2010_conf.link = ia_2011_conf.link AND
              ia_2011_conf.link = ia_2012_conf.link AND
              ia_2012_conf.link = ia_2013_conf.link
            order by acres desc"""


    print query
    cur.execute(query)
    conn.commit()
    # query.replace('ia_', 'mo_')
    # print query.replace('ia_', 'mo_')

    # query.replace('a', '%temp%').replace('b', 'a').replace('%temp%', 'b')
    




def rasterToPoly():
  #####  Set environment settings
  env.workspace = data['core']['gdb']
  print env.workspace

  rasters = arcpy.ListRasters("*", "All")
  for raster in rasters:
    print(raster)
    outPolygons = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\r2_2\\sf\\{}.shp".format(raster)
    field = "VALUE"

    ####  Execute RasterToPolygon
    arcpy.RasterToPolygon_conversion(raster, outPolygons, "NO_SIMPLIFY", field)



####################### call main functions ##########################################################################

### functions for confidence datasets
# main_setUpGDB()
# main_createFC()



#### functions to export shapefile 
# testit()
rasterToPoly()
