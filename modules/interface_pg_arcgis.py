# import psycopg2
# import pandas as pd
# from sqlalchemy import create_engine
# import arcpy
# from arcpy import env
# from arcpy.sa import *
# import os, errno
# import glob
# import math
# import json
# import shutil


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
import general as gen
import json
import fnmatch



arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 



try:
    conn = psycopg2.connect("dbname='lem' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"






try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"








def convertFCtoPG(gdb, pgdb, schema, table):
    command = 'ogr2ogr -f "PostgreSQL" PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" {0} -nlt PROMOTE_TO_MULTI -nln {2}.{3} {3} -progress --config PG_USE_COPY YES'.format(gdb, pgdb, schema, table)
    
    os.system(command)

    # gen.alterGeomSRID(pgdb, schema, table, epsg)


def convertPGtoFC(gdb, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update D:\\projects\\lem\\matt\\gdbases\\{0}.gdb PG:"dbname=lem user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {1}.{2}" -nln {2} -nlt MULTIPOLYGON'.format(gdb, schema, table)
    print command
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