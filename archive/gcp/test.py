from sqlalchemy import create_engine
import numpy as np, sys, os
import gdal
import subprocess
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
import fnmatch
import rasterstats

# import general as gen 

case=['Bougie','Gibbs']

try:
    conn = psycopg2.connect("dbname='ksu' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




# import os

# psql --host=144.92.235.105 --port=5432 --username=mbougie --dbname=ksu
def exportPGtoCSV():
    cur = conn.cursor()
    sys.stdout = open('C:\\Users\\Bougie\\Desktop\\Gibbs\\temp\\testit.csv', 'w')
    

    cur.copy_expert("COPY samples.samples_al TO STDOUT WITH CSV HEADER", sys.stdout)
    
    conn.commit()
    print "Records created successfully";
    conn.close




####  call functions  ##############################
tryit()