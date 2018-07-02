from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
import geopandas as gpd
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
import subprocess


try:
    conn = psycopg2.connect("dbname='intact_lands' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"









































#===========================================================================
#============  metadata functions  =========================================
#===========================================================================

#__________create the metatable for all states
# createMetaTable()

#_________  fill out  ____________________________________
# popCounts_years()



#===========================================================================
#============  processing functions  =======================================
#===========================================================================

#__________reproject the max date sf for a given year and state and store as fc in geodatabase
# mainProjectSF()


#________  merge datasets by state and year _____________________________________________________________
# mainMergeFC()


#_________  create the merged subset feature classes  ____________________________________
# mainSubsetFC()


#_________  fill out  ____________________________________
# mergeAllyearsByState()


#_________  fill out  ____________________________________
# changeCRPvaluesMT()


#_________  fill out  ____________________________________
# mergeAllyearsAllstates()




























