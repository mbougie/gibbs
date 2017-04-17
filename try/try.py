# from sqlalchemy import create_engine
# import numpy as np, sys, os
from osgeo import gdal
from osgeo.gdalconst import *
# # from pandas import read_sql_query
# import pandas as pd
# # import tables
# import collections
# from collections import namedtuple
# import openpyxl
# import arcpy
# from arcpy import env
# from arcpy.sa import *
# import glob
# import psycopg2
import subprocess


file_in = 'D:/gibbs/refinement/traj_n8h_mtr_8w_msk45_nbl1.img'
file_out = 'D:/gibbs/refinement/traj_n8h_mtr_8w_msk45_nbl1_sieve.img'
# mask='C:/Users/bougie/Desktop/gibbs/usxp/tiffs/try_mask.img'


# merge_command = ["python", "C:/Python27/ArcGISx6410.4/Lib/site-packages/osgeo/scripts/gdal_fillnodata.py", file_in,"-of", "HFA","-md", "30", file_out]
# subprocess.call(merge_command)

# gdal_sieve.py [-q] [-st threshold] [-4] [-8] [-o name=value] srcfile [-nomask] [-mask filename] [-of format] [dstfile]

merge_command = ["python", "C:/Python27/ArcGISx6410.4/Lib/site-packages/osgeo/scripts/gdal_sieve.py", file_in,"-of", "HFA", file_out, "-st", "45", "-4"]
subprocess.call(merge_command)













###### PRE STEP BEFORE USING THIS FUNCTION #################################################
# Con(IsNull("traj_n8h_mtr_8w_mask68_dane"),"traj_n8h_mtr_8w_mask68_dane","traj_n8h_mtr_dane")



# Con(IsNull("traj_n8h_mtr_mask68applied_dane.img"), FocalStatistics("traj_n8h_mtr_mask68applied_dane.img", NbrRectangle(10,10, "CELL"), "MAJORITY"), "traj_n8h_mtr_mask68applied_dane.img")