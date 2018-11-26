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






myArray = np.loadtxt(open("D:\\projects\\lem\\matt\\control\\CONUS_biomes_na_to_0.csv", "rb"), delimiter=",")
print myArray
print type(myArray)
print myArray.shape

# myRaster = arcpy.NumPyArrayToRaster(np.flipud(np.transpose(myArray)), arcpy.Point(-125.1666692,22.99999492), 0.1666666666666667, 0.1666666666666667)
myRaster = arcpy.NumPyArrayToRaster(myArray, arcpy.Point(-125.1666692,22.99999492), 0.1666666666666667, 0.1666666666666667)

myRaster.save("D:\\projects\\lem\\matt\\control\\carol\\conus_biomes_intial.tif")


