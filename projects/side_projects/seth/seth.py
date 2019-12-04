import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from numpy import copy
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




pgdb = 'side_projects'
csv_path = 'I:\\d_drive\\projects\\side_projects\\seth\\gadm36_0\\gadm36_0\\global_forest_origin_forJoin.csv'
table = 'global_forest_origin_forJoin'
schema = 'seth'

gen.importCSVtoPG(pgdb, csv_path, table, schema)


