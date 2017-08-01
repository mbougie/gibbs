from sqlalchemy import create_engine
import numpy as np, sys, os
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
import general as gen

'''######## DEFINE THESE EACH TIME ##########'''
#NOTE: need to declare if want to process ytc or yfc
yxc = 'yfc'

#the associated mtr value qwith the yxc
yxc_mtr = {'ytc':'3', 'yfc':'4'}

#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")



###################  declare functions  #######################################################
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 






# gen.addGDBTable2postgres(['refinement','refinement'],'*','refinement')

# gen.getPGColumnsList("'refinement'", "'counties_yfc_bfnc'", " , ")


gen.transposeTable(['refinement','refinement'],'counties_yfc_years')


# select 


# SELECT index, GREATEST(value_1 , value_2 , value_3 , value_4 , value_5 , value_6 , value_10 , value_12 , value_21 , value_22 , value_23 , value_24 , value_26 , value_27 , value_28 , value_30 , value_31 , value_34 , value_35 , value_36 , value_38 , value_41 , value_42 , value_43 , value_44 , value_45 , value_46 , value_47 , value_48 , value_49 , value_50 , value_52 , value_53 , value_54 , value_57 , value_58 , value_59 , value_61 , value_66 , value_68 , value_69 , value_71 , value_72 , value_74 , value_75 , value_76 , value_205 , value_206 , value_212 , value_213 , value_214 , value_219 , value_225 , value_226 , value_227 , value_229 , value_230 , value_236 , value_243) 

# FROM 
#   refinement.counties_yfc_bfnc;



#########################  GOOD SQL  ##################################################################

create table refinement.counties_yfc_bfnc_w_percent as

SELECT 
   a.*,
   round(a.acres/b.total_acres * 100,2) as percent 
FROM 
  refinement.counties_yfc_bfnc as a, (SELECT 
   stco, sum(acres) total_acres
FROM 
  refinement.counties_yfc_bfnc
group by stco) as b
where a.stco = b.stco

order by stco, percent desc


################  ANOTHER GOOD SQL #########################################################


# create table refinement.counties_yfc_years_max_percent as 
# select a.*, b.max_years_percent
# from refinement.counties_yfc_years_w_percent as a,  
# (SELECT 
#   stco, max(percent) as max_years_percent
# FROM 
#   refinement.counties_yfc_years_w_percent
# group by stco) as b
# where a.stco = b.stco and a.percent = b.max_years_percent
  



################  ANOTHER GOOD SQL #########################################################

# create table 
# SELECT 
#   counties_yfc_bfnc_max_percent.stco, 
#   counties_yfc_bfnc_max_percent.lc, 
#   counties_yfc_bfnc_max_percent.max_bfnc_percent, 
#   counties_yfc_fnc_max_percent.lc, 
#   counties_yfc_fnc_max_percent.max_fnc_percent, 
#   counties_yfc_years_max_percent.lc, 
#   counties_yfc_years_max_percent.max_years_percent
# FROM 
#   refinement.counties_yfc_bfnc_max_percent, 
#   refinement.counties_yfc_fnc_max_percent, 
#   refinement.counties_yfc_years_max_percent
# WHERE 
#   counties_yfc_bfnc_max_percent.stco = counties_yfc_fnc_max_percent.stco AND
#   counties_yfc_fnc_max_percent.stco = counties_yfc_years_max_percent.stco;