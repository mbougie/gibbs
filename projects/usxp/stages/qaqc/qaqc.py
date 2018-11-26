# Import system modules
import sys 
# sys.path.append("C:\Users\Bougie\Desktop\Gibbs\scripts\usxp\\misc")
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os
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





def reclassifyRaster(data, mask):

    dict_qaqc = {"mask_fp_2007":{"traj":data['pre']['traj']['path'], "output":"C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\qaqc.gdb\\qaqc_mask_fp_2007_traj", "query":"SELECT \"Value\" from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc=2009 or yfc=2009".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])},
                 "mask_fp_nlcd_yxc":{"traj":data['pre']['traj_yfc']['path'], "output":"C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\qaqc.gdb\\qaqc_mask_fp_nlcd_yxc_traj", "query":"SELECT \"Value\" from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE mtr=3 OR mtr=4".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])}
                }

    print "trajectory:", dict_qaqc[mask]["traj"]
    print "query:", dict_qaqc[mask]["query"]
    
    ###reclassify the filtered raster to the MTR labels
    out_reclass = Reclassify(dict_qaqc[mask]["traj"], "Value", RemapRange(createReclassifyList(dict_qaqc[mask]["query"])), "NODATA")
    
    print dict_qaqc[mask]["output"]
    
    ###save object as raster
    out_reclass.save(dict_qaqc[mask]["output"])
    
    # ###build pyramids
    gen.buildPyramids(dict_qaqc[mask]["output"])




def createReclassifyList(query):
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query(query, con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        value=row['Value'] 
        templist.append(int(value))
        templist.append(int(value))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist




 




def run(data):
    ##reclass the trajectory 
    reclassifyRaster(data, 'mask_fp_nlcd_yxc')

    ###preform combine function
    ###still need to do this !!!!!!!!!!!!!!

    ###import into postgres##########################################################################################################
    # path = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\qaqc.gdb\\qaqc_mask_fp_2007_combine'
    # filename = 'qaqc_mask_fp_2007'
    # path = 


    # database='usxp'
    # schema='qaqc_refine'
    # gen.addRasterAttrib2postgres_recent(path, filename, database, schema)








if __name__ == '__main__':
    run(data)