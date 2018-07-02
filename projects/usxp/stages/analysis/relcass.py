import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general as gen
import json


#import extension
arcpy.CheckOutExtension("Spatial")
# # arcpy.env.parallelProcessingFactor = "95%"
# arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 



def createReclassifyList():
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    # query = " SELECT \"Value\", mtr from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])
    query = "SELECT a.\"Value\", b.ytc FROM pre.v4_traj_lookup_2008to2017_v3 as b,pre.v4_traj_cdl30_b_2008to2017 as a WHERE b.traj_array = a.traj_array and hard is not null"
    print 'query:', query
    df = pd.read_sql_query(query, con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        value=row['Value'] 
        mtr=row['ytc']  
        templist.append(int(value))
        templist.append(int(mtr))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist

 


rg_combos = {'4w':["FOUR", "WITHIN"], '8w':["EIGHT", "WITHIN"], '4c':["FOUR", "CROSS"], '8c':["EIGHT", "CROSS"]}
rg_instance = rg_combos['8w']

# for count in masks_list:
cond = "Count < "
print 'cond: ',cond




traj=Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj\\v4\\v4_traj.gdb\\v4_traj_cdl30_b_2008to2017')
    
    
####reclassify the filtered raster to the MTR labels
raster_yxc = Reclassify(traj, "Value", RemapRange(createReclassifyList()), "NODATA")
traj=None

# ### perform region group on the raster_yxc to get the number of pixels for each region
# raster_rg = RegionGroup(raster_yxc, rg_instance[0], rg_instance[1], "NO_LINK")
    
### set null the regions that are less than the mmu treshold
# raster_mask = SetNull(raster_rg, raster_yxc, cond)
# raster_yxc=None
# raster_rg=None




# outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/data/", r"tiles", outname)

# raster_shrink.save(outpath)
raster_yxc.save('D:\\projects\\usxp\\qaqc.gdb\\ytc_hard')
raster_yxc=None









