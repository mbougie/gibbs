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





# def reclassifyRaster(data, mask):

#     dict_qaqc = {"mask_fp_2007":{"traj":data['pre']['traj']['path'], "output":"C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\qaqc.gdb\\qaqc_mask_fp_2007_traj", "query":"SELECT \"Value\" from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE ytc=2009 or yfc=2009".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])},
#                  "mask_fp_nlcd_yxc":{"traj":data['pre']['traj_yfc']['path'], "output":"C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s35\\qaqc.gdb\\qaqc_mask_fp_nlcd_yxc_traj", "query":"SELECT \"Value\" from pre.{} as a JOIN pre.{} as b ON a.traj_array = b.traj_array WHERE mtr=3 OR mtr=4".format(data['pre']['traj']['filename'], data['pre']['traj']['lookup_name'])}
#                 }

#     print "trajectory:", dict_qaqc[mask]["traj"]
#     print "query:", dict_qaqc[mask]["query"]
    
#     ###reclassify the filtered raster to the MTR labels
#     out_reclass = Reclassify(dict_qaqc[mask]["traj"], "Value", RemapRange(createReclassifyList(dict_qaqc[mask]["query"])), "NODATA")
    
#     print dict_qaqc[mask]["output"]
    
#     ###save object as raster
#     out_reclass.save(dict_qaqc[mask]["output"])
    
#     # ###build pyramids
#     gen.buildPyramids(dict_qaqc[mask]["output"])




# def createReclassifyList(query):
#     engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
#     df = pd.read_sql_query(query, con=engine)
#     print df
#     fulllist=[[0,0,"NODATA"]]
#     for index, row in df.iterrows():
#         templist=[]
#         value=row['Value'] 
#         templist.append(int(value))
#         templist.append(int(value))
#         fulllist.append(templist)
#     print 'fulllist: ', fulllist
#     return fulllist




def getDatasetName(dataset):
    df.loc[df['value'] == 3]



def getCounts():
    df['count'].sum()


 




def run(data):
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    ##reclass the trajectory 
    print('----------------------------------------------------------------')
    # mtr = (data['core']['path'])

    series_dict = {'mtr':data['core']['path'], 
                   'ytc':data['post']['ytc']['path'],
                   'bfc':data['post']['ytc']['bfc']['path'],
                   'fc':data['post']['ytc']['fc']['path'],
                   'yfc':data['post']['yfc']['path'],
                   'bfnc':data['post']['yfc']['bfnc']['path'],
                   'fnc':data['post']['yfc']['fnc']['path']}


    # series_dict = {'mtr':data['core']['path']}


    final_dict = {'dataset':[], 'count':[]}

    for dataset, path in series_dict.iteritems():
        print 'dataset-path:', dataset, path
        df = gen.GDBTable2DF(pgdb='qaqc', currentobject=path)
        print '-------------- returned df ----------------------'
        print df
        
        if dataset == 'mtr':
            final_dict['dataset'].append('mtr_3')
            print 'yo-----------------------'
            # print df.loc[df['value'] == 3]
            final_dict['count'].append(df.at[2, 'count'])
            # final_dict['count'].append(df.loc[df['value'] == 3])

            final_dict['dataset'].append('mtr_4')
            final_dict['count'].append(df.at[3, 'count'])
            # final_dict['count'].append(df.loc[df['value'] == 4])


        else:
            final_dict['dataset'].append(dataset)
            final_dict['count'].append(df['count'].sum())


    #     series_dict.update({dataset:df})


    #     final_dict['dataset'].append(dataset)

    #     final_dict['count'].append()




    # print('--------final--------------------')
    # print(series_dict['mtr'])


    













    df = pd.DataFrame.from_dict(final_dict)

    print df


    df.to_sql('s35_series', engine, schema='qaqc')












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