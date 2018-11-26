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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
# import general_deliverables as gen_dev

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")





def processingCluster(instance, inraster, outraster):
    #####  reclass  ####################################################
    ## Reclassify (in_raster, reclass_field, remap, {missing_values})
    blockstats_objects = {}

    for mtr, reclasslist in instance['reclass'].iteritems():
        reclass_raster = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
        print 'finished reclass_raster.............'
        reclass_raster_setnull = Con(IsNull(reclass_raster), 0, reclass_raster)
        print 'reclass_raster_setnull.............'
        for key, value in instance['scale'].iteritems():

            nbr = NbrRectangle(value, value, "CELL")
            outBlockStat = BlockStatistics(reclass_raster_setnull, nbr, "SUM", "DATA")
            print 'finished block stats.............'

            blockstats_objects[mtr] = outBlockStat

            print 'blockstats_objects ', blockstats_objects 


    print 'test', blockstats_objects 

    net_raster = Minus(blockstats_objects['mtr3'], blockstats_objects['mtr4'])

    net_raster.save(outraster)

    addField(outraster, value)   

    gen.buildPyramids_new(outraster, 'NEAREST')






def addField(raster, value):
    normalizer = value*value
    ##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
    arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

    cur = arcpy.UpdateCursor(raster)

    for row in cur:
        row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
        cur.updateRow(row)






def main(instance):
    inraster=Raster('D:\\projects\\usxp\\deliverables\\{0}\\{0}.gdb\\{0}_mtr'.format(instance['series']))
    print 'inraster', inraster

    # for key, reclasslist in instance['reclass'].iteritems():
    for scale in instance['scale'].keys():
        outraster = 'D:\\projects\\usxp\\deliverables\\{0}\\maps\\gross_net\\net.gdb\\{0}_{1}_net'.format(instance['series'], scale)
        print 'outraster', outraster

        processingCluster(instance, inraster, outraster)






instance = { 'scale':{'3km':100}, 'series':'s35', 'reclass':{'mtr3':[[3,1]], 'mtr4':[[4,1]]} }
main(instance)











