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



##DESCRIPTION: create net raster from subtracting mtr4 object from mtr3 object



# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")





def processingCluster(instance, inraster, outraster):
    ###Steps:
    ###1) reclass mtr value to 1  --- so can use the sum agregation fparameter in bloack stats
    ###2) set the null values to 0
    ###3) apply block stats to add all the 1 values within a block
    ###4) append this blockstat object to blockstats_objects dictionary to perform minus function to create net_raster


    ##create a empty dictionary to hold each gross object (abandonment and expansion) in order to derive net raster
    blockstats_objects = {}

    for mtr, reclasslist in instance['reclass'].iteritems():
        ##reclass each mtr object as 1
        reclass_raster = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
        print 'finished reclass_raster.............'
        ##reclass each null to zero
        reclass_raster_setnull = Con(IsNull(reclass_raster), 0, reclass_raster)
        print 'reclass_raster_setnull.............'
        for key, value in instance['scale'].iteritems():

            nbr = NbrRectangle(value, value, "CELL")
            ###sum all the values within the block
            outBlockStat = BlockStatistics(reclass_raster_setnull, nbr, "SUM", "DATA")
            print 'finished block stats.............'
            ###add processed object to the blockstats_objects
            blockstats_objects[mtr] = outBlockStat

            print 'blockstats_objects ', blockstats_objects 


    print 'blockstats_objects completed---', blockstats_objects 

    ##derive net from the 2 objects stored in blockstats_objects
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
        outraster = 'D:\\projects\\usxp\\deliverables\\maps\\gross_net\\net.gdb\\{0}_{1}_net'.format(instance['series'], scale)
        print 'outraster', outraster

        processingCluster(instance, inraster, outraster)






instance = { 'scale':{'6km':200}, 'series':'s35', 'reclass':{'mtr3':[[3,1]], 'mtr4':[[4,1]]} }
main(instance)











