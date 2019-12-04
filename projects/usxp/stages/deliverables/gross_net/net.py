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

    ###Steps----------------------------------------------:
    ###1) reclass mtr value to 1  --- so can use the sum agregation fparameter in bloack stats
    ###2) set the null values to 0
    ###3) apply block stats to add all the 1 values within a block
    ###4) append this blockstat object to raster_objects dictionary to perform minus function to create net_raster


    ##create a empty dictionary to hold each gross object (abandonment and expansion) in order to derive net raster
    raster_objects = {}

    ####get global variables
    def getKeyValue():
        for key, value in instance['scale'].iteritems():
            return key, value
    scale_km,cell_factor = getKeyValue()
    print scale_km
    print cell_factor
    

    for key, value in instance['dataset'].iteritems():
        print key
        print value
        reclasslist = instance['dataset'][key]['reclasslist']
        print reclasslist
        outraster_mtr = instance['dataset'][key]['outraster']
        print outraster_mtr

        #reclass each mtr object as 1
        reclass_raster = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
        print 'finished reclass_raster.............'
        ##reclass each null to zero
        reclass_raster_setnull = Con(IsNull(reclass_raster), 0, reclass_raster)
        print 'reclass_raster_setnull.............'

        # Execute Aggregate
        outAggreg = Aggregate(in_raster=reclass_raster_setnull, cell_factor=cell_factor, aggregation_type="SUM", extent_handling="EXPAND", ignore_nodata="DATA")
        
        print('mtr raster saved')
        outAggreg.save(outraster_mtr)
        
        raster_objects[key] = outAggreg

 




        print 'raster_objects ', raster_objects 


    print 'raster_objects completed---', raster_objects 

    ##derive net from the 2 objects stored in raster_objects
    net_raster = Minus(raster_objects['mtr3'], raster_objects['mtr4'])

    net_raster.save(outraster)

    addField(outraster, cell_factor)   







def addField(raster, cell_factor):
    normalizer = cell_factor*cell_factor
    ##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
    arcpy.AddField_management(in_table=raster, field_name='percent', field_type='INTEGER')

    cur = arcpy.UpdateCursor(raster)

    for row in cur:
        row.setValue('percent', round((float(row.getValue('Value'))/normalizer)*100))
        cur.updateRow(row)



def main(instance):

    inraster=Raster('I:\\d_drive\\projects\\usxp\\series\\{0}\\{0}.gdb\\{0}_mtr'.format(instance['series']))
    print 'inraster', inraster

    # for key, reclasslist in instance['reclass'].iteritems():
    for scale in instance['scale'].keys():
        outraster = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net\\tif\\{0}_net_agg3km.tif'.format(instance['series'])
        print 'outraster', outraster

        processingCluster(instance, inraster, outraster)






instance = { 'scale':{'3km':100}, 
             'series':'s35', 
             'dataset':{'mtr3':{"reclasslist":[[3,1]],"outraster":"I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net\\tif\\{}_expand_agg3km.tif".format('s35')}, 
                        'mtr4':{"reclasslist":[[4,1]],"outraster":"I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net\\tif\\{}_abandon_agg3km.tif".format('s35')}
                        }
            }
main(instance)











