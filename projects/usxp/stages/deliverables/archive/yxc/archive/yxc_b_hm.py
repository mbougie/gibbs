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
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
# import general_deliverables as gen_dev
import json



# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")




def processingCluster(instance, inraster, outraster, reclasslist):
    #####  reclass  ####################################################
    ## Reclassify (in_raster, reclass_field, remap, {missing_values})
    outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
    print 'finished outReclass..............'


    ######  block stats  ###############################################
    ##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})
    
    for key, value in instance['scale'].iteritems():

        nbr = NbrRectangle(value, value, "CELL")
        outBlockStat = BlockStatistics(outReclass, nbr, "SUM", "DATA")
        print 'finished block stats.............'
        outBlockStat.save(outraster)


        addField(outraster, value)


        gen.buildPyramids_new(outraster, "NEAREST")





def addField(raster, value):
        normalizer = value*value
        ##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
        arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

        cur = arcpy.UpdateCursor(raster)

        for row in cur:
            row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
            cur.updateRow(row)






def styleLayer(in_raster, layername, mxd):
    ### ApplySymbologyFromLayer_management (in_layer, in_symbology_layer, {symbology_fields}, {update_symbology})
    ### Purpose: apply the symbology from one layer to another


    # Set layer to apply symbology to
    inputLayer = "D:\\projects\\usxp\\deliverables\\groups\\yxc\\ytc\\layers\\{}.lyr".format(layername)

    arcpy.MakeRasterLayer_management (in_raster, inputLayer)

    # # Set layer that output symbology will be based on
    symbologyLayer = "D:\\projects\\usxp\\deliverables\\groups\\yxc\\ytc\\layers\\ytc_{}_template.lyr".format(res)

    # # Apply the symbology from the symbology layer to the input layer
    arcpy.ApplySymbologyFromLayer_management(inputLayer, symbologyLayer)

    arcpy.SaveToLayerFile_management(inputLayer, inputLayer)

    addLayerToGroup(inputLayer, layername, mxd)






def addLayerToGroup(inputLayer, layername, mxd):
    # mxd = arcpy.mapping.MapDocument('D:\\projects\\usxp\\deliverables\\groups\\yxc\\yxc.mxd')
    df = arcpy.mapping.ListDataFrames(mxd, "*")[0]
    print df.name
    targetGroupLayer = arcpy.mapping.ListLayers(mxd, "groupholder", df)[0]
    addLayer = arcpy.mapping.Layer(inputLayer)
    arcpy.mapping.AddLayerToGroup(df, targetGroupLayer, addLayer, "BOTTOM")
    del addLayer

    exportLayerToPDF(layername, mxd)

    deleteLayer(layername, mxd)

    







    

def exportLayerToPDF(layername, mxd):
    arcpy.mapping.ExportToPDF(mxd, r"C:\\Users\\Bougie\\Desktop\\temp\\test_pdf\\{}.pdf".format(layername))
    del mxd


def deleteLayer(layername, mxd):
    for df in arcpy.mapping.ListDataFrames(mxd):
        for lyr in arcpy.mapping.ListLayers(mxd, "*{}*".format(layername), df):
            print 'lyr----------------', lyr
            # if lyr.name.lower() == "*{}*".format(layername):
            arcpy.mapping.RemoveLayer(df, lyr)


def changeTitle(mxd, year):
    for elm in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
        elm.text = str(year)
        mxd.save()
            



def main(instance):

    for key, reclasslist in instance['reclass'].iteritems():

        print key, reclasslist

        inraster=Raster('D:\\projects\\usxp\\deliverables\\{0}\\{0}.gdb\\{0}_{1}'.format(instance['series'], instance['yxc']))
        print inraster
        for scale in instance['scale'].keys():
            layername = '{0}_{1}_{2}'.format(instance['series'], scale, key)
            print layername
            outraster='D:\\projects\\usxp\\deliverables\\{0}\\maps\\yxc\\yxc_b_hm\\yxc_b_hm.gdb\\{1}'.format(instance['series'], layername)
            print outraster

            processingCluster(instance, inraster, outraster, reclasslist)




instance = {'scale':{'6km':200}, 'series':'s26', 'yxc':'ytc', 'reclass':{'pre':[[2009,1],[2010,1],[2011,1],[2012,1]], 'post':[[2013,1],[2014,1],[2015,1],[2016,1]] }}
main(instance)









