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
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general_deliverables as gen_dev
import json



# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")




def processingCluster(inraster, outraster, year, value):
    #####  reclass  ####################################################
    ## Reclassify (in_raster, reclass_field, remap, {missing_values})
    outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
    print 'finished outReclass'


    ######  block stats  ###############################################
    ##BlockStatistics (in_raster, {neighborhood}, {statistics_type}, {ignore_nodata})
    nbr = NbrRectangle(value, value, "CELL")
    outBlockStat = BlockStatistics(outReclass, nbr, "SUM", "DATA")
    print 'finished block stats'


    outBlockStat.save(outraster)


    ######  resample  ###############################################
    ##Resample_management (in_raster, out_raster, {cell_size}, {resampling_type})
    # resample_value = str(30 * value)
    # arcpy.Resample_management(outBlockStat, outraster, resample_value, "NEAREST")

    # arcpy.Resample_management(outBlockStat, outraster+'t1', "90000 90000", "NEAREST")
    # arcpy.Resample_management(inraster, outraster, "90000 90000", "NEAREST")








def addField(raster, normalizer):
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
            



def main():

    for key, value in scale_dict.iteritems():

       
        
        for year in years:
            print year

            changeTitle(mxd, year)

            # yxc_list = ['ytc','yfc']
            yxc_list = ['ytc']

            for yxc in yxc_list:
            # D:\projects\usxp\deliverables\groups\yxc\yxc_90km.gdb\s25_ytc_2009_b_bs_90km
                inraster=Raster('D:\\projects\\usxp\\deliverables\\s25.gdb\\s25_{}'.format(yxc))
                layername = 's{}_{1}_{2}_b_bs_{0}'.format(key, yxc, str(year))
                outraster='D:\\projects\\usxp\\deliverables\\groups\\yxc\\yxc_{0}.gdb\\{1}'.format(series, layername)
                # outraster='D:\\projects\\usxp\\deliverables\\groups\\yxc\\yxc_{0}.gdb\\s25_{1}_{2}_b_bs_{0}_hi'.format(key, yxc, str(year

                yxc_dict = {'forest':[[63,1],[141,1],[142,1],[143,1]], 'wetland':[[83,1],[87,1],[190,1],[195,1]], 'grassland':[[37,1],[62,1],[171,1],[176,1],[181,1]], 'shrubland':[[64,1],[65,1],[131,1],[152,1]]}                
                

                processingCluster(inraster, outraster, year, value)


            #     addField(outraster, (value*value))
            
         
            # styleLayer(outraster, 's25_{1}_{2}_b_bs_{0}'.format(key, yxc, str(year)), mxd)


        
# mxd.saveACopy('D:\\projects\\usxp\\deliverables\\groups\\yxc\\yxc3.mxd')
# del mxd
        


        

        
# scale_dict = {'3km':100, '90km':3000}
scale_dict = {'9km':300}
years = range(2009,2017)
series='s35'
mxd = arcpy.mapping.MapDocument('D:\\projects\\usxp\\deliverables\\groups\\yxc\\template.mxd')







