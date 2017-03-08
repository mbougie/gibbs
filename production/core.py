from sqlalchemy import create_engine
import numpy as np, sys, os
from osgeo import gdal
from osgeo.gdalconst import *
# from pandas import read_sql_query
import pandas as pd
# import tables
import collections
from collections import namedtuple
import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2


production_type='production_ND'
arcpy.CheckOutExtension("Spatial")
env.scratchWorkspace ="C:/Users/bougie/Documents/ArcGIS/scratch.gdb"


###################  declare functions  #######################################################

def addColorMap(inraster,template):
    ##Add Colormap
    ##Usage: AddColormap_management in_raster {in_template_raster} {input_CLR_file}

    try:
        import arcpy
        # arcpy.env.workspace = r'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'
        
        ##Assign colormap using template image
        arcpy.AddColormap_management(inraster, "#", template)
        

    except:
        print "Add Colormap example failed."
        print arcpy.GetMessages()



def createMTR(gdb_in):
    gdb_in=gdb_in[0]
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/'+gdb_in+'.gdb'
    for raster in arcpy.ListDatasets('*', "Raster"): 
        inRaster=Raster(raster)
        print inRaster

        raster_out = raster+'_mtr'
        output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/mtr.gdb/'+raster_out
        print output

        reclassArray = createReclassifyList() 
        outReclass = Reclassify(inRaster, "Value", RemapRange(reclassArray), "NODATA")
        
        outReclass.save(output)

        addColorMap(output,'C:/Users/bougie/Desktop/gibbs/colormaps/mtr.clr')


def createReclassifyList():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/pre')
    df = pd.read_sql_query('select "Value","MTR_2012-2015" from mtr.trajectories WHERE "Value" != 0',con=engine)
    fulllist=[[0,0,"NODATA"]]
    # fulllist=[]
    for index, row in df.iterrows():
        templist=[]
        value=row['Value'] 
        mtr=row['MTR_2012-2015']  
        templist.append(int(value))
        templist.append(int(mtr))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist



def majorityFilter(gdb_in):
    gdb_in=gdb_in[0]
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/pre/'+gdb_in+'.gdb'

    # filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    filter_combos = {'n8h':["EIGHT", "HALF"]}
    for k, v in filter_combos.iteritems():
        print k,v
        for raster in arcpy.ListDatasets("traj", "Raster"): 
            print 'raster: ', raster
    
            raster_out=raster+'_'+k

            # Execute MajorityFilter
            outMajFilt = MajorityFilter(raster, v[0], v[1])
            
            output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/filter.gdb/'+raster_out
            
            #save processed raster to new file
            outMajFilt.save(output)

            addColorMap(output,'C:/Users/bougie/Desktop/gibbs/colormaps/filter_and_mmu.clr')



def focalStats(index,dir_in,dir_out):

    #NOT CONVERTED TO GDB YET!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print dir_in,dir_out
    ds = DirStructure(dir_in, dir_out)

    filter_combos = {'k3':[3, 3, "CELL"],'k5':[5, 5, "CELL"]}
    for k, v in filter_combos.iteritems():
        print k,v
        os.chdir(ds.dir_in)
        for file in glob.glob("*.img"):
            #fnf=file name fragments
            fnf=(os.path.splitext(file)[0]).split(".")
            
            #create file structure
            fs = FileStructure(fnf[0]+'_'+k, '.img')

            neighborhood = NbrRectangle(v[0], v[1], v[2])

            # Check out the ArcGIS Spatial Analyst extension license
            arcpy.CheckOutExtension("Spatial")

            # Execute FocalStatistics
            outFocalStatistics = FocalStatistics(ds.dir_in+file, neighborhood, "MAJORITY")

            output = ds.dir_out+fs.file_out

            # Save the output 
            outFocalStatistics.save(output)

            addColorMap(output,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/filter_and_mmu.clr')

####################  mmu functions  ##########################################

def regionGroup(gdb_in):
    gdb_in=gdb_in[0]
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/'+gdb_in+'.gdb'


    # filter_combos = {'4w':["FOUR", "WITHIN"],'4c':["FOUR", "CROSS"],'8w':["EIGHT", "WITHIN"],'8c':["EIGHT", "CROSS"]}
    filter_combos = {'8w':["EIGHT", "WITHIN"]}
    for k, v in filter_combos.iteritems():
        print k,v
        for raster in arcpy.ListDatasets("*", "Raster"): 
            print 'raster: ', raster
    
            raster_out=raster+'_'+k
            print raster_out
            
            output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/mmu.gdb/'+raster_out

            # Execute RegionGroup
            outRegionGrp = RegionGroup(raster, v[0], v[1],"NO_LINK")

            # Save the output 
            print 'save the output'
            outRegionGrp.save(output)






def mask(gdb_in):
    gdb_in=gdb_in[0]
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/'+gdb_in+'.gdb'
    for raster in arcpy.ListDatasets('*8w', "Raster"): 
        inRaster=Raster(raster)
        print inRaster

         #################  CONDITION  #######################################
        # CONVERSION: 900square miles = 0.222395 acres
        
        #acres   count
         #5       23
         #10      45
         #15      68


        # masks=['23','45','68']
        masks=['45']
        for count in masks:
            cond = "Count < " + count
            print 'cond: ',cond

            raster_out = raster+'_msk'+ count
            output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/mmu.gdb/'+raster_out
            print output

            outSetNull = SetNull(inRaster, 1, cond)

            # Save the output 
            outSetNull.save(output)



def nibble(gdb_in):

    gdb_in=gdb_in[0]
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/'+gdb_in+'.gdb'
    for mask in arcpy.ListDatasets('*_msk*', "Raster"): 
    
        #create file structure
        output = mask+'_nbl'
    
        ####  create the paths to the mask files  ############# 
        raster_in='C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/mtr.gdb'+'/traj_n8h_mtr'
      
        ###  Execute Nibble  #####################
        nibbleOut = Nibble(raster_in, mask, "DATA_ONLY")

        ###  Save the output  ################### 
        nibbleOut.save(output)

        addColorMap(output,'C:/Users/bougie/Desktop/gibbs/colormaps/mmu.clr')






#############################  Call Functions ######################################



def fire_all(func_list,params):
    for f in func_list:
        f(params)



    
def runit():

    fct_list = {'createMTR': [createMTR],
                'majorityFilter': [majorityFilter],
                'focalStats': [focalStats],
                'regionGroup': [regionGroup],
                'mask': [mask],
                'nibble': [nibble]
               }


    engine = create_engine('postgresql://postgres:postgres@localhost:5432/metadata')
    df = pd.read_sql_query('SELECT * FROM routes_prod.routes_core ORDER BY serial',con=engine)
    
    routes=df['routes']
    for i, route in enumerate(routes):
        print i
        print route
        for step in route:
            print 'step-----------------', step
         
            params=df[step][i]
            fire_all(fct_list[step],params)
    





##############  call functions  #####################################################
runit()
