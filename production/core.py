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


production_type='production'
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
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/'+gdb_in+'.gdb'
    for raster in arcpy.ListDatasets('*', "Raster"): 
   
        raster_out = raster+'_mtr'
        output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/mtr.gdb/'+raster_out
        print output

        reclassArray = createReclassifyList() 
        outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
        
        outReclass.save(output)

        addColorMap(output,'C:/Users/bougie/Desktop/gibbs/colormaps/mmu.clr')



def createReclassifyList():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/pre')
    df = pd.read_sql_query('select "Value","mtr" from mtr.trajectories',con=engine)
    fulllist=[[0,0,"NODATA"]]
    # fulllist=[]
    for index, row in df.iterrows():
        templist=[]
        value=row['Value'] 
        mtr=row['mtr']  
        templist.append(int(value))
        templist.append(int(mtr))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist



# def majorityFilter(gdb_in):
#     arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/pre/'+gdb_in+'.gdb'

#     # filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
#     filter_combos = {'n8h':["EIGHT", "HALF"]}
#     for k, v in filter_combos.iteritems():
#         print k,v
#         for raster in arcpy.ListDatasets("traj", "Raster"): 
#             print 'raster: ', raster
    
#             raster_out=raster+'_'+k

#             # Execute MajorityFilter
#             outMajFilt = MajorityFilter(raster, v[0], v[1])
            
#             output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/filter.gdb/'+raster_out
            
#             #save processed raster to new file
#             outMajFilt.save(output)

#             # addColorMap(output,'C:/Users/bougie/Desktop/gibbs/colormaps/filter_and_mmu.clr')

def majorityFilter():
    dir = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/pre/'

    # filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    filter_combos = {'n8h':["EIGHT", "HALF"]}
    for k, v in filter_combos.iteritems():
        print k,v
        os.chdir(dir)
        for file in glob.glob("*.img"):
            print(file)
    
            raster_out=file[:-4]+'_'+k
            print 'raster_out: ',raster_out

            # Execute MajorityFilter
            outMajFilt = MajorityFilter(Raster(file), v[0], v[1])
            
            output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/filter.gdb/'+raster_out
            
            #save processed raster to new file
            outMajFilt.save(output)

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

            # addColorMap(output,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/filter_and_mmu.clr')

####################  mmu functions  ##########################################

def regionGroup(gdb_in):
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/'+gdb_in+'.gdb'


    # filter_combos = {'4w':["FOUR", "WITHIN"],'4c':["FOUR", "CROSS"],'8w':["EIGHT", "WITHIN"],'8c':["EIGHT", "CROSS"]}
    filter_combos = {'8w':["EIGHT", "WITHIN"]}
    for k, v in filter_combos.iteritems():
        print k,v
        for raster in arcpy.ListDatasets("*", "Raster"): 
            print 'raster: ', raster
    
            raster_out=raster+'_'+k
            print raster_out
            
            output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/mmu.gdb/'+raster_out

            # Execute RegionGroup
            outRegionGrp = RegionGroup(raster, v[0], v[1],"NO_LINK")

            # Save the output 
            print 'save the output'
            outRegionGrp.save(output)






def mask(masks_list):
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/mmu.gdb'
    for raster in arcpy.ListDatasets('*8w', "Raster"): 

         #################  CONDITION  #######################################
        # CONVERSION: 900square miles = 0.222395 acres
        
        #acres   count
         #5       23
         #10      45
         #15      68

        # masks=['23','45','68']
        
        for count in masks_list:
            cond = "Count < " + count
            print 'cond: ',cond

            output = raster+'_msk'+ count
            # output = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/mmu.gdb/'+raster_out
            print output

            outSetNull = SetNull(raster, 1, cond)

            # Save the output 
            outSetNull.save(output)



def nibble(maskSize):
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/mmu.gdb'
    for mask in arcpy.ListDatasets('*_msk'+maskSize, "Raster"): 
        print mask

        #create file structure
        output = mask+'_nbl'
        print output
    
        ####  create the paths to the mask files  ############# 
        raster_in='C:/Users/bougie/Desktop/gibbs/'+production_type+'/processes/core/mtr.gdb/traj_n8h_mtr'
      
        ###  Execute Nibble  #####################
        nibbleOut = Nibble(Raster(raster_in), mask, "DATA_ONLY")

        ###  Save the output  ################### 
        nibbleOut.save(output)

        addColorMap(output,'C:/Users/bougie/Desktop/gibbs/colormaps/mmu.clr')


#############################  Call Functions ######################################

# majorityFilter()
# createMTR('filter')
# regionGroup('mtr')
mask(['68'])
nibble('68')

