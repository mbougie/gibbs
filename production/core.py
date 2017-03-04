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


rootdir='C:/Users/bougie/Desktop/gibbs/production/rasters/'



###################  set up directory and file structure  #####################################
class DirStructure:
    
    def __init__(self, dir_in, dir_out):
        rootdir='C:/Users/bougie/Desktop/gibbs/production/rasters/'
        
        self.dir_in = rootdir + dir_in
        self.dir_out = rootdir + dir_out
    

class FileStructure:
    
    def __init__(self, fname, ext):
        self.fname = fname
        self.ext = ext
        self.file_out=fname+ext


###################  declare functions  #######################################################

def addColorMap(inraster,template):
    ##Add Colormap
    ##Usage: AddColormap_management in_raster {in_template_raster} {input_CLR_file}

    try:
        import arcpy
        arcpy.env.workspace = r'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'
        
        ##Assign colormap using template image
        arcpy.AddColormap_management(inraster, "#", template)
        

    except:
        print "Add Colormap example failed."
        print arcpy.GetMessages()



def subsetRasterbyVector():

    for index, row in df.iterrows():
    	index= str(row['gid'])
        name=row['st_abbrev']

    	task = 'gdalwarp -cutline "PG:dbname=filters host=localhost port=5432 \
    	user=postgres password=postgres" -csql "select geom from shapefiles.shapefiles_states where gid = '+index+' " -crop_to_cutline\
    	-tr 30.0 30.0 -of HFA C:/Users/bougie/Desktop/gibbs/rasters/trajectories/trajectories_b3_8bit.img \
    	C:/Users/bougie/Desktop/gibbs/rasters/subset/traj/traj_'+name+'.img'
    	print task
    	os.system(task)


def createMTR(index,dir_in,dir_out):

    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print dir_in,dir_out
    ds = DirStructure(dir_in, dir_out)

    os.chdir(ds.dir_in)
    for file in glob.glob("*.img"):
        print(file)
        fnf=(os.path.splitext(file)[0]).split(".")
        
        #fnf=file name fragments
        print fnf
        
        fs = FileStructure(fnf[0]+'_mtr', '.img')
        print 'fs', fs 

        reclassArray = createReclassifyList() 
        print 'reclassArray----->', reclassArray
        outReclass = Reclassify(file, "Value", RemapRange(reclassArray), "NODATA")
        
        output = ds.dir_out+fs.file_out
        print 'output------->', output 
        outReclass.save(output)

        addColorMap(output,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/mtr.clr')


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
    return fulllist



def majorityFilter(index,dir_in,dir_out):
    
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print dir_in,dir_out
    ds = DirStructure(dir_in, dir_out)

    # filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    filter_combos = {'n8h':["EIGHT", "HALF"]}
    for k, v in filter_combos.iteritems():
        print k,v
        os.chdir(ds.dir_in)
        for file in glob.glob("*.img"):
            #fnf=file name fragments
            fnf=(os.path.splitext(file)[0]).split(".")
            
            #create file structure
            fs = FileStructure(fnf[0]+'_'+k, '.img')

            # Execute MajorityFilter
            outMajFilt = MajorityFilter(ds.dir_in+file, v[0], v[1])
            
            output = ds.dir_out+fs.file_out
            #save processed raster to new file
            outMajFilt.save(output)

            addColorMap(output,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/filter_and_mmu.clr')

            del outMajFilt


def focalStats(index,dir_in,dir_out):
    
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

def regionGroup(index,dir_in,dir_out):
    # Name: RegionGroup_Ex_02.py
    # Description: Records, for each cell in the output, the
    #              identity of the connected region to which 
    #              it belongs within the Analysis window. A 
    #              unique number is assigned to each region.
    # Requirements: Spatial Analyst Extension
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print dir_in,dir_out
    ds = DirStructure(dir_in, dir_out)

    # filter_combos = {'4w':["FOUR", "WITHIN"],'4c':["FOUR", "CROSS"],'8w':["EIGHT", "WITHIN"],'8c':["EIGHT", "CROSS"]}
    filter_combos = {'8w':["EIGHT", "WITHIN"]}
    for k, v in filter_combos.iteritems():
        print k,v
        os.chdir(ds.dir_in)
        for file in glob.glob("*.img"):
            #fnf=file name fragments
            fnf=(os.path.splitext(file)[0]).split(".")
            
            #create file structure
            fs = FileStructure(fnf[0]+'_'+k, '.img')
            # Check out the ArcGIS Spatial Analyst extension license
            arcpy.CheckOutExtension("Spatial")

            # Execute RegionGroup
            outRegionGrp = RegionGroup(ds.dir_in+file, v[0], v[1],"NO_LINK")

            # Save the output 
            print 'save the output'
            outRegionGrp.save(ds.dir_out+fs.file_out)


def mask(index,dir_in,dir_out):
    #NOTE: DON'T have a generic env.workspace becacuse it will return a value -128 versus NODATA!!
    arcpy.CheckOutExtension("Spatial")

    print dir_in,dir_out
    ds = DirStructure(dir_in, dir_out)

    os.chdir(ds.dir_in)
    for file in glob.glob("*.img"):
        #fnf=file name fragments
        fnf=(os.path.splitext(file)[0]).split(".")

           #################  CONDITION  #######################################
        # CONVERSION: 900square miles = 0.222395 acres


        #5 acres 
        # count = '23'
        # cond = "Count < " + count

        #10 acres
        count = '45'
        cond = "Count < " + count

        #15acres
        # count = '68'
        # cond = "Count < " + count
        
        #create file structure
        fs = FileStructure(fnf[0]+'_m'+ count, '.img')
 
        #Note Cells with NoData as their value will be nibbled in the in_raster so therefore 
        #select all regions < 68 pixels to be defined as NULL.
        

     
      
        #####################################################################

        inRas1 = Raster(ds.dir_in+file)
    
        # Execute SetNull
        outSetNull = SetNull(inRas1, 1, cond)

        # Save the output 
        outSetNull.save(ds.dir_out+fs.file_out)


def nibble(index,dir_in,dir_out):

    # The raster used as the mask.
    # It must be of integer type.
    # Cells with NoData as their value will be nibbled in the in_raster.
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print index,dir_in,dir_out
    ds = DirStructure(dir_in,dir_out)

  

    os.chdir(ds.dir_in)
    for file in glob.glob("*.img"):
        #fnf=file name fragments
        fnf=(os.path.splitext(file)[0]).split(".")
        print file

        #create file structure
        fs = FileStructure(fnf[0]+'_nbl', '.img')

    
        ####  create the paths to the mask files  ############# 
        inFile = None
        chunk=file[:-11]
        print chunk
        inFile=rootdir+'core/mtr/'+chunk+'.img'
      
        ###  Execute Nibble  #####################
        nibbleOut = Nibble(inFile, ds.dir_in+file, "DATA_ONLY")

        output = ds.dir_out+fs.file_out

        ###  Save the output  ################### 
        nibbleOut.save(output)

        addColorMap(output,'C:/Users/bougie/Desktop/gibbs/development/rasters/colormaps/filter_and_mmu.clr')



def setBackground(index,dir_in,dir_out):
    #Attach the cdl values to the binary ytc/yfc rasters 
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print dir_in,dir_out
    ds = DirStructure(dir_in, dir_out)

    os.chdir(ds.dir_in)
    for file in glob.glob("*.img"):
        print(file)

        fnf=(os.path.splitext(file)[0]).split(".")
        print 'fnf:', fnf
        # path_out=createFileOut(dir_root,file,'bg1')

        fs = FileStructure(fnf[0]+"_bg1",".img")
        print fs.file_out

        outRaster = ds.dir_out+fs.file_out
        print 'outRaster: ', outRaster
 
        inRas1 = Raster(ds.dir_in+file)
        cond = "Value = 0"

        # Execute values in condition to 1
        OutRas = Con(inRas1, 1, inRas1, cond)

        # Save the output 
        OutRas.save(outRaster)


        addColorMap(outRaster,inRas1)





######################################################################################



def fire_all(func_list,index,dir_in,dir_out):
    for f in func_list:
        f(index,dir_in,dir_out)



    
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
         
            dir_in=df[step][i][0] 
            print dir_in
            dir_out=df[step][i][1]
            print dir_out
            fire_all(fct_list[step],i,dir_in,dir_out)
    





##############  call functions  #####################################################
runit()








# {majorityFilter,createMTR,regionGroup}



 