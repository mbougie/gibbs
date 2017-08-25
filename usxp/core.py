from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
# from pandas import read_sql_query
import pandas as pd
# import tables
import collections
from collections import namedtuple
# import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
import general as gen



'''######## DEFINE THESE EACH TIME ##########'''
#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.parallelProcessingFactor = "95%"

###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path



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



def createMTR(gdb_args_in, traj_dataset, gdb_args_out):
    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    arcpy.env.workspace = defineGDBpath(gdb_args_in)
    for raster in arcpy.ListDatasets(traj_dataset+'*', "Raster"): 
        print 'raster:', raster
        raster_out = raster+'_mtr'
        output = defineGDBpath(gdb_args_out)+raster_out
        print 'output:', output

        reclassArray = createReclassifyList(traj_dataset) 

        outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
        
        outReclass.save(output)

        gen.buildPyramids(output)



def createReclassifyList(traj_dataset):
    #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value

    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query('SELECT "Value", mtr from pre.' + traj_dataset + ' as a JOIN pre.' + traj_dataset + '_lookup as b ON a.traj_array = b.traj_array',con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        value=row['Value'] 
        mtr=row['mtr']  
        templist.append(int(value))
        templist.append(int(mtr))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist



def majorityFilter(gdb_args_in, dataset, gdb_args_out):
    arcpy.env.workspace = defineGDBpath(gdb_args_in)

    #filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    filter_combos = {'n8h':["EIGHT", "HALF"]}
    for k, v in filter_combos.iteritems():
        print k,v
        for raster in arcpy.ListDatasets(dataset, "Raster"): 
            print 'raster: ', raster
    
            raster_out=raster+'_'+k
            
            # Execute MajorityFilter
            outMajFilt = MajorityFilter(raster, v[0], v[1])
            
            output = defineGDBpath(gdb_args_out)+raster_out
            print 'output: ',output
            
            #save processed raster to new file
            outMajFilt.save(output)

            gen.buildPyramids(output)




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

def regionGroup(gdb_args_in, wc):
    #define workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)

    filter_combos = {'8w':["EIGHT", "WITHIN"]}
    for k, v in filter_combos.iteritems():
        print k,v
        for raster in arcpy.ListDatasets("*"+wc+"*", "Raster"): 
            print 'raster: ', raster
    
            raster_out=raster+'_'+k
            print 'raster_out', raster_out
            output=defineGDBpath([gdb_args_in[0],'mmu'])+raster_out
            
            print 'output: ',output
            # Execute RegionGroup
            outRegionGrp = RegionGroup(raster, v[0], v[1],"NO_LINK")

            # Save the output 
            print 'save the output'
            outRegionGrp.save(output)

            gen.buildPyramids(output)






def clipByMMUmask(gdb_args_in, wc, masks_list):
    #define workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)


    for raster in arcpy.ListDatasets('*'+wc+'*8w', "Raster"): 

        print 'raster: ', raster
        '''
        -------------------- CONDITION  ------------------------------------------
        CONVERSION: 900square miles = 0.222395 acres
        
        acres   count
        5       23
        10      45
        15      68

        example: masks=['23','45','68']

        --------------------------------------------------------------------------
        '''

        for count in masks_list:
            cond = "Count < " + count
            print 'cond: ',cond

            output = raster+'_msk'+ count
    
            print output

            outSetNull = SetNull(raster, 1, cond)

            # Save the output 
            outSetNull.save(output)



def nibble(maskSize,arg_list1,arg_list2,filename):
    #define workspace
    arcpy.env.workspace=defineGDBpath(arg_list1)

    #find mask raster in gdb
    for mask in arcpy.ListDatasets('*_msk'+maskSize, "Raster"): 
        print 'mask: ',  mask

        #create file structure
        output = mask+'_nbl'
        print 'output: ', output
    
        ####  create the paths to the mask files  ############# 
        raster_in=defineGDBpath(arg_list2)+filename
        print 'raster_in: ', raster_in
      
        ###  Execute Nibble  #####################
        nibbleOut = Nibble(Raster(raster_in), mask, "DATA_ONLY")

        ###  Save the output  ################### 
        nibbleOut.save(output)

    #     addColorMap(output,'C:/Users/bougie/Desktop/gibbs/colormaps/mmu.clr')






#############################  Call Functions ######################################
##------filter gdb--------------
# majorityFilter(['pre','trajectories'],"traj_cdl30_b_8to12", ['core_8to12','filter'])

##------mtr gdb-----------------
# createMTR(['core_8to12','filter'], "traj_cdl30_b_8to12", ['core_8to12','mtr'])

##------mmu gdb-----------------
# regionGroup(['core_8to12','mtr'], "cdl30")
# clipByMMUmask(['core_8to12','mmu'], 'cdl30', ['23'])
# nibble('23',['core','mmu'],['core','mtr'],'traj_rfnd_n8h_mtr')






