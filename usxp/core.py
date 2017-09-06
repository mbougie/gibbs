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

try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"

#################### class to create yxc object  ####################################################
class ConversionObject:

    def __init__(self, res, years, wc, mmu):
        self.res = res
        self.datarange = str(years[0])+'to'+str(years[1])
        print self.datarange
        self.directory = 'core_' + self.datarange
        self.traj_name = "traj_cdl"+self.res+"_b_"+self.datarange+"_rfnd"
        self.traj_path = defineGDBpath(['pre','trajectories'])+self.traj_name
        self.wc = "*"+wc+"*"
        self.mmu = mmu



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



def createMTR():
    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    arcpy.env.workspace = defineGDBpath([yxc.directory,'filter'])
    
    for raster in arcpy.ListDatasets(yxc.wc, "Raster"): 
        print 'raster:', raster
        output = defineGDBpath([yxc.directory,'mtr'])+raster+'_mtr'
        print 'output:', output

        reclassArray = createReclassifyList() 

        outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
        
        outReclass.save(output)

        gen.buildPyramids(output)



def createReclassifyList():
    #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value

    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query('SELECT "Value", mtr from pre.traj_cdl' + yxc.res + '_b_' + yxc.datarange + ' as a JOIN pre.traj_cdl' + yxc.res + '_b_' + yxc.datarange + '_lookup as b ON a.traj_array = b.traj_array',con=engine)
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



def majorityFilter():
    #filter_combos = {'n4h':["FOUR", "HALF"],'n4m':["FOUR", "MAJORITY"],'n8h':["EIGHT", "HALF"],'n8m':["EIGHT", "MAJORITY"]}
    filter_combos = {'n8h':["EIGHT", "HALF"]}
    for k, v in filter_combos.iteritems():
        print k,v

        # Execute MajorityFilter
        outMajFilt = MajorityFilter(yxc.traj_path, v[0], v[1])
        
        output = defineGDBpath([yxc.directory,'filter'])+yxc.traj_name+'_'+k
        print 'output: ',output
        
        #save processed raster to new file
        outMajFilt.save(output)

        gen.buildPyramids(output)



def focalStats(gdb_args_in, dataset, gdb_args_out):
    # arcpy.CheckOutExtension("Spatial")
    # arcpy.env.workspace = defineGDBpath(gdb_args_in)

    # filter_combos = {'k3':[3, 3, "CELL"],'k5':[5, 5, "CELL"]}
    filter_combos = {'k3':[3, 3, "CELL"]}
    for k, v in filter_combos.iteritems():
        print k,v
        # for raster in arcpy.ListDatasets(dataset, "Raster"): 
        raster_in = defineGDBpath(gdb_args_in) + dataset
        print 'raster_in: ', raster_in

        output = defineGDBpath(gdb_args_out)+dataset+'_'+k
        print 'output: ',output

        neighborhood = NbrRectangle(v[0], v[1], v[2])

        # Execute FocalStatistics
        outFocalStatistics = FocalStatistics(Raster(raster_in), neighborhood, "MAJORITY")

        outFocalStatistics.save(output)
        
        gen.buildPyramids(output)




def focalStats_initial(index,dir_in,dir_out):

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

def regionGroup():
    #define workspace
    arcpy.env.workspace=defineGDBpath([yxc.directory, 'mtr'])

    filter_combos = {'8w':["EIGHT", "WITHIN"]}
    for k, v in filter_combos.iteritems():
        print k,v
        for raster in arcpy.ListDatasets(yxc.wc, "Raster"): 
            print 'raster: ', raster
            
            output=defineGDBpath([yxc.directory,'mmu'])+raster+'_'+k
            print 'output: ',output

            # Execute RegionGroup
            outRegionGrp = RegionGroup(Raster(raster), v[0], v[1],"NO_LINK")

            # Save the output 
            print 'save the output'
            outRegionGrp.save(output)

            gen.buildPyramids(output)






def clipByMMUmask(gdb_args_in, wc, masksize):
    #define workspace
    arcpy.env.workspace=defineGDBpath(gdb_args_in)


    for raster in arcpy.ListDatasets('*'+wc+'*8w', "Raster"): 

        print 'raster: ', raster

        # for count in masks_list:
        cond = "Count < " + str(gen.getPixelCount(yxc.res, masksize))
        print 'cond: ',cond

        output = raster+'_msk'+ str(masksize)

        print output

        outSetNull = SetNull(raster, 1, cond)

        # Save the output 
        outSetNull.save(output)



# def nibble(maskSize,arg_list1,arg_list2,filename):
#     #define workspace
#     arcpy.env.workspace=defineGDBpath(arg_list1)

#     #find mask raster in gdb
#     for mask in arcpy.ListDatasets('*_msk'+maskSize, "Raster"): 
#         print 'mask: ',  mask

#         #create file structure
#         output = mask+'_nbl'
#         print 'output: ', output
    
#         ####  create the paths to the mask files  ############# 
#         raster_in=defineGDBpath(arg_list2)+filename
#         print 'raster_in: ', raster_in
      
#         ###  Execute Nibble  #####################
#         nibbleOut = Nibble(Raster(raster_in), mask, "DATA_ONLY")

#         ###  Save the output  ################### 
#         nibbleOut.save(output)

#     #     addColorMap(output,'C:/Users/bougie/Desktop/gibbs/colormaps/mmu.clr')






def addGDBTable2postgres(gdb_args,wc,pg_shema):
    print 'running addGDBTable2postgres() function....'
    ####description: adds tables in geodatabse to postgres
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(gdb_args)

    for table in arcpy.ListTables(wc): 
        print 'table: ', table

        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(table)]
        print fields

        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(table,fields)
        print arr

        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df

        df.columns = map(str.lower, df.columns)

        # use pandas method to import table into psotgres
        # df.to_sql(table, engine, schema=pg_shema)

        #add trajectory field to table
        addAcresField(table, pg_shema)





def addAcresField(tablename, schema):
    #this is a sub function for addGDBTable2postgres()
    
    cur = conn.cursor()
    
    #DDL: add column to hold arrays
    cur.execute('ALTER TABLE ' + schema + '.' + tablename + ' ADD COLUMN acres bigint;');
    
    #DML: insert values into new array column
    cur.execute('UPDATE '+ schema + '.' + tablename + ' SET acres = count * ' + gen.getPixelConversion2Acres(yxc.res));
    
    conn.commit()
    print "Records created successfully";
    conn.close()


















################ Instantiate the class to create yxc object  ########################
yxc = ConversionObject(
      '56',
      ## data range---i.e. all the cdl years you are referencing 
      [2008,2012],
      'n8h',
       15
      )



#############################  Call Functions ######################################
##------filter gdb--------------
# majorityFilter()

##------mtr gdb-----------------
# createMTR()

# ##------mmu gdb-----------------
regionGroup()
# clipByMMUmask()



##########  NEW     ###################################
# addGDBTable2postgres(['core_2008to2012','mmu'],'*','counts')





