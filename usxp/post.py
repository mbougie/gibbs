from sqlalchemy import create_engine
import numpy as np, sys, os
# from osgeo import gdal
# from osgeo.gdalconst import *
import pandas as pd
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


###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path


#################### class to create yxc object  ####################################################

class ProcessingObject(object):

    def __init__(self, res, mmu, years, name, subname):
        self.name = name
        self.subname = subname
        self.res = str(res)
        self.mmu = str(mmu)
        self.years = years
        
        self.datarange = str(self.years[0])+'to'+str(self.years[1])
        print 'self.datarange:', self.datarange
        
        self.conversionyears = range(self.years[0]+2, self.years[1])
        print 'self.conversionyears:', self.conversionyears
        
        self.traj_dataset = "traj_cdl"+self.res+"_b_"+self.datarange
        
        self.mmu_gdb=defineGDBpath(['core','mmu'])


        mtr = defineGDBpath(['core','mtr'])+'traj_cdl30_b_2008to2016_rfnd_n8h_mtr'
        ytc = defineGDBpath(['post','ytc'])+'ytc30_2008to2016_mmu5'
        outCon = Con((mtr == 3) & (ytc >= 2008), ytc, Con((mtr == 3) & (IsNull(ytc)), 3))

        output = defineGDBpath(['post','ytc'])+'ytc30_2008to2016_mmu5_msk'

        # if self.years[1] == 2016:
        #     self.datarange = str(self.years[0])+'to'+str(self.years[1]-1)
        #     print 'self.datarange:', self.datarange
        #     self.conversionyears = range(self.years[0]+1, self.years[1])
        #     print 'self.conversionyears:', self.conversionyears
        # else:
        #     self.datarange = str(self.years[0])+'to'+str(self.years[1])
        #     print 'self.datarange:', self.datarange
        #     self.conversionyears = range(self.years[0]+1, self.years[1] + 1)
        #     print 'self.conversionyears:', self.conversionyears
        

        if self.name == 'ytc':
            self.mtr = '3'
        elif self.name == 'yfc':
            self.mtr = '4'
    
    #function for to get correct cdl for the attachCDL() function
    def getAssociatedCDL(self, year):
        if self.subname == 'bfc' or  self.subname == 'bfnc':
            # subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl_'+ str(year - 1)
            return cdl_file

        elif self.subname == 'fc' or  self.subname == 'fnc':
            # subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl_'+ str(year)
            return cdl_file

        

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


def createYearbinaries():
    print "-----------------createYearbinaries() function-------------------------------"
    #DESCRIPTION:subset the trajectoires by year to create binary ytc or ytc raster by year that represent the conversion to/from crop between succesive years
    arcpy.env.workspace=defineGDBpath(['post',post.name])
    
    output = post.name+post.res+'_'+post.datarange
    print 'output: ', output
    
    # traj_cdl56_b_2008to2012_rfnd
    ###copy trajectory raster so it can be modified iteritively
    arcpy.CopyRaster_management(defineGDBpath(['pre', 'trajectories']) + 'traj_cdl'+post.res+'_b_'+post.datarange+'_rfnd', output)
    
    arcpy.CheckOutExtension("Spatial")

    #Connect to postgres database to get values from traj dataset 
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    df = pd.read_sql_query('select * from pre.traj_cdl'+post.res+'_b_'+post.datarange+' as a JOIN pre.traj_' + post.datarange + '_lookup as b ON a.traj_array = b.traj_array WHERE b.'+post.name+' IS NOT NULL',con=engine)
    print 'df--',df
    
    # loop through rows in the dataframe
    for index, row in df.iterrows():
        #get the arbitrary value assigned to the specific trajectory
        value=str(row['Value'])
        print 'value: ', value

        #cy is acronym for conversion year
        cy = str(row[post.name])
        print 'cy:', cy
        
        # allow raster to be overwritten
        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput
    
        #establish the condition
        cond = "Value = " + value
        print 'cond: ', cond
        
        # set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
        OutRas = Con(output, cy, output, cond)
   
        OutRas.save(output)

    #build pyramids t the end
    gen.buildPyramids(output)

    # removeArbitraryValuesFromYearbinaries()




def createYearbinaries_better():
        # DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    print "-----------------  createYearbinaries_better()  -------------------------------"

    def createReclassifyList():
        #this is a sub function for createYearbinaries_better().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value

        engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
        query = 'SELECT * from pre.' + post.traj_dataset + ' as a JOIN pre.traj_' + post.datarange + '_lookup as b ON a.traj_array = b.traj_array WHERE '+post.name+' IS NOT NULL'
        print 'query:', query
        df = pd.read_sql_query(query, con=engine)
        print df
        fulllist=[[0,0,"NODATA"]]
        for index, row in df.iterrows():
            templist=[]
            value=row['Value'] 
            yxc=row[post.name]  
            templist.append(int(value))
            templist.append(int(yxc))
            fulllist.append(templist)
        print 'fulllist: ', fulllist
        return fulllist


    ## replace the arbitrary values in the trajectories dataset with the yxc values.
    raster = Raster(defineGDBpath(['pre','trajectories'])+post.traj_dataset)
    print 'raster:', raster

    output = defineGDBpath(['post',post.name])+post.name+post.res+'_'+post.datarange
    print 'output: ', output

    reclassArray = createReclassifyList() 

    outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
    
    outReclass.save(output)

    gen.buildPyramids(output)


def removeArbitraryValuesFromYearbinaries():
    print "-----------------removeArbitraryValuesFromYearbinaries() function-------------------------------"
    #DESCRIPTION: remove the arbitrary values from the 'yfc_years_'+mmu dataset

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(['post',post.name])

    arcpy.CheckOutExtension("Spatial")
    
    #get raster from geodatabse
    raster_input = post.name+post.res+'_'+post.datarange + '_mmu' + post.mmu
    output = raster_input+'_clean'

    print 'output:', output

    ##only keep year values in map
        
    cond = "Value < " + str(post.years[0])
    print 'cond: ', cond
        
    # set mmu raster to null where value is less 2013 (i.e. get rid of attribute values)
    outSetNull = SetNull(raster_input, raster_input,  cond)
    
    #Save the output 
    outSetNull.save(output)

    gen.buildPyramids(output)



def clipByMMUmask():
    #DESCRIPTION: subset the mosiac years raster by mtr3 or mtr4

    #define gdb workspace
    arcpy.env.workspace=defineGDBpath(['post',post.name])
    
    #get the years raster from geodatabase
    traj_years=post.name+post.res+'_'+post.datarange
    print 'traj_years:', traj_years
    mmu_Raster=defineGDBpath(['core','mmu'])+'traj_cdl'+post.res+'_b_'+post.datarange+'_rfnd_n8h_mtr_8w_msk'+post.mmu+'_nbl'
    print 'mmu_Raster:', mmu_Raster
    #create output file 
    output = traj_years + '_mmu' + post.mmu
    print 'output: ', output
    
    #condition value is dependent of yxc (i.e. ytc = 3 and yfc = 4)
    cond = "Value <> " + post.mtr
    print 'cond: ', cond
    
    # set mmu raster to null where not equal to value and then attached the values fron traj_years tp these [value] patches
    outSetNull = SetNull(Raster(mmu_Raster), Raster(traj_years),  cond)
    
    #Save the output 
    outSetNull.save(output)

    gen.buildPyramids(output)


def createMask():
    mtr = Raster(defineGDBpath(['core','mmu'])+'traj_cdl30_b_2008to2016_rfnd_n8h_mtr_8w_msk5_nbl')
    ytc = Raster(defineGDBpath(['post','ytc'])+'ytc30_2008to2016_mmu5')

    outCon = Con((mtr == 3) & (IsNull(ytc)), 3, Con((mtr == 3) & (ytc >= 2008), ytc))
    output = defineGDBpath(['post','ytc'])+'ytc30_2008to2016_mmu5_msk'
    outCon.save(output)
    gen.buildPyramids(output)





def attachCDL(gdb_args_in):
    # DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    arcpy.env.workspace=defineGDBpath(gdb_args_in)
    
    # arcpy.env.rasterStatistics = 'STATISTICS'
    # # Set the cell size environment using a raster dataset.
    # arcpy.env.cellSize = "ytc_years_traj_cdl_b_n8h_mtr_8w_msk23_nbl"

    # # Set Snap Raster environment
    # arcpy.env.snapRaster = "ytc_years_traj_cdl_b_n8h_mtr_8w_msk23_nbl"

    # # #copy raster
    # arcpy.CopyRaster_management("ytc_years_traj_cdl_b_n8h_mtr_8w_msk23_nbl", "ytc_years_traj_cdl_b_n8h_mtr_8w_msk23_nbl_"+post.subname)
    
    wc = '*'+post.subname
    print wc

    for raster in arcpy.ListDatasets(wc, "Raster"): 
      
        for year in  post.conversionyears:
            print 'raster: ', raster
            print 'year: ', year

            # allow raster to be overwritten
            arcpy.env.overwriteOutput = True
            print "overwrite on? ", arcpy.env.overwriteOutput
        
            #establish the condition
            cond = "Value = " + str(year)
            print 'cond: ', cond

            cdl_file= post.getAssociatedCDL(year)
            print 'associated cdl file: ', cdl_file
            
            # # set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
            OutRas = Con(raster, cdl_file, raster, cond)
       
            OutRas.save(raster)



################ Instantiate the class to create yxc object  ########################
# post = ConversionObject(
#       'ytc',
#       'fc',
#       '30',
#       '5',
#       ## these are the conversion years 
#       [2008,2016]
#       )

post = ProcessingObject(
      #resolution
      30,
      #mmu
      5,
      #data range---i.e. all the cdl years you are referencing 
      [2008,2016],
      #name
      'ytc',
      #subname
      'fc'
      )

################ call functions  #####################################################
createYearbinaries_better()
# createMask()





######### old #########################
# createYearbinaries()
# removeArbitraryValuesFromYearbinaries()
# attachCDL(['post',post.name])







