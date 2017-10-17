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



try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


###################  Define the environment  #######################################################
#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path


#################### class to create yxc object  ####################################################
class RefineObject:

    def __init__(self, name, res, years, nlcd_years):
        self.name = name
        self.res = res
        self.years = years
        self.yearcount=len(range(self.years[0], self.years[1]+1))
        self.datarange = str(self.years[0])+'to'+str(self.years[1])
        print 'self.datarange:', self.datarange
        self.conversionyears = range(self.years[0]+2, self.years[1])
        print 'self.conversionyears:', self.conversionyears
        self.traj_dataset = "traj_cdl"+self.res+"_b_"+self.datarange
        self.nlcd_years = nlcd_years

        # if self.years[1] == 2016:
        #     self.conversionyears = range(self.years[0]+2, self.years[1])
        #     print 'self.conversionyears:', self.conversionyears
        # else:
        #     self.conversionyears = range(self.years[0]+1, self.years[1] + 1)
        #     print 'self.conversionyears:', self.conversionyears


        
     
        # self.mmu_Raster=Raster(defineGDBpath([gdb,'mtr']) + 'traj_cdl'+res+'_b_8to12_mtr')
        # def getConversionyears():
        #     if self.datarange == '2008to2012':
        #         self.conversionyears = range(self.years[0]+1, self.years[1] + 1)
        #         print self.conversionyears
        #     elif self.datarange == '2008to2016':
        #         self.conversionyears = range(self.years[0]+1, self.years[1])
        #         print self.conversionyears

        def getYXCAttributes():
            if self.name == 'ytc':
                self.mtr = '3'
                # self.subtypelist = ['sc']
                self.subtypelist = ['bfc','fc']
                print 'yo', self.subtypelist
                self.traj_change = 1

        
            elif self.name == 'yfc':
                self.mtr = '4'
                self.subtypelist = ['bfnc','fnc']
                print 'yo', self.subtypelist
                # self.traj_change = 2



        
        #call functions 
        # getConversionyears()
        getYXCAttributes()


        def getCDLlist(self):
            cdl_list = []
            for year in self.data_years:
                cdl_dataset = 'cdl'+self.res+'_b_'+str(year)
                cdl_list.append(cdl_dataset)
            print'cdl_list: ', cdl_list
            return cdl_list







##############  Declare functions  ######################################################
# createMTR("traj_cdl30_b_2008to2012", ['refinement_2008to2012','mtr'])   
def createMTR():
        # DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    print "-----------------createMTR() function-------------------------------"

    def createReclassifyList():
        #this is a sub function for createMTR().  references the mtr value in psotgres to create a list containing arbitray trajectory value and associated new mtr value

        engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
        query = 'SELECT * from pre.' + refine.traj_dataset + ' as a JOIN pre.traj_' + refine.datarange + '_lookup as b ON a.traj_array = b.traj_array WHERE '+refine.name+' IS NOT NULL'
        print 'query:', query
        df = pd.read_sql_query(query, con=engine)
        print df
        fulllist=[[0,0,"NODATA"]]
        for index, row in df.iterrows():
            templist=[]
            value=row['Value'] 
            mtr=row[refine.name]  
            templist.append(int(value))
            templist.append(int(mtr))
            fulllist.append(templist)
        print 'fulllist: ', fulllist
        return fulllist


    ## replace the arbitrary values in the trajectories dataset with the mtr values 1-5.
    # defineGDBpath(['pre','trajectories'])

    raster = Raster(defineGDBpath(['pre','trajectories'])+refine.traj_dataset)
    print 'raster:', raster

    # output = defineGDBpath(['refine','mtr'])+refine.traj_dataset+'_mtr'
    # print 'output:', output

    output = defineGDBpath(['refine','ytc'])+refine.name+refine.res+'_'+refine.datarange
    print 'output: ', output

    reclassArray = createReclassifyList() 

    # outReclass = Reclassify(raster, "Value", RemapRange(reclassArray), "NODATA")
    
    # outReclass.save(output)

    # gen.buildPyramids(output)






def attachCDL(subtype):
    # DESCRIPTION:attach the appropriate cdl value to each year binary dataset
    print "-----------------attachCDL() function-------------------------------"


    def getAssociatedCDL(subtype, year):
        #this is an aux function for attachCDL() function to get correct cdl for the attachCDL() function

        if subtype == 'bfc' or  subtype == 'bfnc':
            # NOTE: subtract 1 from every year in list
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ refine.res + '_' + str(year - 1)
            return cdl_file

        elif subtype == 'fc' or  subtype == 'fnc':
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ refine.res + '_' + str(year)
            return cdl_file

        elif subtype == 'sc':
            cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ refine.res + '_' + str(year + 1)
            return cdl_file



    # NOTE: Need to copy the yxc_clean dataset and rename it with subtype after it
    arcpy.env.workspace=defineGDBpath(['refine',refine.name])

    inputraster = refine.name+refine.res+'_'+refine.datarange
    print "inputraster: ", inputraster
    
    output = inputraster+'_'+subtype
    print "output: ", output
    
    ##copy binary years raster so it can be modified iteritively
    arcpy.CopyRaster_management(inputraster, output)

    #note:checkout out spatial extension after creating a copy or issues
    arcpy.CheckOutExtension("Spatial")
 
    wc = '*'+refine.res+'*'+subtype
    print wc
  
    for year in  refine.conversionyears:
        print 'year: ', year

        # allow output to be overwritten
        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput
    
        #establish the condition
        cond = "Value = " + str(year)
        print 'cond: ', cond

        cdl_file= getAssociatedCDL(subtype, year)
        print 'associated cdl file: ', cdl_file
        
        #set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year
        OutRas = Con(output, cdl_file, output, cond)
   
        OutRas.save(output)

    #build pyramids t the end
    gen.buildPyramids(output)



def createChangeTrajectories():
    print "-----------------createChangeTrajectories() function-------------------------------"
    # Description: "Combines multiple rasters so that a unique output value is assigned to each unique combination of input values" -arcGIS def
    #the rasters where combined in chronoloigal order with the recalssifed nlcd raster being in the inital spot.

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(['refine',refine.name])
    
    def getRasterList():
        orderedlist = []
        for subtype in refine.subtypelist:
            orderedlist.append(refine.name+refine.res+'_'+refine.datarange+'_'+subtype)
        print orderedlist
        return orderedlist



    
    output = defineGDBpath(['refine','trajectories'])+'traj_'+refine.name+refine.res+'_'+refine.datarange
    print 'output', output

    # ##Execute Combine
    outCombine = Combine(getRasterList())
  
    ###Save the output 
    outCombine.save(output)

    ###create pyraminds
    gen.buildPyramids(output)



def createMask_nlcdtraj(join_operator):

    nlcd2001 = Raster(defineGDBpath(['ancillary','nlcd'])+'nlcd'+refine.res+'_'+refine.nlcd_years[0])
    nlcd2006 = Raster(defineGDBpath(['ancillary','nlcd'])+'nlcd'+refine.res+'_'+refine.nlcd_years[1])
    nlcd2011 = Raster(defineGDBpath(['ancillary','nlcd'])+'nlcd'+refine.res+'_'+refine.nlcd_years[2])
    refinement_mtr = Raster(defineGDBpath(['refine','mtr'])+'traj_cdl'+refine.res+'_b_'+refine.datarange+'_mtr')
    ytc_clean = Raster(defineGDBpath(['refine','ytc'])+'ytc'+refine.res+'_'+refine.datarange)
   

    # If condition is true set pixel to 23 else set pixel value to NULL
    #NOTE: both of the hardcoded values are ok because uniform accross resolutions and they are constant
    # outCon = Con((nlcd2001 == 82) & (refinement_mtr == 3), getArbitraryCropValue(), Con((nlcd2 == 82) & (refinement_mtr == 3), getArbitraryCropValue()))
    if join_operator == 'or':
        output = defineGDBpath(['refine','masks'])+'traj_nlcd'+refine.res+'_b_'+refine.nlcd_years[0]+'or'+refine.nlcd_years[1]+'_'+refine.name+refine.datarange+'_mask'
        print 'output:', output
        outCon = Con((nlcd2001 == 82) & (refinement_mtr == 3) & (ytc_clean < 2012), getArbitraryCropValue(), Con((nlcd2006 == 82) & (refinement_mtr == 3), getArbitraryCropValue() , Con((nlcd2006 == 82) & (refinement_mtr == 3) & (ytc_clean > 2011), getArbitraryCropValue()))) 
        print outCon
    elif join_operator == 'and':
        output = defineGDBpath(['refine','masks'])+'traj_nlcd'+refine.res+'_b_'+refine.nlcd_years[0]+'and'+refine.nlcd_years[1]+'_'+refine.name+refine.datarange+'_mask'
        print 'output:', output
        outCon = Con(((nlcd2001 == 82) & (nlcd2 == 82) & (refinement_mtr == 3)), getArbitraryCropValue())
    
    outCon.save(output)

    gen.buildPyramids(output)


def getArbitraryCropValue():
    #def this is a sub function for createMask_nlcdtraj()
    #this is to return the arbitrary value assocaited with full crop over entire years span

    yotest = [1] * refine.yearcount
    print yotest
    columnList = ','.join(str(e) for e in yotest)
    print 'columnlist', columnList

    cur = conn.cursor()
   
    cur.execute("SELECT \"Value\" FROM pre.traj_cdl"+refine.res+"_b_"+refine.datarange+" Where traj_array = '{"+columnList+"}' ")

    # fetch all rows from table
    rows = cur.fetchall()
    print 'arbitrary value in ' + 'pre.traj_cdl'+refine.res+'_b_'+refine.datarange+':', rows[0][0]
    return rows[0][0]




def createMask_trajytc():
    traj_ytc = defineGDBpath(['refine','trajectories'])+'traj_'+refine.name+refine.res+'_'+refine.datarange


    output = defineGDBpath(['refine','masks'])+'traj_'+refine.name+refine.res+'_'+refine.datarange+'_mask'
    print 'output:', output

    reclassArray = createReclassifyList() 

    outReclass = Reclassify(traj_ytc, "Value", RemapRange(reclassArray), "NODATA")
    
    outReclass.save(output)

    gen.buildPyramids(output)



def createReclassifyList():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
   
    query = (
    "SELECT DISTINCT \"Value\" "
    "FROM refinement.traj_"+refine.name+refine.res+"_"+refine.datarange+" "
    "WHERE 122 = traj_array[1] "
    "OR 123 = traj_array[1] "
    "OR 124 = traj_array[1] "
    "OR 61 = traj_array[2] "
    "OR '{37,36,36}' = traj_array "
    "OR '{152,36,36}' = traj_array "
    "OR '{176,36,36}' = traj_array"
    )

    # query = (
    # "SELECT DISTINCT \"Value\" "
    # "FROM refinement.traj_"+refine.name+refine.res+"_"+refine.datarange+" "
    # "WHERE 61 = traj_array[2] "
    # )

    print query

    cur.execute(query)
    #create empty list
    fulllist=[[0,0,"NODATA"]]

    # fetch all rows from table
    rows = cur.fetchall()
    print 'number of records in lookup table', len(rows)
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    for row in rows:
        templist=[]
        templist.append(row[0])
        templist.append(refine.traj_change)
        fulllist.append(templist)
    print fulllist
    return fulllist

def createReclassifyList_mod():
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
   
    # query = (
    # "SELECT DISTINCT \"Value\" "
    # "FROM refinement.traj_"+refine.name+refine.res+"_"+refine.datarange+" "
    # "WHERE 122 = traj_array[1] "
    # "OR 123 = traj_array[1] "
    # "OR 124 = traj_array[1] "
    # "OR 61 = traj_array[2] "
    # "OR '{37,36,36}' = traj_array "
    # "OR '{152,36,36}' = traj_array "
    # "OR '{176,36,36}' = traj_array"
    # )

    query = (
    "SELECT DISTINCT \"Value\" "
    "FROM refinement.traj_"+refine.name+refine.res+"_"+refine.datarange+" "
    "WHERE 61 = traj_array[2] "
    )

    print query

    cur.execute(query)
    #create empty list
    fulllist=[[0,0,"NODATA"]]

    # fetch all rows from table
    rows = cur.fetchall()
    return rows
    print 'number of records in lookup table', len(rows)
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    # for row in rows:
    #     templist=[]
    #     templist.append(row[0])
    #     templist.append(refine.traj_change)
    #     fulllist.append(templist)
    # print fulllist
    # return fulllist


 

def addGDBTable2postgres():
    def addTrajArrayField(tablename, fields, schema):
        #this is a sub function for addGDBTable2postgres()
        
        cur = conn.cursor()
        
        #convert the rasterList into a string
        columnList = ','.join(fields[3:])
        print columnList

        #DDL: add column to hold arrays
        cur.execute('ALTER TABLE ' + schema + '.' + tablename + ' ADD COLUMN traj_array integer[];');
        
        #DML: insert values into new array column
        cur.execute('UPDATE ' + schema + '.' + tablename + ' SET traj_array = ARRAY['+columnList+'];');
        
        conn.commit()
        print "Records created successfully";
        # conn.close()


    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    
    tablename = "traj_"+refine.name+refine.res+"_"+refine.datarange
    # path to the table you want to import into postgres
    input = defineGDBpath(['refine', 'trajectories'])+tablename

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(input)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(input,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    print df
    
    # use pandas method to import table into psotgres
    df.to_sql(tablename, engine, schema='refinement')
    
    #add trajectory field to table
    addTrajArrayField(tablename, fields, 'refinement')




def createRefinedTrajectory():
    
    traj = defineGDBpath(['pre','trajectories']) + 'traj_cdl'+refine.res+'_b_'+refine.datarange
    nlcd_mask = defineGDBpath(['refine','masks']) + 'traj_nlcd'+refine.res+'_b_'+refine.nlcd_years[0]+'or'+refine.nlcd_years[1]+'or'+refine.nlcd_years[2]+'_'+refine.name+refine.datarange+'_mask'
    trajYTC_mask = defineGDBpath(['refine','masks']) + 'traj_'+refine.name+refine.res+'_'+refine.datarange+'_mask'
    output = 'traj_cdl'+refine.res+'_b_'+refine.datarange+'_rfnd'
    print 'output:', output
    
    # create a filelist to format the argument for MosaicToNewRaster_management() function
    filelist = [traj, nlcd_mask, trajYTC_mask]
    print '-----[traj, nlcd_mask, trajYTC_mask]-----'
    print filelist

    #mosaicRasters():
    arcpy.MosaicToNewRaster_management(filelist, defineGDBpath(['pre','trajectories']), output, Raster(traj).spatialReference, '16_BIT_UNSIGNED', refine.res, "1", "LAST","FIRST")



################ Instantiate the class to create yxc object  ########################
refine = RefineObject(
      'ytc',
      '30',
      ## cdl data range---i.e. all the cdl years you are referencing 
      [2008,2016],
      ## ncdl datasets
      ['2001','2006','2011']
      )





##################  call functions  ############################################
   
###  create the mtr directly from the trajectories without filtering  ###################----NOTE NEED TO DO THIS TWICE NOW!!!
# createMTR()


### attach the cld values to the years binaries  #######################
for subtype in refine.subtypelist:
    print subtype
    # attachCDL(subtype)


### create change trajectories for subcategories in yxc ---- OK
# createChangeTrajectories()


###  add traj_yxc[res]_[datarange] attribute to postgres database in the refinement schema ----- ok
# addGDBTable2postgres() 


### create mask  #####################
##NOTE NEED TO GENERALIZE THIS FUNCTION!!!!!!!!!!!!!!!

###----hard coded still. Also has questionable inner method!
createMask_nlcdtraj('or')

###----?????  ---ok but has questionable inner method!
# createMask_trajytc()


### create the refined trajectory ########################
###----hard coded still
# createRefinedTrajectory()




# createReclassifyList()


























# arcpy.env.workspace = defineGDBpath(['ancillary','temp'])


# rasterfile = 'composite_cnty'
# traj_ytc = 'test_cnty'


# result = arcpy.GetCellValue_management(traj_ytc, "0 28")
# cellSize = result.getOutput(0)
# print(cellSize)



# result = arcpy.GetCellValue_management(rasterfile, "11616 14631")
# cellSize = result.getOutput(0)
# print(cellSize)


# # Get input Raster properties
# inRas = arcpy.Raster('C:/data/inRaster')
# lowerLeft = arcpy.Point(inRas.extent.XMin,inRas.extent.YMin)
# cellSize = ras.meanCellWidth

# # Convert Raster to numpy array
# arr = arcpy.RasterToNumPyArray(inRas,nodata_to_value=0)

# # Calculate percentage of the row for each cell value
# arrSum = arr.sum(1)
# arrSum.shape = (arr.shape[0],1)
# arrPerc = (arr)/arrSum

# #Convert Array to raster (keep the origin and cellsize the same as the input)
# newRaster = arcpy.NumPyArrayToRaster(arrPerc,lowerLeft,cellSize,
#                                      value_to_nodata=0)

# raster = arcpy.Raster(traj_ytc)
# array = arcpy.RasterToNumPyArray(raster)
# (height, width)=array.shape
# for row in range(0,height):
#     for col in range(0,width):
#         # print str(row)+","+str(col)+":"+str(array.item(row,col))

#         result = arcpy.GetCellValue_management(raster, str(row)+" "+str(col))
#         cellValue = result.getOutput(0)
#         print cellValue
#         if cellValue == 1229:
#             print(cellValue)




# raster1 = arcpy.Raster(traj_ytc)
# ex = arcpy.RasterToNumPyArray(raster1)
# # ex=np.arange(30)
# # ex=np.reshape(ex,[3,10])
# # print ex
# thelist = (ex == 5392).nonzero()
# print thelist[0]
# print thelist[1]

# yo = np.dstack((thelist[0],thelist[1]))
# print yo
# tt = yo.flat
# print tt
# print ex[11524,14463]










# raster2 = arcpy.Raster(rasterfile)
# ex2 = arcpy.RasterToNumPyArray(raster2)

# for x in yo:
#     print x[0][0]
#     print ex2[1,1]
    # result = arcpy.GetCellValue_management(raster, "18 14213")
    # cellSize = result.getOutput(0)
    # print(cellSize)



# result = arcpy.GetCellValue_management(rasterfile, "18 14213")
# cellSize = result.getOutput(0)
# print(cellSize)






# lowerLeft = arcpy.Point(inRas.extent.XMin,inRas.extent.YMin)
# cellSize = inRas.meanCellWidth

# # # Convert Raster to numpy array
# arr = arcpy.RasterToNumPyArray(inRas,nodata_to_value=0)


# import arcgisscripting
# gp=arcgisscripting.create()
# gp.multioutputmapalgebra(r'%s=sample(%s)' % ('Desktop/test.csv',traj_ytc))

# def getCDLvalueByYear(x):
#     print 'fdf', arr_comp[x[0],x[1]]
    # print arr_comp[x[0],x[1]]
    # return arr_comp[x[0],x[1]]



# import arcpy
# import numpy

# Get input Raster properties
# inRas = arcpy.Raster('C:/data/inRaster')

# mask = np.zeros((13789, 21973), dtype=np.int)
# # mask = np.zeros((20, 20))
# print mask


# inYTC = Raster(defineGDBpath(['ancillary','temp'])+'ytc_fish')
# arr_ytc = arcpy.RasterToNumPyArray(inYTC)
# inComp = Raster(defineGDBpath(['ancillary','temp'])+'composite_fish')
# arr_comp = arcpy.RasterToNumPyArray(inComp)
# inTraj = Raster(defineGDBpath(['ancillary','temp'])+'traj_fish_t2')
# arr_traj = arcpy.RasterToNumPyArray(inTraj)



#ytc dataset
# inYTC = Raster(defineGDBpath(['ancillary','temp'])+'ytc_fish')
# inYTC = Raster(defineGDBpath(['refine','ytc'])+'ytc30_2008to2016')
# # arr_ytc = arcpy.RasterToNumPyArray(in_raster=inYTC, lower_left_corner = arcpy.Point(inYTC.extent.XMin,inYTC.extent.YMin), ncols = 13789, nrows = 21973, nodata_to_value = 0)
# arr_ytc = arcpy.RasterToNumPyArray(in_raster=inYTC, lower_left_corner = arcpy.Point(inYTC.extent.XMin,inYTC.extent.YMin), nrows = 13789, ncols = 21973)
# print 'art',arr_ytc
#composite stack of clds

# self.inComp = defineGDBpath(['ancillary','temp'])+'composite'
# # self.arr_comp = arcpy.RasterToNumPyArray(Raster(inComp))
# self.inTraj = defineGDBpath(['refine','trajectories'])+'traj_ytc30_2008to2016'
# # inComp = Raster(defineGDBpath(['ancillary','temp'])+'composite_fish')


# inComp = Raster(defineGDBpath(['ancillary','temp'])+'composite')
# arr_comp = arcpy.RasterToNumPyArray(in_raster=inComp, lower_left_corner = arcpy.Point(inYTC.extent.XMin,inYTC.extent.YMin), nrows = 13789, ncols = 21973)


# inTraj = Raster(defineGDBpath(['refine','trajectories'])+'traj_ytc30_2008to2016')
# arr_traj = arcpy.RasterToNumPyArray(in_raster=inTraj, lower_left_corner = arcpy.Point(inYTC.extent.XMin,inYTC.extent.YMin), nrows = 13789, ncols = 21973)

#get the trajectory values that satify the condtion from postgres
# rows=createReclassifyList_mod()
# print 'rows------', rows

# thelist = (arr_traj == 5392).nonzero()

#find the location of each pixel labeled with specific arbitray value in the rows list  
# for row in rows:
#     # print 'arbitrary trajectory label:', row[0]

#     #Return the indices of the elements that are non-zero.
#     thelist = (arr_traj == row[0]).nonzero()
#     # print 'thelist----', thelist

#     ww=np.column_stack((thelist[0],thelist[1]))
#     # print ww
#     # print 'len----', len(ww)
#     count = 0
#     for x in ww:
#         yearlist=range(arr_ytc[x[0],x[1]], 2017)
#         # print 'yearlist----', yearlist
#         bandindexstart = 9 - len(yearlist)
#         bandindexlist=range(bandindexstart, 9)
#         # print bandindexlist

#         for index, bandindex in enumerate(bandindexlist):
#             currentband = arr_comp[bandindex]
#             # print currentband[x[0],x[1]]
#             bandindexlist[index] = currentband[x[0],x[1]]
#         # print bandindexlist

#         if bandindexlist.count(bandindexlist[0]) == len(bandindexlist):
#             print '-----------------same--------------------------------'
#             print bandindexlist
#             print 'x:',x[0]
#             print 'y:',x[1]
#             mask[x[0],x[1]] = 1


# myRaster = arcpy.NumPyArrayToRaster(mask,x_cell_size=30, y_cell_size=30, value_to_nodata=0)

# raster_path = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\temp_rasters\\'
# raster_name = 'mask_t41.tif'

# in_dataset = raster_path + raster_name

# myRaster.save(in_dataset)

#define projerction of the new raster
# arcpy.DefineProjection_management (in_dataset, arcpy.SpatialReference("NAD 1983 UTM Zone 11N"))

