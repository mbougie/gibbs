import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import arcpy
from arcpy import env
from arcpy.sa import *
import os, errno
import glob
import math
import json
import shutil




try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"






'''######## DEFINE THESE EACH TIME ##########'''
#NOTE: need to declare if want to process ytc or yfc
yxc = 'yfc'

#the associated mtr value qwith the yxc
yxc_mtr = {'ytc':'3', 'yfc':'4'}

#Note: need to change this each time on different machine
case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")



def establishConn(db):
    try:
        conn = psycopg2.connect("dbname="+db+" user='mbougie' host='144.92.235.105' password='Mend0ta!'")
        return conn
    except:
        print "I am unable to connect to the database"


#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'
# rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'
# ### establish gdb path  ####
# def defineGDBpath(arg_list):
#     gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
#     print 'gdb path: ', gdb_path 
#     return gdb_path


def defineGDBpath(args_list):
    gdb_path = '{}{}/{}/{}.gdb/'.format(rootpath,args_list[0],args_list[1],args_list[2])
    # print 'gdb path: ', gdb_path 
    return gdb_path



            


def importCSVtoPG():
    print pd.read_csv('D:\\projects\\nri\\NRI\\Raw_Data\\NRIdata\\nri12_cty_121115.csv', nrows=5)

    # df = pd.read_csv('D:\\projects\\nri\\NRI\\Raw_Data\\NRIdata\\nri12_cty_121115.csv')


    # # df.columns = [c.lower() for c in df.columns] #postgres doesn't like capitals or spaces
    # # df.columns = ['year_'+str(c) for c in df.columns]
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nri')

    # df.to_sql("try", engine, schema='control')
    csv_file = 'D:\\projects\\nri\\NRI\\Raw_Data\\NRIdata\\nri12_cty_121115.csv'
    chunksize = 100
    i = 0
    j = 1
    for df in pd.read_csv(csv_file, chunksize=chunksize, iterator=True):
          # df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
          df.index += j
          i+=1
          print i
          df.to_sql("nri12_cty_121115", engine, schema='control', if_exists='append')
          j = df.index[-1] + 1



def fetchPG(db, query):
    conn=establishConn(db)
    print query
    cur = conn.cursor()
    cur.execute(query)
    the_tuple = cur.fetchall()
    the_list = [i[0] for i in the_tuple]
    return the_list


def commitPG(query):
    print query
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()



def getPixelConversion2Acres(res):
    res_dict = {56:0.774922476, 30:0.222395}

    return res_dict[res]



def getPixelConversion2Hectares(res):
    res_dict = {30:0.09}

    return res_dict[res]

    # if resolution == 56:
    #     coef = 0.774922476
    #     return coef
    # elif resolution == 30:
    #     coef = 0.222395
    #     return coef


def getPixelCount(resolution, acres):
    # '''
    #  -------------------- CONDITION  ------------------------------------------
    #  CONVERSION: 900square miles = 0.222395 acres

    #  acres   count
    #  5       23
    #  10      45
    #  15      68

    #  example: masks=['23','45','68']

    #  --------------------------------------------------------------------------
    #  '''
    if resolution == '56':
        pixels = math.ceil(acres/0.774922476)
        print pixels
        return pixels
    elif resolution == '30':
        pixels = math.ceil(acres/0.222395)
        print pixels
        return pixels


def getAcres(pixel_count, resolution):
    if resolution == 56:
        acres = pixel_count*0.774922476
        print acres
        return acres
    elif resolution == 30:
        acres = pixel_count*0.222395
        print acres
        return acres

def addGDBTable2postgres_table(gdb, pgdb, schema, table):
    arcpy.env.workspace = gdb
    # tables = arcpy.ListTables()
    # for table in tables:
    #     print(table)
    fields = [f.name for f in arcpy.ListFields(table)]
    print fields


    arr = arcpy.da.TableToNumPyArray(table, fields)
    print arr



def addGDBTable2postgres_raster(gdb, pgdb, schema, table):
    print("addGDBTable2postgres().............")
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{0}'.format(pgdb))

    currentobject = '{}//{}'.format(gdb,table)
    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr

    # # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)
    print 'df-----------------------', df

    # df.to_sql(table, con=engine, schema=schema)



def addGDBTable2postgres_fc(gdb, pgdb, schema, table):
    print("addGDBTable2postgres().............")
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{0}'.format(pgdb))

    currentobject = '{}//{}'.format(gdb,table)
    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    ###need to remove shape column because its a list and destroys the one dimensionality requirement
    fields.remove('Shape')

    print fields

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr

    # # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)
    print 'df-----------------------', df

    df.to_sql(table, con=engine, schema=schema)






def GDBTable2DF(pgdb, currentobject):
    print 'addGDBTable2postgres_histo..................................................'
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ### remove column
    del df['OBJECTID']
    print df

    # ##perform a psuedo pivot table
    # df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_st", value_name="count")


    df.columns = map(str.lower, df.columns)

    print df

    return df

# def addGDBTable2postgres_state(pgdb, currentobject):
#     print 'addGDBTable2postgres_histo..................................................'
#     print currentobject

#     ##set the engine.....
#     engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

#     # Execute AddField twice for two new fields
#     fields = [f.name for f in arcpy.ListFields(currentobject)]

#     # converts a table to NumPy structured array.
#     arr = arcpy.da.TableToNumPyArray(currentobject,fields)
#     print arr


#     #### convert numpy array to pandas dataframe
#     df = pd.DataFrame(data=arr)
#     ### remove column
#     del df['OBJECTID']
#     print df

#     # ##perform a psuedo pivot table
#     # df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_st", value_name="count")


#     df.columns = map(str.lower, df.columns)

#     print df
    
#     # #### format column in df #########################
#     # ## strip character string off all cells in column
#     # df['atlas_st'] = df['atlas_st'].map(lambda x: x.strip('atlas_'))
#     # ## remove comma from year
#     # df['value'] = df['label'].str.replace(',', '')

#     # print df


#     # print 'pixel conversion:', getPixelConversion2Acres(30)

#     # ####add column 
#     # df['acres'] = df['count']*getPixelConversion2Acres(30)

#     tablename = currentobject.split('\\')[-1]
#     print 'tablename', tablename

#     print df

#     df.to_sql(tablename, engine, schema='synthesis_extensification')

#     # MergeWithGeom(df, tablename, eu, eu_col)








def tableTotable():
    # Import system modules
    import arcpy
    from arcpy import env
     
    # Set environment settings
    env.workspace = "C:/data"
     
    # Set local variables
    inTable = "vegtable.dbf"
    outLocation = "C:/output/output.gdb"
    outTable = "estuarine"

    # Set the expression, with help from the AddFieldDelimiters function to select the appropriate field delimiters for the data type
    expression = arcpy.AddFieldDelimiters(env.workspace, "VEG_TYPE") + " = 'Estuarine'"
     
    # Execute TableToTable
    arcpy.TableToTable_conversion(inTable, outLocation, outTable, expression)












def addRasterAttributeTable(gdb_path, wc):

    arcpy.env.workspace = defineGDBpath(gdb_path)

    for raster in arcpy.ListDatasets(wc, "Raster"): 
        print 'raster: ',raster

        list_count=[]
        list_acres=[]


        
        #loop through each row and get the value for specified columns
        rows = arcpy.SearchCursor(raster)
        for row in rows:
            value = row.getValue('value')
            print value
            count = row.getValue('Count')
            print count



            cur = conn.cursor()
            query="INSERT INTO qaqc.counts_rasters VALUES ('" + str(raster) + "' , " + str(sum(list_count)) + " , " + str(res) + " , " + str(sum(list_acres))+ ")"
            print query
            cur.execute(query)
            conn.commit()
            # print type(count)
            # list_count.append(count)
            
            # res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")

            # #convert the result object into and integer
            # res = res.getOutput(0)
            # print res
            # print type(res)
            # acreage = g.getAcres(int(count), int(res))
            # list_acres.append(acreage)



def addRasterAttributeTableByRow(gdb_path, wc, tablename):

    arcpy.env.workspace = defineGDBpath(gdb_path)

    for raster in arcpy.ListDatasets(wc, "Raster"): 
        print 'raster: ',raster

        # list_count=[]
        # list_acres=[]


        
        #loop through each row and get the value for specified columns
        rows = arcpy.SearchCursor(raster)
        for row in rows:
            value = row.getValue('value')
            print value
            count = row.getValue('Count')
            print count
            acres = row.getValue('acres')
            print acres
            percent = row.getValue('percent')
            print percent



            
            # print type(count)
            # list_count.append(count)
            
            # res = arcpy.GetRasterProperties_management(raster, "CELLSIZEX")

            # #convert the result object into and integer
            # res = res.getOutput(0)
            # print res
            # print type(res)
            # acreage = getAcres(int(count), int(res))
            # # list_acres.append(acreage)
            # percent = ()


            # cur = conn.cursor()
            # query="INSERT INTO "+tablename+" VALUES ('" + str(raster) + "' , " + str(value) + " , "str(count) + " , " + str(acres) + " , " + str(percent)+ ")"
            # print query
            # cur.execute(query)
            # conn.commit()




def transposeTable(gdb_path, wc):

    arcpy.env.workspace = defineGDBpath(gdb_path)

    for table in arcpy.ListTables(wc): 
        print 'table: ', table
        fields = arcpy.ListFields(table)
        
        for field in fields:
            #constrant column names by excluding the below fields from the processing
            if field.name == 'OBJECTID' or field.name == 'ATLAS_STCO':
                print field.name
            else:
                # loop through each row and get the value for specified columns
                rows = arcpy.SearchCursor(table)
                for row in rows:
                    lc = row.getValue(field.name)
                    stco = row.getValue('ATLAS_STCO')
                    print 'table: ', table
                    print 'stco: ', stco
                    print 'field.name: ', field.name
                    print 'lc: ', lc
                    
                    cur = conn.cursor()
                    query="INSERT INTO refinement."+wc+" VALUES ('" + str(stco) + "' , '" + str(field.name) + "' , " + str(lc) + ")"
                    print query
                    cur.execute(query)
                    conn.commit()
     




def getPGTablesList(schema,wc):
    query = """SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = """+ schema + """
    AND table_name LIKE """ + wc
    print query
    return createListfromCursor(query)



def getPGColumnsList(schema, table, delimiter):
    query = """SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = """+ schema + """
    AND table_name = """ + table + """
    AND data_type = 'double precision' """
    print query
    return CreateStringFromList(createListfromCursor(query), delimiter)




def getChildDatasets(parent):
    query="""SELECT 
                dataset 
             FROM 
                qaqc.lookup_inheritance
             where parent = """ + parent
    return createListfromCursor(query)



def createListfromCursor(query):
    templist = []
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        templist.append(row[0])
    return templist


def CreateStringFromList(thelist, delimiter):
    # str1 = '+'.join('"{0}"'.format(w) for w in thelist)
    str1 = delimiter.join(thelist)
    print "str1", str1
    return str1














def createGSconvByYearANDlcc(wc, gdb_path, filename, years):

    arcpy.env.workspace = defineGDBpath(['deliverables','deliverables_refined'])

    print 'wc: ', wc

    for raster in arcpy.ListDatasets(wc, "Raster"): 

        print 'raster: ',raster

        # Set the cell size environment using a raster dataset.
        arcpy.env.cellSize = raster

        # Set Snap Raster environment
        arcpy.env.snapRaster = raster

        #set up the 2 datasets that will be used in the Con() function below
        yr_dset=Raster(defineGDBpath(gdb_path)+filename)
        lcc=Raster(defineGDBpath(['ancillary','misc'])+'LCC_100m')

        
        for year in years:
            output = 'gsconv_'+year+'_lcc'
            print 'output: ', output

            print year[2:]


            cond = "Value <> " + year[2:]
            print cond

            # # using 3 rasters in this condtion. only select pixels of year x, if true get the lcc value for that pixel if fale set to null!!
            OutRas=Con((yr_dset == int(year[2:])) & raster, lcc,(SetNull(raster,raster)))

            #Save the output 
            OutRas.save(output)


         
def setValueToNull(gdb_path, wc):
    #description: tabulate the values of the raster by county 

    # Set environment settings
    arcpy.env.workspace = defineGDBpath(gdb_path)
    
    # Set local variables
    for raster in arcpy.ListDatasets(wc, "Raster"): 
        print 'raster: ', raster

        arcpy.env.overwriteOutput = True
        print "overwrite on? ", arcpy.env.overwriteOutput

        #condition value is dependent of yxc (i.e. ytc = 3 and yfc = 4)
        cond = "Value = 0"
        print 'cond: ', cond

        # set mmu raster to null where not equal to value and then attached the values fron traj_years tp these [value] patches
        outSetNull = SetNull(raster, raster,  cond)

        #Save the output 
        outSetNull.save(raster)



# def reprojectRaster(gdb_path, wc):
#     #description: tabulate the values of the raster by county 

#     # Set environment settings
#     arcpy.env.workspace = defineGDBpath(gdb_path)
    
#     # Set local variables
#     for raster in arcpy.ListDatasets(wc, "Raster"): 
#         print 'raster: ', raster

#         raster_out = raster+'_initial2'


#         spatial_ref = arcpy.Describe(defineGDBpath(['ancillary','cdl'])+'cdl_2010').spatialReference
#         # spatial_ref = arcpy.Describe(raster).spatialReference
#         sr = arcpy.SpatialReference("Hawaii Albers Equal Area Conic")

#         print spatial_ref.Name
#         print spatial_ref.PCSCode
#         print spatial_ref.alias

#         arcpy.ProjectRaster_management(raster, raster_out, sr)



def reprojectRaster(raster_path, raster_name, raster_extension):
    #description: tabulate the values of the raster by county 

    # Set environment settings
    # arcpy.env.workspace = defineGDBpath(gdb_path)
    
    # Set local variables
    # for raster in arcpy.ListDatasets(wc, "Raster"): 
    print 'raster: ', raster

    raster_out = raster_path+'hello'+raster_extension


    spatial_ref = arcpy.Describe(defineGDBpath(['ancillary','cdl'])+'cdl_2010').spatialReference
    # spatial_ref = arcpy.Describe(raster).spatialReference
    sr = arcpy.SpatialReference("Hawaii Albers Equal Area Conic")

    print spatial_ref.Name
    print spatial_ref.PCSCode
    print spatial_ref.alias

    arcpy.ProjectRaster_management(raster, raster_out, sr)



    


# setValueToNull(['ancillary','xp_initial'],'*')
# reprojectRaster(['ancillary','xp_initial'],'ytc')




# wkt = "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],\
#                PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],\
#                VERTCS['WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],\
#                PARAMETER['Vertical_Shift',0.0],PARAMETER['Direction',1.0],UNIT['Meter',1.0]];\
#                -400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119522E-09;\
#                0.001;0.001;IsHighPrecision"


# wkt = "GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,6356752.314140356]],\
#                PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],\
#                VERTCS['WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],\
#                PARAMETER['Vertical_Shift',0.0],PARAMETER['Direction',1.0],UNIT['Meter',1.0]];\
#                -400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119522E-09;\
#                0.001;0.001;IsHighPrecision"




# Projection: Albers
# false_easting: 0.0
# false_northing: 0.0
# central_meridian: -96.0
# standard_parallel_1: 29.5
# standard_parallel_2: 45.5
# latitude_of_origin: 23.0
# Linear Unit: Meter (1.0)

# Geographic Coordinate System: GCS_North_American_1983
# Angular Unit: Degree (0.0174532925199433)
# Prime Meridian: Greenwich (0.0)
# Datum: D_North_American_1983
#   Spheroid: GRS_1980
#     Semimajor Axis: 6378137.0
#     Semiminor Axis: 6356752.314140356
#     Inverse Flattening: 298.257222101



# PROJCS["USA_Contiguous_Albers_Equal_Area_Conic",GEOGCS["GCS_North_American_1983",DATUM["North_American_Datum_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["longitude_of_center",-96],PARAMETER["Standard_Parallel_1",29.5],PARAMETER["Standard_Parallel_2",45.5],PARAMETER["latitude_of_center",37.5],UNIT["Meter",1],AUTHORITY["EPSG","102003"]]










'''
DROP TABLE qaqc.lookup_inheritance;

CREATE TABLE qaqc.lookup_inheritance as
select b.*,a.*
from qaqc.lookup_inheritance as a right outer join
(SELECT 
  counts_rasters.dataset
FROM 
  qaqc.counts_rasters
UNION

SELECT
  counts_tables.dataset
FROM
  qaqc.counts_tables) as b

on a.child = b.dataset;

'''



def addFIeldtoRaster(gdb_path, wc):
    arcpy.env.workspace = defineGDBpath(gdb_path)
    
    # fieldnames=['legend']
    fieldnames=[['name','Text'],['acres','Double'],['percent','Double'],['legend','Text']]
    
    for attributetable in arcpy.ListDatasets(wc, "Raster"): 
        print 'raster: ',attributetable
        for field in fieldnames:
            print field
            # in_table = attributetable
            # field_name = field
            # field_type = "DOUBLE"
            # fieldName1 = "acres"

         
            # Execute AddField twice for two new fields
            # arcpy.AddField_management(in_table=attributetable, field_name=field[0], field_type=field[1])
  

        


def fieldCalculator(gdb_path, wc):
    attributetable = defineGDBpath(gdb_path)+wc
    rows = arcpy.SearchCursor(attributetable)

    for row in rows:
        yo=row.getValue("value")
        newvalue = yo * 661
        print newvalue
        # value = row.getValue("value")
 
    update(attributetable,templist)


def getColumnValue(table,columnname):
    rows = arcpy.SearchCursor(table)
    for row in rows:

        atlas_stco = row.getValue(columnname)
        print atlas_stco



def update(attributetable,wc,newvalue):
    with arcpy.da.UpdateCursor(attributetable, wc) as cursor:
        for row in cursor:
            print row
            # if row == ''
            #     value=getColumnValue(attributetable,"value")
            # print row[0]
            # print row[1]
            # print row[2]
            # string = str(row[1]) + " " + str(row[2])
            # print string
            # row[0] = string
            
            # print row[1]
            # newValue = row[0]+'dsdsd'
            row[0] = newvalue
            cursor.updateRow(row)


def fieldCalculator2(gdb_path, wc):
    attributetable = defineGDBpath(gdb_path)+wc
    # rows = arcpy.SearchCursor(attributetable):
    print attributetable

    # print fc

    # # Define field name and expression
    field = 'acres'
    # field = 'percent'
    coef = getPixelConversion2Acres(resolution)
    expression = '!Count!*' + coef
    # expression = sum('!Count!')

    # # Create a new field with a new name
    # arcpy.AddField_management(fc,field,"TEXT")

    # # Calculate field here
    # arcpy.CalculateField_management(inFeatures, fieldName, expression, "PYTHON_9.3")
    arcpy.CalculateField_management(attributetable, field, expression, "PYTHON")




def createPGtableFromRaster():
    arcpy.env.workspace = defineGDBpath(['deliverables','xp_update_refined'])

    for raster in arcpy.ListDatasets('*', "Raster"): 
        print 'table: ', raster
        
        #define table and column names
        tablename = 'xp_update.'+raster
        print 'tablename', tablename

        cur = conn.cursor()
        query='CREATE TABLE '+tablename+'(dataset text, value integer, count integer, acres double precision, percent double precision)'

        # print query
        # cur.execute(query)
        # conn.commit()


        addRasterAttributeTableByRow(['deliverables','xp_update_refined_cartography'], '*', tablename)






def buildPyramids(inras):
    print 'running buildPyramids() function....'

    #Build Pyramids for single Raster Dataset
    #Define the type and compression of pyramids in the tool
    #Skip if dataset already has pyramids

    pyramid_level = "-1"
    skipfirst = "NONE"
    resample_technique = "BILINEAR"
    compression_type = "JPEG"
    compression_quality = "100"
    skipexist = "OVERWRITE"

    arcpy.BuildPyramids_management(inras, pyramid_level, skipfirst, resample_technique, 
                                   compression_type, compression_quality, skipexist)


def buildPyramids_new(inras, technique):
    print 'running buildPyramids() function....'

    #Build Pyramids for single Raster Dataset
    #Define the type and compression of pyramids in the tool
    #Skip if dataset already has pyramids

    pyramid_level = "-1"
    skipfirst = "NONE"
    resample_technique = technique
    compression_type = "JPEG"
    compression_quality = "100"
    skipexist = "OVERWRITE"

    arcpy.BuildPyramids_management(inras, pyramid_level, skipfirst, resample_technique, compression_type, compression_quality, skipexist)


############   CALL FUNCTIONS   #######################################

# addFIeldtoRaster(['deliverables','xp_update_refined'], 'bfc')
# fieldCalculator2(['deliverables','xp_update_refined'], 'bfc')
# update(attributetable,wc,newvalue)

# addRasterAttributeTable(['deliverables','xp_update_refined_cartography'], '*')


# createPGtableFromRaster()

def deleteFiles():
    filelist = glob.glob("*")
    for f in filelist:
        os.remove(f)




def getStatesField(field):
    fc = defineGDBpath(['ancillary', 'vector', 'shapefiles'])+'states'
    
    cursor = arcpy.SearchCursor(fc)
    states = []
    for row in cursor:
        state = row.getValue(field)
        states.append(state)
    return states



def getWorkSpaces(rootdir):
    # arcpy.env.workspace = defineGDBpath(gdb_path)
    arcpy.env.workspace = rootpath+rootdir

    # List all file geodatabases in the current workspace
    workspaces = arcpy.ListWorkspaces("*", "FileGDB")
     
    return workspaces




def commitQuery(query):
    #small wrapper function to commit a query
    cur = conn.cursor()

    cur.execute(query)

    conn.commit()
    print "Records created successfully";
    # conn.close()


def createDirectory(directory):
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise



def getCurrentInstance():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\instances\\current_instance.json') as json_data:
        template = json.load(json_data)
        return template



def zipFolder():
    shutil.make_archive(output_filename, 'zip', dir_name)




def getTablesInSchema(schema):
    cur = conn.cursor()
    query="SELECT table_name FROM information_schema.tables WHERE table_schema = '{}'".format(schema)
    print query

    cur.execute(query)
    rows = cur.fetchall()
    print rows
    return rows






def addRasterAttrib2postgres_recent(path, filename, database, schema):
    print path
    print filename
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(database))
    
    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(path)]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(path,fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df

    # total=df['count'].sum()
    
    # # # use pandas method to import table into psotgres
    df.to_sql(filename, engine, schema=schema, if_exists='replace')
    
    # #add trajectory field to table
    addAcresField('counts_yxc', data['post'][yxc]['filename'], yxc, 30, total)



def alterGeomSRID(db, schema, table, epsg):
    print 'alterGeomSRID()....................................'
    
    conn = psycopg2.connect("dbname={} user='mbougie' host='144.92.235.105' password='Mend0ta!'".format(db))

    query = 'ALTER TABLE {0}.{1} ALTER COLUMN wkb_geometry TYPE geometry(MultiPolygon,{2}) USING ST_SetSRID(wkb_geometry,{2});'.format(schema, table, epsg)

    print 'query', query

    #small wrapper function to commit a query
    cur = conn.cursor()

    cur.execute(query)

    conn.commit()







def addGDBTable2postgres_histo_state(pgdb, schema, currentobject):
    print 'addGDBTable2postgres_histo..................................................'
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ### remove column
    del df['OBJECTID']
    print df

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_st", value_name="count")


    df.columns = map(str.lower, df.columns)

    print df
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df['atlas_st'] = df['atlas_st'].map(lambda x: x.strip('atlas_'))
    ## remove comma from year
    df['value'] = df['label'].str.replace(',', '')

    print df


    print 'pixel conversion:', getPixelConversion2Acres(30)

    ####add column 
    df['acres'] = df['count']*getPixelConversion2Acres(30)

    tablename = currentobject.split('\\')[-1]
    print 'tablename', tablename

    print df

    df.to_sql(tablename, engine, schema=schema)

    # MergeWithGeom(df, tablename, eu, eu_col)



def addGDBTable2postgres_histo_county(pgdb, schema, currentobject):
    print 'addGDBTable2postgres_histo..................................................'
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ### remove column
    del df['OBJECTID']
    # print df

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_stco", value_name="count")

    df.columns = map(str.lower, df.columns)

    print df
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df['atlas_stco'] = df['atlas_stco'].map(lambda x: x.strip('ATLAS_'))
    ## remove comma from year
    # df['value'] = df['label'].str.replace(',', '')

    print df


    print 'pixel conversion:', getPixelConversion2Acres(30)

    ####add column 
    df['acres'] = df['count']*getPixelConversion2Acres(30)

    tablename = currentobject.split('\\')[-1]
    print 'tablename', tablename

    print df

    df.to_sql(tablename, engine, schema=schema)

    # MergeWithGeom(df, tablename, eu, eu_col)




def addGDBTable2postgres_state(pgdb, schema, currentobject):
    print 'addGDBTable2postgres_histo..................................................'
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ### remove column
    del df['OBJECTID']
    print df

    # ##perform a psuedo pivot table
    # df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_st", value_name="count")


    df.columns = map(str.lower, df.columns)

    print df
    
    # #### format column in df #########################
    # ## strip character string off all cells in column
    # df['atlas_st'] = df['atlas_st'].map(lambda x: x.strip('atlas_'))
    # ## remove comma from year
    # df['value'] = df['label'].str.replace(',', '')

    # print df


    # print 'pixel conversion:', getPixelConversion2Acres(30)

    # ####add column 
    # df['acres'] = df['count']*getPixelConversion2Acres(30)

    tablename = currentobject.split('\\')[-1]
    print 'tablename', tablename

    print df

    df.to_sql(tablename, engine, schema=schema)

    # MergeWithGeom(df, tablename, eu, eu_col)





def convertPGtoFC(gdb, pgdb, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -a_srs EPSG:5070 -f "FileGDB" -progress -update {0} PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {2}.{3}" -nln {3} -nlt MULTIPOLYGON'.format(gdb, pgdb, schema, table)
    print command
    os.system(command)





# def convertFCtoPG(gdb, pgdb, schema, table, epsg):
#     command = 'ogr2ogr -f "PostgreSQL" PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" {0} -nlt PROMOTE_TO_MULTI -nln {2}.{2} {3} -progress --config PG_USE_COPY YES'.format(gdb, pgdb, schema, table)
    
#     os.system(command)

    # alterGeomSRID(pgdb, schema, table, epsg)



def convertFCtoPG(gdb, pgdb, schema, table, epsg):
    command = 'ogr2ogr -t_srs EPSG:102003 -f "PostgreSQL" PG:"dbname={1} user=mbougie host=144.92.235.105 password=Mend0ta!" {0} -nln {2}.{3} {3} -progress -nlt MULTIPOLYGON --config PG_USE_COPY YES'.format(gdb, pgdb, schema, table)
    
    os.system(command)

    alterGeomSRID(pgdb, schema, table, epsg)


def null2value(in_raster, true_value, false_value):
    reclass = Con(IsNull(in_raster), true_value, false_value)
    return reclass



def createReclassifyList(pgdb, query):
    ###sub-function for reclassRaster
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))
    print 'query:', query
    df = pd.read_sql_query(query, con=engine)
    print df
    # fulllist=[[0,0,"NODATA"]]
    fulllist=[]
    for index, row in df.iterrows():
        templist=[]
        templist.append(row[0])
        templist.append(row[1])
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist



def addValue2Field(fc, field, value):
    cur = arcpy.UpdateCursor(fc)

    for row in cur:

        row.setValue(field, value)

        cur.updateRow(row)