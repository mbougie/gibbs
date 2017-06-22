import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import arcpy
from arcpy import env
from arcpy.sa import *





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




try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



arcpy.CheckOutExtension("Spatial")





















def fetchPG(query):
    print query
    cur = conn.cursor()
    cur.execute(query)
    count = cur.fetchall()
    return count


def commitPG(query):
    print query
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()




def getAcres(pixel_count, resolution):
    if resolution == 56:
        acres = pixel_count*0.774922476
        print acres
        return acres
    elif resolution == 30:
        acres = pixel_count*0.222395
        print acres
        return acres



def addGDBTable2postgres(gdb_args,wc,pg_shema):
    print 'hello'
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

    arcpy.env.workspace = defineGDBpath(gdb_args)

    for table in arcpy.ListTables(wc): 
        print 'table: ', table

        # Execute AddField twice for two new fields
        fields = [f.name for f in arcpy.ListFields(table)]
        
        # converts a table to NumPy structured array.
        arr = arcpy.da.TableToNumPyArray(table,fields)
        print arr
        
        # convert numpy array to pandas dataframe
        df = pd.DataFrame(data=arr)

        print df

        df.columns = map(str.lower, df.columns)
        
        # use pandas method to import table into psotgres
        df.to_sql(table, engine, schema=pg_shema)



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



def getPGTablesList(schema,wc):
    query = """SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = """+ schema + """
    AND table_name LIKE """ + wc
    print query
    return createListfromCursor(query)



def getPGColumnsList(schema, table):
    query = """SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = """+ schema + """
    AND table_name = """ + table + """
    AND data_type = 'double precision' """
    print query
    return CreateStringFromList(createListfromCursor(query))




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


def CreateStringFromList(thelist):
    # str1 = '+'.join('"{0}"'.format(w) for w in thelist)
    str1 = ' + '.join(thelist)
    print "str1", str1
    return str1





###################  declare functions  #######################################################
def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 