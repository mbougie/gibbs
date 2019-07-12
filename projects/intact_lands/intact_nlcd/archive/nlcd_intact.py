# Import system modules
import sys 
# sys.path.append("C:\Users\Bougie\Desktop\Gibbs\scripts\usxp\\misc")
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2
from itertools import groupby
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import fnmatch


arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 


try:
    conn = psycopg2.connect("dbname='intact_lands' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



def getGDBpath(wc):
    for root, dirnames, filenames in os.walk("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\"):
        for dirnames in fnmatch.filter(dirnames, '*{}*.gdb'.format(wc)):
            print dirnames
            gdbmatches = os.path.join(root, dirnames)
    print gdbmatches
    # return json.dumps(gdbmatches)
    return gdbmatches




# data = gen.getJSONfile()
# print data



def mosiacRasters():
    arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\'

    cdl_raster=Raster("cdl30_2015")
  

    elevSTDResult = arcpy.GetRasterProperties_management(cdl_raster, "TOP")
    YMax = elevSTDResult.getOutput(0)
    elevSTDResult = arcpy.GetRasterProperties_management(cdl_raster, "BOTTOM")
    YMin = elevSTDResult.getOutput(0)
    elevSTDResult = arcpy.GetRasterProperties_management(cdl_raster, "LEFT")
    XMin = elevSTDResult.getOutput(0)
    elevSTDResult = arcpy.GetRasterProperties_management(cdl_raster, "RIGHT")
    XMax = elevSTDResult.getOutput(0)

    arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

    #### need to wrap these paths with Raster() fct or complains about the paths being a string
    
    rasterlist = ['resampled_cdl30_2007_p1','D:\\projects\\ksu\\v2\\attributes\\rasters\\cdl30_2007.img']

    ######mosiac tiles together into a new raster
    arcpy.MosaicToNewRaster_management(rasterlist, data['refine']['gdb'], data['refine']['mask_2007']['filename'], cdl_raster.spatialReference, "16_BIT_UNSIGNED", 30, "1", "LAST","FIRST")


    #Overwrite the existing attribute table file
    arcpy.BuildRasterAttributeTable_management(data['refine']['mask_2007']['path'], "Overwrite")

    # Overwrite pyramids
    gen.buildPyramids(data['refine']['mask_2007']['path'])


    
def reclassifyRaster():
    # Description: reclass cdl rasters based on the specific arc_reclassify_table 
    gdb_args_in = ['ancillary', 'cdl']
    # Set environment settings
    arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\'

    raster = 'cdl30_2016'    
    print 'raster: ',raster

    outraster = raster.replace("_", "_b_")
    print 'outraster: ', outraster

    #define the output
    output = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\binaries.gdb\\'+outraster
    print 'output: ', output

    return_string=getReclassifyValuesString(gdb_args_in[1], 'b')

    #Execute Reclassify
    arcpy.gp.Reclassify_sa(raster, "Value", return_string, output, "NODATA")

    #create pyraminds
    gen.buildPyramids(output)



def getReclassifyValuesString(ds, reclass_degree):
    #Note: this is a aux function that the reclassifyRaster() function references
    cur = conn.cursor()
    
    query = 'SELECT value::text,'+reclass_degree+' FROM misc.lookup_'+ds+' WHERE '+reclass_degree+' IS NOT NULL ORDER BY value'
    #DDL: add column to hold arrays
    cur.execute(query);
    
    #create empty list
    reclassifylist=[]

    # fetch all rows from table
    rows = cur.fetchall()
    
    # interate through rows tuple to format the values into an array that is is then appended to the reclassifylist
    for row in rows:
        ww = [row[0] + ' ' + row[1]]
        reclassifylist.append(ww)
    
    print reclassifylist
    #flatten the nested array and then convert it to a string with a ";" separator to match arcgis format 
    columnList = ';'.join(sum(reclassifylist, []))
    print columnList
    
    #return list to reclassifyRaster() fct
    return columnList



def getCDLlist(data):
    cdl_list = []
    for year in data['global']['years']:
        print 'year:', year
        cdl_dataset = 'cdl{0}_b_{1}'.format(str(data['global']['res']),str(year))
        cdl_list.append(cdl_dataset)
    print'cdl_list: ', cdl_list
    return cdl_list





def createTrajectories(data):
    # Description: "Combines multiple rasters so that a unique output value is assigned to each unique combination of input values" -arcGIS def
    #the rasters where combined in chronoloigal order.

    # Set environment settings
    arcpy.env.workspace = getGDBpath('binaries')

    output = data['pre']['traj']['path']
    print 'output', output
    
    # ###Execute Combine
    outCombine = Combine(['cdl30_b_2008', 'cdl30_b_2009', 'cdl30_b_2010', 'cdl30_b_2011', 'cdl30_b_2012', 'cdl30_b_2013', 'cdl30_b_2014', 'cdl30_b_2015'])
  
    ###Save the output 
    outCombine.save(output)

    ###create pyraminds
    gen.buildPyramids(output)



def addGDBTable2postgres(schema, table):
	env.workspace = "D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb"
	# set the engine.....
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/intact_lands')

	# Execute AddField twice for two new fields
	fields = [f.name for f in arcpy.ListFields(table)]

	# converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(table,fields)
	print arr

	# convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)

	print df

	# use pandas method to import table into psotgres
	df.to_sql(table, engine, schema=schema)

	#add trajectory field to table
	addTrajArrayField(schema, table, fields)




def addTrajArrayField(schema, table, fields):
	#this is a sub function for addGDBTable2postgres()
	cur = conn.cursor()

	#convert the rasterList into a string
	columnList = ','.join(fields[3:])
	print columnList

	#DDL: add column to hold arrays
	cur.execute('ALTER TABLE {}.{} ADD COLUMN traj_array integer[];'.format(schema, table));

	#DML: insert values into new array column
	cur.execute('UPDATE {}.{} SET traj_array = ARRAY[{}];'.format(schema, table, columnList));

	conn.commit()
	print "Records created successfully";
	conn.close()






def createAddYFCTrajectory(data):
    print 'createAddYFCTrajectory(data)'
    # filelist = [data['pre']['traj']['path'], data['refine']['mask_fn_yfc_61']['path'], data['refine']['mask_fn_yfc_nlcd_mtr1']['path']]
    filelist = [data['pre']['traj']['path'], data['refine']['mask_fn_yfc_61']['path']]
    
    print 'filelist:', filelist
    
    ##### mosaicRasters():
    arcpy.MosaicToNewRaster_management(filelist, data['pre']['traj_rfnd']['gdb'], data['pre']['traj_yfc']['filename'], Raster(data['pre']['traj']['path']).spatialReference, '16_BIT_UNSIGNED', data['global']['res'], "1", "LAST","FIRST")

    #Overwrite the existing attribute table file
    arcpy.BuildRasterAttributeTable_management(data['pre']['traj_yfc']['path'], "Overwrite")

    # Overwrite pyramids
    gen.buildPyramids(data['pre']['traj_yfc']['path'])



def createRefinedTrajectory(data):
    # mask_fp_2007.run(data)
    # mask_fp_yfc_potential.run(data)
    # mask_fp_nlcd_yfc.run(data)
    # mask_fp_nlcd_ytc.run(data)
    # mask_fp_yfc.run(data)
    # mask_fp_ytc.run(data)


    ##### loop through each of the cdl rasters and make sure nlcd is last 
    # filelist = [data['pre']['traj']['path'], data['refine']['masks_yfc']['path'], data['refine']['masks_ytc']['path'], data['refine']['mask_2007']['path'], data['refine']['mask_nlcd']['path']]
    # filelist = [data['pre']['traj_yfc']['path'], data['refine']['mask_fp_2007']['path'], data['refine']['mask_fp_yfc_potential']['path'], data['refine']['mask_fp_nlcd_yfc']['path'], data['refine']['mask_fp_nlcd_ytc']['path'], data['refine']['mask_fp_yfc']['path'], data['refine']['mask_fp_ytc']['path']]
    filelist = [data['pre']['traj_yfc']['path'], data['refine']['mask_fp_2007']['path'], data['refine']['mask_fp_nlcd_yfc']['path'], data['refine']['mask_fp_nlcd_ytc']['path'], data['refine']['mask_fp_yfc']['path'], data['refine']['mask_fp_ytc']['path']]
    


    print 'filelist:', filelist
    
    ##### mosaicRasters():
    arcpy.MosaicToNewRaster_management(filelist, data['pre']['traj_rfnd']['gdb'], data['pre']['traj_rfnd']['filename'], Raster(data['pre']['traj']['path']).spatialReference, '16_BIT_UNSIGNED', data['global']['res'], "1", "LAST","FIRST")

    #Overwrite the existing attribute table file
    arcpy.BuildRasterAttributeTable_management(data['pre']['traj_rfnd']['path'], "Overwrite")

    # Overwrite pyramids
    gen.buildPyramids(data['pre']['traj_rfnd']['path'])




# reclassifyRaster()
# mosiacRasters()



####  these functions create the trajectory table  #############
# createTrajectories()
# addGDBTable2postgres('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\refine\\traj_traj.gdb\\', 'refinement_new', 'traj_try')
# createRefinedTrajectory()


#######  these functions are to update the lookup tables  ######
# labelTrajectories()
# FindRedundantTrajectories()

def addBinaryField(schema, table):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/intact_lands')
	query = "SELECT * FROM nlcd_intact.combine_nlcd_intact_initial"
	print 'query:', query
	df = pd.read_sql_query(query, con=engine)
	print df

	# df = df.assign(e=pd.Series(np.random.randn(len(df['index']))).values)
	# print df

	for index, row in df.iterrows():

   		print index
   		print row['traj_array']
	# 	# print x
		false_list=[1, 81, 82, 22, 23, 24]
		if(np.isin(row['traj_array'], false_list).any()):
			print "this is a bad array"
			df.loc[index,'b'] = 0
	
		else:
			print "this is a good array"
			df.loc[index,'b'] = 1

	print 'df', df

	df.to_sql(table, engine, schema=schema)






def reclassRaster(inraster, outraster, query):
	print "reclassRaster..........."
	env.workspace = 'D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb'
	raster_reclass = Reclassify((inraster), "Value", RemapRange(createReclassifyList(query)), "NODATA")
	raster_reclass.save(outraster)



def createReclassifyList(query):
    ###sub-function for reclassRaster
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/intact_lands')
    print 'query:', query
    df = pd.read_sql_query(query, con=engine)
    print df
    fulllist=[[0,0,"NODATA"]]
    for index, row in df.iterrows():
        templist=[]
        templist.append(row['Value'])
        templist.append(int(row['b']))
        fulllist.append(templist)
    print 'fulllist: ', fulllist
    return fulllist


def reclassRaster_small():
	inraster = Raster("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2008")
	outraster = "D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\test"
	raster_reclass = Reclassify((inraster), "Value", RemapRange([[37,1],[176,1]]), "NODATA")
	raster_reclass.save(outraster)

def addCDL2008():
	raster_cdl2008 = Raster("C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2008")
	raster_combine = Raster("D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\combine_nlcd_intact_rc")
	outCon = Con((((raster_cdl2008 == 176) | (raster_cdl2008 == 37)) & (raster_combine==1)), 1, 0)
	outCon.save("D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\intact_grasslands_2008")




############### block stats code ######################################################

def processingCluster(inraster, outraster, scale):
	for key, value in scale.iteritems():

		nbr = NbrRectangle(value, value, "CELL")
		###using sum is the superior choice becasue it still expresses the block regardless if it is not a majority.  Can use a percent column to express gradeint of the block
		##depending on how many of the pixels are inside of the block
		# outBlockStat = BlockStatistics(inraster, nbr, "SUM", "DATA")
		outAggreg = Aggregate(inraster, value, "SUM", "EXPAND", "DATA")
		print 'finished block stats.............'

	outAggreg.save(outraster) 

	###add a percent field to determine what gradient value the block gets!
	addField(outraster, value)   

	gen.buildPyramids_new(outraster, 'NEAREST')



def addField(raster, value):
    normalizer = value*value
    ##AddField_management (in_table, field_name, field_type, {field_precision}, {field_scale}, {field_length}, {field_alias}, {field_is_nullable}, {field_is_required}, {field_domain})
    arcpy.AddField_management(in_table=raster, field_name='percent', field_type='FLOAT')

    cur = arcpy.UpdateCursor(raster)

    for row in cur:
    	print (float(row.getValue('Value'))/normalizer)*100
        row.setValue('percent', ((float(row.getValue('Value'))/normalizer)*100))
        cur.updateRow(row)








def convertedGrasslands():
	raster_s35_bfc = Raster("D:\\projects\\usxp\\deliverables\\s35\\s35.gdb\\s35_bfc")
	raster_intact_grasslands_2008 = Raster("D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\intact_grasslands_2008")

	outCon = Con((((raster_s35_bfc == 176) | (raster_s35_bfc == 37)) & (raster_intact_grasslands_2008==1)), 1, 0)
	outCon.save("D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\converted_grasslands_2016")




# addGDBTable2postgres(schema='nlcd_intact', table='combine_nlcd_intact_initial')
# addBinaryField(schema='nlcd_intact', table='combine_nlcd_intact')
# reclassRaster(inraster='combine_nlcd_intact', outraster='combine_nlcd_intact_rc', query='SELECT * FROM nlcd_intact.combine_nlcd_intact')
# addCDL2008()
# reclassRaster_small()
# convertedGrasslands()


###block stats
# processingCluster(inraster="D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\intact_grasslands_2008", outraster="D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\intact_grasslands_2008_bs3km", scale={'3km':100})
# processingCluster(inraster="D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\converted_grasslands_2016", outraster="D:\\projects\\intact_land\\nlcd_intact\\nlcd_intact_t2.gdb\\converted_grasslands_2016_bs3km", scale={'3km':100})



####intact_converted stuff #####################################
addGDBTable2postgres('intact_converted', 'intact_grasslands_2008_bs3km')



