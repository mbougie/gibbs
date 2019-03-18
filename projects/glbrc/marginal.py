import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys



arcpy.CheckOutExtension("Spatial")



def addGDBTable2postgres(in_raster):
    # set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/glbrc')
    
    # # path to the table you want to import into postgres
    # input = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\pre\\traj_rfnd\\v3\\v3_traj_rfnd.gdb\\v4_traj_cdl30_b_2008to2017_rfnd_v3'

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields('D:\\projects\\glbrc\\marginal.gdb\\{}'.format(in_raster))]
   
    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray('D:\\projects\\glbrc\\marginal.gdb\\{}'.format(in_raster),fields)
    print arr
    
    # convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    df.columns = map(str.lower, df.columns)

    print 'df-----------------------', df

    # total=df['count'].sum()
    
    # # # # use pandas method to import table into psotgres
    df.to_sql(in_raster, engine, schema='marginal')
    
    # # #add trajectory field to table
    # addAcresField('counts_yxc', data['post'][yxc]['filename'], yxc, 30, total)





def createReclassifyList(query):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/glbrc')
	# query_lsl = ''' SELECT 
	# 			value,
	# 			1 as new_value 
	# 			FROM 
	# 			marginal.combine_lsl
	# 			WHERE cdl30_2017 IN (37,152,176,195) AND gssurgo_muaggatt IN (5,6,7,8)
	# 			ORDER BY gssurgo_muaggatt'''

	# query_ral = ''' SELECT 
	# 			value,
	# 			1 as new_value  
	# 			FROM 
	# 			  marginal.combine_ral
	# 			WHERE value <> 1'''


   


	df = pd.read_sql_query(query, con=engine)
	print df
	fulllist=[[0,0,"NODATA"]]
	for index, row in df.iterrows():
	    templist=[int(row[0]), int(row[1])]
	    fulllist.append(templist)
	return fulllist



def reclassRaster(dict_lsl):
	arcpy.env.workspace = 'D:\\projects\\glbrc\\marginal.gdb'
	raster_reclassed = Reclassify(Raster(dict_lsl['in_raster']), dict_lsl['reclass_field'], RemapRange(dict_lsl['reclasslist']), "NODATA")
	raster_reclassed.save(dict_lsl['out_raster'])


def reclassRaster_nlcd(in_raster):
	print('--------------  reclassRaster_nlcd ---------------', in_raster)
	arcpy.env.workspace = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\nlcd.gdb'
	outCon = Con(IsNull(Raster(in_raster)),0, Con((Raster(in_raster) == 82),1,0))
	outCon.save('{}_rc82'.format(in_raster))
	# return outCon



# def combineRasters():

def combineRasters(raster_list, out_raster):
	print('--------------  combine raster  ---------------')
	outCombine = Combine(raster_list)
	outCombine.save(out_raster)




# def reclassRaster(dict_lowsuitabilityland):
# 	print dict_lowsuitabilityland
# 	outReclass1 = Reclassify(dict_lowsuitabilityland["landuse"], dict_lowsuitabilityland["Value"], RemapRange(dict_lowsuitabilityland["reclasslist"]), "NODATA")
# 	return outReclass1
# 	# outReclass1.save(out_raster)



# dict_lowsuitabilityland = {'in_raster':'dfdfdfdfdf','reclass_field':"Value",'reclasslist':[[37,1],[152,1],[176,1],[195,1]],'out_raster':'fdfdfdf'}
# part1 = reclassRaster(dict_lowsuitabilityland)


############### call functions ###############################################

addGDBTable2postgres('potential_biofuels')

# dict_lsl = {'in_raster':'combine_lsl','reclass_field':"value",'reclasslist':createReclassifyList(),'out_raster':'lsl'}
# reclassRaster(dict_lsl)
# dict_ral = {'in_raster':'combine_ral','reclass_field':"value",'reclasslist':createReclassifyList(),'out_raster':'marginal_ral'}
# reclassRaster(dict_ral)
# dict_biofuel_potential={'in_raster':'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\raster\\cdl.gdb\\cdl30_2017',
# 						'reclass_field':"value",
# 						'reclasslist':createReclassifyList(query="Select value "),
# 						'out_raster':'marginal_ral'
# 						}


# cdl30_2017=Raster('D:\\projects\\glbrc\\marginal.gdb\\cdl30_2017_rc')
# nlcd30_1992=reclassRaster_nlcd(in_raster='nlcd30_1992')
# nlcd30_2001=reclassRaster_nlcd(in_raster='nlcd30_2001')
# nlcd30_2006=reclassRaster_nlcd(in_raster='nlcd30_2006')
# nlcd30_2011=reclassRaster_nlcd(in_raster='nlcd30_2011')

# raster_list = [cdl30_2017, nlcd30_1992, nlcd30_2001, nlcd30_2006, nlcd30_2011]
# # raster_list = [cdl30_2017, nlcd30_1992]
# out_raster = 'D:\\projects\\glbrc\\marginal.gdb\\combine_ral'

# combineRasters(raster_list, out_raster)


# nlcd30_1992.save('D:\\projects\\glbrc\\marginal.gdb\\nlcd30_1992_t2')
# outCon.save('D:\\projects\\glbrc\\marginal.gdb\\nlcd30_1992')