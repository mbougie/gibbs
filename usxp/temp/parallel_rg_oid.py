import arcpy
from arcpy import env
from arcpy.sa import *
import os
import glob
import sys
from sqlalchemy import create_engine
import pandas as pd
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen



#import extension
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 




def addGDBTable2postgres(input):
	# Execute AddField twice for two new fields
	fields = [f.name for f in arcpy.ListFields(input)]
	print fields

	# # converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(input,fields)
	print arr

	# # convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)
	print df

	reclassifylist=[['0 NODATA']]
	for index, row in df.iterrows():
		templist=[]

		value=row['Value']
		mtr=row['oid'] 
		templist.append(str(value))
		templist.append(str(mtr))
		print templist
		str1 = ' '.join(templist)
		print str1
		# ww = [value + ' ' + mtr]
		meow = [str1]
		reclassifylist.append(meow)

	print reclassifylist

	stringlist = ';'.join(sum(reclassifylist, []))
	print stringlist

	return stringlist

'0 NODATA;1 1;10 1;11'



def tyit():
	rg_combos = {'4w':["FOUR", "WITHIN"], '8w':["EIGHT", "WITHIN"], '4c':["FOUR", "CROSS"], '8c':["EIGHT", "CROSS"]}
	rg_instance = rg_combos['8w']

	raster_in = Raster('C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s18\\core\\core_s18.gdb\\s18_v4_traj_cdl30_b_2008to2017_rfnd_v2_n8h_mtr_8w_mmu5')
	raster_rg = RegionGroup(raster_in, rg_instance[0], rg_instance[1], "ADD_LINK")
	raster_count = Lookup(raster_rg, "Count")
	raster_mask = SetNull(raster_rg, raster_count, cond)


	# #clear out the extent for next time
	# arcpy.ClearEnvironment("extent")

	# outname = "tile_rg_" + str(fc_count) +'.tif'

	outpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s18\\core\\core_s18.gdb\\raster_rg_gui'

	# # raster_shrink.save(outpath)
	raster_rg.save(outpath)

 #    arcpy.AddField_management(in_table='{}\\{}'.format(gdb,filename), field_name='oid', field_type='LONG')

	# arcpy.CalculateField_management('{}\\{}'.format(gdb,filename), "oid", "!OBJECTID!", "PYTHON_9.3", "")






def execute_task(mtr_value):


	raster_in = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s18\\core\\core_s18.gdb\\s18_{}_counts'.format(mtr_value)

	raster_out = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s18\\core\\core_s18.gdb\\s18_{}_oid'.format(mtr_value)


	return_string=addGDBTable2postgres(raster_in)

	# Execute Reclassify
	arcpy.gp.Reclassify_sa(raster_in, "Value", return_string, raster_out, "NODATA")

	gen.buildPyramids(raster_out)





# tyit()
# execute_task('mtr3')












