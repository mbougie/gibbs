import arcpy, os  
from arcpy import env  
from arcpy.sa import *  
import glob


arcpy.CheckOutExtension("Spatial")  
arcpy.env.overwriteOutput = True  


env.workspace = 'D:\\projects\\usxp\\deliverables\\maps\\synthesis\\intensification\\eric\\n_application\\n_application.gdb'

scene_list = list(range(1, 6))
years_list = list(range(2007, 2011))
# years_list = [2007]
print years_list

for scene in scene_list:

	print 'scene', scene

	processed_list = []
	print 'processed_list', processed_list
	for year in years_list:
		print 'year', year

		raster_list = glob.glob('D:\\projects\\usxp\\deliverables\\maps\\synthesis\\intensification\\eric\\n_application\\Scen{}\\*_{}.tif'.format(str(scene), str(year)))

		print 'raster_list', raster_list

		# Execute CellStatistics  
		processed_list.append(CellStatistics(raster_list, "SUM", "DATA"))

		raster_list = None

	raster_mean = CellStatistics(processed_list, "MEAN", "DATA")

	del processed_list[:]

	# Save the output   
	raster_mean.save("Napplication2007_2016mean_Scen{}".format(str(scene))) 

	raster_mean = None






	
