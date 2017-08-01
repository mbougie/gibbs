import arcpy
import multiprocessing
import os
import glob
import sys
import time
import logging
import general as gen
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager




case=['Bougie','Gibbs']

#import extension


def defineGDBpath(arg_list):
    gdb_path = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/arcgis/geodatabases/'+arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path 


arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 
arcpy.env.workspace = defineGDBpath(['sensitivity_analysis','mmu'])
arcpy.CheckOutExtension("Spatial")




def create_fishnet(wc):
	# arcpy.env.workspace = defineGDBpath(gdb_args_in)
	for raster in arcpy.ListDatasets(wc, "Raster"): 
        
		in_raster = arcpy.Raster(raster)
		print in_raster

		#delete previous fishnet feature class
		# arcpy.Delete_management(out_fishnet)
        if arcpy.Exists('fishnet'):
            print "fishnet already exists"
        else:

			#create a fishnet for the geodatabase
			out_fishnet = 'fishnet'

			#acquire parameters for creatfisnet function
			XMin = in_raster.extent.XMin
			YMin = in_raster.extent.YMin
			XMax = in_raster.extent.XMax
			YMax = in_raster.extent.YMax

			origCord = "{} {}".format(XMin, YMin)
			YAxisCord = "{} {}".format(XMin, YMax)
			cornerCord = "{} {}".format(XMax, YMax)

			cellSizeW = "0"
			cellSizeH = "0"

			numRows = 7
			numCols = 7

			geotype = "POLYGON"

			arcpy.env.outputCoordinateSystem = in_raster.spatialReference
			print in_raster.spatialReference.name

			#call CreateFishnet_management function
			arcpy.CreateFishnet_management(out_fishnet, origCord, YAxisCord, cellSizeW, cellSizeH, numRows, numCols, cornerCord, "NO_LABELS", "", geotype)

			#refine fisshnet by removing all cells that are null
			# refineFishnet(in_raster)

    
        




def refineFishnet(in_raster):
    print 'running createZonalStats() function....'
    def createZonalStats():

		# Set local variables
		in_zone_data = "fishnet"
		zone_field = "OID"
		in_value_raster = in_raster 

		# Check out the ArcGIS Spatial Analyst extension license
		arcpy.CheckOutExtension("Spatial")

		# Execute ZonalStatistics
		outZonalStatistics = arcpy.sa.ZonalStatisticsAsTable(in_zone_data, zone_field, in_value_raster,
		                                     "RANGE", "NODATA")

		# Save the output 
		outZonalStatistics.save("zonalStats")

    def refine():
			    #create list of tiles from zonal stats that have pixel values in them
		fields = arcpy.ListFields("zonalStats","OID")
		list_zonal = []
		for field in fields:
			fnf=(os.path.splitext(field.name)[0]).split("_")
			print int(fnf[1])
			list_zonal.append(int(fnf[1]))
		print list_zonal 

		#create list of all the tiles from the fishnet
		fishnet_list = []
		field = "OID"
		cursor = arcpy.SearchCursor("fishnet")
		for row in cursor:
		    fishnet_list.append(row.getValue(field))
		print fishnet_list

		#find the difference between the 2 lists that represent tiles with no pixels of value
		null_tiles = set(fishnet_list).difference(list_zonal)
		print null_tiles

		#delete null tiles from fishnet feature class
		# with arcpy.da.UpdateCursor("fishnet", "OID") as cursor:
		#     for row in cursor:
		#         if row[0] in null_tiles:
		#             cursor.deleteRow()

    
    #call local functions
    createZonalStats()
    refine()








# def execute_Nibble(in_extentDict):
# 	in_raster = defineGDBpath(['post','yfc'])+'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_fnc'
# 	ras1 = arcpy.Raster(in_raster)


# 	fc_count = in_extentDict[0]
# 	print fc_count
# 	procExt = in_extentDict[1]
# 	print procExt
# 	XMin = procExt[0]
# 	YMin = procExt[1]
# 	XMax = procExt[2]
# 	YMax = procExt[3]

# 	#set environments
# 	 #The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
# 	arcpy.env.snapRaster = in_raster
# 	arcpy.env.cellsize = in_raster
# 	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)
   
# 	in_mask_raster = defineGDBpath(['post','yfc'])+'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_mask'
# 	print 'in_mask_raster: ', in_mask_raster

# 	###  Execute Nibble  #####################
# 	ras_out = arcpy.sa.Nibble(in_raster, in_mask_raster, "DATA_ONLY")

# 	#clear out the extent for next time
# 	arcpy.ClearEnvironment("extent")
    
#     # print fc_count
# 	outname = "tile_test" + str(fc_count) +'.tif'

# 	outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/temp2", r"tiles", outname)

# 	ras_out.save(outpath)



def execute_Nibble(in_extentDict):
    def nibble(wc):
	    # def nibble(wc):
		arcpy.env.workspace = defineGDBpath(['post','yfc'])
		
		for raster in arcpy.ListDatasets('*'+wc+'*', "Raster"): 
			# clear out previous rasters
			os.chdir("C:/Users/Bougie/Desktop/Gibbs/temp_processing/tiles")
			gen.deleteFiles()

			# print 'raster: ', raster
			temp_raster = raster.strip(wc)
			in_raster = arcpy.Raster(temp_raster)
			print in_raster
			in_mask_raster = arcpy.Raster(raster)
			# in_raster = arcpy.Raster('traj_cdl_b_n4h_mtr_8w')
			# in_mask_raster = arcpy.Raster('traj_cdl_b_n4h_mtr_8w_msk23')

			# in_raster = defineGDBpath(['post','yfc'])+'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_fnc'
			# in_mask_raster = defineGDBpath(['post','yfc'])+'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_mask'
			print 'in_mask_raster: ', in_mask_raster

			# setEnvironment(in_extentDict, in_raster)
			print in_extentDict
			fc_count = in_extentDict[0]
			print fc_count
			procExt = in_extentDict[1]
			print procExt
			XMin = procExt[0]
			YMin = procExt[1]
			XMax = procExt[2]
			YMax = procExt[3]


			arcpy.env.snapRaster = in_raster
			arcpy.env.cellsize = in_raster
			arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)

			###  Execute Nibble  #####################
			ras_out = arcpy.sa.Nibble(in_raster, in_mask_raster, "DATA_ONLY")

			#clear out the extent for next time
			arcpy.ClearEnvironment("extent")
		    
		    # print fc_count
			fc_count = in_extentDict[0]
			print fc_count
			outname = "tile_" + str(fc_count) + '_' + raster + '.tif'
		 
			outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/temp_processing", r"tiles", outname)

		   	ras_out.save(outpath)

    ##ccall regionGroup local function
    nibble("_nbl_")








def execute_RegionGroup(in_extentDict):

	regionGroup_combos = {'full':{'4w':["FOUR", "WITHIN"],'4c':["FOUR", "CROSS"],'8w':["EIGHT", "WITHIN"],'8c':["EIGHT", "CROSS"]},'baseline': {'8w':["EIGHT", "WITHIN"]}}


	def regionGroup(gdb_args_in, wc, region_combos):

	    #define workspace
	    arcpy.env.workspace=defineGDBpath(gdb_args_in)

	    for k, v in region_combos.iteritems():
	        print k,v
	        for raster in arcpy.ListDatasets(wc, "Raster"): 
				print 'raster: ', raster

				in_raster = arcpy.Raster(raster)
				print in_raster

				setEnvironment(in_extentDict, in_raster)

				###  Execute Region Group  #####################
				ras_out = arcpy.sa.RegionGroup(in_raster, v[0], v[1],"NO_LINK")

				#clear out the extent for next time
				arcpy.ClearEnvironment("extent")
                
				fc_count = in_extentDict[0]

				# print fc_count
				outname = "tile_test" + str(fc_count) +'.tif'

				outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/temp3", r"tiles", outname)

				ras_out.save(outpath)

	  

	 #ccall regionGroup local function
	regionGroup(['sensitivity_analysis','mtr'], "traj_cdl_b_n4h_mtr", regionGroup_combos['baseline'])




def setEnvironment(in_extentDict, in_raster):
	print in_extentDict
	fc_count = in_extentDict[0]
	print fc_count
	procExt = in_extentDict[1]
	print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]


	arcpy.env.snapRaster = in_raster
	arcpy.env.cellsize = in_raster
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)







def createMosaicRaster_Nibble():

	templist = glob.glob("C:/Users/Bougie/Desktop/Gibbs/temp2/tiles/*.tif")
	print templist 
	######mosiac tiles together into a new raster
	arcpy.MosaicToNewRaster_management(glob.glob("C:/Users/Bougie/Desktop/Gibbs/temp/tiles/*.tif"), defineGDBpath(['post','yfc']), 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_fnc_fnl', in_raster.spatialReference, "8_BIT_UNSIGNED", "30", "1", "LAST","FIRST")

	##Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(defineGDBpath(['post','yfc']) + 'yfc_years_traj_rfnd_n8h_mtr_8w_msk23_nbl_fnc_fnl', "Overwrite")






if __name__ == '__main__':
	

    ###### NOTE FISHNET FUNCTION NEEDS WORK !!!!!!  ###########################
	# create_fishnet('*')

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor("fishnet_refined", ["SHAPE@"]):
		extent_curr = row[0].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[count] = ls
		count+=1
    
	print extDict
	print extDict.items()
    

	# execute_Nibble(extDict.items())
	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=cpu_count())
	pool.map(execute_Nibble, extDict.items())
	pool.close()
	pool.join





    

