import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
import os
import glob
import sys
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
import general as gen


# arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 

case=['Bougie','Gibbs']

# #import extension
arcpy.CheckOutExtension("Spatial")

#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'
# rootpath = 'D:/projects/ksu/v2/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path



#######  define raster and mask  ####################
class ProcessingObject(object):

    def __init__(self, series, res, mmu, years, name):

        self.series = series
        self.res = str(res)
        self.mmu = str(mmu)
        self.years = years
        self.name = name

# s12_ytc30_2008to2016_mmu15_nbl
        ##### years objects
        self.yearcount=len(range(self.years[0], self.years[1]+1))
        self.datarange = str(self.years[0])+'to'+str(self.years[1])
        print 'self.datarange:', self.datarange
        self.conversionyears = range(self.years[0]+2, self.years[1])
        print 'self.conversionyears:', str(self.conversionyears)
        
        ##### derived datsets
        # self.traj_dataset = "traj_cdl"+self.res+"_b_"+self.datarange
        # self.traj_rfnd_dataset = self.series+"_traj_cdl"+self.res+"_b_"+self.datarange+'_rfnd'
        
        # self.traj_dataset_path = defineGDBpath(['pre','trajectories']) + self.traj_dataset
        self.yxc_dataset = self.series+"_"+self.name+self.res+'_'+self.datarange+"_mmu"+self.mmu+"_nbl"
        self.inputraster = defineGDBpath(['post',self.name])+self.yxc_dataset
        self.out_fishnet = defineGDBpath(['ancillary', 'shapefiles']) + 'fishnet_ytc'
        self.dir_tiles = 'C:/Users/Bougie/Desktop/Gibbs/tiles/'
        # self.raster_subtype = defineGDBpath(['post',self.name])+self.yxc_dataset+'_fc'




        def getYXCAttributes():
            if self.name == 'ytc':
                self.mtr = '3'
                self.subtypelist = ['bfc','fc']
                print 'yo', self.subtypelist
                self.traj_change = 1

        
            elif self.name == 'yfc':
                self.mtr = '4'
                self.subtypelist = ['bfnc','fnc']
                print 'yo', self.subtypelist



        getYXCAttributes()


        def getCDLlist(self):
            cdl_list = []
            for year in self.data_years:
                cdl_dataset = 'cdl'+self.res+'_b_'+str(year)
                cdl_list.append(cdl_dataset)
            print'cdl_list: ', cdl_list
            return cdl_list

  

def execute_task(args):
	in_extentDict, pp_cdl = args


	fc_count = in_extentDict[0]
	# print fc_count
	procExt = in_extentDict[1]
	# print procExt
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]

	#set environments
	 #The brilliant thing here is that using the extents with the full dataset!!!!!!   DONT EVEN NEED TO CLIP THE FULL RASTER TO THE FISHNET BECASUE 
	arcpy.env.snapRaster = pp_cdl.yxc_dataset
	arcpy.env.cellsize = pp_cdl.yxc_dataset
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)


	###  Execute Nibble  #####################
	# attachCDL(fc_count, pp_cdl)

	#clear out the extent for next time
	# arcpy.ClearEnvironment("extent")
    
 #    # print fc_count
	# outname = "tile_" + str(fc_count) +'.tif'

	# #create Directory

	# outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

	# ras_out.save(outpath)



	def getAssociatedCDL(subtype, year):
	#this is an aux function for attachCDL() function to get correct cdl for the attachCDL() function

		if subtype == 'bfc' or  subtype == 'bfnc':
			# NOTE: subtract 1 from every year in list
			cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ pp_cdl.res + '_' + str(year - 1)
			return cdl_file

		elif subtype == 'fc' or  subtype == 'fnc':
			cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ pp_cdl.res + '_' + str(year)
			return cdl_file




	# NOTE: Need to copy the yxc_clean dataset and rename it with subtype after it
	# arcpy.env.workspace=defineGDBpath(['post',pp_cdl.name])


	# inputraster = defineGDBpath(['refine',refine.name])+refine.yxc_dataset

	# print "inputraster: ", inputraster

	# output = inputraster+'_'+subtype
	# print "output: ", output

	# ##copy binary years raster so it can be modified iteritively
	# arcpy.CopyRaster_management(inputraster, output)
	subtype = 'fc'



	wc = '*'+pp_cdl.res+'*'+subtype
	print wc
	# year = 2010
	for year in  pp_cdl.conversionyears:
		print 'year: ', year

		# allow output to be overwritten
		# arcpy.env.overwriteOutput = True
		# print "overwrite on? ", arcpy.env.overwriteOutput

		#establish the condition
		cond = "Value <> " + str(year)
		print 'cond: ', cond
		print fc_count

		cdl_file= getAssociatedCDL(subtype, year)
		print 'associated cdl file: ', cdl_file

		#set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year



		# print fc_count
		outname = "tile_"+str(fc_count)+"_"+str(year)+'.tif'
		print outname

		#create Directory

		outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

		ras_out = SetNull(pp_cdl.inputraster, cdl_file, cond)

		arcpy.ClearEnvironment("extent")

		# ras_out.save(outpath)

		





# def attachCDL(fc_count, pp_cdl):
#     # DESCRIPTION:attach the appropriate cdl value to each year binary dataset
#     print "-----------------attachCDL() function-------------------------------"


#     def getAssociatedCDL(subtype, year):
#         #this is an aux function for attachCDL() function to get correct cdl for the attachCDL() function

#         if subtype == 'bfc' or  subtype == 'bfnc':
#             # NOTE: subtract 1 from every year in list
#             cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ pp_cdl.res + '_' + str(year - 1)
#             return cdl_file

#         elif subtype == 'fc' or  subtype == 'fnc':
#             cdl_file = defineGDBpath(['ancillary','cdl'])+'cdl'+ pp_cdl.res + '_' + str(year)
#             return cdl_file





#     # NOTE: Need to copy the yxc_clean dataset and rename it with subtype after it
#     arcpy.env.workspace=defineGDBpath(['post',pp_cdl.name])

#     # inputraster = defineGDBpath(['refine',refine.name])+refine.yxc_dataset

#     # print "inputraster: ", inputraster
    
#     # output = inputraster+'_'+subtype
#     # print "output: ", output
    
#     # ##copy binary years raster so it can be modified iteritively
#     # arcpy.CopyRaster_management(inputraster, output)
#     subtype = 'fc'


 
#     wc = '*'+pp_cdl.res+'*'+subtype
#     print wc
  
#     for year in  pp_cdl.conversionyears:
# 		print 'year: ', year

# 		# allow output to be overwritten
# 		# arcpy.env.overwriteOutput = True
# 		# print "overwrite on? ", arcpy.env.overwriteOutput

# 		#establish the condition
# 		cond = "Value = " + str(year)
# 		print 'cond: ', cond
# 		print fc_count

# 		cdl_file= getAssociatedCDL(subtype, year)
# 		print 'associated cdl file: ', cdl_file

# 		#set everthing not equal to the unique trajectory value to null label this abitray value equal to conversion year

# 		arcpy.ClearEnvironment("extent")

# 		# print fc_count
# 		outname = "tile_"+str(fc_count)+'.tif'
# 		print outname

# 		#create Directory

# 		outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)
        


# 		ras_out = Con(pp_cdl.raster_subtype, cdl_file, pp_cdl.raster_subtype, cond)


# 		ras_out.save(outpath)


# 		#     OutRas.save(output)

#     # #build pyramids t the end
#     # gen.buildPyramids(output)




def mosiacRasters(nibble):
	tilelist = glob.glob(pp_cdl.dir_tiles+'*.tif')
	print tilelist 
	######mosiac tiles together into a new raster
	nbl_raster = pp_cdl.mask_name + '_nbl'
	print 'nbl_raster: ', nbl_raster

	arcpy.MosaicToNewRaster_management(tilelist, pp_cdl.gdb_path, nbl_raster, Raster(pp_cdl.in_raster).spatialReference, pp_cdl.pixel_type, pp_cdl.res, "1", "LAST","FIRST")

	##Overwrite the existing attribute table file
	arcpy.BuildRasterAttributeTable_management(pp_cdl.gdb_path + nbl_raster, "Overwrite")

	## Overwrite pyramids
	gen.buildPyramids(pp_cdl.gdb_path + nbl_raster)



  
def run(series, res, mmu, years, name):  
	#instantiate the class inside run() function
	pp_cdl = ProcessingObject(series, res, mmu, years, name)
	
	

	print "inputraster: ", pp_cdl.inputraster

	# output = inputraster+'_'+subtype
        #note:checkout out spatial extension after creating a copy or issues
    

	##copy binary years raster so it can be modified iteritively
	# arcpy.CopyRaster_management(pp_cdl.inputraster, pp_cdl.output)
	# arcpy.CheckOutExtension("Spatial")

	#remove a files in tiles directory
	tiles = glob.glob(pp_cdl.dir_tiles+"*")
	for tile in tiles:
		os.remove(tile)

	#get extents of individual features and add it to a dictionary
	extDict = {}
	count = 1 

	for row in arcpy.da.SearchCursor(pp_cdl.out_fishnet, ["SHAPE@"]):
		extent_curr = row[0].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[count] = ls
		count+=1
    
	# print 'extDict', extDict
	# print'extDict.items()',  extDict.items()

	######create a process and pass dictionary of extent to execute task
	pool = Pool(processes=1)
	pool.map(execute_task, [(ed, pp_cdl) for ed in extDict.items()])
	pool.close()
	pool.join

	# mosiacRasters(nibble)