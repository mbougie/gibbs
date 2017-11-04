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
import numpy as np, sys, os
import pandas as pd
import collections
from collections import namedtuple
from collections import Counter
import psycopg2
from sqlalchemy import create_engine
import gdal



try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"


arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 

case=['Bougie','Gibbs']

#import extension
arcpy.CheckOutExtension("Spatial")

#establish root path for this the main project (i.e. usxp)
rootpath = 'C:/Users/'+case[0]+'/Desktop/'+case[1]+'/data/usxp/'

### establish gdb path  ####
def defineGDBpath(arg_list):
    gdb_path = rootpath + arg_list[0]+'/'+arg_list[1]+'.gdb/'
    print 'gdb path: ', gdb_path 
    return gdb_path





gdal.AllRegister()

# open the image
inDs = gdal.Open("C:/Users/Bougie/Desktop/Gibbs/temp/test_temp.tif")


# create the output image
driver = inDs.GetDriver()


inRas = defineGDBpath(['temp', 'temp']) + 'merged'


mask = np.zeros((13789, 21973), dtype=np.int)

my_array = arcpy.RasterToNumPyArray(inRas,nodata_to_value=255)

u, counts = np.unique(my_array, return_counts=True)

nodata_count = counts[5]
print nodata_count


def getww(my_array):
	# first create an indices list
	indexlist = np.where(my_array == 255)
	print indexlist
	ww=np.column_stack((indexlist[0],indexlist[1]))
	print ww
	return ww

def attachValue(ww):
	for x in ww:
		# print x
		# print x[0]
		# print x[1]
		if x[0] > 0 and x[0] < 13788 and x[1] < 21972:
			w_arg = [x[0]-1,x[1]]
			e_arg = [x[0]+1,x[1]]
			n_arg = [x[0],x[1]+1]
			s_arg = [x[0],x[1]-1]

			w = my_array[w_arg[0],w_arg[1]]
			e = my_array[e_arg[0],e_arg[1]]
			n = my_array[n_arg[0],n_arg[1]]
			s = my_array[s_arg[0],s_arg[1]]


			cardinal = np.array([n,w,e,s])
			print 'cardinal', cardinal
			counts = np.bincount(cardinal)
			print 'counts', counts


		
			a = np.argmax(counts)
			# print 'max: ', a
			if a == 255:
				print 'a == 255 so get min value'
				a = np.amin(cardinal)
				print 'min: ', a
				my_array[x[0],x[1]] = a

			else:
				c = np.random.choice(np.flatnonzero(counts == counts.max()))
				print 'random:', c
				my_array[x[0],x[1]] = c

			# counter = counts[np.where(counts == 2)]
			# a = np.argmax(counts)
			# # print 'counter', counter
			# if len(counter) == 2:
			# 	# print 'tied'
			# 	index = np.where(counts == 2)[0]
			# 	# print 'index', index
			# 	if index[0] == 3 or index[1] == 3:
			# 		my_array[x[0],x[1]] = 3
			# 		# print 'new label is 3' 
			# 	elif index[0] == 255 or index[1] == 255:
			# 		a = np.amin(cardinal)
			# 		# print 'new label', a
			# 		my_array[x[0],x[1]] = a
			# 	else:
			# 		a = np.argmax(counts)
			# 		# 'new label', a
			# 		my_array[x[0],x[1]] = a
			
			# elif a == 255:
			# 	a = np.amin(cardinal)
			# 	# print 'a ==255 new label', a
			# else:
			# 	# print 'new label', a
			# 	my_array[x[0],x[1]] = a



				
				# print counts[4]
				# print counts[5]
			# else:
			# 	a = np.argmax(counts)
			# 	print counter
			# 	print 'a', a

			# if a == 255:
			# 	a = np.amin(cardinal)
			# 	# print 'change the value to:', a
			# # if cardinal.count(cardinal[0]) == len(cardinal):

			# my_array[x[0],x[1]] = a

			# # if x[0] > 0:
			# w = my_array[x[0]-1,x[1]]
			# e = my_array[x[0]+1,x[1]]
			# n = my_array[x[0],x[1]+1]
			# s = my_array[x[0],x[1]-1]


			# mask[x[0],x[1]] = 34
		# elif x[0] == 0 or x[0] > 13788 or x[1] > 21972:
		# 	print 'hi'






# while (nodata_count > 81776):

ww = getww(my_array)
attachValue(ww)

u, counts = np.unique(my_array, return_counts=True)
nodata_count = counts[5]
print nodata_count

print my_array

# print fc_count
outname = "tile_test30.tif"

#create
outpath = os.path.join("C:/Users/Bougie/Desktop/Gibbs/", r"tiles", outname)

cls = 21973
rws = 13789
outDs = driver.Create(outpath, cls, rws, 1, gdal.GDT_Int32)

outBand = outDs.GetRasterBand(1)

	
# # write the data
outBand.WriteArray(my_array)

# flush data to disk, set the NoData value and calculate stats
outBand.FlushCache()

PIXEL_SIZE = 30  # size of the pixel...        
YMax = 2758935
XMin = -378525

outDs.SetGeoTransform((XMin,PIXEL_SIZE,0,YMax,0,-PIXEL_SIZE))  
# outDs.SetGeoTransform(inDs.GetGeoTransform())
outDs.SetProjection(inDs.GetProjection())

# del outData   




# col_num = 
# if value in my_array[:, col_num]:
# 	print vaue
#     do_whatever


