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
import time
import logging
from multiprocessing import Process, Queue, Pool, cpu_count, current_process, Manager
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import json
import webcolors as wc
import palettable
# import matplotlib as mpl

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
# import replace_61_w_hard_crop


#import extension
arcpy.CheckOutExtension("Spatial")
# arcpy.env.parallelProcessingFactor = "95%"
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = "in_memory" 







def conversionHEX2RGB():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

	yo=palettable.colorbrewer.diverging.BrBG_8.colors
	print yo
	yo = yo[::-1]

	df = getPGtable()
	for index, row in df.iterrows():
		print index
		print row['class'], row['hex'], row['colormap']
		hi = yo[index]
		print 'hi', hi
		mi = map(str, hi)
		mimi = [str(row['class'])] + mi
		print 'yo map', mimi
		rgb_list = [str(row['class']), str(wc.hex_to_rgb(str(row['hex']))[0]), str(wc.hex_to_rgb(str(row['hex']))[1]), str(wc.hex_to_rgb(str(row['hex']))[2])]
		# rgb_list = [str(row['class'])]
		print rgb_list

		rgb_string = ' '.join(mimi)
	# 	print rgb_string

	# 	print df.at[index, 'colormap'] 
		df.at[index, 'colormap'] = rgb_string

	# print type(df)
	df = df['colormap']
	print 'fdfdf', df
	#  # df.to_sql(name='colormap_suitability', con=engine, schema='suitability', index=False, if_exists='replace')


	df.to_csv(path='C:\\Users\\Bougie\\Desktop\\misc\\colormaps\\suitability_t2.clr', index=False)











def conversionHEX2RGB_2(cba):

	print cba

	cba = map(str, cba)
	print cba

	# for color in cba:
	# 	###convert elements in list from interger to string
	# 	color = map(str, color)
	# 	print ' '.join(color)






def conversionHEX2RGB_3(pallette, flip):
	# engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

	
	print pallette
	yo = pallette[::-1]
	print yo

	# df = getPGtable()
	# for index, row in df.iterrows():
	# 	print index
	# 	print row['class'], row['hex'], row['colormap']
	# 	hi = yo[index]
	# 	print 'hi', hi
	# 	mi = map(str, hi)
	# 	mimi = [str(row['class'])] + mi
	# 	print 'yo map', mimi
	# 	rgb_list = [str(row['class']), str(wc.hex_to_rgb(str(row['hex']))[0]), str(wc.hex_to_rgb(str(row['hex']))[1]), str(wc.hex_to_rgb(str(row['hex']))[2])]
	# 	# rgb_list = [str(row['class'])]
	# 	print rgb_list

	# 	rgb_string = ' '.join(mimi)
	# # 	print rgb_string

	# # 	print df.at[index, 'colormap'] 
	# 	df.at[index, 'colormap'] = rgb_string

	# # print type(df)
	# df = df['colormap']
	# print 'fdfdf', df
	#  # df.to_sql(name='colormap_suitability', con=engine, schema='suitability', index=False, if_exists='replace')


	# df.to_csv(path='C:\\Users\\Bougie\\Desktop\\misc\\colormaps\\suitability_t2.clr', index=False)









# conversionHEX2RGB()
pallette = palettable.colorbrewer.diverging.BrBG_8.colors

conversionHEX2RGB_2(pallette)





