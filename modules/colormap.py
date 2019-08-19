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



def getColorMap(engine, query, arglist, out_text):
	###description: function to convert hex values to rgb

	df = pd.read_sql_query(query,con=engine)

	print df 

	for index, row in df.iterrows():

		rgb =  wc.hex_to_rgb(row[arglist[1]])
		print rgb

		r = rgb[0]
		g = rgb[1]
		b = rgb[2]

		rgb_string = '{0} {1} {2} {3}'.format(row[arglist[0]], r, g, b)
		print rgb_string
		df.at[index, 'colormap'] = rgb_string



	print df
	df['colormap'].to_csv(out_text, sep='\t', index=False)




#####create parameters for function
engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/intactland')

query = """SELECT
			value,
			label,
			hex,

			CASE
			when label = 'non_intact_np' then 1
			when label = 'non_intact_p' then 2
			when label = 'nodata_np' then 3
			when label = 'nodata_p' then 4


			when label = 'forest_np' then 8
			when label = 'forest_p' then 9
			when label = 'shrubland_np' then 10
			when label = 'shrubland_p' then 11
			when label = 'wetland_np' then 12
			when label= 'wetland_p' then 13
			when label = 'grassland_np' then 14
			when label = 'grassland_p' then 15
			END raster_values

			FROM
			(SELECT 
			  combined.new_value as value,
			  combined.label, 
			  combined.hex
			FROM 
			  intact_clu.combined
			WHERE new_value IS NOT NULL


			UNION

			SELECT 
			  intactland_15_refined_cdl15_broad_pad.new_value as value, 
			  intactland_15_refined_cdl15_broad_pad.label, 
			  intactland_15_refined_cdl15_broad_pad.hex
			FROM 
			  intact_clu.intactland_15_refined_cdl15_broad_pad) as current_values


			order by value
		"""

out_text = 'C:\\Users\\Bougie\\Desktop\\current.clr'


####call function#################################################
#### map_main########
# arglist = ['raster_values', 'hex']
# getColorMap(engine=engine, query=query, arglist=arglist, out_text=out_text)


out_text = 'C:\\Users\\Bougie\\Desktop\\current.clr'
# gen.addGDBTable2postgres_raster(gdb='D:\\intactland\\intact_compare\\intact_compare.gdb', pgdb='intactland', schema='intact_compare', intable='intact_compare', outtable='intact_compare')

arglist = ['value', 'hex_sdsu']
getColorMap(engine=engine, query='SELECT {},{} FROM intact_compare.intact_compare'.format(arglist[0], arglist[1]), arglist=arglist, out_text=out_text)







#######################################################################################
#################old code get rid of eventaully #####################################
##########################################################################################


# import matplotlib as mpl

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
# import replace_61_w_hard_crop


# #import extension
# arcpy.CheckOutExtension("Spatial")
# # arcpy.env.parallelProcessingFactor = "95%"
# arcpy.env.overwriteOutput = True
# arcpy.env.scratchWorkspace = "in_memory" 







# def conversionHEX2RGB():
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

# 	yo=palettable.colorbrewer.diverging.BrBG_8.colors
# 	print yo
# 	yo = yo[::-1]

# 	df = getPGtable()
# 	for index, row in df.iterrows():
# 		print index
# 		print row['class'], row['hex'], row['colormap']
# 		hi = yo[index]
# 		print 'hi', hi
# 		mi = map(str, hi)
# 		mimi = [str(row['class'])] + mi
# 		print 'yo map', mimi
# 		rgb_list = [str(row['class']), str(wc.hex_to_rgb(str(row['hex']))[0]), str(wc.hex_to_rgb(str(row['hex']))[1]), str(wc.hex_to_rgb(str(row['hex']))[2])]
# 		# rgb_list = [str(row['class'])]
# 		print rgb_list

# 		rgb_string = ' '.join(mimi)
# 	# 	print rgb_string

# 	# 	print df.at[index, 'colormap'] 
# 		df.at[index, 'colormap'] = rgb_string

# 	# print type(df)
# 	df = df['colormap']
# 	print 'fdfdf', df
# 	#  # df.to_sql(name='colormap_suitability', con=engine, schema='suitability', index=False, if_exists='replace')


# 	df.to_csv(path='C:\\Users\\Bougie\\Desktop\\misc\\colormaps\\suitability_t2.clr', index=False)











# def conversionHEX2RGB_2(cba):

# 	print cba

# 	cba = map(str, cba)
# 	print cba

	# for color in cba:
	# 	###convert elements in list from interger to string
	# 	color = map(str, color)
	# 	print ' '.join(color)






# def conversionHEX2RGB_3(pallette, flip):
	# engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

	
	# print pallette
	# yo = pallette[::-1]
	# print yo

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


