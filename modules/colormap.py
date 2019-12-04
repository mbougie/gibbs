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




####call function#################################################
engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')
out_text = 'C:\\Users\\Bougie\\Desktop\\current.clr'
arglist = ['initial', 'hex']

getColorMap(engine=engine, query='SELECT {},{} FROM public.nwalt_lookup'.format(arglist[0], arglist[1]), arglist=arglist, out_text=out_text)




