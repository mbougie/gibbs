
from sqlalchemy import create_engine
import numpy as np, sys, os
import fnmatch
import pandas as pd
import collections
from collections import namedtuple
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen
import shutil
import matplotlib.pyplot as plt



try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"



engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')



def getTotalCrop():

	### get each states tables from NASS using the NASS api and import each table into postgres database 
	# engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')


	df_mtr5_list = []
	df_net_list = []

	for year in range(2009,2017):

		df_mtr5_list.append(createMTR5df(year))
		df_net_list.append(createNetdf(year))

		# for year in range(2009,2017):

		# 	df_net_list.append(createNetdf(year))


		# print(df_mtr5_list)

		#    ## merge all dataframes in list into one postgres table
		# df_mtr5=pd.concat(df_mtr5_list)

		# print 'df_final:', df_mtr5

		# df_final.to_sql('total_acres', engine, schema='counts_gen', if_exists='replace')
    
	
	df_mtr5=pd.concat(df_mtr5_list)
	print df_mtr5
	df_net=pd.concat(df_net_list)
	print df_net

	df_final = pd.concat([df_net, df_mtr5], axis=1, join='inner')
	print df_final

	df_final['cumm'] = df_final.net.cumsum()+288128028
	df_final = df_final[['year_yo','cumm', 'mtr5']]
	print df_final
	df_final.to_sql('s22_total_traj', engine, schema='counts_total', if_exists='replace')




# def testit(year, conv_type, binary):
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

# 	query = ('SELECT sum("Count") as count, count("Count") as rows FROM pre.v4_traj_cdl30_b_2008to2017 a INNER JOIN pre.v4_traj_lookup_2008to2017_v3 b USING(traj_array) WHERE cdl30_b_{} = {} AND b.mtr = 5'.format(year, binary))

# 	print(query)
# 	df = pd.read_sql_query(query, engine)
# 	print 'df------',df

# 	#### remove column to table ########################
# 	# del df['index']

# 	#### add columns to table ########################
# 	df['year'] = year
# 	df['conv_type'] = conv_type
# 	df['acres'] = gen.getAcres(df['Count'], 30)

# 	print 'df------',df
# 	return df



def createMTR5df(year):
	# engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

	####  this is the QAQC query!!! -------query = ('SELECT * FROM pre.v4_traj_cdl30_b_2008to2017 a INNER JOIN pre.v4_traj_lookup_2008to2017_v3 b USING(traj_array) WHERE b.mtr={mtr_stable} or (b.mtr={mtr_conv} and ytc={year} and cdl30_b_{year} = {binary}) or (b.mtr=5 and cdl30_b_{year} = {binary})'.format(mtr_stable=mtr_stable, mtr_conv=mtr_conv, year=year, binary=binary))
	# query = ('SELECT sum("Count") as count, count("Count") as rows FROM pre.v4_traj_cdl30_b_2008to2017 a INNER JOIN pre.v4_traj_lookup_2008to2017_v3 b USING(traj_array) WHERE b.mtr={mtr_stable} or (b.mtr={mtr_conv} and ytc={year} and cdl30_b_{year} = {binary}) or (b.mtr=5 and cdl30_b_{year} = {binary})'.format(mtr_stable=mtr_stable, mtr_conv=mtr_conv, year=year, binary=binary))
	query = ('SELECT sum("Count") as count, round((sum("Count")*0.222395)::numeric,0) as mtr5 FROM pre.v4_traj_cdl30_b_2008to2017 a INNER JOIN pre.v4_traj_lookup_2008to2017_v3 b USING(traj_array) WHERE b.mtr=5 and cdl30_b_{year} = 1'.format(year=year))
	print(query)
	df_mtr5 = pd.read_sql_query(query, engine)
	print 'df------',df_mtr5

	#### remove column to table ########################
	# del df['index']

	#### add columns to table ########################
	df_mtr5['year_yo'] = year


	print 'df------',df_mtr5
	return df_mtr5




def createNetdf(year):

	print 'year-----------------------', year
	####  this is the QAQC query!!! -------query = ('SELECT * FROM pre.v4_traj_cdl30_b_2008to2017 a INNER JOIN pre.v4_traj_lookup_2008to2017_v3 b USING(traj_array) WHERE b.mtr={mtr_stable} or (b.mtr={mtr_conv} and ytc={year} and cdl30_b_{year} = {binary}) or (b.mtr=5 and cdl30_b_{year} = {binary})'.format(mtr_stable=mtr_stable, mtr_conv=mtr_conv, year=year, binary=binary))
	# query = ('SELECT sum("Count") as count, count("Count") as rows FROM pre.v4_traj_cdl30_b_2008to2017 a INNER JOIN pre.v4_traj_lookup_2008to2017_v3 b USING(traj_array) WHERE b.mtr={mtr_stable} or (b.mtr={mtr_conv} and ytc={year} and cdl30_b_{year} = {binary}) or (b.mtr=5 and cdl30_b_{year} = {binary})'.format(mtr_stable=mtr_stable, mtr_conv=mtr_conv, year=year, binary=binary))
	query = ('SELECT round((sum("Count")*0.222395)::numeric,0) as mtr3 FROM pre.v4_traj_cdl30_b_2008to2017 as a, pre.v4_traj_cdl30_b_2008to2017_rfnd_v5 as b , pre.v4_traj_lookup_2008to2017_v3 as c WHERE a.traj_array = c.traj_array AND b.value = a."Value" AND ytc = {}'.format(year))
	print(query)
	df_mtr3 = pd.read_sql_query(query, engine)

	df_mtr3['year'] = year

	print 'df------',df_mtr3


	query = ('SELECT round((sum("Count")*0.222395)::numeric,0) as mtr4 FROM pre.v4_traj_cdl30_b_2008to2017 as a, pre.v4_traj_cdl30_b_2008to2017_rfnd_v5 as b , pre.v4_traj_lookup_2008to2017_v3 as c WHERE a.traj_array = c.traj_array AND b.value = a."Value" AND yfc = {}'.format(year))
	print(query)
	df_mtr4 = pd.read_sql_query(query, engine)

	df_mtr4['year'] = year

	print 'df------',df_mtr4


	df = pd.concat([df_mtr3, df_mtr4], axis=1, join='inner')
	print df
	# df['net'] = df[['mtr3', 'mtr4']].sum(axis=1)
	df["net"] = df['mtr3'].subtract(df['mtr4'], fill_value=0)
	print df
	df = df[['year','net']]
	print df

	return df



	#### remove column to table ########################
	# del df['index']

	# #### add columns to table ########################
	# df_mtr5['year'] = year


	# print 'df------',df_mtr5
	# return df_mtr5







if __name__ == '__main__':



    #################  call functions  #####################################
	#### get tables via the api
	getTotalCrop()

	#### create the base table
	# createBase(query_create_base)

	#### create and modify the counts and stats tables
	# executeQueries([query_create_base_counts, query_update_base_counts, query_create_base_stats])

	#### file in the r2 and slope fields
	# updatePGtableWithStats() 
							  






























########################################################################################################################
#########  functions related to updating nass.base_stats  ##############################################################

def getyears():
	arcpy.env.workspace = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/ancillary/vector/shapefiles.gdb/'

	# Use the ListFeatureClasses function to return a list of shapefiles.
	fc = 'states'
    
	zonelist = []
	cursor = arcpy.da.SearchCursor(fc, ['st_abbrev'])
	for row in cursor:
		zonelist.append(row[0])
	return zonelist



def createNassLookupList(query):

	## component function of CreateBaseHybrid() function ---child
	## create the lookup table by combining all the state tables together and using groupby function??? 
	querylist = []
	for table in refInfoSchema():
		print table
		print query.format(table)
		querylist.append(query.format(table))
	print querylist
	return querylist





def CreateBaseTable(query):
    ## create the base table by merging all states datasets from 2008 - 2016 together into one table
	cur = conn.cursor()

	querylist = createNassLookupList(query)
	queryyo = ' UNION '.join(querylist)
	
	finalquery = 'CREATE TABLE nass.base as ' + queryyo
	print finalquery

	cur.execute(finalquery);

	conn.commit()

	conn.close()








def executeQueries(querylist):
	for query in querylist:

		print query
		cur = conn.cursor()

		cur.execute(query);

		conn.commit()


















def updatePGtableWithStats():
	print 'getPGtables()'
	fipslist = getDistinctValues()
	for fips in fipslist:


		slope,r_value = getStatsByFIPS(fips[0])
		print slope
		print r_value

		# updatePGtable(fips[0], r_value, slope)




def updatePGtable(fips, r_value, slope):
	### function to update the r2 and slope stats columns in table
	print 'updatePGtable(fips, r_value, slope)'
	cur = conn.cursor()

	query = "UPDATE nass.base_stats set r_value={1}, slope={2} WHERE fips = '{0}'".format(fips, r_value, slope)
	print query
	
	cur.execute(query);
	conn.commit()
	print "Records created successfully"




def getStatsByFIPS(fips):
	#### get the stats for each stats(slope and r_value) for the years 2008 to 2016
	print 'fips:', fips
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	query = """ SELECT
					acres_merged, 
					year::integer as years
				FROM 
					nass.base_counts
				WHERE fips = '{0}' and year::integer <= 2016
				ORDER BY years""".format(fips)
	df = pd.read_sql_query(query, engine)
	print df
	print df.size
	if df.size >= 8 and fips != '48291':
		acres= df['acres_merged'].values.tolist()
		print 'acres', acres
		# acres_avg = np.mean(df.as_matrix(columns=['acres_merged']))
		# print 'acres_avg', acres_avg
		years= df['years'].values.tolist()
		print 'years', years

		slope, intercept, r_value, p_value, std_err = stats.linregress(years,acres)
		print 'slope', slope
		print 'r_value', r_value

		##create figure object and then plot it out
		fig = plt.figure()
		plt.scatter(df['years'], df['acres_merged'])

		# save figure to pdf
		fig.savefig("C:\\Users\\Bougie\\Desktop\\Gibbs\\pdf\\{0}.pdf".format(fips), bbox_inches='tight')

		##clear the figure once it is saved to pdf
		fig.clf()

		## check if r_value is nan
		if math.isnan(r_value):
			return ('NULL','NULL')
		else:
			return (slope,r_value)	

	else:
		return ('NULL','NULL')
		



def getDistinctValues():
	##loop though the base_counts table to only get the states that have records
	print 'loopThroughPGcolumn()-------------------------------------------------'
	cur = conn.cursor()

	query = "SELECT DISTINCT fips From nass.base_counts"
	print query
	cur.execute(query);

	# # fetch all rows from table
	rows = cur.fetchall()
	return rows
#######################################  end of grouping ##################################################







