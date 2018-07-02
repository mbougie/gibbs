import nass
import pandas as pd
import arcpy
from arcpy import env
from arcpy.sa import *
from sqlalchemy import create_engine
from scipy import stats
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
import psycopg2
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen








########################  NEW  #######################################################



def addGDBTable2postgresyo(raster, cy):
	print 'raster:', raster
	print 'cy:', cy

	tablename=raster

	# Execute AddField twice for two new fields
	fields = [f.name for f in arcpy.ListFields(tablename)]

	# converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(tablename,fields)
	print arr

	# # convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)
	df.columns = map(str.lower, df.columns)
	print df

	df['year'] = cy
	df['acres'] = gen.getAcres(df['count'], 30)

	print df
	return df



	# # use pandas method to import table into psotgres
	# df.to_sql(data['pre']['traj']['filename'], engine, schema=schema)

	# #add trajectory field to table
	# addTrajArrayField(schema, data['pre']['traj']['filename'], fields)









def createDFfromQuery(cy):
	## component function of CreateBaseHybrid() function --grandchild
	## get all the tables with nass wildcard
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')


	query = "SELECT * FROM counts_imw.imw_{}".format(str(cy))

	print(query)
	df = pd.read_sql_query(query, engine)
	print 'df------',df
	return df



def pivotTableFromPostGres():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

	df = pd.read_sql_query('select * from counts_imw.merged_imw',con=engine)

	###add a string to mtr value to make column names nicer
	df['mtr'] = 'mtr' + df['mtr'].astype(str)

	##perform  a pivot table
	y=df.pivot(index='year', columns='mtr', values='acres')
	# df=pd.melt(df, id_vars=["year"],var_name="mtr", value_name="acres")

	print y
	# df.columns = map(str.lower, df.columns)
	y.to_sql('merged_imw_pvt', engine, schema='counts_imw', if_exists='replace')






def main():
	arcpy.env.workspace = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\sa\\r2\\s21\\core\\core_s21.gdb"
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	
	df_list = []
	rasters = arcpy.ListRasters("*", "GRID")
	for raster in rasters:
		print(raster)
		print raster.split('_')[5]
		cy_dict = {'2008to2010':2009, '2008to2011':2010, '2009to2012':2011, '2010to2013':2012, '2011to2014':2013, '2012to2015':2014, '2013to2016':2015, '2014to2017':2016}
		cy = cy_dict.get(raster.split('_')[5])
		print 'cy-----------------',cy
        # df=addGDBTable2postgresyo(raster, cy)


		tablename=raster

		# Execute AddField twice for two new fields
		fields = [f.name for f in arcpy.ListFields(tablename)]

		# converts a table to NumPy structured array.
		arr = arcpy.da.TableToNumPyArray(tablename,fields)
		print arr

		# # convert numpy array to pandas dataframe
		df = pd.DataFrame(data=arr)
		df.columns = map(str.lower, df.columns)
		print df

		df=df.rename(columns = {'value':'mtr'})

		df['year'] = cy
		print df

		df['acres'] = gen.getAcres(df['count'], 30)

		print df

		df.to_sql('imw_{}'.format(str(cy)), engine, schema='counts_imw', if_exists='replace')

		df_list.append(createDFfromQuery(cy))



	# ## MERGE all dataframes in list into one postgres table
	df_final=pd.concat(df_list)

	print 'df_final:', df_final

	df_final.to_sql('merged_imw', engine, schema='counts_imw', if_exists='replace')

	pivotTableFromPostGres()





#################  call functions  #####################################
#### get tables via the api
# applyAPI()


#### create the base table
# createBase(query_create_base)

#### create and modify the counts and stats tables
# executeQueries([query_create_base_counts, query_update_base_counts, query_create_base_stats])

#### file in the r2 and slope fields
# updatePGtableWithStats() 
						  

main()

