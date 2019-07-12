import os
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
import geopandas as gpd



try:
    conn = psycopg2.connect("dbname='nri' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




# # def getZonalinfo():
# # 	arcpy.env.workspace = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/ancillary/vector/shapefiles.gdb/'

# # 	# Use the ListFeatureClasses function to return a list of shapefiles.
# # 	fc = 'states'
    
# # 	zonelist = []
# # 	cursor = arcpy.da.SearchCursor(fc, ['st_abbrev'])
# # 	for row in cursor:
# # 		zonelist.append(row[0])
# # 	return zonelist



# def applyAPI():

# 	### get each states tables from NASS using the NASS api and import each table into postgres database 
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nass')
# 	api_key = '19FA0F9C-F4C3-31F3-AB57-AFA3C5346527'
# 	api = nass.NassApi(api_key)
# 	q = api.query()

# 	df_list = []
# 	for state in getZonalinfo():
# 		# print 'county---', county
# 		q.filter('source_desc', 'SURVEY').filter('sector_desc', 'CROPS').filter('group_desc', 'FIELD CROPS').filter('agg_level_desc', 'COUNTY').filter('state_alpha', state).filter('year__GE', 2008).filter('freq_desc', 'ANNUAL')
# 		print q.count()

# 		if q.count() > 0:

# 			df = pd.DataFrame(q.execute())
# 			print df
# 			del df['CV (%)']

# 			##export df to postgres table
# 			table_name = 'nass_'+state.lower()
# 			# print table_name

# 			#### create individual state table in postgres
# 			df.to_sql(table_name, engine, schema='states') 

# 			#### create a dataframe from the individaul state table and append it to df_list array
# 			df_list.append(createDFfromQuery(state))

#         else:
#         	print 'no records for state:', state


# 	print(df_list)

#     ## MERGE all dataframes in list into one postgres table
# 	df_final=pd.concat(df_list)

# 	print 'df_final:', df_final

# 	df_final.to_sql('merged_acres', engine, schema='counts', if_exists='replace')




# def createDFfromQuery(state):
# 	## component function of CreateBaseHybrid() function --grandchild
# 	## get all the tables with nass wildcard
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nass')


# 	query = ("""SELECT 
# 				  short_desc,
# 				  state_name,
# 				  state_alpha,
# 				  state_ansi,
# 				  year,
# 				  sum(cast(coalesce(nullif(regexp_replace("Value", '[^0-9]+', '', 'g'),''),'0') as numeric))as acres

# 				FROM 
# 				  states.nass_{}
# 				group by
# 				  short_desc,
# 				  state_name,
# 				  state_alpha,
# 				  state_ansi,
# 				  year

# 				order by year""".format(state))


# 	print(query)
# 	df = pd.read_sql_query(query, engine)
# 	print 'df------',df
# 	return df





























































# def milkweed_abandonment_year():
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nri')
# 	df = pd.read_sql_query('SELECT sum(abandon_08) as cy_2008, sum(abandon_09) as cy_2009, sum(abandon_10) as cy_2010, sum(abandon_11) as cy_2011, sum(abandon_12) as cy_2012 FROM main.milkweed',con=engine)
# 	print df


# 	##perform a psuedo pivot table
# 	df=pd.melt(df,var_name="year", value_name="acres")
# 	df['year'] = df['year'].str.replace('cy_', '')
# 	print df


# 	df.to_sql('milkweed_abandonment_year', engine, schema='main')




# def milkweed_abandonment_year_state():
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nri')
# 	df = pd.read_sql_query('SELECT st_abbrev, sum(abandon_08) as cy_2008, sum(abandon_09) as cy_2009, sum(abandon_10) as cy_2010, sum(abandon_11) as cy_2011, sum(abandon_12) as cy_2012 FROM main.milkweed GROUP BY st_abbrev',con=engine)
# 	print df


# 	##perform a psuedo pivot table
# 	df=pd.melt(df, id_vars=['st_abbrev'], var_name="year", value_name="acres")
# 	df['year'] = df['year'].str.replace('cy_', '')
# 	print df


# 	df.to_sql('milkweed_abandonment_year_state', engine, schema='main')



# def milkweed_crp_year():
# 	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nri')
# 	df = pd.read_sql_query('SELECT sum(crp_expansion_08) as cy_2008, sum(crp_expansion_09) as cy_2009, sum(crp_expansion_10) as cy_2010, sum(crp_expansion_11) as cy_2011, sum(crp_expansion_12) as cy_2012 FROM main.milkweed',con=engine)
# 	print df


# 	##perform a psuedo pivot table
# 	df=pd.melt(df,var_name="year", value_name="acres")
# 	df['year'] = df['year'].str.replace('cy_', '')
# 	print df


# 	df.to_sql('milkweed_crp_year', engine, schema='main')






























# def net_crop():
# 	## create the base table by merging all states datasets from 2008 - 2016 together into one table
# 	cur = conn.cursor()

# 	for column in list_years:

# 		query = ''' ALTER TABLE main.base ADD COLUMN crop_{0} integer;
# 		UPDATE main.base
# 		SET mtr1_{0}=subquery.sum
# 		FROM (SELECT fips, sum(broad{0}) as sum
# 		FROM  main.nri12_cty_121115_core
# 		WHERE nri12_cty_121115_core.broad{0} <> 1 AND nri12_cty_121115_core.broad{0} <> 2 AND nri12_cty_121115_core.broad{0} <> 12 
# 		GROUP BY fips) AS subquery
# 		WHERE base.atlas_stco=subquery.fips;'''.format(column)

# 		print query

# 		cur.execute(query);

# 		conn.commit()

# 	conn.close()

# def net_noncrop():
# 	## create the base table by merging all states datasets from 2008 - 2016 together into one table
# 	cur = conn.cursor()

# 	for column in list_years:

# 		query = ''' ALTER TABLE main.base ADD COLUMN nonvcrop_{0} integer;
# 		UPDATE main.base
# 		SET mtr2_{0}=subquery.sum
# 		FROM (SELECT fips, sum(broad{0}) as sum
# 		FROM  main.nri12_cty_121115_core
# 		WHERE nri12_cty_121115_core.broad{0} = 1 or nri12_cty_121115_core.broad{0} = 2
# 		GROUP BY fips) AS subquery
# 		WHERE base.atlas_stco=subquery.fips;'''.format(column)


# 		print query

# 		cur.execute(query);

# 		conn.commit()

# 	conn.close()











#### create the base table
# createBase(query_create_base)

#### create and modify the counts and stats tables
# executeQueries([query_create_base_counts, query_update_base_counts, query_create_base_stats])

#### file in the r2 and slope fields
# updatePGtableWithStats() 
						  








########################  OLD STUFF (probably delete soon after push to github)  ##################################################





def refInfoSchema():
	## component function of CreateBaseHybrid() function --grandchild
	## get all the tables with nass wildcard
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'nass' and table_name like '%%nass%%'"
	df = pd.read_sql_query(query, engine)
	tables = df['table_name'].values.tolist()
	print 'tables------',type(tables)
	return tables








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











#########  functions related to updating nass.base_stats  ##############################################################

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












# if __name__ == '__main__':

# 	##########  create queries ##################################################################################################################

# 	query_create_base = """SELECT nass_state.index, lookup.serial, nass_state."Value" as value,nass_state.state_alpha, nass_state.state_fips_code, nass_state.state_name, 
# 				nass_state.county_name, nass_state.county_code, nass_state.short_desc, nass_state.statisticcat_desc, nass_state.unit_desc, nass_state.year 
# 				FROM nass.{0} as nass_state, nass.lookup WHERE lookup.short_desc = nass_state.short_desc and lookup.hybrid = 'y'""" 


#     ## get the a
# 	query_create_base_counts = """  CREATE TABLE nass.base_counts as
# 									SELECT 
# 										state_alpha, 
# 										state_fips_code, 
# 										state_name, 
# 										county_name, 
# 										county_code, 
# 										year
# 									FROM 
# 										nass.base
# 									group by 
# 										state_alpha, 
# 										state_fips_code, 
# 										state_name, 
# 										county_name, 
# 										county_code, 
# 										year;

# 									ALTER TABLE nass.base_counts ADD COLUMN fips text;
# 									ALTER TABLE nass.base_counts ADD COLUMN acres_merged numeric;
# 									UPDATE nass.base SET value = NULL WHERE value = '                 (D)';"""




# 	query_update_base_counts = """  UPDATE nass.base_counts
# 									SET fips = base_counts.state_fips_code || base_counts.county_code, acres_merged = sq.sum
# 									FROM (SELECT 
# 										SUM(translate(value, ',', '')::integer), 
# 										state_alpha, 
# 										state_fips_code, 
# 										state_name, 
# 										county_name, 
# 										county_code, 
# 										year
# 									FROM 
# 										nass.base
# 									GROUP BY
# 										state_alpha, 
# 										state_fips_code, 
# 										state_name, 
# 										county_name, 
# 										county_code, 
# 										year 
# 									ORDER BY
# 										state_name, 
# 										county_name,
# 										year) AS sq
# 									WHERE base_counts.state_fips_code=sq.state_fips_code and base_counts.county_code=sq.county_code and base_counts.year=sq.year; """



# 	query_create_base_stats = """   CREATE TABLE nass.base_stats as
# 									SELECT 
# 										state_alpha, 
# 										state_fips_code, 
# 										state_name, 
# 										county_name, 
# 										county_code,  
# 										fips, 
# 										counties.shape_area*0.000247105 as shape_acres, 
# 										counties.geom
# 									FROM 
# 										nass.base_counts,
# 										spatial.counties
# 									WHERE 
# 										fips = counties.atlas_stco
# 									group by 
# 										state_alpha, 
# 										state_fips_code, 
# 										state_name, 
# 										county_name, 
# 										county_code,  
# 										fips, 
# 										counties.shape_area, 
# 										counties.geom;

# 									ALTER TABLE nass.base_stats ADD COLUMN r_value numeric;
# 									ALTER TABLE nass.base_stats ADD COLUMN slope numeric; """






