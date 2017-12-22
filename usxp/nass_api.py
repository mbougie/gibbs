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



try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"




def getZonalinfo():
	arcpy.env.workspace = 'C:/Users/Bougie/Desktop/Gibbs/data/usxp/ancillary/shapefiles.gdb/'

	# Use the ListFeatureClasses function to return a list of shapefiles.
	fc = 'states'
    
	zonelist = []
	cursor = arcpy.da.SearchCursor(fc, ['st_abbrev'])
	for row in cursor:
		zonelist.append(row[0])
	return zonelist





def applyAPI():

	### get each states tables from NASS using the NASS api and import each table into postgres database 
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	api_key = '19FA0F9C-F4C3-31F3-AB57-AFA3C5346527'
	api = nass.NassApi(api_key)
	q = api.query()


	for state in getZonalinfo():
		# print 'county---', county
		q.filter('source_desc', 'SURVEY').filter('sector_desc', 'CROPS').filter('group_desc', 'FIELD CROPS').filter('agg_level_desc', 'COUNTY').filter('state_alpha', state).filter('year__GE', 2008).filter('freq_desc', 'ANNUAL')
		print q.count()

		if q.count() > 0:

			df = pd.DataFrame(q.execute())
			print df
			del df['CV (%)']

			##export df to postgres table
			table_name = 'nass_'+state.lower()
			print table_name 
			df.to_sql(table_name, engine, schema='nass')
        else:
        	print 'no records for state:', state




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
	## create the lookup table by combining all the 
	querylist = []
	for table in refInfoSchema():
		print table
		print query.format(table)
		querylist.append(query.format(table))
	print querylist
	return querylist





def CreateBaseHybrid(query):
	## create the base table by merging all states datasets from 2008 - 2016 together into one table
	querylist = createNassLookupList(query)
	query = ' UNION '.join(querylist)
	print query





def updatePGtableWithStats():
	print 'getPGtables()'
	fipslist = loopThroughPGcolumn()
	for fips in fipslist:


		slope,r_value = getStatsByFIPS(fips[0])
		print slope
		print r_value

		updatePGtable(fips[0], r_value, slope)






def updatePGtable(fips, r_value, slope):
	### function to update the r2 and slope stats columns in table
	print 'updatePGtable(fips, r_value, slope)'
	cur = conn.cursor()

	query = "UPDATE nass.base_hybrid_stats set r2 = {1}, slope={2} WHERE fips = '{0}'".format(fips, r_value, slope)
	print query
	
	cur.execute(query);
	conn.commit()
	print "Records created successfully"



def loopThroughPGcolumn():
	##loop though the base_hybrid_counts table to only get the states that have records
	print 'loopThroughPGcolumn()-------------------------------------------------'
	cur = conn.cursor()

	query = "SELECT DISTINCT fips From nass.base_hybrid_counts"
	print query
	cur.execute(query);

	# # fetch all rows from table
	rows = cur.fetchall()
	return rows




def getStatsByFIPS(fips):
	#### get the stats for each stats(slope and r_value) for the years 2008 to 2016
	print 'fips:', fips
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	query = """SELECT
		  acres_merged, 
		  year::integer as years
		FROM 
		  nass.base_hybrid_counts
		WHERE fips = '{0}' and year::integer <= 2016
		ORDER BY years""".format(fips)
	df = pd.read_sql_query(query, engine)
	print df
	print df.size
	if df.size >= 8:
		acres= df['acres_merged'].values.tolist()
		print 'acres', acres
		acres_avg = np.mean(df.as_matrix(columns=['acres_merged']))
		print 'acres_avg', acres_avg
		years= df['years'].values.tolist()
		print 'years', years

		slope, intercept, r_value, p_value, std_err = stats.linregress(years,acres)
		print 'slope', slope
		print 'r_value', r_value
		f = plt.figure()
		plt.scatter(df['years'], df['acres_merged'])
		# plt.show()
		f.savefig("C:\\Users\\Bougie\\Desktop\\Gibbs\\pdf\\{0}.pdf".format(fips), bbox_inches='tight')


		return (slope,r_value)
	else:
		return (0,0)
		





######  call functions  ###############################################
# getPGtables()

query1 = 'SELECT short_desc, statisticcat_desc, unit_desc FROM nass.{0} GROUP BY short_desc, statisticcat_desc, unit_desc'
query2 = """SELECT nass_state.index, lookup.serial, nass_state."Value" as value,nass_state.state_alpha, nass_state.state_fips_code, nass_state.state_name, nass_state.county_name, nass_state.county_code, nass_state.short_desc, nass_state.statisticcat_desc, nass_state.unit_desc, nass_state.year FROM nass.{0} as nass_state, nass.lookup WHERE lookup.short_desc = nass_state.short_desc and lookup.hybrid = 'y'""" 
CreateBaseHybrid(query2)


# applyAPI()
# temp()
# loopThroughPGcolumn()


# getStatsByFIPS('19029')







######  SAVE SQL ###########################################
####################QUERY1################################
# create table nass.base_hybrid_counts as

# SELECT 
#   state_alpha, 
#   state_fips_code, 
#   state_name, 
#   county_name, 
#   county_code, 
#   year
# FROM 
#   nass.base_hybrid
# group by 
#   state_alpha, 
#   state_fips_code, 
#   state_name, 
#   county_name, 
#   county_code, 
#   year;

#   ALTER TABLE nass.base_hybrid_counts ADD COLUMN fips text;
#   ALTER TABLE nass.base_hybrid_counts ADD COLUMN acres_merged numeric;
# UPDATE nass.base_hybrid SET value = NULL WHERE value = '                 (D)';

####################QUERY2################################
###DESCRIPTION:  This table 
# UPDATE nass.base_hybrid_counts
# SET fips = base_hybrid_counts.state_fips_code || base_hybrid_counts.county_code,
#     acres_merged = sq.sum
# FROM (SELECT 
# SUM(translate(value, ',', '')::integer), 
# state_alpha, 
# state_fips_code, 
# state_name, 
# county_name, 
# county_code, 
# year
# FROM 
# nass.base_hybrid
# GROUP BY
# state_alpha, 
# state_fips_code, 
# state_name, 
# county_name, 
# county_code, 
# year 
# ORDER BY
# state_name, 
# county_name,
# year) AS sq
# WHERE base_hybrid_counts.state_fips_code=sq.state_fips_code and base_hybrid_counts.county_code=sq.county_code and base_hybrid_counts.year=sq.year


#####################QUERY3######################################
# create table nass.base_hybrid_stats as
# SELECT 
#   state_alpha, 
#   state_fips_code, 
#   state_name, 
#   county_name, 
#   county_code,  
#   fips, 
#   counties.shape_area*0.000247105 as shape_acres, 
#   counties.geom
# FROM 
#   nass.base_hybrid_counts,
#   spatial.counties
# WHERE 
#   fips = counties.atlas_stco
# group by 
#   state_alpha, 
#   state_fips_code, 
#   state_name, 
#   county_name, 
#   county_code,  
#   fips, 
#   counties.shape_area, 
#   counties.geom

#   ALTER TABLE nass.base_hybrid_stats ADD COLUMN r2 numeric;
#   ALTER TABLE nass.base_hybrid_stats ADD COLUMN slope numeric;