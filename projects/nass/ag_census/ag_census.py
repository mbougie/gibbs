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



try:
    conn = psycopg2.connect("dbname='nass' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"





def main(table):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nass')
	df = pd.read_csv('C:\\Users\\Bougie\\Box\\Gibbs_MattBougie\\projects\\nass\\agcensus\\csv\\{}.csv'.format(table))
	print df

	del df['CV (%)']

	df.columns = map(str.lower, df.columns)
	print df
	
	###format values column by removing commas from value field
	df['value_new'] = df['value'].str.replace(',', '').replace(' (D)', 0).astype(int)
	print df['value_new']	
	
	###create the fips by modify the two ansci columns for state and county and then concatenate these modified columns
	df["fips"] = df['state ansi'].astype(int).apply(str).str.pad(width=2, side='left', fillchar='0') + df['county ansi'].fillna(0).astype(int).apply(str).str.pad(width=3, side='left', fillchar='0')
	print df["fips"]

	pg_schema = 'ag_census'
	df.to_sql(table, engine, schema=pg_schema)

























	# ### REFINEMENTS ON TABLES  ##############################
	# DELETE FROM ag_census.harvested_2012 where "Value" = ' (D)'


#     ## QUERY  ###################################
# create table ag_census.diff_2012_2007 as

# SELECT
#   LPAD(b."State ANSI"::text, 2, '0') ||  LPAD(b."County ANSI"::text, 3, '0') as b_fips,
#   a."Year" as year_2007,
#   a."Value" as value_2007,
#   b."Year" as year_2012,
#   b."Value" as value_2012,
#   c.acres_calc,
#   (((b."Value"::bigint) - (a."Value"::bigint))/c.acres_calc) * 100 as net_perc,
#   c.geom
  
# FROM 
#   ag_census.harvested_2007 as a INNER JOIN ag_census.harvested_2012 as b ON (LPAD(a."State ANSI"::text, 2, '0') ||  LPAD(a."County ANSI"::text, 3, '0')) = (LPAD(b."State ANSI"::text, 2, '0') ||  LPAD(b."County ANSI"::text, 3, '0')) INNER JOIN spatial.counties as c on (LPAD(b."State ANSI"::text, 2, '0') ||  LPAD(b."County ANSI"::text, 3, '0')) = c.atlas_stco

# order by atlas_stco


##### call functions ##############################################################
# main(table='agcensus_2007_3metrics')
# main(table='agcensus_2017_3metrics')



gen.convertPGtoFC(gdb='D:\\projects\\usxp\\series\\s35\\maps\\choropleths\\choropleths.gdb', pgdb='nass', schema='ag_census', table='ag_census_expansion')
