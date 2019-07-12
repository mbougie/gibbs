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



       
#####  import the csv into postgres   ##########################################

def importCSVtoPG():
    print pd.read_csv('D:\\projects\\nri\\NRI\\Raw_Data\\NRIdata\\nri12_cty_121115.csv', nrows=5)

    # df = pd.read_csv('D:\\projects\\nri\\NRI\\Raw_Data\\NRIdata\\nri12_cty_121115.csv')


    # # df.columns = [c.lower() for c in df.columns] #postgres doesn't like capitals or spaces
    # # df.columns = ['year_'+str(c) for c in df.columns]
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nri')

    # df.to_sql("try", engine, schema='control')
    csv_file = 'D:\\projects\\nri\\NRI\\Raw_Data\\NRIdata\\nri12_cty_121115.csv'
    chunksize = 100
    i = 0
    j = 1
    for df in pd.read_csv(csv_file, chunksize=chunksize, iterator=True):
          # df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
          df.index += j
          i+=1
          print i
          df.to_sql("nri12_cty_121115", engine, schema='control', if_exists='append')
          j = df.index[-1] + 1



def createNRItable():
	cur = conn.cursor()

	query = '''CREATE TABLE main.nri as 
			SELECT 
			  counties.objectid,
			  counties.atlas_st, 
			  counties.st_abbrev, 
			  counties.state_name, 
			  counties.atlas_stco, 
			  counties.atlas_name, 
			  counties.acres_calc
			  --counties.geom
			FROM 
			  spatial.counties;'''

	cur.execute(query);

	conn.commit()




# def milkweed_abandonment(list_years, psu):
# 	## create the base table by merging all states datasets from 2008 - 2016 together into one table
# 	cur = conn.cursor()

# 	i = 0
# 	while i < (len(list_years)-1):

# 		query = ''' ALTER TABLE main.milkweed ADD COLUMN abandon_{1} integer;
# 		UPDATE main.milkweed
# 		SET abandon_{1}=subquery.sum*({2})
# 		FROM (SELECT 
# 		  nri12_cty_121115_core.fips, 
# 		  sum(nri12_cty_121115_core.xfact) as sum
# 		FROM 
# 		  main.nri12_cty_121115_core
# 		WHERE broad{0} IN (1,2) AND broad{1} NOT IN (1,2,12)
# 		GROUP BY fips) AS subquery
# 		WHERE milkweed.atlas_stco=subquery.fips;'''.format(list_years[i], list_years[i+1], psu)

# 		print query

# 		# cur.execute(query);

# 		# conn.commit()

# 		i += 1



def grossExpansion(list_years, psu):
	## create the base table by merging all states datasets from 2008 - 2016 together into one table
	cur = conn.cursor()

	i = 0
	while i < (len(list_years)-1):
					###add column expansion_[year] to table
		query = ''' ALTER TABLE main.nri ADD COLUMN expansion_{1} integer;
					
					---add values to expansion_[year] column
					UPDATE main.nri
					---for each year of expansion (expansion_year)
					SET expansion_{1}=subquery.sum*({2})
					
					FROM (SELECT 
					  nri12_cty_121115_core.fips, 
					  sum(nri12_cty_121115_core.xfact) as sum
					FROM 
					  main.nri12_cty_121115_core
					WHERE broad{0} NOT IN (1,2) AND broad{1} IN (1,2)
					GROUP BY fips) AS subquery
					WHERE nri.atlas_stco=subquery.fips;'''.format(list_years[i], list_years[i+1], psu)

		print query

		cur.execute(query);

		conn.commit()

		i += 1





def sumColumns(wc):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nri')
	query = 'SELECT * FROM main.nri'
	df = pd.read_sql_query('SELECT * FROM main.nri',con=engine)
	print df

	col_list = [col for col in df.columns if wc in col]
	print(list(df.columns))
	print(col_list)


	df[wc] = df[col_list].sum(axis=1)
	print df
	df.to_sql('nri', engine, schema='main', if_exists='replace', index=False)





def percentExpansion():
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/nri')

	df = pd.read_sql_query('SELECT * FROM main.nri WHERE expansion IS NOT NULL', con=engine)
	print df

	df['perc_expansion']= (df['expansion']/df['acres_calc'])*100

	print df['perc_expansion']

	df.to_sql('nri', engine, schema='main', if_exists='replace', index=False)




def convertPGtoFC(db, schema, table):
    # command = ["ogr2ogr.exe", "PostgreSQL", "PG:host=144.92.235.105 user=mbougie password=Mend0ta! dbname=lem", "D:\\projects\\lem\\lem.gdb", "-nlt", "PROMOTE_TO_MULTI", "-nln", "blocks.us_blck_grp_2016_mainland_5070", "us_blck_grp_2016_mainland_5070", "-progress", "--config", "PG_USE_COPY", "YES"]
    command = 'ogr2ogr -f "FileGDB" -progress -update D:\\projects\\usxp\\deliverables\\maps\\choropleths\\choropleths.gdb PG:"dbname={0} user=mbougie host=144.92.235.105 password=Mend0ta!" -sql "SELECT * FROM {1}.{2}" -nln {2} -nlt MULTIPOLYGON'.format(db, schema, table)
    print command
    os.system(command)









def main():
	# #### years of analysis
	list_years = ['07', '08', '09', '10', '11', '12']

	#### an area of land for which the sample point is assigned. Typically 40,100,160 or 640 acres in size
	psu=100

	#### call functions
	# importCSVtoPG()
	
	#### create and kernel of nri table from county dataset (3070 records)
	# createNRItable()

	# grossExpansion(list_years, psu)
	# sumColumns('expansion')
	percentExpansion()









#################  call functions  #####################################
main()








