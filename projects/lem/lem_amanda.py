from sqlalchemy import create_engine
import pandas as pd

import arcpy
from arcpy import env
from arcpy.sa import *

sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen

# Check out the ArcGIS Spatial Analyst extension license
arcpy.CheckOutExtension("Spatial")



def addGDBTable2postgres_histo_counties(gdb, pgdb, schema, table):
    arcpy.env.workspace = gdb
    print("addGDBTable2postgres().............")

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(table)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(table,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ### remove column
    del df['OBJECTID']
    # print df

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_stco", value_name="count")

    df.columns = map(str.lower, df.columns)

    print df
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df['atlas_stco'] = df['atlas_stco'].map(lambda x: x.strip('GEOID_'))
    ## remove comma from year
    # df['value'] = df['label'].str.replace(',', '')

    print df

    tablename = table.split('\\')[-1]
    print 'tablename', tablename

    print df

    df.to_sql(tablename, engine, schema=schema)







def importCSV():
	###DESCRIPTION: function to import census csv into postgres
	
	#####load csv into dataframe
	indata='I:\\d_drive\\projects\\lem\\projects\\amanda\\cc-est2018-alldata.csv'

	df = pd.read_csv(indata)
	# print df

	#### subset table by columns
	# df = df[['STATE','STNAME','CTYNAME','YEAR','AGEGRP']]
	df = df[['STATE','COUNTY','YEAR','AGEGRP','TOT_POP']]
	df.columns = map(str.lower, df.columns)
	print 'df-----------------------', df

	#### subset table by rows
	df=df.loc[(df['year'] == 11) & (df['agegrp'] == 0)]
	print df
	print df.dtypes

	


	# ###export df to postgres
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')
	df.to_sql(con=engine, name='census_2018', schema='amanda', index=False)


def createZonalHist():

	###DESCRIPTION: create majority zonal stats with raw NWALT raster #####################################################
	
	####create parameters for zonal histogram function
	in_zone_data='I:\\d_drive\\projects\\lem\\data\\gdbases\\census_features_v3_2.gdb\\county'
	zone_field="geoid"
	in_value_raster='I:\\d_drive\\projects\\lem\\data\\gdbases\\rasters.gdb\\nwalt_rc_60m'
	out_table='I:\\d_drive\\projects\\lem\\data\\gdbases\\projects\\amanda.gdb\\county_zonal_hist_nwalt_rc_60m'

	###call zonal-hist fct  <----NOTE: I had to do this in the GUI because it was returning a table without the LABEL column
	ZonalHistogram(in_zone_data=in_zone_data, zone_field=zone_field, in_value_raster=in_value_raster, out_table=out_table)
	
	###add zonal-hist table to postgres
	addGDBTable2postgres_histo_counties(gdb='I:\\d_drive\\projects\\lem\\data\\gdbases\\projects\\amanda.gdb', pgdb='lem', schema='amanda', table='county_zonal_hist_nwalt_rc_60m')










####### run functions  ##################
if __name__ == '__main__':
	###load census csv into psotgres
	# importCSV()

	###create zonal histogram by county using biomes and luc rasters
	createZonalHist()

