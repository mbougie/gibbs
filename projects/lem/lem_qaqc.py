import json
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
import arcpy
from arcpy import env
from arcpy.sa import *
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen

# indata='I:\\d_drive\\projects\\lem\\data\\deliverables\\version\\v3\\v3_2\\county.json'


# with open(indata) as data_file:    
#     d= json.load(data_file)  

# df = json_normalize(d)
# print (df["features"][0]["properties"])





# gen.addGDBTable2postgres_state(gdb='I:\\d_drive\\projects\\lem\\data\\gdbases\\v3_2_qaqc.gdb', pgdb='lem', schema='v3_2_qaqc', table='zonal_hist_nwalt_60m_state')



def addGDBTable2postgres_histo_states(gdb, pgdb, schema, table):
    arcpy.env.workspace = gdb
    print("addGDBTable2postgres().............")

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(table)]
    print fields

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(table,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)
    ## remove column
    del df['OBJECTID']
    print df

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["LABEL"],var_name="objectid", value_name="count")


    df.columns = map(str.lower, df.columns)

    print df
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df['objectid'] = df['objectid'].map(lambda x: x.strip('OBJEC_'))
    ## remove comma from year
    # df['value'] = df['label'].str.replace(',', '')

    print df

Con(inRas1 < 45,1, Con((inRas1 >= 45) & (inRas1 < 47),2, Con((inRas1 >= 47) & (inRas1 < 49),3, Con(inRas1 >= 49

    df.to_sql(table, engine, schema=schema)




addGDBTable2postgres_histo_states(gdb='I:\\d_drive\\projects\\lem\\projects\\qaqc\\qaqc.gdb', pgdb='lem', schema='qaqc', table='block_group_springfield_zonal_hist_nwalt_unique')