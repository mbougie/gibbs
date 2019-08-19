from sqlalchemy import create_engine
import pandas as pd
import psycopg2 
import io
import StringIO

engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/ksu_v4')

df_header = pd.read_csv('I:\\d_drive\\projects\\synthesis\\s35\\intensification\\rda_unique_objectids.csv', nrows=0, index_col=0) 

# df_header.drop(df_header.columns[[0]], axis=1, inplace=True)
df_header.columns = map(str.lower, df_header.columns)

print df_header
# df.head(0).to_sql('table_name', engine, if_exists='replace',index=False) #truncates the table
df_header.to_sql('yo3', engine, schema='merged', index=False)#truncates the table

df = pd.read_csv('I:\\d_drive\\projects\\synthesis\\s35\\intensification\\rda_unique_objectids.csv', index_col=0) 
print 'df-----------------'
print df

# df.drop(df.columns[[0]], axis=1, inplace=True)
print df
# df.fillna(0)
df = df.fillna(0)
print df



conn = engine.raw_connection()

cur = conn.cursor()

# output = io.BytesIO
output = StringIO.StringIO()

df.to_csv(output, sep=',', header=False, index_col=0, null='')

# df.to_csv(output, sep='\t', header=False, index=False, null=0)

output.seek(0)

contents = output.getvalue()

cur.copy_from(output, 'merged.yo3') # null values become ''

conn.commit()



# def addGDBTable2postgres_io(gdb, schema, table):

#     print("addGDBTable2postgres().............")
#     # set the engine.....
#     engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/lem')

#     # # path to the table you want to import into postgres
#     input_table = '{}.gdb\\{}'.format(gdb, table)

#     # Execute AddField twice for two new fields
#     fields = [f.name for f in arcpy.ListFields(input_table)]

#     print fields

#     # converts a table to NumPy structured array.
#     arr = arcpy.da.TableToNumPyArray(input_table,fields)
#     print arr


#     # # convert numpy array to pandas dataframe
#     df = pd.DataFrame(data=arr)

#     df.columns = map(str.lower, df.columns)
#     print 'df-----------------------', df


#     df.head(0).to_sql(table, engine, schema=schema, index=False)#truncates the table


#     print df
#     # df.fillna(0)
#     df = df.fillna(0)
#     print df



#     conn = engine.raw_connection()

#     cur = conn.cursor()

#     # output = io.BytesIO
#     output = StringIO.StringIO()

#     # df.to_csv(output, sep=',', header=False, index_col=0, null='')

#     df.to_csv(output, sep='\t', header=False, index=False, null=0)

#     output.seek(0)

#     contents = output.getvalue()

#     cur.copy_from(output, '{0}.{1}'.format(schema, table)) # null values become ''

#     conn.commit()


