from sqlalchemy import create_engine
import pandas as pd
import psycopg2 
import io
import StringIO

engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp_deliverables')

df_header = pd.read_csv('D:\\projects\\usxp\\deliverables\\maps\\synthesis\\intensification\\seth\\rfs_intensification_n2o.csv', nrows=0, index_col=0) 

# df_header.drop(df_header.columns[[0]], axis=1, inplace=True)
df_header.columns = map(str.lower, df_header.columns)

print df_header
# df.head(0).to_sql('table_name', engine, if_exists='replace',index=False) #truncates the table
df_header.to_sql('rfs_intensification_v3_agroibis_n2o', engine, schema='synthesis_intensification', index=False)#truncates the table

df = pd.read_csv('D:\\projects\\usxp\\deliverables\\maps\\synthesis\\intensification\\seth\\rfs_intensification_n2o.csv', index_col=0) 
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

# df.to_csv(output, sep=',', header=False, index_col=0, null='')

df.to_csv(output, sep='\t', header=False, index=False, null=0)

output.seek(0)

contents = output.getvalue()

cur.copy_from(output, 'synthesis_intensification.rfs_intensification_v3_agroibis_n2o') # null values become ''

conn.commit()


