from sqlalchemy import create_engine
import pandas as pd
import psycopg2 
import io
import StringIO





def pandasCSV_io(pgdb, file, schema, table):
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

	df = pd.read_csv(file) 

	df.columns = map(str.lower, df.columns)
	print 'df-----------------------', df


	df.head(0).to_sql(table, engine, schema=schema, index=False)#truncates the table

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

	cur.copy_from(output, '{0}.{1}'.format(schema, table)) # null values become ''

	conn.commit()

	###close things
	conn.close()
	cur.close()