import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/ksu_v4')

df = pd.read_csv('I:\\d_drive\\projects\\synthesis\\s35\\intensification\\rda_unique_objectids.csv', sep=',').replace(to_replace='null', value=np.NaN)
df.to_sql('rda_unique_objectids', engine, schema='merged', if_exists='replace')