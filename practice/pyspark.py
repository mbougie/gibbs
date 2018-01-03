import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
from scipy import stats

##chnage the default parametes of matplotlib
matplotlib.rc('axes.formatter', useoffset=False)







# ts = pd.Series(np.random.randn(1000), index=pd.date_range('1/1/2000', periods=1000))

# ts.cumsum()
# print ts
# print ts.cumsum()

# df = pd.DataFrame(np.random.randn(1000, 4), index=ts.index, columns=['s13','s14','s15','s16'])

# df = df.cumsum()
# print df

# plt.figure();
# df.plot(); 
# plt.legend(loc='best')


# # plt.show()
# fips = 'tryit3'
# plt.savefig("C:\\Users\\Bougie\\Desktop\\Gibbs\\pdf\\{0}.pdf".format(fips), bbox_inches='tight')


def refInfoSchema():
	## component function of CreateBaseHybrid() function --grandchild
	## get all the tables with nass wildcard
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
	# query = "SELECT value as years,acres FROM counts.s13_ytc30_2008to2016_mmu5_nbl "


	table_dict = {'s1':'counts.s1_ytc56_2008to2012_mmu15_nbl',
				  's2':'counts.s2_ytc56_2008to2012_mmu15_nbl',
	              's6':'counts.s6_ytc30_2008to2016_mmu5_nbl',
				  's14':'counts.s14_ytc30_2008to2016_mmu5_nbl',
				  's15':'counts.s15_ytc30_2008to2012_mmu5_nbl'
				 }

	query = """SELECT value as years,a.acres as s1, b.acres as s2, c.acres as s6, d.acres as s14, e.acres as s15 
	           FROM counts.s1_ytc56_2008to2012_mmu15_nbl as a 
	           FULL JOIN counts.s2_ytc56_2008to2012_mmu15_nbl as b using(value)
	           FULL JOIN counts.s6_ytc30_2008to2016_mmu5_nbl as c using(value) 
	           FULL JOIN counts.s14_ytc30_2008to2016_mmu5_nbl as d using(value)
	           FULL JOIN counts.s15_ytc30_2008to2012_mmu5_nbl as e using(value)"""
	
	df = pd.read_sql_query(sql=query, con=engine, index_col='years')
	print df
	df.plot(use_index=True)
	# plt.show()
	filename = 'yo7'
	plt.savefig("C:\\Users\\Bougie\\Desktop\\Gibbs\\pdf\\{0}.pdf".format(filename), bbox_inches='tight')



refInfoSchema()

