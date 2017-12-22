"""
Run multiple series.
"""

import sys
import os
from config import from_config
from sqlalchemy import create_engine
import pandas as pd

import pre
import parallel_61and36mask as pp_36and61mask
import core
import parallel_regiongroup as prg
import parallel_nibble_temp as nibble
import post_temp as post
import parallel_attachCDL as pp_cdl
import createJSON






def getSeries(step):
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')
    query = "SELECT * FROM series.params inner join series.{} using(series) where params.series='s15';".format(step)
    print 'query:-------------------------------->', query
    df = pd.read_sql_query(query, con=engine)
    # print df
    for index, row in df.iterrows():
        print row
        return row






def run_series(series, res ,mmu, years, pre_arg, refine_arg, pp_36and61mask_arg, core_filter, pp_rg_arg, pp_nbl_mtr_arg, post_arg, pp_nbl_ytc_arg, pp_cdl_arg):


	####  CREATE INSTANCES  ##################################################################
	# pre.pre = pre.ProcessingObject(series,res,mmu,years)
	# core.core = core.ProcessingObject(series,res,mmu,years,core_arg['filter'])
	# core.core = core.ProcessingObject(series,'r2',res,mmu,years,core_filter['filter_gdb'], core_filter['filter_key'])
	# post.post = post.ProcessingObject(series,res,mmu,years,post_arg['name'],post_arg['subname'])






	###  CALL METHODS FOR EACH PROCESSING STAGE #############################################

	#----------  perform pre processing  -------------------------------------------------
	# pre.createTrajectories()
	# pre.addGDBTable2postgres()
	# pre.FindRedundantTrajectories()



	#----------  perform refinement processing  ------------------------------------------
	# pp_36and61mask.run(series, res, mmu, years, 'ytc')

	#----------  perform core processing  ------------------------------------------------
	# mtr_parent = '_'+core_filter['filter_key']
	# mtr_child = mtr_parent+'_mtr'

	# parent_rg = mtr_parent+'_mtr'
	# print parent_rg
	# child_rg = '_'+pp_rg_arg['rg_key']+'_rgmask'+str(mmu)
	# print child_rg

	# child_mmu = '_'+pp_rg_arg['rg_key']+'_mmu'+str(mmu)
	# print child_mmu



	#### filter ####################
	# core.majorityFilter()
	# core.createMTR() 
	# prg.run(series, res, mmu, years, pp_rg_arg['name'], pp_rg_arg['rg_key'], pp_rg_arg['gdb_parent'], mtr_child, pp_rg_arg['gdb_child'], child_rg)
	# nibble.run(series, res, mmu, years, pp_nbl_mtr_arg['name'], pp_nbl_mtr_arg['subname'], pp_nbl_mtr_arg['pixel_type'], pp_nbl_mtr_arg['gdb_parent'], parent_rg, pp_nbl_mtr_arg['gdb_child'], child_rg, child_mmu)

    

	#---------- perform post processing  -------------------------------------------------
	# post.createYearbinaries()
	# post.createMask()
	# post.clipByMMU()

	post.run(getSeries('post'))
	# nibble.run(getSeries('post'))
	# nibble.run(series, res, mmu, years, pp_nbl_ytc_arg['name'], pp_nbl_ytc_arg['subname'], pp_nbl_ytc_arg['pixel_type'], pp_nbl_ytc_arg['gdb_parent'], parent_rg, pp_nbl_ytc_arg['gdb_child'], child_rg, child_mmu)
	# post.addGDBTable2postgres()
	# pp_cdl.run(series, res, mmu, years, 'ytc')




# Question: @from_config?
@from_config
# Question: where do you call this function?
# Question: Where does series come from (is it the path to multiple_series.json)
def main(series):
    for series_filename in series:
    	print "series: {}".format(series_filename)
        from_config(run_series)(series_filename)



if __name__ == '__main__':
	#sys.argv tuple of arguments taht get passed to the script
	# print sys.argv
	main(sys.argv[1])