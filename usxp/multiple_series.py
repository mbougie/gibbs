"""
Run multiple series.
"""

import sys
import os
from config import from_config

import pre
# import refinement
# import parallel_false as pf
import core
import parallel_regiongroup as prg
import parallel_nibble as nibble
import post






def run_series(series,res,mmu,years,pre_arg, core_arg, pp_rg_arg, pp_nbl_mtr_arg, post_arg, pp_nbl_ytc_arg):
	

	####  CREATE INSTANCES  ##################################################################
	# pre.pre = pre.ProcessingObject(series,res,mmu,years)
	core.core = core.ProcessingObject(series,res,mmu,years,core_arg['filter'])
	post.post = post.ProcessingObject(series,res,mmu,years,post_arg['name'],post_arg['subname'])

  





    ###  CALL METHODS FOR EACH PROCESSING STAGE #############################################

    #----------  perform pre processing  -------------------------------------------------
	# pre.createTrajectories()
	# pre.addGDBTable2postgres()
	# pre.FindRedundantTrajectories()

    

    #----------  perform refinement processing  ------------------------------------------




	#----------  perform core processing  ------------------------------------------------
	core.majorityFilter()
	core.createMTR() 
	prg.run(series, res, mmu, years, pp_rg_arg['name'])
	nibble.run(series, res, mmu, years, pp_nbl_mtr_arg['subname'], pp_nbl_mtr_arg['pixel_type'])
	


	#---------- perform post processing  -------------------------------------------------
	post.createYearbinaries()
	post.createMask()
	post.clipByMMU()
	nibble.run(series, res, mmu, years, pp_nbl_ytc_arg['name'], pp_nbl_ytc_arg['pixel_type'])
	post.addGDBTable2postgres()




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