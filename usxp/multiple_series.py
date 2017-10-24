"""
Run multiple series.
"""

import sys
import os
from config import from_config

# import pre
# import parallel_false as pf
# import core
import parallel_regiongroup as prg
# import parallel_nibble_mtr as pn_mtr
# import post
# import parallel_nibble_ytc as pn_ytc

# import refinement



def run_series(res,mmu,years,pre_arg, core_arg, prg_arg, post_arg):
	# Run a series from a personal ConfigObject

	# config = ConfigObject(a, b, c, d, e)

	# Rest of series code using config object

	# print("Contents of this config file: {} {}".format(pre, core))
	# print "pre: {}".format(str(pre_arg))
	# print "core: {}".format(str(core))

	####!!!!!! create an instance that the pre.py script can reference
	# pre.pre = pre.ProcessingObject(res,mmu,years)
	# core.core = core.ProcessingObject(res,mmu,years,core_arg['filter'])
	# post.post = post.ProcessingObject(res,mmu,years,post_arg['name'],post_arg['subname'])
	# prg.prg = prg.ProcessingObject(res,mmu,years,prg_arg['subtype'])
	# prg_config = prg.ProcessingObject(...)
  

    ####!!!!!! call functions of each script using the specific instance created above
	####  call functions in pre.py
	# pre.createTrajectories()
	# pre.addGDBTable2postgres()
	# pre.FindRedundantTrajectories()

    
	####  preform refinement processing
    




	####  preform core processing
	# core.majorityFilter()
	# core.createMTR() 
	# prg.run(prg_config)
	# prg_config = {'res': res, 'mmu': mmu, ...}
	prg.run(res, mmu, years, prg_arg['subname'])
	# pn_mtr.run()
	# core.addGDBTable2postgres()


	### preform post processing
	# post.createYearbinaries_better()
	# post.createMask()
	# pn.run()



	# os.system('parallel_regiongroup.py')






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