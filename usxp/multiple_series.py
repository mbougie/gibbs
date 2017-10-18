"""
Run multiple series.
"""

import sys
from config import from_config

import pre
import core
# import refinement


def run_series(res,mmu,years,pre_arg, core, hi):
	# Run a series from a personal ConfigObject

	# config = ConfigObject(a, b, c, d, e)

	# Rest of series code using config object

	# print("Contents of this config file: {} {}".format(pre, core))
	print "pre: {}".format(str(pre_arg))
	print "core: {}".format(str(core))
	
	create an instance that the pre.py script can reference
	pre.pre = pre.ProcessingObject(res,mmu,years)
	
	pre.run()

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