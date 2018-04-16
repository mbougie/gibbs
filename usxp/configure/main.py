"""
Run multiple series.
"""

import sys
import os
# from config import from_config
from sqlalchemy import create_engine
import pandas as pd
import json
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen


sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\pre\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\core\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\post\\')
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\qaqc\\')
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\temp\\')



import create_containers as cc
import create_instance as ci
import parallel_core as core
import parallel_yxc as yxc
import parallel_cdl as cdl
import add2pg
# import qaqc_now as qaqc
# import temp_rg as temp


import post_imw












def getjsonfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\configure\\multiple_instances.json') as json_data:
        template = json.load(json_data)
        return template

def getKernelfile(kernelpath):
    with open(kernelpath) as json_data:
        template = json.load(json_data)
        return template

# print  getKernelfile()

if __name__ == '__main__':
	kernels = getjsonfile()
	print 'kernels------------------------', kernels
	for key, list_kernels in kernels.iteritems():
		for kernelpath in list_kernels:
			print kernelpath
			data_kernel = getKernelfile(kernelpath)
			print data_kernel
			route = data_kernel['core']['route']
			print route
			instance = data_kernel['global']['instance']
			print 'instance---------', instance
			

			# cc.run(route,instance)
			# ci.run([key,route,instance])




			data = gen.getJSONfile()
			# temp.run(data)


            ####------this is imw only----------------start-----------------------
			## create the mosaiced dataset
			# post_imw.run(data, 'yfc')
            
            ## add the table to merged_table
			add2pg.run(data, 'yfc')
            
            ####------this is imw only---------------end-------------------------
			# core.run(data)

			# yxc.run(data, 'ytc')
			# add2pg.run(data, 'ytc')
			# yxc.run(data, 'yfc')
			# add2pg.run(data, 'yfc')
			
			# cdl.run(data)



			# qaqc.addGDBTable2postgres(data)






			# qaqc.rasterToPoly(data, data['core']['gdb'])
			
		



