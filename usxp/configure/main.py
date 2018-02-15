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


# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\pre\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\core\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\post\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\qaqc\\')



import create_containers as cc
import create_instance as ci
import parallel_core as core
import parallel_yxc as yxc
import parallel_cdl as cdl
import qaqc_now as qaqc












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




			core.run(data)
			# yxc.run(data)
			# cdl.run(data)



			# qaqc.addGDBTable2postgres(data)






			# qaqc.rasterToPoly(data, data['core']['gdb'])
			
		




