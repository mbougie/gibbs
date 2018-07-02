"""
Run multiple series.
"""

import sys
import os
# from config import from_config
from sqlalchemy import create_engine
import pandas as pd
import json
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen


sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\pre\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\refine')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\core\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\yxc\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\test\\')
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\qaqc\\')
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\temp\\')



import create_containers as cc
import create_instance as ci

### import pre
import pre

###  import refinement scripts  ###
# import parallel_mask_2007 as mask_2007
# import parallel_mask_nlcd as mask_nlcd

# import parallel_masks_yfc as masks_yfc
# import parallel_masks_ytc as masks_ytc
# import parallel_masks_ytc_df as masks_ytc_df



# import parallel_core as core
# import parallel_yxc as yxc
# import parallel_cdl_ytc as cdl_ytc
# import parallel_cdl as cdl
# import parallel_cdl_year as cdl_year
import add2pg_mtr
import add2pg_yxc
# import add2pg_cdl
# import deliver
# import qaqc_now as qaqc
# import temp_rg as temp




def getTemplateJSONpath():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\configure\\multiple_instances.json') as json_data:
        template = json.load(json_data)
        print 'template', template
        return template

def getKernelfile(kernelpath):
    with open(kernelpath) as json_data:
        template = json.load(json_data)
        return template

# print  getKernelfile()

if __name__ == '__main__':
	kernels = getTemplateJSONpath()
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


			### NOTE: need to have all gdb's made in pre and refine stages!  create_containers script ONLY create gdbs for the specific series
			##########  create the geodatabase structure  #######################
			# cc.run(route,instance)


			##========  create the intial chunk of the current instance  =============
			##########NOTE!!!!  DONT run this unless you need to create a new trajectory   !!! #################
			# ci.run([key,route,instance],'initial')
			# data = gen.getKernels()
			# pre.run(data)
			##========================================================================




			##========  update the current instance  =================================
			# ci.run([key,route,instance],'final')
			data = gen.getCurrentInstance()
			##========================================================================


			#######  refinement scripts  ############################################
			##______create the 3 masks___________________________________
			# mask_2007.run(data)
			# mask_nlcd.run(data)
			
			# masks_yfc.run(data)
			# masks_ytc_df.run(data)
			# masks_ytc.run(data)
			# masks_ytc_df.run(data)

			### create the refined trajectories dataset 
			# pre.run(data)

			######  core script  ###################################################
			# core.run(data)
			add2pg_mtr.run(data)



			# masks_ytc_df.run(data)


	
			######  post script  ###################################################
			# yxc.run(data, 'ytc')
			# add2pg_yxc.run(data, 'ytc')
			
			
			# add2pg_cdl.run(data, 'ytc', 'bfc')

			# cdl_ytc.run(data, 'ytc', 'fc')
			# cdl.run(data, 'ytc', 'bfc')
			

			# add2pg_cdl.run(data, 'ytc', 'fc')

			# yxc.run(data, 'yfc')
			# add2pg_yxc.run(data, 'yfc')
			# cdl.run(data, 'yfc', 'bfnc')
			# # add2pg_cdl.run(data, 'yfc', 'bfnc')
			# cdl.run(data, 'yfc', 'fnc')
			# add2pg_cdl.run(data, 'yfc', 'fnc')
			




			# deliver.run(data)

			######  DML----add mtr3 value to pgtable  #####################################

			# dmlPGtable(data, yxc)






			# ##################  test  ##############################################

			# test_main.run(data, 'ytc', 'fc')







			# 45572010
			# 45571996
			# 45572009


