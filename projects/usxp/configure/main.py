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
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\qaqc\\')
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\temp\\')



import create_containers as cc
import create_instance as ci

### import pre
import pre

###  import refinement scripts  ###
import parallel_mask_2007 as mask_2007
import parallel_mask_nlcd as mask_nlcd
import parallel_masks_dev_36_61 as masks


import parallel_core as core
import parallel_yxc as yxc
import parallel_cdl as cdl
import parallel_cdl_year as cdl_year
import add2pg
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



            ##########  create the geodatabase structure  #######################
			# cc.run(route,instance)


            #############  could be junk!!!!  ########################################

			# ci.run([key,route,instance],'initial')
			# data = gen.getKernels()
			# pre.run(data)
			# ci.run([key,route,instance],'final')


			##========  create the intial chunk of the current instance  =============
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
			# masks.run(data)

			### create the refined trajectories dataset 
			# pre.run(data)

			######  core script  ###################################################
			core.run(data)

			######  post script  ###################################################
			# yxc.run(data, 'ytc')
			# add2pg.run(data, 'ytc')
			# yxc.run(data, 'yfc')
			# add2pg.run(data, 'yfc')

			
			######  DML----add mtr3 value to pgtable  #####################################
			# dmlPGtable(data, yxc)



37664686

37650966

4485928
4481090


























##################  possible junk  ##############################################################################
			# data = gen.getKernels()
			# temp.run(data)


			####------this is imw only----------------start-----------------------
			## create the mosaiced dataset
			# post_imw.run(data, 'yfc')

			## add the table to merged_table
			# add2pg.run(data, 'yfc')

			####------this is imw only---------------end-------------------------
			# core.run(data)

			# yxc.run(data, 'ytc')
			# add2pg.run(data, 'ytc')
			# yxc.run(data, 'yfc')
			# add2pg.run(data, 'yfc')

			# cdl_year.run(data, 'fc')



			# qaqc.addGDBTable2postgres(data)






			# qaqc.rasterToPoly(data, data['core']['gdb'])
			
		





