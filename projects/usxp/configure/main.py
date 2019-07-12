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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\pre\\lookup_scripts')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\refine')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\core\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\yxc\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\cdl\\')

sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\choropleths\\')

sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\qaqc\\')

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\qaqc\\')
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\temp\\')



import create_containers as cc
import create_instance as ci

### import pre #######
import pre
import lookup_scripts_v4

###  import refinement scripts  ###
import mask_fn_yfc_61
# import mask_fn_yfc_nlcd_mtr1

import mask_fp_2007
# import mask_fp_yfc_potential
import mask_fp_nlcd_yfc
import mask_fp_nlcd_ytc
import mask_fp_yfc
import mask_fp_ytc
# 
### import core scripts ########## 
import parallel_core as core

### import post scripts ##########
import parallel_yxc as yxc
import parallel_cdl as cdl
import parallel_cdl_fc_bfnc as cdl_fc_bfnc
import parallel_cdl_fnc
import replace_61_w_hard_crop
# # import parallel_cdl_year as cdl_year
# # import add2pg_mtr
import add2pg_yxc
import add2pg_cdl
# # import deliver
# # import qaqc_now as qaqc
# # import temp_rg as temp
#import parallel_rg_eric_v2 as eric_v2
# import zonal_yxc_cdl
import choropleth_current


import qaqc




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

			####################################################################################################################################################
			### set-up stage ###################################################################################################################################
			####################################################################################################################################################
			
			### NOTE: need to have all gdb's made in pre and refine stages!  create_containers script ONLY create gdbs for the specific series
			### NOTE: cannot have similar named gdb with current code of referencing gdb (sub-optimal code)
			
			##########  create the geodatabase structure  #######################
			# cc.run(route,instance)


			##========  create the intial chunk of the current instance  =============
			##########NOTE!!!!  DONT run this unless you need to create a new trajectory   !!! #################
			# ci.run([key,route,instance],'initial')
			# data = gen.getKernels()
			# pre.run(data)
			##========================================================================


			#####################################################################################################################################################
			### pre and refinement stages #######################################################################################################################
			#####################################################################################################################################################
			
			###update current instance########################
			# ci.run([key,route,instance],'add_yfc')
			# data = gen.getCurrentInstance()

			###ONLY run script to create new traj lookup#################
			# lookup_scripts_v4.run(data)

			#___________________________________________________________________
			#____false negative refinement______________________________________
			#___________________________________________________________________
			# mask_fn_yfc_nlcd_mtr1.run(data)
			# mask_fn_yfc_61.run(data)
       
			#####create the add_yfc trajectories dataset############################
			# pre.run(data)   #####NEED TO DO A QAQC ON THIS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            #_____________________________________________________________________
			#____false positve refinement________________________________________
			#_____________________________________________________________________
			# mask_fp_2007.run(data)
			# mask_fp_nlcd_yfc.run(data)
			# mask_fp_nlcd_ytc.run(data)
			# mask_fp_yfc.run(data)
			# mask_fp_ytc.run(data)

			#####create the refined trajectories dataset############################
			# ci.run([key,route,instance],'final')
			data = gen.getCurrentInstance()
			print(data)
			
			######################create the rfnd dataset###################################
			# pre.run(data)


            #####################################################################################################################################################
			###### core stage ###################################################################################################################################
			#####################################################################################################################################################
			# core.run(data)
			# add2pg_mtr.run(data)


            #####################################################################################################################################################
			###### post stage ###################################################################################################################################
			#####################################################################################################################################################

			###_______YTC________________________________________________
			# yxc.run(data, 'ytc')
			# cdl_fc_bfnc.run(data, 'ytc', 'fc')
			# cdl.run(data, 'ytc', 'bfc')

			# add2pg_yxc.run(data, 'ytc')
			# add2pg_cdl.run(data, 'ytc', 'bfc')
			# add2pg_cdl.run(data, 'ytc', 'fc')


			###________YFC_______________________________________________
			# yxc.run(data, 'yfc')
			# cdl_fc_bfnc.run(data, 'yfc', 'bfnc')
			# cdl.run(data, 'yfc', 'fnc')

			# add2pg_yxc.run(data, 'yfc')
			# add2pg_cdl.run(data, 'yfc', 'bfnc')
			# add2pg_cdl.run(data, 'yfc', 'fnc')


			# parallel_cdl_fnc.run(data, 'yfc', 'fnc')
			

            ###################################################################################################################################################
			###### other ######################################################################################################################################
			###################################################################################################################################################
			# eric_v2.run(data)










			# for yxc in ['ytc']:
			# 	instance = {'series':'s35', 'yxc':[yxc], 'reclasslist':[[2009,1], [2010,1], [2011,1], [2012,1], [2013,1], [2014,1], [2015,1], [2016,1]], 'enumeration_unit':'states'}
			# zonal_yxc_cdl.run()



			# for mtr in []:
			# instance = {'series':'s35', 'enumeration_unit':'counties'}
			# choropleth_current.run(instance, data)



			

			# deliver.run(data)

			######  DML----add mtr3 value to pgtable  #####################################

			# dmlPGtable(data, yxc)


			# gen.addGDBTable2postgres_recent(data['core']['path'], data['core']['filename'])
			# gen.addRasterAttrib2postgres_recent(path='C:\\Users\\Bougie\\Desktop\\Gibbs\\data\usxp\\sa\\r2\\s35\\post\\ytc_s35.gdb\\s35_combine_state_ytc_fc', filename='s35_combine_state_ytc_fc', database='usxp', schema='combine')



			######qaqc#######################################################################
			qaqc.run(data)






			#### test #####################################################################
			# (10, [-1696904.9999999981, 1104255.0000000019, -1037714.999999998, 1517925.0000000023]),
			# yxc_inraster = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\temp\\tiles_ytc\\tile_10.tif'
			# inraster = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\temp\\tiles_fc\\tile_10.tif'
			# replace_61_w_hard_crop.run(data, yxc_inraster, inraster, -1696904.9999999981, 1104255.0000000019, -1037714.999999998, 1517925.0000000023)







