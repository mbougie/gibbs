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
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\refine\\imw\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\core\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\post\\')

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\qaqc\\')
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\temp\\')



import create_containers as cc
import create_instance_tmw as ci

###  import pre-processing script ###
import pre_tmw as pre

###  import refinement scripts  ###
import parallel_mask_2007 as mask_2007
import parallel_mask_nlcd as mask_nlcd
import parallel_masks_dev_36_61 as masks

###  import core-processing script ###
import parallel_core as core
import parallel_yxc as yxc
import parallel_cdl as cdl
import add2pg
# import qaqc_now as qaqc
# import temp_rg as temp




def subsetList(data):
    year_dict = {}
    
    ##size od subset list
    size = 4
    ##iteration set (move down the list one time for each group)
    step = 1

    data = [data[i : i + size] for i in range(0, len(data), step)]

    for nestedlist in data:
        if len(nestedlist) == 4:
            year_dict[nestedlist[2]] = nestedlist

    return year_dict



def getjsonfile():
    with open('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\configure\\multiple_instances.json') as json_data:
        template = json.load(json_data)
        return template

def getKernelfile(kernelpath):
    with open(kernelpath) as json_data:
        template = json.load(json_data)
        return template


def getKernelDict():
	kernels = getjsonfile()
	print 'kernels------------------------', kernels
	for key, list_kernels in kernels.iteritems():
		print key
		print list_kernels
		for kernelpath in list_kernels:
			data_kernel = getKernelfile(kernelpath)
			# data_kernel[key]=list_kernels[0]
			return data_kernel



if __name__ == '__main__':
	kernel=getKernelDict()
	# cc.run(route,instance)
	############  pre  #######################################
	years_list = range(kernel['global']['years'][0], kernel['global']['years'][1]+1)
	years_subset_dict = subsetList(years_list)

	for cy, years in years_subset_dict.iteritems():
		print cy
		if cy == 2009:
			years = [2008, 2009, 2010]
			ci.run(kernel, {cy:years}, version='base')
			data = gen.getJSONfile()
			# pre.run(data)
			ci.run(kernel, {cy:years}, version='refine')
			data = gen.getJSONfile()
			# mask_2007.run(data)
			mask_nlcd.run(data)
			# masks.run(data)


		print 'years:', years
        
         

        
		
       

      
		
		#########################  base  #############################################################################################
		
		## initilize the current instance so it can be reference for the base processing #############################################
		# ci.run(kernel, {cy:years}, version='base')
		# data = gen.getJSONfile()

		# ##_____pre processing_______________________
		# pre.run(data)

		##_____refinement processing________________


        #########################  instance  #############################################################################################
		
		## finish creating the current instance so it can be reference for the series processing #####################################
		# ci.run(kernel, {cy:years}, version='instance')
		# data = gen.getJSONfile()

		##_____core_________________________________ 
		# core.run(data)

		##_____post_________________________________
		# yxc.run(data, 'ytc')
		# add2pg.run(data, 'ytc')
		# yxc.run(data, 'yfc')
		# add2pg.run(data, 'yfc')

			
				














#############  most likely junk ###########################################



			# data = gen.getJSONfile()
			# temp.run(data)
            


            ############  core  #######################################
			# core.run(data)

            ############  post  #######################################
			# yxc.run(data, 'ytc')
			# add2pg.run(data, 'ytc')
			# yxc.run(data, 'yfc')
			# add2pg.run(data, 'yfc')
			
			# cdl.run(data)



			# qaqc.addGDBTable2postgres(data)






			# qaqc.rasterToPoly(data, data['core']['gdb'])
			
		




