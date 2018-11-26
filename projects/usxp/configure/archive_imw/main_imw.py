"""
Run multiple series.
"""

import sys
import os
# from config import from_config
from sqlalchemy import create_engine
import pandas as pd
import json
import psycopg2
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\misc\\')
import general as gen


sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\pre\\imw\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\refine\\imw\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\core\\imw\\')
sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\post\\')

# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\qaqc\\')
# sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\usxp\\temp\\')



import create_containers as cc
import create_instance_tmw as ci

###  import pre-processing script ###
import pre_imw as pre

###  import refinement scripts  ###
import parallel_mask_2007_imw as mask_2007
import parallel_mask_nlcd_imw as mask_nlcd
import parallel_masks_dev_36_61_imw as masks

###  import core-processing script ###
import parallel_core_imw as core
import parallel_yxc as yxc
import parallel_cdl as cdl
import post_imw as post
import add2pg
# import qaqc_now as qaqc
# import temp_rg as temp



try:
    conn = psycopg2.connect("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")
except:
    print "I am unable to connect to the database"









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




def getDF(data, yxc):
	mtr = {'ytc':3, 'yfc':4}
	# set the engine.....
	engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/usxp')

	# Execute AddField twice for two new fields
	fields = [f.name for f in arcpy.ListFields(data['core']['path'])]

	# converts a table to NumPy structured array.
	arr = arcpy.da.TableToNumPyArray(data['core']['path'],fields)
	print arr

	# convert numpy array to pandas dataframe
	df = pd.DataFrame(data=arr)

	df.columns = map(str.lower, df.columns)

	### return the cell value 
	row = df.loc[df['value'] == mtr[yxc]]
	cell = row['count']
	return int(cell)

    




def ddlPGtable(kernel, yxc):
    #this is a sub function for addGDBTable2postgres()
    print kernel['global']['instance']

    cur = conn.cursor()
   
    query = 'DROP TABLE IF EXISTS counts.{0}_{1}30_2008to2017_mmu5_separate; CREATE TABLE counts.{0}_{1}30_2008to2017_mmu5_separate (value integer,count double precision,acres bigint,series text,yxc text)'.format(kernel['global']['instance'], yxc)

    print query
    cur.execute(query)

    conn.commit() 




def dmlPGtable(data, yxc):
    #this is a sub function for addGDBTable2postgres()

    
    value = data['global']['years_conv']
    print value
    count = getDF(data, yxc)
    print count
    acres = gen.getAcres(count, int(data['global']['res']))
    print acres
    series = data['global']['instance']+'_seperate'
    print series
    

    cur = conn.cursor()
   
    query = "INSERT INTO counts.{0}_{5}30_2008to2017_mmu5_separate (value, count, acres, series, yxc) VALUES ({1},{2},{3},'{4}','{5}')".format(data['global']['instance'], value, count, acres ,series, yxc)

    print query
    cur.execute(query)

    conn.commit() 






if __name__ == '__main__':
	yxc = 'yfc'
	kernel=getKernelDict()
	# cc.run(route,instance)

    ### DDL-----create the count table in postgres  
	# ddlPGtable(kernel, yxc)

	############  pre  #######################################
	years_list = range(kernel['global']['years'][0], kernel['global']['years'][1]+1)
	years_subset_dict = subsetList(years_list)

	# for cy, years in years_subset_dict.iteritems():
	# 	print cy
	# 	if cy == 2009:
	# 		print 'cy------------', cy
	# 		years = [2008, 2009, 2010]

	# 		##========  create the intial chunk of the current instance  =============
	# 		ci.run(kernel, {cy:years}, version='initial')
	# 		# data = gen.getJSONfile()
	# 		# pre.run(data)
	# 		##========================================================================

	# 		##========  update the current instance  =================================
	# 		ci.run(kernel, {cy:years}, version='final')
	# 		data = gen.getJSONfile()
	# 		##========================================================================


	# 		#######  refinement scripts  ############################################
	# 		##______create the 3 masks___________________________________
	# 		# mask_2007.run(data)
	# 		# mask_nlcd.run(data)
	# 		# masks.run(data)
	# 		# 
	# 		### create the refined trajectories dataset 
	# 		# pre.run(data)

	# 		######  core script  ###################################################
	# 		# core.run(data)

	# 		######  DML----add mtr3 value to pgtable  #####################################
	# 		dmlPGtable(data, yxc)



	# 	else:
	# 		print 'cy------------', cy

	# 		##========  create the intial chunk of the current instance  =============
	# 		ci.run(kernel, {cy:years}, version='initial')
	# 		# data = gen.getJSONfile()
	# 		##========================================================================

	# 		#######  pre processing scripts  #########################################
	# 		##desciption
	# 		# pre.run(data)


	# 		##========  update the current instance  =================================
	# 		ci.run(kernel, {cy:years}, version='final')
	# 		data = gen.getJSONfile()
	# 		##========================================================================


	# 		#######  refinement scripts  ############################################
	# 		### create the 2 masks (Note: dont need to create mask_2007!)
	# 		# mask_nlcd.run(data)
	# 		# masks.run(data)

	# 		##create the refined trajectories dataset 
	# 		# pre.run(data)

	# 		######  core script  ###################################################
	# 		# core.run(data)


	# 		######  add mtr3 value to pgtable  #####################################
	# 		dmlPGtable(data, yxc)
    





	
        
         

        
		
       

      
		
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
			
		




