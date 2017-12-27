"""
Run multiple series.
"""

import sys
import os
from config import from_config

import pre
import parallel_61and36mask as pp_36and61mask
import core
import parallel_regiongroup as prg
import parallel_nibble as nibble
import parallel_mtr as pp_mtr
import post
import parallel_attachCDL as pp_cdl






def run_series(series, route, res, mmu, years, core_filter, pp_rg_arg, pp_mtr_arg, pp_nbl_mtr_arg):


    ####  CREATE INSTANCES  ##################################################################
    core.core = core.ProcessingObject(series,route,res,mmu,years,core_filter['filter_gdb'], core_filter['filter_key'])


# self, series, res, mmu, years, name, subname, pixel_type, gdb_parent, parent_seq, gdb_child, mask_seq
    

    #----------  perform core processing  ------------------------------------------------
    if route == 'r1':
        # core.createMTR() 
        core.majorityFilter()
        # prg.run(series, res, mmu, years, pp_rg_arg['name'])
        # nibble.run(series, res, mmu, years, pp_nbl_mtr_arg['subname'], pp_nbl_mtr_arg['pixel_type'])

    elif route == 'r2':
        print '----route2-------'
        mtr_parent = '_'+core_filter['filter_key']
        mtr_child = mtr_parent+'_mtr'
        
        parent_rg = mtr_parent+'_mtr'
        print parent_rg
        child_rg = '_'+pp_rg_arg['rg_key']+'_rgmask'+str(mmu)
        print child_rg
        
        child_mmu = '_'+pp_rg_arg['rg_key']+'_mmu'+str(mmu)
        print child_mmu

        #### filter ####################
        core.majorityFilter()

        
        ### mtr #######################
        # pp_mtr.run(series, res, mmu, years, pp_mtr_arg['name'], pp_mtr_arg['gdb_parent'], mtr_parent, pp_mtr_arg['gdb_child'], mtr_child)
       

        ### mmu #######################
        # prg.run(series, res, mmu, years, pp_rg_arg['name'],  pp_rg_arg['rg_key'], pp_rg_arg['gdb_parent'], mtr_child, pp_rg_arg['gdb_child'], child_rg)
        nibble.run(series, res, mmu, years, pp_nbl_mtr_arg['name'], pp_nbl_mtr_arg['subname'], pp_nbl_mtr_arg['pixel_type'], pp_nbl_mtr_arg['gdb_parent'], parent_rg, pp_nbl_mtr_arg['gdb_child'], child_rg, child_mmu)







    elif route == 'r3':
        print '----route3-------'
        parent_rg = '_'+core_filter['filter_key']
        print parent_rg
        child_rg = '_'+pp_rg_arg['rg_key']+'_rgmask'+str(mmu)
        print child_rg
        child_mmu = parent_rg+'_'+pp_rg_arg['rg_key']+'_mmu'+str(mmu)
        print child_mmu

        mtr_child =  child_mmu+'_mtr'
        
        ####filter#######
        core.majorityFilter()


        ### mmu #######################
        prg.run(series, res, mmu, years, pp_rg_arg['name'],  pp_rg_arg['rg_key'], pp_rg_arg['gdb_parent'], parent_rg, pp_rg_arg['gdb_child'], child_rg)
        nibble.run(series, res, mmu, years, pp_nbl_mtr_arg['name'], pp_nbl_mtr_arg['subname'], pp_nbl_mtr_arg['pixel_type'], pp_nbl_mtr_arg['gdb_parent'], parent_rg, pp_nbl_mtr_arg['gdb_child'], child_rg, child_mmu)


        #### mtr #######################
        pp_mtr.run(series, res, mmu, years, pp_mtr_arg['name'], pp_mtr_arg['gdb_parent'], child_mmu, pp_mtr_arg['gdb_child'], mtr_child)
    
        
    




# Question: @from_config?
@from_config
# Question: where do you call this function?
# Question: Where does series come from (is it the path to multiple_series.json)
def main(route):
    for series_filename in route:
        print "series: {}".format(series_filename)
        from_config(run_series)(series_filename)



if __name__ == '__main__':
    #sys.argv tuple of arguments taht get passed to the script
    main(sys.argv[1])