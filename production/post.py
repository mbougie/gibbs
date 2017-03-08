from sqlalchemy import create_engine
import numpy as np, sys, os
from osgeo import gdal
from osgeo.gdalconst import *
# from pandas import read_sql_query
import pandas as pd
# import tables
import collections
from collections import namedtuple
import openpyxl
import arcpy
from arcpy import env
from arcpy.sa import *
import glob
import psycopg2

###################  set up environment  #####################################

production_type='production_ND'
arcpy.CheckOutExtension("Spatial")
env.scratchWorkspace ="C:/Users/bougie/Documents/ArcGIS/scratch.gdb"
mmu_gdb='C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/core/mmu.gdb/'






###################  set up directory and file structure  #####################################
class DirStructure:
    
    def __init__(self, dir_in, dir_out):
        #root dir for all directories
        rootdir='C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/'
        #a constant cdl dir to reference
        self.dir_cdl=rootdir+"pre/cdl/"

        self.dir_in = rootdir + dir_in

        self.dir_out = rootdir + dir_out
    

class FileStructure:
    
    def __init__(self, fname, ext):
        self.fname = fname
        self.ext = ext
        self.file_out=fname+ext



############################################################################################
def createFileOut(dir_root,file,fileend):
    print 'in!'
    #acronym: fnf=file name fragments
    file_out=(os.path.splitext(file)[0]).split(".")
    file_out=file_out[0]+"_"+fileend+".img"
    path_out=dir_root+fileend+"/"+file_out
    print path_out
    return path_out



def addColorMap(production_type,inraster,template):
    ##Add Colormap
    ##Usage: AddColormap_management in_raster {in_template_raster} {input_CLR_file}

    try:
        import arcpy
        # arcpy.CheckOutExtension("Spatial")
        # arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/ytc.gdb'
        
        ##Assign colormap using template image
        arcpy.AddColormap_management(inraster, "#", template)
        

    except:
        print "Add Colormap example failed."
        print arcpy.GetMessages()





def createBinaries(proc):
    print proc
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/pre/pre.gdb'
    #DESCRIPTION:subset the trajectoires by year to create binary ytc or yfc raster by year that represent the conversion to/from crop between succesive years
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/pre')
    df = pd.read_sql_query('select * from mtr.trajectories WHERE '+proc[0]+' IS NOT NULL',con=engine)
    print 'df--',df

    for index, row in df.iterrows():
        value=str(int(row['Value'])) 
        #cy=conversion year
        cy=str(int(row[proc[0]]))
        print type(cy)
        if cy not in {'2012','2016'}:

            
            output= 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/ytc.gdb/'+proc[0]+"_"+cy+"_b"
            print output

            #Get trajectories layer
            inRas = Raster('traj')

            print value
            
            # cond = "Value = "+value

            # # OutRas = Con(inRas, cy[2:], 0, cond)
            # # OutRas.save(output)
            # OutRas = Con(inRas, cy[2:], 0, cond)
            # OutRas.save(output)

  

            # print 'yyy',type(int(cy[2:]))

            cond = "Value <> "+value
            outSetNull = SetNull(inRas, cy[2:], cond)
            outSetNull.save(output)


            # cond = "Value <> "+value
            # if cy == '2013':
            # # outSetNull = SetNull(inRas, int(cy[2:]), cond)
            #     outSetNull = SetNull(inRas, 13, cond)
            #     outSetNull.save(output)
            # elif cy == '2014':
            # # outSetNull = SetNull(inRas, int(cy[2:]), cond)
            #     outSetNull = SetNull(inRas, 14, cond)
            #     outSetNull.save(output)
            # elif cy == '2015':
            # # outSetNull = SetNull(inRas, int(cy[2:]), cond)
            #     outSetNull = SetNull(inRas, 15, cond)
            #     outSetNull.save(output)





def attachCDL(params):
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/ytc.gdb'
    for raster in arcpy.ListDatasets("*_b", "Raster"): 
        print raster

        #######  GET YEAR  #############################
        #acronym: fnf=file name fragments
        fnf=(os.path.splitext(raster)[0]).split("_")
        print 'fnf:', fnf
        year = int(fnf[1]) - int(params[0])
        print year
        
        #######  DEFINE OUT RASTER  #####################
        x=(os.path.splitext(raster)[0]).split(".")
        file_out = x[0] + '_fc'
        print 'file_out: ', file_out

        
        #######  GET APPROPRIATE CDL BY YEAR  #############
        year=str(year)
        # cond = "Value = "+year[2:]
        

        

        ##NOTE STATIC ISSUE HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        cdl = Raster('D:/gibbs/production_pre/rasters/pre/cdl/'+str(year)+"_30m_cdls.img")
        print cdl


        cond = "Value <> "+year[2:]
        print cond
        outSetNull = SetNull(raster, cdl, cond)
        outSetNull.save(file_out)

        #NEED TO FIX CRASHING PYHTON POSSIBLY ADD ENVIRONMENT?????????????????????????????????????????
        # addColorMap(production_type,file_out,cdl)





def cellStats(params):
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/ytc.gdb'
    print 'params', params
    wc=params[0]


    files_list = []
    for raster in arcpy.ListDatasets("*"+wc, "Raster"): 
        print raster
        files_list.append(raster)

    file_out='ytc_'+wc+'_mosaic'
    print 'file_out: ', file_out
    
    # Save the output 
    outCellStatistics = CellStatistics([files_list[0],files_list[1],files_list[2]], "SUM", "DATA")
    outCellStatistics.save(file_out)




def mask(params):
    # params[x]:
    # 0=mask name
    # 1=wildcard
    # 2
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/ytc.gdb'
    mmu = 'ND_traj_n8h_mtr_8w_msk23_nbl'
    mmu_path = mmu_gdb + mmu
    
    if params[0] == 'clipByMMU':
        # arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/ytc.gdb'
        # raster='ytc_fc_mosaic'
        # inRaster=Raster(raster)
        # print inRaster

        for raster in arcpy.ListDatasets(params[1], "Raster"): 
            print raster

            # mmu = 'ND_traj_n8h_mtr_8w_msk23_nbl'
            mmu_path = mmu_gdb + mmu
            
            outRaster = raster+'_'+mmu

            outSetNull = SetNull(mmu_path, raster,  "Value <> 3")
            # # #Save the output 
            outSetNull.save(outRaster)

            # #msthc this to the cdl
            # addColorMap(outRaster,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/cdl.clr') 

    
    elif params[0] == 'ndTo1': 
    
        outRaster = 'ndTo1'
        print 'outRaster: ', outRaster

        OutRas=Con(((mmu_path == 3) & (IsNull('ytc_b_mosaic_ND_traj_n8h_mtr_8w_msk23_nbl'))), 1, 'ytc_b_mosaic_ND_traj_n8h_mtr_8w_msk23_nbl')
     
        OutRas.save(outRaster)

        # Con((("ND_traj_n8h_mtr_8w_msk23_nbl" == 3) & (IsNull("ytc_fc_mosaic_ND_traj_n8h_mtr_8w_msk23_nbl"))), "ytc_fc_mosaic_ND_traj_n8h_mtr_8w_msk23_nbl", 1)
        
# Con(("ND_traj_n8h_mtr_8w_msk23_nbl" == 3) & (IsNull("ytc_b_mosaic_ND_traj_n8h_mtr_8w_msk23_nbl")),1,"ytc_b_mosaic_ND_traj_n8h_mtr_8w_msk23_nbl")




        # Con((IsNull("ytc_fc_mosaic_ND_traj_n8h_mtr_8w_m45_nbl_fnl") & (IsNull("ytc_fc_mosaic_ND_traj_n8h_mtr_8w_m45_nbl_mask"))), "ytc_fc_mosaic_ND_traj_n8h_mtr_8w_m45_nbl_mask", 1)


        #     OutRas.save(outRaster)
        # inRaster = Raster(rootdir + params[0] + 'ytc_fc_mosiac_ND_traj_n8h_mtr_8w_m45_nbl.img')
        # print inRaster
        # mmu = Raster(rootdir + params[1] + 'traj_n8h_mtr_8w_m45_nbl.img')
        # print mmu
        # outRaster = rootdir + params[2] + 'ytc_fc_mosiac_ND_traj_n8h_mtr_8w_m45_nbl_ndTo1.img'
        # print outRaster
        # OutRas=Con((mmu == 3) & (IsNull(inRaster)), 1, inRaster)
    # elif params[0] == 'ndTo1': 
    #     # for raster in arcpy.ListDatasets('ytc_fc_mosaic_ND_traj_n8h_mtr_8w_m45_nbl', "Raster"): 
    #     raster='ytc_fc_mosaic_ND_traj_n8h_mtr_8w_m45_nbl'
    #     # raster='ytc_fc_mosaic'
    #     inRaster=Raster(raster)
    #     print 'inRaster: ', inRaster

    #     x='ND_traj_n8h_mtr_8w_m45_nbl'
    #     mmu=Raster(x)
        
    #     outRaster = raster+'_ndTo1'
    #     # outRaster = raster+'_ndTo1_full'
    #     print 'outRaster: ', outRaster

    #     OutRas=Con((mmu == 3) & (IsNull(inRaster)), 1, inRaster)
    
    #     #Save the output 
    #     OutRas.save(outRaster)

            # msthc this to the cdl
            # addColorMap(outRaster,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/cdl.clr')


def getRasterInGDB(wildcard):
    # files_list = []
    for raster in arcpy.ListDatasets(wildcard, "Raster"): 
        print raster
        # files_list.append(raster)
        return raster 





def nibble(params):
    arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/ytc.gdb'
    raster = 'ndTo1'

    for mask in arcpy.ListDatasets('*_nbl', "Raster"): 
        print mask

        outRaster = mask+'_fnl_python'
        print 'outRaster: ', outRaster

        ###  Execute Nibble  #####################
        nibbleOut = Nibble(raster, mask, "DATA_ONLY")

        ###  Save the output  ################### 
        nibbleOut.save(outRaster)


    # os.chdir(ds.dir_in)
    # for file in glob.glob("*.img"):
    #     #fnf=file name fragments
    #     fnf=(os.path.splitext(file)[0]).split("ndTo1")
    #     print file
    #     file_in=rootdir+params[0]+file
    #     print 'file_in: ', file_in
    #     mask=rootdir+params[1]+fnf[0]+'mask.img'
    #     print 'mask: ', mask
    #     file_out=rootdir+params[2]+fnf[0]+'mmuNibble_allvalues.img'
    #     print 'file_out: ', file_out

      
    #     ###  Execute Nibble  #####################
    #     nibbleOut = Nibble(file_in, mask, "ALL_VALUES")

    #     ###  Save the output  ################### 
    #     nibbleOut.save(file_out)

        # addColorMap(file_out,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/cdl.clr')






# def nibble(index,dir_in,dir_out):

#     # The raster used as the mask.
#     # It must be of integer type.
#     # Cells with NoData as their value will be nibbled in the in_raster.
#     arcpy.CheckOutExtension("Spatial")
#     env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

#     print index,dir_in,dir_out
#     ds = DirStructure(dir_in,dir_out)

  

#     os.chdir(ds.dir_in)
#     for file in glob.glob("*.img"):
#         #fnf=file name fragments
#         fnf=(os.path.splitext(file)[0]).split(".")
#         print file

#         #create file structure
#         fs = FileStructure(fnf[0]+'_nbl', '.img')

    
#         ####  create the paths to the mask files  ############# 
#         inFile = None
#         chunk=file[:-11]
#         print chunk
#         if index == 0:
#             inFile=rootdir+'core/filters/r1/'+chunk+'.img'
#             print inFile
#         if index == 1:
#             inFile=rootdir+'core/mtr/r2/'+chunk+'.img'
#             print inFile
#         if index == 2:
#             inFile=rootdir+'core/filters/r2/'+chunk+'.img'
#             print inFile

#         ###  Execute Nibble  #####################
#         nibbleOut = Nibble(inFile, ds.dir_in+file, "DATA_ONLY")

#         output = ds.dir_out+fs.file_out

#         ###  Save the output  ################### 
#         nibbleOut.save(output)

#         addColorMap(output,'C:/Users/bougie/Desktop/gibbs/development/rasters/colormaps/filter_and_mmu.clr')


######################################################################################


def fire_all(func_list,params):
    for f in func_list:
        f(params)


    
def runit():

    fct_list = {'createBinaries':[createBinaries],
                'attachCDL': [attachCDL],
                'cellStats': [cellStats],
                'mask':[mask],
                'mask_clipByMMU':[mask],
                'mask_prepNibble':[mask],
                'mask_ndTo1':[mask],
                'nibble':[nibble]
               }


    engine = create_engine('postgresql://postgres:postgres@localhost:5432/metadata')
    df = pd.read_sql_query('SELECT * FROM routes_prod.routes_post ORDER BY serial',con=engine)
    
    routes=df['routes']
    for i, route in enumerate(routes):
        print i
        print route
        for step in route:
            print 'step-----------------', step
         
            params=df[step][i]
            print 'params: ', params
            fire_all(fct_list[step],params)
    



##############  call functions  #####################################################
runit()




# base=arcpy.env.workspace+'/ytc_fc_mosaic'



# # base = "D:/gibbs/development/rasters/pre/cdl_ND_2010.img"
# out_coor_system = arcpy.Describe(base).spatialReference

# print out_coor_system















# createEmptyRaster()

# rootdir='C:/Users/bougie/Desktop/gibbs/production_pre/rasters/'
# #Attach the cdl values to the binary ytc/yfc rasters 
# arcpy.CheckOutExtension("Spatial")
# env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'


# arcpy.CopyRaster_management(
#     "Con_img1", "Con_img1_copy", 
#     format="IMAGINE Image")

# # /{subsetTrajectories,multiplyRasters_cdl,setBackground,stackRasters}

# #SetNull("ND_traj_n8h_mtr_8w_m45_nbl.img" != 3,"ytc_fc_mosiac.img")

# ytc_fc_mosaic
# # Local variables:
# Con_img10 = "C:\\Users\\bougie\\Documents\\ArcGIS\\mosaic.gdb\\ytc_fc_mosaic"
# Con_img10_CopyRaster = "C:\\Users\\bougie\\Documents\\ArcGIS\\Default.gdb\\Con_img10_CopyRaster"

# # Process: Copy Raster
# arcpy.CopyRaster_management(Con_img10, Con_img10_CopyRaster, "", "", "", "NONE", "NONE", "8_BIT_SIGNED", "NONE", "NONE", "IMAGINE Image", "NONE")






###################   call functions  ######################################################

# subsetTrajectories('ytc')
# multiplyRasters_cdl('ytc')
# setBackground('ytc')
# stackRasters('ytc')
# SetNull(("step1_t2.img" == 1)  &  ("yo.img" == 'NODATA'), 1)


# Con(("Raster1" == 20) | ("Raster1" == 24), 0, 1)









