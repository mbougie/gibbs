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

production_type='production_pre'
arcpy.CheckOutExtension("Spatial")
rootdir='C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/'
env.scratchWorkspace ="C:/Users/bougie/Documents/ArcGIS/scratch.gdb"
arcpy.env.workspace = 'C:/Users/bougie/Desktop/gibbs/'+production_type+'/rasters/post/ytc.gdb'






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





def createBinaries(index,proc,params):
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

            print value
            cond = "Value = "+value
            file_out= proc[0]+"_"+cy+"_b"
            print file_out

            #Get trajectories layer
            inRas = Raster('D:/gibbs/'+production_type+'/rasters/pre/traj/trajectories.img')

            OutRas = Con(inRas, 1, 0, cond)
            OutRas.save(file_out)

            # arcpy.BuildPyramids_management(OutRas)



def attachCDL(index,type,params):

    for raster in arcpy.ListDatasets("*_b", "Raster"): 
        print raster

        #######  GET YEAR  #############################
        #acronym: fnf=file name fragments
        fnf=(os.path.splitext(raster)[0]).split("_")
        print 'fnf:', fnf
        year = int(fnf[1]) - int(params[2])
        print year
        
        #######  DEFINE OUT RASTER  #####################
        x=(os.path.splitext(raster)[0]).split(".")
        file_out = x[0] + '_fc'
        print 'file_out: ', file_out

        
        #######  GET APPROPRIATE CDL BY YEAR  #############
        cdl = Raster('D:/gibbs/'+production_type+'/rasters/pre/cdl/'+str(year)+"_30m_cdls.img")
        print cdl


        OutRas=Con(raster, cdl, 0, "Value = 1")


        # # Save the output 
        OutRas.save(file_out)

        #NEED TO FIX CRASHING PYHTON POSSIBLY ADD ENVIRONMENT?????????????????????????????????????????
        # addColorMap(production_type,file_out,cdl)




def mask(index,type,params):
    # params[x]:
    # 0=mask name
    # 1=wildcard
    # 2

    # rootdir='C:/Users/bougie/Desktop/gibbs/'+pproduction_type+'/rasters/'
    # #Attach the cdl values to the binary ytc/yfc rasters 
    # arcpy.CheckOutExtension("Spatial")
    # env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print 'params: ', params
    
    if params[0] == 'clipByMMU':
        # for raster in arcpy.ListDatasets('*mosaic', "Raster"): 
        raster='ytc_fc_mosaic'
        inRaster=Raster(raster)
        print inRaster

        x='ND_traj_n8h_mtr_8w_m45_nbl'
        mmu=Raster(x)
        
        outRaster = raster+'_'+x
        print 'outRaster: ', outRaster

        outSetNull = SetNull(mmu, inRaster,  "Value <> 3")
        # #Save the output 
        outSetNull.save(outRaster)

        # #msthc this to the cdl
        # addColorMap(outRaster,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/cdl.clr') 

    
    elif params[0] == 'prepNibble': 
        # for raster in arcpy.ListDatasets('*mosaic', "Raster"): 
        raster='ytc_fc_mosaic_ND_traj_n8h_mtr_8w_m45_nbl'
        inRaster=Raster(raster)
        print inRaster

        x='ND_traj_n8h_mtr_8w_m45_nbl'
        mmu=Raster(x)
        
        outRaster = raster+'_mask'
        print 'outRaster: ', outRaster

        OutRas=Con((mmu == 3) & (IsNull(inRaster)), inRaster, 1)
    
        #Save the output 
        OutRas.save(outRaster)






        #     OutRas.save(outRaster)
        # inRaster = Raster(rootdir + params[0] + 'ytc_fc_mosiac_ND_traj_n8h_mtr_8w_m45_nbl.img')
        # print inRaster
        # mmu = Raster(rootdir + params[1] + 'traj_n8h_mtr_8w_m45_nbl.img')
        # print mmu
        # outRaster = rootdir + params[2] + 'ytc_fc_mosiac_ND_traj_n8h_mtr_8w_m45_nbl_ndTo1.img'
        # print outRaster
        # OutRas=Con((mmu == 3) & (IsNull(inRaster)), 1, inRaster)
    elif params[0] == 'ndTo1': 
        # for raster in arcpy.ListDatasets('ytc_fc_mosaic_ND_traj_n8h_mtr_8w_m45_nbl', "Raster"): 
        raster='ytc_fc_mosaic_ND_traj_n8h_mtr_8w_m45_nbl'
        inRaster=Raster(raster)
        print 'inRaster: ', inRaster

        x='ND_traj_n8h_mtr_8w_m45_nbl'
        mmu=Raster(x)
        
        outRaster = raster+'_ndTo1'
        print 'outRaster: ', outRaster

        OutRas=Con((mmu == 3) & (IsNull(inRaster)), 1, inRaster)
    
        #Save the output 
        OutRas.save(outRaster)

            # msthc this to the cdl
            # addColorMap(outRaster,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/cdl.clr')


def getRasterInGDB(wildcard):
    # files_list = []
    for raster in arcpy.ListDatasets(wildcard, "Raster"): 
        print raster
        # files_list.append(raster)
        return raster 




def createEmptyRaster():
    dataset = "D:/gibbs/production/pre/cdl/2013_30m_cdls.img"
    spatial_ref = arcpy.Describe(dataset).spatialReference
    # arcpy.CreateRasterDataset_management(arcpy.env.workspace, "ytc_fc_mosaic", "8_BIT_UNSIGNED", "1")
    arcpy.CreateRasterDataset_management(arcpy.env.workspace,"ytc_fc_mosaic","30","8_BIT_UNSIGNED",spatial_ref, "1")



def mosiacStack(index,type,params):

    createEmptyRaster()

    files_list = []
    for raster in arcpy.ListDatasets("*fc", "Raster"): 
        print raster
        files_list.append(raster)
      
  
    files_in=';'.join(files_list)
    print files_in

    # file_out=ds.dir_out+'unsign8.img'
    file_out='ytc_fc_mosaic'
    print 'file_out: ', file_out
    # Save the output 
    arcpy.Mosaic_management(files_in,file_out,"LAST","FIRST","0", "0", "", "", "")



def nibble(index,type,params):
    inRaster=Raster(raster)
    print inRaster

    x='ND_traj_n8h_mtr_8w_m45_nbl'
    mmu=Raster(x)

    outRaster = raster+'_'+x+'nibble'
    print 'outRaster: ', outRaster

    ###  Execute Nibble  #####################
    nibbleOut = Nibble(file_in, mask, "ALL_VALUES")

    ###  Save the output  ################### 
    nibbleOut.save(file_out)


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



def fire_all(func_list,index,type,params):
    for f in func_list:
        f(index,type,params)


    
def runit():

    fct_list = {'createBinaries':[createBinaries],
                'attachCDL': [attachCDL],
                'mosiacStack': [mosiacStack],
                'mask':[mask],
                'clipByMMU':[mask],
                'mask_prepNibble':[mask],
                'mask_ndTo1':[mask],
                'nibble':[nibble]
               }


    engine = create_engine('postgresql://postgres:postgres@localhost:5432/metadata')
    df = pd.read_sql_query('SELECT * FROM routes_prod.routes_post ORDER BY serial',con=engine)
    
    routes=df['routes']
    for i, route in enumerate(routes):
        print i
        print 'route: ', route
        for step in route:
            print step
         
            params=df[step][i]
            type=df['type'][i]
            print type
            fire_all(fct_list[step],i,type,params)
    





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









