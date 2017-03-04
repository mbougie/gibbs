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

rootdir='C:/Users/bougie/Desktop/gibbs/production_pre/rasters/'
###################  set up directory and file structure  #####################################
class DirStructure:
    
    def __init__(self, dir_in, dir_out):
        #root dir for all directories
        rootdir='C:/Users/bougie/Desktop/gibbs/production_pre/rasters/'
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



def addColorMap(inraster,template):
    ##Add Colormap
    ##Usage: AddColormap_management in_raster {in_template_raster} {input_CLR_file}

    try:
        import arcpy
        arcpy.env.workspace = r'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'
        
        ##Assign colormap using template image
        arcpy.AddColormap_management(inraster, "#", template)
        

    except:
        print "Add Colormap example failed."
        print arcpy.GetMessages()





def subsetTrajectories(x):
    #NOTE !!!! THIS CODE IS NOT DONE!!!!!!!!!!!!!!!!!!!!!!


    #DESCRIPTION:subset the trajectoires by year to create binary ytc or yfc raster by year that represent the conversion to/from crop between succesive years
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/pre')
    df = pd.read_sql_query('select * from mtr.trajectories WHERE '+x+' IS NOT NULL',con=engine)
    print 'df--',df
    fulllist=[]
    # fulllist=[]
    for index, row in df.iterrows():
        templist=[]
        value=str(int(row['Value'])) 
        #cy=conversion year
        cy=str(int(row[x]))
        print type(cy)
        if cy not in {'2012','2016'}:
            print cy 

            #need generic workspace
            env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

            cond = "Value = "+value
            filename = x+"_"+cy+"_b.img"

            inRas1 = Raster('C:/Users/bougie/Desktop/gibbs/development/rasters/pre/traj/ND_traj.img')
            arcpy.CheckOutExtension("Spatial")
            OutRas = Con(inRas1, 1, 0, cond)
            OutRas.save("C:/Users/bougie/Desktop/gibbs/production_pre/rasters/post/"+x+"/binary/"+filename)





def multiplyRasters_cdl(index,type,params):
     #Attach the cdl values to the binary ytc/yfc rasters 
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    ds = DirStructure(params[0], params[1])

    print 'type', type[0]

    #directory to cdl that will by referenced
    ds.dir_cdl


    os.chdir(ds.dir_in)
    for file in glob.glob("*.img"):
        print(file)
        #acronym: fnf=file name fragments
        fnf=(os.path.splitext(file)[0]).split("_")
        print 'fnf:', fnf
        year = int(fnf[1]) - int(params[2])
        print year
        file_out=(os.path.splitext(file)[0]).split(".")
        fs = FileStructure(file_out[0]+"_"+type[1],".img")
        print 'fs: ', fs.file_out

        # Set local variables
        inRaster1 = Raster(ds.dir_in+file)
      
        inRaster2 = Raster(ds.dir_cdl+"cdl_ND_"+str(year)+".img")

        outRaster = ds.dir_out+fs.file_out

        # Check out the ArcGIS Spatial Analyst extension license
        arcpy.CheckOutExtension("Spatial")

        # Execute Times
        outTimes = inRaster1 * inRaster2

        # Save the output 
        outTimes.save(outRaster)

        #
        addColorMap(outRaster,inRaster2)




def mask(index,type,params):
    rootdir='C:/Users/bougie/Desktop/gibbs/production_pre/rasters/'
    #Attach the cdl values to the binary ytc/yfc rasters 
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print 'params: ', params
    
    if params[3] == 'clipmmu':
        inRaster = Raster(rootdir + params[0] + 'ytc_fc_mosiac.img')
        print inRaster
        mmu = Raster(rootdir + params[1] + 'ND_traj_n8h_mtr_8w_m45_nbl.img')
        print mmu
        outRaster = rootdir + params[2] + 'ytc_fc_mosiac_ND_traj_n8h_mtr_8w_m45_nbl.img'
        print outRaster
        OutRas=SetNull(mmu != 3,inRaster)
        
        #Save the output 
        OutRas.save(outRaster)

        #msthc this to the cdl
        addColorMap(outRaster,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/cdl.clr') 

    
    elif params[3] == 'prepNibble': 
        inRaster = Raster(rootdir + params[0] + 'ytc_fc_mosiac.img')
        print inRaster
        mmu = Raster(rootdir + params[1] + 'ND_traj_n8h_mtr_8w_m45_nbl.img')
        print mmu
        outRaster = rootdir + params[2] + 'ytc_fc_mosiac_ND_traj_n8h_mtr_8w_m45_nbl_mask.img'
        print outRaster
        OutRas=Con((mmu == 3) & (IsNull(inRaster)), inRaster, 1)
        
        #Save the output 
        OutRas.save(outRaster)


    elif params[3] == 'ndTo1': 
        inRaster = Raster(rootdir + params[0] + 'ytc_fc_mosiac_ND_traj_n8h_mtr_8w_m45_nbl.img')
        print inRaster
        mmu = Raster(rootdir + params[1] + 'ND_traj_n8h_mtr_8w_m45_nbl.img')
        print mmu
        outRaster = rootdir + params[2] + 'ytc_fc_mosiac_ND_traj_n8h_mtr_8w_m45_nbl_ndTo1.img'
        print outRaster
        OutRas=Con((mmu == 3) & (IsNull(inRaster)), 1, inRaster)
        
        #Save the output 
        # OutRas.save(outRaster)

        #msthc this to the cdl
        addColorMap(outRaster,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/cdl.clr')








def createEmptyRaster(dir_out):
    import arcpy
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    arcpy.CreateRasterDataset_management(dir_out,"ytc_fc_mosiac.img","30","8_BIT_UNSIGNED",arcpy.SpatialReference(5070), "1")



def mosiacStack(index,type,params):
    ##Mosaic two TIFF images to a single TIFF image
    ##Background value: 0
    ##Nodata value: 0
    # arcpy.Mosaic_management("landsatb4a.tif;landsatb4b.tif","Mosaic\\landsat.tif","LAST","FIRST","0", "9", "", "", "")
    



    #Attach the cdl values to the binary ytc/yfc rasters 
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    ds = DirStructure(params[0], params[1])

    #create the empty raster
    createEmptyRaster(ds.dir_out)

    os.chdir(ds.dir_in)

    rasters = glob.glob("*.img")

    files_list = []
    print 'rasters: ', rasters
    for file in rasters:
        print file
        files_list.append(ds.dir_in + file)
      
  
    files_in=';'.join(files_list)
    print files_in

    file_out=ds.dir_out+'ytc_fc_mosiac.img'
    print 'file_out', file_out
    # Save the output 
    arcpy.Mosaic_management(files_in,file_out,"LAST","FIRST","0", "0", "", "", "")



def nibble(index,type,params):

    # The raster used as the mask.
    # It must be of integer type.
    # Cells with NoData as their value will be nibbled in the in_raster.
    arcpy.CheckOutExtension("Spatial")
    env.workspace = 'C:/Users/Bougie/Documents/ArcGIS/Default.gdb'

    print params

    ds = DirStructure(params[0], params[2])

    os.chdir(ds.dir_in)
    for file in glob.glob("*.img"):
        #fnf=file name fragments
        fnf=(os.path.splitext(file)[0]).split("ndTo1")
        print file
        file_in=rootdir+params[0]+file
        print 'file_in: ', file_in
        mask=rootdir+params[1]+fnf[0]+'mask.img'
        print 'mask: ', mask
        file_out=rootdir+params[2]+fnf[0]+'mmuNibble_allvalues.img'
        print 'file_out: ', file_out

      
        ###  Execute Nibble  #####################
        nibbleOut = Nibble(file_in, mask, "ALL_VALUES")

        ###  Save the output  ################### 
        nibbleOut.save(file_out)

        addColorMap(file_out,'C:/Users/bougie/Desktop/gibbs/production_pre/rasters/colormaps/cdl.clr')






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

    fct_list = {'subsetTrajectories': [subsetTrajectories],
                'multiplyRasters_cdl': [multiplyRasters_cdl],
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
# subsetTrajectories('ytc')
runit()
# createEmptyRaster()



# /{subsetTrajectories,multiplyRasters_cdl,setBackground,stackRasters}

#SetNull("ND_traj_n8h_mtr_8w_m45_nbl.img" != 3,"ytc_fc_mosiac.img")







###################   call functions  ######################################################

# subsetTrajectories('ytc')
# multiplyRasters_cdl('ytc')
# setBackground('ytc')
# stackRasters('ytc')
# SetNull(("step1_t2.img" == 1)  &  ("yo.img" == 'NODATA'), 1)


# Con(("Raster1" == 20) | ("Raster1" == 24), 0, 1)









