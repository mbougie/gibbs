import arcpy
from arcpy import env
from arcpy.sa import *
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import os
import glob
import sys



sys.path.append('C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\modules\\')
import general as gen


def addGDBTable2postgres_histo_county(pgdb, schema, currentobject):
    print 'addGDBTable2postgres_histo..................................................'
    print currentobject

    ##set the engine.....
    engine = create_engine('postgresql://mbougie:Mend0ta!@144.92.235.105:5432/{}'.format(pgdb))

    # Execute AddField twice for two new fields
    fields = [f.name for f in arcpy.ListFields(currentobject)]

    # converts a table to NumPy structured array.
    arr = arcpy.da.TableToNumPyArray(currentobject,fields)
    print arr


    #### convert numpy array to pandas dataframe
    df = pd.DataFrame(data=arr)

    del df['OBJECTID']

    ##perform a psuedo pivot table
    df=pd.melt(df, id_vars=["LABEL"],var_name="atlas_stco", value_name="count")

    df.columns = map(str.lower, df.columns)

    print df
    
    #### format column in df #########################
    ## strip character string off all cells in column
    df['atlas_stco'] = df['atlas_stco'].map(lambda x: x.strip('ATLAS_'))
    ## remove comma from year
    # df['value'] = df['label'].str.replace(',', '')

    print df


    print 'pixel conversion:', gen.getPixelConversion2Acres(30)

    ####add column 
    df['acres'] = df['count']*gen.getPixelConversion2Acres(30)

    tablename = currentobject.split('\\')[-1]
    print 'tablename', tablename

    print df

    df.to_sql(tablename, engine, schema=schema)

    # MergeWithGeom(df, tablename, eu, eu_col)




def reclass():
    reclasslist = [[63,1],[141,1],[142,1],[143,1],[83,2],[87,2],[190,2],[195,2],[37,3],[62,3],[171,3],[176,3],[181,3],[64,4],[65,4],[131,4],[152,4]]
    inraster = 'D:\\projects\\glbrc\\temp.gdb\\fp_raster_cdl'
    outraster = 'D:\\projects\\glbrc\\temp.gdb\\abandonment'
    outReclass = Reclassify(inraster, "Value", RemapValue(reclasslist), "NODATA")
    outReclass.save(outraster)





def applyMMU(mmu):

    # Set the workspace environment to local file geodatabase
    arcpy.env.workspace = "D:\\projects\\glbrc\\temp.gdb" 
   

    rg_combos = {'4w':["FOUR", "WITHIN"], '8w':["EIGHT", "WITHIN"], '4c':["FOUR", "CROSS"], '8c':["EIGHT", "CROSS"]}
    
    
    for acres, count in mmu.iteritems():

        print acres, count


        for key, value in rg_combos.iteritems():

            print key, value

            ###########  count 14 is ~ 3 acres
            cond = "Count < {}".format(count)
            print 'cond: ',cond

            
            # raster_rg = RegionGroup(raster_filter, rg_instance[0], rg_instance[1],"NO_LINK")
            raster_rg = RegionGroup('abandonment', value[0], value[1], "NO_LINK")
            raster_mask = SetNull(raster_rg, 'abandonment', cond)

            outraster= 'abandonment_{}_{}'.format(key, acres)
            print 'outraster', outraster
            raster_mask.save(outraster)


            # Overwrite pyramids
            gen.buildPyramids(outraster)

            outraster = None



########  STEPS  ##########################################

#### 1.1) take inverse of intactlands by using erase function to get suitable lands

#### 1.2) import attribute table from suitable lands feature class into postgres
# gen.addGDBTable2postgres_fc(gdb='D:\\projects\\intactland\\intact_clu\\temp.gdb', pgdb='glbrc', schema='new', table='glbrc')

#### 2.1) get landcover composition by of the suitable land using zonal histogram

#### 2.2) import zonal histogram into postgres
addGDBTable2postgres_histo_county(pgdb='glbrc', schema='new', currentobject='D:\\projects\\glbrc\\abandonment.gdb\\zonal_test')

### run sql to derive products ------NOTE: remember to create the label column from the objectid column!!




#########  reclassify polygon to raster  ########################

# reclass()


# applyMMU()



# mmu = {'mmu3':'14', 'mmu5':'23'}
# applyMMU(mmu)