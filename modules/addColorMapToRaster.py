import arcpy
import general as gen

arcpy.env.workspace = 'D:\\projects\\usxp\\deliverables\\s35\\s35.gdb'

in_raster = 's35_mtr'

clr = 'C:\\Users\\Bougie\\Desktop\\misc\\colormaps\\mtr.clr'

arcpy.AddColormap_management(in_raster=in_raster, input_CLR_file=clr)

# gen.buildPyramids(in_raster)