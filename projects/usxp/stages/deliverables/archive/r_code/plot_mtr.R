# library(plyr)      # for join(...)
# library(rgdal)     # for readOGR(...)
# library(ggplot2)   # for fortify(...)
# 
# 
# 
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



# oregon <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="tryit_dissolved")
# PG <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="tryit_dissolved")
# AG <- fortify(PG)




shp <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="merged_ytc_6km")
# AG <- fortify(shp)

map <- ggplot() + geom_polygon(data = shp, aes(x = long, y = lat, group = group), colour = "black", fill = NA)

map

# d = ggplot() +
#        geom_polygon(data=AG, aes( x=long, y=lat, group=group), fill = 'grey70', colour = 'grey50', size = 0.25) + facet_wrap(~shp$GRIDCODE)
# 
# 
# 
# 
# d


# # not tested...
# library(plyr)      # for join(...)
# library(rgdal)     # for readOGR(...)
# library(ggplot2)   # for fortify(...)
# 
# mapa <- readOGR(dsn=".",layer="shapefile name w/o .shp extension")
# map@data$id <- rownames(mapa@data)
# mapa@data   <- join(mapa@data, data, by="CD_GEOCODI")
# mapa.df     <- fortify(mapa)
# mapa.df     <- join(mapa.df,mapa@data, by="id")
# 
# ggplot(mapa.df, aes(x=long, y=lat, group=group))+
#   geom_polygon(aes(fill=Population))+
#   coord_fixed()


# mapa <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="fn_6km")
# 
# # mapa <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="fn_100km")
# 
# mapa.df <- fortify(mapa)
# 
# 
# print(mapa.df['id'])
# # ## query the data from postgreSQL
# query <- dbGetQuery(con, "SELECT label::int as year, LTRIM(variable,'FID_') as id,value FROM deliverables.zh_6km WHERE value > 0.1;")
# 
# d = merge(x = mapa.df, y = query, by.x = "id", by.y='id', all.y = TRUE)
# 
# print(d)
# 
# cnty_fnl<-d[order(d$order),]
# 
# rr = (ggplot() +
#        geom_polygon(data=cnty_fnl, aes(y=lat, x=long, group=group,  fill = value), colour = 'grey50', size = 0.25) + facet_wrap(~year)+ theme(strip.text.x = element_text(size = 8, colour = "steelblue", face = "bold.italic")) +
#       
#        theme(plot.title = element_text(colour = "steelblue",  face = "bold", family = "Helvetica", hjust = 0.5),
#              axis.text.x = element_blank(),
#              axis.title.x=element_blank(),
#              axis.text.y = element_blank(),
#              axis.title.y=element_blank(),
#              axis.ticks = element_blank(),
#              panel.grid.major = element_blank(),
#              legend.position="none"))
# 
# rr + ggtitle("Percent Conversion to Crop (6 km)")
# 
# 











# # 
# # ## If I don't reorder the order of lat long it "tears" the polygons!
# # cnty_fnl<-d[order(d$order),]
# 
# # ggplot(mapa.df, aes(x=long, y=lat, group=group))+
# #   geom_polygon(aes(fill=Population))+
# #   coord_fixed()
# 
# 
# ggplot() + geom_polygon(data=mapa.df, aes( x=long, y=lat, group=group), fill = 'grey70', colour = 'grey50', size = 0.25) + facet_wrap(~oid_)+
#   theme(plot.title = element_text(colour = "steelblue",  face = "bold", family = "Helvetica", hjust = 0.5), 
#         axis.text.x = element_blank(),
#         axis.title.x=element_blank(),
#         axis.text.y = element_blank(),
#         axis.title.y=element_blank(),
#         axis.ticks = element_blank(),
#         panel.grid.major = element_blank(),
#         legend.position="none")
# 
# 
# 


# ##########################  using supplied county dataset######################################   
# state <- map_data("state") 
# ##subset state dataset
# state_ss <- subset(state, region=='montana' | region=='wyoming' | region == 'north dakota'| region=='south dakota' | region == 'minnesota' | region=='iowa' | region == 'nebraska')
# 
# ##create counties dataset
# cnty <- map_data("county")
# 
# ##get the fips code
# data(county.fips)
# 
# ## attach fips code to counties dataset
# cnty2 <- cnty %>%
#   mutate(polyname = paste(region,subregion,sep=",")) %>%
#   left_join(county.fips, by="polyname")
# 
# ## query the data from postgreSQL 
# yo <- dbGetQuery(con, "SELECT * FROM public.testunion2;")
# 
# ## merge cnty2 with yo
# d = merge(cnty2, yo, sort = TRUE, by.x='fips', by.y='atlas_stco')
# 
# ## If I don't reorder the order of lat long it "tears" the polygons!
# cnty_fnl<-d[order(d$order),]
# 
# 
# d = (ggplot() +
#        geom_polygon(data=cnty_fnl, aes(y=lat, x=long, group=group), fill = 'grey70', colour = 'grey50', size = 0.25) + facet_wrap(~year)+ theme(strip.text.x = element_text(size = 8, colour = "steelblue", face = "bold.italic")) +
#        geom_polygon(data=state_ss, aes(y=lat, x=long, group=group), fill = 'grey70', alpha=0, colour = 'white', size = 0.25)+
#        coord_map(project="polyconic") +
#        theme(plot.title = element_text(colour = "steelblue",  face = "bold", family = "Helvetica", hjust = 0.5), 
#              axis.text.x = element_blank(),
#              axis.title.x=element_blank(),
#              axis.text.y = element_blank(),
#              axis.title.y=element_blank(),
#              axis.ticks = element_blank(),
#              panel.grid.major = element_blank(),
#              legend.position="none"))
# 
# d + ggtitle("County CLU Datasets by Year")
# 

