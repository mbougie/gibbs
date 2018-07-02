library(plyr)      # for join(...)
library(rgdal)     # for readOGR(...)
library(ggplot2)   # for fortify(...)




# oregon <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="tryit_dissolved")
# PG <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="tryit_dissolved")
# AG <- fortify(PG)




# shp <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="tryit_dissolved")
# AG <- fortify(shp)

# map <- ggplot() + geom_polygon(data = shp, aes(x = lat, y = long, group = group), colour = "black", fill = NA)

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




mapa <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="tryit_dissolved")

rownames(mapa@data)
mapa@data$id
mapa@data$id <- rownames(mapa@data)
mapa@data$id
mapa@data
# mapa@data   <- join(mapa@data, data, by="GRIDCODE")
mapa.df     <- fortify(mapa)
mapa.df     <- join(mapa.df,mapa@data, by="id")

# ggplot(mapa.df, aes(x=long, y=lat, group=group))+
#   geom_polygon(aes(fill=Population))+
#   coord_fixed()


ggplot() + geom_polygon(data=mapa.df, aes( x=long, y=lat, group=group), fill = 'grey70', colour = 'grey50', size = 0.25) + facet_wrap(~GRIDCODE)+
  theme(plot.title = element_text(colour = "steelblue",  face = "bold", family = "Helvetica", hjust = 0.5), 
        axis.text.x = element_blank(),
        axis.title.x=element_blank(),
        axis.text.y = element_blank(),
        axis.title.y=element_blank(),
        axis.ticks = element_blank(),
        panel.grid.major = element_blank(),
        legend.position="none")