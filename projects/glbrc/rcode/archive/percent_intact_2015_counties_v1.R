
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "glbrc",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



##########################  using supplied county dataset######################################   
state <- map_data("state") 
##subset state dataset
state_ss <- subset(state, region=='montana' | region=='wyoming' | region == 'north dakota'| region=='south dakota' | region == 'minnesota' | region=='iowa' | region == 'nebraska')

##create counties dataset
cnty <- map_data("county")

##get the fips code
data(county.fips)

## attach fips code to counties dataset
cnty2 <- cnty %>%
  mutate(polyname = paste(region,subregion,sep=",")) %>%
  left_join(county.fips, by="polyname")

# ## query the data from postgreSQL 
# yo <- dbGetQuery(con, "SELECT 
#                          zonal_hist_formatted.atlas_stco, 
#                          counties.atlas_name, 
#                          sum(zonal_hist_formatted.acres), 
#                          counties.acres_calc,
#                          (sum(zonal_hist_formatted.acres)/counties.acres_calc)*100 as percent
#                        FROM 
#                          spatial.counties, 
#                          new.zonal_hist_formatted
#                        WHERE 
#                          zonal_hist_formatted.atlas_stco = counties.atlas_stco
#                        GROUP BY
#                          zonal_hist_formatted.atlas_stco, 
#                          counties.atlas_name,
#                          counties.acres_calc")


yo <- dbGetQuery(con, "SELECT 
                 abandon_cdl_counties.atlas_stco, 
                 counties.atlas_name, 
                 sum(abandon_cdl_counties.acres), 
                 counties.acres_calc,
                 (sum(abandon_cdl_counties.acres)/counties.acres_calc)*100 as percent
                 FROM 
                 spatial.counties, 
                 new.abandon_cdl_counties
                 WHERE 
                 abandon_cdl_counties.atlas_stco = counties.atlas_stco
                 GROUP BY
                 abandon_cdl_counties.atlas_stco, 
                 counties.atlas_name,
                 counties.acres_calc")

## merge cnty2 with yo
d = merge(cnty2, yo, sort = TRUE, by.x='fips', by.y='atlas_stco')

## If I don't reorder the order of lat long it "tears" the polygons!
cnty_fnl<-d[order(d$order),]


ggplot() +
  geom_polygon(data=cnty_fnl, aes(y=lat, x=long, group=group, fill = percent), colour = 'grey50', size = 0.25) + 
  geom_polygon(data=state_ss, aes(y=lat, x=long, group=group), fill = 'grey70', alpha=0, colour = 'white', size = 0.25)+
  coord_map(project="polyconic") +
  scale_fill_continuous(trans = 'reverse') + 
  theme(plot.title = element_text(colour = "steelblue",  face = "bold.italic", family = "Helvetica", hjust = 0.5),
        axis.text.x = element_blank(),
        axis.title.x=element_blank(),
        axis.text.y = element_blank(),
        axis.title.y=element_blank(),
        axis.ticks = element_blank(),
        panel.grid.major = element_blank())

# d + guides(fill = guide_colorbar(reverse = TRUE, barwidth = 7, barheight = 0.5, title.position = 'top')) + theme(legend.direction = "horizontal")
