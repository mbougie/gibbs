
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
library(scales)
drv <- dbDriver("PostgreSQL")

# con <- dbConnect(drv, dbname = "glbrc",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")



##########################  using supplied county dataset######################################   

#### get the built in state feature layer
state <- map_data("state") 

#### subset state dataset
state_ss <- subset(state, region=='montana' | region=='wyoming' | region == 'north dakota'| region=='south dakota' | region == 'minnesota' | region=='iowa' | region == 'nebraska')
# state_ss <- subset(state, region == 'north dakota')

#### create counties dataset from buit in dataset
cnty_intial <- map_data("county")

#### get the fips code
data(county.fips)

#### attach fips code to counties dataset
cnty <- cnty_intial %>%
  mutate(polyname = paste(region,subregion,sep=",")) %>%
  left_join(county.fips, by="polyname")

## query the data from postgreSQL 
# postgres_query <- dbGetQuery(con, "SELECT atlas_stco, percent FROM counts.abandoned_nd;")
postgres_query <- dbGetQuery(con, "SELECT 
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
## merge cnty2 with postgres_query
d = merge(cnty, postgres_query, sort = TRUE, by.x='fips', by.y='atlas_stco')

## If I don't reorder the order of lat long it "tears" the polygons!
cnty_fnl<-d[order(d$order),]


d = ggplot() + 
  geom_polygon(
    data=cnty_fnl,
    aes(y=lat, x=long, group=group, fill = percent),
    colour = 'grey50',
    size = 0.25
  ) + 
  scale_fill_distiller(name="Percent", palette = "Greens", breaks = pretty_breaks(n = 5), trans = "reverse")+
  # scale_fill_continuous(name="percent", trans = "reverse") +
  geom_polygon(
    data=state,
    aes(y=lat, x=long, group=group),
    fill='grey70',
    alpha=0,
    colour='white',
    size=0.9
  )+
  coord_map(project="polyconic") +
  labs(
    title = "Percent Abandoned by County",
    subtitle = "For cumulative year 2015"
  ) +
  theme(
    text = element_text(color = "#22211d"), 
    axis.text.x = element_blank(),
    axis.title.x=element_blank(),
    axis.text.y = element_blank(),
    axis.title.y=element_blank(),
    axis.ticks = element_blank(),
    panel.grid.major = element_blank(),
    plot.background = element_rect(fill = "white", color = NA), 
    panel.background = element_rect(fill = "white", color = NA), 
    legend.background = element_rect(fill = "white", color = NA),
    plot.title = element_text(size= 15, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.1, l = 0.01, unit = "cm")),
    plot.subtitle = element_text(size= 10, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.20, l = 2, unit = "cm")),
    legend.position = c(0.20, 0.35)
  )

d + guides(fill = guide_colorbar(reverse = TRUE, barwidth = 7, barheight = 0.5, title.position = 'top')) + theme(legend.direction = "horizontal")