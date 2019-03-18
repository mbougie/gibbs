
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
library(scales)
require(RColorBrewer)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "usxp_deliverables",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")



##########################  using supplied county dataset######################################   

#### get the built in state feature layer
state <- map_data("state") 

#### subset state dataset
# state_ss <- subset(state, region=='montana' | region=='wyoming' | region == 'north dakota'| region=='south dakota' | region == 'minnesota' | region=='iowa' | region == 'nebraska')
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
postgres_query <- dbGetQuery(con, "SELECT atlas_stco, acres_change_rot_cc as percent FROM synthesis_intensification.rfs_intensification_results_counties;")

## merge cnty2 with postgres_query
d = merge(cnty, postgres_query, sort = TRUE, by.x='fips', by.y='atlas_stco')

## If I don't reorder the order of lat long it "tears" the polygons!
cnty_fnl<-d[order(d$order),]


cnty_fnl$test = cut(cnty_fnl$percent, breaks= c(-Inf, -5000, 0, 5000, 10000, 20000, 40000, 80000))
cnty_fnl$test

# scale_fill_manual(values = brewer.pal(10, 'BrBG')[1:7])+
                  # labels = c(' ', '-10%', '-5%', '-2.5%', '0%'),# '2%', '5%', '10%', '15%', ' '),
                  # guide = guide_legend(
                  #   reverse = T,
                  #   title="Yield\nDifferential",
                  #   direction = "vertical",
                  #   title.position = "top",
                  #   label.position = "right",
                  #   label.hjust = 0,  #0.5
                  #   label.vjust = -0.5)) +  #1

formatAC<-function(x){x/10000}

d = ggplot() + 

  scale_fill_manual(values = rev(brewer.pal(10, 'BrBG')[1:7]),
                    labels = c('-0.5',' 0.0', ' 0.5', ' 1.0', ' 2.0',' 4.0',' 8.0'),# '2%', '5%', '10%', '15%', ' '),
                    
                    guide = guide_legend( title="Change in continuous corn (CC) due to the RFS \n(thousands of acres)", 
                                          title.theme = element_text(
                                                                    size = 14,
                                                                    # face = "italic",
                                                                    color = "#4e4d47",
                                                                    
                                                                    angle = 0
                                                                   ), 
                                           keyheight = unit(1.2, units = "mm"), 
                                           keywidth=unit(10, units = "mm"), 
                                           label.position = "bottom", 
                                           title.position = 'top', 
                                           nrow=1,
                                           ####these control the placement of the numeric values for the legend
                                           label.vjust=2,
                                          ncol=10,
                                          label.hjust=2.2
                                        ) 
                     ) +
   
  
  ### state grey background ###########
   geom_polygon(
    data=state,
    aes(y=lat, x=long, group=group),
    fill='#cccccc'
  )+
   
  ### county choropleth map ###########
  geom_polygon(
    data=cnty_fnl,
    # fill='grey70',
    aes(y=lat, x=long, group=group, fill = test),
    colour = '#cccccc',
    size = 0.5
  ) +
  
  ### state boundary strokes ###########
  geom_polygon(
    data=state,
    aes(y=lat, x=long, group=group),
    # fill='grey70',
    alpha=0,
    colour='white',
    size=0.5
  )+
  
  # geom_polygon(data = states.df, aes(x = long, y = lat, group = group), fill = '#cccccc', colour = NA) +
  coord_map(project="polyconic") +
  # labs(
  #   title = "Percent Abandoned by County"
  #   # subtitle = "For cumulative year 2015"
  # ) +
  theme(
    text = element_text(color = "#4e4d47", size=14),   ##these are the legend numeric values
    axis.text.x = element_blank(),
    axis.title.x=element_blank(),
    axis.text.y = element_blank(),
    axis.title.y=element_blank(),
    axis.ticks = element_blank(),
    panel.grid.major = element_blank(),
    plot.background = element_rect(fill = NA, color = NA),
    panel.background = element_rect(fill = NA, color = NA),
    # legend.background = element_rect(fill = "white", color = NA),
    # plot.title = element_text(size= 10, color = "#4e4d47", hjust=0.80, margin = margin(b = -15.75, t = 0.1, l = 0.01, unit = "cm")),
    # plot.subtitle = element_text(size= 10,vjust=0.5, hjust=0.55, color = "#4e4d47", margin = margin(b = -0.1, t = 0.20, l = 2, unit = "cm")),
    legend.position = c(0.10, -0.09)
  )

  
  
  d
  
  ggsave('C:/Users/Bougie/Desktop/temp/saved_image2.png', width = 18, height = 12, dpi = 800)
# d + guides(fill = guide_colorbar(reverse = TRUE, barwidth = 7, barheight = 0.5, title.position = 'top')) + theme(legend.direction = "horizontal")