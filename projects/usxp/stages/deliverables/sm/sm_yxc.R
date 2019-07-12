# library(extrafont)
# extrafont::loadfonts(device="win")
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)

drv <- dbDriver("PostgreSQL")

# font_import(pattern = 'ARIALN')
# font_import(pattern = 'ARIALNB')

con2 <- dbConnect(drv, dbname = "usxp",
                  host = "144.92.235.105", port = 5432,
                  user = "mbougie", password = "Mend0ta!")


s35_ytc <- dbGetQuery(con2,"SELECT b.st_abbrev as state,a.value::int as year,a.acres::int, a.atlas_st::int as rank
                            FROM sm.s35_ytc_hist as a, spatial.states as b 
                            WHERE b.atlas_st = a.atlas_st
                            
                            UNION
                            
                            SELECT 
                            'CONUS'::text as state,
                            value as year,
                            acres/10,
                            100::integer as rank
                            FROM 
                            counts_yxc.s35_ytc30_2008to2017_mmu5")

s35_yfc <- dbGetQuery(con2,"SELECT b.st_abbrev as state,a.value::int as year,a.acres::int, a.atlas_st::int as rank
                            FROM sm.s35_yfc_hist as a, spatial.states as b 
                            WHERE b.atlas_st = a.atlas_st
                            
                            UNION
                            
                            SELECT 
                            'CONUS'::text as state,
                            value as year,
                            ---NOTE:Divide acres by 10 to maintain scale consistency!!!
                            acres/10,
                            100::integer as rank
                            FROM 
                            counts_yxc.s35_yfc30_2008to2017_mmu5")

#Abandonment
formatAC<-function(x){x/1000000}
# color2011<-ifelse(df25$year==2010,"turquoise","black")

# x$name <- factor(x$name, levels = x$name[order(x$val)])

####change the ranking of state based on rank column for BOTH dataframes
s35_ytc <- mutate(s35_ytc, state = reorder(state, rank))
print(s35_ytc$state)

s35_yfc <- mutate(s35_yfc, state = reorder(state, rank))
print(s35_yfc$state)




plotResult<-ggplot(NULL, aes(x=year, group=state)) +
  geom_line(data = s35_ytc, aes(y=acres,color="tomato2"),alpha=0.8)+
  geom_line(data = s35_yfc, aes(y=acres,color="turquoise"),alpha=0.8)+
  scale_y_continuous(
    labels=formatAC,
    "Expansion(acres in mil)", 
    sec.axis = sec_axis(~ . * 1.20, name = "Abandonment(acres in mil)", labels=formatAC)
  )+
  scale_x_continuous(breaks=c(2008,2009,2010,2011,2012,2013,2014,2015,2016,2017))+
  labs(y="Expansion(acres in mil)",x="Years",color="none",caption="NOTE: CONUS conversion values were divided by 10 to maintain scale consistency.")+
  facet_wrap(~ state) +
  ggtitle("Comparison Between Cropland Expansion and Abandonment 2008-2016")+
  theme(strip.text.x = element_text(size = 7, margin = margin(1,0,1,0, "mm")),
        axis.ticks = element_blank(),
        axis.text.x = element_text(size=6,angle=90),
        axis.text.y = element_text(size=6,vjust=0),
        axis.title.y=element_text(color="tomato"),
        axis.title.y.right=element_text(color="turquoise"),
        panel.grid.minor.x=element_blank(),
        # panel.background = element_rect(size = 5),
        plot.title = element_text(hjust = 0.5),
        plot.caption = element_text(hjust = 0.5),
        legend.position ="None"
  )

plotResult
# ###create panel image ######################
dir = "C:\\Users\\Bougie\\Desktop\\temp\\"
fileout=paste(dir,"s35_yxc_sm",".png", sep="")
ggsave(fileout, width = 20, height = 20, dpi = 800)




