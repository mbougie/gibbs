library(ggplot2)
library(scales)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
drv <- dbDriver("PostgreSQL")



con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")


df <- dbGetQuery(con, "SELECT name,count,round(perc,2) as perc, color FROM counts_cdl.s9_ytc_fc_fnl2 Where perc > 1.5 and color IS NOT NULL and name != 'Grassland/Pasture' order by perc desc")




# df$name <- factor(df$name, levels = df$name[order(df$perc)])

##this reorders the labels in the legend in chronological order
df$name <- with(df, reorder(name, -perc))
print(df$name) 
jColors <- df$color
print(jColors)
names(jColors) <- df$name
print(head(jColors))

# The first step is specifying the basic form of the graph
# ggplot(data=df, aes(x=value, y=acres, fill=factor(value)))


ggplot(df, aes(name, perc, fill=name)) + 
  geom_bar(stat = "identity", width = 0.25)+
  theme(aspect.ratio = 1/3, 
        legend.position="none",
        axis.title.y=element_blank(), 
        axis.ticks.y=element_blank(),
        panel.background = element_blank())+
  labs(x="Acreage in Millions")+
  coord_flip()+
  
  geom_text(aes(label = paste0(perc,"%")), position=position_dodge(width=0.5), hjust= -0.4, vjust= 0.3, size=3, fontface="bold.italic") +
  scale_fill_manual(values = jColors) +
  scale_y_continuous(expand = c(0, 0), limits = c(0, 30)) 

