
# library(ggplot2)
# library(scales)
# library(maps)
# library(rgdal)# R wrapper around GDAL/OGR
# library(sp)
# require("RPostgreSQL")
# library(plyr)
# library(dplyr)
# library(viridis)
# library(extrafont)
# drv <- dbDriver("PostgreSQL")
# 
# font_import()
# print(fonts())

# con <- dbConnect(drv, dbname = "usxp",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")

# df_1 <- dbGetQuery(con, "SELECT name, group_name, count, acres, round(perc::numeric,1) as perc, color FROM counts_cdl.s9_ytc_fc_fnl6 WHERE perc > 1.5  and name != 'Grassland/Pasture' or (name = 'Durum Wheat' and color IS NOT NULL) ")
df <- dbGetQuery(con, "SELECT name, group_name, count_test, acres, round(perc::numeric,1) as perc, color FROM counts_cdl.s9_ytc_fc_fnl7 WHERE perc > 1.5  and name != 'Grassland/Pasture' or (name = 'Durum Wheat' and color IS NOT NULL) ")

format_mil<-function(x){x/1000000}
format_yo<-function(x){x*5}

# df$name <- factor(df$name, levels = df$name[order(df$perc)])

##this reorders the labels in the legend in chronological order
df$name <- with(df, reorder(name, -acres))
print(df$name) 
jColors <- df$color
print(jColors)
names(jColors) <- df$name
print(head(jColors))


## this is used to sort the bars by group name
df2 <- aggregate(df$perc, by=list(group_name=df$group_name), FUN=sum)
print(df2)

total <- merge(df,df2,by="group_name")
total$group_name <- with(total, reorder(group_name, -x))
print(total)

total2 <- total[!duplicated(total[,c('group_name','x')]),]
print(total2)

ggplot(total, aes(x=group_name, y=acres, fill=name)) + 
  geom_bar(stat = "identity", width = 0.25)+
  theme(aspect.ratio = 1/3, 
        legend.position="none",
        axis.title.y=element_blank(), 
        axis.ticks.y=element_blank(),
        panel.background = element_blank())+
  labs(y="Acreage in Millions")+
  coord_flip()+
  
  geom_text(aes(label = paste0(perc,"%"), label=(acres)), position=position_dodge(width=0.5), hjust= -0.4, vjust= 0.3, size=3, fontface="bold.italic") +

  scale_fill_manual(values = jColors)+
  scale_y_continuous(labels=format_mil, expand = c(0, 0), limits = c(0, 3000000)) 

