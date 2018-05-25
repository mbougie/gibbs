# 
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

font_import()
print(fonts())

con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")

# df_1 <- dbGetQuery(con, "SELECT name, group_name, count, acres, round(perc::numeric,1) as perc, color FROM counts_cdl.s9_ytc_fc_fnl6 WHERE perc > 1.5  and name != 'Grassland/Pasture' or (name = 'Durum Wheat' and color IS NOT NULL) ")
df <- dbGetQuery(con, "SELECT a.value as year, acres, color_ytc, round(((acres/(select sum(acres) from counts_yxc.s22_ytc30_2008to2017_mmu5))*100),2) as perc FROM misc.lookup_yxc as a, counts_yxc.s22_ytc30_2008to2017_mmu5 as b WHERE a.value = b.value;")

format_mil<-function(x){x/1000000}
format_yo<-function(x){x*5}

# df$name <- factor(df$name, levels = df$name[order(df$perc)])

##this reorders the labels in the legend in chronological order
df$year <- with(df, reorder(year, year))
print(df$year)
jColors <- df$color_ytc
print(jColors)
names(jColors) <- df$year
print(head(jColors))


p <- ggplot(df, aes(x=year, y=acres, fill=year)) + 
  geom_bar(stat = "identity", width = 0.25)+
  theme(aspect.ratio = 1/3, 
        legend.position="none",
        axis.title.y=element_blank(), 
        axis.ticks.y=element_blank(),
        panel.background = element_blank())+
  labs(y="Acreage in Millions")+
  coord_flip()+
  
  geom_text(aes(label = paste0(perc,"%")), position=position_dodge(width=0.5), hjust= -0.4, vjust= 0.3, size=3, fontface="bold.italic")+

  scale_fill_manual(values = jColors)+
  scale_y_continuous(labels=format_mil, expand = c(0, 0), limits = c(0, 2500000)) 


# png('D:\\projects\\usxp\\current_deliverable\\5_23_18\\tr_tst2.png',bg = "transparent")
ggsave(p, filename = 'D:\\projects\\usxp\\current_deliverable\\5_23_18\\tr_tst3.png',  bg = "transparent")
print(p)
dev.off()
