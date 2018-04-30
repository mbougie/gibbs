library(ggplot2)
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

format_mil<-function(x){x/1000000}
format_bil<-function(x){x/1000000000}



####  important series   ##########################################################################################

###query the data from postgreSQL
df <- dbGetQuery(con, "SELECT year,sum(acres) as acres, description FROM counts_imw.merged_imw a INNER JOIN misc.lookup_mtr b using(mtr) WHERE mtr=1 or mtr=2 or mtr=5 GROUP BY year, mtr, description")


hi <- ggplot(df, aes(x=year, y=acres, group=description, color=description, ordered = TRUE)) +
  geom_line(size=0.40) +
  scale_linetype_manual(values=c("dashed"))+
  scale_y_continuous(labels=format_bil) +
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(y="Acreage in Billions",x="Years")+
  ggtitle('IMW Non-Conversion Classes') + theme(plot.title = element_text(hjust = 0.5)) +
  theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position = c(0.06, -0.26))   ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
  # scale_colour_manual(values = c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00", "#ff7f00"))

pdf("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\core\\imw\\imw_mtr1_2_5.pdf")
print(hi)
dev.off()
