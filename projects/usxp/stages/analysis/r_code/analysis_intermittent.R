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

formatAC<-function(x){x/1000000}

####  important series   ##########################################################################################

###query the data from postgreSQL
df <- dbGetQuery(con, "SELECT count, year, conv_type::text, acres::integer FROM counts_gen.merged_intermittent;")

##this reorders the labels in the legend in chronological order
# df$name <- with(df, reorder(name, -acres))
# jColors <- df$color
# names(jColors) <- df$name
# print(head(jColors))

yo = ggplot(df, aes(x=year, y=acres, group=conv_type, color=conv_type, ordered = TRUE)) +
  geom_line(size=0.80) +
  scale_linetype_manual(values=c("dashed"))+
  scale_y_continuous(labels=formatAC) +
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(y="Acreage in Millions",x="Years")+
  ggtitle('Intermittent by Year For MW') + theme(plot.title = element_text(hjust = 0.5)) +
  theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position = c(0.12, -0.18)) ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
  # scale_colour_manual(values = jColors)

pdf("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\analysis\\intermittent_tofrom.pdf")
print(yo)
dev.off()
