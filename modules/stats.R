# data processing
library(ggplot2)
# spatial
library(raster)
library(rasterVis)
library(rgdal)
library(viridis)
require(RColorBrewer)
library(ggmap)
library(sos)
library(spatstat)



###read in csv
df <- read.csv(file='C:\\Users\\Bougie\\Desktop\\current_stats.csv',head=TRUE,sep=",")
df = subset(df, value<=100)


df_yo <- df[rep(df$value, df$count), 1:2]
df_yo <- na.omit(df_yo)

median_yo = median(df_yo$X)
mean_yo = mean(df_yo$X)

d <- ggplot(data = df_yo, aes(x = "", y = X)) +
  geom_boxplot() + 
  coord_flip() + 
  theme_bw() + 
  scale_y_continuous(breaks=c(1,median_yo,5,11,62)) +
  labs(y="Percent conversion (noncrop to crop)",x="",color="none",caption="Note: comparison at 9km squared")+
  geom_hline(yintercept = median_yo, color="blue", linetype="dashed", size=1)
d 
summary(df_yo)















#####weighted stuff ################################################
# w_mean <- weighted.mean(df$value, df$count)
# w_median <- weighted.median(df$value, df$count)


# Basic histogram
p <- ggplot(df) + 
  geom_bar(aes(x=value, y=count), stat="identity") + 
  scale_x_continuous(breaks=c(as.integer(median_yo),as.integer(mean_yo),11,62)) + 
  labs(x="Percent conversion (noncrop to crop)",y="number of 9km squared blocks",color="none",caption="Note: comparison at 9km squared")+

geom_vline(aes(xintercept=median_yo),
              color="red", linetype="dashed", size=1) + 

geom_vline(aes(xintercept=mean_yo),
           color="blue", linetype="dashed", size=1)
p

















# Change the width of bins
ggplot(df, aes(x=value)) + 
  geom_histogram(binwidth=1)
# Change colors
p<-ggplot(df, aes(x=value)) + 
  geom_histogram(color="black", fill="white")
p

# Add mean line
p+ geom_vline(aes(xintercept=mean(value)),
              color="blue", linetype="dashed", size=1)
# Histogram with density plot
ggplot(df, aes(x=value)) + 
  geom_histogram(aes(y=..density..), colour="black", fill="white")+
  geom_density(alpha=.2, fill="#FF6666") 

