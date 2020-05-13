library(tidyverse)
library(feather)
library(marmap)
library(gganimate)

setwd("~/Projects/San-Diego-IUU/")

ais <- read_feather("data/SanDiego_region1_2017-09-01_2017-10-01.feather")

nrow(dat)    #764,977,824

lon1 = -120
lon2 = -114
lat1 = 28
lat2 = 35 

# aisdat <- filter(dat, timestamp >= "2017-09-01 01:00:00" & timestamp <= "2017-09-02 01:00:00")
aisdat <- filter(aisdat, vessel_A_lat <= 34 & vessel_A_lon >= -120)

bat <- getNOAA.bathy(lon1, lon2, lat1, lat2, res = 1, keep = TRUE)

# # Creating a custom palette of blues
# blues <- c("lightsteelblue4", "lightsteelblue3",
# "lightsteelblue2", "lightsteelblue1")
# 
# # Plotting the bathymetry with different colors for land and sea
plot(bat, image = TRUE, land = TRUE, lwd = 0.1, bpal = list(c(0, max(bat), "grey"), c(min(bat), 0, blues)))

# # Making the coastline more visible
plot(bat, deep = 0, shallow = 0, step = 0, lwd = 0.4, add = TRUE)
# points(test$vessel_A_lon, test$vessel_A_lat)



p1 <- autoplot(bat, geom=c("raster"), coast = TRUE) + 
  scale_fill_gradientn(colours = c("lightsteelblue4", "lightsteelblue2"), 
                         values = rescale(c(-1000, -500, 0)),
                         guide = "colorbar",
                       limits = c(-5000, 0)) +
  coord_cartesian(expand = 0)+
  geom_point(data=filter(aisdat, vessel_A_lat <= 34 & vessel_A_lon >= -120), aes(vessel_A_lon, vessel_A_lat), size=0.5) + 
  # geom_point(data=NULL, aes(-117.1611, 32.7157), color="red") +    # San Diego Lat/Lon
  geom_point(data=NULL, aes(-117.2653, 32.9595), color="red") +    # Del Mar Lat/Lon
  annotate("text", x = -116.8, y = 32.9595, color="red", label="Del Mar") +
  theme(legend.position = "none") +
  labs(x="Longitude", y="Latitude", title = "{current_frame}") +
  
  transition_manual(frames = timestamp) +
  
  NULL

ap1 <- animate(p1, nframes = 200)

anim_save("figures/Sept2017_animation.gif", ap1)