# if exists push home geocodes to Postgres, either shapefile or csv format

options(stringsAsFactors = TRUE)

# assume we have run "params" and "setup" scripts
# setups
source("tools/setup.R")

# shapefile input ---------------------------------------------------------
# data are expected to be a shapefile in 
# file.path(projectdir, "home_geocode/home_geocode_wave.shp")
# geometry in WGS84
# use shp2pgsql.exe
shp2pgsql <- function(shapefile){
    # a base command; make sure the postgres\version\bin is added to the system path
    cmd <- "cmd /c shp2pgsql.exe -s 4326 -d -g the_geom_4326 -I xxxSHPxxx xxxSCHEMAxxx.home_geocode | psql -h localhost xxxDBNAMExxx"
    # get the wave from the shapefile name
    wave <- gsub("home_geocode_|\\.shp", "", basename(shapefile))
    # get the dbname
    dbname <- dbGetInfo(dbconn)$dbname
    # reformat the command
    cmd <- gsub("xxxSCHEMAxxx", wave, gsub("xxxSHPxxx", shapefile, gsub("xxxDBNAMExxx", dbname, cmd)))
    # run the command
    message(cmd)
    O <- system(cmd, intern = TRUE)
    # geom in mysrid
    sql1 <- sprintf("select addgeometrycolumn('%s', 'home_geocode', 'the_geom_%s', %s, 'POINT', 2);", wave, mysrid, mysrid)
    message("adding the_geom_", mysrid)
    O <- dbGetQuery(dbconn, sql1)
    sql2 <- sprintf("update %s.home_geocode set the_geom_%s = st_transform(the_geom_4326, %s);", wave, mysrid, mysrid)
    message("updating the_geom_", mysrid)
    O <- dbGetQuery(dbconn, sql2)
}



# csv input ---------------------------------------------------------------
#### ---> after performing these steps, I converted to shapefile for editing in QGIS.
#### ---> home points were moved to the center of the home point cloud
#### ---> then re-imported to the db (renamed the original to y0.home_geocode_uncorrected)

push.home.geocodes <- function(csvfile){
    # read the USPS geocoded file or any file with WGS84 "longitude" and "latitude" fields
    geocodes <- read.csv(csvfile, as.is=TRUE)
    # standardize punctuations in columns
    colnames(geocodes) <- gsub("[[:punct:]]+", "_", colnames(geocodes))

    # write to db
    O <- dbWriteTable(dbconn, c("y0","home_geocode"), geocodes, row.names=FALSE, overwrite=TRUE)
    # geom
    O <- dbGetQuery(dbconn, "select addgeometrycolumn('y0','home_geocode','the_geom_4326',4326,'POINT',2);")
    O <- dbGetQuery(dbconn, "update y0.home_geocode set the_geom_4326 = st_setsrid(st_makepoint(longitude, latitude), 4326);")
    O <- dbGetQuery(dbconn, sprintf("select addgeometrycolumn('y0','home_geocode','the_geom_%s',%s, 'POINT',2);", mysrid, mysrid))
    O <- dbGetQuery(dbconn, sprintf("update y0.home_geocode set the_geom_%s = st_transform(the_geom_4326, %s);", mysrid, mysrid))
}
