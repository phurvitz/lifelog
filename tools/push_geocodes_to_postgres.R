## ---- geocodes

# creates PostGIS points from a USPS geocoded file
options(stringsAsFactors = TRUE)

# assume we have run "params" and "setup" scripts


#### ---> after performing these steps, I converted to shapefile for editing in QGIS.
#### ---> home points were moved to the center of the home point cloud
#### ---> then re-imported to the db (renamed the original to y0.home_geocode_uncorrected)


push.home.geocodes <- function(csvfile){
    # read the USPS geocoded file or any file with WGS84 "longitude" and "latitude" fields
    geocodes <- read.csv(csvfile, as.is=TRUE)
    # standardize punctuations in columns
    colnames(geocodes) <- gsub("[[:punct:]]+", "_", colnames(geocodes))
    # rename the unique ID column (which I made for specifying the residential sequential ID and measurement wave)
    colnames(geocodes)[1] <- "idseqwave"
    # parse the id into its chunks
    ids <- (do.call(rbind, strsplit(geocodes$id, split = "_")))
    # parse the latlong into its chunks
    latlong <- (do.call(rbind, strsplit(geocodes$latitude_longitude, split = ",")))
    
    # make a data frame from the chunks and the original USPS data
    addresses <- data.frame(id=ids[,1], idseq=ids[,2], wave=ids[,3], latitude=as.numeric(latlong[,2]), longitude=as.numeric(latlong[,1]), geocodes)
    
    addresses$id <- gsub("[[:punct:]]", "_", addresses$id)
    
    # write to db
    O <- dbWriteTable(dbconn, c("y0","home_geocode"), addresses, row.names=FALSE, overwrite=TRUE)
    # geom
    O <- dbGetQuery(dbconn, "select addgeometrycolumn('y0','home_geocode','the_geom_4326',4326,'POINT',2);")
    O <- dbGetQuery(dbconn, "update y0.home_geocode set the_geom_4326 = st_setsrid(st_makepoint(longitude, latitude), 4326);")
    O <- dbGetQuery(dbconn, sprintf("select addgeometrycolumn('y0','home_geocode','the_geom_%s',%s, 'POINT',2);", mysrid, mysrid))
    O <- dbGetQuery(dbconn, sprintf("update y0.home_geocode set the_geom_%s = st_transform(the_geom_4326, %s);", mysrid, mysrid))
}