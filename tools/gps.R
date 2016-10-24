## ---- gps
# read in the GPS data

library(RPostgreSQL)
#library(data.table)

# ---> assumes "setup.R" has been modified <---
# loads setups, including projectdir, dbname, etc.
source("tools/setup.R")

#========
# run this as `qstarz.to.db.all(mywave='y0')'
#   or `qstarz.to.db.all()' [see arguments below]
#========

# qstarz.to.df ------------------------------------------------------------
# a function for processing one raw Qstarz file to a data frame, ASSUMED TO BE CALLED BY qstarz.to.dfs for multiples
qstarz.to.df <- function(fname, wave, gpss, where="", timezone){
    # get the ID for this file name
    id <- sqldf(sprintf("select id from gpss where fname = '%s' and wave = '%s';", fname, wave))$id
    message(sprintf("reading in %s ....", fname))
    # read data
    gps <- read.csv(fname, as.is=T, strip.white=T)
    # munge the column names
    colnames(gps) <- gsub("_$", "", gsub("[[:punct:]][[:punct:]]*", "_", tolower(colnames(gps))))
    # drop null records
    gps <- gps[grep("-", gps$utc_date, invert=T),]

    # reformat lat & long
    gps$latitude <- ifelse(gps$n_s=="S", -gps$latitude, gps$latitude)
    gps$longitude <- ifelse(gps$e_w=="W", 0 - gps$longitude, gps$longitude)
    # some points are not valid
    gps <- gps[gps$valid != "NO FIX",]
    # add the subject ID
    gps <- data.frame(id, gps)
    # does this meet specification?
    if(any(grepl("altitude", names(gps)))){
        colnames(gps)[grep("altitude", colnames(gps))] <- "height"
        colnames(gps)[grep("track.id", colnames(gps))] <- "rcr"
        gps$nsat_used <- "NA/NA"
        # drop unnecessar columns
        gps$g.x <- gps$g. <- gps$g.z <- NULL
        # add "placeholder" columns
        gps$dsta <- gps$dage <- gps$pdop <-gps $vdop <-gps $hdop <- gps$distance_m <- NA
    }
    # reformat height, speed, distance, nsat used, nsat view: remove units from values and add to field names
    gps$height <- as.numeric(sub("M", "", gps$height))
    colnames(gps)[grep("height", colnames(gps))] <- "height_m"
    gps$speed <- as.numeric(sub("km/h", "", gps$speed))
    colnames(gps)[grep("speed", colnames(gps))] <- "speed_kmh"
    if(any(grepl("distance", colnames(gps)))){
        gps$distance <- sub("M", "", gps$distance)
        colnames(gps)[grep("distance", colnames(gps))] <- "distance_m"
    }
    
    # split nsat into view and used
    if(any(grepl("nsat_", colnames(gps)))){
        nsat <- strsplit(gsub(" ", "", gps$nsat_used_view), split="/")
        gps$nsat_used <- as.integer(do.call(c, lapply(nsat, function(x) x[1])))
        gps$nsat_view <- as.integer(do.call(c, lapply(nsat, function(x) x[2])))
        gps$nsat_used_view <- NULL
    }

    # handle timestamps: create a single UTC full timestamp
    gps$time_gps <- as.POSIXct(sprintf("%s %s", gps$utc_date, gps$utc_time), format="%Y/%m/%d %H:%M:%S", tz="UTC")
    # convert to local
    attr(gps$time_gps, "tzone") <- timezone

    # Julian day, 3 AM change
    gps$jdaya_gps <- as.integer(sqldf("select extract(doy from time_gps - interval '3 hours') as jdaya_gps from gps;")$jdaya_gps)

    # drop records from 1980
    gps <- gps[format(gps$time_gps, "%Y")!="1980",]
    # source
    gps$gps_src <- fname
    
    # missing distance.m?
    if(!any(grepl("distance_m", colnames(gps)))){
        gps$distance.m <- NA
    } else {
        gps$distance_m <- as.numeric(gps$distance_m)
    }
assign("gpsx", gps, envir = .GlobalEnv)
    # standardize column name positions
    column_names_pat <- "id|index|rcr|utc_date|utc_time|local_date|local_time|ms|valid|latitude|n_s|longitude|e_w|height_m|speed_kmh|heading|dsta|dage|pdop|hdop|vdop|nsat_used|nsat_view|distance_m|time_gps|gps_src|jdaya_gps"
    gpscols <- grep(column_names_pat, colnames(gps))
    gps <- gps[,gpscols]
    # return a data frame of the GPS
    return(gps)
}

# qstarz.to.dfs -----------------------------------------------------------
# creates a single data frame from multiple files from a subject
qstarz.to.dfs <- function(id, gpss=gpss, wave, timezone=timezone, where="", verbose=F){
    idfiles <- sqldf(sprintf("select fname from gpss where id = '%s' and wave = '%s';", id, wave))
    # what am I doing?
    message("processing ", id)
    # an empty list for individual files
    list.gps <- list()
    # process each GPS file for this id
    for(j in idfiles){
        # make the GPS data frame
        gps.id <- qstarz.to.df(fname=j, wave = wave, gpss = gpss, timezone = timezone, where=where)
        # add it to the list
        list.gps[[j]] <- gps.id
    } 
    assign("list.gps", list.gps, envir=.GlobalEnv)
    # kill time overlaps if necessary from multiple files
    dat <- kill.collisions(list.gps)

    # add the recnum_gps
    dat$recnum_gps <- 1:nrow(dat)
    
    # set the id attribute on the data frame
    attributes(dat)$id <- id
    # drop row names
    rownames(dat) <- NULL
    
    return(dat)
}

# qstarz.to.db ------------------------------------------------------------
# read the GPS from the raw file and write to the db, make points
qstarz.to.db <- function(conn, id, gpss, wave, timezone, where="", overwrite=FALSE, srid=26982){
    # table name
    tname <- sprintf("gps_%s", id)
    
    message(tname)

    # deal with overwrite
    if(dbGetQuery(conn, sprintf("select count(*) from information_schema.tables where table_schema = '%s' and table_name = '%s';", wave, tname))$count == 1){
        if(overwrite){
            message("dropping ", tname)
            O <- dbGetQuery(conn, sprintf("DROP TABLE IF EXISTS %s.%s CASCADE;", wave, tname))
        } else {
            message(sprintf("%s already exists; run with overwrite=TRUE if you want to overwrite", tname))
            return(invisible())
        }
    }
    
    # create a data frame from all files for this id
    dat <- qstarz.to.dfs(id=id, gpss=gpss, wave=wave, timezone=timezone)
    # deal with time stamp time zone
    dat$time_gps <- force_tz(dat$time_gps, timezone)
    dat$time_gps <- format(dat$time_gps, format="%F %T %z")
    # write to the db
    O <- dbWriteTable(conn, c(wave, tname), dat, append=T, row.names=F)
    # force the timestamptz
    dbGetQuery(conn, statement = sprintf("alter table %s.%s alter COLUMN time_gps set data type timestamptz USING time_gps::timestamptz", wave, tname))
    
    # add WGS84 and state plane geometry values
    message("creating spatial points")
    O <- dbGetQuery(conn, sprintf("select addgeometrycolumn('%s', '%s', 'the_geom_4326', 4326, 'POINT', 2);", wave, tname))
    O <- dbGetQuery(conn, sprintf("update %s.%s set the_geom_4326 = st_setsrid(st_makepoint(longitude, latitude), 4326);", wave, tname))
    O <- dbGetQuery(conn, sprintf("create index idx_%s_the_geom_4326 on %s.%s using gist(the_geom_4326);", tname, wave, tname))
    
    O <- dbGetQuery(conn, sprintf("select addgeometrycolumn('%s', '%s', 'the_geom_%s', %s, 'POINT', 2);", wave, tname, srid, srid))
    O <- dbGetQuery(conn, sprintf("update %s.%s set the_geom_%s = st_transform(the_geom_4326, %s);", wave, tname, srid, srid))  
    O <- dbGetQuery(conn, sprintf("create index idx_%s_the_geom_%s on %s.%s using gist(the_geom_%s);", tname, srid, wave, tname, srid))
    
    # check to see if wave.home_geocode exits
    if(dbGetQuery(conn = conn, statement = sprintf("select count(*) = 1 as geocode_existss from information_schema.tables where table_schema = '%s' and table_name = 'home_geocode';", wave))$geocode_exists){
        dist.to.home.one(conn = conn, wave = wave, id = id, force = TRUE)
    }
    
}

# qstarz.to.db.all ---------------------------------------------------------------
# assuming wd is a folder with many csv files
qstarz.to.db.all <- function(myconn = dbconn, mygpss = gpss, mywave, mytimezone = timezone, overwrite=F, where="", startid){   #WHERE valid='DGPS' AND hdop < 5", verbose=F){
    # a list of GPS raw files 
    gpsids <- sqldf("select distinct id from gpss order by id;")$id
    # extract only the subject id part of the file names
    # start id?
    if(!missing("startid")){
        pos <- grep(startid, gpsids) + 1
        gpsids <- gpsids[pos:length(gpsids)]
    }
    # print(where)
    # for each file
    O <- lapply(gpsids, function(x) qstarz.to.db(conn=myconn, id = x, wave = mywave, gpss = mygpss, timezone = mytimezone, overwrite=overwrite, where=where))
}


# distance to home --------------------------------------------------------
dist.to.home.one <- function(conn = dbconn, wave, id, force = FALSE){
    # get the tablename
    tablename <- sprintf("gps_%s", id)
    message("\n",id)
    # if the column doesn't exist, create
    if(!colExists(conn = conn, tableschema = wave, tablename = tablename, colname = "dist_to_home_m")){
        message(sprintf("adding dist_to_home_m to %s.%s", wave, tablename))
        O <- dbGetQuery(conn = conn, sprintf("alter table %s.%s add column dist_to_home_m float;", wave, tablename))
        sql <- "with h as (select the_geom_4326 as p from xxxSCHEMAxxx.home_geocode where id = 'xxxIDxxx')
update xxxSCHEMAxxx.xxxTABLExxx set dist_to_home_m = st_distancespheroid(h.p, the_geom_4326, 'SPHEROID[\"WGS 84\",6378137,298.257223563]') from h"
        sql <- gsub("xxxSCHEMAxxx", wave, gsub("xxxIDxxx", id, gsub("xxxTABLExxx", tablename, sql)))
        message(sql)
        O <- dbGetQuery(conn=conn, statement = sql)
    } else {
        if(force){
            sql <- "with h as (select the_geom_4326 as p from xxxSCHEMAxxx.home_geocode where id = 'xxxIDxxx')
update xxxSCHEMAxxx.xxxTABLExxx set dist_to_home_m = st_distance_spheroid(h.p, the_geom_4326, 'SPHEROID[\"WGS 84\",6378137,298.257223563]') from h"
            sql <- gsub("xxxSCHEMAxxx", wave, gsub("xxxIDxxx", id, gsub("xxxTABLExxx", tablename, sql)))
            message(sql)
            O <- dbGetQuery(conn=conn, statement = sql)
        }
    }
    
}

dist.to.home.all <- function(conn, force = FALSE){
    # IDs from GPS tables
    gpstables <- dbGetQuery(conn=conn, statement = "select table_name, table_schema from information_schema.tables where table_name ~ '^gps_' order by table_schema, table_name;")
    for(i in 1:nrow(gpstables)){
        tablename <- gpstables$table_name[i]
        id <- gsub("gps_", "", tablename)
        wave <- gpstables$table_schema[i]
        dist.to.home.one(conn = conn, wave = wave, id = id, force = force)
    }
}

# a function to see if a column exists
colExists <- function(conn, tableschema, tablename, colname){
    dbGetQuery(conn=conn, statement = sprintf("select count(*) = 1 as colexists from information_schema.columns where table_schema = '%s' and table_name = '%s' and column_name = 'dist_to_home_m';", tableschema, tablename))$colexists
}

# kill.collisions ---------------------------------------------------------
# drop records that overlap in time from multiple data frames for one subject (x is a list)
kill.collisions <- function(x){
    # if there is only one data frame
    if(length(x)==1){
        return(x[[1]])
    }
    # if we got here there are is than 1 data frame
    # we assume that times overlapping at the end of one df are erroneous (e.g., from a misplaced device), so we trim those
    names(x) <- lapply(x, function(i) basename(i$gps_src[1]))
    assign("foo", x, envir = .GlobalEnv)
    # order the data frames in the list by first timestamp
    # first time stamp
    ts1 <- do.call(c, lapply(x, function(y) y$time_gps[1]))
    # file names, assume ordered by date
    fname <- do.call(c, lapply(x, function(y) y$gps_src[1]))
    # a data frame with order
    fd <- data.frame(ts1, fname)
    fd$ord <- 1:nrow(fd)
    fd <- fd[order(fd$fname),]
    # a new list
    l <- list()
    # pull from original list ordered by file name
    for(i in 1:length(x)){
        # file order
        f.o <- fd[i, "ord"]
        l[[i]] <- x[[f.o]]
    }
    #assign("l", l, envir = .GlobalEnv)
    for(i in 1:(length(l)-1)){
        # the index data frame
        xi <- l[[i]]
        # the next data frame
        xiplus <- l[[i+1]]
        # start date of i+1th file
        iplus.start <- xiplus[1,"time_gps"]
        # records in the ith file that were earlier than the start of the iplus file
        xnew <- xi[xi$time_gps < iplus.start,]
        # if the count of records is different after attempting to remove overlapped records
        if(nrow(xnew) != nrow(xi)){
            message(sprintf("replacing %s as necessary ....\n", i))
            # replace the earlier data frame
            l[[i]] <- xnew
        }
    }
    # return the potentially truncated data frame (rbind-ed list)
    return(do.call(rbind, l))
}

