## ---- accelerometry
# phil@philhurvitz.com

# Process AGD files: coalesce rewears, create data frames, identify wearing times and bouts, write to database
library(accelerometry)
library(lubridate)
#library(convertagd)
library(sqldf)
library(RSQLite)

# ---> assumes "setup.R" has been modified <---
# loads setups, including projectdir, dbname, etc.
source("tools/setup.R")

# agd.to.df.one.file ------------------------------------------------------
# get a data frame from an accelerometer agd file, returns settings & a raw data set (native epoch duration).
agd.to.df.one.file <- function(fname, agds, timezone){
    #message(fname)
    bn <- basename(fname)
    # the ID from the table of files/ids
    id <- sqldf(sprintf("select id from agds where bn = '%s';", bn), drv="SQLite")$id

    # pull the data
    dblite <- dbConnect(SQLite(), dbname=fname)
    settings <- dbGetQuery(dblite, "select * from settings;")
    dat <- dbGetQuery(dblite, "select * from data;")
    colnames(dat) <- tolower(colnames(dat))
    # drop the wonky timestamp column, also it won't adjust for ST/DT
    dat$datatimestamp <- NULL
    dbDisconnect(dblite)
    
    # reformat the settings
    colnames(settings) <- tolower(colnames(settings))
    epochduration <- settings[settings$settingname=="epochlength","settingvalue"]
    epochseconds <- sprintf("%s secs", epochduration)
    # start date time is 100 nanoseconds since 0001-01-01 00:00
    startdatetime  <- as.numeric(settings[settings$settingname=="startdatetime","settingvalue"])/10000000
    # set the origin in UTC
    origin <- as.POSIXct("0001-01-01 00:00", tz="UTC")
    # start time as a POSIXct, using our time zone
    (time_start <- force_tz(origin + startdatetime, timezone))
    
    # rejigger the time stamp to avoid DST issues from actilife
    dat$time_acc <- seq.POSIXt(from = time_start, by = epochseconds, along.with = dat$axis1)
    
    # vector magnitude
    dat$vm3 <- sqrt(dat$axis1^2 + dat$axis2^2 + dat$axis3^2)    
    
    # julian day break at 3 AM
    jdaya <- sqldf("select extract(doy from time_acc - interval '3 hours')::integer as jdaya from dat;")
    
    # return the read in and reformatted data frame
    dat <- data.frame(id = tolower(id), dat, acc_src=fname, jdaya)
    
    # name for the data set
    datname <- sprintf("dat_%ss", epochduration)
    
    # return list
    out <- list(settings, dat)
    names(out) <- c("settings", datname)
    return(out)
}

# agd.to.df.one.id --------------------------------------------------------
# combines multiple accelerometry data sets for an individual (ostensibly for rewears), aggregates to 60 s
agd.to.df.one.id <- function(id, agds, wave, timezone){
    # need a wave
    if(missing(wave)){
        warning("need to specify a wave")
        return(invisible())
    }
    # lowercase IDs
    id <- tolower(id)
    # get the file names for this ID
    agd.files.id <- sqldf(sprintf("select fname from agds where id = '%s' and wave = '%s';", id, wave), drv="SQLite")
    # initialize an empty list
    dat_raw <- NULL
    # pull data from each agd if there are multiples
    for(i in agd.files.id){
        agd_data <- agd.to.df.one.file(i, agds = agds, timezone = timezone)
        settings <- agd_data[[1]]
        dat <- agd_data[[2]]
        # raw data
        dat_raw <- rbind(dat_raw, dat)
    }
    # if there were multiple, drop the older duplicates
    if(length(agd.files.id) > 1){
        message("dropping duplicate records")
        # reverse order by file name and date
        do <- dat_raw[order(dat_raw$acc_src, dat_raw$time_acc, decreasing=T),]
        # kill collisions
        dat_raw <- do[!duplicated(do$time_acc),]
        # reorder
        dat_raw <- dat_raw[order(dat_raw$time_acc),]
    }
    # set the name of dat_raw
    eval(parse(text=sprintf("%s <- dat", names(agd_data)[2])))
    
    # aggregate to 60 s
    message(id, ": aggregating to 60 s")
    dat_60s <- sqldf("with foo as (select lower(id) as id, acc_src, date_trunc('minute', time_acc) as time_acc, axis1, axis2, axis3 from dat_raw) select id, acc_src, time_acc, sum(axis1) as axis1, sum(axis2) as axis2, sum(axis3) as axis3 from foo group by id, time_acc, acc_src order by time_acc")
    # force the time zone again
    dat_60s$time_acc <- force_tz(dat_60s$time_acc, timezone)
    
    # vector magnitude
    dat_60s$vm3 <- sqrt(dat_60s$axis1^2 + dat_60s$axis2^2 + dat_60s$axis3^2)   
    
    # julian day break at 3 AM
    jdaya <- sqldf("select extract(doy from time_acc - interval '3 hours') as jdaya from dat_60s;")
    dat_60s <- data.frame(dat_60s, jdaya)
    
    # return a list
    out <- list(dat_60s, dat)
    names(out) <- c("dat_60s", names(agd_data)[2])
    return(out)
}

# wear_bout ---------------------------------------------------------------
# add wear and bout info to a data frame
# x is a list of the 60 s data frame and the raw epoch data frame, countscol and weartimecol are the column names for counts and wearing; other options are from accel.bouts. Default thresholds 
wear_bout <- function(x, countscol="axis1", weartimecol="wearing", bout.length=10, thresh.lower=1952, thresh.upper=Inf, tol=2, nci=TRUE){
    # get the 60 s data
    dat_60s <- x$dat_60s
    # van domelen wearing using default arguments
    dat_60s$wearing <- accel.weartime(dat_60s[,countscol])
    # valid day = 10+ hours of wearing
    wearing_hrs <- sqldf("select jdaya, sum(wearing) / 60 as wear_hours, sum(wearing) > 600 as valid_day_10h from dat_60s group by jdaya order by jdaya;")
    
    # van domelen bouts
    bout <- accel.bouts(counts = dat_60s[,countscol] , weartime = dat_60s[,weartimecol], bout.length = bout.length, thresh.lower = thresh.lower, thresh.upper = thresh.upper, tol = tol, nci = TRUE)
    boutcolname <- tolower(sprintf("bout_%sx%s_vm3_%s_%s", bout.length, tol, thresh.lower, thresh.upper))
    # add bout column
    eval(parse(text=sprintf("dat_60s$%s <- bout", boutcolname)))
    # van domelen bout numbers
    brle <- rle(bout)
    v <- ifelse(brle$values, 1, 0)
    cs <- cumsum(v)
    brle$values <- ifelse(v==1, cs, 0)
    bout_nums <- as.integer(inverse.rle(brle))
    # bout num column name
    boutnumcolname <- tolower(sprintf("bout_num_%sx%s_vm3_%s_%s", bout.length, tol, thresh.lower, thresh.upper))
    eval(parse(text=sprintf("dat_60s$%s <- bout_nums", boutnumcolname)))
    
    # update valid_wear_day on both data sets
    sql <- sprintf("select id, jdaya, row_number() over() as recnum_acc, time_acc, axis1::integer, axis2, axis3, vm3, acc_src, wear_hours, valid_day_10h, %s, %s from dat_60s join wearing_hrs using (jdaya) order by time_acc;", boutcolname, boutnumcolname)
    dat_60s <- sqldf(sql)
    # integerize some cols
    intcols <- grep('jdaya|recnum_acc|axis|wear_hours|bout_10x2|bout_num_', colnames(dat_60s))
    for(i in intcols){
        dat_60s[,i] <- as.integer(dat_60s[,i])
    }

    # raw data is the second element
    dat_raw <- x[[2]]
    dat_raw <- sqldf("select * from dat_raw join wearing_hrs using (jdaya) order by time_acc;")
    
    # purloin the wearing and bout info from the 60 s
    sql2 <- sprintf("with d_raw as (select *, date_trunc('minute', time_acc) as minute from dat_raw), d60 as (select time_acc as minute, %s, %s from dat_60s) select *, row_number() over() as recnum_acc from d_raw full join d60 using(minute) order by time_acc;", boutcolname, boutnumcolname)
    dat_raw <- sqldf(sql2)
    # integerize some cols
    intcols <- grep('jdaya|recnum_acc|axis|wear_hours|bout_10x2|bout_num|steps|lux|incline', colnames(dat_raw))
    for(i in intcols){
        dat_raw[,i] <- as.integer(dat_raw[,i])
    }    
    
    # reorder columns
    # columns not in common between 60s and raw
    colsrawext <- colnames(dat_raw)[! colnames(dat_raw) %in% colnames(dat_60s) ]
    # columns to extract
    cols <- paste(c(colnames(dat_60s), colsrawext), collapse=",")
    dat_raw <- sqldf(sprintf("select %s from dat_raw;", cols))
    
    # a list to return, including 60 s and raw
    out <- list(dat_60s, dat_raw)
    names(out) <- c("dat_60s", names(x)[2])
    return(out)
}

# agd.to.db.one.id --------------------------------------------------------
# write one subject's data to the database. will drop and recreate existing table.
# agd.to.db.one.id(conn=dbconn, id=agds$id[1], agds = agds, timezone=timezone, countscol="axis1", weartimecol="wearing", bout.length=10, thresh.lower=1952, thresh.upper=Inf, tol=2, nci=TRUE, wave = 'y0')
agd.to.db.one.id <- function(conn, wave, id, agds = agds, timezone=timezone, countscol="axis1", weartimecol="wearing", bout.length=10, thresh.lower=1952, thresh.upper=Inf, tol=2, nci=TRUE){
    # need a wave
    if(missing(wave)){
        warning("need to specify a wave")
        return(invisible())
    }    
    # get a data frame for this ID
    message("reading and converting AGD data for ", id, " wave ", wave)
    dats <- agd.to.df.one.id(id=id, wave = wave, agds = agds, timezone = timezone)
    # wearing
    message("determining wear time and bouts for ", id, " wave ", wave)
    dats_wear <- wear_bout(dats, countscol=countscol, weartimecol=weartimecol, bout.length=bout.length, thresh.lower=thresh.lower, thresh.upper=thresh.upper, tol=tol, nci=nci)
    # there are two data frames in dats
    dat_60s <- dats_wear$dat_60s
    eval(parse(text=sprintf("%s <- dats[[2]]", names(dats_wear)[2])))
    
    # write 60 s data to database
    # note need to convert any punct to underscore for table names
    tname60s <- sprintf("acc_%s_60s", gsub("[[:punct:]]","_", id))
    message("writing ", tname60s, " to db")
    dbGetQuery(conn, sprintf("drop table if exists %s.%s;", wave, tname60s))
    # change the POSIXct time stamp to a text version indicating the hours of offset from UTC
    dat_60s$time_acc <- force_tz(dat_60s$time_acc, timezone)
    dat_60s$time_acc <- format(dat_60s$time_acc, format="%F %T %z")
    dbWriteTable(conn = conn, name = c(sprintf("%s", wave),tname60s), value = dat_60s, overwrite = TRUE, row.names = FALSE)
    # set the time column as timestamptz from text
    dbGetQuery(conn, statement = sprintf("alter table %s.%s alter COLUMN time_acc set data type timestamptz USING time_acc::timestamptz", wave, tname60s))
    # index on time
    O <- dbGetQuery(conn, sprintf("create index idx_acc_%s_time_acc on %s.%s using btree(time_acc);", tname60s, wave, tname60s))

    # write raw epoch data to database
    tnameraw <- sprintf("acc_%s", gsub("[[:punct:]]", "_", id))
    message("writing ", tnameraw, " to db")
    dbGetQuery(conn, sprintf("drop table if exists %s.%s;", wave, tnameraw))
    # change the POSIXct time stamp to a text version indicating the hours of offset from UTC
    dats_wear[[2]]$time_acc <- force_tz(dats_wear[[2]]$time_acc, timezone)
    dats_wear[[2]]$time_acc <- format(dats_wear[[2]]$time_acc, format="%F %T %z")    
    dbWriteTable(conn = conn, name = c(sprintf("%s", wave),tnameraw), value = dats_wear[[2]], overwrite = TRUE, row.names = FALSE)
    # set the time column as timestamptz from text
    dbGetQuery(conn, statement = sprintf("alter table %s.%s alter COLUMN time_acc set data type timestamptz USING time_acc::timestamptz", wave, tnameraw))
    # index on time
    O <- dbGetQuery(conn, sprintf("create index idx_acc_%s_time_acc on %s.%s using btree(time_acc);", tnameraw, wave, tnameraw))
    O <- dbGetQuery(conn, sprintf("create index idx_acc_%s_minute on %s.%s using btree(minute);", tnameraw, wave, tnameraw))
    
    message("----")
}

# agd.to.db.all.ids -------------------------------------------------------
# runs once for each ID: reads the AGDs, processes wearing and bouts, and writes to the database
#----
# agd.to.db.all.ids(conn=dbconn, wave = 'y0', agds = agds, timezone=timezone, countscol="axis1", weartimecol="wearing", bout.length=10, thresh.lower=1952, thresh.upper=Inf, tol=2, nci=TRUE)
agd.to.db.all.ids <- function(conn, wave, agds, timezone, countscol, weartimecol, bout.length, thresh.lower, thresh.upper, tol, nci){
    agdids <- unique(agds$id)
    for(i in 1:length(agdids)){
        # get the ID
        id <- agdids[i]
        # run the process
        X <- agd.to.db.one.id(conn=conn, id=id, wave, agds = agds, timezone=timezone, countscol=countscol, weartimecol=weartimecol, bout.length=bout.length, thresh.lower=thresh.lower, thresh.upper=thresh.upper, tol=tol, nci=nci)
    }
    valid.day.count.all(conn = conn, wave = wave)
}
#----


# counts of valid days and weeks
#----
valid.day.count <- function(conn, acctable, wave){
    message("processing ", wave, ".", acctable)
    sql <- sprintf("with foo as (select id, count (distinct jdaya) as n_valid_days from %s.%s where wear_hours >= 10 group by id) select id, '%s'::text as wave, n_valid_days, n_valid_days/7::float as n_valid_weeks from foo;", wave, acctable, wave)
    dbGetQuery(conn, sql)
}

valid.day.count.all <- function(conn, wave){
    boutlist <- dbGetQuery(conn, "select table_name from information_schema.tables where table_name ~ '^acc_' order by table_name;")$table_name
    vds <- do.call(rbind, lapply(boutlist, function(x) valid.day.count(conn, x, wave)))
    dbWriteTable(conn, name = c(sprintf("%s", wave),"valid_day_counts"), value = vds, row.names=F, overwrite=T)
    # write.csv(vds, file = "/projects/twins/socenvr01/results/valid_day_counts.csv", row.names = FALSE, quote = FALSE)
}
#----