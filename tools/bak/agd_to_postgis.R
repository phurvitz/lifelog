# setup
library(tcltk)
library(accelerometry)
library(convertagd)
library(RPostgreSQL)
library(tcltk)

# db connection
source("tools/setup.R")

# get a data frame from an accelerometer agd file, returns settings, a raw data set, and one aggregated to 60 s.
#----
agd.to.df.onefile <- function(fname, tz="PST8PDT"){
    message(fname)
    # the ID
    id <- gsub(" .*", "", basename(fname))
    # basic function to pull the data, from van Domelen
    X <- read_agd(file = fname, tz = tz)
    # get the raw data
    dat <- as.data.frame(X$raw.data)
    
    # rename column 'date'
    colnames(dat)[grep("date", colnames(dat))] <- "time_acc"
    settings <- as.data.frame(X$settings)
    
    # vector magnitude
    dat$vm3 <- sqrt(dat$axis1^2 + dat$axis2^2 + dat$axis3^2)    
    
    # julian day break at 3 AM
    jdaya <- sqldf("select extract(doy from time_acc - interval '3 hours')::integer as jdaya from dat;")
    
    # return the read in and reformatted data frame
    dat <- data.frame(id = tolower(id), dat, src=fname, jdaya)
    return(dat=dat)
}
#----

# combines multiple accelerometry data sets for an individual, run van domelen stuff
#----
agd.to.df.one.id <- function(id, agdfiles, tz="PST8PDT", overwrite = FALSE){
    idlower <- tolower(id)
    idupper <- toupper(id)
    agdfiles.id <- grep(idupper, agdfiles, value = TRUE)
    # initialize an empty list
    dat <- NULL
    # pull data from each agd if there are multiples
    for(i in agdfiles.id){
        X <- agd.to.df.onefile(i, tz = tz)
        # raw data
        dat <- rbind(dat, X)
    }
    # if there were multiple, drop the older duplicates
    if(length(agdfiles.id) > 1){
        message("dropping duplicate records")
        # reverse order by file name and date
        do <- dat[order(dat$src, dat$time_acc, decreasing=T),]
        # kill collisions
        dat <- do[!duplicated(do$time_acc),]
        # reorder
        dat <- dat[order(dat$time_acc),]
    } 
    
    # aggregate to 60 s
    message(id, ": aggregating to 60 s")
    dat_60s <- sqldf("with foo as (select lower(id) as id, src, date_trunc('minute', time_acc) as time_acc, axis1, axis2, axis3 from dat) select id, src, time_acc, sum(axis1) as axis1, sum(axis2) as axis2, sum(axis3) as axis3 from foo group by id, time_acc, src order by time_acc")
    
    # vector magnitude
    dat_60s$vm3 <- sqrt(dat_60s$axis1^2 + dat_60s$axis2^2 + dat_60s$axis3^2)   
    
    # julian day break at 3 AM
    jdaya <- sqldf("select extract(doy from time_acc - interval '3 hours') as jdaya from dat_60s;")
    dat_60s <- data.frame(dat_60s, jdaya)
    
    # van domelen wearing using default arguments
    dat_60s$wearing <- accel.weartime(dat_60s$axis1)
    # valid day = 10+ hours of wearing
    wearing_hrs <- sqldf("select jdaya, sum(wearing) / 60 as wear_hours, sum(wearing) > 600 as valid_day_10h from dat_60s group by jdaya order by jdaya;")
    
    # van domelen bouts
    dat_60s$bout_10x2_vm3_2690_inf <- accel.bouts(counts = dat_60s$vm3 , weartime = dat_60s$wearing, bout.length = 10, thresh.lower = 2690, thresh.upper = Inf, tol = 2, tol.upper = 0, nci = TRUE)
    # van domelen bout numbers
    brle <- rle(dat_60s$bout_10x2_vm3_2690_inf)
    v <- ifelse(brle$values, 1, 0)
    cs <- cumsum(v)
    brle$values <- ifelse(v==1, cs, 0)
    bout_nums <- as.integer(inverse.rle(brle))
    dat_60s$bout_num_10x2_vm3_2690_inf <- bout_nums
    
    dat_60s$bout_10x2_vm3_1952_6166 <- accel.bouts(counts = dat_60s$vm3 , weartime = dat_60s$wearing, bout.length = 10, thresh.lower = 1952, thresh.upper = 6166, tol = 2, tol.upper = 0, nci = TRUE)
    # van domelen bout numbers
    brle <- rle(dat_60s$bout_10x2_vm3_1952_6166)
    v <- ifelse(brle$values, 1, 0)
    cs <- cumsum(v)
    brle$values <- ifelse(v==1, cs, 0)
    bout_nums <- as.integer(inverse.rle(brle))
    dat_60s$bout_num_10x2_vm3_1952_6166 <- bout_nums    
    
    
    # update valid_wear_day on both data sets
    dat_60s <- sqldf("select id, jdaya, row_number() over() as recnum_acc, time_acc, axis1, axis2, axis3, vm3, src, wear_hours, valid_day_10h, bout_10x2_vm3_2690_inf, bout_num_10x2_vm3_2690_inf, bout_10x2_vm3_1952_6166, bout_num_10x2_vm3_1952_6166 from dat_60s join wearing_hrs using (jdaya) order by time_acc;")
    dat_10s <- sqldf("select * from dat join wearing_hrs using (jdaya) order by time_acc;")
    
    # update bout info on 10 s data
    dat_10s <- sqldf("with d10 as (select *, date_trunc('minute', time_acc) as minute from dat_10s), d60 as (select time_acc as minute, bout_10x2_vm3_1952_6166, bout_num_10x2_vm3_1952_6166, bout_10x2_vm3_2690_inf, bout_num_10x2_vm3_2690_inf from dat_60s) select id, jdaya, row_number() over() as recnum_acc, time_acc, minute, axis1, axis2, axis3, vm3, src, wear_hours, valid_day_10h, bout_10x2_vm3_1952_6166, bout_num_10x2_vm3_1952_6166, bout_10x2_vm3_2690_inf, bout_num_10x2_vm3_2690_inf from d10 full join d60 using(minute) order by time_acc;")
    
    return(list(dat_10s=dat_10s, dat_60s=dat_60s))
}
#----

agd.to.db.one.id <- function(id, agdfiles=agdfiles, tz="PST8PDT"){
    idupper <- toupper(id)
    idlower <- tolower(id)
    # get a data frame for this ID
    dats <- agd.to.df.one.id(idupper, agdfiles = agdfiles, tz=tz)
    dat_10s <- dats$dat_10s
    dat_60s <- dats$dat_60s
    # write to database
    tname <- sprintf("acc_%s", tolower(id))
    message("writing to db")
    
    # SQL to create the table
    sqlc <- "
    DROP TABLE IF EXISTS gis.acc_xxxIDxxx;
    CREATE TABLE gis.acc_xxxIDxxx 
    ( id text,
    jdaya int,
    recnum_acc int,
    time_acc timestamp with time zone,
    minute timestamp with time zone,
    axis1 int,
    axis2 int,
    axis3 int,
    vm3 float8,
    acc_src text,
    wear_hours int,
    valid_day_10h bool,
    bout_10x2_vm3_1952_6166 integer,
    bout_num_10x2_vm3_1952_6166 integer,
    bout_10x2_vm3_2690_inf integer,
    bout_num_10x2_vm3_2690_inf integer 
    ) ;  
    "
    # create the table
    O <- dbGetQuery(conn, gsub("xxxIDxxx", idlower, sqlc))
    # write the table contents
    O <- dbWriteTable(conn, name = c("gis",tname), dat_10s, row.names = FALSE, append = TRUE)
    # index on time
    O <- dbGetQuery(conn, sprintf("create index idx_acc_%s_time_acc on gis.%s using btree(time_acc);", idlower, tname))
    O <- dbGetQuery(conn, sprintf("create index idx_acc_%s_minute on gis.%s using btree(minute);", idlower, tname))
    
    sqlc <- "
    DROP TABLE IF EXISTS s60.acc_xxxIDxxx;
    CREATE TABLE s60.acc_xxxIDxxx 
    ( id text,
    jdaya int,
    recnum_acc int,
    time_acc timestamp with time zone,
    axis1 int,
    axis2 int,
    axis3 int,
    vm3 float8,
    acc_src text,
    wear_hours int,
    valid_day_10h bool,
    bout_10x2_vm3_1952_6166 integer,
    bout_num_10x2_vm3_1952_6166 integer,
    bout_10x2_vm3_2690_inf integer,
    bout_num_10x2_vm3_2690_inf integer 
    ) ;  
    "
    O <- dbGetQuery(conn, gsub("xxxIDxxx", idlower, sqlc))    
    O <- dbWriteTable(conn, name = c("s60",tname), dat_60s, row.names = FALSE, append = TRUE)
    O <- dbGetQuery(conn, sprintf("create index idx_acc_%s_time_acc on s60.%s using btree(time_acc);", idlower, tname))
    message("----")
}

# run once for each ID: read the AGDs and write to the database
#----
agd.to.db.all.ids <- function(agd.ids=agd.ids, agdfiles=agdfiles, tz="PST8PDT"){
    for(i in 1:length(agd.ids)){
        # get the ID
        idupper <- agd.ids[i]
        # get the files for this ID
        idfiles <- grep(idupper, agdfiles, value = TRUE)
        # run the process
        X <- agd.to.db.one.id(id = idupper, agdfiles = agdfiles, tz = tz)
    }
}
#----

# counts of valid days and weeks
#----
valid.day.count <- function(llt){
    message("processing ", llt)
    sql <- sprintf("with foo as (select id, count (distinct jdaya) as n_valid_days from gis.%s where wear_hours >= 10 group by id) select id, n_valid_days, n_valid_days/7::float as n_valid_weeks from foo;", llt)
    dbGetQuery(conn, sql)
}

valid.day.count.all <- function(conn=conn){
    boutlist <- dbGetQuery(conn, "select table_name from information_schema.tables where table_name ~ '^ll_' order by table_name;")$table_name
    vds <- do.call(rbind, lapply(boutlist, valid.day.count))
    dbWriteTable(conn, name = c("bouts","valid_day_counts"), value = vds, row.names=F, overwrite=T)
    write.csv(vds, file = "/projects/twins/socenvr01/results/valid_day_counts.csv", row.names = FALSE, quote = FALSE)
}
#----