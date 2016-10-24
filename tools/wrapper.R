## ---- wrapper
# a wrapper for all of the processes

# the parameter is the dbase name = project name = project folder name
go.wrapper <- function(dbname){
    # define the project dir here
    projectdir <- file.path("c:/users/phurvitz", dbname)
    toolsdir <- file.path(projectdir, "tools")
    datadir <- file.path(projectdir, "data")
    
    # create the database
    # db exists?
    cmd <- sprintf("cmd /c psql -c \"select count(*) = 1 from pg_catalog.pg_database where datname = '%s';\" template1", dbname)
    # returns a string vector, 3rd position is 't' or 'f', force to uppercase and make a Boolean
    dbExists <- as.logical(toupper(gsub(" ", "", system(cmd, intern = TRUE))[3]))
    # if the db doesn't exist
    if(!dbExists){
        message("creating database")
        # create the db
        cmd <- sprintf("cmd /c createdb %s ", basename(projectdir))
        system(cmd)
        # enable postgis and create the schemas
        message("creating PostGIS extension and schemas to hold data")
        cmd <- sprintf("cmd /c psql -c \"create extension postgis;\"", dbname)
    }
    
    # setup
    message("setup and config")
    source(file.path(toolsdir, "setup.R"))
    
    # accelerometry
    message("processing accelerometry data")
    source(file.path(toolsdir, "accelerometry.R"))
    agd.to.db.all.ids(mywave = "y0", myagds = agds)
    
    # home geocodes
    message("creating home geocode data")
    source(file.path(toolsdir, "push_geocodes_to_postgres.R"))
    push.home.geocodes(csvfile = file.path(datadir, "home_geocode/home_address_y0.csv"))
    
    # GPS
    message("processing GPS data")
    source(file.path(toolsdir, "gps.R"))
    qstarz.to.db.all(mywave = "y0", mygpss = gpss, mytimezone = timezone, overwrite = TRUE)
    dist.to.home.all(conn = dbconn, force = TRUE)
    
    # lifelog
    message("processing lifelogs")
    source(file.path(toolsdir, "lifelog.R"))
    create.lifelog.all.ids(conn = dbconn, wave = "y0")
}