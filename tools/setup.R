## ---- setup 
# phil@philhurvitz.com

# setups
library(RPostgreSQL)
library(sqldf)
library(tcltk)
options(stringsAsFactors = FALSE)

# pull parameters from the params.R file
source("params.R")
if(!exists("projectdir")){
    if(.Platform$OS.type=="windows"){
        projectdir <- choose.dir("c:/users")
    } else {
        projectdir <- choose.dir()
    }
}
if(is.na(projectdir)){
    rm(projectdir)
    stop("You have not specified a projectdir. Please re-run setup.R")
}

if(!dir.exists(projectdir)){
    stop("The projectdir ", projectdir, " does not exist.")
}
source(file.path(projectdir, "params.R"))

##############################
### ASSUMED FILE STRUCTURE ###
# projectdir/passwd.txt
# projectdir/data
# projectdir/data/gps (contains csv files)
# projectdir/data/acc (contains agd files)
##############################

##############################
#### FILE NOMENCLATURE example
# 1000_0_1.agd
# 1000         = ppt ID
#      0       = measurement wave
#        1     = first wear
##############################

# Note I change everything to lowercase and replace any punctuation with underscores for compliance with PostgreSQL table and column naming requirements.


# agd ---------------------------------------------------------------------
# data source for AGD files
# maningful parts of the file name are set above.
agd.dir <- file.path(projectdir, "data/acc")
agd.files <- list.files(agd.dir, "*.agd$", full.names = TRUE)
# unique IDs from the file names
# file name chunks separated by underscores, lowercased
fnchunks <- strsplit(basename(agd.files), split = "_")
fnchunks <- lapply(fnchunks, function(x) tolower(gsub("[[:punct:]]", "_", x)))
fnchunks <- do.call(rbind, fnchunks)
fnchunks <- fnchunks[,fnchunk.positions]
colnames(fnchunks) <- fnchunk.names
agds <- data.frame(fname=agd.files, fnchunks)
# 3rd column needs attention
agds$wave <- tolower(substr(x = agds$wave, start = 1, stop = 2))
# basename of file name
agds$bn <- basename(agds$fname)

# gps ---------------------------------------------------------------------
# data source for GPS files
gps.dir <- file.path(projectdir, "data/gps")
gps.files <- list.files(gps.dir, "*.csv$", full.names = TRUE)
# unique IDs from the file names
# file name chunks separated by underscores, lowercased
fnchunks <- strsplit(basename(tolower(gps.files)), split = "_")
fnchunks <- lapply(fnchunks, function(x) tolower(gsub("[[:punct:]]", "_", x)))
fnchunks <- do.call(rbind, fnchunks)
fnchunks <- fnchunks[,fnchunk.positions]
colnames(fnchunks) <- fnchunk.names
gpss <- data.frame(fname=gps.files, fnchunks)
# 3rd column needs attention
gpss$wave <- tolower(substr(x = gpss$wave, start = 1, stop = 2))
# basename of file name
gpss$bn <- basename(gpss$fname)

# db ---------------------------------------------------------------------
# db connection function
# assumes we are only using one database for the project, with dbname = projectname and connection = "dbconn"
connectDB <- function(dbname=mydbname, host="localhost", port=5432, myusername){
    # read the password file
    password <- scan(file.path(projectdir, "passwd.txt"), what = "character", quiet = TRUE)
    if(!exists("dbconn")){
        #message(dbname, " not connected")
        dbconn <- dbConnect(dbDriver("PostgreSQL"), dbname = mydbname, host=host, password = password, port=port, user = myusername)
        message(dbname, " now connected as dbconn")
        return(dbconn)
    }
    if(exists("dbconn")){
        connectionIsCurrent <- isPostgresqlIdCurrent(dbconn)
        #message(connectionIsCurrent)
        if(!connectionIsCurrent){
            message("establising connection to ", dbname)
            dbconn <- dbConnect(dbDriver("PostgreSQL"), dbname = mydbname, host=host, password = password, port=port, user = myusername)
            #message(dbname, " now connected")
        } 
        return(dbconn)
    }
}
#connect <- connectDB

# db connect ---------------------------------------------------------------------
# connect to the db
dbconn <- connectDB(dbname=mydbname, myusername = myusername)

# sqldf options for Postgres
mypassword <- scan(file.path(projectdir, "passwd.txt"), what = "character", quiet = TRUE)
options(sqldf.RPostgreSQL.user = myusername, 
        sqldf.RPostgreSQL.password = mypassword,
        sqldf.RPostgreSQL.dbname = mydbname,
        sqldf.RPostgreSQL.hostname = "localhost", 
        sqldf.RPostgreSQL.port = 5432)