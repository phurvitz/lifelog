# check file names to match requirements

# a function to report on file names
# datadir is the path to the folder containing data
# for example I run this as 'check.filenames("C:/users/phurvitz/tigerkids/data")'
check.filenames <- function(datadir){
    # GPS
    # for example, for TIGERKids
    # A00-03-5859_BROEM_Y0_GPS.csv
    # the regexp pattern for the example file name
    gpspat <- "[[:upper:]]{1}[[:digit:]]{2}-[[:digit:]]{2}-[[:digit:]]{4}_[[:upper:]]{5}_[[:upper:]]{1}[[:digit:]]{1}_GPS.csv" 
    
    # GPS dir
    gpsdir <- file.path(datadir, "gps")
    gpsfiles <- list.files(gpsdir, "*.csv$")
    gps.filename.compliant <- grepl(pattern = gpspat, gpsfiles)
    
    # a data frame for the reporting
    gps <- data.frame(gpsfiles, gps.filename.compliant)
    
    # print the report data frame
    message("GPS files")
    print(gps)
    
    
    # A00_06_1199_SPRCA_Y015sec.agd
    agdpat <- "[[:upper:]]{1}[[:digit:]]{2}-[[:digit:]]{2}-[[:digit:]]{4}_[[:upper:]]{5}_[[:upper:]]{1}[[:digit:]]{1}_15sec.agd" 
    
    # AGD dir
    agddir <- file.path(datadir, "acc")
    agdfiles <- list.files(agddir, "*.agd$")
    agd.filename.compliant <- grepl(pattern = agdpat, agdfiles)
    
    # a data frame for the reporting
    agd <- data.frame(agdfiles, agd.filename.compliant)
    
    # print the report data frame
    message("accelerometry files")
    print(agd)
    
}