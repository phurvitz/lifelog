## ---- params 
# parameters ---------------------------------------------------------------
##############################
### THESE NEED TO BE EDITED / DEFINED ###
# who am I?
myusername <- "phurvitz"
# directory containing all the data
projectdir <- "C:/Users/phurvitz/tigerkids"
# database name
mydbname <- basename(projectdir)
# time zone
timezone <- "CST6CDT"
# file name chunks (positions of ID, wave, rewear, etc. assuming underscore separation), pattern is "name_part = position"
fnchunk.nameparts <- list(id=1, wave=3)
#fnchunk.nameparts <- list(id=1, wave=2, wear=3)
fnchunk.names <- names(fnchunk.nameparts)
fnchunk.positions <- do.call(c, fnchunk.nameparts)
# SRID
mysrid <- 26982
setups <- list(
    myusername = myusername
    , projectdir = projectdir
    , mydbname = mydbname
    , timezone = timezone
    , fnchunk.positions = fnchunk.positions
    , mysrid = mysrid
)
##############################