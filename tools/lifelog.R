# script for creating lifelogs from accelerometry and GPS tables

## ---- lifelog
# create lifelog tables by rounding the GPS data to match the accelerometry interval and join by these sets of time stamps
create.lifelog.one.id <- function(conn, id, wave){
    # look in the database for the two necessary files
    id <- gsub("[[:punct:]]", "_", id)
    message("creating lifelog ll_", id)
    acc.exists <- dbGetQuery(conn, sprintf("select case when count(*)=1 then true else false end as acc_exists from information_schema.tables where table_schema = '%s' and table_name = 'acc_%s';", wave, id))$acc_exists
    gps.exists <- dbGetQuery(conn, sprintf("select case when count(*)=1 then true else false end as acc_exists from information_schema.tables where table_schema = '%s' and table_name = 'gps_%s';", wave, id))$acc_exists

    # both ACC and GPS must exist
    if(!acc.exists | !gps.exists){
        message(sprintf("either ACC or GPS do not exist for %s", id))
        return(invisible())
    }
    
    # get the time difference between the first two accelerometry records
    epoch <- dbGetQuery(conn, sprintf("with a as (select time_acc from %s.acc_%s order by time_acc limit 2) select extract(epoch from max(a.time_acc) - min(a.time_acc))::text || ' seconds' from a;", wave, id))[1,1]
    
    # if we got here then we have both files
    # the join
    sql <- "drop table if exists xxxSCHEMAxxx.ll_xxxIDxxx;
        create table xxxSCHEMAxxx.ll_xxxIDxxx as 
        with a as (select * from xxxSCHEMAxxx.acc_xxxIDxxx)
        , g as (select *, gis.timestamp_round(time_gps, 'xxxEPOCHxxx', 'floor') as time_acc from xxxSCHEMAxxx.gps_xxxIDxxx)
        , ag as (select * from a left join g using (id, time_acc))
        select distinct on (time_acc) * from ag order by time_acc;"
    sql <- gsub("xxxSCHEMAxxx", wave, gsub("xxxIDxxx", id, gsub("xxxEPOCHxxx", epoch, sql)))
    dbGetQuery(conn, sql)
    
    # indexes on geom columns
    message("  creating indexes on ll_", id)
    cn <- dbGetQuery(conn, sprintf("select column_name from information_schema.columns where table_schema = '%s' and table_name = 'll_%s' and column_name ~ '^the_geom';", wave, id))$column_name
    for(i in cn){
        O <- dbGetQuery(conn, sprintf("create index idx_%s_ll_%s on %s.ll_%s using gist(%s);", i, id, wave, id, i))
    }
    # indexes on time columns
    cn <- dbGetQuery(conn, sprintf("select column_name from information_schema.columns where table_schema = '%s' and table_name = 'll_%s' and column_name ~ '^time';", wave, id))$column_name
    for(i in cn){
        O <- dbGetQuery(conn, sprintf("create index idx_%s_ll_%s on %s.ll_%s using btree(%s);", i, id, wave, id, i))
    }    
}

# run over all acc tables
create.lifelog.all.ids <- function(conn, wave ){
    # get the acc files for this wave
    ids <- dbGetQuery(conn, sprintf("select replace(table_name, 'acc_', '') as id from information_schema.tables where table_schema = '%s' and table_name ~ '^acc_.*[[:digit:]]$' order by table_name;", wave))$id
    for(i in ids){
        create.lifelog.one.id(conn = conn, id = i, wave = wave)
    }
}


# timestamp_round_pg ------------------------------------------------------
# check to see if the function exists
pgfcn.exists <- function(conn, schema, function.name){
    dbGetQuery(conn, sprintf("SELECT case when count(*)=0 then false else true end as function_exists
        FROM pg_catalog.pg_proc p
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname ~ '^(timestamp_round)$'
          AND n.nspname ~ '^(gis)$'", schema, function.name))$function_exists
}
    
if(!pgfcn.exists(conn=dbconn, "gis", "timestamp_round")){
    O <- dbGetQuery(conn=conn, "-- timestamp rounding function, option for round, floor, or ceiling
    -- inputs are a timestamptz object, rounding interval, and round type ({'round', 'floor', 'ceiling'})
    -- e.g., see below
    CREATE OR REPLACE FUNCTION gis.timestamp_round(base_date timestamp with time zone, round_interval interval, round_type text)
        RETURNS timestamp with time zone AS
    $$
    declare
        rounded_timestamp timestamptz;
    begin
        -- example: 
            -- select current_timestamp, timestamp_round (current_timestamp, interval '10 seconds', 'floor') as floor, 
            -- timestamp_round (current_timestamp, interval '10 seconds', 'round') as round, 
            -- timestamp_round (current_timestamp, interval '10 seconds', 'ceiling') as ceiling;
        -- options for round type handled by if/elsif
        -- simple round
        IF round_type = 'round' then
            SELECT into rounded_timestamp
            TIMESTAMP WITH TIME ZONE 'epoch' + (
                EXTRACT ( EPOCH FROM $1 )::INTEGER + 
                EXTRACT ( EPOCH FROM $2 )::INTEGER / 2 
            ) /
                EXTRACT ( EPOCH FROM $2 )::INTEGER *
                EXTRACT ( EPOCH FROM $2 )::INTEGER * 
            INTERVAL '1 second';
        -- round to floor
        elsif round_type = 'floor' then
            SELECT into rounded_timestamp
            TIMESTAMP WITH TIME ZONE 'epoch' + (
                EXTRACT ( EPOCH FROM $1 - $2/2)::INTEGER + 
                EXTRACT ( EPOCH FROM $2 )::INTEGER / 2 
            ) /
                EXTRACT ( EPOCH FROM $2 )::INTEGER *
                EXTRACT ( EPOCH FROM $2 )::INTEGER * 
            INTERVAL '1 second';
        -- round to ceiling
        elsif round_type = 'ceiling' then
            SELECT into rounded_timestamp
            TIMESTAMP WITH TIME ZONE 'epoch' + (
                EXTRACT ( EPOCH FROM $1 + $2/2)::INTEGER + 
                EXTRACT ( EPOCH FROM $2 )::INTEGER / 2 
            ) /
                EXTRACT ( EPOCH FROM $2 )::INTEGER *
                EXTRACT ( EPOCH FROM $2 )::INTEGER * 
            INTERVAL '1 second';
        END IF;
        -- return the rounded timestamptz object
        return rounded_timestamp;
    end;
    $$
        LANGUAGE plpgsql IMMUTABLE STRICT
        COST 100;
    end;")
}
