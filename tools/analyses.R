# some analsyses

# buffers
sql <- "
select row_number() over() as gid, id, st_buffer(the_geom_26982, 5280 * .75) as bgeom from y0.home_geocode;
"

# high PA within home buffers
sql <- "
with ll as (select axis1, the_geom_26982 from y0.ll_a00_03_5859)
, llhi as (select * from ll where axis1 > 2296 / 4)
, llbuf as (select id, st_buffer(the_geom_26982, 5280 * .75) as bgeom from y0.home_geocode where id = 'a00_03_5859')
, llhibuf as (select 'home'::text as buffer, * from llhi, llbuf where st_intersects(llbuf.bgeom, llhi.the_geom_26982))
, llhiout as (select 'nonhome'::text as buffer, * from llhi, llbuf where not st_intersects(llbuf.bgeom, llhi.the_geom_26982))
, llcomb as (select * from llhibuf union all llhiout)
select row_number() over() as gid, * from llcomb;
"