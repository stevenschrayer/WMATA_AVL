select svc_date, count(*) from (select * from BUS_STATE_PAX_LOAD_HMBRW_V01 where svc_date < to_date('12-JUN-19','DD-MON-YY'))
group by svc_date
order by svc_date;

with bus_dates as (
select svc_date, bus_id, count(*) cnt
from 
(select svc_date, bus_id from 
BUS_STATE_PAX_LOAD_HMBRW_V01 
where svc_date between to_date('01-JUN-19','DD-MON-YY') and to_date('30-JUN-19','DD-MON-YY')
and route_id like '52%')
group by svc_date, bus_id
)
select
'rawnav'||bus_id||to_char(svc_date, 'YYMMDD')||'.txt' as filename
from
bus_dates
where cnt > 10
order by svc_date
;

select * from BUS_SCHED_ROUTE;

with current_version_id as (
select max(versionid) versionid from BUS_SCHED_STOP_SEQUENCE_V)
select 
seq.route, seq.variation as pattern, seq.pattern_id, seq.directiondescription direction, seq.routename route_text
, seq.routevarname pattern_name, seq.geostopid geoid, seq.stopid stop_id, seq.directionid direction_id, route.pattern_destination
,seq.geostopdescription as geo_description, seq.latitude stop_lat, seq.longitude stop_lon, seq.stop_sequence, route.pattern_distance trip_length
from BUS_SCHED_STOP_SEQUENCE_V seq 
join current_version_id curr on (curr.versionid = seq.versionid)
left join bus_sched_route_v route on (seq.versionid = route.versionid and seq.routevarid = route.ROUTEVARID)
where seq.PATTERN_ID = '5201' ;