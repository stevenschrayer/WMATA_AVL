select 
distinct cc.vehicleid
, sched.route
from arcccad.ar_log_cc_vehiclework@bdwp.wmata.com cc
left join BUS_SCHED_ROUTE sched on (sched.versionid = cc.versionid and sched.routevarid = cc.currentroutevarid)
where locationupdatedts between 
to_date('15-NOV-19','DD-MON-YY') 
and to_date('17-NOV-19','DD-MON-YY')
;

select * from ARCCCAD.AR_BT_VERSION@bdwp.wmata.com order by activationdts;

select * from BUS_SCHED_ROUTE sched where route = 'S4' and versionid = 70;