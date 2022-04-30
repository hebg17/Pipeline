CREATE OR REPLACE PROCEDURE insertadatos(
	IN p_id integer,
	IN pid numeric,
	IN pdate_updated timestamp without time zone,
	IN pvehicle_id numeric,
	IN pvehicle_label numeric,
	IN pvehicle_current_status numeric,
	IN pposition_latitude numeric,
	IN pposition_longitude numeric,
	IN pgeographic_point text,
	IN pposition_speed numeric,
	IN pposition_odometer numeric,
	IN ptrip_schedule_relationship numeric,
	IN ptrip_id numeric,
	IN ptrip_start_date numeric,
	IN ptrip_route_id numeric)
LANGUAGE 'sql'
AS $BODY$
	INSERT INTO unidades (_id, id, date_updated, vehicle_id, vehicle_label, vehicle_current_status, position_latitude, position_longitude, geographic_point, position_speed, position_odometer, trip_schedule_relationship, trip_id, trip_start_date, trip_route_id) VALUES (p_id, pid, pDate_updated, pVehicle_id, pVehicle_label, pVehicle_current_status, pPosition_latitude, pPosition_longitude, pGeographic_point, pPosition_speed, pPosition_odometer, pTrip_schedule_relationship, pTrip_id, pTrip_start_date, pTrip_route_id);
$BODY$;
ALTER PROCEDURE insertadatos(integer, numeric, timestamp without time zone, numeric, numeric, numeric, numeric, numeric, text, numeric, numeric, numeric, numeric, numeric, numeric)
    OWNER TO postgres;
