SET statement_timeout = 0;
SET lock_timeout = 0;

CREATE TABLE alcaldias
(
    _id integer,
    id numeric,
    nomgeo text,
    cve_mun numeric,
    cve_ent numeric,
    cvegeo numeric,
    geo_point_2d text,
    geo_shape text,
    municipio numeric
);

CREATE TABLE unidades
(
    _id integer,
    id numeric,
    date_updated timestamp without time zone,
    vehicle_id numeric,
    vehicle_label numeric,
    vehicle_current_status numeric,
    position_latitude numeric,
    position_longitude numeric,
    geographic_point text COLLATE pg_catalog."default",
    position_speed numeric,
    position_odometer numeric,
    trip_schedule_relationship numeric,
    trip_id numeric,
    trip_start_date numeric,
    trip_route_id numeric
);
