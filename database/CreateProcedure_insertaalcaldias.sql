CREATE OR REPLACE PROCEDURE MetrobusCDMX.insertaalcaldias(
	IN p_id integer,
	IN pid numeric,
	IN pnomgeo text,
	IN pcve_mun numeric,
	IN pcve_ent numeric,
	IN pcvegeo numeric,
	IN pgeo_point_2d text,
	IN pgeo_shape text,
	IN pmunicipio numeric)
LANGUAGE 'sql'
AS $BODY$
	INSERT INTO MetrobusCDMX.alcaldias (_id, id, nomgeo, cve_mun, cve_ent, cvegeo, geo_point_2d, geo_shape, municipio) VALUES (p_id, pid, pnomgeo, pcve_mun, pcve_ent, pcvegeo, pgeo_point_2d, pgeo_shape, pmunicipio);
$BODY$;
ALTER PROCEDURE metrobuscdmx.insertaalcaldias(integer, numeric, text, numeric, numeric, numeric, text, text, numeric)
    OWNER TO postgres;
