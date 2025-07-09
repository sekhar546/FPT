WITH next_records AS (
SELECT
  o.orig,
	o.dest,
	o.id,
	o.actl_dep_lcl_tms,
	o.actl_arr_lcl_tms,
	o.flight_num,
	o.flights,
	o.acft_regs_cde,
	o.airborne_lcl_tms,
	o.landing_lcl_tms,
  r.id AS next_flight_id,
  RANK () OVER (PARTITION BY o.acft_regs_cde, o.actl_arr_lcl_tms ORDER BY r.actl_dep_lcl_tms) AS rnk
FROM "FPT Interview".main."FPT Interview" o
LEFT JOIN "FPT Interview".main."FPT Interview" r
ON o.orig = r.dest
AND o.dest = r.orig
AND o.acft_regs_cde = r.acft_regs_cde
-- Assuming the return flight is scheduled within two days of arrival, this is to reduce the number of joins
AND r.actl_dep_lcl_tms BETWEEN o.actl_arr_lcl_tms AND o.actl_arr_lcl_tms + INTERVAL '2 days'
)
SELECT
  orig,
	dest,
	id,
	actl_dep_lcl_tms,
	actl_arr_lcl_tms,
	flight_num,
	flights,
	acft_regs_cde,
	airborne_lcl_tms,
	landing_lcl_tms,
  next_flight_id
FROM next_records
WHERE rnk = 1
ORDER BY id