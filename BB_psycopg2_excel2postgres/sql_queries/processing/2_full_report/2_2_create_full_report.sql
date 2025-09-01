CREATE TABLE detn.ilka_full_report AS
WITH med_an_name_claen AS (
	SELECT 
		id,
		name,
		is_simple,
		REPLACE(
			REPLACE(min_value, CHR(160), ''),
			',', '.'
		)::REAL AS min_value,
		REPLACE(
			REPLACE(max_value, CHR(160), ''),
			',', '.'
		)::REAL AS max_value
	FROM 
		de.med_an_name
), report2 AS (
	SELECT
		mn.id AS patient_id,
		mn.phone AS patient_phone,
		mn.name AS patient_name,
		man.id AS analysis_id,
		man.name AS analysis_name,
		CASE
			WHEN is_simple = 'N' THEN
				CASE
					WHEN m.an_value IS NULL
						THEN NULL
					WHEN m.an_value::real > man.max_value::real
						THEN 'Повышен'
					WHEN m.an_value::real < man.min_value::real
						THEN 'Понижен'
					ELSE 'В норме'
				END
			WHEN is_simple = 'Y'
				THEN report_mp.clean_value
		END AS conclusion
	FROM
		detn.ilka_medicine AS m
	JOIN
		de.med_name AS mn
			ON m.med_id = mn.id
	JOIN
		med_an_name_claen AS man
			ON m.an_id = man.id
	LEFT JOIN
		detn.ilka_binary_report_mapping AS report_mp
			ON m.an_value = report_mp.raw_value
)
SELECT
	*,
	CASE
		WHEN conclusion = 'В норме'
			THEN TRUE
		WHEN conclusion = 'Отрицательный'
			THEN TRUE
		WHEN conclusion IS NULL
			THEN NULL
		ELSE FALSE
	END AS is_normal
FROM report2