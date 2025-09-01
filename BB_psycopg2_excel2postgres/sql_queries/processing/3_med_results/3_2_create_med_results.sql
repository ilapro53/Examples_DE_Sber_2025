CREATE TABLE detn.ilka_med_results AS
WITH prep AS (
	SELECT 
		patient_phone,
		patient_name,
		analysis_name,
		conclusion,
		SUM((NOT is_normal)::INT4) OVER(PARTITION BY patient_id) AS not_normal_count,
		is_normal
	FROM detn.ilka_full_report
)
SELECT 
	patient_phone,
	patient_name,
	analysis_name,
	conclusion
FROM prep
WHERE 
	prep.not_normal_count >= 2
	AND NOT prep.is_normal