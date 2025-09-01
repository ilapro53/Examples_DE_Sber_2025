
--- medicine

DROP TABLE IF EXISTS detn.ilka_medicine;

CREATE TABLE IF NOT EXISTS detn.ilka_medicine (
    med_id INT4,
    an_id VARCHAR(50),
    an_value VARCHAR(50)
);

INSERT INTO detn.ilka_medicine (med_id, an_id, an_value)
VALUES %s;



--- binary_report_mapping

DROP TABLE IF EXISTS detn.ilka_binary_report_mapping;

CREATE TABLE detn.ilka_binary_report_mapping (
    raw_value VARCHAR(50),
    clean_value VARCHAR(50)
);

INSERT INTO detn.ilka_binary_report_mapping VALUES
    ('Положительный', 'Положительный'),
    ('Отрицательный', 'Отрицательный'),
    ('Положительно', 'Положительный'),
    ('Отрицательно', 'Отрицательный'),
    ('Положит.', 'Положительный'),
    ('Отриц.', 'Отрицательный'),
    ('Пол', 'Положительный'),
    ('Отр', 'Отрицательный'),
    ('+', 'Положительный'),
    ('-', 'Отрицательный')
;



--- full_report

DROP TABLE IF EXISTS detn.ilka_full_report;

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
FROM report2;



--- med_results

DROP TABLE IF EXISTS detn.ilka_med_results;

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
    AND NOT prep.is_normal;

SELECT * FROM detn.ilka_med_results;
