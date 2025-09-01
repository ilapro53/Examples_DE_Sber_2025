
DROP TABLE IF EXISTS detn.ilka_promo_effectiveness_analysis;
CREATE TABLE detn.ilka_promo_effectiveness_analysis (
    analysis_id BIGSERIAL PRIMARY KEY,
    promo_id INT NOT NULL,
    promo_name VARCHAR(128),
    discount_percent NUMERIC(5, 2),
    start_date DATE,
    end_date DATE,
    promo_duration_days INT,
    total_sales_quantity INT,
    total_revenue NUMERIC(15, 2),
    avg_daily_sales NUMERIC(15, 4),
    avg_daily_revenue NUMERIC(15, 4),
    unique_customers_count INT,
    unique_products_count INT,
    unique_stores_count INT,
    conversion_rate NUMERIC(6, 4),
    effectiveness_ratio NUMERIC(6, 4),
    revenue_per_day NUMERIC(15, 4),
    category_id INT,
    category_name VARCHAR(128),
    department_id INT,
    department_name VARCHAR(128),
    region VARCHAR(128),
    loyalty_level SMALLINT,
    group_sales_quantity INT,
    group_revenue NUMERIC(15, 2),
    group_sales_ratio NUMERIC(6, 4),
    calculation_date DATE NOT NULL DEFAULT CURRENT_DATE,
    etl_version INT NOT NULL,
    CONSTRAINT unique_combination
        UNIQUE (calculation_date, promo_id, category_id, region, loyalty_level)
);


CREATE INDEX IF NOT EXISTS ix_ilka_promo_id
    ON detn.ilka_promo_effectiveness_analysis (promo_id);

CREATE INDEX IF NOT EXISTS ix_ilka_calculation_date
    ON detn.ilka_promo_effectiveness_analysis (calculation_date);

CREATE INDEX IF NOT EXISTS ix_ilka_promo_unique
    ON detn.ilka_promo_effectiveness_analysis (calculation_date, promo_id, category_id, region, loyalty_level);


DELETE FROM detn.ilka_promo_effectiveness_analysis
WHERE calculation_date = CURRENT_DATE;


WITH
constants AS (
    SELECT
        1::INTEGER AS etl_version,
        TO_DATE('9999-12-31', 'YYYY-MM-DD') AS date_tec_inf,
        TO_DATE('1900-01-01', 'YYYY-MM-DD') AS date_tec_anti_inf
),
prep_products AS (
    SELECT
        product_id::INT,
        product_name::VARCHAR(128),
        category_id::INT,
        price::NUMERIC(10,2),
        brand_id::INT,
        COALESCE(
            TO_DATE(effective_from, 'YYYY-MM-DD'),
            (SELECT date_tec_anti_inf FROM constants)
        )::DATE AS effective_from,
        COALESCE(
            TO_DATE(effective_to, 'YYYY-MM-DD'),
            (SELECT date_tec_inf FROM constants)
        )::DATE AS effective_to,
        is_current::INT,
        version_number::INT
    FROM sales.products
),
prep_categories AS (
    SELECT
        category_id::INT,
        category_name::VARCHAR(128),
        department_id::INT,
        COALESCE(
            TO_DATE(effective_from, 'YYYY-MM-DD'),
            (SELECT date_tec_anti_inf FROM constants)
        )::DATE AS effective_from,
        COALESCE(
            TO_DATE(effective_to, 'YYYY-MM-DD'),
            (SELECT date_tec_inf FROM constants)
        )::DATE AS effective_to,
        is_current::INT,
        version_number::INT
    FROM sales.categories
),
prep_departments AS (
    SELECT
        department_id::INT,
        department_name::VARCHAR(128),
        COALESCE(
            TO_DATE(effective_from, 'YYYY-MM-DD'),
            (SELECT date_tec_anti_inf FROM constants)
        )::DATE AS effective_from,
        COALESCE(
            TO_DATE(effective_to, 'YYYY-MM-DD'),
            (SELECT date_tec_inf FROM constants)
        )::DATE AS effective_to,
        is_current::INT,
        version_number::INT
    FROM sales.departments
),
prep_brands AS (
    SELECT
        brand_id::INT,
        brand_name::VARCHAR(128),
        COALESCE(
            TO_DATE(effective_from, 'YYYY-MM-DD'),
            (SELECT date_tec_anti_inf FROM constants)
        )::DATE AS effective_from,
        COALESCE(
            TO_DATE(effective_to, 'YYYY-MM-DD'),
            (SELECT date_tec_inf FROM constants)
        )::DATE AS effective_to,
        is_current::INT,
        version_number::INT
    FROM sales.brands
),
prep_promo_sales AS (
    SELECT
        sale_id::BIGINT,
        product_id::INT,
        store_id::INT,
        customer_id::INT,
        TO_TIMESTAMP("date"::INTEGER)::DATE AS "date",
        quantity::INT,
        promo_id::INT
    FROM sales.sales
    WHERE promo_id IS NOT NULL
),
prep_promotions AS (
    SELECT
        promo_id::INT,
        promo_name::VARCHAR(128),
        discount_percent::NUMERIC(5,2),
        TO_DATE(start_date, 'YYYY-MM-DD')::DATE AS start_date,
        TO_DATE(end_date, 'YYYY-MM-DD')::DATE AS end_date,
        COALESCE(
            TO_DATE(effective_from, 'YYYY-MM-DD'),
            (SELECT date_tec_anti_inf FROM constants)
        )::DATE AS effective_from,
        COALESCE(
            TO_DATE(effective_to, 'YYYY-MM-DD'),
            (SELECT date_tec_inf FROM constants)
        )::DATE AS effective_to,
        is_current::INT,
        version_number::INT,
        (TO_DATE(end_date, 'YYYY-MM-DD') - TO_DATE(start_date, 'YYYY-MM-DD') + 1)::INT AS promo_duration_days
    FROM sales.promotions
),
prep_stores AS (
    SELECT
        store_id::INT,
        store_name::VARCHAR(128),
        city::VARCHAR(128),
        region::VARCHAR(128),
        COALESCE(
            TO_DATE(effective_from, 'YYYY-MM-DD'),
            (SELECT date_tec_anti_inf FROM constants)
        )::DATE AS effective_from,
        COALESCE(
            TO_DATE(effective_to, 'YYYY-MM-DD'),
            (SELECT date_tec_inf FROM constants)
        )::DATE AS effective_to,
        is_current::INT,
        version_number::INT
    FROM sales.stores
),
prep_customers AS (
    SELECT
        customer_id::INT,
        customer_name::VARCHAR(128),
        TO_DATE(registration_date, 'YYYY-MM-DD')::DATE AS registration_date,
        loyalty_level::SMALLINT,
        phone_number::VARCHAR(20),
        COALESCE(
            TO_DATE(effective_from, 'YYYY-MM-DD'),
            (SELECT date_tec_anti_inf FROM constants)
        )::DATE AS effective_from,
        COALESCE(
            TO_DATE(effective_to, 'YYYY-MM-DD'),
            (SELECT date_tec_inf FROM constants)
        )::DATE AS effective_to,
        is_current::INT,
        version_number::INT
    FROM sales.customers
),
prep_current_products AS
    (SELECT * FROM prep_products WHERE is_current = 1),
prep_current_categories AS
    (SELECT * FROM prep_categories WHERE is_current = 1),
prep_current_departments AS
    (SELECT * FROM prep_departments WHERE is_current = 1),
prep_current_brands AS
    (SELECT * FROM prep_brands WHERE is_current = 1),
prep_current_promotions AS
    (SELECT * FROM prep_promotions WHERE is_current = 1),
prep_current_stores AS
    (SELECT * FROM prep_stores WHERE is_current = 1),
prep_current_customers AS
    (SELECT * FROM prep_customers WHERE is_current = 1),
category_to_department AS (
    SELECT DISTINCT ON (cat.category_id)
        cat.category_id,
        cat.category_name,
        dep.department_id,
        dep.department_name
    FROM prep_current_categories cat
    LEFT JOIN prep_current_departments dep
        ON cat.department_id = dep.department_id
    ORDER BY cat.category_id
),
sales_extended AS (
    SELECT
        -- Sales
        sls.sale_id,
        sls."date",
        sls.quantity,
        -- Promotions
        prm.promo_id,
        prm.promo_name,
        prm.discount_percent,
        prm.start_date AS promo_start_date,
        prm.end_date AS promo_end_date,
        prm.promo_duration_days,
        -- Products
        prd.product_id,
        prd.price,
        -- Customers
        cust.customer_id,
        cust.loyalty_level,
        -- Stores
        stor.store_id,
        stor.region,
        -- category_to_department
        c2d.category_id,
        c2d.category_name,
        c2d.department_id,
        c2d.department_name,
        -- Рассчеты
        ((prd.price * sls.quantity::NUMERIC(15,2))::NUMERIC(15,2) * (1 - prm.discount_percent/100))::NUMERIC(15,2) AS revenue_with_discount,
        (prd.price * sls.quantity::NUMERIC(15,2))::NUMERIC(15,2) AS potential_revenue
    FROM prep_promo_sales sls
    LEFT JOIN prep_current_promotions prm ON sls.promo_id = prm.promo_id
    LEFT JOIN prep_current_products prd ON sls.product_id = prd.product_id
    LEFT JOIN prep_current_customers cust ON sls.customer_id = cust.customer_id
    LEFT JOIN prep_current_stores stor ON sls.store_id = stor.store_id
    LEFT JOIN category_to_department c2d ON prd.category_id = c2d.category_id
),
customers_stats AS (
    SELECT count(DISTINCT customer_id) AS total_customers_count
    FROM prep_current_customers
),
sales_agg1 AS (
    SELECT
        q1.promo_id,
        AVG(q1.daily_sales)::NUMERIC(15,4) AS avg_daily_sales,
        AVG(q1.daily_revenue)::NUMERIC(15,4) AS avg_daily_revenue
    FROM (
        SELECT
            sls.promo_id,
            SUM(sls.quantity)::NUMERIC(15,4) AS daily_sales,
            SUM(sls.revenue_with_discount)::NUMERIC(15,4) AS daily_revenue
        FROM sales_extended sls
        GROUP BY
            sls.promo_id,
            sls."date"
    ) AS q1
    GROUP BY
        q1.promo_id
),
sales_agg2 AS (
    SELECT
        sls_ext.promo_id,
        SUM(sls_ext.quantity) AS total_sales_quantity,
        SUM(sls_ext.revenue_with_discount)::NUMERIC(15, 2) AS total_revenue,
        COUNT(DISTINCT sls_ext.customer_id) AS unique_customers_count,
        COUNT(DISTINCT sls_ext.product_id) AS unique_products_count,
        COUNT(DISTINCT sls_ext.store_id) AS unique_stores_count,
        (
            COUNT(DISTINCT sls_ext.customer_id)::NUMERIC(15,4) / -- unique_customers_count
            NULLIF((SELECT total_customers_count::NUMERIC(15,4) FROM customers_stats), 0) -- customers_stats.total_customers_count
        )::NUMERIC(6,4) AS conversion_rate,
        (
            SUM(sls_ext.revenue_with_discount)::NUMERIC(15,4) / -- total_revenue
            NULLIF(SUM(sls_ext.potential_revenue)::NUMERIC(15,4), 0) -- total_potential_revenue
        )::NUMERIC(6,4) AS effectiveness_ratio
    FROM sales_extended sls_ext
    GROUP BY
        sls_ext.promo_id
),
-- group_*
grp_by_category AS (
    SELECT promo_id, category_id,
        SUM(quantity) AS group_sales_quantity,
        SUM(revenue_with_discount)::NUMERIC(15,2) AS group_revenue,
        (SUM(quantity)::NUMERIC(15,4)
            / (NULLIF(SUM(SUM(quantity)) OVER (PARTITION BY promo_id), 0))::NUMERIC(15,4)
        )::NUMERIC(6,4) AS group_sales_ratio
    FROM sales_extended
    GROUP BY promo_id, category_id
),
grp_by_region AS (
    SELECT promo_id, region,
        SUM(quantity) AS group_sales_quantity,
        SUM(revenue_with_discount)::NUMERIC(15,2) AS group_revenue,
        (SUM(quantity)::NUMERIC(15,4)
            / (NULLIF(SUM(SUM(quantity)) OVER (PARTITION BY promo_id), 0))::NUMERIC(15,4)
        )::NUMERIC(6,4) AS group_sales_ratio
    FROM sales_extended
    GROUP BY promo_id, region
),
grp_by_loyalty AS (
    SELECT promo_id, loyalty_level,
        SUM(quantity) AS group_sales_quantity,
        SUM(revenue_with_discount)::NUMERIC(15,2) AS group_revenue,
        (SUM(quantity)::NUMERIC(15,4)
            / (NULLIF(SUM(SUM(quantity)) OVER (PARTITION BY promo_id), 0))::NUMERIC(15,4)
        )::NUMERIC(6,4) AS group_sales_ratio
    FROM sales_extended
    GROUP BY promo_id, loyalty_level
),
-- сетка: промокации, категории, регионы, уровни лояльности
grid AS (
    SELECT -- составляем кобинации полей для акцйий с продажами
        cat.promo_id,
        cat.category_id,
        reg.region,
        loy.loyalty_level
    FROM -- промокации X категории X регионы X уровни лояльности
         (SELECT DISTINCT promo_id, category_id FROM sales_extended) cat -- промокации X категории
    JOIN (SELECT DISTINCT promo_id, region FROM sales_extended) reg -- промокации X регионы
        ON reg.promo_id = cat.promo_id
    JOIN (SELECT DISTINCT promo_id, loyalty_level FROM sales_extended) loy -- промокации X уровни лояльности
        ON loy.promo_id = cat.promo_id
    UNION ALL  -- добавляем акции без продаж
    SELECT -- промокации X (категории=NULL, регионы=NULL, уровни лояльности=NULL)
        p.promo_id, NULL, NULL, NULL -- т.к. у акции нет продаж, то у нее нет категорий, регионов и уровней лояльности
    FROM prep_current_promotions p
    WHERE NOT EXISTS ( -- Выбрать такую акцию, у которой нет ни одной строки в sales_extended (т.е. нет продаж)
        SELECT 1 FROM sales_extended se WHERE se.promo_id = p.promo_id
    )
),
-- обогащаем сетку именами категорий и департаментов
grid_enriched AS (
    SELECT DISTINCT
        g.*,
        c2d.category_name,
        c2d.department_id,
        c2d.department_name
    FROM grid g
    LEFT JOIN category_to_department c2d
        ON g.category_id = c2d.category_id
),
-- обогащаем промоакции метриками по агрецациям
promo_with_agg AS (
    SELECT
        p.promo_id,
        p.promo_name,
        p.discount_percent,
        p.start_date,
        p.end_date,
        p.promo_duration_days,
        -- промо-метрики
        a2.total_sales_quantity,
        a2.total_revenue,
        a1.avg_daily_sales,
        a1.avg_daily_revenue,
        a2.unique_customers_count,
        a2.unique_products_count,
        a2.unique_stores_count,
        a2.conversion_rate,
        a2.effectiveness_ratio,
        (a2.total_revenue::NUMERIC(15,4) / NULLIF(p.promo_duration_days, 0)::NUMERIC(15,4))::NUMERIC(15,4) AS revenue_per_day
    FROM prep_current_promotions p
    LEFT JOIN sales_agg1 a1 ON a1.promo_id = p.promo_id
    LEFT JOIN sales_agg2 a2 ON a2.promo_id = p.promo_id
),
-- метрики промоакций X (сетка + group_*)
final_rows AS (
    SELECT
        -- атрибуты и метрики промо-акций
        pa.promo_id,
        pa.promo_name,
        pa.discount_percent,
        pa.start_date,
        pa.end_date,
        pa.promo_duration_days,
        pa.total_sales_quantity,
        pa.total_revenue,
        pa.avg_daily_sales,
        pa.avg_daily_revenue,
        pa.unique_customers_count,
        pa.unique_products_count,
        pa.unique_stores_count,
        pa.conversion_rate,
        pa.effectiveness_ratio,
        pa.revenue_per_day,
        -- сетка измерений
        ge.category_id,
        ge.category_name,
        ge.department_id,
        ge.department_name,
        ge.region,
        ge.loyalty_level,
        -- group_* через COALESCE (категория // регион // лояльность)
        COALESCE(gc.group_sales_quantity, gr.group_sales_quantity, gl.group_sales_quantity) AS group_sales_quantity,
        COALESCE(gc.group_revenue, gr.group_revenue, gl.group_revenue) AS group_revenue,
        COALESCE(gc.group_sales_ratio, gr.group_sales_ratio, gl.group_sales_ratio) AS group_sales_ratio
    FROM promo_with_agg pa
    JOIN grid_enriched ge ON ge.promo_id = pa.promo_id -- промоакции X сетка
    LEFT JOIN grp_by_category gc -- сетка X метрики по группировке в категориях
        ON gc.promo_id = ge.promo_id
               AND gc.category_id IS NOT DISTINCT FROM ge.category_id -- соединяем, учитывая NULL
    LEFT JOIN grp_by_region gr -- сетка X метрики по группировке в регионах
        ON gr.promo_id = ge.promo_id
               AND gr.region IS NOT DISTINCT FROM ge.region -- соединяем, учитывая NULL
    LEFT JOIN grp_by_loyalty gl -- сетка X метрики по группировке в ур. лояльноси
        ON gl.promo_id = ge.promo_id
               AND gl.loyalty_level IS NOT DISTINCT FROM ge.loyalty_level -- соединяем, учитывая NULL
)
INSERT INTO detn.ilka_promo_effectiveness_analysis
(
    promo_id,
    promo_name,
    discount_percent,
    start_date,
    end_date,
    promo_duration_days,
    total_sales_quantity,
    total_revenue,
    avg_daily_sales,
    avg_daily_revenue,
    unique_customers_count,
    unique_products_count,
    unique_stores_count,
    conversion_rate,
    effectiveness_ratio,
    revenue_per_day,
    category_id,
    category_name,
    department_id,
    department_name,
    region,
    loyalty_level,
    group_sales_quantity,
    group_revenue,
    group_sales_ratio,
    calculation_date,
    etl_version
)
SELECT
    *,
    CURRENT_DATE AS calculation_date,
    (SELECT etl_version FROM constants)
FROM final_rows
;

SELECT * FROM detn.ilka_promo_effectiveness_analysis;

