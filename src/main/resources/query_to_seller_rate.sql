WITH

click_product AS (
    SELECT
        keyword,
        product_id
    FROM `tiki-dwh.search_metrics_overall.base_{{ ds_nodash }}`
    WHERE
        action = "click on result" AND
        product_id IS NOT NULL
),

click_product_seller AS (
    SELECT
        click_product.product_id,
        seller_id,
        seller_name,
        keyword
    FROM `tiki-dwh.dwh.dim_product_full`, click_product
    WHERE
        entity_type LIKE "seller_%" AND
        click_product.product_id = IFNULL(psuper_id, IFNULL(pmaster_id, product_key))
),

count_total AS (
    SELECT
        keyword,
        COUNT(*) AS total
    FROM click_product_seller
    GROUP BY keyword
),

count_store_per_keyword AS (
    SELECT
        keyword,
        seller_name,
        seller_id,
        COUNT(*) AS count_store
    FROM click_product_seller
    GROUP BY keyword, seller_name, seller_id
),

final AS (
    SELECT
        count_total.keyword,
        seller_name,
        seller_id,
        total,
        count_store,
        SAFE_DIVIDE(count_store, total) AS rate
    FROM count_total, count_store_per_keyword
    WHERE
        count_total.keyword = count_store_per_keyword.keyword AND
        total > 50 AND
        seller_name != "Tiki Trading"
    ORDER BY rate DESC, total DESC
)

SELECT
    TRIM(REGEXP_REPLACE(keyword, r"[\t\n\r]", " ")) as query,
    TRIM(REGEXP_REPLACE(seller_name, r"[\t\n\r]", " ")) as seller_name,
    seller_id,
    total,
    count_store,
    rate
FROM final
WHERE rate > 0.8
ORDER BY seller_id

