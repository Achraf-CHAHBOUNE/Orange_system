CREATE OR REPLACE VIEW vw_traffic_combined AS
SELECT 
    date,
    node,
    operator,
    suffix,
    traffic AS total_traffic,
    tentative_appel AS total_tentative_appel,
    appel_repondu AS total_appel_repondu,
    0 AS total_appel_non_repondu,
    'Inbound' AS type
FROM (
    SELECT 
        ks.date,
        ks.node,
        te.operator,
        te.suffix,
        te.traffic,
        te.tentative_appel,
        te.appel_repondu
    FROM kpi_summary ks
    JOIN traffic_entree te ON ks.id = te.kpi_id
    WHERE ks.date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) 
    AND (te.traffic <> 0 OR te.tentative_appel <> 0 OR te.appel_repondu <> 0)
) inbound

UNION ALL

SELECT 
    date,
    node,
    operator,
    suffix,
    traffic AS total_traffic,
    tentative_appel AS total_tentative_appel,
    appel_repondu AS total_appel_repondu,
    appel_non_repondu AS total_appel_non_repondu,
    'Outbound' AS type
FROM (
    SELECT 
        ks.date,
        ks.node,
        ts.operator,
        ts.suffix,
        ts.traffic,
        ts.tentative_appel,
        ts.appel_repondu,
        ts.appel_non_repondu
    FROM kpi_summary ks
    JOIN traffic_sortie ts ON ks.id = ts.kpi_id
    WHERE ks.date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) 
    AND (ts.traffic <> 0 OR ts.tentative_appel <> 0 OR ts.appel_repondu <> 0 OR ts.appel_non_repondu <> 0)
) outbound

UNION ALL

SELECT 
    date,
    node,
    operator,
    suffix,
    total_traffic,
    total_tentative_appel,
    total_appel_repondu,
    total_appel_non_repondu,
    'Total' AS type
FROM (
    SELECT 
        date,
        node,
        operator,
        suffix,
        SUM(entree_traffic) + SUM(sortie_traffic) AS total_traffic,
        SUM(entree_tentative_appel) + SUM(sortie_tentative_appel) AS total_tentative_appel,
        SUM(entree_appel_repondu) + SUM(sortie_appel_repondu) AS total_appel_repondu,
        SUM(sortie_appel_non_repondu) AS total_appel_non_repondu
    FROM (
        SELECT 
            ks.date AS date, ks.node AS node, te.operator AS operator, te.suffix AS suffix,
            te.traffic AS entree_traffic, te.tentative_appel AS entree_tentative_appel, 
            te.appel_repondu AS entree_appel_repondu, 0 AS sortie_traffic, 
            0 AS sortie_tentative_appel, 0 AS sortie_appel_repondu, 0 AS sortie_appel_non_repondu
        FROM kpi_summary ks
        JOIN traffic_entree te ON ks.id = te.kpi_id
        WHERE ks.date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) 
        AND (te.traffic <> 0 OR te.tentative_appel <> 0 OR te.appel_repondu <> 0)
        
        UNION ALL
        
        SELECT 
            ks.date AS date, ks.node AS node, ts.operator AS operator, ts.suffix AS suffix,
            0 AS entree_traffic, 0 AS entree_tentative_appel, 0 AS entree_appel_repondu,
            ts.traffic AS sortie_traffic, ts.tentative_appel AS sortie_tentative_appel, 
            ts.appel_repondu AS sortie_appel_repondu, ts.appel_non_repondu AS sortie_appel_non_repondu
        FROM kpi_summary ks
        JOIN traffic_sortie ts ON ks.id = ts.kpi_id
        WHERE ks.date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) 
        AND (ts.traffic <> 0 OR ts.tentative_appel <> 0 OR ts.appel_repondu <> 0 OR ts.appel_non_repondu <> 0)
    ) combined
    GROUP BY date, node, operator, suffix
) total;