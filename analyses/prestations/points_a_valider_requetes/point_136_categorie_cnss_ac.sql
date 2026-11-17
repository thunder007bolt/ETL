-- =============================================================================
-- Point 136. La catégorie CNSS_AC
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « DOSSIER, DOS_CATEGORIE = CNSS_AC. » ; « fin 2025, 34
--   pensions de vieillesse (T32, dont 2 payées par la CNSS) et 79 pensions de
--   veuves et de veufs (T34) sont dans des dossiers CNSS_AC. »
-- Requêtes :
--   1. Les dossiers de pension par catégorie.
--   2. Les pensions de T32 à T36 en 2025 par catégorie du dossier.
-- Reprise de : lot3b_controles_t25.sql, P48 (exécutée le 30/10/2026) ;
--   lot5_beneficiaires_31_12.sql, P78 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 2 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t25_requete_proposee.sql, blocs prest et recep_cand
--   (les CNSS_AC sont écartés de T25) ; t32_t41_requete_proposee.sql, bloc
--   en_cours (ils restent au 31/12). Fiche des règles : règles 18 et 58.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Dossiers de pension (TDOS_CODE = V) par catégorie.
-- -----------------------------------------------------------------------------
WITH notif AS (
    SELECT DOS_CODE, TRUNC(MIN(PE_DATE_NOTIFICATION)) AS date_liquidation
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
)
SELECT NVL(d.DOS_CATEGORIE, '(vide)')                                          AS categorie,
       COUNT(*)                                                                AS nb_dossiers,
       SUM(CASE WHEN d.DOS_CODE_AUTRE_CAISSE IS NOT NULL THEN 1 ELSE 0 END)    AS avec_code_autre_caisse,
       SUM(CASE WHEN r.RD_ID IS NOT NULL THEN 1 ELSE 0 END)                    AS avec_reception,
       SUM(CASE WHEN n.date_liquidation IS NOT NULL THEN 1 ELSE 0 END)         AS avec_notification,
       SUM(CASE WHEN r.RD_DATE_RECEPTION >= DATE '2025-01-01'
                 AND r.RD_DATE_RECEPTION <  DATE '2026-01-01' THEN 1 ELSE 0 END) AS deposes_2025,
       SUM(CASE WHEN n.date_liquidation >= DATE '2025-01-01'
                 AND n.date_liquidation <  DATE '2026-01-01' THEN 1 ELSE 0 END)  AS liquides_2025,
       ROUND(MEDIAN(CASE WHEN n.date_liquidation >= DATE '2025-01-01'
                          AND n.date_liquidation <  DATE '2026-01-01'
                         THEN n.date_liquidation - TRUNC(r.RD_DATE_RECEPTION)
                    END))                                                      AS duree_mediane_2025_j
FROM USER_DWH.DOSSIER d
LEFT JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID    = d.RD_ID
LEFT JOIN notif                      n ON n.DOS_CODE = d.DOS_CODE
WHERE d.TDOS_CODE = 'V'
GROUP BY NVL(d.DOS_CATEGORIE, '(vide)')
ORDER BY nb_dossiers DESC;

-- Résultat obtenu (P48, exécutée le 30/10/2026) : CNSS_AC 153 dossiers, 12
-- avec réception, 128 avec notification ; aucun déposé en 2025, 1 liquidé.


-- -----------------------------------------------------------------------------
-- Requête 2. Par tableau et par catégorie : pensions comptées au 31/12/2025,
-- payées par la CNSS, montant.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
),
compte AS (
    SELECT p.PE_ID,
           pm.tableau,
           NVL(d.DOS_CATEGORIE, '(sans catégorie)')               AS categorie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2025-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2025-12-31')
      AND (pm.tableau <> 'T35'
           OR 2025 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
),
paie AS (
    SELECT deb.PE_ID, SUM(deb.DEB_MONTANT) AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2025-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM compte)
    GROUP BY deb.PE_ID
)
SELECT c.tableau,
       c.categorie,
       COUNT(*)                                                   AS pensions_comptees,
       SUM(CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END)      AS payees_par_la_cnss_2025,
       ROUND(SUM(pa.montant))                                     AS montant_2025
FROM compte c
LEFT JOIN paie pa ON pa.PE_ID = c.PE_ID
GROUP BY c.tableau, c.categorie
ORDER BY c.tableau, pensions_comptees DESC;

-- Résultat obtenu (P78, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P78_coordination_2025.tsv), catégorie CNSS_AC : T32 34
-- pensions, 2 payées par la CNSS ; T34 79 pensions, 69 payées ; T35 2.
-- Lecture : le sens de CNSS_AC est à faire dire par la DSI ; ces dossiers
--   sont retirés de T25 à T27 mais gardés au 31/12 (point 101).
