-- =============================================================================
-- Point 68. Les dates de naissance approchées
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « INDIVIDU, IND_DATE_NAISSANCE : parmi les personnes
--   de 100 ans et plus, plusieurs nées la même année partagent la même date,
--   rarement un 1er janvier. »
-- Requête : les bénéficiaires comptés fin 2024 qui ont 100 ans ou plus, ou un
--   âge négatif, par année de naissance : personnes, dates différentes, nés un
--   1er janvier.
-- Reprise de : lot6_ecarts_t32_t41.sql, P80 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement ; les dates de naissance ne sortent que
--   par année, jamais par personne.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc tranche (l'âge
--   se calcule sur l'année de naissance). Fiche des règles : règles 12 et 52.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Bénéficiaires comptés au 31/12/2024 qui ont 100 ans ou plus ou un âge
-- négatif, par tableau et par année de naissance.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T41', 'FAF'  FROM DUAL
),
compte AS (
    SELECT DISTINCT
           pm.tableau,
           p.IND_ID_PREST,
           TRUNC(i.IND_DATE_NAISSANCE)                            AS date_naissance,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2024-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
      AND (pm.tableau NOT IN ('T35', 'T39')
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
      AND (pm.tableau <> 'T41'
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) <= 21)
)
SELECT tableau,
       NVL(TO_CHAR(EXTRACT(YEAR FROM date_naissance)), '(vide)')  AS annee_naissance,
       COUNT(*)                                                   AS personnes,
       COUNT(DISTINCT date_naissance)                             AS dates_distinctes,
       SUM(CASE WHEN TO_CHAR(date_naissance, 'DDMM') = '0101'
                THEN 1 ELSE 0 END)                                AS nes_un_1er_janvier
FROM compte
WHERE age IS NULL OR age >= 100 OR age < 0
GROUP BY tableau, EXTRACT(YEAR FROM date_naissance)
ORDER BY tableau, EXTRACT(YEAR FROM date_naissance);

-- Résultat obtenu (P80, exécutée le 01/11/2026 ; détail dans
-- resultats/lot6_P80_dates_naissance_2024.tsv) : chaque année, plusieurs
-- personnes ont la même date de naissance, qui n'est pas le 1er janvier
-- (T32 : 14 nés en 1922 sur une seule date ; T34 : 21 nés en 1922, une
-- date). Presque personne n'est né un 1er janvier (4 veuves de 1900, 2 nées
-- en 1920 et 1924).
-- Lecture : ce sont sans doute des dates approchées (« né vers ») ; l'âge se
--   calculant sur l'année, elles ne changent rien si l'année est juste.
