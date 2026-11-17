-- =============================================================================
-- Point 101. Les dossiers transférés à une autre caisse
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « DOSSIER, DOS_CATEGORIE = CNSS_AC, lu « transféré à
--   une autre caisse » ; son sens est demandé à la DSI. » ; « fin 2025, 34
--   pensions de vieillesse dans ce cas (2 payées par la CNSS) et 79 pensions
--   de veuve ou de veuf. »
-- Requête : les pensions de T32 à T36 en 2025 par catégorie du dossier :
--   comptées, payées par la CNSS, montant.
-- Reprise de : lot5_beneficiaires_31_12.sql, P78 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc en_cours (aucun
--   filtre sur DOS_CATEGORIE : les CNSS_AC restent comptés). Fiche des
--   règles : règle 58.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Par tableau et par catégorie du dossier (DOS_CATEGORIE) : pensions comptées
-- au 31/12/2025, payées par la CNSS en 2025, montant.
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
-- resultats/lot5_P78_coordination_2025.tsv), catégorie CNSS_AC :
--   Tableau   Pensions comptées   Payées par la CNSS   Montant 2025
--   T32       34                  2                    1 553 100
--   T34       79                  69                   7 842 164
--   T35        2                   1                       23 400
-- Lecture : 34 pensions de vieillesse et 79 pensions de veuve ou de veuf sont
--   dans des dossiers CNSS_AC ; la plupart des veuves sont encore payées par
--   la CNSS : le sens de CNSS_AC est à préciser par la DSI.
