-- =============================================================================
-- Point 81. Le dénominateur du taux de contrôle
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T28 à
--   T30.
-- Ce que dit le point : « un dossier payé a un débours PM au statut D ou P
--   dans la période (DEBOURS). Une coordination payée par l'autre caisse
--   (DOS_CATEGORIE = COOR_AC) est en portefeuille sans rien recevoir de la
--   CNSS. »
-- Requêtes :
--   1. Les dossiers payés de 2025 par branche, au sens du point (débours PM
--      au statut D ou P).
--   2. Les pensions de T32 en 2025 par catégorie du dossier : comptées, et
--      payées par la CNSS.
-- Reprise de : la requête 1 est nouvelle (P13 comptait tout débours) ; la
--   requête 2 est lot5_beneficiaires_31_12.sql, P78 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : aucune requête proposée, T28 à T30 ne sont pas
--   produits. Fiche des règles : règles 38 et 41.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Dossiers payés en 2025 : au moins un débours de paiement
-- (DEB_TYPE = PM) au statut D ou P dont le mois payé (DEB_DATE_EFFET) tombe
-- en 2025, par branche (CODE_BRANCHE), avec le compte de P13 (tout débours)
-- pour comparer.
-- -----------------------------------------------------------------------------
SELECT TO_CHAR(CODE_BRANCHE)                                      AS branche,
       COUNT(DISTINCT CASE WHEN DEB_TYPE = 'PM' AND DEB_STATUT IN ('D', 'P')
                           THEN DOS_CODE END)                     AS dossiers_payes_pm,
       COUNT(DISTINCT DOS_CODE)                                   AS dossiers_tout_debours
FROM USER_DWH.DEBOURS
WHERE DEB_DATE_EFFET >= DATE '2025-01-01'
  AND DEB_DATE_EFFET <  DATE '2026-01-01'
GROUP BY CODE_BRANCHE
ORDER BY 1;

-- Résultat : pas encore exécutée. La colonne dossiers_tout_debours doit
--   redonner P13 (lot 1) : A 2 134, F 100 113, M 5 227, V 47 732.


-- -----------------------------------------------------------------------------
-- Requête 2. Pensions de T32 au 31/12/2025 par catégorie du dossier
-- (DOS_CATEGORIE) : comptées, payées par la CNSS, montant.
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
-- resultats/lot5_P78_coordination_2025.tsv) :
--   Catégorie   Pensions comptées   Payées par la CNSS   Montant
--   COOR_AC     2 901               0                    0
--   (les autres catégories : voir point_019_coordination_dans_t25_a_t27.sql)
-- Lecture : les 2 901 pensions COOR_AC sont en portefeuille sans aucun
--   paiement de la CNSS. Avec « dossiers payés », un contrôle sur ces dossiers
--   ne compte pas ; avec « en portefeuille », il compterait.
