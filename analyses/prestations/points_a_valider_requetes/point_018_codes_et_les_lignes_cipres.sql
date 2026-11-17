-- =============================================================================
-- Point 18. Les codes et les lignes CIPRES
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « chaque prestation a un code (TPE_CODE), par exemple
--   RPV pour la vieillesse ; la liste de chaque tableau est à la fin du
--   document. » ; « l'annuaire CNSS 2024 fait pareil, avec une ligne par
--   code : fin 2024, 31 093 pensionnés de vieillesse et de retraite anticipée
--   dans la base, 30 808 publiés. »
-- Requêtes :
--   1. La liste complète des codes de prestation en espèces, avec leur
--      libellé et leur nombre.
--   2. Le stock au 31/12 de chaque tableau, de 2020 à 2025.
-- Reprise de : lot3_listes_de_codes.sql, P34 (exécutée le 29/10/2026) ;
--   lot5_beneficiaires_31_12.sql, P74 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 2
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : le bloc perimetre de chaque requête proposée
--   (t25_requete_proposee.sql à t50_t57_requete_proposee.sql) range chaque
--   code sur sa ligne. Fiche des règles : règles 6, 28, 48, 61 et 68.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Types de prestation en espèces (codes TPE_CODE) : libellé,
-- branche, nombre de prestations.
-- -----------------------------------------------------------------------------
SELECT t.TPE_CODE, t.TDOS_CODE, t.TPE_DESCRIPTION, t.TPE_FREQ_PAIEMENT,
       (SELECT COUNT(*) FROM USER_DWH.PRESTATION_ESP pe WHERE pe.TPE_CODE = t.TPE_CODE) AS nb_prestations
FROM USER_DWH.TYPE_PRESTATION_ESP t
ORDER BY t.TDOS_CODE, t.TPE_CODE;

-- Résultat obtenu (P34, exécutée le 29/10/2026) : 28 codes, 947 953
-- prestations. Pensions (V, 17 codes) : RPV vieillesse 46 836, RPRA retraite
-- anticipée 176, RPI invalidité 231, RPSV veuve 25 541, RPSF veuf 424, RPSE
-- orphelin 29 689, RPA ascendant 81, RAV allocation de vieillesse 12 833,
-- RAS 864 et ses parts RASC 1 469, RASO 7 580, RASA 327, RBEC 68 836, RME 762,
-- RME-COL 18, RCCM 70, RRC 17 ; accidents du travail (A, 7 codes) et
-- prestations familiales (le reste) : voir le lot.


-- -----------------------------------------------------------------------------
-- Requête 2. Par année et par tableau : prestations en cours au 31/12 et
-- personnes comptées.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
perimetre AS (
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
pe AS (
    SELECT p.PE_ID,
           p.DOS_CODE,
           pm.tableau,
           p.IND_ID_PREST,
           p.IND_ID_BENEF,
           d.IND_ID                                               AS ind_dossier,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre             pm ON pm.code   = p.TPE_CODE
    JOIN USER_DWH.DOSSIER      d  ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID   = p.IND_ID_PREST
),
paie AS (
    SELECT DISTINCT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
),
base AS (
    SELECT a.annee,
           pe.tableau,
           pe.DOS_CODE,
           pe.IND_ID_PREST,
           pe.IND_ID_BENEF,
           pe.ind_dossier,
           a.annee - pe.annee_naissance                           AS age,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                THEN 1 ELSE 0 END                                 AS en_cours,
           CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END       AS paye
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
),
compte AS (
    SELECT b.*,
           CASE WHEN en_cours = 1
                 AND (tableau NOT IN ('T35', 'T39') OR age < 25)
                 AND (tableau <> 'T41' OR age <= 21)
                THEN 1 ELSE 0 END                                 AS compte
    FROM base b
)
SELECT annee,
       tableau,
       SUM(en_cours)                                              AS prestations_en_cours,
       SUM(compte)                                                AS prestations_comptees,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN IND_ID_PREST END) AS personnes_comptees,
       COUNT(DISTINCT CASE WHEN compte = 1 AND tableau = 'T41'
                           THEN DOS_CODE END)                     AS dossiers_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND tableau = 'T41'
                           THEN ind_dossier END)                  AS titulaires_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND tableau = 'T41'
                           THEN IND_ID_BENEF END)                 AS payes_comptes,
       COUNT(DISTINCT CASE WHEN paye = 1 THEN IND_ID_PREST END)   AS personnes_payees_annee
FROM compte
GROUP BY annee, tableau
ORDER BY tableau, annee;

-- Résultat obtenu (P74, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P74_stock_2020_2025.tsv) : T32 (RPV et RPRA) au 31/12/2024,
-- 31 093 personnes.
-- Face au tableau 18 de l'annuaire CNSS 2024 (p. 16), une ligne par type :
-- 30 730 retraités de vieillesse et 78 en retraite anticipée, soit 30 808.
-- Lecture : en rangeant chaque code sur sa ligne, la base retombe à 0,9 % près
--   sur l'annuaire.
