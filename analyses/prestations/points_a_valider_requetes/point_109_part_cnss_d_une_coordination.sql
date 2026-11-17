-- =============================================================================
-- Point 109. La part CNSS d'une coordination (T42)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « pour les mois, PE_MOIS_COTISATION moins
--   PE_COT_AUTRE (voir le point 11) ; pour le montant, les paiements de la
--   CNSS (DEBOURS), donc rien quand DOS_CATEGORIE = COOR_AC. » ; « 234 nouveaux
--   retraités payés par l'autre caisse en 2024 comptent dans T42, avec un
--   montant nul. »
-- Requêtes :
--   1. Les entrées de T42 à T49, de 2020 à 2025, dont celles en coordination.
--   2. Le montant servi aux nouveaux de 2024, et les nouveaux sans paiement.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P82 et P85 (exécutées le
--   01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. La
--   requête 2 lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, blocs entree
--   (PE_MOIS_COTISATION moins PE_COT_AUTRE) et paie (les paiements de la
--   CNSS). Fiche des règles : règles 11 et 66.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par année et par tableau : personnes saisies, première fois,
-- notifiées, et nouveaux en coordination payée par une autre caisse
-- (COOR_AC) ou instruite par une autre caisse (CNSS_AC).
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
perimetre AS (
    SELECT 'T42' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T42', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T44', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T45', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T46', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T47', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T48', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T49', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T49', 'ARSF' FROM DUAL
),
pe AS (
    SELECT pm.tableau,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           TRUNC(p.PE_DATE_NOTIFICATION)                          AS date_notification,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces,
           NVL(d.DOS_CATEGORIE, '(sans catégorie)')               AS categorie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre             pm ON pm.code   = p.TPE_CODE
    JOIN USER_DWH.DOSSIER      d  ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID   = p.IND_ID_PREST
),
-- la première prestation de la personne dans le tableau
premiere AS (
    SELECT tableau, IND_ID_PREST, MIN(date_saisie) AS premiere_saisie
    FROM pe
    GROUP BY tableau, IND_ID_PREST
),
base AS (
    SELECT a.annee,
           pe.tableau,
           pe.IND_ID_PREST,
           pe.categorie,
           CASE WHEN EXTRACT(YEAR FROM pe.date_saisie) = a.annee
                THEN 1 ELSE 0 END                                 AS saisie,
           CASE WHEN EXTRACT(YEAR FROM pe.date_notification) = a.annee
                THEN 1 ELSE 0 END                                 AS notifiee,
           CASE WHEN EXTRACT(YEAR FROM pr.premiere_saisie) = a.annee
                THEN 1 ELSE 0 END                                 AS premiere_fois,
           CASE WHEN pe.date_radiation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                  OR pe.date_deces     <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                  OR (    pe.date_suspension <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      AND (   pe.date_reactivation IS NULL
                           OR pe.date_reactivation < pe.date_suspension
                           OR pe.date_reactivation > TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                THEN 1 ELSE 0 END                                 AS sortie
    FROM annees a
    CROSS JOIN pe
    JOIN premiere pr ON pr.tableau      = pe.tableau
                    AND pr.IND_ID_PREST = pe.IND_ID_PREST
)
SELECT annee,
       tableau,
       SUM(saisie)                                                AS prestations_saisies,
       COUNT(DISTINCT CASE WHEN saisie = 1 THEN IND_ID_PREST END) AS personnes_saisies,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND premiere_fois = 1
                           THEN IND_ID_PREST END)                 AS premiere_fois,
       COUNT(DISTINCT CASE WHEN notifiee = 1
                           THEN IND_ID_PREST END)                 AS personnes_notifiees,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND sortie = 1
                           THEN IND_ID_PREST END)                 AS sorties_avant_31_12,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND categorie = 'COOR_AC'
                           THEN IND_ID_PREST END)                 AS coor_ac,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND categorie = 'CNSS_AC'
                           THEN IND_ID_PREST END)                 AS cnss_ac
FROM base
GROUP BY annee, tableau
ORDER BY tableau, annee;

-- Résultat obtenu (P82, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P82_entrees_2020_2025.tsv) : en 2024, 234 nouveaux
-- retraités de T42 en COOR_AC (sur 2 463 en première fois) ; aucune rente.


-- -----------------------------------------------------------------------------
-- Requête 2. Prestations saisies en 2024, par tableau : montant des mois de
-- 2024, saisi en 2024, rappel ; personnes sans paiement d'un mois de 2024.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T42' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T42', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T44', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T45', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T46', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T47', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T48', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T49', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T49', 'ARSF' FROM DUAL
),
nouveaux AS (
    SELECT p.PE_ID,
           pm.tableau,
           p.IND_ID_PREST
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    WHERE p.PE_DATE_INSERT >= DATE '2024-01-01'
      AND p.PE_DATE_INSERT <  DATE '2025-01-01'
),
paie AS (
    SELECT deb.PE_ID,
           deb.DEB_MONTANT                                        AS montant,
           TRUNC(deb.DEB_DATE_EFFET)                              AS mois,
           TRUNC(deb.DEB_DATE_INSERT)                             AS saisie
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.PE_ID IN (SELECT PE_ID FROM nouveaux)
)
SELECT n.tableau,
       COUNT(DISTINCT n.IND_ID_PREST)                             AS personnes,
       COUNT(DISTINCT CASE WHEN pa.mois >= DATE '2024-01-01'
                            AND pa.mois <  DATE '2025-01-01'
                           THEN n.IND_ID_PREST END)               AS personnes_payees_mois_2024,
       ROUND(SUM(CASE WHEN pa.mois >= DATE '2024-01-01'
                       AND pa.mois <  DATE '2025-01-01'
                      THEN pa.montant END))                       AS montant_mois_2024,
       ROUND(SUM(CASE WHEN pa.mois >= DATE '2024-01-01'
                       AND pa.mois <  DATE '2025-01-01'
                       AND pa.saisie >= DATE '2025-01-01'
                      THEN pa.montant END))                       AS dont_saisi_en_2025_ou_apres,
       ROUND(SUM(CASE WHEN pa.saisie >= DATE '2024-01-01'
                       AND pa.saisie <  DATE '2025-01-01'
                      THEN pa.montant END))                       AS montant_saisi_2024,
       ROUND(SUM(CASE WHEN pa.saisie >= DATE '2024-01-01'
                       AND pa.saisie <  DATE '2025-01-01'
                       AND pa.mois   <  DATE '2024-01-01'
                      THEN pa.montant END))                       AS dont_mois_avant_2024
FROM nouveaux n
LEFT JOIN paie pa ON pa.PE_ID = n.PE_ID
GROUP BY n.tableau
ORDER BY n.tableau;

-- Résultat obtenu (P85, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P85_montants_nouveaux_2024.tsv) : 321 nouveaux retraités de
-- T42 sans aucun paiement d'un mois de 2024, surtout les 234 COOR_AC.
-- Lecture : les 234 nouveaux retraités payés par l'autre caisse comptent
--   dans T42, avec un montant nul.
