-- =============================================================================
-- Point 99. L'enfant payé dans les 12 mois (T41)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « DEBOURS, type PM, statut D ou P, mois payé
--   (DEB_DATE_EFFET). » ; « en 2024, avec les seuls enfants payés au dernier
--   trimestre, on serait 1 % sous l'annuaire CNSS 2024 ; avec ceux payés dans
--   l'année, 0,7 % au-dessus. »
-- Requête : T41 selon plusieurs définitions, de 2020 à 2025, dont « payés dans
--   l'année » et « payés au dernier trimestre ».
-- Reprise de : lot6_ecarts_t32_t41.sql, P79 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. Lit DEBOURS :
--   plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc paie (paiements
--   de l'exercice ; la fenêtre de 12 mois en cours d'année reste à écrire).
--   Fiche des règles : règle 55.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- T41 de 2020 à 2025 : allocataires et enfants selon plusieurs définitions.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
pe AS (
    SELECT p.PE_ID,
           p.DOS_CODE,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE = 'FAF'
),
-- un paiement par prestation et par année ; t4 = un mois payé d'octobre à
-- décembre
paie AS (
    SELECT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee,
           MAX(CASE WHEN EXTRACT(MONTH FROM deb.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS t4
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID, EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.DOS_CODE,
           pe.IND_ID_PREST,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND a.annee - pe.annee_naissance <= 21
                THEN 1 ELSE 0 END                                 AS compte,
           CASE WHEN a.annee - pe.annee_naissance <= 21
                THEN 1 ELSE 0 END                                 AS age_ok,
           CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END       AS paye,
           NVL(pa.t4, 0)                                          AS paye_t4,
           CASE WHEN pe.date_saisie <= TO_DATE((a.annee + 1) || '0630', 'YYYYMMDD')
                THEN 1 ELSE 0 END                                 AS vu
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN IND_ID_PREST END) AS enfants_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye_t4 = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_t4,
       COUNT(DISTINCT CASE WHEN compte = 1 AND vu = 1
                           THEN IND_ID_PREST END)                 AS enfants_vus,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1 AND vu = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_vus,
       COUNT(DISTINCT CASE WHEN age_ok = 1 AND paye = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_annee,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN DOS_CODE END)     AS allocataires_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1
                           THEN DOS_CODE END)                     AS allocataires_payes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye_t4 = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_t4,
       COUNT(DISTINCT CASE WHEN compte = 1 AND vu = 1
                           THEN DOS_CODE END)                     AS allocataires_vus,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1 AND vu = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_vus,
       COUNT(DISTINCT CASE WHEN age_ok = 1 AND paye = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_annee
FROM base
GROUP BY annee
ORDER BY annee;

-- Résultat obtenu (P79, exécutée le 01/11/2026 ; détail dans
-- resultats/lot6_P79_t41_variantes.tsv), 2024 :
--   Définition                     Allocataires   Écart       Enfants    Écart
--   publiés (tableaux 36 et 37)    97 378                     268 481
--   payés dans l'année             98 048         +0,7 %      271 577    +1,2 %
--   payés au dernier trimestre     96 384         −1,0 %      263 659    −1,8 %
-- Lecture : au 31/12, l'année civile retombe le mieux sur l'annuaire ; en
--   cours d'année, la règle prend les 12 mois avant la date du tableau.
