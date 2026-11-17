-- =============================================================================
-- Point 117. Le montant liquidé de la maternité
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « les paiements (DEBOURS, DEB_TYPE = PM, statut D ou
--   P) des prestations MIJ, MAP et FAP notifiées dans l'année
--   (PRESTATION_ESP, PE_DATE_NOTIFICATION), quelle que soit la date du
--   paiement. » ; « indemnités de maternité de 2024 : 2,75 Md payés dans
--   l'année, pour 2,87 Md de « montants perçus » dans l'annuaire CNSS 2024
--   (tableau 35). Le montant liquidé reste à mesurer. »
-- Requêtes :
--   1. T50 à T55 de 2020 à 2024 : personnes et montants de l'année.
--   2. Le montant liquidé de 2024 : tout ce qui est payé, à n'importe quelle
--      date, pour les prestations de maternité notifiées en 2024.
-- Reprise de : lot8_evolution_5_ans.sql, P92 (exécutée le 01/11/2026) ; la
--   requête 2 est nouvelle.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Les deux
--   requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc montants, qui
--   prend encore les paiements de l'année, pas le montant liquidé. Fiche des
--   règles : règle 70.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par année et par tableau (T50 à T55) : personnes liquidées et
-- payées, montant des mois de l'année, montant saisi dans l'année.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
perimetre AS (
    SELECT 'T50' AS tableau, 'RAV' AS code FROM DUAL
    UNION ALL SELECT 'T51', 'RAS'  FROM DUAL
    UNION ALL SELECT 'T51', 'RASC' FROM DUAL
    UNION ALL SELECT 'T51', 'RASO' FROM DUAL
    UNION ALL SELECT 'T51', 'RASA' FROM DUAL
    UNION ALL SELECT 'T52', 'AIJ'  FROM DUAL
    UNION ALL SELECT 'T53', 'AAI'  FROM DUAL
    UNION ALL SELECT 'T54', 'MIJ'  FROM DUAL
    UNION ALL SELECT 'T55', 'FAP'  FROM DUAL
    UNION ALL SELECT 'T55', 'MAP'  FROM DUAL
),
pe AS (
    SELECT p.PE_ID,
           pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION)              AS annee_liquidation,
           EXTRACT(YEAR FROM p.PE_DATE_INSERT)                    AS annee_saisie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
),
paie AS (
    SELECT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee_mois,
           EXTRACT(YEAR FROM deb.DEB_DATE_INSERT)                 AS annee_saisie,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID, EXTRACT(YEAR FROM deb.DEB_DATE_EFFET),
             EXTRACT(YEAR FROM deb.DEB_DATE_INSERT)
),
liquidees AS (
    SELECT annee_liquidation                                      AS annee,
           tableau,
           COUNT(*)                                               AS prestations,
           COUNT(DISTINCT IND_ID_PREST)                           AS personnes,
           COUNT(DISTINCT CASE WHEN sexe = '1' THEN IND_ID_PREST END) AS sexe_1,
           COUNT(DISTINCT CASE WHEN sexe = '2' THEN IND_ID_PREST END) AS sexe_2
    FROM pe
    GROUP BY annee_liquidation, tableau
),
saisies AS (
    SELECT annee_saisie AS annee, tableau, COUNT(DISTINCT IND_ID_PREST) AS personnes
    FROM pe
    GROUP BY annee_saisie, tableau
),
payees AS (
    SELECT pa.annee_mois                                          AS annee,
           pe.tableau,
           COUNT(DISTINCT pe.IND_ID_PREST)                        AS personnes,
           COUNT(DISTINCT CASE WHEN pe.sexe = '1' THEN pe.IND_ID_PREST END) AS sexe_1,
           COUNT(DISTINCT CASE WHEN pe.sexe = '2' THEN pe.IND_ID_PREST END) AS sexe_2,
           SUM(pa.montant)                                        AS montant
    FROM paie pa
    JOIN pe ON pe.PE_ID = pa.PE_ID
    GROUP BY pa.annee_mois, pe.tableau
),
payees_saisie AS (
    SELECT pa.annee_saisie AS annee, pe.tableau, SUM(pa.montant) AS montant
    FROM paie pa
    JOIN pe ON pe.PE_ID = pa.PE_ID
    GROUP BY pa.annee_saisie, pe.tableau
)
SELECT a.annee,
       t.tableau,
       NVL(l.prestations, 0)                                      AS prestations_liquidees,
       NVL(l.personnes, 0)                                        AS personnes_liquidees,
       NVL(l.sexe_1, 0)                                           AS liquidees_sexe_1,
       NVL(l.sexe_2, 0)                                           AS liquidees_sexe_2,
       NVL(s.personnes, 0)                                        AS personnes_saisies,
       NVL(p.personnes, 0)                                        AS personnes_payees,
       NVL(p.sexe_1, 0)                                           AS payees_sexe_1,
       NVL(p.sexe_2, 0)                                           AS payees_sexe_2,
       ROUND(NVL(p.montant, 0))                                   AS montant_mois_annee,
       ROUND(NVL(ps.montant, 0))                                  AS montant_saisi_annee
FROM annees a
CROSS JOIN (SELECT DISTINCT tableau FROM perimetre) t
LEFT JOIN liquidees     l  ON l.annee  = a.annee AND l.tableau  = t.tableau
LEFT JOIN saisies       s  ON s.annee  = a.annee AND s.tableau  = t.tableau
LEFT JOIN payees        p  ON p.annee  = a.annee AND p.tableau  = t.tableau
LEFT JOIN payees_saisie ps ON ps.annee = a.annee AND ps.tableau = t.tableau
ORDER BY t.tableau, a.annee;

-- Résultat obtenu (P92, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P92_t50_t55_par_annee.tsv), T54 (IJ de maternité) en 2024 :
-- 2 745 737 842 pour les mois de 2024, 2 749 474 700 saisis en 2024.
-- Face au tableau 35 de l'annuaire CNSS 2024 (p. 30) : 2 865 630 056 de
-- montants perçus.


-- -----------------------------------------------------------------------------
-- Requête 2. Montant liquidé de 2024 : pour les prestations MIJ, MAP et FAP
-- dont la première notification tombe en 2024, la somme de tous leurs
-- paiements (PM au statut D ou P), quelle que soit leur date.
-- -----------------------------------------------------------------------------
WITH liquidees AS (
    SELECT p.PE_ID,
           p.TPE_CODE
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE IN ('MIJ', 'MAP', 'FAP')
      AND p.PE_DATE_NOTIFICATION >= DATE '2024-01-01'
      AND p.PE_DATE_NOTIFICATION <  DATE '2025-01-01'
)
SELECT CASE WHEN l.TPE_CODE = 'MIJ' THEN 'T54' ELSE 'T55' END       AS tableau,
       COUNT(DISTINCT l.PE_ID)                                    AS prestations_liquidees,
       ROUND(SUM(deb.DEB_MONTANT))                                AS montant_liquide,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_EFFET >= DATE '2024-01-01'
                       AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS dont_mois_de_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_EFFET >= DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS dont_mois_de_2025_ou_apres
FROM liquidees l
LEFT JOIN USER_DWH.DEBOURS deb ON deb.PE_ID      = l.PE_ID
                              AND deb.DEB_TYPE   = 'PM'
                              AND deb.DEB_STATUT IN ('D', 'P')
GROUP BY CASE WHEN l.TPE_CODE = 'MIJ' THEN 'T54' ELSE 'T55' END
ORDER BY 1;

-- Résultat : pas encore exécutée.
-- Lecture attendue : si montant_liquide de T54 retombe près des 2 865 630 056
--   du tableau 35, l'annuaire publie le montant liquidé.
