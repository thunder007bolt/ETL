-- =============================================================================
-- Point 49. Le bénéficiaire de l'année
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « une personne compte l'année où une prestation du
--   tableau lui est notifiée (PRESTATION_ESP, PE_DATE_NOTIFICATION). » ;
--   « l'annuaire CNSS 2024 fait pareil pour T50 : 831 allocataires de
--   vieillesse liquidés en 2024 pour 833 publiés (tableau 24). En comptant
--   plutôt les personnes payées, T51 aurait 420 personnes en 2024, au lieu de
--   571. »
-- Requête : T50 à T55 de 2020 à 2024 : personnes liquidées, payées, saisies,
--   et montants.
-- Reprise de : lot8_evolution_5_ans.sql, P92 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, blocs liquides et
--   effectifs (la personne compte l'année de sa notification). Fiche des
--   règles : règle 69.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Par année et par tableau (T50 à T55) : prestations et personnes liquidées
-- (première notification dans l'année), par sexe, personnes saisies,
-- personnes payées, montant des mois de l'année, montant saisi dans l'année.
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
-- resultats/lot8_P92_t50_t55_par_annee.tsv), en 2024 :
--   Tableau   Liquidées   Payées   Saisies
--   T50       831         753      873
--   T51       571         420      585
--   T52       195         155      224
--   T53        53          56       55
--   T54       3 890       4 881    3 979
--   T55       3 912       6 192    4 010
-- Face au tableau 24 de l'annuaire CNSS 2024 (p. 19) : 833 allocations de
-- vieillesse liquidées en 2024.
-- Lecture : à la notification, T50 retombe sur l'annuaire (831 pour 833) ;
--   avec les personnes payées, T51 aurait 420 personnes au lieu de 571.
