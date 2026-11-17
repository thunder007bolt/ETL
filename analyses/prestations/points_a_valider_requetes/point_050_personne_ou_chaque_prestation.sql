-- =============================================================================
-- Point 50. Une personne ou chaque prestation
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « l'ayant droit de la prestation (PRESTATION_ESP,
--   IND_ID_PREST) ; pour un orphelin, l'orphelin, pas son tuteur. » ; « 195
--   victimes ont eu une indemnité journalière liquidée en 2024, pour 371
--   indemnités. »
-- Requête : le détail par code en 2024 : prestations liquidées, personnes
--   liquidées, personnes payées, montant.
-- Reprise de : lot8_evolution_5_ans.sql, P93 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc effectifs (chaque
--   personne une fois par tableau et par année). Fiche des règles : règles
--   10 et 69.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Par code de prestation en 2024 : prestations liquidées, personnes
-- liquidées (IND_ID_PREST distincts), personnes payées, montant des mois de
-- 2024.
-- -----------------------------------------------------------------------------
WITH pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           p.IND_ID_PREST,
           EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION)              AS annee_liquidation
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE IN ('RAV', 'RAS', 'RASC', 'RASO', 'RASA', 'AIJ', 'AAI', 'MIJ',
                         'FAP', 'MAP')
),
paie AS (
    SELECT deb.PE_ID,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID
),
avec_ras AS (
    SELECT DISTINCT IND_ID_PREST FROM pe WHERE TPE_CODE = 'RAS'
)
SELECT pe.TPE_CODE                                                AS code,
       SUM(CASE WHEN pe.annee_liquidation = 2024 THEN 1 ELSE 0 END) AS prestations_liquidees,
       COUNT(DISTINCT CASE WHEN pe.annee_liquidation = 2024
                           THEN pe.IND_ID_PREST END)              AS personnes_liquidees,
       COUNT(DISTINCT CASE WHEN pa.PE_ID IS NOT NULL
                           THEN pe.IND_ID_PREST END)              AS personnes_payees_2024,
       ROUND(SUM(pa.montant))                                     AS montant_mois_2024,
       COUNT(DISTINCT CASE WHEN r.IND_ID_PREST IS NOT NULL
                            AND pe.TPE_CODE <> 'RAS'
                           THEN pe.IND_ID_PREST END)              AS personnes_aussi_en_ras
FROM pe
LEFT JOIN paie     pa ON pa.PE_ID       = pe.PE_ID
LEFT JOIN avec_ras r  ON r.IND_ID_PREST = pe.IND_ID_PREST
GROUP BY pe.TPE_CODE
ORDER BY pe.TPE_CODE;

-- Résultat obtenu (P93, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P93_detail_par_code_2024.tsv), la ligne des IJ d'accident
-- du travail :
--   Code   Prestations liquidées   Personnes liquidées   Personnes payées   Montant des mois de 2024
--   AIJ    371                     195                   155                46 020 175
-- Lecture : une IJ se liquide en plusieurs fois (une par arrêt ou
--   prolongation) : 371 indemnités pour 195 victimes. T52 compte les
--   personnes.
