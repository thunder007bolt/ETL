-- =============================================================================
-- Point 13. Les dossiers (T25 à T27)
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « une demande est déposée à sa réception
--   (RECEPTION_DOSSIER, RD_DATE_RECEPTION), liquidée à la première
--   notification (PRESTATION_ESP, PE_DATE_NOTIFICATION), rejetée si elle est
--   dans DOSSIER_REJETE, sinon en instance. » ; « l'annuaire CNSS 2024 fait
--   pareil : 831 personnes liquidées en 2024 pour une allocation de vieillesse
--   (RAV), pour 833 publiées (tableau 24). »
-- Requête : les prestations de 2024 par code, dont les allocations de
--   vieillesse liquidées (première notification en 2024).
-- Reprise de : lot8_evolution_5_ans.sql, P93 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
--   Lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t25_requete_proposee.sql, t26_requete_proposee.sql
--   et t27_requete_proposee.sql : bloc depot (le dépôt), bloc demande ou liq
--   (la liquidation, première notification), bloc rej (le rejet), bloc
--   instance_fin (en instance) et bloc liq_agg (la durée et ses tranches).
--   Fiche des règles : règles 19 à 24.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Le détail par code en 2024 : prestations liquidées (première
-- notification en 2024), personnes liquidées, personnes payées, montant des
-- mois de 2024.
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
-- resultats/lot8_P93_detail_par_code_2024.tsv), la ligne qui sert au point :
--   Code   Prestations liquidées   Personnes liquidées   Personnes payées 2024   Montant des mois de 2024
--   RAV    832                     831                   753                     517 334 647
-- Face au tableau 24 de l'annuaire CNSS 2024 (p. 19) : 833 allocations de
-- vieillesse liquidées en 2024.
-- Lecture : à la première notification, la base trouve 831 personnes pour
--   833 publiées (−2, soit −0,24 %, calculé ici). L'annuaire compte donc
--   bien une liquidation à la notification des droits.
