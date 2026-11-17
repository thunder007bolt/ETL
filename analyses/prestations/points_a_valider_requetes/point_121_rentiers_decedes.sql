-- =============================================================================
-- Point 121. Les rentiers décédés (T57)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « les rentes (PRESTATION_ESP, codes ARI, ARSV, ARSF,
--   ARSE, ARSA), leur radiation (PE_DATE_RADIATION) et le décès du titulaire
--   (INDIVIDU, IND_DATE_DECES). » ; « 16 rentiers décédés en 2024 dans la
--   base ; l'annuaire CNSS 2024 en compte 24 (tableau 81). »
-- Requête : T57 de 2020 à 2025, par année du décès, dont les rentiers.
-- Reprise de : lot8_evolution_5_ans.sql, P97 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc en_cours (la rente
--   en cours au décès : non radiée, ou radiée au plus tôt 31 jours avant).
--   Fiche des règles : règle 74.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Décès par année : pension directe, tous pensionnés, rentiers (rente viagère,
-- rente de survivant), victimes, allocataires.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
deces AS (
    SELECT IND_ID,
           TRUNC(IND_DATE_DECES)                                  AS date_deces,
           EXTRACT(YEAR FROM IND_DATE_DECES)                      AS annee
    FROM USER_DWH.INDIVIDU
    WHERE IND_DATE_DECES >= DATE '2020-01-01'
      AND IND_DATE_DECES <  DATE '2026-01-01'
),
-- les prestations en cours au décès (radiées au plus tôt un mois avant)
en_cours AS (
    SELECT DISTINCT de.annee, de.IND_ID, p.TPE_CODE
    FROM deces de
    JOIN USER_DWH.PRESTATION_ESP p ON p.IND_ID_PREST = de.IND_ID
    WHERE p.TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                         'ARI', 'ARSV', 'ARSF', 'ARSE', 'ARSA')
      AND p.PE_DATE_EFFET <= de.date_deces
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION >= de.date_deces - 31)
),
-- les victimes d'un accident du travail, décédées dans l'année qui suit
victimes AS (
    SELECT DISTINCT de.annee, de.IND_ID
    FROM deces de
    JOIN USER_DWH.DOSSIER ds ON ds.IND_ID = de.IND_ID
    WHERE ds.TDOS_CODE = 'A'
      AND ds.DOS_DATE_ACCIDENT <= de.date_deces
      AND de.date_deces < ds.DOS_DATE_ACCIDENT + 366
),
-- les titulaires d'un dossier d'allocations familiales en cours au décès
allocataires AS (
    SELECT DISTINCT de.annee, de.IND_ID
    FROM deces de
    JOIN USER_DWH.DOSSIER        ds ON ds.IND_ID   = de.IND_ID
    JOIN USER_DWH.PRESTATION_ESP p  ON p.DOS_CODE  = ds.DOS_CODE
    WHERE p.TPE_CODE = 'FAF'
      AND p.PE_DATE_EFFET <= de.date_deces
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION >= de.date_deces - 31)
)
SELECT a.annee,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE IN ('RPV', 'RPRA', 'RPI'))   AS pension_directe,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE = 'RPV')                      AS dont_vieillesse,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE LIKE 'RP%')                   AS tous_pensionnes,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE = 'ARI')                      AS rente_viagere,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE LIKE 'ARS%')                  AS rente_survivant,
       (SELECT COUNT(*) FROM victimes v WHERE v.annee = a.annee)            AS victimes_at,
       (SELECT COUNT(*) FROM allocataires f WHERE f.annee = a.annee)        AS allocataires_pf
FROM annees a
ORDER BY a.annee;

-- Résultat obtenu (P97, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P97_decedes_par_branche.tsv), rentiers décédés en 2024 : 15
-- de rente viagère et 1 de rente de survivant, soit 16.
-- Face au tableau 81 de l'annuaire CNSS 2024 (p. 62) : 24 rentiers décédés.
-- Lecture : 16 dans la base pour 24 publiés ; une rente radiée longtemps
--   avant le décès (faute de certificat de vie) n'est pas comptée.
