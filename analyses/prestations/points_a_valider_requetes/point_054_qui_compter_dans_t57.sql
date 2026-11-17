-- =============================================================================
-- Point 54. Qui compter dans T57
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « la date du décès (INDIVIDU, IND_DATE_DECES) et, en
--   vieillesse, les titulaires d'une pension directe en cours
--   (PRESTATION_ESP, RPV, RPRA, RPI) ; avec le filtre « ayant droit », aussi
--   RPSV, RPSF, RPSE et RPA. » ; « 622 décès en 2024 parmi les titulaires
--   d'une pension directe dans la copie du 02/02/2026 [...]. L'annuaire CNSS
--   2024 publie 648 pensionnés décédés, dont 451 de droit direct (tableau
--   26). »
-- Requête : T57 de 2020 à 2025, par année du décès : pension directe,
--   vieillesse, tous pensionnés, rentiers, victimes, allocataires.
-- Reprise de : lot8_evolution_5_ans.sql, P97 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, blocs deces, en_cours,
--   victimes, allocataires et decedes (T57). Fiche des règles : règle 74.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Décès par année (IND_DATE_DECES) : titulaires d'une pension directe en
-- cours à leur décès, dont vieillesse, tous pensionnés (survivants compris),
-- rentiers (rente viagère, rente de survivant), victimes d'accident, et
-- allocataires de prestations familiales.
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
-- resultats/lot8_P97_decedes_par_branche.tsv) :
--   Année   Pension directe   dont vieillesse   Tous pensionnés   Rente viagère   Rente de survivant   Victimes   Allocataires
--   2020    704               693               813               24              3                    22         347
--   2021    703               694               789               27              3                    17         328
--   2022    585               580               692               17              1                    26         285
--   2023    550               546               657               18              3                    14         286
--   2024    622               619               747               15              1                    10         194
--   2025    303               299               380                7              0                     2          60
-- Face au tableau 26 de l'annuaire CNSS 2024 (p. 21) : 648 pensionnés
-- décédés en 2024, dont 451 de droit direct (vieillesse 447, invalidité 3,
-- anticipée 1).
-- Lecture : 622 titulaires de pension directe morts en 2024 dans la copie
--   d'aujourd'hui, qui a des décès saisis après la publication (point 2).
