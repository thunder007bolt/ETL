-- =============================================================================
-- Point 62. Les mois cotisés
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « PRESTATION_ESP, PE_MOIS_COTISATION, recopié sur
--   chaque prestation du dossier. Il vaut parfois 999. »
-- Requête : la qualité des mois cotisés des pensions et des rentes : médiane,
--   moyenne, plus grand, plus de 600 mois.
-- Reprise de : lot2_statuts_stocks_nouveaux.sql, P30 (exécutée le
--   26/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et statistiques seulement, aucune donnée
--   nominative.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, blocs entree et
--   beneficiaire, et t50_t57_requete_proposee.sql (de 1 à 600 mois). Fiche des
--   règles : règles 66 et 71.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Mois cotisés (PE_MOIS_COTISATION) par code de prestation : médiane,
-- moyenne, plus grand, nombre au-dessus de 600, total face à obligatoires et
-- volontaires.
-- -----------------------------------------------------------------------------
SELECT TPE_CODE,
       COUNT(*)                                                           AS nb_prestations,
       SUM(CASE WHEN PE_MOIS_COTISATION IS NULL THEN 1 ELSE 0 END)        AS vides,
       SUM(CASE WHEN PE_MOIS_COTISATION = 0 THEN 1 ELSE 0 END)            AS a_zero,
       MIN(PE_MOIS_COTISATION)                                            AS minimum,
       ROUND(MEDIAN(PE_MOIS_COTISATION))                                  AS mediane,
       ROUND(AVG(PE_MOIS_COTISATION), 1)                                  AS moyenne,
       MAX(PE_MOIS_COTISATION)                                            AS maximum,
       SUM(CASE WHEN PE_MOIS_COTISATION > 600 THEN 1 ELSE 0 END)          AS plus_de_600,
       SUM(CASE WHEN PE_MOIS_COTISATION IS NOT NULL
                 AND NVL(PE_MOIS_COTI_ASSU_OBLI, 0) + NVL(PE_MOIS_COTI_ASSU_VOL, 0)
                     <> PE_MOIS_COTISATION
                THEN 1 ELSE 0 END)                                        AS total_different,
       SUM(CASE WHEN PE_MOIS_COTI_ASSU_VOL > 0 THEN 1 ELSE 0 END)         AS avec_volontaire,
       SUM(CASE WHEN PE_MOIS_NOUVELLE_LOI IS NOT NULL THEN 1 ELSE 0 END)  AS avec_mois_nouvelle_loi,
       ROUND(AVG(CASE WHEN PE_DATE_EFFET >= DATE '2025-01-01'
                       AND PE_DATE_EFFET <  DATE '2026-01-01'
                      THEN PE_MOIS_COTISATION END), 1)                    AS moyenne_effet_2025
FROM USER_DWH.PRESTATION_ESP
WHERE TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RAV', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                   'ARI', 'ARSV', 'ARSE', 'AIJ', 'MIJ')
GROUP BY TPE_CODE
ORDER BY nb_prestations DESC;

-- Résultat obtenu (P30, exécutée le 26/10/2026) : unité, le mois (RPV :
-- médiane 295, moyenne 304). Plus grand : 999 pour RPV, RPSE, RPSV et RPI ;
-- plus de 600 mois : RPV 229, RPSE 519, RPSV 1 133. RAV : médiane 92, plus
-- grand 489.
-- Lecture : 999 semble une valeur par défaut ; au-dessus de 600 mois (50 ans),
--   la valeur est écartée des services passés.
