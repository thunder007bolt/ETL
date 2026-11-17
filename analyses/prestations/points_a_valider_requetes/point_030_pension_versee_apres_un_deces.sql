-- =============================================================================
-- Point 30. La pension versée après un décès (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « la date du décès (INDIVIDU, IND_DATE_DECES) et les
--   trop-perçus (AJUSTEMENT, TAJ_CODE = TP ou TP-CONV) établis après ce décès
--   (AJ_DATE_ETABLISSEMENT). » ; « en 2024, 7 trop-perçus seulement sont
--   établis après un décès, alors qu'environ 175 décès de retraités de 2024
--   n'ont été saisis qu'après mars 2025 (voir le point 2). »
-- Requêtes :
--   1. Les trop-perçus de 2024 : lien avec une prestation, montant, et ceux
--      établis après le décès de la personne.
--   2. Les retraités morts en 2024 : décès dans la base et décès connus fin
--      mars 2025.
-- Reprise de : lot9_indus.sql, P106 (exécutée le 01/11/2026) ;
--   lot8_evolution_5_ans.sql, P99 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements (seuls
--   les trop-perçus enregistrés comptent). Fiche des règles : règle 43.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Trop-perçus (TP et TP-CONV) établis en 2024 : lien avec une
-- prestation (PE_AJ), montant médian, et ceux établis après le décès de la
-- personne (INDIVIDU, IND_DATE_DECES).
-- -----------------------------------------------------------------------------
WITH code_lie AS (
    SELECT pa.AJ_ID,
           MIN(p.TPE_CODE)                                        AS code
    FROM USER_DWH.PE_AJ pa
    JOIN USER_DWH.PRESTATION_ESP p ON p.PE_ID = pa.PE_ID
    GROUP BY pa.AJ_ID
)
SELECT NVL(c.code, '(sans prestation)')                           AS code_prestation,
       COUNT(*)                                                   AS trop_percus,
       ROUND(SUM(aj.AJ_MONTANT))                                  AS montant,
       ROUND(MEDIAN(aj.AJ_MONTANT))                               AS mediane,
       SUM(CASE WHEN i.IND_DATE_DECES < aj.AJ_DATE_ETABLISSEMENT
                THEN 1 ELSE 0 END)                                AS apres_deces,
       ROUND(SUM(CASE WHEN i.IND_DATE_DECES < aj.AJ_DATE_ETABLISSEMENT
                      THEN aj.AJ_MONTANT END))                    AS montant_apres_deces
FROM USER_DWH.AJUSTEMENT aj
LEFT JOIN code_lie c           ON c.AJ_ID  = aj.AJ_ID
LEFT JOIN USER_DWH.INDIVIDU i  ON i.IND_ID = aj.IND_ID
WHERE aj.TAJ_CODE IN ('TP', 'TP-CONV')
  AND aj.AJ_DATE_ETABLISSEMENT >= DATE '2024-01-01'
  AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2025-01-01'
GROUP BY NVL(c.code, '(sans prestation)')
ORDER BY montant DESC;

-- Résultat obtenu (P106, exécutée le 01/11/2026 ; détail dans
-- resultats/lot9_P106_trop_percus_2024.tsv) :
--   · 684 trop-perçus en 2024, pour 229 484 759 ; aucun n'a de prestation
--     liée dans PE_AJ ; médiane 110 000.
--   · 7 sont établis après le décès de la personne, pour 2 714 115.


-- -----------------------------------------------------------------------------
-- Requête 2. Retraités (pension de vieillesse RPV en cours) morts en 2024,
-- par mois du décès : décès, pension radiée, pension modifiée pour la
-- dernière fois avant avril ou avant juillet 2025.
-- -----------------------------------------------------------------------------
WITH deces AS (
    SELECT i.IND_ID,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces,
           TRUNC(i.IND_DATE_UPDATE)                               AS maj_personne,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_UPDATE)                                AS maj_pension
    FROM USER_DWH.INDIVIDU i
    JOIN USER_DWH.PRESTATION_ESP p ON p.IND_ID_PREST = i.IND_ID
    WHERE p.TPE_CODE = 'RPV'
      AND i.IND_DATE_DECES >= DATE '2024-01-01'
      AND i.IND_DATE_DECES <  DATE '2025-01-01'
      AND p.PE_DATE_EFFET <= i.IND_DATE_DECES
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION >= i.IND_DATE_DECES - 31)
)
SELECT EXTRACT(MONTH FROM date_deces)                             AS mois_deces,
       COUNT(DISTINCT IND_ID)                                     AS deces,
       COUNT(DISTINCT CASE WHEN date_radiation IS NOT NULL
                           THEN IND_ID END)                       AS pension_radiee,
       COUNT(DISTINCT CASE WHEN maj_pension < DATE '2025-04-01'
                           THEN IND_ID END)                       AS pension_maj_avant_avril,
       COUNT(DISTINCT CASE WHEN maj_pension < DATE '2025-07-01'
                           THEN IND_ID END)                       AS pension_maj_avant_juillet,
       COUNT(DISTINCT CASE WHEN maj_personne < DATE '2025-04-01'
                           THEN IND_ID END)                       AS personne_maj_avant_avril
FROM deces
GROUP BY EXTRACT(MONTH FROM date_deces)
ORDER BY mois_deces;

-- Résultat obtenu (P99, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P99_deces_retraites_2024_par_mois.tsv) : 619 décès de
-- retraités en 2024 ; 444 connus avant avril 2025 (pension modifiée pour la
-- dernière fois avant le 01/04/2025), 530 avant juillet.
-- Lecture (calculé ici) : 619 − 444 = 175 décès semblent saisis après mars
--   2025, mais 7 trop-perçus seulement suivent un décès en 2024. Les pensions
--   versées après un décès déclaré tard ne deviennent pas des trop-perçus, ou
--   sont reprises autrement.
