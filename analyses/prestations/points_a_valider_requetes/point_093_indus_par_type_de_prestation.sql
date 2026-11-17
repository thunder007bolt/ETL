-- =============================================================================
-- Point 93. Les indus par type de prestation (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « aucun des 684 trop-perçus de 2024 n'est relié à une
--   prestation (table PE_AJ) : on ne connaît que la branche, par le dossier
--   (TDOS_CODE). »
-- Requête : les trop-perçus de 2024 et leur lien avec une prestation.
-- Reprise de : lot9_indus.sql, P106 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
-- Règle appliquée dans : t31_requete_proposee.sql, blocs ajustements et
--   par_branche (le taux par branche seulement). Fiche des règles : règle 46.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Trop-perçus (TP et TP-CONV) établis en 2024 : nombre, montant, avec une
-- prestation liée dans PE_AJ, montant médian, établis après le décès.
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
-- resultats/lot9_P106_trop_percus_2024.tsv) : 684 trop-perçus en 2024, pour
-- 229 484 759 ; aucun n'a de prestation liée dans PE_AJ.
-- Lecture : un trop-perçu ne dit pas quelle prestation il corrige ; sa
--   branche se lit par le dossier (DOSSIER.TDOS_CODE). Le taux par type de
--   prestation ne peut pas se calculer.
