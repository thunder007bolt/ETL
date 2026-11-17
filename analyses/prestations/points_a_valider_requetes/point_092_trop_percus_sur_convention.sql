-- =============================================================================
-- Point 92. Les trop-perçus sur convention (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « AJUSTEMENT, TAJ_CODE = TP-CONV, par date
--   d'établissement (AJ_DATE_ETABLISSEMENT). » ; « 15 en 2024 pour 12,1 M ;
--   758 en 2025 pour 54,8 M. »
-- Requête : les ajustements par année, branche et type, établis de 2023 à
--   2025.
-- Reprise de : lot9_indus.sql, P102 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements (TP-CONV
--   compté avec les trop-perçus, dans la branche du dossier). Fiche des
--   règles : règle 43.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Ajustements (AJUSTEMENT) établis de 2023 à 2025, par branche et par type
-- (TAJ_CODE), dont les trop-perçus sur convention (TP-CONV).
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM aj.AJ_DATE_ETABLISSEMENT)                AS annee,
       CASE d.TDOS_CODE
           WHEN 'V' THEN 'PVID'
           WHEN 'A' THEN 'AT/MP'
           WHEN 'F' THEN 'PF'
           WHEN 'M' THEN 'PF maternité'
           ELSE          '(sans dossier)'
       END                                                        AS branche,
       CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP', 'MP', 'RC', 'AV')
            THEN aj.TAJ_CODE ELSE 'AUTRES' END                    AS type_ajustement,
       COUNT(*)                                                   AS ajustements,
       ROUND(SUM(aj.AJ_MONTANT))                                  AS montant,
       SUM(CASE WHEN aj.AJ_STATUT = 'A' THEN 1 ELSE 0 END)        AS statut_a,
       SUM(CASE WHEN aj.AJ_STATUT = 'C' THEN 1 ELSE 0 END)        AS statut_c,
       SUM(CASE WHEN aj.AJ_STATUT = 'I' THEN 1 ELSE 0 END)        AS statut_i,
       SUM(CASE WHEN aj.AJ_STATUT = 'R' THEN 1 ELSE 0 END)        AS statut_r
FROM USER_DWH.AJUSTEMENT aj
LEFT JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = aj.DOS_CODE
WHERE aj.AJ_DATE_ETABLISSEMENT >= DATE '2023-01-01'
  AND aj.AJ_DATE_ETABLISSEMENT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM aj.AJ_DATE_ETABLISSEMENT),
         CASE d.TDOS_CODE
             WHEN 'V' THEN 'PVID'
             WHEN 'A' THEN 'AT/MP'
             WHEN 'F' THEN 'PF'
             WHEN 'M' THEN 'PF maternité'
             ELSE          '(sans dossier)'
         END,
         CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV', 'REM-TP', 'MP', 'RC', 'AV')
              THEN aj.TAJ_CODE ELSE 'AUTRES' END
ORDER BY annee, branche, type_ajustement;

-- Résultat obtenu (P102, exécutée le 01/11/2026 ; détail dans
-- resultats/lot9_P102_ajustements_par_type.tsv), les TP-CONV :
--   Année   Nombre   Montant
--   2023     37      14 735 346
--   2024     15      12 135 453
--   2025    758      54 771 312
-- Lecture : 15 trop-perçus sur convention en 2024 pour 12,1 M, 758 en 2025
--   pour 54,8 M : un bond à expliquer.
