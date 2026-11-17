-- =============================================================================
-- Point 32. Les moins-perçus des prestations familiales (T31)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T31.
-- Ce que dit le point : « AJUSTEMENT, TAJ_CODE = MP, sur un dossier de
--   prestations familiales (TDOS_CODE = F), compté l'année où il est établi
--   (AJ_DATE_ETABLISSEMENT). » ; « 56 362 moins-perçus en 2023, 17 126 en
--   2024, 1 384 en 2025. »
-- Requête : les ajustements par année, branche et type, établis de 2023 à
--   2025.
-- Reprise de : lot9_indus.sql, P102 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative.
-- Règle appliquée dans : t31_requete_proposee.sql, bloc ajustements (MP et RC,
--   mois de AJ_DATE_ETABLISSEMENT). Fiche des règles : règles 43 et 45.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Ajustements (AJUSTEMENT) établis de 2023 à 2025, par branche du
-- dossier (PVID, PF, PF maternité, AT/MP) et par type (TAJ_CODE).
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
-- resultats/lot9_P102_ajustements_par_type.tsv), les moins-perçus (MP) des
-- prestations familiales (branche PF, sans la maternité) :
--   Année   Moins-perçus   Montant
--   2023    56 362         484 537 248
--   2024    17 126         132 014 360
--   2025     1 384           7 967 651
-- Lecture : les moins-perçus des prestations familiales fondent de 2023 à
--   2025 : chute réelle, ou rappels versés autrement ? C'est la question.
