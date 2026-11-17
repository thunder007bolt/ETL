-- =============================================================================
-- Point 98. Le taux de remplacement
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « PRESTATION_ESP, codes RPV, RPRA, RPI, et le salaire
--   moyen de chaque pension (PE_SAL_MOYEN), jamais étudié (voir le point
--   133). »
-- Requête : le salaire moyen des pensions directes en cours au 31/12/2025 :
--   rempli ou non, et ses valeurs.
-- Nouvelle requête : aucun lot n'avait étudié PE_SAL_MOYEN.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et statistiques seulement, aucune donnée
--   nominative.
-- Règle appliquée dans : aucune requête proposée, le taux de remplacement
--   n'est pas encore calculé. Fiche des règles : règle 57.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Pensions directes (RPV, RPRA, RPI) en cours au 31/12/2025 (commencées avant,
-- ni radiées ni suspendues ce jour-là, ayant droit en vie), par code : nombre,
-- salaire moyen rempli, nul ou vide, médiane, plus petit et plus grand.
-- -----------------------------------------------------------------------------
SELECT p.TPE_CODE                                                 AS code,
       COUNT(*)                                                   AS pensions_en_cours,
       SUM(CASE WHEN p.PE_SAL_MOYEN > 0 THEN 1 ELSE 0 END)        AS salaire_moyen_rempli,
       SUM(CASE WHEN p.PE_SAL_MOYEN IS NULL OR p.PE_SAL_MOYEN = 0
                THEN 1 ELSE 0 END)                                AS salaire_moyen_vide,
       MEDIAN(CASE WHEN p.PE_SAL_MOYEN > 0 THEN p.PE_SAL_MOYEN END) AS mediane,
       MIN(CASE WHEN p.PE_SAL_MOYEN > 0 THEN p.PE_SAL_MOYEN END)  AS plus_petit,
       MAX(p.PE_SAL_MOYEN)                                        AS plus_grand
FROM USER_DWH.PRESTATION_ESP p
LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
WHERE p.TPE_CODE IN ('RPV', 'RPRA', 'RPI')
  AND p.PE_DATE_EFFET <= DATE '2025-12-31'
  AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
  AND (   p.PE_DATE_SUSPENSION IS NULL
       OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
       OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
           AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
  AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2025-12-31')
GROUP BY p.TPE_CODE
ORDER BY p.TPE_CODE;

-- Résultat : pas encore exécutée.
-- Lecture attendue : si le salaire moyen est rempli et plausible (en FCFA
--   par mois ou par an, à faire dire par la DSI), le taux de remplacement se
--   calcule : pension ÷ salaire moyen des cinq meilleures années.
