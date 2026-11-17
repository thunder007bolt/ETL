-- =============================================================================
-- Point 45. Le rappel d'un nouveau retraité
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « les paiements (DEBOURS), avec le mois payé
--   (DEB_DATE_EFFET) et la date de saisie (DEB_DATE_INSERT). » ; « nouveaux
--   retraités saisis en 2024, révisions comprises : 2,86 Md payés pour les
--   mois de 2024 ; le rappel saisi en 2024, 0,23 Md, vaut 8 % de ce
--   montant. »
-- Requête : le montant servi aux nouveaux de 2024, par tableau : mois de
--   2024, saisi en 2024, et le rappel des mois d'avant.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P85 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Sommes seulement, aucune donnée nominative. Lit DEBOURS :
--   plusieurs minutes.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, bloc paie (les mois de
--   l'exercice ; le rappel n'y est pas, sa colonne à part reste à écrire).
--   Fiche des règles : règle 64.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Prestations saisies en 2024, par tableau : montant des mois de 2024, dont
-- saisi en 2025 ou après, montant saisi en 2024, dont rappel des mois d'avant
-- 2024.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T42' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T42', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T44', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T45', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T46', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T47', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T48', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T49', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T49', 'ARSF' FROM DUAL
),
nouveaux AS (
    SELECT p.PE_ID,
           pm.tableau,
           p.IND_ID_PREST
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    WHERE p.PE_DATE_INSERT >= DATE '2024-01-01'
      AND p.PE_DATE_INSERT <  DATE '2025-01-01'
),
paie AS (
    SELECT deb.PE_ID,
           deb.DEB_MONTANT                                        AS montant,
           TRUNC(deb.DEB_DATE_EFFET)                              AS mois,
           TRUNC(deb.DEB_DATE_INSERT)                             AS saisie
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.PE_ID IN (SELECT PE_ID FROM nouveaux)
)
SELECT n.tableau,
       COUNT(DISTINCT n.IND_ID_PREST)                             AS personnes,
       COUNT(DISTINCT CASE WHEN pa.mois >= DATE '2024-01-01'
                            AND pa.mois <  DATE '2025-01-01'
                           THEN n.IND_ID_PREST END)               AS personnes_payees_mois_2024,
       ROUND(SUM(CASE WHEN pa.mois >= DATE '2024-01-01'
                       AND pa.mois <  DATE '2025-01-01'
                      THEN pa.montant END))                       AS montant_mois_2024,
       ROUND(SUM(CASE WHEN pa.mois >= DATE '2024-01-01'
                       AND pa.mois <  DATE '2025-01-01'
                       AND pa.saisie >= DATE '2025-01-01'
                      THEN pa.montant END))                       AS dont_saisi_en_2025_ou_apres,
       ROUND(SUM(CASE WHEN pa.saisie >= DATE '2024-01-01'
                       AND pa.saisie <  DATE '2025-01-01'
                      THEN pa.montant END))                       AS montant_saisi_2024,
       ROUND(SUM(CASE WHEN pa.saisie >= DATE '2024-01-01'
                       AND pa.saisie <  DATE '2025-01-01'
                       AND pa.mois   <  DATE '2024-01-01'
                      THEN pa.montant END))                       AS dont_mois_avant_2024
FROM nouveaux n
LEFT JOIN paie pa ON pa.PE_ID = n.PE_ID
GROUP BY n.tableau
ORDER BY n.tableau;

-- Résultat obtenu (P85, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P85_montants_nouveaux_2024.tsv) :
--   Tableau   Mois de 2024    dont saisi en 2025 ou après   Saisi en 2024   dont rappel
--   T42       2 860 877 000   149 132 389                   3 215 719 411   226 860 416
--   T43         333 267 149    35 061 901                     408 854 769    83 334 211
--   T44         157 117 394    15 066 202                     214 467 126    60 663 732
-- Lecture (calculé ici) : pour T42, 226 860 416 ÷ 2 860 877 000 = 7,9 %,
--   « 8 % » : 0,23 Md de rappel pour 2,86 Md payés pour les mois de 2024.
