-- =============================================================================
-- Point 55. La date d'un décès (T57)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « la date du décès (INDIVIDU, IND_DATE_DECES). Le jour
--   de la saisie n'est pas gardé : on s'en approche par la dernière
--   modification de la pension (PRESTATION_ESP, PE_DATE_UPDATE). » ;
--   « retraités morts en 2024 : 619 dans la copie de la base du 02/02/2026,
--   environ 444 déjà saisis fin mars 2025 ; l'annuaire CNSS 2024 en publie
--   447 (tableau 26). »
-- Requête : les retraités morts en 2024, par mois du décès, et ceux dont la
--   pension n'a plus été modifiée après mars 2025.
-- Reprise de : lot8_evolution_5_ans.sql, P99 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc deces (l'année
--   est celle de IND_DATE_DECES, sur la photo donnée au lancement). Fiche des
--   règles : règles 2 et 4.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Retraités (RPV en cours) morts en 2024, par mois du décès : décès, pension
-- radiée, pension modifiée pour la dernière fois avant avril ou avant juillet
-- 2025.
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
-- retraités en 2024 ; 444 dont la pension n'a plus été modifiée après mars
-- 2025 ; 530 avant juillet.
-- Face au tableau 26 de l'annuaire CNSS 2024 (p. 21) : 447 décès de
-- retraités de vieillesse.
-- Lecture : compté à sa date, un décès n'entre que s'il est saisi quand on
--   calcule ; environ 444 l'étaient fin mars 2025. Le détail mois par mois
--   est dans point_002_date_d_arret_de_l_annuaire.sql.
