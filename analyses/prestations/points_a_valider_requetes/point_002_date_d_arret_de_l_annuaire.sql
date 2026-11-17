-- =============================================================================
-- Point 2. La date d'arrêt de l'annuaire
-- Fiche : points_a_valider_v2.md, rubrique « À trancher en priorité ».
-- Ce que dit le point : « environ 444 retraités morts en 2024 semblent déjà
--   saisis fin mars 2025 (leur pension n'a plus été modifiée après), pour 447
--   au tableau 26 de l'annuaire CNSS 2024 ; la base d'aujourd'hui en compte
--   619. »
-- Requête : les retraités morts en 2024, mois par mois : décès dans la base,
--   décès « connus fin mars 2025 », décès publiés au tableau 26.
-- Reprise de : lot8_evolution_5_ans.sql, P99 (exécutée le 01/11/2026). Même
--   périmètre, mêmes filtres ; les chiffres publiés sont écrits dans la
--   requête.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : toutes les requêtes proposées reçoivent la photo de
--   la base en paramètre (:1, la colonne CLICHE). Fiche des règles : règles 1,
--   2 et 4.
-- À noter : depuis le 09/11/2026, la fiche des règles (règles 1 et 2) retient
--   la photo reprise à la fin de janvier N+1, après les corrections des
--   directions. Le texte de ce point (fin mars) n'a pas été réécrit.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Retraité mort en 2024 : titulaire d'une pension de vieillesse (RPV) en cours
-- à son décès (IND_DATE_DECES en 2024). « Connu fin mars 2025 » : sa pension
-- a été modifiée pour la dernière fois avant le 01/04/2025 (PE_DATE_UPDATE).
-- La base ne garde pas le jour où le décès a été saisi : ce n'est qu'un
-- indice. Une pension modifiée plus tard pour une autre raison paraît
-- déclarée tard.
-- -----------------------------------------------------------------------------
WITH deces AS (
    SELECT i.IND_ID,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces,
           TRUNC(p.PE_DATE_UPDATE)                                AS maj_pension
    FROM USER_DWH.INDIVIDU i
    JOIN USER_DWH.PRESTATION_ESP p ON p.IND_ID_PREST = i.IND_ID
    WHERE p.TPE_CODE = 'RPV'
      AND i.IND_DATE_DECES >= DATE '2024-01-01'
      AND i.IND_DATE_DECES <  DATE '2025-01-01'
      AND p.PE_DATE_EFFET <= i.IND_DATE_DECES
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION >= i.IND_DATE_DECES - 31)
),
par_mois AS (
    SELECT EXTRACT(MONTH FROM date_deces)                         AS mois,
           COUNT(DISTINCT IND_ID)                                 AS deces_base,
           COUNT(DISTINCT CASE WHEN maj_pension < DATE '2025-04-01'
                               THEN IND_ID END)                   AS connus_fin_mars_2025
    FROM deces
    GROUP BY EXTRACT(MONTH FROM date_deces)
),
publie AS (
    -- Annuaire CNSS 2024, tableau 26 (p. 21), colonne « Pension de vieillesse ».
    SELECT 1 AS mois, 33 AS publie FROM DUAL
    UNION ALL SELECT  2, 29 FROM DUAL
    UNION ALL SELECT  3, 58 FROM DUAL
    UNION ALL SELECT  4, 82 FROM DUAL
    UNION ALL SELECT  5, 65 FROM DUAL
    UNION ALL SELECT  6, 27 FROM DUAL
    UNION ALL SELECT  7, 27 FROM DUAL
    UNION ALL SELECT  8, 32 FROM DUAL
    UNION ALL SELECT  9, 36 FROM DUAL
    UNION ALL SELECT 10, 31 FROM DUAL
    UNION ALL SELECT 11, 14 FROM DUAL
    UNION ALL SELECT 12, 13 FROM DUAL
)
SELECT pu.mois                                                    AS ordre,
       TO_CHAR(pu.mois)                                           AS mois_deces,
       pm.deces_base,
       pm.connus_fin_mars_2025,
       pu.publie                                                  AS publie_tableau_26
FROM publie pu
LEFT JOIN par_mois pm ON pm.mois = pu.mois
UNION ALL
SELECT 13,
       'Total',
       SUM(pm.deces_base),
       SUM(pm.connus_fin_mars_2025),
       SUM(pu.publie)
FROM publie pu
LEFT JOIN par_mois pm ON pm.mois = pu.mois
ORDER BY 1;

-- Résultat obtenu (P99, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P99_deces_retraites_2024_par_mois.tsv) :
--   Mois       Décès dans la base   Connus fin mars 2025   Publiés (tableau 26)
--   1            42                   33                     33
--   2            33                   28                     29
--   3            72                   59                     58
--   4           102                   82                     82
--   5            76                   66                     65
--   6            36                   27                     27
--   7            36                   24                     27
--   8            45                   32                     32
--   9            52                   34                     36
--   10           45                   29                     31
--   11           32                   14                     14
--   12           48                   16                     13
--   Total       619                  444                    447
-- Lecture :
--   · 444 décès connus fin mars 2025 pour 447 publiés, et le même rythme mois
--     par mois (cinq mois tombent juste : janvier, avril, juin, août et
--     novembre). Les 175 autres décès de 2024 semblent saisis plus tard.
--   · C'est un indice fort que l'annuaire a été fait avec la base de la fin
--     mars 2025, pas une preuve : la base ne date pas la saisie du décès.
--   · Le constat de P99, dans le lot 8, dit « six mois exacts » : on en
--     compte cinq.
