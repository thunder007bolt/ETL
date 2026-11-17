-- =============================================================================
-- Point 56. Le sinistré (T57)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T50 à
--   T57.
-- Ce que dit le point : « la date du décès de la victime (INDIVIDU,
--   IND_DATE_DECES), face à la date de l'accident. Le champ
--   DOS_DECES_IMMEDIAT (DOSSIER) n'est rempli que pour 2 % des accidents de
--   2024. » ; « par année de l'accident, de 2020 à 2024 : 20, 17, 24, 16 et 9
--   victimes mortes dans l'année dans la base. L'annuaire CNSS 2024 compte
--   autrement : il ne publie que les décès immédiats, 13, 11, 13, 11 et 14
--   (tableau 49). »
-- Requête : les dossiers d'accident du travail par année de l'accident et
--   valeur de DOS_DECES_IMMEDIAT : victimes décédées, décédées dans l'année.
-- Reprise de : lot8_evolution_5_ans.sql, P100 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc victimes (une
--   victime morte dans l'année qui suit son accident). Fiche des règles :
--   règle 76.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Dossiers d'accident du travail (TDOS_CODE = A) par année de l'accident et
-- valeur de DOS_DECES_IMMEDIAT : dossiers, victime décédée, décédée dans
-- l'année qui suit l'accident, avec une rente de survivant.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM d.DOS_DATE_ACCIDENT)                     AS annee_accident,
       NVL(TO_CHAR(d.DOS_DECES_IMMEDIAT), '(vide)')               AS deces_immediat,
       COUNT(*)                                                   AS dossiers,
       SUM(CASE WHEN i.IND_DATE_DECES IS NOT NULL THEN 1 ELSE 0 END) AS victime_decedee,
       SUM(CASE WHEN i.IND_DATE_DECES >= d.DOS_DATE_ACCIDENT
                 AND i.IND_DATE_DECES <  d.DOS_DATE_ACCIDENT + 366
                THEN 1 ELSE 0 END)                                AS decedee_dans_l_annee,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_DWH.PRESTATION_ESP p
                             WHERE p.DOS_CODE = d.DOS_CODE
                               AND p.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA'))
                THEN 1 ELSE 0 END)                                AS avec_rente_survivant
FROM USER_DWH.DOSSIER d
LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = d.IND_ID
WHERE d.TDOS_CODE = 'A'
  AND d.DOS_DATE_ACCIDENT >= DATE '2020-01-01'
  AND d.DOS_DATE_ACCIDENT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM d.DOS_DATE_ACCIDENT), NVL(TO_CHAR(d.DOS_DECES_IMMEDIAT), '(vide)')
ORDER BY annee_accident, deces_immediat;

-- Résultat obtenu (P100, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P100_deces_immediat.tsv) :
--   Année de l'accident   DOS_DECES_IMMEDIAT          Décédées dans l'année
--   2020                  vide 1 385                  20
--   2021                  vide 1 370                  17
--   2022                  N 2, O 1, vide 1 355        1 + 23 = 24
--   2023                  N 3, vide 1 231             1 + 15 = 16
--   2024                  N 22, O 3, vide 1 167       1 + 8 = 9
--   (le détail par valeur est dans le TSV ; l'en-tête du lot 8 ne recopie
--   pas le décès de la ligne N de 2023)
-- Face au tableau 49 de l'annuaire CNSS 2024 : décès immédiats 13, 11, 13,
-- 11 et 14 de 2020 à 2024.
-- Lecture (calculé ici) : en 2024, DOS_DECES_IMMEDIAT n'est rempli que pour
--   25 dossiers sur 1 192, soit 2 % ; la base compte 20, 17, 24, 16 et 9
--   victimes mortes dans l'année.
