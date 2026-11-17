-- =============================================================================
-- Point 106. L'âge dans l'annuaire CNSS
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « la date de naissance (INDIVIDU,
--   IND_DATE_NAISSANCE). » ; « nouveaux retraités de 2024, révisions
--   comprises : par tranche de 5 ans, l'annuaire CNSS 2024 fait pareil (55 à
--   59 ans : 1 450 dans la base pour 1 429 publiés). Âge par âge, nos âges
--   glissent souvent d'un an (60 ans : 377 pour 519). »
-- Requête : les nouveaux retraités de 2024 par âge et par sexe.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P83 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement ; les âges sortent par année d'âge,
--   jamais par personne.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, bloc detail (âge =
--   année de l'exercice moins année de naissance). Fiche des règles :
--   règle 12.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Nouveaux retraités (RPV) saisis en 2024, par âge (année de l'exercice moins
-- année de naissance) et par sexe.
-- -----------------------------------------------------------------------------
WITH nouveaux AS (
    SELECT DISTINCT
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE = 'RPV'
      AND p.PE_DATE_INSERT >= DATE '2024-01-01'
      AND p.PE_DATE_INSERT <  DATE '2025-01-01'
)
SELECT CASE WHEN age IS NULL OR age < 0 OR age >= 120 THEN 'âge inconnu'
            WHEN age < 53  THEN 'moins de 53 ans'
            WHEN age >= 70 THEN '70 ans et plus'
            ELSE TO_CHAR(age) || ' ans'
       END                                                        AS age_2024,
       SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END)                AS sexe_1,
       SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END)                AS sexe_2,
       SUM(CASE WHEN sexe NOT IN ('1', '2') THEN 1 ELSE 0 END)    AS sexe_inconnu,
       COUNT(*)                                                   AS personnes
FROM nouveaux
GROUP BY CASE WHEN age IS NULL OR age < 0 OR age >= 120 THEN 'âge inconnu'
              WHEN age < 53  THEN 'moins de 53 ans'
              WHEN age >= 70 THEN '70 ans et plus'
              ELSE TO_CHAR(age) || ' ans'
         END
ORDER BY MIN(CASE WHEN age IS NULL OR age < 0 OR age >= 120 THEN 999 ELSE age END);

-- Résultat obtenu (P83, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P83_ages_nouveaux_retraites_2024.tsv), base / publié
-- (tableau 25 de l'annuaire CNSS 2024, p. 20) :
--   moins de 55 ans 1 / 1 ; 55 ans 52 / 12 ; 56 ans 266 / 350 ; 57 ans
--   360 / 334 ; 58 ans 382 / 397 ; 59 ans 390 / 336 ; 60 ans 377 / 519 ;
--   61 ans 353 / 256 ; 62 ans 97 / 94 ; 63 ans 63 / 35 ; 64 ans 51 / 52 ;
--   65 à 69 ans 89 / 89 ; 70 ans et plus 25 / 11.
--   Par tranche de 5 ans : 55-59 ans 1 450 / 1 429 ; 60-64 ans 941 / 956 ;
--   65-69 ans 89 / 89.
-- Lecture : par tranche de 5 ans, l'âge retombe ; âge par âge, il glisse
--   d'un an (60 ans : 377 pour 519 ; 61 ans : 353 pour 256). La CNSS calcule
--   sans doute l'âge à une autre date.
