-- =============================================================================
-- Point 9. L'âge et les naissances en 1900
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « INDIVIDU, IND_DATE_NAISSANCE. L'année 1900 est une
--   date mise par défaut à la saisie : 503 personnes en 2024, dont 499
--   veuves, qui ont donc 120 ans et plus. » ; « l'annuaire CNSS 2024 fait
--   pareil : 679 femmes de 100 ans et plus parmi les veuves et les ascendants
--   en 2024 ; on en trouve 635 avec ces dates, 135 sans elles. »
-- Requêtes :
--   1. Les bénéficiaires comptés fin 2024 qui ont 100 ans ou plus, ou un âge
--      négatif, par année de naissance.
--   2. Les bénéficiaires comptés fin 2024 par tableau, tranche d'âge et sexe.
-- Reprise de : lot6_ecarts_t32_t41.sql, P80 (exécutée le 01/11/2026) ;
--   lot5_beneficiaires_31_12.sql, P76 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement ; les dates de naissance ne sortent que
--   par année, jamais par personne.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc tranche. Fiche des
--   règles : règle 52.
-- À noter : le bloc tranche de t32_t41_requete_proposee.sql range encore les
--   âges de 120 ans et plus en « Âge inconnu ». Ce point et la règle 52 de la
--   fiche disent « 100 ans et plus ».
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Bénéficiaires comptés au 31/12/2024 qui ont 100 ans ou plus ou un
-- âge négatif, par tableau et par année de naissance : personnes, nombre de
-- dates de naissance différentes, nés un 1er janvier.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T41', 'FAF'  FROM DUAL
),
compte AS (
    SELECT DISTINCT
           pm.tableau,
           p.IND_ID_PREST,
           TRUNC(i.IND_DATE_NAISSANCE)                            AS date_naissance,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2024-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
      AND (pm.tableau NOT IN ('T35', 'T39')
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
      AND (pm.tableau <> 'T41'
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) <= 21)
)
SELECT tableau,
       NVL(TO_CHAR(EXTRACT(YEAR FROM date_naissance)), '(vide)')  AS annee_naissance,
       COUNT(*)                                                   AS personnes,
       COUNT(DISTINCT date_naissance)                             AS dates_distinctes,
       SUM(CASE WHEN TO_CHAR(date_naissance, 'DDMM') = '0101'
                THEN 1 ELSE 0 END)                                AS nes_un_1er_janvier
FROM compte
WHERE age IS NULL OR age >= 100 OR age < 0
GROUP BY tableau, EXTRACT(YEAR FROM date_naissance)
ORDER BY tableau, EXTRACT(YEAR FROM date_naissance);

-- Résultat obtenu (P80, exécutée le 01/11/2026 ; détail dans
-- resultats/lot6_P80_dates_naissance_2024.tsv) :
--   · T34 : 499 nés en 1900, sur 4 dates ; 136 nés de 1904 à 1924.
--   · T32 : 1 né en 1900 ; 73 nés de 1908 à 1924.
--   · T38 : 3 nés en 1900, une seule date.
--   · T41 : 17 enfants nés en 2025.
-- Lecture : 503 bénéficiaires nés « en 1900 » (499 veuves de T34, 3 de T38,
--   1 de T32) : une date mise par défaut.


-- -----------------------------------------------------------------------------
-- Requête 2. Bénéficiaires comptés au 31/12/2024, par tableau, tranche d'âge
-- de 5 ans et sexe.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T41', 'FAF'  FROM DUAL
),
compte AS (
    SELECT DISTINCT pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2024-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
      AND (pm.tableau NOT IN ('T35', 'T39')
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
      AND (pm.tableau <> 'T41'
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) <= 21)
)
SELECT tableau,
       CASE WHEN age IS NULL THEN 'âge inconnu'
            WHEN age < 0     THEN 'âge négatif'
            ELSE LPAD(TO_CHAR(FLOOR(age / 5) * 5), 3, '0') || '-'
                 || LPAD(TO_CHAR(FLOOR(age / 5) * 5 + 4), 3, '0')
       END                                                        AS tranche_5_ans,
       SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END)                AS sexe_1,
       SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END)                AS sexe_2,
       SUM(CASE WHEN sexe NOT IN ('1', '2') THEN 1 ELSE 0 END)    AS sexe_inconnu,
       COUNT(*)                                                   AS personnes
FROM compte
GROUP BY tableau,
         CASE WHEN age IS NULL THEN 'âge inconnu'
              WHEN age < 0     THEN 'âge négatif'
              ELSE LPAD(TO_CHAR(FLOOR(age / 5) * 5), 3, '0') || '-'
                   || LPAD(TO_CHAR(FLOOR(age / 5) * 5 + 4), 3, '0')
         END
ORDER BY tableau, tranche_5_ans;

-- Résultat obtenu (P76, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P76_ages_2024.tsv), T34 et T36, 100 ans et plus, femmes :
--   100-104 : 93 ; 105-109 : 31 ; 110-114 : 10 ; 115-119 : 1 ; 120-124 : 500.
-- Face au tableau 20 de l'annuaire CNSS 2024 (p. 17) : 679 femmes de 100 ans
-- et plus parmi les veuves, veufs et ascendants.
-- Lecture (calculé ici) : 635 femmes de 100 ans et plus avec les dates de
--   1900, 135 sans elles. L'annuaire ne peut atteindre 679 qu'en les
--   comptant dans « 100 ans et plus ».
