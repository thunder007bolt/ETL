-- =============================================================================
-- LOT 7 — Contrôles pour T42 à T49 (nouveaux pensionnés et crédirentiers)
-- Rédigé le 01/11/2026. Partie A (P82 à P87) et partie B (P88 à P91),
-- ajoutée après ses résultats, exécutées le même jour.
-- Sur la base source, schéma USER_DWH, comme les lots 1 à 6.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages et sommes
-- seulement. P85 lit DEBOURS : quelques minutes.
-- Données nominatives : jamais lues. Sexe, date de naissance et date de décès
-- ne servent qu'à compter.
-- Pourquoi ce lot : T42 à T49 répartissent par tranche d'âge et par sexe les
-- nouveaux bénéficiaires de l'exercice et ce qui leur a été servi, pour la
-- « loi des entrées » des études actuarielles (Annuaire p. 15 et 16,
-- maquettes p. 42 à 45). La règle d'entrée est déjà proche : une pension
-- saisie dans l'exercice (R7 ; A03 : 4 090 pour 4 037 publiés en 2024). Ce lot
-- tranche ce qui reste :
--   · les entrées de chaque tableau, de 2020 à 2025 : saisie, première entrée
--     de la personne, notification, sorties avant le 31/12, coordination → P82
--   · l'âge et le sexe des nouveaux retraités, face au tableau 25 de
--     l'annuaire de la CNSS                                             → P83
--   · l'âge et le sexe de tous les nouveaux, par tranche de 5 ans        → P84
--   · le montant servi aux nouveaux : au mois payé, ou à la saisie du
--     paiement, rappels compris                                         → P85
--   · les services passés de T42 (en mois, atelier de Kaya 2017)         → P86
--   · le degré d'IPP de T48                                             → P87
-- Le périmètre de chaque tableau (proposition, à valider) :
--   T42 RPV, RPRA · T43 RPSV, RPSF · T44 RPSE · T45 toutes les rentes AT/MP
--   (ARI, ARSV, ARSF, ARSE, ARSA) · T46 ARSE · T47 ARSA · T48 ARI ·
--   T49 ARSV, ARSF.
--   T45 « nouveaux crédirentiers de la branche AT/MP » n'a pas de type : on
--   le lit comme le total de la branche. T47 « nouveaux crédirentiers
--   ascendants » est rangé avec les AT/MP par l'atelier de Kaya 2017 : ARSA.
--   Les nouvelles pensions d'invalidité (RPI) et d'ascendants (RPA) n'ont pas
--   de tableau.
-- Chiffres publiés pour comparer (annuaire de la CNSS 2024) :
--   nouveaux pensionnés de 2024 (tableau 23) : vieillesse 2 486, anticipée 3,
--   veuves 732, veufs 34, orphelins 769, invalidité 6, ascendants 7 ; total
--   4 037.
--   nouveaux retraités de 2024 par âge (tableau 25) : 1 959 hommes, 527
--   femmes ; 50-54 ans 1 ; 55-59 ans 1 429 (H 1 138, F 291) ; 60-64 ans 956
--   (H 732, F 224) ; 65-69 ans 89 (H 79, F 10) ; 70 ans et plus 11 (H 9,
--   F 2). Âges les plus fréquents : 60 ans (519), 58 ans (397), 56 ans (350).
--   nouvelles rentes viagères de 2024 (tableau 75) : 32 hommes, 12 femmes ;
--   25-29 ans H 1 ; 30-34 H 1 ; 35-39 H 4 ; 40-44 H 7, F 6 ; 45-49 H 4, F 1 ;
--   50-54 H 3, F 3 ; 55-59 H 11, F 2 ; 60-64 H 1.
--   nouveaux ayants droit de rentes de 2024 (tableau 77) : veuves 4, veufs 0,
--   orphelins 44, ascendants 13 ; total 61.
-- Numérotation : suite du lot 6, P82 à P87.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P82 — Les entrées de chaque tableau, de 2020 à 2025
-- Question : combien de nouveaux bénéficiaires chaque année, et quelle date
--            fait l'entrée ?
-- Objectif : pour chaque année et chaque tableau : prestations saisies dans
--            l'année (R7) ; personnes distinctes ; celles dont c'est la
--            première prestation du tableau (pas une révision ni une
--            reliquidation) ; personnes notifiées dans l'année ; nouveaux
--            déjà sortis au 31/12 (radiés, décédés, suspendus) ; nouveaux en
--            coordination payée par une autre caisse (COOR_AC) ou instruite
--            par une autre caisse (CNSS_AC).
-- Lecture  : en 2024, comparer aux chiffres publiés (en-tête) : T42 2 489,
--            T43 766, T44 769, T46 44, T47 13, T48 44, T49 4, T45 105 si c'est
--            le total des rentes. Si « première fois » retombe mieux que
--            « personnes saisies », une révision ne fait pas un nouveau. Les
--            sorties avant le 31/12 disent si un nouveau doit encore être en
--            cours en fin d'année.
-- Tableaux : T42 à T49 ; tableaux 23, 75 et 77 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P82_entrees_2020_2025.tsv. En 2024, personnes
--            saisies / première fois / notifiées, face au publié :
--            · T42 2 510 / 2 463 / 2 397 pour 2 489 (vieillesse et
--              anticipée) ;
--            · T43 772 / 771 / 730 pour 766 ;
--            · T44 774 / 773 / 731 pour 769 ;
--            · T45 131 / 102 / 123 pour 105 (rentes viagères et ayants
--              droit) ;
--            · T46 44 / 44 / 47 pour 44 ; T47 15 / 15 / 15 pour 13 ; T48 68 /
--              39 / 57 pour 44 ; T49 4 / 4 / 4 pour 4.
--            · Nouveaux sortis avant le 31/12/2024 : T42 35, T43 7, T44 7, T45
--              2. En coordination payée par une autre caisse (COOR_AC) :
--              T42 234, T43 15, T44 31, aucune rente. CNSS_AC : 0 ou 1 par an.
--            · Personnes saisies qui avaient déjà une prestation du tableau,
--              de 2020 à 2025 : T42 18 / 18 / 28 / 23 / 47 / 220 ; T48 18 /
--              32 / 52 / 12 / 29 / 49.
--            · Première fois, T42, de 2020 à 2025 : 1 891 / 1 965 / 2 127 /
--              2 333 / 2 463 / 2 725.
-- Constat  : · R7 confirmée pour les pensions : la saisie retombe sur
--              l'annuaire à 1 % près (T42 −1,0 % en première fois, T43 +0,7 %,
--              T44 +0,5 %, T46 et T49 exacts) ; la notification donne 4 à 5 %
--              de moins.
--            · Première fois : une révision ou une reliquidation n'est pas une
--              entrée. Elles restent rares jusqu'en 2024 (47 en T42) mais
--              montent à 220 en 2025 : compter les prestations saisies
--              gonflerait la loi des entrées. On garde la première prestation
--              de la personne dans le tableau.
--            · Rentes viagères (T48) : 39 en première fois, 68 saisies, 44
--              publiées. Une rente viagère est souvent recalculée (29 en
--              2024 ; Kaya 2017 demande de réévaluer le taux d'IPP). La CNSS
--              en compte quelques-unes de plus que nos premières fois : peut-
--              être une rente par accident, donc par dossier ; P91 le teste.
--            · Coordination : 234 nouveaux retraités sur 2 463 sont payés par
--              une autre caisse. L'annuaire les compte (sans eux, −10 %) : on
--              les garde, sans montant, comme pour T32.
--            · Les nouveaux sortis avant le 31/12 sont peu nombreux (35 en
--              T42, 1,4 %) : on les compte, c'est une entrée de l'exercice.
--            · T45 retombe sur le total des rentes (102 pour 105) : la lecture
--              de T45 comme total de la branche tient. T47 : 15 pour 13.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
perimetre AS (
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
pe AS (
    SELECT pm.tableau,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           TRUNC(p.PE_DATE_NOTIFICATION)                          AS date_notification,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces,
           NVL(d.DOS_CATEGORIE, '(sans catégorie)')               AS categorie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre             pm ON pm.code   = p.TPE_CODE
    JOIN USER_DWH.DOSSIER      d  ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID   = p.IND_ID_PREST
),
-- la première prestation de la personne dans le tableau
premiere AS (
    SELECT tableau, IND_ID_PREST, MIN(date_saisie) AS premiere_saisie
    FROM pe
    GROUP BY tableau, IND_ID_PREST
),
base AS (
    SELECT a.annee,
           pe.tableau,
           pe.IND_ID_PREST,
           pe.categorie,
           CASE WHEN EXTRACT(YEAR FROM pe.date_saisie) = a.annee
                THEN 1 ELSE 0 END                                 AS saisie,
           CASE WHEN EXTRACT(YEAR FROM pe.date_notification) = a.annee
                THEN 1 ELSE 0 END                                 AS notifiee,
           CASE WHEN EXTRACT(YEAR FROM pr.premiere_saisie) = a.annee
                THEN 1 ELSE 0 END                                 AS premiere_fois,
           CASE WHEN pe.date_radiation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                  OR pe.date_deces     <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                  OR (    pe.date_suspension <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      AND (   pe.date_reactivation IS NULL
                           OR pe.date_reactivation < pe.date_suspension
                           OR pe.date_reactivation > TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                THEN 1 ELSE 0 END                                 AS sortie
    FROM annees a
    CROSS JOIN pe
    JOIN premiere pr ON pr.tableau      = pe.tableau
                    AND pr.IND_ID_PREST = pe.IND_ID_PREST
)
SELECT annee,
       tableau,
       SUM(saisie)                                                AS prestations_saisies,
       COUNT(DISTINCT CASE WHEN saisie = 1 THEN IND_ID_PREST END) AS personnes_saisies,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND premiere_fois = 1
                           THEN IND_ID_PREST END)                 AS premiere_fois,
       COUNT(DISTINCT CASE WHEN notifiee = 1
                           THEN IND_ID_PREST END)                 AS personnes_notifiees,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND sortie = 1
                           THEN IND_ID_PREST END)                 AS sorties_avant_31_12,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND categorie = 'COOR_AC'
                           THEN IND_ID_PREST END)                 AS coor_ac,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND categorie = 'CNSS_AC'
                           THEN IND_ID_PREST END)                 AS cnss_ac
FROM base
GROUP BY annee, tableau
ORDER BY tableau, annee;


-- -----------------------------------------------------------------------------
-- P83 — L'âge et le sexe des nouveaux retraités de 2024
-- Question : notre âge (année moins année de naissance, R8) et notre sexe
--            retombent-ils sur le tableau 25 de l'annuaire de la CNSS ?
-- Objectif : personnes qui ont une pension de vieillesse (RPV) saisie en 2024,
--            par âge en 2024, de « moins de 53 ans » à « 70 ans et plus »,
--            et par sexe.
-- Lecture  : comparer âge par âge au tableau 25 (en-tête du lot) : 2 486
--            nouveaux retraités, 1 959 hommes et 527 femmes, surtout à 56, 58
--            et 60 ans. Un décalage d'un an voudrait dire que la CNSS calcule
--            l'âge autrement (à la date d'effet, par exemple).
-- Tableaux : T42 ; tableau 25 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P83_ages_nouveaux_retraites_2024.tsv. Nouveaux
--            retraités (RPV) de 2024 par âge, nous / publié (tableau 25) :
--            · moins de 55 ans 1 / 1 ; 55 ans 52 / 12 ; 56 ans 266 / 350 ; 57
--              ans 360 / 334 ; 58 ans 382 / 397 ; 59 ans 390 / 336 ; 60 ans
--              377 / 519 ; 61 ans 353 / 256 ; 62 ans 97 / 94 ; 63 ans 63 /
--              35 ; 64 ans 51 / 52 ; 65 à 69 ans 89 / 89 ; 70 ans et plus 25 /
--              11.
--            · Par tranche de 5 ans : 55-59 ans 1 450 / 1 429 ; 60-64 ans
--              941 / 956 ; 65-69 ans 89 / 89.
--            · Sexe : 1 865 hommes et 641 femmes, pour 1 959 et 527 publiés ;
--              2 506 personnes.
-- Constat  : · Par tranche de 5 ans, celles de T42, l'âge retombe sur
--              l'annuaire (55-59 ans +1,5 %, 60-64 ans −1,6 %, 65-69 ans
--              exact) : la règle R8 suffit pour T42.
--            · Âge par âge, les chiffres se décalent (60 ans : 377 contre
--              519 ; 61 ans : 353 contre 256) : la CNSS calcule sans doute
--              l'âge à une autre date, par exemple à la date d'effet de la
--              pension, qui remonte souvent à l'année d'avant. Presque sans
--              effet sur les tranches de 5 ans.
--            · Le sexe ne retombe pas : 114 femmes de plus et 94 hommes de
--              moins que le tableau 25, alors que le stock des retraités
--              retombait à moins de 3 % (P75). Question à la CNSS : d'où vient
--              le sexe du tableau 25 ?
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


-- -----------------------------------------------------------------------------
-- P84 — L'âge et le sexe de tous les nouveaux de 2024
-- Question : les grilles de l'Annuaire (p. 42 à 45) conviennent-elles, et les
--            nouvelles rentes viagères retombent-elles sur le tableau 75 ?
-- Objectif : pour 2024, par tableau, tranche de 5 ans et sexe : personnes qui
--            ont une prestation du tableau saisie en 2024.
-- Lecture  : T48 face au tableau 75 (32 hommes, 12 femmes, de 25 à 64 ans).
--            Les orphelins (T44, T46) doivent avoir moins de 25 ans ; les
--            veuves (T43, T49) sont dans des grilles qui s'arrêtent à
--            « 60 ans et plus » ou « 65 ans et plus ».
-- Tableaux : T42 à T49 ; tableau 75 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P84_ages_nouveaux_2024.tsv. Personnes saisies en
--            2024 :
--            · T42 2 510 : 50-54 ans 4, 55-59 ans 1 451, 60-64 ans 941, 65-69
--              ans 89, 70-74 ans 23, 75-79 ans 2 ;
--            · T43 772, de 25 à 89 ans, dont 413 de 60 ans et plus (53 %) ; 35
--              hommes ;
--            · T44 774, de 0 à 29 ans, dont 2 de 25 à 29 ans ;
--            · T46 44 de 0 à 24 ans ; T47 15 de 50 à 79 ans ; T49 4 femmes de
--              35 à 59 ans ;
--            · T48 68 : 53 hommes et 15 femmes, de 30 à 84 ans, dont 9 de 65
--              ans et plus ; T45 131, de 0 à 84 ans.
-- Constat  : · Les grilles conviennent : T42 tient dans 50 à 79 ans, T44 et
--              T46 dans 0 à 24 ans (2 orphelins de 25 à 29 ans en T44), T47
--              dans 50 ans et plus.
--            · T43 : plus de la moitié des nouvelles veuves ont 60 ans ou
--              plus, la dernière tranche de la grille : le tableau dira peu de
--              leur âge. C'est la maquette de l'Annuaire (p. 42).
--            · T48 face au tableau 75 (32 hommes, 12 femmes, de 25 à 64 ans) :
--              nos 68 comptent les révisions, et les 9 rentes de 65 ans et
--              plus, absentes de l'annuaire, en sont sans doute. La tranche
--              40-44 ans retombe exactement (7 hommes, 6 femmes).
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
    SELECT DISTINCT
           pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_INSERT >= DATE '2024-01-01'
      AND p.PE_DATE_INSERT <  DATE '2025-01-01'
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
FROM nouveaux
GROUP BY tableau,
         CASE WHEN age IS NULL THEN 'âge inconnu'
              WHEN age < 0     THEN 'âge négatif'
              ELSE LPAD(TO_CHAR(FLOOR(age / 5) * 5), 3, '0') || '-'
                   || LPAD(TO_CHAR(FLOOR(age / 5) * 5 + 4), 3, '0')
         END
ORDER BY tableau, tranche_5_ans;


-- -----------------------------------------------------------------------------
-- P85 — Le montant servi aux nouveaux de 2024
-- Question : quel montant mettre en face d'un nouveau bénéficiaire ? Sa
--            pension est souvent payée avec un rappel qui remonte avant
--            l'année (lot 2, P28).
-- Objectif : pour les prestations saisies en 2024, par tableau : personnes ;
--            personnes payées pour un mois de 2024 ; montant des mois de 2024
--            (R4), dont la part saisie en 2025 ou après ; montant des
--            paiements saisis en 2024, dont la part qui paie des mois d'avant
--            2024 (rappels). Débours de paiement (PM) au statut D ou P (R3).
-- Lecture  : « mois de 2024 » suit la règle R4 de T32 à T41 : le rappel des
--            mois d'avant 2024 n'y est pas. « Saisis en 2024 » donne ce qui a
--            été versé dans l'année, rappel compris. L'annuaire de la CNSS ne
--            publie pas ce montant : le choix reviendra au métier. La part
--            saisie en 2025 ou après manquera à la BI, qui calcule l'exercice
--            au début de l'année suivante.
-- Tableaux : T42 à T49.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P85_montants_nouveaux_2024.tsv. Prestations
--            saisies en 2024, montant des mois de 2024 / dont saisi en 2025 ou
--            après / montant saisi en 2024 / dont rappel des mois d'avant
--            2024 :
--            · T42 2 860 877 000 / 149 132 389 / 3 215 719 411 / 226 860 416 ;
--            · T43 333 267 149 / 35 061 901 / 408 854 769 / 83 334 211 ;
--            · T44 157 117 394 / 15 066 202 / 214 467 126 / 60 663 732 ;
--            · T45 42 761 951 / 763 716 / 92 006 985 / 47 306 747 ;
--            · T46 17 939 948 / vide / 54 170 260 / 35 506 712 ;
--            · T47 2 332 987 / vide / 5 519 985 / 3 122 798 ;
--            · T48 18 117 416 / 763 716 / 20 349 547 / 1 445 944 ;
--            · T49 4 371 600 / vide / 11 967 193 / 7 231 293.
--            · Personnes sans paiement d'un mois de 2024 : T42 321, T44 71,
--              T43 28, T45 14.
-- Constat  : · Pour les nouveaux retraités, le rappel pèse peu : 8 % du
--              montant des mois de 2024 (T42, T48).
--            · Pour les survivants, il pèse lourd : 25 % pour les veuves
--              (T43), 39 % pour les orphelins (T44), et plus que les mois de
--              l'année pour les rentes de survivants (T46 198 %, T49 165 %,
--              T47 134 %). Leurs dossiers attendent longtemps : de 795 à 992
--              jours entre l'effet et la notification (P28).
--            · Proposition BI : garder le montant des mois de l'exercice (R4),
--              comme T32 à T41, et montrer le rappel à part. Question à la
--              CNSS : les « montants servis » comprennent-ils le rappel ?
--            · La part des mois de 2024 saisie en 2025 ou après (5 % en T42,
--              10 % en T43 et T44) manquera à un calcul fait tout au début de
--              l'année suivante.
--            · Les 321 nouveaux retraités sans paiement de 2024 sont surtout
--              ceux de la coordination payée par une autre caisse (234).
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


-- -----------------------------------------------------------------------------
-- P86 — Les services passés des nouveaux retraités (T42)
-- Question : le « total des services passés » de T42 peut-il se calculer, en
--            mois (atelier de Kaya 2017) ? Pour la coordination, Kaya demande
--            la seule part CNSS.
-- Objectif : pour les personnes qui ont une pension de vieillesse ou de
--            retraite anticipée saisie dans l'année, de 2020 à 2025, la
--            dernière saisie de l'année, par catégorie de dossier : mois
--            cotisés vides, à zéro, au-dessus de 600 (écartés, R9), valides ;
--            total, moyenne et médiane des mois valides ; prestations qui ont
--            des cotisations dans une autre caisse (PE_COT_AUTRE rempli).
-- Lecture  : si presque tous les mois sont valides, le total des services
--            passés se calcule. Si les dossiers de coordination ont autant de
--            mois que les autres et des cotisations « autre caisse » à part,
--            PE_MOIS_COTISATION est déjà la part CNSS.
-- Tableaux : T42 (et T50 plus tard).
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P86_services_passes.tsv. Nouveaux retraités (RPV,
--            RPRA), dernière prestation saisie de l'année : aucun mois vide ni
--            à zéro ; un seul au-dessus de 600 (2025, COOR_AC).
--            · En 2024, personnes / moyenne / médiane des mois / avec
--              cotisations dans une autre caisse : ORDINAIRE 2 152 / 286,8 /
--              270 / 1 ; COOR_AC 234 / 342,3 / 342,5 / 234 ; COOR_CNSS 123 /
--              334,2 / 322 / 118 ; sans catégorie 1 / 454 / 454 / 1.
--            · Total des mois valides en 2024 : 738 743.
-- Constat  : · Les services passés de T42 se calculent : les mois cotisés des
--              nouveaux retraités sont toujours remplis et plausibles (24 ans
--              en médiane pour les dossiers ordinaires).
--            · Coordination : PE_COT_AUTRE n'est rempli que pour les dossiers
--              de coordination (234 sur 234 COOR_AC, 118 sur 123 COOR_CNSS).
--              Leurs mois cotisés sont plus longs que ceux des dossiers
--              ordinaires (342 contre 287 en moyenne) : PE_MOIS_COTISATION
--              compte sans doute aussi les années dans l'autre caisse. Kaya
--              2017 demande la seule part CNSS : P88 à P90 regardent ce que
--              contient PE_COT_AUTRE.
-- -----------------------------------------------------------------------------
WITH nouveaux AS (
    SELECT EXTRACT(YEAR FROM p.PE_DATE_INSERT)                    AS annee,
           p.PE_MOIS_COTISATION                                   AS mois,
           p.PE_COT_AUTRE                                         AS cot_autre,
           NVL(d.DOS_CATEGORIE, '(sans catégorie)')               AS categorie,
           ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM p.PE_DATE_INSERT),
                                           p.IND_ID_PREST
                              ORDER BY p.PE_DATE_INSERT DESC, p.PE_ID DESC) AS rang
    FROM USER_DWH.PRESTATION_ESP p
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
    WHERE p.TPE_CODE IN ('RPV', 'RPRA')
      AND p.PE_DATE_INSERT >= DATE '2020-01-01'
      AND p.PE_DATE_INSERT <  DATE '2026-01-01'
)
SELECT annee,
       categorie,
       COUNT(*)                                                   AS personnes,
       SUM(CASE WHEN mois IS NULL THEN 1 ELSE 0 END)              AS mois_vides,
       SUM(CASE WHEN mois = 0 THEN 1 ELSE 0 END)                  AS mois_a_zero,
       SUM(CASE WHEN mois > 600 THEN 1 ELSE 0 END)                AS plus_de_600,
       SUM(CASE WHEN mois BETWEEN 1 AND 600 THEN 1 ELSE 0 END)    AS mois_valides,
       SUM(CASE WHEN mois BETWEEN 1 AND 600 THEN mois END)        AS total_mois_valides,
       ROUND(AVG(CASE WHEN mois BETWEEN 1 AND 600
                      THEN mois END), 1)                          AS moyenne,
       MEDIAN(CASE WHEN mois BETWEEN 1 AND 600 THEN mois END)     AS mediane,
       COUNT(cot_autre)                                           AS avec_cot_autre
FROM nouveaux
WHERE rang = 1
GROUP BY annee, categorie
ORDER BY annee, personnes DESC;


-- -----------------------------------------------------------------------------
-- P87 — Le degré d'IPP des nouvelles rentes viagères (T48)
-- Question : le « degré moyen IPP » de T48 peut-il se calculer ? L'atelier de
--            Kaya 2017 demande de réévaluer périodiquement le taux d'IPP.
-- Objectif : pour les rentes viagères (ARI) saisies chaque année de 2020 à
--            2025 : personnes, rentes, taux d'incapacité vide, à zéro,
--            au-dessus de 100 ; minimum, médiane, moyenne (de 1 à 100) et
--            maximum ; répartition par degré.
-- Lecture  : si le taux est rempli et compris entre 1 et 100, la moyenne par
--            tranche et par sexe se calcule. L'allocation d'incapacité (AAI)
--            couvre les petits taux, 5 à 10 en médiane (P72) : une rente
--            viagère doit avoir des taux plus forts.
-- Tableaux : T48.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P87_taux_ipp_rentes_viageres.tsv. Rentes viagères
--            saisies chaque année, de 2020 à 2025, personnes / rentes /
--            minimum, médiane, moyenne, maximum du taux / de 10 à 19, de 20 à
--            39, de 40 à 59, de 60 à 100 :
--            · 2020 43 / 43 / 15, 25, 31,7, 92 / 8, 25, 5, 5 ;
--            · 2021 63 / 63 / 15, 25, 30,3, 85 / 15, 32, 11, 5 ;
--            · 2022 76 / 78 / 15, 25, 28,1, 70 / 24, 40, 8, 6 ;
--            · 2023 41 / 41 / 15, 23,5, 29,6, 85 / 13, 19, 6, 3 ;
--            · 2024 68 / 69 / 15, 24, 27,3, 70 / 21, 35, 10, 3 ;
--            · 2025 84 / 85 / 15, 25, 30,6, 100 / 17, 48, 11, 9.
--            · Jamais vide, jamais à zéro, jamais au-dessus de 100 ; aucun
--              taux de 1 à 9.
-- Constat  : · Le degré moyen d'IPP de T48 se calcule : le taux est toujours
--              rempli et plausible, autour de 25 en médiane et de 27 à 32 en
--              moyenne.
--            · Aucune rente viagère sous 15 % : c'est sans doute le seuil de
--              la rente ; en dessous, c'est l'allocation d'incapacité, de 5 à
--              10 en médiane (P72).
--            · Ces chiffres comptent les révisions (69 rentes en 2024 pour 39
--              nouvelles, P91) : la requête prend le taux de la dernière rente
--              saisie dans l'année par un nouveau bénéficiaire.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM PE_DATE_INSERT)                          AS annee,
       COUNT(DISTINCT IND_ID_PREST)                               AS personnes,
       COUNT(*)                                                   AS rentes,
       SUM(CASE WHEN PE_TAUX_INCAPACITE IS NULL THEN 1 ELSE 0 END) AS taux_vide,
       SUM(CASE WHEN PE_TAUX_INCAPACITE = 0 THEN 1 ELSE 0 END)    AS taux_a_zero,
       SUM(CASE WHEN PE_TAUX_INCAPACITE > 100 THEN 1 ELSE 0 END)  AS plus_de_100,
       MIN(PE_TAUX_INCAPACITE)                                    AS minimum,
       MEDIAN(PE_TAUX_INCAPACITE)                                 AS mediane,
       ROUND(AVG(CASE WHEN PE_TAUX_INCAPACITE BETWEEN 1 AND 100
                      THEN PE_TAUX_INCAPACITE END), 1)            AS moyenne,
       MAX(PE_TAUX_INCAPACITE)                                    AS maximum,
       SUM(CASE WHEN PE_TAUX_INCAPACITE > 0
                 AND PE_TAUX_INCAPACITE < 10 THEN 1 ELSE 0 END)   AS de_1_a_9,
       SUM(CASE WHEN PE_TAUX_INCAPACITE >= 10
                 AND PE_TAUX_INCAPACITE < 20 THEN 1 ELSE 0 END)   AS de_10_a_19,
       SUM(CASE WHEN PE_TAUX_INCAPACITE >= 20
                 AND PE_TAUX_INCAPACITE < 40 THEN 1 ELSE 0 END)   AS de_20_a_39,
       SUM(CASE WHEN PE_TAUX_INCAPACITE >= 40
                 AND PE_TAUX_INCAPACITE < 60 THEN 1 ELSE 0 END)   AS de_40_a_59,
       SUM(CASE WHEN PE_TAUX_INCAPACITE >= 60
                 AND PE_TAUX_INCAPACITE <= 100 THEN 1 ELSE 0 END) AS de_60_a_100
FROM USER_DWH.PRESTATION_ESP
WHERE TPE_CODE = 'ARI'
  AND PE_DATE_INSERT >= DATE '2020-01-01'
  AND PE_DATE_INSERT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM PE_DATE_INSERT)
ORDER BY annee;


-- =============================================================================
-- PARTIE B — SUITES DU LOT 7 (rédigée et exécutée le 01/11/2026 après les
-- résultats de P82 à P86)
-- Deux points que la partie A n'a pas tranchés :
--   T42 la part CNSS des services passés en coordination (Kaya 2017) :
--       que contient PE_COT_AUTRE ?                       → P88, P89, P90
--   T48 44 nouvelles rentes viagères publiées, 39 en première fois, 68
--       saisies : une rente par accident (par dossier) ?   → P91
-- Aucune ne lit DEBOURS. PE_NO_PENSION_AUTRE (numéro de pension dans l'autre
-- caisse) n'est que compté.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P88 — Le type des colonnes de mois cotisés et de coordination
-- Question : PE_COT_AUTRE est-il un nombre (des mois ? un montant ?) ou un
--            texte ? P89 et P90 en dépendent.
-- Objectif : le type, la longueur et la précision de PE_MOIS_COTISATION,
--            PE_MOIS_COTI_ASSU_OBLI, PE_MOIS_COTI_ASSU_VOL,
--            PE_MOIS_NOUVELLE_LOI, PE_COT_AUTRE et PE_NO_PENSION_AUTRE,
--            d'après le dictionnaire d'Oracle.
-- Lecture  : NUMBER pour PE_COT_AUTRE veut dire une durée ou un montant ; P90
--            montre lequel.
-- Tableaux : T42.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P88_types_colonnes.tsv. PE_MOIS_COTISATION,
--            PE_MOIS_COTI_ASSU_OBLI, PE_MOIS_COTI_ASSU_VOL et
--            PE_MOIS_NOUVELLE_LOI : NUMBER(3,0) ; PE_COT_AUTRE : NUMBER sans
--            précision ; PE_NO_PENSION_AUTRE : VARCHAR2(15).
-- Constat  : · PE_COT_AUTRE est un nombre ; P90 montre que ce sont des mois.
-- -----------------------------------------------------------------------------
SELECT COLUMN_NAME,
       DATA_TYPE,
       DATA_LENGTH,
       DATA_PRECISION,
       DATA_SCALE
FROM ALL_TAB_COLUMNS
WHERE OWNER      = 'USER_DWH'
  AND TABLE_NAME = 'PRESTATION_ESP'
  AND COLUMN_NAME IN ('PE_MOIS_COTISATION', 'PE_MOIS_COTI_ASSU_OBLI',
                      'PE_MOIS_COTI_ASSU_VOL', 'PE_MOIS_NOUVELLE_LOI',
                      'PE_COT_AUTRE', 'PE_NO_PENSION_AUTRE')
ORDER BY COLUMN_NAME;


-- -----------------------------------------------------------------------------
-- P89 — Les mois cotisés des nouveaux retraités, par catégorie de dossier
-- Question : pour la coordination, PE_MOIS_COTISATION compte-t-il aussi les
--            années dans l'autre caisse ?
-- Objectif : pour les pensions de vieillesse et de retraite anticipée saisies
--            en 2024, par catégorie de dossier : prestations ; moyenne des
--            mois cotisés (total, obligatoires, volontaires) ; PE_COT_AUTRE
--            rempli, nombre de valeurs, minimum, maximum ;
--            PE_MOIS_NOUVELLE_LOI et PE_NO_PENSION_AUTRE remplis.
-- Lecture  : si, en coordination, les mois obligatoires sont nettement plus
--            courts que le total, le total compte l'autre caisse et la part
--            CNSS est ailleurs (les mois obligatoires, ou le total moins
--            PE_COT_AUTRE). Les mois de P86 dépassaient de 55 en moyenne ceux
--            des dossiers ordinaires (342 contre 287).
-- Tableaux : T42.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P89_mois_par_categorie_2024.tsv. Pensions de
--            vieillesse et de retraite anticipée saisies en 2024,
--            prestations / moyenne des mois total, obligatoires, volontaires /
--            PE_COT_AUTRE rempli, valeurs, minimum, maximum :
--            · ORDINAIRE 2 162 / 286,8, 171, 54,4 / 2, 1, 6, 6 ;
--            · COOR_AC 235 / 342, 68,5, 33,1 / 235, 148, 110, 446 ;
--            · COOR_CNSS 126 / 332,6, 217,4, 24 / 120, 74, 6, 464 ;
--            · sans catégorie 1 / 454, vide, vide / 1, 1, 5, 5.
--            · PE_MOIS_NOUVELLE_LOI jamais rempli ; PE_NO_PENSION_AUTRE rempli
--              2 fois.
-- Constat  : · PE_COT_AUTRE est rempli pour toutes les pensions de
--              coordination et presque jamais ailleurs (2 dossiers
--              ordinaires). Ses valeurs, de 6 à 464, ont l'ordre de grandeur
--              des mois cotisés : ce sont des mois dans l'autre caisse.
--            · Quand l'autre caisse paie (COOR_AC), elle a la plus grande part
--              de la carrière (de 110 à 446 mois) ; quand la CNSS paie
--              (COOR_CNSS), l'autre caisse a la petite part (à partir de 6
--              mois).
--            · Les mois obligatoires et volontaires ne servent pas : ils ne
--              s'additionnent pas au total (P30) et sont souvent vides en
--              coordination (P90).
-- -----------------------------------------------------------------------------
SELECT NVL(d.DOS_CATEGORIE, '(sans catégorie)')                   AS categorie,
       COUNT(*)                                                   AS prestations,
       ROUND(AVG(p.PE_MOIS_COTISATION), 1)                        AS moy_mois_total,
       ROUND(AVG(p.PE_MOIS_COTI_ASSU_OBLI), 1)                    AS moy_mois_obligatoires,
       ROUND(AVG(p.PE_MOIS_COTI_ASSU_VOL), 1)                     AS moy_mois_volontaires,
       COUNT(p.PE_COT_AUTRE)                                      AS avec_cot_autre,
       COUNT(DISTINCT p.PE_COT_AUTRE)                             AS valeurs_cot_autre,
       MIN(p.PE_COT_AUTRE)                                        AS min_cot_autre,
       MAX(p.PE_COT_AUTRE)                                        AS max_cot_autre,
       COUNT(p.PE_MOIS_NOUVELLE_LOI)                              AS avec_mois_nouvelle_loi,
       COUNT(p.PE_NO_PENSION_AUTRE)                               AS avec_no_pension_autre
FROM USER_DWH.PRESTATION_ESP p
JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
WHERE p.TPE_CODE IN ('RPV', 'RPRA')
  AND p.PE_DATE_INSERT >= DATE '2024-01-01'
  AND p.PE_DATE_INSERT <  DATE '2025-01-01'
GROUP BY NVL(d.DOS_CATEGORIE, '(sans catégorie)')
ORDER BY prestations DESC;


-- -----------------------------------------------------------------------------
-- P90 — Les valeurs de PE_COT_AUTRE en coordination
-- Question : PE_COT_AUTRE est-il un nombre de mois dans l'autre caisse ?
-- Objectif : pour les pensions de vieillesse et de retraite anticipée de
--            coordination (COOR_AC, COOR_CNSS) saisies en 2024 : les 20
--            valeurs de PE_COT_AUTRE les plus fréquentes (3 prestations au
--            moins), avec la moyenne des mois cotisés de ces prestations.
-- Lecture  : des valeurs de 12 à 400 environ, qui montent avec les mois
--            cotisés, sont des mois ; des valeurs de plusieurs milliers sont
--            des montants. Si ce sont des mois et qu'ils se retranchent du
--            total, la part CNSS est PE_MOIS_COTISATION moins PE_COT_AUTRE.
-- Tableaux : T42.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P90_valeurs_cot_autre_2024.tsv. Les 20 valeurs les
--            plus fréquentes de PE_COT_AUTRE en coordination (2024), valeur /
--            prestations / moyenne des mois total :
--            · 280 / 10 / 362 ; 288 / 6 / 365,5 ; 83 / 5 / 476,4 ; 55 / 5 /
--              311 ; 285 / 5 / 359,6 ; 218 / 5 / 327,2 ; 100 / 4 / 306,3 ;
--              160 / 4 / 331,5 ; 146 / 4 / 285,3 ; 203 / 4 / 398,3 ; 41 / 4 /
--              302,8 ; 275 / 4 / 347,3 ; 88 / 4 / 352 ; 13 / 4 / 246,8 ; 59 /
--              4 / 259,5 ; 153 / 3 / 245,3 ; 252 / 3 / 359,3 ; 6 / 3 / 366 ;
--              223 / 3 / 274,3 ; 171 / 3 / 274,7.
--            · Mois obligatoires : vides pour presque toutes ces valeurs (187
--              pour 218, 24 pour 153).
-- Constat  : · Pour chaque valeur, le total des mois cotisés la dépasse : 280
--              mois dans l'autre caisse pour 362 au total, 218 pour 327, 223
--              pour 274. Le total compte sans doute les deux caisses, et
--              PE_COT_AUTRE la part de l'autre.
--            · Proposition BI pour T42 : en coordination, services passés =
--              PE_MOIS_COTISATION moins PE_COT_AUTRE, la part CNSS demandée
--              par l'atelier de Kaya 2017 (de 51 à 195 mois pour les valeurs
--              de plus de 200). À confirmer par la direction des prestations :
--              que contient PE_COT_AUTRE ?
-- -----------------------------------------------------------------------------
SELECT *
FROM (
    SELECT p.PE_COT_AUTRE                                         AS valeur,
           COUNT(*)                                               AS prestations,
           ROUND(AVG(p.PE_MOIS_COTISATION), 1)                    AS moy_mois_total,
           ROUND(AVG(p.PE_MOIS_COTI_ASSU_OBLI), 1)                AS moy_mois_obligatoires
    FROM USER_DWH.PRESTATION_ESP p
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
    WHERE p.TPE_CODE IN ('RPV', 'RPRA')
      AND d.DOS_CATEGORIE IN ('COOR_AC', 'COOR_CNSS')
      AND p.PE_DATE_INSERT >= DATE '2024-01-01'
      AND p.PE_DATE_INSERT <  DATE '2025-01-01'
      AND p.PE_COT_AUTRE IS NOT NULL
    GROUP BY p.PE_COT_AUTRE
    HAVING COUNT(*) >= 3
    ORDER BY COUNT(*) DESC
)
WHERE ROWNUM <= 20;


-- -----------------------------------------------------------------------------
-- P91 — Les nouvelles rentes viagères : par personne ou par accident ?
-- Question : l'annuaire de la CNSS publie 44 nouvelles rentes viagères en 2024
--            (tableau 75) ; nous en avons 39 en première fois de la personne,
--            68 saisies. Compte-t-il une rente par accident (par dossier) ?
-- Objectif : pour les rentes viagères (ARI) saisies chaque année de 2020 à
--            2025 : personnes ; personnes dont c'est la première rente
--            viagère ; personnes dont c'est la première rente viagère du
--            dossier (un nouvel accident) ; dossiers concernés ; personnes
--            dont la rente remplace une rente du même dossier (révision).
-- Lecture  : si « première du dossier » retombe sur 44 en 2024, la CNSS compte
--            un nouveau bénéficiaire à chaque accident qui donne une rente, et
--            T48 se compte par dossier. Sinon, la première fois de la personne
--            reste la règle.
-- Tableaux : T45, T48 ; tableau 75 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot7_P91_rentes_viageres_par_dossier.tsv. Rentes
--            viagères saisies de 2020 à 2025, personnes / première de la
--            personne / première du dossier / dossiers / révision dans le même
--            dossier :
--            · 2020 43 / 25 / 25 / 25 / 18 ;
--            · 2021 63 / 31 / 32 / 32 / 31 ;
--            · 2022 76 / 24 / 24 / 24 / 52 ;
--            · 2023 41 / 29 / 29 / 29 / 12 ;
--            · 2024 68 / 39 / 39 / 39 / 29 ;
--            · 2025 84 / 35 / 35 / 35 / 49.
-- Constat  : · Compter par dossier ne change rien : une personne n'a qu'un
--              accident qui donne une rente. L'écart avec l'annuaire (39 pour
--              44 en 2024) reste, petit et non expliqué : question à la CNSS.
--            · Les révisions sont nombreuses : de 12 à 52 par an, souvent plus
--              que les nouvelles rentes. Les compter doublerait T48 certaines
--              années. On garde la première rente viagère de la personne (R7).
--            · Le degré d'IPP de T48 est celui de la dernière rente saisie
--              dans l'année par le nouveau bénéficiaire (P87, pas encore
--              collée).
-- -----------------------------------------------------------------------------
WITH ari AS (
    SELECT p.DOS_CODE,
           p.IND_ID_PREST,
           p.PE_DATE_INSERT,
           ROW_NUMBER() OVER (PARTITION BY p.IND_ID_PREST
                              ORDER BY p.PE_DATE_INSERT, p.PE_ID)          AS rang_personne,
           ROW_NUMBER() OVER (PARTITION BY p.DOS_CODE, p.IND_ID_PREST
                              ORDER BY p.PE_DATE_INSERT, p.PE_ID)          AS rang_dossier
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE = 'ARI'
)
SELECT EXTRACT(YEAR FROM PE_DATE_INSERT)                          AS annee,
       COUNT(DISTINCT IND_ID_PREST)                               AS personnes,
       COUNT(DISTINCT CASE WHEN rang_personne = 1
                           THEN IND_ID_PREST END)                 AS premiere_de_la_personne,
       COUNT(DISTINCT CASE WHEN rang_dossier = 1
                           THEN IND_ID_PREST END)                 AS premiere_du_dossier,
       COUNT(DISTINCT CASE WHEN rang_dossier = 1
                           THEN DOS_CODE END)                     AS dossiers_premiere,
       COUNT(DISTINCT CASE WHEN rang_dossier > 1
                           THEN IND_ID_PREST END)                 AS revision_meme_dossier
FROM ari
WHERE PE_DATE_INSERT >= DATE '2020-01-01'
  AND PE_DATE_INSERT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM PE_DATE_INSERT)
ORDER BY annee;
