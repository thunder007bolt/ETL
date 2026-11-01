-- =============================================================================
-- LOT 6 — Les écarts qui restent pour T32 à T41
-- Rédigé le 31/10/2026, exécuté le 01/11/2026 ; résultats dans les en-têtes
-- et dans resultats/lot6_*.tsv.
-- Sur la base source, schéma USER_DWH, comme les lots 1 à 5.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement.
-- P79 et P81 lisent DEBOURS : plusieurs minutes chacune.
-- Données nominatives : jamais lues. Date de naissance, date de décès et date
-- de saisie ne servent qu'à compter.
-- Pourquoi ce lot : le lot 5 confirme les règles de T32 à T40 à quelques
-- pourcents de l'annuaire de la CNSS 2024. Il reste trois écarts :
--   · T41 : 102 643 dossiers contre 97 378 allocataires publiés en 2024, et
--     283 534 enfants contre 268 481. Les enfants comptés et payés dans
--     l'année retombent à +1,1 % : qu'en est-il des allocataires ?     → P79
--   · des âges impossibles : 500 veuves de 120 à 124 ans en T34, 17 enfants
--     d'âge négatif en T41 : une date de naissance par défaut ?        → P80
--   · T34 et T35 sous le publié depuis 2023 (veuves −4,8 %, orphelins
--     −15,9 % en 2024) : la CNSS compte-t-elle les pensions suspendues ? → P81
--     (L'autre piste, des radiations saisies après la publication, est testée
--     par A06 et A07 dans analyses/comparaison_annuaire_cnss_2024.)
-- Chiffres publiés pour comparer (annuaire de la CNSS 2024, au 31/12) :
--   prestations familiales (tableau 38, de 2020 à 2024) : allocataires 84 401 /
--   88 587 / 91 085 / 93 233 / 97 378 ; enfants 245 540 / 254 013 / 257 676 /
--   257 461 / 268 481.
--   pensions 2024 (tableau 18) : veuves et veufs 21 767 ; orphelins 9 478 ;
--   vieillesse et anticipée 30 808.
-- Numérotation : suite du lot 5, P79 à P81.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P79 — T41 : qui compter comme allocataire et comme enfant ?
-- Question : quelle définition retombe sur les allocataires et les enfants
--            publiés par la CNSS ?
-- Objectif : pour chaque année de 2020 à 2025, sur les allocations familiales
--            (FAF) des enfants de 21 ans au plus, en cours au 31/12 (R6) :
--            enfants et dossiers comptés aujourd'hui ; les mêmes, payés dans
--            l'année ; payés pour un mois du dernier trimestre ; saisis au
--            plus tard le 30/06 de l'année suivante (la base vue à la
--            publication de l'annuaire) ; payés dans l'année et saisis avant
--            la publication. Et, sans regarder le 31/12, les enfants de 21
--            ans au plus et les dossiers payés dans l'année.
-- Lecture  : la colonne qui retombe sur le tableau 38 chaque année, pour les
--            allocataires comme pour les enfants, donne la définition de la
--            CNSS. Si c'est « payés », un enfant compte s'il a été payé dans
--            l'année : la règle de T41 s'écarte alors de R6 (payée ou non),
--            qui reste vraie pour les pensions. Si l'écart baisse avec « vus
--            à la publication », il vient des dossiers saisis après coup avec
--            un effet ancien : rien à corriger, car la BI calcule chaque
--            exercice sur le cliché du début de l'année suivante.
-- Tableaux : T41 ; tableau 38 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot6_P79_t41_variantes.tsv. Allocataires (dossiers) de
--            2020 / 2021 / 2022 / 2023 / 2024 :
--            · publiés 84 401 / 88 587 / 91 085 / 93 233 / 97 378 ;
--            · comptés aujourd'hui 96 666 / 98 934 / 101 036 / 101 222 /
--              102 643 ;
--            · payés dans l'année 93 972 / 96 656 / 98 570 / 98 128 / 98 048 ;
--            · payés au dernier trimestre 93 105 / 96 038 / 97 977 / 97 469 /
--              96 384 ;
--            · vus à la publication 96 104 / 98 495 / 100 683 / 100 867 /
--              102 421 ;
--            · payés et vus 93 436 / 96 247 / 98 253 / 97 862 / 97 923 ;
--            · payés dans l'année, sans regarder le 31/12, 98 872 / 102 052 /
--              103 414 / 105 535 / 104 611.
--            · Enfants, dans le même ordre : publiés 245 540 / 254 013 /
--              257 676 / 257 461 / 268 481 ; comptés 264 178 / 271 585 /
--              278 386 / 280 288 / 283 534 ; payés 257 550 / 266 045 / 272 604
--              / 272 723 / 271 577 ; dernier trimestre 252 348 / 261 307 /
--              267 868 / 268 408 / 263 659 ; vus 259 526 / 266 722 / 272 761 /
--              275 017 / 280 541 ; payés et vus 253 058 / 261 332 / 267 243 /
--              268 359 / 269 689 ; payés sans regarder le 31/12 270 830 /
--              280 708 / 285 239 / 291 366 / 288 346.
--            · 2025 : comptés 102 969 dossiers et 282 036 enfants ; payés
--              95 300 et 262 568.
-- Constat  : · En 2024, « payés dans l'année » retombe sur l'annuaire : 98 048
--              allocataires (+0,7 %) et 271 577 enfants (+1,2 %). Sans
--              condition de paiement : +5,4 % et +5,6 % ; dernier trimestre
--              seul : −1,0 % et −1,8 %.
--            · Règle proposée pour T41 : un enfant compte s'il a droit au
--              31/12 (allocation en cours, 21 ans au plus, en vie) et a été
--              payé dans l'année ; l'allocataire est le dossier qui a au moins
--              un enfant compté. T41 s'écarte ainsi de R6 (payée ou non), qui
--              reste vraie pour les pensions : un enfant sans certificat n'est
--              pas radié, il n'est plus payé.
--            · « Vus à la publication » ne change presque rien (−0,2 %
--              d'allocataires en 2024) : l'écart ne vient pas de dossiers
--              saisis après coup.
--            · Avant 2024, l'écart reste grand pour les allocataires (+5,3 %
--              en 2023, +11,3 % en 2020) et baisse d'année en année.
--              Hypothèse, non vérifiée : les chiffres anciens de l'annuaire
--              sont ceux publiés à l'époque, et des paiements saisis depuis
--              pour des mois anciens (rappels, P20) ont ajouté des enfants
--              payés aux années passées. Sans effet pour la BI, qui calcule
--              chaque exercice sur le cliché du début de l'année suivante,
--              comme l'annuaire.
--            · 2025, avec la règle proposée : 95 300 allocataires et 262 568
--              enfants.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
pe AS (
    SELECT p.PE_ID,
           p.DOS_CODE,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE = 'FAF'
),
-- un paiement par prestation et par année ; t4 = un mois payé d'octobre à
-- décembre
paie AS (
    SELECT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee,
           MAX(CASE WHEN EXTRACT(MONTH FROM deb.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS t4
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID, EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.DOS_CODE,
           pe.IND_ID_PREST,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND a.annee - pe.annee_naissance <= 21
                THEN 1 ELSE 0 END                                 AS compte,
           CASE WHEN a.annee - pe.annee_naissance <= 21
                THEN 1 ELSE 0 END                                 AS age_ok,
           CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END       AS paye,
           NVL(pa.t4, 0)                                          AS paye_t4,
           CASE WHEN pe.date_saisie <= TO_DATE((a.annee + 1) || '0630', 'YYYYMMDD')
                THEN 1 ELSE 0 END                                 AS vu
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN IND_ID_PREST END) AS enfants_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye_t4 = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_t4,
       COUNT(DISTINCT CASE WHEN compte = 1 AND vu = 1
                           THEN IND_ID_PREST END)                 AS enfants_vus,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1 AND vu = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_vus,
       COUNT(DISTINCT CASE WHEN age_ok = 1 AND paye = 1
                           THEN IND_ID_PREST END)                 AS enfants_payes_annee,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN DOS_CODE END)     AS allocataires_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1
                           THEN DOS_CODE END)                     AS allocataires_payes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye_t4 = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_t4,
       COUNT(DISTINCT CASE WHEN compte = 1 AND vu = 1
                           THEN DOS_CODE END)                     AS allocataires_vus,
       COUNT(DISTINCT CASE WHEN compte = 1 AND paye = 1 AND vu = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_vus,
       COUNT(DISTINCT CASE WHEN age_ok = 1 AND paye = 1
                           THEN DOS_CODE END)                     AS allocataires_payes_annee
FROM base
GROUP BY annee
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- P80 — Les âges impossibles : une date de naissance par défaut ?
-- Question : les 500 veuves de 120 à 124 ans (T34), les pensionnés de plus de
--            105 ans (T32, T38, T40) et les 17 enfants d'âge négatif (T41)
--            ont-ils une date de naissance par défaut ?
-- Objectif : pour les bénéficiaires comptés au 31/12/2024 (comme le lot 5) qui
--            ont 100 ans ou plus, un âge négatif ou pas de date de naissance,
--            par tableau et année de naissance : personnes, dates de
--            naissance distinctes, personnes nées un 1er janvier.
-- Lecture  : beaucoup de personnes pour une seule date (par exemple le
--            01/01/1900) = une date par défaut, mise faute de mieux à la
--            saisie. Proposition BI : ces âges passent en « Âge inconnu »
--            (la requête proposée le fait dès 120 ans). Les dates étalées,
--            elles, sont de vrais grands âges, ou des erreurs une à une.
-- Tableaux : T32 à T41.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot6_P80_dates_naissance_2024.tsv. Bénéficiaires
--            comptés au 31/12/2024 qui ont 100 ans ou plus ou un âge négatif
--            (aucune date vide) :
--            · T34 : 499 nés en 1900, sur 4 dates ; 136 nés de 1904 à 1924 ;
--            · T32 : 1 né en 1900 ; 73 nés de 1908 à 1924 ;
--            · T38 : 3 nés en 1900, une seule date ; T37 1, T38 1 et T40 4 nés
--              de 1910 à 1924 ;
--            · T41 : 17 enfants nés en 2025, sur 15 dates.
--            · Presque personne n'est né un 1er janvier (4 veuves de 1900, 2
--              nées en 1920 et 1924).
-- Constat  : · 1900 est une date par défaut : 503 bénéficiaires (499 veuves de
--              T34, 3 de T38, 1 de T32), presque tous sur une même date. La
--              requête les met en « âge inconnu » (120 ans et plus) : règle
--              confirmée.
--            · Les autres très grands âges partagent leur date de naissance :
--              chaque année, plusieurs personnes ont la même date, qui n'est
--              pas le 1er janvier (T32 : 14 nés en 1922 sur une seule date ;
--              T34 : 21 nés en 1922, une date). Ce sont sans doute des dates
--              approchées (« né vers »), donc des âges à peu près justes : ils
--              restent dans « 100 ans et plus ».
--            · Quelques âges restent peu crédibles (T32 : 3 pensionnés nés en
--              1910, 114 ans) : la question du contrôle d'existence (fiche
--              T32, Q4) vaut pour eux.
--            · Les 17 enfants nés en 2025 ont une allocation qui court dès
--              2024 : erreurs de saisie ou dossiers ouverts avant la
--              naissance. Ils vont en « âge inconnu ».
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


-- -----------------------------------------------------------------------------
-- P81 — Les pensions et rentes suspendues au 31/12
-- Question : la CNSS compte-t-elle les pensions suspendues dans ses
--            effectifs ? Nos veuves (T34) et nos orphelins (T35) sont sous le
--            publié depuis 2023.
-- Objectif : pour 2022 à 2025 et chaque tableau de T32 à T40 : prestations
--            suspendues au 31/12 et pas réactivées (effet avant, pas radiées,
--            ayant droit en vie, limite d'âge des orphelins), dont celles
--            suspendues dans l'année, celles payées dans l'année, et les
--            personnes concernées.
-- Lecture  : en 2024, il manque environ 1 050 veuves et veufs (T34) et 1 500
--            orphelins (T35) par rapport à l'annuaire, alors que T32 est déjà
--            à +0,9 %. Si les suspendues de T34 et T35 comblent ces écarts
--            sans trop gonfler T32, la CNSS compte les suspendues : R6 serait
--            à revoir. Une hausse des suspensions en 2023 (contrôle
--            d'existence, certificats de vie ou de scolarité) expliquerait
--            aussi le décrochage.
-- Tableaux : T32 à T40 ; tableau 18 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot6_P81_suspensions.tsv. Prestations suspendues au
--            31/12 et pas réactivées, 2022 / 2023 / 2024 / 2025 :
--            · T32 479 / 260 / 253 / 279 ;
--            · T33 7 / 6 / 6 / 6 ;
--            · T34 413 / 264 / 250 / 252 ;
--            · T35 52 / 31 / 25 / 19 ;
--            · T36 2 / 0 / 0 / 0 ;
--            · T37 140 / 81 / 79 / 97 ;
--            · T38 25 / 13 / 15 / 16 ;
--            · T39 6 / 4 / 4 / 4 ;
--            · T40 35 / 32 / 31 / 35.
--            · Suspendues dans l'année : 30 au plus par tableau ; une personne
--              par prestation, ou presque.
-- Constat  : · Les suspensions sont rares : 250 veuves et veufs et 25
--              orphelins en 2024, pour des écarts de −1 052 et −1 508 à
--              l'annuaire. Elles n'expliquent pas le décrochage de T34 et T35
--              depuis 2023, et les compter pousserait T32 à +1,7 %.
--            · R6 reste : une pension suspendue au 31/12 n'est pas comptée.
--            · Reste la piste des radiations datées de 2023 et saisies après
--              la publication : A06 et A07, dans
--              analyses/comparaison_annuaire_cnss_2024, sont à exécuter.
--            · Le stock de suspendues baisse de 2022 à 2023 (T32 479 → 260,
--              T34 413 → 264), l'année où les veuves et les orphelins
--              décrochent : des suspensions levées ou changées en radiations ?
--              À rapprocher de A07.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2021 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 4
),
perimetre AS (
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
),
pe AS (
    SELECT p.PE_ID,
           pm.tableau,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_SUSPENSION IS NOT NULL
),
paie AS (
    SELECT DISTINCT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2022-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
),
suspendue AS (
    SELECT a.annee,
           pe.tableau,
           pe.IND_ID_PREST,
           CASE WHEN EXTRACT(YEAR FROM pe.date_suspension) = a.annee
                THEN 1 ELSE 0 END                                 AS dans_annee,
           CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END       AS paye
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
    WHERE pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
      AND (pe.date_radiation IS NULL
           OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
      AND pe.date_suspension <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
      -- pas réactivée au 31/12 (une réactivation vide ne doit pas tout exclure)
      AND (   pe.date_reactivation IS NULL
           OR pe.date_reactivation < pe.date_suspension
           OR pe.date_reactivation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
      AND (pe.date_deces IS NULL
           OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
      AND (pe.tableau NOT IN ('T35', 'T39') OR a.annee - pe.annee_naissance < 25)
)
SELECT annee,
       tableau,
       COUNT(*)                                                   AS suspendues_au_31_12,
       SUM(dans_annee)                                            AS dont_suspendues_dans_annee,
       SUM(paye)                                                  AS dont_payees_dans_annee,
       COUNT(DISTINCT IND_ID_PREST)                               AS personnes
FROM suspendue
GROUP BY annee, tableau
ORDER BY tableau, annee;
