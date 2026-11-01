-- =============================================================================
-- LOT 8 — Contrôles pour T50 à T57 (évolution sur 5 ans, récapitulatifs)
-- Rédigé le 01/11/2026. Partie A (P92 à P97) et partie B (P98 à P101),
-- ajoutée après ses résultats, exécutées le même jour.
-- Sur la base source, schéma USER_DWH, comme les lots 1 à 7.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages et sommes
-- seulement. P92, P93 et P95 lisent DEBOURS : plusieurs minutes chacune.
-- Données nominatives : jamais lues. Sexe et dates de naissance et de décès
-- ne servent qu'à compter.
-- Pourquoi ce lot : T50 à T55 suivent sur 5 ans le nombre de bénéficiaires et
-- les montants servis d'une prestation, par sexe ; T56 récapitule les
-- prestations servies par branche, T57 les décédés par branche (Annuaire
-- p. 16, maquettes p. 46 à 48). Ce lot tranche :
--   · qui compter chaque année : les prestations liquidées (notifiées) dans
--     l'année, payées dans l'année ou saisies ; et quel montant          → P92
--   · le détail par code : l'allocation de survivants et ses parts,
--     l'allocation prénatale de la PF et de la maternité                → P93
--   · le nombre moyen de services passés des allocations de vieillesse  → P94
--   · les montants servis par branche (T56)                             → P95
--   · les colonnes de décès de la base                                  → P96
--   · les décédés par branche (T57)                                     → P97
-- Le périmètre de chaque tableau (correspondance du README) :
--   T50 RAV · T51 RAS, RASC, RASO, RASA · T52 AIJ · T53 AAI · T54 MIJ ·
--   T55 FAP, MAP.
-- Atelier de Kaya 2017 : T54 et T55 comptent les indemnités et allocations
-- liquidées, pas les payées, avec une ligne Total ; T57 devient « assurés
-- décédés déclarés par branche » : pension directe pour les pensions, rentier
-- et sinistré pour les AT, allocataire pour les PF, avec une colonne et une
-- ligne Total.
-- Chiffres publiés pour comparer (annuaire de la CNSS 2024) :
--   allocations de vieillesse liquidées en 2024 (tableau 24) : 833 ; montant
--   2024 (tableau 34) : 497 726 931 ; allocations de survivants : 197 906 991.
--   IJ AT/MP (tableau 97), 2020 à 2024 : 94 182 006 / 94 190 676 / 69 008 587
--   / 81 867 247 / 65 420 082 ; allocations d'incapacité : 25 558 532 /
--   34 091 560 / 19 188 366 / 34 496 568 / 20 034 111.
--   IJ de maternité (tableau 35), 2020 à 2024 : bénéficiaires 3 428 / 3 810 /
--   3 973 / 4 050 / 3 893 ; montants 1 530 554 843 / 2 357 429 788 /
--   2 560 989 546 / 2 817 073 092 / 2 865 630 056.
--   prestations familiales servies en 2024 (tableau 40) : allocations
--   prénatales 52 789 000 ; allocations familiales 8 236 741 500 ; IJ
--   2 865 630 056 ; frais médicaux 25 030 869 ; total 11 180 191 425.
--   assurance vieillesse servie en 2024 (tableau 34) : pensions
--   45 933 991 634, allocations 695 633 922, total 46 629 625 556.
--   risques professionnels servis (tableau 99), 2020 à 2024 : 924 131 793 /
--   1 025 338 054 / 973 733 533 / 1 061 233 811 / 1 074 113 823, dont en
--   espèces 782 901 197 / 840 775 119 / 802 054 456 / 918 307 061 /
--   934 445 128.
--   décès de pensionnés en 2024 (tableau 26) : 648, dont vieillesse 447,
--   veuves 103, ascendants 89, veufs 3, invalidité 3, orphelins 2,
--   anticipée 1 ; décès de rentiers (tableau 81) : 24, dont rentes viagères
--   21, veuves et veufs 2, ascendants 1.
-- Numérotation : suite du lot 7, P92 à P97.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P92 — T50 à T55 : qui compter chaque année, et quel montant
-- Question : un bénéficiaire de l'année est-il celui dont la prestation est
--            liquidée dans l'année, payée dans l'année, ou saisie ?
-- Objectif : pour chaque année de 2020 à 2025 et chaque tableau : prestations
--            et personnes liquidées (première notification, comme T25), par
--            sexe ; personnes saisies ; personnes payées pour un mois de
--            l'année, par sexe ; montant des mois de l'année (R3, R4) ; montant
--            des paiements saisis dans l'année.
-- Lecture  : comparer 2020 à 2024 aux chiffres publiés (en-tête) : T50 833
--            liquidées en 2024 ; T54 3 428 à 4 050 bénéficiaires par an. Les
--            prestations de T50 à T55 se paient en une fois ou sur une courte
--            période : liquidées et payées doivent être proches. Kaya 2017
--            retient les liquidées pour T54 et T55.
-- Tableaux : T50 à T55 ; tableaux 24, 34, 35, 40 et 97 de l'annuaire de la
--            CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P92_t50_t55_par_annee.tsv. En 2024, personnes
--            liquidées / payées / saisies ; montant des mois de l'année / des
--            paiements saisis dans l'année :
--            · T50 831 / 753 / 873 ; 517 334 647 / 497 818 548 ;
--            · T51 571 / 420 / 585 ; 187 083 247 / 190 149 232 ;
--            · T52 195 / 155 / 224 ; 46 020 175 / 51 859 178 ;
--            · T53 53 / 56 / 55 ; 23 238 000 / 17 812 800 ;
--            · T54 3 890 / 4 881 / 3 979 ; 2 745 737 842 / 2 749 474 700 ;
--            · T55 3 912 / 6 192 / 4 010 ; 50 677 500 / 52 149 000.
--            · T54 de 2020 à 2024 : liquidées 3 438 / 3 810 / 3 834 / 4 069 /
--              3 890 ; montants saisis 2 193 049 270 / 2 357 360 490 /
--              2 552 489 880 / 2 757 274 340 / 2 749 474 700.
--            · T54 et T55 : que des femmes. T52 : 371 IJ liquidées en 2024
--              pour 195 personnes.
-- Constat  : · Le bénéficiaire de l'année est la prestation liquidée : T50 831
--              pour 833 allocations de vieillesse liquidées publiées
--              (−0,2 %) ; T54 3 890 pour 3 893 (−0,1 %), et de 2020 à 2024 à
--              moins de 1 % sauf 2022 (−3,5 %). Les payées s'en écartent (T54
--              +25 %). Kaya 2017 est confirmé, et la règle vaut pour T50 à
--              T55.
--            · Le montant suit la date de saisie du paiement, pas le mois
--              payé : T50 497 818 548 saisis en 2024 pour 497 726 931 publiés
--              (0,02 %), contre +3,9 % au mois payé ; T55 −1,2 % contre
--              −4,0 % ; T51 −3,9 % contre −5,5 % ; T54 en 2021 à 0,003 %. La
--              CNSS publie ce qu'elle a versé dans l'année, rappels compris :
--              la règle R4 est à revoir pour les montants (P98).
--            · Les IJ AT/MP (T52) et les allocations d'incapacité (T53)
--              restent loin du publié (−21 % et −11 % en 2024), dans un sens
--              puis dans l'autre selon les années : petites sommes, sources à
--              éclaircir (P98).
--            · T54 en 2020 : 1,53 Md publiés pour nos 2,19 Md ; le chiffre
--              publié de cette année-là semble incomplet.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
perimetre AS (
    SELECT 'T50' AS tableau, 'RAV' AS code FROM DUAL
    UNION ALL SELECT 'T51', 'RAS'  FROM DUAL
    UNION ALL SELECT 'T51', 'RASC' FROM DUAL
    UNION ALL SELECT 'T51', 'RASO' FROM DUAL
    UNION ALL SELECT 'T51', 'RASA' FROM DUAL
    UNION ALL SELECT 'T52', 'AIJ'  FROM DUAL
    UNION ALL SELECT 'T53', 'AAI'  FROM DUAL
    UNION ALL SELECT 'T54', 'MIJ'  FROM DUAL
    UNION ALL SELECT 'T55', 'FAP'  FROM DUAL
    UNION ALL SELECT 'T55', 'MAP'  FROM DUAL
),
pe AS (
    SELECT p.PE_ID,
           pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION)              AS annee_liquidation,
           EXTRACT(YEAR FROM p.PE_DATE_INSERT)                    AS annee_saisie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
),
paie AS (
    SELECT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee_mois,
           EXTRACT(YEAR FROM deb.DEB_DATE_INSERT)                 AS annee_saisie,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID, EXTRACT(YEAR FROM deb.DEB_DATE_EFFET),
             EXTRACT(YEAR FROM deb.DEB_DATE_INSERT)
),
liquidees AS (
    SELECT annee_liquidation                                      AS annee,
           tableau,
           COUNT(*)                                               AS prestations,
           COUNT(DISTINCT IND_ID_PREST)                           AS personnes,
           COUNT(DISTINCT CASE WHEN sexe = '1' THEN IND_ID_PREST END) AS sexe_1,
           COUNT(DISTINCT CASE WHEN sexe = '2' THEN IND_ID_PREST END) AS sexe_2
    FROM pe
    GROUP BY annee_liquidation, tableau
),
saisies AS (
    SELECT annee_saisie AS annee, tableau, COUNT(DISTINCT IND_ID_PREST) AS personnes
    FROM pe
    GROUP BY annee_saisie, tableau
),
payees AS (
    SELECT pa.annee_mois                                          AS annee,
           pe.tableau,
           COUNT(DISTINCT pe.IND_ID_PREST)                        AS personnes,
           COUNT(DISTINCT CASE WHEN pe.sexe = '1' THEN pe.IND_ID_PREST END) AS sexe_1,
           COUNT(DISTINCT CASE WHEN pe.sexe = '2' THEN pe.IND_ID_PREST END) AS sexe_2,
           SUM(pa.montant)                                        AS montant
    FROM paie pa
    JOIN pe ON pe.PE_ID = pa.PE_ID
    GROUP BY pa.annee_mois, pe.tableau
),
payees_saisie AS (
    SELECT pa.annee_saisie AS annee, pe.tableau, SUM(pa.montant) AS montant
    FROM paie pa
    JOIN pe ON pe.PE_ID = pa.PE_ID
    GROUP BY pa.annee_saisie, pe.tableau
)
SELECT a.annee,
       t.tableau,
       NVL(l.prestations, 0)                                      AS prestations_liquidees,
       NVL(l.personnes, 0)                                        AS personnes_liquidees,
       NVL(l.sexe_1, 0)                                           AS liquidees_sexe_1,
       NVL(l.sexe_2, 0)                                           AS liquidees_sexe_2,
       NVL(s.personnes, 0)                                        AS personnes_saisies,
       NVL(p.personnes, 0)                                        AS personnes_payees,
       NVL(p.sexe_1, 0)                                           AS payees_sexe_1,
       NVL(p.sexe_2, 0)                                           AS payees_sexe_2,
       ROUND(NVL(p.montant, 0))                                   AS montant_mois_annee,
       ROUND(NVL(ps.montant, 0))                                  AS montant_saisi_annee
FROM annees a
CROSS JOIN (SELECT DISTINCT tableau FROM perimetre) t
LEFT JOIN liquidees     l  ON l.annee  = a.annee AND l.tableau  = t.tableau
LEFT JOIN saisies       s  ON s.annee  = a.annee AND s.tableau  = t.tableau
LEFT JOIN payees        p  ON p.annee  = a.annee AND p.tableau  = t.tableau
LEFT JOIN payees_saisie ps ON ps.annee = a.annee AND ps.tableau = t.tableau
ORDER BY t.tableau, a.annee;


-- -----------------------------------------------------------------------------
-- P93 — Le détail par code en 2024
-- Question : l'allocation de survivants (RAS) et ses parts (RASC conjoint,
--            RASO orphelin, RASA ascendant) comptent-elles les mêmes
--            personnes deux fois ? Quelle part de T55 vient de l'ancienne
--            allocation prénatale des PF (FAP) et de celle de la maternité
--            (MAP) ?
-- Objectif : pour 2024, par code : prestations et personnes liquidées ;
--            personnes payées pour un mois de 2024 ; montant des mois de 2024 ;
--            personnes qui ont aussi une prestation RAS (pour les parts).
-- Lecture  : si les personnes des parts ont aussi une RAS, on compte la
--            personne une fois (R5). Le montant de T55 se compare aux
--            52 789 000 d'allocations prénatales servies en 2024 (tableau 40).
-- Tableaux : T51, T55 ; tableaux 34 et 40 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P93_detail_par_code_2024.tsv. En 2024, par code,
--            prestations / personnes liquidées / personnes payées / montant
--            des mois de 2024 :
--            · RAV 832 / 831 / 753 / 517 334 647 ; RAS 0 / 0 / 0 / vide ; RASC
--              81 / 81 / 60 / 46 355 951 ; RASO 472 / 472 / 352 /
--              134 732 387 ; RASA 18 / 18 / 8 / 5 994 909 ;
--            · AIJ 371 / 195 / 155 / 46 020 175 ; AAI 53 / 53 / 56 /
--              23 238 000 ;
--            · MIJ 3 892 / 3 890 / 4 881 / 2 745 737 842 ; MAP 3 858 / 3 857 /
--              6 143 / 50 361 000 ; FAP 57 / 56 / 50 / 316 500.
--            · Personnes des parts qui ont aussi une RAS : 3 (RASC), 1 (RASO).
-- Constat  : · L'allocation de survivants ne se liquide plus sous RAS : ce
--              sont ses parts, conjoint (81), orphelin (472) et ascendant
--              (18), qui portent les personnes, sans doublon. T51 compte ces
--              personnes.
--            · T55 est presque tout entier la prénatale de la maternité (MAP,
--              3 857) ; l'ancienne FAP des PF s'éteint (56).
--            · Une IJ AT/MP se liquide en plusieurs fois (371 IJ pour 195
--              victimes) : T52 compte les personnes.
-- -----------------------------------------------------------------------------
WITH pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           p.IND_ID_PREST,
           EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION)              AS annee_liquidation
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE IN ('RAV', 'RAS', 'RASC', 'RASO', 'RASA', 'AIJ', 'AAI', 'MIJ',
                         'FAP', 'MAP')
),
paie AS (
    SELECT deb.PE_ID,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID
),
avec_ras AS (
    SELECT DISTINCT IND_ID_PREST FROM pe WHERE TPE_CODE = 'RAS'
)
SELECT pe.TPE_CODE                                                AS code,
       SUM(CASE WHEN pe.annee_liquidation = 2024 THEN 1 ELSE 0 END) AS prestations_liquidees,
       COUNT(DISTINCT CASE WHEN pe.annee_liquidation = 2024
                           THEN pe.IND_ID_PREST END)              AS personnes_liquidees,
       COUNT(DISTINCT CASE WHEN pa.PE_ID IS NOT NULL
                           THEN pe.IND_ID_PREST END)              AS personnes_payees_2024,
       ROUND(SUM(pa.montant))                                     AS montant_mois_2024,
       COUNT(DISTINCT CASE WHEN r.IND_ID_PREST IS NOT NULL
                            AND pe.TPE_CODE <> 'RAS'
                           THEN pe.IND_ID_PREST END)              AS personnes_aussi_en_ras
FROM pe
LEFT JOIN paie     pa ON pa.PE_ID       = pe.PE_ID
LEFT JOIN avec_ras r  ON r.IND_ID_PREST = pe.IND_ID_PREST
GROUP BY pe.TPE_CODE
ORDER BY pe.TPE_CODE;


-- -----------------------------------------------------------------------------
-- P94 — Les services passés des allocations de vieillesse (T50)
-- Question : le « nombre moyen de services passés » de T50 se calcule-t-il,
--            comme le total de T42 ?
-- Objectif : pour les allocations de vieillesse (RAV) liquidées chaque année de
--            2020 à 2025, la dernière de la personne dans l'année : mois
--            cotisés vides, à zéro, au-dessus de 600, valides ; moyenne et
--            médiane des mois valides ; cotisations dans une autre caisse
--            (PE_COT_AUTRE) ; moyenne de la part CNSS (total moins autre
--            caisse).
-- Lecture  : l'allocation de vieillesse va aux carrières courtes (lot 2 P30 :
--            92 mois en médiane, 489 au plus). Si les mois sont remplis, la
--            moyenne de T50 se calcule comme T42 (part CNSS en coordination).
-- Tableaux : T50.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P94_services_passes_rav.tsv. Allocations de
--            vieillesse liquidées, personnes / moyenne / médiane des mois /
--            part CNSS moyenne :
--            · 2020 585 / 85,2 / 86 / 84,9 ; 2021 594 / 88,2 / 85 / 88,2 ;
--              2022 707 / 89,5 / 86 / 89,3 ; 2023 738 / 93,9 / 96 / 93,9 ;
--              2024 831 / 95,7 / 98 / 95,5 ; 2025 823 / 95,6 / 96 / 95,6.
--            · Aucun mois vide, à zéro ou au-dessus de 600 ; cotisations dans
--              une autre caisse : 0 à 4 par an.
-- Constat  : · Le nombre moyen de services passés de T50 se calcule : environ
--              8 ans (96 mois en 2024), loin des 22 ans des nouveaux retraités
--              (T42). La coordination n'y pèse presque pas (0,2 mois).
-- -----------------------------------------------------------------------------
WITH rav AS (
    SELECT EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION)              AS annee,
           p.PE_MOIS_COTISATION                                   AS mois,
           p.PE_COT_AUTRE                                         AS mois_autre_caisse,
           ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION),
                                           p.IND_ID_PREST
                              ORDER BY p.PE_DATE_NOTIFICATION DESC, p.PE_ID DESC) AS rang
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE = 'RAV'
      AND p.PE_DATE_NOTIFICATION >= DATE '2020-01-01'
      AND p.PE_DATE_NOTIFICATION <  DATE '2026-01-01'
)
SELECT annee,
       COUNT(*)                                                   AS personnes,
       SUM(CASE WHEN mois IS NULL THEN 1 ELSE 0 END)              AS mois_vides,
       SUM(CASE WHEN mois = 0 THEN 1 ELSE 0 END)                  AS mois_a_zero,
       SUM(CASE WHEN mois > 600 THEN 1 ELSE 0 END)                AS plus_de_600,
       SUM(CASE WHEN mois BETWEEN 1 AND 600 THEN 1 ELSE 0 END)    AS mois_valides,
       ROUND(AVG(CASE WHEN mois BETWEEN 1 AND 600
                      THEN mois END), 1)                          AS moyenne,
       MEDIAN(CASE WHEN mois BETWEEN 1 AND 600 THEN mois END)     AS mediane,
       COUNT(mois_autre_caisse)                                   AS avec_autre_caisse,
       ROUND(AVG(CASE WHEN mois BETWEEN 1 AND 600
                      THEN GREATEST(mois - NVL(mois_autre_caisse, 0), 0)
                 END), 1)                                         AS moyenne_part_cnss
FROM rav
WHERE rang = 1
GROUP BY annee
ORDER BY annee;


-- -----------------------------------------------------------------------------
-- P95 — T56 : les montants servis par branche
-- Question : la somme des débours payés par branche retombe-t-elle sur les
--            montants publiés ? Où sont les prestations en nature des AT/MP ?
-- Objectif : pour chaque année de 2020 à 2025 (mois payé, R4), par branche du
--            débours (CODE_BRANCHE) et par type de débours : débours au statut
--            D ou P, montant, débours sans prestation en espèces (PE_ID vide)
--            et leur montant.
-- Lecture  : en 2024, V face aux 46 629 625 556 de l'assurance vieillesse
--            (pensions et allocations) ; F et M ensemble face aux
--            11 180 191 425 des prestations familiales ; A face aux
--            1 074 113 823 des risques professionnels, dont 934 445 128 en
--            espèces. Les débours sans PE_ID sont sans doute les prestations
--            en nature (soins AT/MP, frais médicaux de maternité).
-- Tableaux : T56 ; tableaux 34, 40, 97 et 99 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P95_montants_par_branche.tsv. En 2024, au mois
--            payé, débours D ou P par branche et type : V paiements (PM)
--            47 237 434 040, ajustements (AJ) 587 162 398 ; F PM
--            7 954 774 500, AJ 201 000 381 ; M PM 2 796 098 842, AJ
--            8 766 415 ; A PM 744 609 484, remboursements (RB) 18 787 945, AJ
--            25 799 280.
--            · A, PM et RB, de 2020 à 2024 : 801 854 467 / 810 053 286 /
--              796 144 584 / 808 542 605 / 763 397 429.
--            · Aucune autre branche. Les paiements (PM) ont toujours une
--              prestation, les ajustements et remboursements jamais.
-- Constat  : · AVS : 47,24 Md de paiements pour 46,63 Md publiés (+1,3 %).
--              PF : 10,75 Md pour 11,18 Md (−3,8 %). AT/MP : 0,76 Md avec les
--              remboursements, pour 1,07 Md publiés dont 0,93 Md en espèces
--              (−18 % sur les espèces seules) ; l'écart grandit depuis 2020.
--            · La ligne « Autres » de T56 est vide : tous les débours sont
--              dans une des quatre branches.
--            · Les ajustements (AJ) corrigent des paiements (trop-perçus,
--              retenues : T31). Proposition BI : T56 = paiements (PM) et
--              remboursements (RB), sans les ajustements.
--            · Les remboursements (18,8 M en 2024) ne couvrent pas les 139,7 M
--              de prestations en nature AT/MP publiées : elles sont payées
--              ailleurs. Question à la DSI.
--            · Au mois payé, il manque surtout les rentes de survivants AT/MP,
--              payées avec un gros rappel (P85) : P98 compare les dates.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                      AS annee,
       NVL(deb.CODE_BRANCHE, '?')                                 AS branche,
       deb.DEB_TYPE                                               AS type_debours,
       COUNT(*)                                                   AS debours,
       ROUND(SUM(deb.DEB_MONTANT))                                AS montant,
       SUM(CASE WHEN deb.PE_ID IS NULL THEN 1 ELSE 0 END)         AS sans_prestation,
       ROUND(SUM(CASE WHEN deb.PE_ID IS NULL
                      THEN deb.DEB_MONTANT END))                  AS montant_sans_prestation
FROM USER_DWH.DEBOURS deb
WHERE deb.DEB_STATUT IN ('D', 'P')
  AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
  AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM deb.DEB_DATE_EFFET), NVL(deb.CODE_BRANCHE, '?'), deb.DEB_TYPE
ORDER BY annee, branche, type_debours;


-- -----------------------------------------------------------------------------
-- P96 — Les colonnes de décès de la base
-- Question : T57 compte les « assurés décédés déclarés » (Kaya 2017). La base
--            a-t-elle une date de déclaration du décès, ou seulement la date du
--            décès ?
-- Objectif : les colonnes des tables de USER_DWH dont le nom parle de décès,
--            d'après le dictionnaire d'Oracle.
-- Lecture  : s'il n'y a que IND_DATE_DECES, T57 se compte à la date du décès,
--            comme le tableau 26 de l'annuaire de la CNSS (décès par mois).
-- Tableaux : T57.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P96_colonnes_deces.tsv. Dates de décès :
--            IND_DATE_DECES (INDIVIDU, et ASSURE, CONJOINT, ENFANT, ASCENDANT,
--            TUTEUR, MEMBRE_FAMILLE, PENSION_RETRAITE53,
--            V_PRESTATAIRES_DECEDES_BI) ; TR_DATE_DECEDE (TRAVAILLEUR) ;
--            DATE_DECES (AGENT_CNSS, AVANCE_RESTANT_DU,
--            TROP_PERCU_RESTANT_DU).
--            · Indicateur DOS_DECES_IMMEDIAT : DOSSIER, DOSSIER_AT,
--              CNSS_DOSSIER, V_DOSSIER_PRESTATION (texte) ; TSINISTRE
--              (nombre).
-- Constat  : · Pas de date de déclaration du décès : T57 se compte à la date
--              du décès. Calculé au début de l'année suivante, il ne voit que
--              les décès déjà connus, donc « déclarés » (Kaya 2017).
--            · DOS_DECES_IMMEDIAT marque sans doute les accidents mortels : la
--              bonne source pour les sinistrés de T57 (P100).
--            · Une vue V_PRESTATAIRES_DECEDES_BI existe déjà pour la BI : sa
--              définition dira ce que la DSI entend par prestataire décédé
--              (P101).
-- -----------------------------------------------------------------------------
SELECT TABLE_NAME,
       COLUMN_NAME,
       DATA_TYPE
FROM ALL_TAB_COLUMNS
WHERE OWNER = 'USER_DWH'
  AND (   COLUMN_NAME LIKE '%DECES%'
       OR COLUMN_NAME LIKE '%DCD%'
       OR COLUMN_NAME LIKE '%DECED%'
       OR COLUMN_NAME LIKE '%MORT%')
ORDER BY TABLE_NAME, COLUMN_NAME;


-- -----------------------------------------------------------------------------
-- P97 — T57 : les décédés par branche
-- Question : combien de décès chaque année, dans chaque branche, selon les
--            personnes que retient l'atelier de Kaya 2017 ?
-- Objectif : pour chaque année de 2020 à 2025 (date du décès), personnes
--            décédées qui avaient une prestation en cours à leur décès (effet
--            avant, pas radiée plus d'un mois avant) : titulaires d'une pension
--            directe (vieillesse, anticipée, invalidité), dont vieillesse ;
--            tous les pensionnés (survivants compris) ; titulaires d'une rente
--            viagère ; titulaires d'une rente de survivant ; victimes d'un
--            accident du travail décédées dans l'année de l'accident ;
--            titulaires d'un dossier d'allocations familiales en cours.
-- Lecture  : en 2024, « tous les pensionnés » face aux 648 décès de
--            pensionnés du tableau 26 (vieillesse 447) ; rentes face aux 24
--            décès de rentiers du tableau 81 (rentes viagères 21). Kaya 2017
--            retient la pension directe, le rentier et le sinistré,
--            l'allocataire.
-- Tableaux : T57 ; tableaux 26 et 81 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P97_decedes_par_branche.tsv. Décès par année (date
--            du décès), pension directe / dont vieillesse / tous pensionnés /
--            rente viagère / rente de survivant / victimes AT / allocataires
--            PF :
--            · 2020 704 / 693 / 813 / 24 / 3 / 22 / 347 ;
--            · 2021 703 / 694 / 789 / 27 / 3 / 17 / 328 ;
--            · 2022 585 / 580 / 692 / 17 / 1 / 26 / 285 ;
--            · 2023 550 / 546 / 657 / 18 / 3 / 14 / 286 ;
--            · 2024 622 / 619 / 747 / 15 / 1 / 10 / 194 ;
--            · 2025 303 / 299 / 380 / 7 / 0 / 2 / 60.
-- Constat  : · En 2024, face à l'annuaire de la CNSS : tous les pensionnés 747
--              pour 648 (+15 %), vieillesse 619 pour 447 (+38 %) ; rentiers 16
--              pour 24.
--            · 2025 n'a que la moitié des années précédentes : les décès se
--              déclarent tard. Hypothèse : la CNSS compte les décès connus
--              quand elle publie, la base y ajoute ceux déclarés depuis. P99
--              le vérifie par la date de saisie de la radiation.
--            · L'annuaire publie 89 décès de pensionnés ascendants en 2024,
--              alors qu'il n'y a que 59 pensions d'ascendant en cours (P74) :
--              chiffre à vérifier avec la CNSS.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
deces AS (
    SELECT IND_ID,
           TRUNC(IND_DATE_DECES)                                  AS date_deces,
           EXTRACT(YEAR FROM IND_DATE_DECES)                      AS annee
    FROM USER_DWH.INDIVIDU
    WHERE IND_DATE_DECES >= DATE '2020-01-01'
      AND IND_DATE_DECES <  DATE '2026-01-01'
),
-- les prestations en cours au décès (radiées au plus tôt un mois avant)
en_cours AS (
    SELECT DISTINCT de.annee, de.IND_ID, p.TPE_CODE
    FROM deces de
    JOIN USER_DWH.PRESTATION_ESP p ON p.IND_ID_PREST = de.IND_ID
    WHERE p.TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RPSV', 'RPSF', 'RPSE', 'RPA',
                         'ARI', 'ARSV', 'ARSF', 'ARSE', 'ARSA')
      AND p.PE_DATE_EFFET <= de.date_deces
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION >= de.date_deces - 31)
),
-- les victimes d'un accident du travail, décédées dans l'année qui suit
victimes AS (
    SELECT DISTINCT de.annee, de.IND_ID
    FROM deces de
    JOIN USER_DWH.DOSSIER ds ON ds.IND_ID = de.IND_ID
    WHERE ds.TDOS_CODE = 'A'
      AND ds.DOS_DATE_ACCIDENT <= de.date_deces
      AND de.date_deces < ds.DOS_DATE_ACCIDENT + 366
),
-- les titulaires d'un dossier d'allocations familiales en cours au décès
allocataires AS (
    SELECT DISTINCT de.annee, de.IND_ID
    FROM deces de
    JOIN USER_DWH.DOSSIER        ds ON ds.IND_ID   = de.IND_ID
    JOIN USER_DWH.PRESTATION_ESP p  ON p.DOS_CODE  = ds.DOS_CODE
    WHERE p.TPE_CODE = 'FAF'
      AND p.PE_DATE_EFFET <= de.date_deces
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION >= de.date_deces - 31)
)
SELECT a.annee,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE IN ('RPV', 'RPRA', 'RPI'))   AS pension_directe,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE = 'RPV')                      AS dont_vieillesse,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE LIKE 'RP%')                   AS tous_pensionnes,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE = 'ARI')                      AS rente_viagere,
       (SELECT COUNT(DISTINCT IND_ID) FROM en_cours e
        WHERE e.annee = a.annee AND e.TPE_CODE LIKE 'ARS%')                  AS rente_survivant,
       (SELECT COUNT(*) FROM victimes v WHERE v.annee = a.annee)            AS victimes_at,
       (SELECT COUNT(*) FROM allocataires f WHERE f.annee = a.annee)        AS allocataires_pf
FROM annees a
ORDER BY a.annee;


-- =============================================================================
-- PARTIE B — SUITES DU LOT 8 (rédigée et exécutée le 01/11/2026 après les
-- résultats de P92 à P97)
-- Trois points que la partie A n'a pas tranchés :
--   les montants : quelle date fait l'année d'un paiement pour la CNSS (mois
--   payé, saisie, approbation, décaissement) ?                           → P98
--   T57 : nos décès de 2024 dépassent l'annuaire de 15 à 38 % ; est-ce le
--   retard de déclaration ?                                              → P99
--   T57 : les sinistrés (accidents mortels) et la vue des décédés de la
--   DSI                                                            → P100, P101
-- P98 lit DEBOURS : plusieurs minutes.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P98 — Les montants de 2024 par code, selon la date retenue
-- Question : la CNSS publie-t-elle les montants au mois payé (règle R4), à la
--            saisie du paiement, à son approbation ou à son décaissement ? P92
--            penche pour la saisie (allocations de vieillesse à 0,02 %).
-- Objectif : pour chaque code de prestation, les paiements (PM) au statut D ou
--            P dont le mois payé, la saisie, l'approbation ou le décaissement
--            tombe en 2024 ; paiements saisis en 2024 sans date de
--            décaissement.
-- Lecture  : comparer chaque ligne à l'annuaire de la CNSS 2024 : RPV et RPRA
--            38 235 880 033 (pensions de vieillesse) ; RBEC 537 343 000
--            (majoration pour enfant) ; RPSV, RPSF, RPSE et RPA 7 047 164 871
--            (survivants) ; RPI 113 603 730 ; RAV 497 726 931 ; RASC, RASO et
--            RASA 197 906 991 ; FAF 8 236 741 500 ; MIJ 2 865 630 056 ; MAP et
--            FAP 52 789 000 ; AIJ 65 420 082 ; AAI 20 034 111 ;
--            ARI 455 021 317 ; ARSV, ARSF, ARSE et ARSA 368 602 710. La
--            colonne qui retombe partout donne la date de la CNSS, et peut-être
--            une nouvelle règle R4 pour tous les montants de T31 à T56.
-- Tableaux : T31 à T56 ; tableaux 34, 40 et 97 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P98_montants_2024_par_date.tsv. Écart à l'annuaire
--            de la CNSS 2024 selon la date retenue (mois payé / saisie /
--            décaissement) :
--            · RPV +0,7 % / +1,5 % / −1,9 % (38 235 880 033
--              publiés) ; RBEC +3,1 % / +0,4 % / −45 % ; survivants (RPSV,
--              RPSF, RPSE, RPA) +2,5 % / +2,0 % / −8,5 % ; RPI +2,2 % /
--              −0,4 % / −4,6 % ; RAV +3,9 % / 0,0 % / −1,7 % ; parts RAS
--              −5,5 % / −3,9 % / −6,5 % ;
--            · FAF −3,4 % / −2,9 % / −27 % ; MIJ −4,2 % / −4,1 % / −3,3 % ;
--              MAP et FAP −4,0 % / −1,2 % / −12 % ;
--            · AIJ −30 % / −21 % / −7 % ; AAI +16 % / −11 % / −26 % ; ARI
--              −12 % / −13 % / −16 % ; rentes de survivants −25 % / −12,5 % /
--              −18 %.
--            · Branche vieillesse : +1,3 % au mois payé, +1,8 % à la saisie,
--              pour 46 629 625 556 publiés. L'approbation donne presque
--              toujours le montant de la saisie. Paiements saisis en 2024 sans
--              date de décaissement : 1 124 364 pour FAF, 99 124 pour RBEC,
--              41 786 pour RPSV.
-- Constat  : · Aucune date ne retombe partout. La saisie du paiement (ou son
--              approbation, presque identique) fait mieux que le mois payé sur
--              11 lignes sur 16, et tombe juste sur les allocations de
--              vieillesse (0,0 %), la majoration pour enfant (+0,4 %) et
--              l'invalidité (−0,4 %). Le mois payé ne fait mieux que sur la
--              pension de vieillesse (+0,7 % contre +1,5 %), la plus grosse
--              ligne.
--            · Le décaissement est trop souvent vide (allocations familiales,
--              majoration) : inutilisable.
--            · Proposition BI : un paiement compte l'année de sa saisie, celle
--              où la CNSS le verse, rappels compris (nouvelle règle R4). À
--              décider : elle changerait les montants de T31 à T56 de 1 à 2 %
--              sur les grosses lignes.
--            · Les rentes AT/MP restent sous le publié quelle que soit la date
--              (rentes viagères −12 %, IJ et allocations d'incapacité de −7 à
--              −26 %) : des paiements AT/MP manquent ailleurs. Question à la
--              DSI, avec les prestations en nature (P95).
-- -----------------------------------------------------------------------------
SELECT p.TPE_CODE                                                 AS code,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_EFFET >= DATE '2024-01-01'
                       AND deb.DEB_DATE_EFFET <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS mois_paye_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_INSERT >= DATE '2024-01-01'
                       AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS saisi_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_APPROBATION >= DATE '2024-01-01'
                       AND deb.DEB_DATE_APPROBATION <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS approuve_2024,
       ROUND(SUM(CASE WHEN deb.DEB_DATE_DECAISSEMENT >= DATE '2024-01-01'
                       AND deb.DEB_DATE_DECAISSEMENT <  DATE '2025-01-01'
                      THEN deb.DEB_MONTANT END))                  AS decaisse_2024,
       SUM(CASE WHEN deb.DEB_DATE_INSERT >= DATE '2024-01-01'
                 AND deb.DEB_DATE_INSERT <  DATE '2025-01-01'
                 AND deb.DEB_DATE_DECAISSEMENT IS NULL
                THEN 1 ELSE 0 END)                                AS saisis_sans_decaissement
FROM USER_DWH.DEBOURS deb
JOIN USER_DWH.PRESTATION_ESP p ON p.PE_ID = deb.PE_ID
WHERE deb.DEB_TYPE = 'PM'
  AND deb.DEB_STATUT IN ('D', 'P')
  AND (   (deb.DEB_DATE_EFFET        >= DATE '2024-01-01' AND deb.DEB_DATE_EFFET        < DATE '2025-01-01')
       OR (deb.DEB_DATE_INSERT       >= DATE '2024-01-01' AND deb.DEB_DATE_INSERT       < DATE '2025-01-01')
       OR (deb.DEB_DATE_APPROBATION  >= DATE '2024-01-01' AND deb.DEB_DATE_APPROBATION  < DATE '2025-01-01')
       OR (deb.DEB_DATE_DECAISSEMENT >= DATE '2024-01-01' AND deb.DEB_DATE_DECAISSEMENT < DATE '2025-01-01'))
GROUP BY p.TPE_CODE
ORDER BY p.TPE_CODE;


-- -----------------------------------------------------------------------------
-- P99 — Les décès de retraités de 2024 : connus quand, et par mois
-- Question : la CNSS publie 447 décès de retraités en 2024 (tableau 26), nous
--            en voyons 619. Les autres ont-ils été déclarés après la
--            publication ?
-- Objectif : pour les titulaires d'une pension de vieillesse en cours à leur
--            décès (comme P97), morts en 2024, par mois du décès : décès ;
--            pension radiée ; pension modifiée pour la dernière fois avant
--            avril 2025, avant juillet 2025 ; fiche de la personne modifiée
--            pour la dernière fois avant avril 2025.
-- Lecture  : comparer mois par mois au tableau 26 (vieillesse : 33, 29, 58,
--            82, 65, 27, 27, 32, 36, 31, 14 et 13 de janvier à décembre). Si
--            les décès « connus avant avril » ou « avant juillet 2025 »
--            retombent sur ces chiffres, l'écart vient du retard de
--            déclaration : rien à corriger, la BI calcule l'exercice au début
--            de l'année suivante. La dernière modification n'est qu'un indice :
--            une pension modifiée plus tard pour une autre raison paraît
--            déclarée tard.
-- Tableaux : T57 ; tableau 26 de l'annuaire de la CNSS.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P99_deces_retraites_2024_par_mois.tsv. Retraités
--            morts en 2024, par mois du décès (décès / pension modifiée pour
--            la dernière fois avant avril 2025 / publié, tableau 26) :
--            · janvier 42 / 33 / 33 ; février 33 / 28 / 29 ; mars 72 / 59 /
--              58 ; avril 102 / 82 / 82 ; mai 76 / 66 / 65 ; juin 36 / 27 /
--              27 ; juillet 36 / 24 / 27 ; août 45 / 32 / 32 ; septembre 52 /
--              34 / 36 ; octobre 45 / 29 / 31 ; novembre 32 / 14 / 14 ;
--              décembre 48 / 16 / 13.
--            · Totaux : 619 décès ; 444 connus avant avril 2025, 530 avant
--              juillet ; publiés 447.
-- Constat  : · Les décès connus de la base avant avril 2025 retombent sur
--              l'annuaire, mois par mois : 444 pour 447, six mois exacts. La
--              CNSS a arrêté ses chiffres de 2024 vers la fin mars 2025 ; les
--              175 autres décès de 2024 ont été déclarés après.
--            · C'est la même cause que l'écart des veuves et des orphelins
--              (T34, T35) : la base d'aujourd'hui sait des choses que
--              l'annuaire ne savait pas. Proposition BI (nouvelle règle R13) :
--              calculer l'exercice N sur le cliché de fin mars N+1, la date de
--              l'annuaire de la CNSS.
--            · T57 se compte à la date du décès, sur ce cliché : il retombera
--              sur les décès « déclarés » de Kaya 2017.
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


-- -----------------------------------------------------------------------------
-- P100 — Les accidents du travail mortels (DOS_DECES_IMMEDIAT)
-- Question : DOS_DECES_IMMEDIAT marque-t-il les accidents mortels, les
--            « sinistrés » de T57 (Kaya 2017) ?
-- Objectif : pour les dossiers AT/MP dont l'accident date de 2020 à 2025, par
--            année de l'accident et valeur de DOS_DECES_IMMEDIAT : dossiers ;
--            victimes décédées (date de décès remplie) ; décédées dans l'année
--            qui suit l'accident ; dossiers qui donnent une rente de
--            survivant.
-- Lecture  : si une valeur (O, 1…) va avec un décès et des rentes de
--            survivants, c'est l'accident mortel : T57 compte ces victimes
--            dans la branche AT/MP, à l'année de leur décès.
-- Tableaux : T57.
-- Résultat : Exécutée le 01/11/2026 ; détail dans
--            resultats/lot8_P100_deces_immediat.tsv. Dossiers AT/MP par année
--            d'accident et valeur de DOS_DECES_IMMEDIAT (dossiers / victime
--            décédée / décédée dans l'année / avec rente de survivant) :
--            · 2020 vide 1 385 / 35 / 20 / 14 ; 2021 vide 1 370 / 23 / 17 /
--              12 ;
--            · 2022 N 2, O 1 / 1 / 1 / 1, vide 1 355 / 26 / 23 / 20 ;
--            · 2023 N 3, vide 1 231 / 15 / 15 / 8 ;
--            · 2024 N 22, O 3 / 1 / 1 / 1, vide 1 167 / 9 / 8 / 5 ;
--            · 2025 N 753, O 6, vide 411.
-- Constat  : · DOS_DECES_IMMEDIAT (O ou N) n'est rempli que depuis 2024-2025 :
--              trop récent pour l'historique de T57.
--            · Les accidents mortels se lisent mieux par le décès de la
--              victime dans l'année qui suit l'accident (de 8 à 23 par an),
--              qui va presque toujours avec une rente de survivant : c'est la
--              règle de la requête T57 pour les sinistrés. DOS_DECES_IMMEDIAT
--              = O pourra la compléter quand il sera rempli partout.
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


-- -----------------------------------------------------------------------------
-- P101 — La vue des prestataires décédés préparée pour la BI
-- Question : la DSI a créé V_PRESTATAIRES_DECEDES_BI. Qui y met-elle ?
-- Objectif : le texte de la vue, d'après le dictionnaire d'Oracle (colonne
--            TEXT, à copier en entier).
-- Lecture  : la définition de la DSI (quelles prestations, quelle date) se
--            compare à la règle de T57 ; si elle convient, T57 peut s'appuyer
--            dessus.
-- Tableaux : T57.
-- Résultat : Exécutée le 01/11/2026 (capture d'écran) ; texte relevé dans
--            resultats/lot8_P101_vue_prestataires_decedes.sql. La vue joint
--            PRESTATION_ESP et INDIVIDU sur l'ayant droit (IND_ID_PREST), et
--            garde les prestations notifiées (PE_DATE_NOTIFICATION remplie)
--            dont l'ayant droit a une date de décès.
--            · Colonnes : celles de la prestation (code, dossier, statut,
--              dates d'effet, de radiation, de saisie et de notification, mois
--              cotisés, taux d'incapacité, PE_COT_AUTRE…) et de la personne
--              (naissance, sexe, décès, saisie, mise à jour), plus le nom, le
--              prénom, l'adresse, la situation matrimoniale et le téléphone.
-- Constat  : · La vue de la DSI prend tous les prestataires décédés, sans
--              filtre de type, d'année ni de prestation en cours : un enfant
--              d'allocataire, une victime payée en IJ, un orphelin y entrent.
--              T57 retient moins de personnes (Kaya 2017 : pension directe,
--              rentier et sinistré, allocataire) : la vue peut servir de
--              source, mais il faut garder les filtres de la requête T57.
--            · Elle ne donne pas de date de déclaration du décès : la règle
--              R13 (cliché de fin mars) reste nécessaire.
--            · Elle expose des données nominatives (nom, prénom, adresse,
--              téléphone) que la BI n'a pas à lire. Question à la DSI : peut-
--              on les retirer de cette vue destinée à la BI ?
-- -----------------------------------------------------------------------------
SELECT VIEW_NAME,
       TEXT_LENGTH,
       TEXT
FROM ALL_VIEWS
WHERE OWNER     = 'USER_DWH'
  AND VIEW_NAME = 'V_PRESTATAIRES_DECEDES_BI';
