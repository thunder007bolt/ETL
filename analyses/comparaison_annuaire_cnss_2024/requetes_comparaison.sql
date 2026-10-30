-- =============================================================================
-- COMPARAISON — La base face au tableau 18 de l'annuaire CNSS 2024
-- Rédigé et exécuté le 30/10/2026 (A04 à relancer après un ORA-01722). La
-- partie B (A06 à A08), ajoutée après les résultats, reste à exécuter.
-- Comparaison isolée, à la demande de l'utilisateur : elle ne fait pas partie
-- du suivi des prestations (analyses/prestations) et n'en change aucune
-- règle. Requêtes numérotées A01 à A08 (A pour annuaire).
-- Sur la base source, schéma USER_DWH.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages et sommes
-- seulement. A01, A04 et A05 lisent DEBOURS (paiements de 2020 à 2024 des
-- seules pensions) : quelques minutes chacune.
-- Données nominatives : jamais lues. Sexe, date de naissance et date de décès
-- ne servent qu'à compter.
-- Pourquoi ce lot : l'utilisateur a retrouvé l'annuaire statistique de la CNSS
-- n° 32, année 2024 (cnss_files/Annuaire_2024_CNSS_BURKINA-FASO.pdf, produit
-- par la DPC à partir de la base et des rapports des directions). Son
-- tableau 18 donne les bénéficiaires de pension de 2020 à 2024 par type.
-- Reconstituer ce tableau dit deux choses :
--   · si notre base donne les mêmes chiffres que ceux publiés par la CNSS ;
--   · quelle définition du « pensionné » la CNSS applique : pension active au
--     31 décembre (le tableau 22 dit « au 31/12/2024 »), payée dans l'année,
--     payée au 4e trimestre… C'est la question de la règle R6 (T32 à T41).
-- Chiffres publiés (annuaire CNSS 2024, p. 16 à 18) :
--   Tableau 18 (2020 / 2021 / 2022 / 2023 / 2024) :
--     retraite vieillesse 25 161 / 26 415 / 27 873 / 29 042 / 30 730
--     veuves              19 309 / 19 964 / 20 734 / 20 832 / 21 438
--     veufs                  225 /    249 /    274 /    298 /    329
--     orphelins            8 982 /  8 789 /  8 667 /  8 687 /  9 478
--     invalidité             114 /    111 /    115 /    116 /    119
--     anticipée               69 /     66 /     68 /     75 /     78
--     ascendants              48 /     53 /     59 /     54 /     59
--     total               53 908 / 55 647 / 57 790 / 59 104 / 62 231
--   Tableau 19 (2024) : retraite vieillesse 24 627 hommes, 6 103 femmes ;
--     38 235 880 033 FCFA.
--   Tableau 20 (2024) : veuves, veufs, orphelins, ascendants 5 792 hommes,
--     25 512 femmes ; 7 047 164 871 FCFA ; les 9 478 orphelins ont tous moins
--     de 25 ans (3 812 ont de 20 à 24 ans).
--   Tableau 21 (2024) : invalidité 86 hommes, 33 femmes ; 113 603 730 FCFA.
--   Tableau 22 (au 31/12/2024, par direction) : DRO 36 797, DRB 18 271, DRN
--     3 305, DRF 2 230, DRD 1 628.
--   Tableau 23 (nouveaux pensionnés de 2024) : vieillesse 2 486, veuves 732,
--     veufs 34, orphelins 769, invalidité 6, anticipée 3, ascendants 7 ;
--     total 4 037.
--   Chiffres clés : prestations de la branche des pensions 46 629 625 556
--     FCFA en 2024.
-- Correspondance (P34) : RPV retraite vieillesse, RPSV veuve, RPSF veuf, RPSE
-- orphelin, RPI invalidité, RPRA retraite anticipée, RPA ascendant.
-- Limite : la base est une copie au 02/02/2026. Une radiation ou un décès
-- saisi en 2025 avec une date de 2024 change « actif au 31/12/2024 » depuis la
-- publication ; les paiements de 2024, eux, ne bougent plus.
-- Numérotation : A01 à A05, puis A06 à A08 (partie B).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- A01 — Tableau 18 : bénéficiaires de pension de 2020 à 2024, selon plusieurs
--       définitions
-- Objectif : pour chaque année et chaque type de pension, compter les
--            pensions :
--            - actives au 31/12 par les dates (effet avant, ni radiée ni
--              suspendue, comme P25) ;
--            - actives, l'ayant droit encore en vie au 31/12 ;
--            - actives, en vie et de moins de 25 ans (utile aux orphelins) ;
--            - payées au moins une fois dans l'année (débours PM au statut D
--              ou P, mois payé dans l'année, règles R3 et R4) ;
--            - payées ou seulement validées (statut V compris) ;
--            - payées au 4e trimestre, payées en décembre ;
--            - actives et payées dans l'année ;
--            et les personnes distinctes payées, le montant payé dans l'année.
-- Lecture  : la colonne qui retombe sur le tableau 18, année après année et
--            ligne par ligne, donne la définition de la CNSS. Si aucune ne
--            tombe juste, l'écart dit ce qui diffère (base modifiée depuis,
--            collecte complétée à la main dans les directions…). Pour
--            2024, comparer aussi le montant aux tableaux 19 à 21.
-- Tableaux : T32 à T36 (CIPRES) ; tableau 18 de l'annuaire CNSS.
-- Résultat : Pensions actives et en vie au 31/12, de 2020 à 2024 : retraite
--            vieillesse 25 246 / 26 558 / 28 037 / 29 230 / 31 180 ; veuves
--            19 643 / 20 300 / 20 861 / 19 945 / 20 369 ; veufs 236 / 276 /
--            304 / 324 / 356 ; orphelins 15 130 / 15 313 / 15 752 / 13 741 /
--            13 769, dont moins de 25 ans 9 705 / 9 558 / 9 296 / 8 123 /
--            7 995 ; invalidité 111 / 109 / 119 / 121 / 125 ; anticipée 70 /
--            70 / 78 / 78 / 87 ; ascendants 58 / 60 / 65 / 59 / 59. Payées dans
--            l'année, retraite vieillesse 23 548 / 24 682 / 25 813 / 27 320 /
--            28 737 ; veuves 19 090 / 19 689 / 20 178 / 20 631 / 19 888 ;
--            orphelins 6 895 / 6 880 / 6 628 / 6 451 / 6 227. Montant payé en
--            2024 : vieillesse 38 496 066 498 ; veuves, veufs, orphelins et
--            ascendants 7 222 987 423 ; invalidité 116 130 355. Toutes les
--            colonnes, avec les chiffres publiés, dans
--            resultats/A01_tableau18.tsv.
-- Constat  : La CNSS compte les pensions actives au 31 décembre dont l'ayant
--            droit est en vie, pas les pensions payées : la retraite vieillesse
--            tombe à 0,3 à 1,5 % près chaque année, les petites lignes à
--            quelques unités. Avec les orphelins de moins de 25 ans (tableau
--            20), le total fait 55 069 / 56 931 / 58 760 / 57 880 / 60 171 pour
--            53 908 / 55 647 / 57 790 / 59 104 / 62 231 publiés (+2,2 %, +2,3 %,
--            +1,7 %, −2,1 %, −3,3 %). Les écarts viennent des changements faits
--            dans la base après la publication : des pensions créées plus tard
--            avec un effet rétroactif (vieillesse : 85 de plus pour 2020, 450
--            pour 2024) ; et, depuis 2023, des pensions de veuve et d'orphelin
--            radiées avec une date de 2023 (nos actives perdent 1 088 veuves et
--            2 012 orphelins en 2023, quand la CNSS publie une hausse), à
--            vérifier (A06, A07). Les montants payés tombent à 0,7 à 2,5 % près
--            sur les tableaux 19 à 21 : la CNSS publie les montants payés.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 5
),
pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
paie AS (
    SELECT d.PE_ID,
           EXTRACT(YEAR FROM d.DEB_DATE_EFFET)                    AS annee,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P') THEN 1 ELSE 0 END) AS paye,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                     AND EXTRACT(MONTH FROM d.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS paye_t4,
           MAX(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                     AND EXTRACT(MONTH FROM d.DEB_DATE_EFFET) = 12
                    THEN 1 ELSE 0 END)                            AS paye_dec,
           SUM(CASE WHEN d.DEB_STATUT IN ('D', 'P')
                    THEN d.DEB_MONTANT ELSE 0 END)                AS montant
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P', 'V')
      AND d.DEB_DATE_EFFET >= DATE '2020-01-01'
      AND d.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID, EXTRACT(YEAR FROM d.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.TPE_CODE,
           pe.IND_ID_PREST,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                THEN 1 ELSE 0 END                                 AS actif,
           CASE WHEN pe.date_deces IS NULL
                  OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                THEN 1 ELSE 0 END                                 AS vivant,
           a.annee - pe.annee_naissance                           AS age,
           pa.PE_ID                                               AS pe_paye,
           NVL(pa.paye, 0)                                        AS paye,
           NVL(pa.paye_t4, 0)                                     AS paye_t4,
           NVL(pa.paye_dec, 0)                                    AS paye_dec,
           pa.montant
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       CASE TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       SUM(actif)                                                 AS actives_31_12,
       SUM(actif * vivant)                                        AS actives_en_vie,
       SUM(CASE WHEN actif = 1 AND vivant = 1 AND age < 25
                THEN 1 ELSE 0 END)                                AS actives_en_vie_moins_25,
       SUM(paye)                                                  AS payees_annee,
       SUM(CASE WHEN pe_paye IS NOT NULL THEN 1 ELSE 0 END)       AS payees_ou_validees,
       SUM(paye_t4)                                               AS payees_t4,
       SUM(paye_dec)                                              AS payees_decembre,
       SUM(CASE WHEN actif = 1 AND paye = 1 THEN 1 ELSE 0 END)    AS actives_et_payees_annee,
       COUNT(DISTINCT CASE WHEN paye = 1 THEN IND_ID_PREST END)   AS personnes_payees_annee,
       ROUND(SUM(montant))                                        AS montant_paye_annee
FROM base
GROUP BY annee,
         CASE TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END
ORDER BY annee, type_pension;


-- -----------------------------------------------------------------------------
-- A02 — Tableau 22 : pensionnés au 31/12/2024 par direction régionale
-- Objectif : pour 2024, par type de pension et par direction régionale du
--            dossier : pensions actives et en vie au 31/12/2024, payées dans
--            l'année, payées au 4e trimestre.
-- Lecture  : la répartition publiée (DRO 36 797, DRB 18 271, DRN 3 305, DRF
--            2 230, DRD 1 628) dit si la direction du dossier est celle que
--            la CNSS utilise, et confirme la définition trouvée par A01.
-- Tableaux : T32 à T36 ; tableau 22 de l'annuaire CNSS.
-- Résultat : Pensions de 2024 par direction (actives et en vie / payées en
--            2024 / payées au 4e trimestre). Retraite vieillesse : DRO 20 352 /
--            18 417 / 18 094 ; DRB 7 638 / 7 368 / 7 192 ; DRN 1 366 / 1 264 /
--            1 225 ; DRF 1 007 / 908 / 885 ; DRD 818 / 780 / 762. Veuves
--            actives : DRO 10 480, DRB 6 974, DRN 1 457, DRF 909, DRD 552.
--            Orphelins actifs de tous âges / payés : DRO 7 189 / 3 673, DRB
--            5 334 / 1 874, DRN 675 / 304, DRF 375 / 249, DRD 196 / 127.
--            Actives : ascendants 48, 8, 1, 1, 1 ; invalidité 81, 31, 5, 7, 1 ;
--            anticipée 57, 27, 2, 1, 0 ; veufs 264, 57, 16, 8, 11. Tout, avec
--            les chiffres publiés, dans resultats/A02_tableau22.tsv.
-- Constat  : La direction du dossier est celle de la CNSS. Avec les pensions
--            actives et en vie, les ascendants tombent juste dans les cinq
--            directions, la retraite vieillesse à 0 à 4 % près (DRO 20 352 pour
--            20 067, DRB 7 638 pour 7 538, DRD 818 pour 817). Il nous manque
--            des veuves à la DRO (−403) et à la DRB (−617) : c'est là qu'ont
--            été faites les radiations que la CNSS ne voyait pas encore. Les
--            orphelins de la DRB ont le plus de pensions actives jamais payées
--            (5 334 actives, 1 874 payées).
-- -----------------------------------------------------------------------------
WITH pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           NVL(dr.DR_SIGLE, '(sans direction)')                   AS direction,
           CASE WHEN p.PE_DATE_EFFET <= DATE '2024-12-31'
                 AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
                 AND (   p.PE_DATE_SUSPENSION IS NULL
                      OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
                      OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
                          AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
                 AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
                THEN 1 ELSE 0 END                                 AS actif_en_vie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN      USER_DWH.DOSSIER             d  ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.DIRECTION_REGIONALE dr ON dr.DR_NO   = d.DR_NO
    LEFT JOIN USER_DWH.INDIVIDU            i  ON i.IND_ID   = p.IND_ID_PREST
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
paie AS (
    SELECT d.PE_ID,
           MAX(CASE WHEN EXTRACT(MONTH FROM d.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS paye_t4
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P')
      AND d.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND d.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID
)
SELECT CASE pe.TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       pe.direction,
       SUM(pe.actif_en_vie)                                       AS actives_en_vie,
       SUM(CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END)      AS payees_2024,
       SUM(NVL(pa.paye_t4, 0))                                    AS payees_t4_2024
FROM pe
LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
GROUP BY CASE pe.TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END,
         pe.direction
ORDER BY type_pension, pe.direction;


-- -----------------------------------------------------------------------------
-- A03 — Tableau 23 : nouveaux pensionnés de 2024
-- Objectif : par type de pension, compter les pensions de 2024 selon la date
--            qui marque l'entrée : notification, date d'effet, saisie de la
--            pension, premier paiement (D ou P) jamais fait.
-- Lecture  : la date qui donne 4 037 (vieillesse 2 486, veuves 732, veufs 34,
--            orphelins 769, invalidité 6, anticipée 3, ascendants 7) est celle
--            de la CNSS pour un nouveau pensionné (règle R7, T42 à T49 ; P27
--            proposait la notification).
-- Tableaux : T42 à T47 ; tableau 23 de l'annuaire CNSS.
-- Résultat : Pensions de 2024 (notifiées / date d'effet / saisies / premier
--            paiement) : vieillesse 2 408 / 2 717 / 2 520 / 2 275 ; veuves
--            696 / 736 / 740 / 672 ; veufs 36 / 37 / 35 / 32 ; orphelins 734 /
--            727 / 779 / 621 ; invalidité 5 / 7 / 5 / 7 ; anticipée 3 / 10 /
--            4 / 8 ; ascendants 6 / 1 / 7 / 1. Total 3 888 / 4 235 / 4 090 /
--            3 616.
-- Constat  : La saisie de la pension est la date la plus proche du tableau 23 :
--            4 090 pour 4 037 publiés (+1,3 %), à quelques unités près sur
--            chaque ligne (vieillesse 2 520 pour 2 486, veuves 740 pour 732,
--            orphelins 779 pour 769, ascendants 7 pour 7). La notification,
--            retenue par la règle R7, donne 3 888 (−3,7 %). La CNSS compte un
--            nouveau pensionné l'année où sa pension est créée : la règle R7
--            est à revoir avec la direction des prestations.
-- -----------------------------------------------------------------------------
WITH pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           p.PE_DATE_NOTIFICATION,
           p.PE_DATE_EFFET,
           p.PE_DATE_INSERT
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
premier AS (
    SELECT d.PE_ID,
           MIN(d.DEB_DATE_EFFET)                                  AS premier_paiement
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P')
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID
)
SELECT CASE pe.TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       SUM(CASE WHEN pe.PE_DATE_NOTIFICATION >= DATE '2024-01-01'
                 AND pe.PE_DATE_NOTIFICATION <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS notifiees_2024,
       SUM(CASE WHEN pe.PE_DATE_EFFET >= DATE '2024-01-01'
                 AND pe.PE_DATE_EFFET <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS effet_2024,
       SUM(CASE WHEN pe.PE_DATE_INSERT >= DATE '2024-01-01'
                 AND pe.PE_DATE_INSERT <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS saisies_2024,
       SUM(CASE WHEN pr.premier_paiement >= DATE '2024-01-01'
                 AND pr.premier_paiement <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS premier_paiement_2024
FROM pe
LEFT JOIN premier pr ON pr.PE_ID = pe.PE_ID
GROUP BY CASE pe.TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END
ORDER BY type_pension;


-- -----------------------------------------------------------------------------
-- A04 — Tableaux 19 à 21 : pensionnés de 2024 par sexe, avec les montants
-- Objectif : pour 2024, par type de pension et par sexe de l'ayant droit :
--            effectif et montant payé dans l'année pour les pensions actives
--            et en vie au 31/12, pour celles payées dans l'année, pour celles
--            payées au 4e trimestre.
-- Lecture  : comparer aux tableaux 19 (vieillesse : 24 627 hommes, 6 103
--            femmes, 38,24 Md), 20 (veuves, veufs, orphelins, ascendants :
--            5 792 hommes, 25 512 femmes, 7,05 Md) et 21 (invalidité : 86
--            hommes, 33 femmes, 113,6 M). Si les effectifs tombent juste mais
--            pas les montants, la CNSS publie peut-être un montant annuel dû
--            (mensualité × 12), pas un montant payé.
-- Tableaux : T32 à T36 ; tableaux 19 à 21 de l'annuaire CNSS.
-- Résultat : Échec le 30/10/2026 : ORA-01722 (nombre non valide). Le sexe est
--            un nombre (1, 2) : NVL(IND_SEXE, '?') ne passe pas. Corrigée avec
--            TO_CHAR, à relancer.
-- Constat  : (à remplir)
-- -----------------------------------------------------------------------------
WITH pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           CASE WHEN p.PE_DATE_EFFET <= DATE '2024-12-31'
                 AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
                 AND (   p.PE_DATE_SUSPENSION IS NULL
                      OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
                      OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
                          AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
                 AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
                THEN 1 ELSE 0 END                                 AS actif_en_vie
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
paie AS (
    SELECT d.PE_ID,
           SUM(d.DEB_MONTANT)                                     AS montant,
           MAX(CASE WHEN EXTRACT(MONTH FROM d.DEB_DATE_EFFET) >= 10
                    THEN 1 ELSE 0 END)                            AS paye_t4
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P')
      AND d.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND d.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID
)
SELECT CASE pe.TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       pe.sexe,
       SUM(pe.actif_en_vie)                                       AS actives_en_vie,
       ROUND(SUM(CASE WHEN pe.actif_en_vie = 1
                      THEN pa.montant END))                       AS montant_actives_en_vie,
       SUM(CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END)      AS payees_2024,
       ROUND(SUM(pa.montant))                                     AS montant_payees_2024,
       SUM(NVL(pa.paye_t4, 0))                                    AS payees_t4,
       ROUND(SUM(CASE WHEN pa.paye_t4 = 1 THEN pa.montant END))   AS montant_payees_t4
FROM pe
LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
GROUP BY CASE pe.TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END,
         pe.sexe
ORDER BY type_pension, pe.sexe;


-- -----------------------------------------------------------------------------
-- A05 — Montant payé en 2024 par la branche des pensions
-- Objectif : paiements de 2024 (débours PM au statut D ou P, mois payé en
--            2024) des dossiers de pension (type V), par code de prestation,
--            avec le total.
-- Lecture  : l'annuaire donne 46 629 625 556 FCFA de prestations de la
--            branche des pensions en 2024. Si notre total en est proche, les
--            paiements de la base sont ceux que publie la CNSS (règle R3) ;
--            l'écart par code dit ce que la CNSS compte en plus ou en moins
--            (allocations versées en une fois, accessoires, ajustements).
-- Tableaux : T50, T51, T56 ; chiffres clés de l'annuaire CNSS.
-- Résultat : Paiements de 2024 des dossiers de pension : 895 750 débours,
--            47 237 434 040 FCFA. Par code : RPV 38 496 066 498 ; RPSV
--            5 725 704 405 ; RPSE 1 243 900 344 ; RBEC 554 254 000 ; RAV
--            517 334 647 ; RPSF 240 805 769 ; RASO 134 732 387 ; RPRA
--            129 117 956 ; RPI 116 130 355 ; RASC 46 355 951 ; RPA 12 576 905 ;
--            RCCM 10 370 122 ; RASA 5 994 909 ; RME 3 942 912 ; RME-COL
--            146 880.
-- Constat  : 47,24 Md pour 46,63 Md publiés : 1,3 % d'écart, sans doute des
--            rappels de 2024 saisis après la publication (9 % des débours sont
--            saisis 2 mois ou plus après leur mois, P20). Les paiements de la
--            base sont ceux que publie la CNSS : la règle R3 (débours PM au
--            statut D ou P, montant du débours) est confirmée par une
--            publication de la CNSS.
-- -----------------------------------------------------------------------------
SELECT NVL(d.TPE_CODE, '(sans code)')                             AS code_prestation,
       COUNT(*)                                                   AS debours_2024,
       ROUND(SUM(d.DEB_MONTANT))                                  AS montant_2024
FROM USER_DWH.DEBOURS d
WHERE d.DEB_TYPE = 'PM'
  AND d.DEB_STATUT IN ('D', 'P')
  AND d.DEB_DATE_EFFET >= DATE '2024-01-01'
  AND d.DEB_DATE_EFFET <  DATE '2025-01-01'
  AND d.DOS_CODE IN (SELECT DOS_CODE FROM USER_DWH.DOSSIER WHERE TDOS_CODE = 'V')
GROUP BY ROLLUP(NVL(d.TPE_CODE, '(sans code)'))
ORDER BY montant_2024 DESC;


-- =============================================================================
-- PARTIE B — EXPLIQUER LES ÉCARTS (rédigée le 30/10/2026 après les résultats
-- de A01 à A05, à exécuter)
-- La CNSS compte les pensions actives au 31/12 dont l'ayant droit est en vie
-- (A01). Les écarts qui restent viendraient de ce que la base a changé depuis
-- la publication : pensions créées plus tard avec un effet rétroactif,
-- radiations saisies plus tard avec une date passée. Trois requêtes pour le
-- vérifier :
--   A06 la base vue à la date de publication de chaque annuaire
--   A07 les radiations, par année de radiation et année de dernière mise à jour
--   A08 les orphelins de 2024 par âge, face au tableau 20
-- =============================================================================


-- -----------------------------------------------------------------------------
-- A06 — Le tableau 18 tel que la base le voyait à la publication
-- Objectif : pour chaque année Y de 2020 à 2024 et chaque type, partir des
--            pensions actives et en vie au 31/12/Y (A01), puis :
--            - retirer celles saisies après la publication (PE_DATE_INSERT
--              après le 30/06/Y+1 : l'annuaire 2024 date de juin 2025) ;
--            - remettre celles radiées avec une date de Y ou avant, mais
--              modifiées pour la dernière fois après la publication
--              (PE_DATE_UPDATE) : la radiation a sans doute été saisie après ;
--            et donner le résultat, « vu à la publication », avec la variante
--            au 31/03/Y+1. Pour les orphelins, la même chose en moins de 25
--            ans.
-- Lecture  : si « vu à la publication » retombe sur le tableau 18, les écarts
--            de A01 sont des corrections faites dans la base après coup, et
--            notre définition est bien celle de la CNSS. PE_DATE_UPDATE n'est
--            que la dernière modification : une pension modifiée pour une
--            autre raison après la radiation passe aussi pour « radiée
--            après », d'où une variante à deux dates.
-- Tableaux : T32 à T36 ; tableau 18 de l'annuaire CNSS.
-- Résultat : (à remplir)
-- Constat  : (à remplir)
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 5
),
pe AS (
    SELECT p.TPE_CODE,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           TRUNC(p.PE_DATE_UPDATE)                                AS date_maj,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
base AS (
    SELECT a.annee,
           pe.TPE_CODE,
           pe.date_saisie,
           pe.date_maj,
           pe.date_radiation,
           TO_DATE(a.annee || '1231', 'YYYYMMDD')                 AS fin,
           TO_DATE((a.annee + 1) || '0630', 'YYYYMMDD')           AS pub_juin,
           TO_DATE((a.annee + 1) || '0331', 'YYYYMMDD')           AS pub_mars,
           CASE WHEN a.annee - pe.annee_naissance < 25 THEN 1 ELSE 0 END AS moins_25,
           -- en cours au 31/12, sans tenir compte de la radiation
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                THEN 1 ELSE 0 END                                 AS en_cours
    FROM annees a
    CROSS JOIN pe
),
flags AS (
    SELECT annee, TPE_CODE, moins_25,
           CASE WHEN en_cours = 1
                 AND (date_radiation IS NULL OR date_radiation > fin)
                THEN 1 ELSE 0 END                                 AS active_en_vie,
           CASE WHEN en_cours = 1
                 AND (date_radiation IS NULL OR date_radiation > fin)
                 AND date_saisie > pub_juin
                THEN 1 ELSE 0 END                                 AS creee_apres,
           CASE WHEN en_cours = 1
                 AND date_radiation <= fin
                 AND date_saisie <= pub_juin
                 AND date_maj > pub_juin
                THEN 1 ELSE 0 END                                 AS radiee_apres,
           CASE WHEN en_cours = 1
                 AND date_saisie <= pub_juin
                 AND (date_radiation IS NULL OR date_radiation > fin
                      OR date_maj > pub_juin)
                THEN 1 ELSE 0 END                                 AS vue_juin,
           CASE WHEN en_cours = 1
                 AND date_saisie <= pub_mars
                 AND (date_radiation IS NULL OR date_radiation > fin
                      OR date_maj > pub_mars)
                THEN 1 ELSE 0 END                                 AS vue_mars
    FROM base
)
SELECT annee,
       CASE TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       SUM(active_en_vie)                                         AS actives_en_vie,
       SUM(creee_apres)                                           AS saisies_apres_publication,
       SUM(radiee_apres)                                          AS radiees_apres_publication,
       SUM(vue_juin)                                              AS vue_publication_juin,
       SUM(vue_mars)                                              AS vue_publication_mars,
       SUM(vue_juin * moins_25)                                   AS vue_juin_moins_25,
       SUM(vue_mars * moins_25)                                   AS vue_mars_moins_25
FROM flags
GROUP BY annee,
         CASE TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END
ORDER BY annee, type_pension;


-- -----------------------------------------------------------------------------
-- A07 — Radiations : quand ont-elles été saisies ?
-- Objectif : pour les pensions de vieillesse, de veuve et d'orphelin radiées
--            avec une date de 2020 à 2025, compter les radiations par année de
--            radiation et par année de dernière modification de la pension.
-- Lecture  : beaucoup de radiations datées de 2023 et modifiées en 2025 (ou
--            après juin 2025) veulent dire une campagne de radiation saisie
--            après coup, invisible pour l'annuaire 2024 : elle explique les
--            veuves et les orphelins qui nous manquent en 2023 et 2024 (A01,
--            A02). Elle serait à confirmer avec la direction des prestations
--            (contrôle d'existence, limite d'âge des orphelins).
-- Tableaux : T32 à T36 ; tableau 18 de l'annuaire CNSS.
-- Résultat : (à remplir)
-- Constat  : (à remplir)
-- -----------------------------------------------------------------------------
SELECT CASE TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           ELSE             '4 orphelins'
       END                                                        AS type_pension,
       EXTRACT(YEAR FROM PE_DATE_RADIATION)                       AS annee_radiation,
       NVL(TO_CHAR(EXTRACT(YEAR FROM PE_DATE_UPDATE)), '(vide)')  AS annee_maj,
       COUNT(*)                                                   AS radiations,
       SUM(CASE WHEN PE_DATE_UPDATE >= DATE '2025-07-01'
                THEN 1 ELSE 0 END)                                AS maj_apres_juin_2025,
       MIN(TRUNC(PE_DATE_UPDATE))                                 AS premiere_maj,
       MAX(TRUNC(PE_DATE_UPDATE))                                 AS derniere_maj
FROM USER_DWH.PRESTATION_ESP
WHERE TPE_CODE IN ('RPV', 'RPSV', 'RPSE')
  AND PE_DATE_RADIATION >= DATE '2020-01-01'
  AND PE_DATE_RADIATION <  DATE '2026-01-01'
GROUP BY CASE TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             ELSE             '4 orphelins'
         END,
         EXTRACT(YEAR FROM PE_DATE_RADIATION),
         NVL(TO_CHAR(EXTRACT(YEAR FROM PE_DATE_UPDATE)), '(vide)')
ORDER BY type_pension, annee_radiation, annee_maj;


-- -----------------------------------------------------------------------------
-- A08 — Orphelins de 2024 par âge, face au tableau 20
-- Objectif : pensions d'orphelin actives et en vie au 31/12/2024, et payées
--            en 2024, par tranche d'âge du tableau 20 (année 2024 moins année
--            de naissance), avec les âges impossibles à calculer.
-- Lecture  : l'annuaire compte 9 478 orphelins, tous de moins de 25 ans :
--            moins de 5 ans 93, 5-9 ans 636, 10-14 ans 1 551, 15-19 ans
--            3 386, 20-24 ans 3 812. Si nos tranches de moins de 25 ans
--            suivent la même forme, la CNSS applique une limite d'âge de 25
--            ans (à confirmer) ; nos pensions de 25 ans et plus sont des
--            orphelins devenus adultes, jamais radiés.
-- Tableaux : T35, T39, T44, T46 ; tableau 20 de l'annuaire CNSS.
-- Résultat : (à remplir)
-- Constat  : (à remplir)
-- -----------------------------------------------------------------------------
WITH paie AS (
    SELECT DISTINCT d.PE_ID
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P')
      AND d.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND d.DEB_DATE_EFFET <  DATE '2025-01-01'
      AND d.PE_ID IN (SELECT PE_ID FROM USER_DWH.PRESTATION_ESP WHERE TPE_CODE = 'RPSE')
),
orph AS (
    SELECT CASE WHEN i.IND_DATE_NAISSANCE IS NULL                   THEN '9 âge inconnu'
                WHEN 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 5  THEN '1 moins de 5 ans'
                WHEN 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 10 THEN '2 5-9 ans'
                WHEN 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 15 THEN '3 10-14 ans'
                WHEN 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 20 THEN '4 15-19 ans'
                WHEN 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25 THEN '5 20-24 ans'
                WHEN 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 30 THEN '6 25-29 ans'
                ELSE                                                      '7 30 ans et plus'
           END                                                    AS tranche,
           CASE WHEN p.PE_DATE_EFFET <= DATE '2024-12-31'
                 AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
                 AND (   p.PE_DATE_SUSPENSION IS NULL
                      OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
                      OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
                          AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
                 AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
                THEN 1 ELSE 0 END                                 AS active_en_vie,
           CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END       AS payee_2024,
           CASE WHEN p.PE_DATE_RADIATION >= DATE '2023-01-01'
                 AND p.PE_DATE_RADIATION <  DATE '2025-01-01'
                THEN 1 ELSE 0 END                                 AS radiee_2023_2024
    FROM USER_DWH.PRESTATION_ESP p
    LEFT JOIN USER_DWH.INDIVIDU i  ON i.IND_ID  = p.IND_ID_PREST
    LEFT JOIN paie              pa ON pa.PE_ID  = p.PE_ID
    WHERE p.TPE_CODE = 'RPSE'
)
SELECT tranche,
       SUM(active_en_vie)                                         AS actives_en_vie,
       SUM(payee_2024)                                            AS payees_2024,
       SUM(CASE WHEN active_en_vie = 1 AND payee_2024 = 1
                THEN 1 ELSE 0 END)                                AS actives_et_payees,
       SUM(radiee_2023_2024)                                      AS radiees_en_2023_2024
FROM orph
GROUP BY tranche
ORDER BY tranche;
