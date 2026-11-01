-- =============================================================================
-- T42 à T49 — Nouveaux pensionnés et crédirentiers de l'exercice, montants,
-- services passés (T42) et degré d'IPP (T48), par tranche d'âge et par sexe
-- PROPOSITION du 01/11/2026, à valider : une seule requête pour les huit
-- tableaux, sur le modèle de celle de T32 à T41 (même âge, même sexe, même
-- montant), avec les règles R2, R3, R4, R5, R7, R8 et R9 du README.
-- Sources : DWH.FAIT_PRESTATION_ESP, FAIT_INDIVIDU, FAIT_DEBOURS (copies des
--           tables de USER_DWH).
-- Grain   : TABLEAU × tranche d'âge (rang) ; colonnes des maquettes de
--           l'Annuaire (p. 42 à 45) : effectif et montant des hommes, des
--           femmes, et total ; T42 ajoute le total des services passés, T48 le
--           degré moyen d'IPP. Une ligne TOTAL par tableau.
-- :1      = CLICHE (MMYYYY) ; :2 = exercice (AAAA), par exemple 2025.
-- Date du calcul (R13, proposition) : l'exercice N se calcule sur le cliché
-- de fin mars N+1, la date à laquelle la CNSS arrête son annuaire (les décès
-- de 2024 connus fin mars 2025 retombent mois par mois sur le tableau 26,
-- lot 8 P99).
-- Date des montants (R4, en attente de la CNSS, question posée au point du
-- 02/11/2026) : un seul réglage, date_montant dans parametres. 'MOIS' (le mois
-- payé) aujourd'hui ; 'SAISIE' (la saisie du paiement, rappels compris) si la
-- CNSS compte à la date du versement (lot 8 P98 : meilleur sur 11 lignes / 16).
-- Règles :
--   · Périmètre (R2, proposition) : T42 RPV, RPRA · T43 RPSV, RPSF · T44 RPSE ·
--     T45 toutes les rentes AT/MP (ARI, ARSV, ARSF, ARSE, ARSA) · T46 ARSE ·
--     T47 ARSA · T48 ARI · T49 ARSV, ARSF. T45 n'a pas de type dans
--     l'Annuaire : on le lit comme le total de la branche. T47 est rangé avec
--     les AT/MP par l'atelier de Kaya 2017.
--   · Nouveau (R7) : la personne dont la première prestation du tableau est
--     saisie dans l'exercice (PE_DATE_INSERT). C'est la date de l'annuaire de
--     la CNSS : 4 090 pensions saisies en 2024 pour 4 037 nouveaux pensionnés
--     publiés (A03). Une révision ou une reliquidation ne fait pas un nouveau :
--     220 retraités en 2025 avaient déjà une pension (lot 7 P82). Un nouveau
--     compte même s'il est sorti avant le 31/12 (35 retraités en 2024) : c'est
--     une entrée de l'exercice. Les nouveaux en coordination payée par une
--     autre caisse (COOR_AC, 234 retraités en 2024) comptent, sans montant,
--     comme dans l'annuaire de la CNSS.
--   · Personne (R5) : l'ayant droit, une fois par tableau.
--   · Âge (R8) : année de l'exercice moins année de naissance ; tranches de 5
--     ans selon la grille de chaque tableau ; un âge négatif, absent, ou de
--     120 ans et plus va en « Âge inconnu ». Par tranche de 5 ans, l'âge des
--     nouveaux retraités retombe sur le tableau 25 de l'annuaire (P83).
--   · Sexe : 1 = hommes, 2 = femmes.
--   · Montant (R3, R4) : débours de paiement (PM) au statut D ou P, mois payé
--     dans l'exercice, versés pour les prestations du tableau saisies dans
--     l'exercice. Le rappel des mois d'avant l'exercice n'y est pas : 8 % pour
--     les retraités, plus que les mois de l'année pour les rentes de
--     survivants (P85) ; question au métier.
--   · Services passés (R9, T42) : mois cotisés de la dernière prestation saisie
--     dans l'exercice, de 1 à 600 (999 et au-delà écartés), additionnés par
--     tranche et par sexe ; en mois (Kaya 2017). Toujours remplis (P86).
--     Pour la coordination, la seule part CNSS (Kaya 2017) : le total moins
--     les mois dans l'autre caisse (PE_COT_AUTRE, rempli pour toutes les
--     pensions de coordination et toujours plus petit que le total, P89,
--     P90).
--   · Degré d'IPP (T48) : taux d'incapacité de la dernière rente viagère saisie
--     dans l'exercice, de 1 à 100, moyenne par tranche et par sexe. Toujours
--     rempli, de 15 à 100, 24 en médiane en 2024 (P87).
-- Contrôle par l'annuaire de la CNSS 2024 (tableaux 23, 25, 75 et 77 ; lot 7
-- P82) : T42 2 463 pour 2 489 (−1,0 %), T43 +0,7 %, T44 +0,5 %, T46 et T49
-- exacts, T47 15 pour 13, T45 102 pour 105 ; T48 39 pour 44 (P91 teste un
-- comptage par accident). Sexe des nouveaux retraités : 641 femmes pour 527
-- publiées, écart non expliqué (P83).
-- Choix encore à valider : le périmètre de T45 et de T47 ; la première entrée
-- plutôt que toute prestation saisie (T48 : 39 pour 44, un comptage par
-- accident ne change rien, P91) ; les sortis avant le 31/12 ; le montant sans
-- le rappel ; la part CNSS des services passés ; la retraite anticipée dans
-- T42 (comme T32).
-- =============================================================================

WITH

parametres AS (
    SELECT TO_NUMBER(:2)                    AS annee,
           TO_DATE(:2 || '0101', 'YYYYMMDD') AS debut,
           TO_DATE(:2 || '1231', 'YYYYMMDD') AS fin,
           -- date d'un paiement (règle R4, en attente de la CNSS) : 'MOIS' = le
           -- mois payé, 'SAISIE' = la saisie du paiement (rappels compris)
           'MOIS'                           AS date_montant
    FROM   DUAL
),

-- ── 1. Les tableaux : leurs codes de prestation et leur grille d'âge ─────────
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

-- Grille de l'Annuaire : « moins de <bas> ans », puis des tranches de 5 ans,
-- puis « <haut> ans et plus » (Annuaire p. 42 à 45).
grille AS (
    SELECT 'T42' AS tableau, 50 AS bas, 100 AS haut FROM DUAL
    UNION ALL SELECT 'T43', 20,  60 FROM DUAL
    UNION ALL SELECT 'T44',  5,  20 FROM DUAL
    UNION ALL SELECT 'T45', 15, 100 FROM DUAL
    UNION ALL SELECT 'T46',  5,  20 FROM DUAL
    UNION ALL SELECT 'T47', 50, 100 FROM DUAL
    UNION ALL SELECT 'T48', 20,  65 FROM DUAL
    UNION ALL SELECT 'T49', 20,  65 FROM DUAL
),

pe AS (
    SELECT pm.tableau,
           p.PE_ID,
           p.IND_ID_PREST,
           p.PE_DATE_INSERT,
           p.PE_MOIS_COTISATION,
           p.PE_COT_AUTRE,
           p.PE_TAUX_INCAPACITE
    FROM DWH.FAIT_PRESTATION_ESP p
    JOIN perimetre               pm ON pm.code = p.TPE_CODE
    WHERE p.CLICHE = :1
),

-- ── 2. Les nouveaux : première prestation du tableau saisie dans l'exercice ─
premiere AS (
    SELECT tableau,
           IND_ID_PREST,
           MIN(PE_DATE_INSERT)                                    AS premiere_saisie
    FROM pe
    GROUP BY tableau, IND_ID_PREST
),

-- leurs prestations saisies dans l'exercice, la dernière en tête (rang 1)
entree AS (
    SELECT pe.tableau,
           pe.PE_ID,
           pe.IND_ID_PREST,
           pe.PE_MOIS_COTISATION                                  AS mois,
           pe.PE_COT_AUTRE                                        AS mois_autre_caisse,
           pe.PE_TAUX_INCAPACITE                                  AS taux,
           ROW_NUMBER() OVER (PARTITION BY pe.tableau, pe.IND_ID_PREST
                              ORDER BY pe.PE_DATE_INSERT DESC, pe.PE_ID DESC) AS rang
    FROM pe
    JOIN premiere   pm ON pm.tableau      = pe.tableau
                      AND pm.IND_ID_PREST = pe.IND_ID_PREST
    CROSS JOIN parametres pr
    WHERE pm.premiere_saisie >= pr.debut
      AND pm.premiere_saisie <  pr.fin + 1
      AND pe.PE_DATE_INSERT  >= pr.debut
      AND pe.PE_DATE_INSERT  <  pr.fin + 1
),

-- ── 3. Le montant payé dans l'exercice (R3, R4) ────────────────────────────
paie AS (
    SELECT deb.PE_ID,
           SUM(deb.DEB_MONTANT)              AS montant
    FROM DWH.FAIT_DEBOURS deb
    CROSS JOIN parametres pr
    WHERE deb.CLICHE   = :1
      AND deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND CASE pr.date_montant WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
                               ELSE deb.DEB_DATE_EFFET END >= pr.debut
      AND CASE pr.date_montant WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
                               ELSE deb.DEB_DATE_EFFET END <  pr.fin + 1
    GROUP BY deb.PE_ID
),

-- ── 4. Les nouveaux : une personne par tableau (R5) ─────────────────────────
-- Le montant de toutes ses nouvelles prestations du tableau ; les mois cotisés
-- (T42) et le taux d'IPP (T48) de la dernière. Les mois sont la part CNSS :
-- le total moins les mois dans l'autre caisse (coordination).
beneficiaire AS (
    SELECT e.tableau,
           e.IND_ID_PREST                    AS personne,
           SUM(NVL(pa.montant, 0))           AS montant,
           MAX(CASE WHEN e.rang = 1 AND e.mois BETWEEN 1 AND 600
                    THEN GREATEST(e.mois - NVL(e.mois_autre_caisse, 0), 0)
               END)                          AS mois,
           MAX(CASE WHEN e.rang = 1 AND e.taux BETWEEN 1 AND 100
                    THEN e.taux END)         AS taux
    FROM      entree e
    LEFT JOIN paie   pa ON pa.PE_ID = e.PE_ID
    GROUP BY e.tableau, e.IND_ID_PREST
),

-- ── 5. L'âge, la tranche et le sexe (R8) ───────────────────────────────────
detail AS (
    SELECT b.tableau,
           b.montant,
           b.mois,
           b.taux,
           TO_CHAR(i.IND_SEXE)                                    AS sexe,
           pr.annee - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)     AS age,
           g.bas,
           g.haut
    FROM      beneficiaire      b
    JOIN      grille            g  ON g.tableau = b.tableau
    CROSS JOIN parametres       pr
    LEFT JOIN DWH.FAIT_INDIVIDU i  ON i.IND_ID  = b.personne
                                  AND i.CLICHE  = :1
),

tranche AS (
    SELECT tableau,
           montant,
           mois,
           taux,
           sexe,
           -- âge inconnu : pas de date, né après l'exercice, ou 120 ans et plus
           CASE WHEN age IS NULL OR age < 0 OR age >= 120 THEN 999
                WHEN age < bas              THEN -1
                WHEN age >= haut            THEN haut
                ELSE FLOOR(age / 5) * 5
           END                                                    AS rang,
           CASE WHEN age IS NULL OR age < 0 OR age >= 120 THEN 'Âge inconnu'
                WHEN age < bas              THEN 'Moins de ' || bas || ' ans'
                WHEN age >= haut            THEN haut || ' ans et plus'
                ELSE FLOOR(age / 5) * 5 || ' – ' || (FLOOR(age / 5) * 5 + 4) || ' ans'
           END                                                    AS libelle
    FROM detail
),

-- ── 6. Les colonnes des maquettes, et le TOTAL ─────────────────────────────
lignes AS (
    SELECT tableau, rang, libelle,
           SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END)            AS eff_h,
           SUM(CASE WHEN sexe = '1' THEN montant ELSE 0 END)      AS mnt_h,
           SUM(CASE WHEN sexe = '1' THEN mois END)                AS sp_h,
           AVG(CASE WHEN sexe = '1' THEN taux END)                AS ipp_h,
           SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END)            AS eff_f,
           SUM(CASE WHEN sexe = '2' THEN montant ELSE 0 END)      AS mnt_f,
           SUM(CASE WHEN sexe = '2' THEN mois END)                AS sp_f,
           AVG(CASE WHEN sexe = '2' THEN taux END)                AS ipp_f,
           COUNT(*)                                               AS eff_t,
           SUM(montant)                                           AS mnt_t
    FROM tranche
    GROUP BY tableau, rang, libelle
    UNION ALL
    SELECT tableau, 1000, 'TOTAL',
           SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END),
           SUM(CASE WHEN sexe = '1' THEN montant ELSE 0 END),
           SUM(CASE WHEN sexe = '1' THEN mois END),
           AVG(CASE WHEN sexe = '1' THEN taux END),
           SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END),
           SUM(CASE WHEN sexe = '2' THEN montant ELSE 0 END),
           SUM(CASE WHEN sexe = '2' THEN mois END),
           AVG(CASE WHEN sexe = '2' THEN taux END),
           COUNT(*),
           SUM(montant)
    FROM tranche
    GROUP BY tableau
)

SELECT pr.annee                                                   AS ANNEE,
       l.tableau                                                  AS TABLEAU,
       l.rang                                                     AS RANG_TRANCHE,
       l.libelle                                                  AS TRANCHE_AGE,
       l.eff_h                                                    AS EFFECTIF_HOMMES,
       ROUND(l.mnt_h)                                             AS MONTANT_HOMMES,
       -- T42 seulement : total des services passés, en mois
       CASE WHEN l.tableau = 'T42' THEN l.sp_h END                AS SERVICES_PASSES_HOMMES,
       -- T48 seulement : degré moyen d'IPP
       CASE WHEN l.tableau = 'T48' THEN ROUND(l.ipp_h, 1) END     AS IPP_MOYEN_HOMMES,
       l.eff_f                                                    AS EFFECTIF_FEMMES,
       ROUND(l.mnt_f)                                             AS MONTANT_FEMMES,
       CASE WHEN l.tableau = 'T42' THEN l.sp_f END                AS SERVICES_PASSES_FEMMES,
       CASE WHEN l.tableau = 'T48' THEN ROUND(l.ipp_f, 1) END     AS IPP_MOYEN_FEMMES,
       l.eff_t                                                    AS EFFECTIF_TOTAL,
       ROUND(l.mnt_t)                                             AS MONTANT_TOTAL,
       :1                                                         AS CLICHE
FROM lignes l
CROSS JOIN parametres pr
ORDER BY l.tableau, l.rang
