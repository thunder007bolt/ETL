-- =============================================================================
-- T32 à T41 — Bénéficiaires au 31/12 et montants, par tranche d'âge et par sexe
-- PROPOSITION du 31/10/2026, revue le 01/11/2026 après les lots 5 et 6, à
-- valider : une seule requête pour les dix tableaux, qui traduit les règles
-- de la fiche Excalidraw « T32 pensionnés de vieillesse » (fiche pilote de la
-- famille) et les règles R2, R3, R4, R5, R6, R8 du README.
-- Sources : DWH.FAIT_PRESTATION_ESP, FAIT_DOSSIER, FAIT_INDIVIDU, FAIT_DEBOURS
--           (copies des tables de USER_DWH).
-- Grain   : TABLEAU × tranche d'âge (rang) ; colonnes de la maquette de
--           l'Annuaire (p. 37 à 41) : effectif et montant des hommes, des
--           femmes, et total. Une ligne TOTAL par tableau.
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
--   · Périmètre (R2) : T32 RPV, RPRA · T33 RPI · T34 RPSV, RPSF · T35 RPSE ·
--     T36 RPA · T37 ARI · T38 ARSV, ARSF · T39 ARSE · T40 ARSA · T41 FAF.
--   · Bénéficiaire au 31/12 (R6) : prestation en cours au 31/12 (effet avant,
--     ni radiée ni suspendue sauf réactivation), ayant droit en vie, payée ou
--     non ; c'est la définition de l'annuaire de la CNSS. Orphelins (T35, T39)
--     de moins de 25 ans ; T41 : enfants de 21 ans au plus, et payés dans
--     l'exercice (sans cette condition, T41 dépasse l'annuaire de 5 % ; avec,
--     +0,7 % d'allocataires et +1,2 % d'enfants en 2024, lot 6 P79).
--   · Personne (R5) : l'ayant droit, une fois par tableau. T41 : un
--     allocataire par dossier qui a au moins un enfant compté (atelier de
--     Kaya 2017), d'âge et de sexe ceux du titulaire du dossier.
--   · Âge (R8) : année de l'exercice moins année de naissance (Annuaire,
--     glossaire) ; tranches de 5 ans selon la grille de chaque tableau. Un
--     âge négatif, absent, ou de 120 ans et plus (l'année 1900 est une date
--     de naissance par défaut : 503 bénéficiaires en 2024, lot 6 P80) va en
--     « Âge inconnu ».
--   · Sexe : 1 = hommes, 2 = femmes (confirmé, lot 5 P75).
--   · Montant (R3, R4) : débours de paiement (PM) au statut D ou P, mois payé
--     dans l'exercice, versés aux bénéficiaires comptés. La part CNSS
--     seulement pour la coordination (Kaya 2017) : ce sont déjà les seuls
--     paiements de la base. Les pensions de coordination payées par une
--     autre caisse (COOR_AC) comptent dans l'effectif, sans montant, comme
--     dans l'annuaire de la CNSS (lot 5 P78).
-- Contrôle par l'annuaire de la CNSS 2024 (lots 5 et 6) : T32 +0,9 %, T36
-- exact, T33 et T37 à T40 à quelques pourcents, T41 +0,7 % ; montant de T32
-- −1,5 %. Écart ouvert : T34 et T35 sous le publié depuis 2023 ; les
-- suspensions ne l'expliquent pas (lot 6 P81) ; A06 et A07 (comparaison avec
-- l'annuaire) testent les radiations saisies après coup.
-- Choix encore à valider (fiche T32) : la retraite anticipée dans T32 ; le
-- montant des bénéficiaires comptés plutôt que tout le payé de l'année (lot 5
-- P75 : l'annuaire tombe entre les deux) ; la pension moyenne avec ou sans
-- les pensions COOR_AC ; T41 limité aux enfants payés dans l'année (lot 6
-- P79).
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

-- Grille de l'Annuaire : « moins de <bas> ans », puis des tranches de 5 ans,
-- puis « <haut> ans et plus » (Annuaire p. 37 à 41 ; T35 sans « 25 ans et
-- plus », atelier de Kaya 2017).
grille AS (
    SELECT 'T32' AS tableau, 55 AS bas, 100 AS haut FROM DUAL
    UNION ALL SELECT 'T33', 25,  60 FROM DUAL
    UNION ALL SELECT 'T34', 15, 100 FROM DUAL
    UNION ALL SELECT 'T35',  5,  25 FROM DUAL
    UNION ALL SELECT 'T36', 50, 100 FROM DUAL
    UNION ALL SELECT 'T37', 20, 100 FROM DUAL
    UNION ALL SELECT 'T38', 20, 100 FROM DUAL
    UNION ALL SELECT 'T39',  5,  25 FROM DUAL
    UNION ALL SELECT 'T40', 50,  80 FROM DUAL
    UNION ALL SELECT 'T41', 20,  80 FROM DUAL
),

-- ── 2. Les prestations en cours au 31/12, ayant droit en vie (R6) ──────────
en_cours AS (
    SELECT pm.tableau,
           pe.PE_ID,
           pe.DOS_CODE,
           pe.IND_ID_PREST
    FROM DWH.FAIT_PRESTATION_ESP pe
    JOIN perimetre               pm ON pm.code   = pe.TPE_CODE
    CROSS JOIN parametres        pr
    LEFT JOIN DWH.FAIT_INDIVIDU  i  ON i.IND_ID  = pe.IND_ID_PREST
                                   AND i.CLICHE  = :1
    WHERE pe.CLICHE = :1
      AND pe.PE_DATE_EFFET <= pr.fin
      AND (pe.PE_DATE_RADIATION IS NULL OR pe.PE_DATE_RADIATION > pr.fin)
      AND (   pe.PE_DATE_SUSPENSION IS NULL
           OR pe.PE_DATE_SUSPENSION > pr.fin
           OR (    pe.PE_DATE_REACTIVATION >= pe.PE_DATE_SUSPENSION
               AND pe.PE_DATE_REACTIVATION <= pr.fin))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > pr.fin)
      -- orphelins de moins de 25 ans ; enfants d'allocataires de 21 ans au plus
      AND (pm.tableau NOT IN ('T35', 'T39')
           OR pr.annee - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
      AND (pm.tableau <> 'T41'
           OR pr.annee - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) <= 21)
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

-- ── 4. Les bénéficiaires : une personne par tableau (R5) ───────────────────
-- T32 à T40 : l'ayant droit, avec le montant de toutes ses prestations du
-- tableau. T41 : un allocataire par dossier, la personne étant le titulaire du
-- dossier, avec le montant des allocations de ses enfants comptés ; un enfant
-- ne compte que s'il a été payé dans l'exercice (jointure sur paie, pas de
-- LEFT JOIN).
beneficiaire AS (
    SELECT ec.tableau,
           ec.IND_ID_PREST                   AS personne,
           SUM(NVL(pa.montant, 0))           AS montant
    FROM      en_cours ec
    LEFT JOIN paie     pa ON pa.PE_ID = ec.PE_ID
    WHERE ec.tableau <> 'T41'
    GROUP BY ec.tableau, ec.IND_ID_PREST
    UNION ALL
    SELECT 'T41',
           d.IND_ID,
           SUM(pa.montant)
    FROM      en_cours         ec
    JOIN      DWH.FAIT_DOSSIER d  ON d.DOS_CODE = ec.DOS_CODE
                                 AND d.CLICHE   = :1
    JOIN      paie             pa ON pa.PE_ID   = ec.PE_ID
    WHERE ec.tableau = 'T41'
    GROUP BY ec.DOS_CODE, d.IND_ID
),

-- ── 5. L'âge, la tranche et le sexe (R8) ───────────────────────────────────
detail AS (
    SELECT b.tableau,
           b.montant,
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

-- ── 6. Les colonnes de la maquette, et le TOTAL ────────────────────────────
lignes AS (
    SELECT tableau, rang, libelle,
           SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END)            AS eff_h,
           SUM(CASE WHEN sexe = '1' THEN montant ELSE 0 END)      AS mnt_h,
           SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END)            AS eff_f,
           SUM(CASE WHEN sexe = '2' THEN montant ELSE 0 END)      AS mnt_f,
           COUNT(*)                                               AS eff_t,
           SUM(montant)                                           AS mnt_t
    FROM tranche
    GROUP BY tableau, rang, libelle
    UNION ALL
    SELECT tableau, 1000, 'TOTAL',
           SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END),
           SUM(CASE WHEN sexe = '1' THEN montant ELSE 0 END),
           SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END),
           SUM(CASE WHEN sexe = '2' THEN montant ELSE 0 END),
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
       l.eff_f                                                    AS EFFECTIF_FEMMES,
       ROUND(l.mnt_f)                                             AS MONTANT_FEMMES,
       l.eff_t                                                    AS EFFECTIF_TOTAL,
       ROUND(l.mnt_t)                                             AS MONTANT_TOTAL,
       -- pension moyenne de l'année (Annuaire p. 13) : montant ÷ effectif
       ROUND(l.mnt_t / NULLIF(l.eff_t, 0))                        AS MONTANT_MOYEN,
       :1                                                         AS CLICHE
FROM lignes l
CROSS JOIN parametres pr
ORDER BY l.tableau, l.rang
