-- =============================================================================
-- T50 à T57 — Évolution sur 5 ans et récapitulatifs par branche
-- PROPOSITION du 01/11/2026, à valider : trois requêtes, une par forme de
-- tableau, avec les règles R1, R3, R4, R5 et R9 du README.
--   · Requête 1 : T50 à T55, une prestation suivie sur 5 ans, effectif et
--     montant par sexe (T50 ajoute le nombre moyen de services passés).
--   · Requête 2 : T56, les prestations servies par branche sur 5 ans.
--   · Requête 3 : T57, les assurés décédés par branche sur 5 ans.
-- Sources : DWH.FAIT_PRESTATION_ESP, FAIT_DOSSIER, FAIT_INDIVIDU, FAIT_DEBOURS
--           (copies des tables de USER_DWH).
-- :1      = CLICHE (MMYYYY) ; :2 = exercice N (AAAA) ; les années vont de N−4
--           à N.
-- L'entrepôt ne garde que le dernier cliché : les années N−4 à N−1 sont
-- recalculées sur la base d'aujourd'hui, et peuvent différer un peu des
-- chiffres publiés à l'époque (corrections et rappels saisis depuis).
-- Date du calcul (R13, proposition) : l'exercice N se calcule sur le cliché
-- de fin mars N+1, la date à laquelle la CNSS arrête son annuaire (les décès
-- de 2024 connus fin mars 2025 retombent mois par mois sur le tableau 26,
-- lot 8 P99).
-- Date des montants (R4, en attente de la CNSS, question posée au point du
-- 02/11/2026) : un seul réglage, date_montant dans parametres. 'MOIS' (le mois
-- payé) aujourd'hui ; 'SAISIE' (la saisie du paiement, rappels compris) si la
-- CNSS compte à la date du versement (lot 8 P98 : meilleur sur 11 lignes / 16).
-- Règles communes :
--   · Branche (R1) : V = AVS (pensions et allocations) ; A = AT/MP ; F et M =
--     PF (maternité comprise).
--   · Paiement (R3, R4) : débours au statut D ou P, au mois payé
--     (DEB_DATE_EFFET).
-- Contrôle par l'annuaire de la CNSS 2024 (lot 8) : allocations de vieillesse
-- liquidées 831 pour 833, IJ de maternité 3 890 pour 3 893 ; AVS +1,3 %, PF
-- −3,8 %, AT/MP −29 % (prestations en nature introuvables) ; décès de
-- pensionnés +15 % (retard de déclaration : confirmé par P99).
-- Choix encore à valider : la date des montants (le mois payé, ou la saisie du
-- paiement, meilleure sur 11 lignes sur 16, P98 : à décider) ;
-- T56 sans les ajustements ; les personnes de T57 (P99 à P101).
-- =============================================================================


-- =============================================================================
-- Requête 1 — T50 à T55 : bénéficiaires et montants de chaque année
-- Règles :
--   · Périmètre (R2) : T50 RAV · T51 RAS, RASC, RASO, RASA · T52 AIJ · T53 AAI
--     · T54 MIJ · T55 FAP, MAP.
--   · Bénéficiaire de l'année : la personne dont une prestation du tableau est
--     liquidée dans l'année, c'est-à-dire notifiée pour la première fois
--     (règle D4 de T25), comme Kaya 2017 le demande pour T54 et T55. Confirmé
--     par l'annuaire de la CNSS : T50 831 pour 833, T54 3 890 pour 3 893 en
--     2024 ; les payées s'en écartent de 25 % (lot 8 P92). T51 : les parts de
--     l'allocation de survivants (RASC, RASO, RASA) ; RAS ne se liquide plus
--     (P93). T52 : une IJ AT/MP se liquide en plusieurs fois, on compte la
--     victime.
--   · Personne (R5) : l'ayant droit, une fois par tableau et par année ; son
--     sexe (1 = hommes). T54 et T55 : les femmes seulement (maquette).
--   · Montant : tout ce qui a été payé pour les mois de l'année (R3, R4). Les
--     paiements saisis dans l'année retombent mieux sur l'annuaire (T50 :
--     0,02 % contre 3,9 %, P92) : la date sera revue avec P98.
--   · Services passés (T50) : moyenne des mois cotisés de la dernière
--     allocation liquidée de la personne, de 1 à 600, part CNSS en
--     coordination (le total moins PE_COT_AUTRE), comme T42.
--   · Une ligne TOTAL par tableau (Kaya 2017 pour T54 et T55) : la somme des
--     5 années.
-- =============================================================================

WITH

parametres AS (
    SELECT TO_NUMBER(:2) AS annee_n,
           -- date d'un paiement (règle R4, en attente de la CNSS) : 'MOIS' = le
           -- mois payé, 'SAISIE' = la saisie du paiement (rappels compris)
           'MOIS'        AS date_montant
    FROM DUAL
),

annees AS (
    SELECT pr.annee_n - 5 + LEVEL AS annee
    FROM parametres pr
    CONNECT BY LEVEL <= 5
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
    SELECT pm.tableau,
           p.PE_ID,
           p.IND_ID_PREST,
           TO_CHAR(i.IND_SEXE)                                    AS sexe,
           EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION)              AS annee_liquidation,
           p.PE_DATE_NOTIFICATION,
           p.PE_MOIS_COTISATION                                   AS mois,
           p.PE_COT_AUTRE                                         AS mois_autre_caisse
    FROM DWH.FAIT_PRESTATION_ESP p
    JOIN perimetre               pm ON pm.code  = p.TPE_CODE
    LEFT JOIN DWH.FAIT_INDIVIDU  i  ON i.IND_ID = p.IND_ID_PREST
                                   AND i.CLICHE = :1
    WHERE p.CLICHE = :1
),

-- ── Les bénéficiaires : liquidés dans l'année, une fois par tableau ─────────
liquides AS (
    SELECT tableau,
           annee_liquidation                                      AS annee,
           IND_ID_PREST,
           sexe,
           mois,
           mois_autre_caisse,
           ROW_NUMBER() OVER (PARTITION BY tableau, annee_liquidation, IND_ID_PREST
                              ORDER BY PE_DATE_NOTIFICATION DESC, PE_ID DESC) AS rang
    FROM pe
    WHERE annee_liquidation IN (SELECT annee FROM annees)
),

effectifs AS (
    SELECT tableau,
           annee,
           SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END)            AS eff_h,
           SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END)            AS eff_f,
           COUNT(*)                                               AS eff_t,
           -- T50 : part CNSS des mois cotisés (le total moins l'autre caisse)
           AVG(CASE WHEN sexe = '1' AND mois BETWEEN 1 AND 600
                    THEN GREATEST(mois - NVL(mois_autre_caisse, 0), 0) END) AS sp_h,
           AVG(CASE WHEN sexe = '2' AND mois BETWEEN 1 AND 600
                    THEN GREATEST(mois - NVL(mois_autre_caisse, 0), 0) END) AS sp_f
    FROM liquides
    WHERE rang = 1
    GROUP BY tableau, annee
),

-- ── Les montants : payés pour les mois de l'année (R3, R4) ─────────────────
montants AS (
    SELECT tableau,
           annee,
           SUM(CASE WHEN sexe = '1' THEN montant ELSE 0 END)      AS mnt_h,
           SUM(CASE WHEN sexe = '2' THEN montant ELSE 0 END)      AS mnt_f,
           SUM(montant)                                           AS mnt_t
    FROM (
        SELECT pe.tableau,
               pe.sexe,
               deb.DEB_MONTANT                                    AS montant,
               EXTRACT(YEAR FROM CASE pr.date_montant
                                     WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
                                     ELSE deb.DEB_DATE_EFFET END) AS annee
        FROM DWH.FAIT_DEBOURS deb
        JOIN pe ON pe.PE_ID = deb.PE_ID
        CROSS JOIN parametres pr
        WHERE deb.CLICHE   = :1
          AND deb.DEB_TYPE = 'PM'
          AND deb.DEB_STATUT IN ('D', 'P')
    )
    WHERE annee IN (SELECT annee FROM annees)
    GROUP BY tableau, annee
),

lignes AS (
    SELECT t.tableau,
           a.annee,
           NVL(e.eff_h, 0) AS eff_h, NVL(m.mnt_h, 0) AS mnt_h, e.sp_h,
           NVL(e.eff_f, 0) AS eff_f, NVL(m.mnt_f, 0) AS mnt_f, e.sp_f,
           NVL(e.eff_t, 0) AS eff_t, NVL(m.mnt_t, 0) AS mnt_t
    FROM (SELECT DISTINCT tableau FROM perimetre) t
    CROSS JOIN annees a
    LEFT JOIN effectifs e ON e.tableau = t.tableau AND e.annee = a.annee
    LEFT JOIN montants  m ON m.tableau = t.tableau AND m.annee = a.annee
)

SELECT tableau                                                    AS TABLEAU,
       TO_CHAR(annee)                                             AS ANNEE,
       eff_h                                                      AS EFFECTIF_HOMMES,
       ROUND(mnt_h)                                               AS MONTANT_HOMMES,
       CASE WHEN tableau = 'T50' THEN ROUND(sp_h, 1) END          AS SERVICES_MOYENS_HOMMES,
       eff_f                                                      AS EFFECTIF_FEMMES,
       ROUND(mnt_f)                                               AS MONTANT_FEMMES,
       CASE WHEN tableau = 'T50' THEN ROUND(sp_f, 1) END          AS SERVICES_MOYENS_FEMMES,
       eff_t                                                      AS EFFECTIF_TOTAL,
       ROUND(mnt_t)                                               AS MONTANT_TOTAL,
       :1                                                         AS CLICHE
FROM lignes
UNION ALL
-- la ligne TOTAL de chaque tableau : la somme des 5 années
SELECT tableau, 'TOTAL',
       SUM(eff_h), ROUND(SUM(mnt_h)), NULL,
       SUM(eff_f), ROUND(SUM(mnt_f)), NULL,
       SUM(eff_t), ROUND(SUM(mnt_t)),
       :1
FROM lignes
GROUP BY tableau
ORDER BY 1, 2;


-- =============================================================================
-- Requête 2 — T56 : les prestations servies par branche, sur 5 ans
-- Règles :
--   · Montant : les paiements (PM) et les remboursements (RB) au statut D ou P,
--     au mois payé (R3, R4) ; pas les ajustements (AJ), qui corrigent des
--     paiements (T31). Les prestations en nature des AT/MP ne sont pas dans
--     les débours (P95) : AT/MP reste sous l'annuaire.
--   · Branche du débours (CODE_BRANCHE, toujours celle du dossier, P07) : AVS
--     = V ; AT/MP = A ; PF = F et M. La ligne AUTRES reste vide : la base n'a
--     pas d'autre branche (P95).
--   · Une ligne TOTAL : la somme des branches.
-- =============================================================================

WITH

parametres AS (
    SELECT TO_NUMBER(:2) AS annee_n,
           -- date d'un paiement (règle R4, en attente de la CNSS) : 'MOIS' = le
           -- mois payé, 'SAISIE' = la saisie du paiement (rappels compris)
           'MOIS'        AS date_montant
    FROM DUAL
),

branches AS (
    SELECT 1 AS rang, 'PF' AS branche FROM DUAL
    UNION ALL SELECT 2, 'AT/MP'  FROM DUAL
    UNION ALL SELECT 3, 'AVS'    FROM DUAL
    UNION ALL SELECT 4, 'AUTRES' FROM DUAL
),

paie AS (
    SELECT CASE deb.CODE_BRANCHE
               WHEN 'V' THEN 'AVS'
               WHEN 'A' THEN 'AT/MP'
               WHEN 'F' THEN 'PF'
               WHEN 'M' THEN 'PF'
               ELSE          'AUTRES'
           END                                                    AS branche,
           pr.annee_n - EXTRACT(YEAR FROM CASE pr.date_montant
                                              WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
                                              ELSE deb.DEB_DATE_EFFET END) AS recul,
           deb.DEB_MONTANT
    FROM DWH.FAIT_DEBOURS deb
    CROSS JOIN parametres pr
    WHERE deb.CLICHE = :1
      AND deb.DEB_TYPE IN ('PM', 'RB')
      AND deb.DEB_STATUT IN ('D', 'P')
      AND EXTRACT(YEAR FROM CASE pr.date_montant
                                WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
                                ELSE deb.DEB_DATE_EFFET END)
          BETWEEN pr.annee_n - 4 AND pr.annee_n
)

SELECT b.rang                                                     AS RANG,
       b.branche                                                  AS BRANCHE,
       ROUND(SUM(CASE WHEN p.recul = 4 THEN p.DEB_MONTANT ELSE 0 END)) AS MONTANT_N_MOINS_4,
       ROUND(SUM(CASE WHEN p.recul = 3 THEN p.DEB_MONTANT ELSE 0 END)) AS MONTANT_N_MOINS_3,
       ROUND(SUM(CASE WHEN p.recul = 2 THEN p.DEB_MONTANT ELSE 0 END)) AS MONTANT_N_MOINS_2,
       ROUND(SUM(CASE WHEN p.recul = 1 THEN p.DEB_MONTANT ELSE 0 END)) AS MONTANT_N_MOINS_1,
       ROUND(SUM(CASE WHEN p.recul = 0 THEN p.DEB_MONTANT ELSE 0 END)) AS MONTANT_N,
       :1                                                         AS CLICHE
FROM branches b
LEFT JOIN paie p ON p.branche = b.branche
GROUP BY b.rang, b.branche
UNION ALL
SELECT 5, 'TOTAL',
       ROUND(SUM(CASE WHEN recul = 4 THEN DEB_MONTANT ELSE 0 END)),
       ROUND(SUM(CASE WHEN recul = 3 THEN DEB_MONTANT ELSE 0 END)),
       ROUND(SUM(CASE WHEN recul = 2 THEN DEB_MONTANT ELSE 0 END)),
       ROUND(SUM(CASE WHEN recul = 1 THEN DEB_MONTANT ELSE 0 END)),
       ROUND(SUM(CASE WHEN recul = 0 THEN DEB_MONTANT ELSE 0 END)),
       :1
FROM paie
ORDER BY 1;


-- =============================================================================
-- Requête 3 — T57 : les assurés décédés par branche, sur 5 ans
-- Règles (atelier de Kaya 2017 : « assurés décédés déclarés par branche ») :
--   · Année : celle de la date du décès (IND_DATE_DECES), comme le tableau 26
--     de l'annuaire de la CNSS ; la base n'a pas de date de déclaration (P96).
--     Calculé sur le cliché de fin mars N+1 (R13), T57 ne voit que les décès
--     déjà déclarés, comme l'annuaire : les décès de retraités de 2024 connus
--     fin mars 2025 retombent mois par mois sur le tableau 26 (444 pour 447,
--     P99) ; la base d'aujourd'hui en compte 619.
--   · AVS : les titulaires d'une pension directe (vieillesse, anticipée,
--     invalidité) en cours à leur décès (pas radiée plus d'un mois avant).
--   · AT/MP : les titulaires d'une rente (viagère ou de survivant) en cours à
--     leur décès, et les victimes d'un accident du travail mortes dans l'année
--     qui suit l'accident (les sinistrés ; DOS_DECES_IMMEDIAT n'est rempli
--     que depuis 2024, P100).
--   · PF : les titulaires d'un dossier d'allocations familiales en cours.
--   · AUTRES : rien pour l'instant ; TOTAL : la somme des branches.
-- =============================================================================

WITH

parametres AS (
    SELECT TO_NUMBER(:2) AS annee_n FROM DUAL
),

deces AS (
    SELECT i.IND_ID,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces,
           EXTRACT(YEAR FROM i.IND_DATE_DECES)                    AS annee
    FROM DWH.FAIT_INDIVIDU i
    CROSS JOIN parametres pr
    WHERE i.CLICHE = :1
      AND EXTRACT(YEAR FROM i.IND_DATE_DECES) BETWEEN pr.annee_n - 4 AND pr.annee_n
),

en_cours AS (
    SELECT DISTINCT de.annee, de.IND_ID,
           CASE WHEN p.TPE_CODE IN ('RPV', 'RPRA', 'RPI') THEN 'AVS'
                ELSE 'AT/MP'
           END                                                    AS branche
    FROM deces de
    JOIN DWH.FAIT_PRESTATION_ESP p ON p.IND_ID_PREST = de.IND_ID
                                  AND p.CLICHE       = :1
    WHERE p.TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'ARI', 'ARSV', 'ARSF', 'ARSE', 'ARSA')
      AND p.PE_DATE_EFFET <= de.date_deces
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION >= de.date_deces - 31)
),

victimes AS (
    SELECT DISTINCT de.annee, de.IND_ID, 'AT/MP' AS branche
    FROM deces de
    JOIN DWH.FAIT_DOSSIER ds ON ds.IND_ID = de.IND_ID
                            AND ds.CLICHE = :1
    WHERE ds.TDOS_CODE = 'A'
      AND ds.DOS_DATE_ACCIDENT <= de.date_deces
      AND de.date_deces < ds.DOS_DATE_ACCIDENT + 366
),

allocataires AS (
    SELECT DISTINCT de.annee, de.IND_ID, 'PF' AS branche
    FROM deces de
    JOIN DWH.FAIT_DOSSIER        ds ON ds.IND_ID  = de.IND_ID
                                   AND ds.CLICHE  = :1
    JOIN DWH.FAIT_PRESTATION_ESP p  ON p.DOS_CODE = ds.DOS_CODE
                                   AND p.CLICHE   = :1
    WHERE p.TPE_CODE = 'FAF'
      AND p.PE_DATE_EFFET <= de.date_deces
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION >= de.date_deces - 31)
),

-- une personne une fois par branche et par année
decedes AS (
    SELECT annee, IND_ID, branche FROM en_cours
    UNION
    SELECT annee, IND_ID, branche FROM victimes
    UNION
    SELECT annee, IND_ID, branche FROM allocataires
)

SELECT TO_CHAR(d.annee)                                           AS ANNEE,
       SUM(CASE WHEN d.branche = 'PF'    THEN 1 ELSE 0 END)       AS PF,
       SUM(CASE WHEN d.branche = 'AT/MP' THEN 1 ELSE 0 END)       AS AT_MP,
       SUM(CASE WHEN d.branche = 'AVS'   THEN 1 ELSE 0 END)       AS AVS,
       0                                                          AS AUTRES,
       COUNT(*)                                                   AS TOTAL,
       :1                                                         AS CLICHE
FROM decedes d
GROUP BY d.annee
UNION ALL
SELECT 'TOTAL',
       SUM(CASE WHEN branche = 'PF'    THEN 1 ELSE 0 END),
       SUM(CASE WHEN branche = 'AT/MP' THEN 1 ELSE 0 END),
       SUM(CASE WHEN branche = 'AVS'   THEN 1 ELSE 0 END),
       0,
       COUNT(*),
       :1
FROM decedes
ORDER BY 1
