-- DTM_CONTROLE_FREQ_EMP V2
-- Sources : DWH.FAIT_CONTROLE    (contrôles employeurs)
--           DWH.FAIT_EMPLOYEUR   (employeurs actifs — fréquence 0)
-- Grain   : ANNEE × NBRE_CONTROLE
--           → NB_EMPLOYEURS ayant subi exactement NBRE_CONTROLE contrôles
--             (par type de contrôle) dans l'année du CLICHE
-- Fréquence 0 : employeurs actifs sans aucun contrôle dans l'année du CLICHE
-- :1 = CLICHE (MMYYYY) — snapshot DWH uniforme

WITH req AS (
    -- Employeurs actifs n'ayant aucun contrôle dans l'année du CLICHE
    SELECT fe.EMP_ID
    FROM   DWH.FAIT_EMPLOYEUR fe
    WHERE  fe.EMP_ETAT = 'A'
      AND  fe.CLICHE   = :1
      AND  fe.EMP_ID NOT IN (
               SELECT fc.EMP_ID
               FROM   DWH.FAIT_CONTROLE fc
               WHERE  fc.CLICHE = :1
                 AND  EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE, 'DD/MM/YY'))
                      = EXTRACT(YEAR FROM TO_DATE(:1, 'MMYYYY'))
           )
),
req1 AS (
    -- Nombre de contrôles par employeur actif, par type et par année du CLICHE
    SELECT
        EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE, 'DD/MM/YY'))  AS ANNEE,
        fc.CTL_TYPE,
        fe.EMP_ID,
        COUNT(*)                                               AS NBRE_CONTROLE
    FROM   DWH.FAIT_EMPLOYEUR  fe
    JOIN   DWH.FAIT_CONTROLE   fc ON fc.EMP_ID = fe.EMP_ID
    WHERE  fc.CLICHE   = :1
      AND  fe.CLICHE   = :1
      AND  fe.EMP_ETAT = 'A'
      AND  EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE, 'DD/MM/YY'))
           = EXTRACT(YEAR FROM TO_DATE(:1, 'MMYYYY'))
    GROUP BY
        EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE, 'DD/MM/YY')),
        fc.CTL_TYPE,
        fe.EMP_ID
)

-- ── Employeurs avec contrôles : distribution par fréquence ─────────────────
SELECT
    TO_CHAR(ANNEE)  AS  AN_ID,
    NBRE_CONTROLE,
    COUNT(EMP_ID)   AS NB_EMPLOYEURS,
    :1              AS CLICHE
FROM   req1
GROUP BY ANNEE, NBRE_CONTROLE

UNION ALL

-- ── Employeurs sans contrôle (fréquence 0) ─────────────────────────────────
SELECT
    TO_CHAR(EXTRACT(YEAR FROM TO_DATE(:1, 'MMYYYY')))  AS AN_ID,
    0                                                    AS NBRE_CONTROLE,
    COUNT(*)                                             AS NB_EMPLOYEURS,
    :1                                                   AS CLICHE
FROM   req
