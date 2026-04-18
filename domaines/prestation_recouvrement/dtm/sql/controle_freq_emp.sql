-- DTM_CONTROLE_FREQ_EMP V1
-- Sources : DWH.FAIT_CONTROLE          (contrôles employeurs)
--           DWH.FAIT_EMPLOYEUR         (employeurs actifs — tranche 0)
--           DTM.DIM_TRANCHE_CONTROLE   (libellés tranches 0/1/2/3+)
-- Grain   : ID_TEMPS × NB_CTL_TRANCHE × LIBELLE_TRANCHE
-- ID_TEMPS : 1er janvier de chaque année de contrôle (AAAA * 10000 + 101)
-- Tranches : 0 = aucun contrôle | 1 = 1 | 2 = 2 | 3 = 3+
-- Tranche 0 : employeurs actifs sans contrôle dans l'année du CLICHE
-- :1 = CLICHE (YYYYMM) — snapshot DWH uniforme

-- ── Tranches 1 / 2 / 3+ : par année de CTL_DATE ──────────────────────────
SELECT
    EXTRACT(YEAR FROM ctl.CTL_DATE) * 10000 + 101   AS ID_TEMPS,
    tc.NB_CTL_TRANCHE,
    tc.LIBELLE_TRANCHE,
    COUNT(*)                                         AS NB_EMPLOYEURS,
    :1                                               AS CLICHE
FROM (
    SELECT
        EMP_ID,
        CTL_DATE,
        CASE
            WHEN COUNT(CTL_ID) >= 3 THEN 3
            ELSE                         COUNT(CTL_ID)
        END                                          AS NB_CTL_TRANCHE
    FROM DWH.FAIT_CONTROLE
    WHERE CLICHE      = :1
      AND CTL_DATE    IS NOT NULL
    GROUP BY EMP_ID, CTL_DATE
) ctl
JOIN DTM.DIM_TRANCHE_CONTROLE  tc
  ON tc.NB_CTL_TRANCHE = ctl.NB_CTL_TRANCHE
GROUP BY
    EXTRACT(YEAR FROM ctl.CTL_DATE) * 10000 + 101,
    tc.NB_CTL_TRANCHE,
    tc.LIBELLE_TRANCHE,
    :1

UNION ALL

-- ── Tranche 0 : employeurs actifs sans contrôle dans l'année du CLICHE ────
SELECT
    EXTRACT(YEAR FROM TO_DATE(:1, 'YYYYMM')) * 10000 + 101  AS ID_TEMPS,
    tc.NB_CTL_TRANCHE,
    tc.LIBELLE_TRANCHE,
    COUNT(DISTINCT e.EMP_ID)                                 AS NB_EMPLOYEURS,
    :1                                                       AS CLICHE
FROM DWH.FAIT_EMPLOYEUR         e
JOIN DTM.DIM_TRANCHE_CONTROLE   tc
  ON tc.NB_CTL_TRANCHE = 0
WHERE e.EMP_ETAT = 'A'
  AND e.CLICHE   = :1
  AND NOT EXISTS (
      SELECT 1
      FROM DWH.FAIT_CONTROLE    c
      WHERE c.EMP_ID  = e.EMP_ID
        AND c.CLICHE  = :1
        AND EXTRACT(YEAR FROM c.CTL_DATE) = EXTRACT(YEAR FROM TO_DATE(:1, 'YYYYMM'))
  )
GROUP BY
    EXTRACT(YEAR FROM TO_DATE(:1, 'YYYYMM')) * 10000 + 101,
    tc.NB_CTL_TRANCHE,
    tc.LIBELLE_TRANCHE,
    :1
