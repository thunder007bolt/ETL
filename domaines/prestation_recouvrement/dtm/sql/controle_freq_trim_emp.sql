-- DTM_CONTROLE_FREQ_TRIM_EMP V1
-- Sources : DWH.FAIT_CONTROLE    (contrôles employeurs)
--           DWH.FAIT_EMPLOYEUR   (employeurs actifs)
-- Grain   : ANNEE × TRIMESTRE × CTL_TYPE
--           → NB_CONTROLES effectués dans le trimestre sur des employeurs actifs
-- :1 = CLICHE (MMYYYY) — snapshot DWH uniforme

SELECT
    EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE, 'DD/MM/YY'))       AS AN_ID,
    TO_CHAR(TO_DATE(fc.CTL_DATE, 'DD/MM/YY'), 'Q')            AS TRIMESTRE,
    fc.CTL_TYPE,
    COUNT(fc.EMP_ID)                                           AS NB_CONTROLES,
    :1                                                         AS CLICHE
FROM   DWH.FAIT_CONTROLE   fc
JOIN   DWH.FAIT_EMPLOYEUR  fe ON fc.EMP_ID = fe.EMP_ID
WHERE  fc.CLICHE   = :1
  AND  fe.CLICHE   = :1
  AND  fe.EMP_ETAT = 'A'
  AND  EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE, 'DD/MM/YY'))
       = EXTRACT(YEAR FROM TO_DATE(:1, 'MMYYYY'))
GROUP BY
    EXTRACT(YEAR FROM TO_DATE(fc.CTL_DATE, 'DD/MM/YY')),
    TO_CHAR(TO_DATE(fc.CTL_DATE, 'DD/MM/YY'), 'Q'),
    fc.CTL_TYPE,
    :1
