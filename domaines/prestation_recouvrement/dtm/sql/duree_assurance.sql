-- DTM_DUREE_ASSURANCE V1
-- Sources : DWH.FAIT_SALAIRE               (SAL_STATUT, DN_ID)
--           DWH.FAIT_DECLARATION_NOMINATIVE (PER_ID, EMP_ID)
--           DWH.FAIT_EMPLOYEUR             (EMP_PERIODICITE — coeff T=3 mois)
--           DWH.FAIT_TRAVAILLEUR           (TR_SEXE)
--           DTM.DIM_TRANCHE_DUREE_ASSURANCE (tranche de durée)
-- Grain   : ID_TEMPS × TR_SEXE × TDA_CODE
-- Logique : durée cumulée = SUM des coefficients périodiques par (TR_ID, PER_ID)
--           Trimestriel (EMP_PERIODICITE='T') → COEFF=3 | sinon COEFF=1
--           MAX par (TR_ID, PER_ID) pour dédupliquer les doublons salaire
-- ID_TEMPS : 1er janvier de l'année de la dernière période cotisée du travailleur
--            FLOOR(MAX(PER_ID) / 100) * 10000 + 101
-- Filtre  : SAL_STATUT IS NULL OR NOT IN ('A','R') | TR_SEXE IN (1, 2)
-- :1 = CLICHE (YYYYMM) — snapshot DWH uniforme

WITH

-- ── CTE 1 : coefficient mensuel par (TR_ID, PER_ID) ──────────────────────
-- MAX(COEFF) pour dédupliquer les lignes salaire multiples sur une même période
coeff_periode AS (
    SELECT
        s.TR_ID,
        dn.PER_ID,
        MAX(CASE e.EMP_PERIODICITE
                WHEN 'T' THEN 3
                ELSE          1
            END)                                         AS COEFF
    FROM DWH.FAIT_SALAIRE                s
    JOIN DWH.FAIT_DECLARATION_NOMINATIVE dn
      ON dn.DN_ID  = s.DN_ID
     AND dn.CLICHE = :1
    JOIN DWH.FAIT_EMPLOYEUR              e
      ON e.EMP_ID  = dn.EMP_ID
     AND e.CLICHE  = :1
    WHERE s.CLICHE  = :1
      AND dn.PER_ID IS NOT NULL
      AND (s.SAL_STATUT IS NULL OR s.SAL_STATUT NOT IN ('A', 'R'))
    GROUP BY s.TR_ID, dn.PER_ID
),

-- ── CTE 2 : durée totale + dernière période par travailleur ───────────────
duree AS (
    SELECT
        cp.TR_ID,
        tr.TR_SEXE,
        SUM(cp.COEFF)                                    AS NB_MOIS,
        MAX(cp.PER_ID)                                   AS MAX_PER_ID
    FROM coeff_periode               cp
    JOIN DWH.FAIT_TRAVAILLEUR        tr
      ON tr.TR_ID    = cp.TR_ID
     AND tr.CLICHE   = :1
    WHERE tr.TR_SEXE IN (1, 2)
    GROUP BY cp.TR_ID, tr.TR_SEXE
)

SELECT
    FLOOR(MAX(d.MAX_PER_ID) / 100) * 10000 + 101        AS ID_TEMPS,
    d.TR_SEXE,
    tda.TDA_CODE,
    CASE d.TR_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END                                                  AS TR_SEXE_LIBELLE,
    COUNT(*)                                             AS NB_COTISANTS,
    :1                                                   AS CLICHE

FROM duree d

JOIN DTM.DIM_TRANCHE_DUREE_ASSURANCE tda
  ON d.NB_MOIS   >= tda.MOIS_MIN
 AND (d.NB_MOIS  <  tda.MOIS_MAX OR tda.MOIS_MAX IS NULL)
 AND tda.TDA_CODE > 0

GROUP BY
    d.TR_SEXE,
    CASE d.TR_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        NULL
    END,
    tda.TDA_CODE,
    :1
