-- DTM_TRAVAILLEUR — Indicateurs travailleur par mois, géographie, sexe, tranche d'âge, forme juridique
-- :1 = CLICHE (MMYYYY) — filtre la période et étiquette les lignes insérées
WITH

-- ── CTE 1 : contrat actif le plus récent ────────────────────────────────
contrat_actif AS (
    SELECT *
    FROM (
        SELECT em.*,
               ROW_NUMBER() OVER (
                   PARTITION BY em.TR_ID
                   ORDER BY em.EM_DATE_DEBUT DESC
               ) rn
        FROM DWH.FAIT_EMPLOI em
        WHERE em.EM_DATE_FIN IS NULL
          AND em.CLICHE = :1
    )
    WHERE rn = 1
),

-- ── CTE 2 : immatriculations ─────────────────────────────────────────────
flux_imm AS (
    SELECT
        TO_NUMBER(TO_CHAR(TRUNC(tr.TR_DATE_IMM,'MM'),'YYYYMMDD'))  AS ID_TEMPS,
        sp.DR_NO,
        tr.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        fj.FJ_CODE                                                  AS EMP_FJ_CODE,
        fj.FJ_CODE_SUP                                              AS EMP_FJ_CODE_SUP,
        fj.SECT_CODE,
        COUNT(DISTINCT tr.TR_ID)                                    AS NB_IMM,
        COUNT(DISTINCT CASE WHEN tr.TR_ETAT = 'A'
              THEN tr.TR_ID END)                                    AS NB_ACTIFS
    FROM DWH.FAIT_TRAVAILLEUR tr

    LEFT JOIN contrat_actif ca
           ON ca.TR_ID = tr.TR_ID

    LEFT JOIN DWH.FAIT_EMPLOYEUR emp
           ON emp.EMP_ID = ca.EMP_ID

    LEFT JOIN DTM.DIM_FORME_JURIDIQUE fj
           ON fj.FJ_CODE = emp.EMP_FORME_JURIDIQUE

    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL sp
           ON sp.SP_NO = tr.SP_NO

    LEFT JOIN DTM.DIM_TRANCHE_AGE tar
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE)/12)
              BETWEEN tar.INF AND tar.SUP

    WHERE tr.TR_DATE_IMM IS NOT NULL
      AND tr.CLICHE = :1

    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(tr.TR_DATE_IMM,'MM'),'YYYYMMDD')),
        sp.DR_NO,
        tr.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        fj.FJ_CODE,
        fj.FJ_CODE_SUP,
        fj.SECT_CODE
),

-- ── CTE 3 : mouvements ───────────────────────────────────────────────────
flux_mvt AS (
    SELECT
        TO_NUMBER(TO_CHAR(TRUNC(em.EM_DATE_FIN,'MM'),'YYYYMMDD'))  AS ID_TEMPS,
        sp.DR_NO,
        e.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        COUNT(DISTINCT CASE WHEN em.EM_MOTIF_SORTIE = 'RETRAITE'
              THEN em.TR_ID END)                                    AS NB_RET,
        COUNT(DISTINCT CASE WHEN em.EM_MOTIF_SORTIE = 'DECES'
              THEN em.TR_ID END)                                    AS NB_DEC,
        COUNT(DISTINCT CASE WHEN em.EM_MOTIF_SORTIE = 'LICENCIEMENT'
              THEN em.TR_ID END)                                    AS NB_LIC,
        COUNT(DISTINCT CASE WHEN em.EM_MOTIF_SORTIE = 'DEMISSION'
              THEN em.TR_ID END)                                    AS NB_DEM,
        COUNT(DISTINCT CASE
              WHEN em.EM_MOTIF_SORTIE IS NOT NULL
               AND em.EM_MOTIF_SORTIE NOT IN
                   ('RETRAITE','DECES','LICENCIEMENT','DEMISSION')
              THEN em.TR_ID END)                                    AS NB_AUT
    FROM DWH.FAIT_EMPLOI em
    LEFT JOIN DWH.FAIT_TRAVAILLEUR tr        ON tr.TR_ID = em.TR_ID
    LEFT JOIN DWH.FAIT_EMPLOYEUR e           ON e.EMP_ID = em.EMP_ID
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL sp  ON sp.SP_NO = e.SP_NO
    LEFT JOIN DTM.DIM_TRANCHE_AGE tar
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE)/12)
              BETWEEN tar.INF AND tar.SUP
    WHERE em.EM_DATE_FIN IS NOT NULL
      AND em.EM_MOTIF_SORTIE IS NOT NULL
      AND em.CLICHE = :1
    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(em.EM_DATE_FIN,'MM'),'YYYYMMDD')),
        sp.DR_NO,
        e.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE
),

-- ── CTE 4 : salaires ─────────────────────────────────────────────────────
flux_sal AS (
    SELECT
        TO_NUMBER(TO_CHAR(TRUNC(
            TO_DATE(TO_CHAR(dn.PER_ID),'YYYYMM'),'MM'),'YYYYMMDD')) AS ID_TEMPS,
        sp.DR_NO,
        e.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        SUM(LEAST(NVL(s.SAL_BASE_COTISATION,0),
            pg.PG_SALAIRE_MAX))                                     AS MASSE_PLAFON,
        SUM(NVL(s.SAL_MONTANT_BRUT,0))                             AS MASSE_BRUT
    FROM DWH.FAIT_SALAIRE s
    INNER JOIN DWH.FAIT_DECLARATION_NOMINATIVE dn ON dn.DN_ID = s.DN_ID
    LEFT JOIN DWH.FAIT_TRAVAILLEUR tr             ON tr.TR_ID = s.TR_ID
    LEFT JOIN DWH.FAIT_EMPLOYEUR e                ON e.EMP_ID = dn.EMP_ID
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL sp        ON sp.SP_NO = e.SP_NO
    LEFT JOIN DTM.DIM_TRANCHE_AGE tar
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE)/12)
              BETWEEN tar.INF AND tar.SUP
    CROSS JOIN USER_DWH.PARAMETRE_GLOBAL pg
    WHERE dn.CLICHE = :1
    GROUP BY
        TO_NUMBER(TO_CHAR(TRUNC(
            TO_DATE(TO_CHAR(dn.PER_ID),'YYYYMM'),'MM'),'YYYYMMDD')),
        sp.DR_NO,
        e.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE
)

-- ── SELECT FINAL ─────────────────────────────────────────────────────────
SELECT
    i.ID_TEMPS,
    i.DR_NO,
    i.SP_NO,
    NULL                                        AS SA_NO,
    i.TR_SEXE,
    CASE i.TR_SEXE
        WHEN 1 THEN 'Masculin'
        WHEN 2 THEN 'Feminin'
        ELSE        'Inconnu'
    END                                         AS TR_SEXE_LIBELLE,
    i.TAG_CODE,
    NULL                                        AS TR_EFFECTIFS_DEBUT,
    i.NB_ACTIFS                                 AS TR_EFFECTIF,
    i.NB_IMM                                    AS TR_IMMATRICULES,
    m.NB_RET                                    AS TR_RETRAITES,
    m.NB_DEC                                    AS TR_DECEDES,
    m.NB_LIC                                    AS TR_LICENCIES,
    m.NB_DEM                                    AS TR_DEMISSIONS,
    m.NB_AUT                                    AS TR_AUTRES,
    sl.MASSE_PLAFON                             AS MASSE_SAL_PLAFON,
    sl.MASSE_BRUT                               AS MASSE_SAL_NON_PLAFON,
    i.EMP_FJ_CODE,
    i.EMP_FJ_CODE_SUP,
    CASE i.SECT_CODE
        WHEN 'PB' THEN 'PUBLIC'
        WHEN 'PV' THEN 'PRIVE'
        ELSE i.SECT_CODE
    END                                         AS EMP_CAT_LIBELLE,
    :1                                          AS CLICHE
FROM flux_imm i
LEFT JOIN flux_mvt m
       ON m.ID_TEMPS  = i.ID_TEMPS
      AND m.DR_NO     = i.DR_NO
      AND m.SP_NO     = i.SP_NO
      AND m.TR_SEXE   = i.TR_SEXE
      AND m.TAG_CODE  = i.TAG_CODE
LEFT JOIN flux_sal sl
       ON sl.ID_TEMPS = i.ID_TEMPS
      AND sl.DR_NO    = i.DR_NO
      AND sl.SP_NO    = i.SP_NO
      AND sl.TR_SEXE  = i.TR_SEXE
      AND sl.TAG_CODE = i.TAG_CODE
