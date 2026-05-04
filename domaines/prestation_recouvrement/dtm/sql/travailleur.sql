
WITH

-- ── SNAPSHOT DEBUT (reconstitution au 31/12) ─────────────────────────────
emp_post_dec AS (
    SELECT EMP_ID
    FROM DWH.FAIT_EMPLOI
    WHERE TO_DATE(EM_DATE_DEBUT, 'DD/MM/RR') > TO_DATE('3112' || (TO_NUMBER(SUBSTR(:1, 3, 4)) - 1), 'DDMMYYYY')
      AND CLICHE = :1
),
emp_actifs_au_dec AS (
    SELECT EMP_ID
    FROM DWH.FAIT_EMPLOI
    WHERE EM_DATE_FIN IS NOT NULL
      AND TO_DATE(EM_DATE_FIN, 'DD/MM/RR') > TO_DATE('3112' || (TO_NUMBER(SUBSTR(:1, 3, 4)) - 1), 'DDMMYYYY')
      AND CLICHE = :1
      AND EMP_ID NOT IN (SELECT EMP_ID FROM emp_post_dec)
),
emp_base_dec AS (
    SELECT *
    FROM DWH.FAIT_EMPLOI
    WHERE EMP_ID NOT IN (SELECT EMP_ID FROM emp_post_dec)
      AND CLICHE = :1
),
emp_snapshot_debut AS (
    SELECT
        e.EMP_ID,
        e.EM_DATE_DEBUT,
        CASE WHEN a.EMP_ID IS NOT NULL THEN NULL ELSE e.EM_DATE_FIN END AS EM_DATE_FIN,
        e.TR_ID,
        e.EM_QUALIFICATION,
        e.EM_TYPE_EMPLOI,
        e.EM_PROFESSION,
        e.EM_CATEGORIE,
        e.EM_ASSURE_VOL,
        e.EM_DATE_INSERT,
        e.EM_DATE_UPDATE,
        e.EM_USAGER_INSERT,
        e.EM_USAGER_UPDATE,
        e.EM_SALAIRE,
        e.EM_MOTIF_SORTIE,
        e.EM_REGUL,
        e.EM_ID,
        e.DMD_ID_ECNSS_INS,
        e.DMD_ID_ECNSS_UPD,
        e.SP_NO,
        e.SP_NO_UPD
    FROM emp_base_dec e
    LEFT JOIN emp_actifs_au_dec a ON e.EMP_ID = a.EMP_ID
),
snapshot_debut_contrat_actif AS (
    SELECT *
    FROM (
        SELECT em.*,
               ROW_NUMBER() OVER (PARTITION BY em.TR_ID ORDER BY em.EM_DATE_DEBUT DESC) rn
        FROM emp_snapshot_debut em
        WHERE em.EM_DATE_FIN IS NULL
    )
    WHERE rn = 1
),
snapshot_actu_contrat_actif AS (
    SELECT *
    FROM (
        SELECT em.*,
               ROW_NUMBER() OVER (PARTITION BY em.TR_ID ORDER BY em.EM_DATE_DEBUT DESC) rn
        FROM DWH.FAIT_EMPLOI em
        WHERE em.EM_DATE_FIN IS NULL
          AND em.CLICHE = :1
    )
    WHERE rn = 1
),

-- ── FLUX MUTATIONS (sorties : retraites, décès, autres) ───────────────────
flux_mutations AS (
    SELECT
        sp.DR_NO,
        e.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        e.SA_NO,
        e.EMP_FORME_JURIDIQUE                                                          AS EMP_FJ_CODE,
        e.EMP_REGIME,
        COUNT(DISTINCT CASE WHEN em.EM_MOTIF_SORTIE = 'RETRAITE'     THEN em.TR_ID END) AS NB_RET,
        COUNT(DISTINCT CASE WHEN em.EM_MOTIF_SORTIE = 'DECES'        THEN em.TR_ID END) AS NB_DEC,
        COUNT(DISTINCT CASE WHEN em.EM_MOTIF_SORTIE = 'LICENCIEMENT' THEN em.TR_ID END) AS NB_LIC,
        COUNT(DISTINCT CASE WHEN em.EM_MOTIF_SORTIE = 'DEMISSION'    THEN em.TR_ID END) AS NB_DEM,
        COUNT(DISTINCT CASE
              WHEN em.EM_MOTIF_SORTIE IS NOT NULL
               AND em.EM_MOTIF_SORTIE NOT IN ('RETRAITE','DECES','LICENCIEMENT','DEMISSION')
              THEN em.TR_ID END)                                                         AS NB_AUT
    FROM DWH.FAIT_EMPLOI em
    LEFT JOIN DWH.FAIT_TRAVAILLEUR       tr  ON tr.TR_ID  = em.TR_ID  AND tr.CLICHE = :1
    LEFT JOIN DWH.FAIT_EMPLOYEUR         e   ON e.EMP_ID  = em.EMP_ID AND e.CLICHE  = :1
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL sp  ON sp.SP_NO  = e.SP_NO
    LEFT JOIN DTM.DIM_TRANCHE_AGE        tar
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE)/12) BETWEEN tar.INF AND tar.SUP
    WHERE em.EM_DATE_FIN   IS NOT NULL
      AND em.EM_MOTIF_SORTIE IS NOT NULL
      AND em.CLICHE = :1
    GROUP BY
        sp.DR_NO,
        e.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        e.SA_NO,
        e.EMP_FORME_JURIDIQUE,
        e.EMP_REGIME
),

-- ── FLUX IMMATRICULATIONS (nouvelles entrées sur la période) ──────────────
flux_imm AS (
    SELECT
        sp.DR_NO,
        tr.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        fj.FJ_CODE                                                  AS EMP_FJ_CODE,
        fj.FJ_CODE_SUP                                              AS EMP_FJ_CODE_SUP,
        fj.SECT_CODE,
        emp.SA_NO,
        emp.EMP_REGIME,
        COUNT(DISTINCT snap.TR_ID)                                  AS NB_IMM,
        COUNT(DISTINCT CASE WHEN tr.TR_ETAT = 'A' THEN snap.TR_ID END) AS NB_ACTIFS
    FROM snapshot_actu_contrat_actif snap
    LEFT JOIN DWH.FAIT_TRAVAILLEUR       tr  ON tr.TR_ID  = snap.TR_ID  AND tr.CLICHE = :1
    LEFT JOIN DWH.FAIT_EMPLOYEUR         emp ON emp.EMP_ID = snap.EMP_ID AND emp.CLICHE = :1
    LEFT JOIN DTM.DIM_FORME_JURIDIQUE    fj  ON fj.FJ_CODE = emp.EMP_FORME_JURIDIQUE
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL sp  ON sp.SP_NO   = tr.SP_NO
    LEFT JOIN DTM.DIM_TRANCHE_AGE        tar
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE)/12) BETWEEN tar.INF AND tar.SUP
    WHERE tr.TR_DATE_IMM >= TO_DATE('0101' || SUBSTR(:1, 3, 4), 'DDMMYYYY')
      AND tr.TR_DATE_IMM <  TO_DATE('0101' || (TO_NUMBER(SUBSTR(:1, 3, 4)) + 1), 'DDMMYYYY')
      AND tr.CLICHE = :1
    GROUP BY
        sp.DR_NO,
        tr.SP_NO,
        emp.SA_NO,
        emp.EMP_REGIME,
        tr.TR_SEXE,
        tar.TAG_CODE,
        fj.FJ_CODE,
        fj.FJ_CODE_SUP,
        fj.SECT_CODE
),

-- ── EFFECTIFS DEBUT PERIODE ───────────────────────────────────────────────
flux_debut AS (
    SELECT
        sp.DR_NO,
        tr.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        fj.FJ_CODE                                                  AS EMP_FJ_CODE,
        fj.FJ_CODE_SUP                                              AS EMP_FJ_CODE_SUP,
        fj.SECT_CODE,
        emp.SA_NO,
        emp.EMP_REGIME,
        COUNT(DISTINCT snap.TR_ID)                                  AS NB_DEBUT
    FROM snapshot_debut_contrat_actif snap
    LEFT JOIN DWH.FAIT_TRAVAILLEUR       tr  ON tr.TR_ID  = snap.TR_ID  AND tr.CLICHE = :1
    LEFT JOIN DWH.FAIT_EMPLOYEUR         emp ON emp.EMP_ID = snap.EMP_ID AND emp.CLICHE = :1
    LEFT JOIN DTM.DIM_FORME_JURIDIQUE    fj  ON fj.FJ_CODE = emp.EMP_FORME_JURIDIQUE
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL sp  ON sp.SP_NO   = tr.SP_NO
    LEFT JOIN DTM.DIM_TRANCHE_AGE        tar
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE)/12) BETWEEN tar.INF AND tar.SUP
    WHERE tr.CLICHE = :1
    GROUP BY
        sp.DR_NO,
        tr.SP_NO,
        emp.SA_NO,
        emp.EMP_REGIME,
        tr.TR_SEXE,
        tar.TAG_CODE,
        fj.FJ_CODE,
        fj.FJ_CODE_SUP,
        fj.SECT_CODE
),

-- ── EFFECTIFS FIN PERIODE ─────────────────────────────────────────────────
flux_fin AS (
    SELECT
        sp.DR_NO,
        tr.SP_NO,
        tr.TR_SEXE,
        tar.TAG_CODE,
        fj.FJ_CODE                                                  AS EMP_FJ_CODE,
        fj.FJ_CODE_SUP                                              AS EMP_FJ_CODE_SUP,
        fj.SECT_CODE,
        emp.SA_NO,
        emp.EMP_REGIME,
        COUNT(DISTINCT snap.TR_ID)                                  AS NB_FIN
    FROM snapshot_actu_contrat_actif snap
    LEFT JOIN DWH.FAIT_TRAVAILLEUR       tr  ON tr.TR_ID  = snap.TR_ID  AND tr.CLICHE = :1
    LEFT JOIN DWH.FAIT_EMPLOYEUR         emp ON emp.EMP_ID = snap.EMP_ID AND emp.CLICHE = :1
    LEFT JOIN DTM.DIM_FORME_JURIDIQUE    fj  ON fj.FJ_CODE = emp.EMP_FORME_JURIDIQUE
    LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL sp  ON sp.SP_NO   = tr.SP_NO
    LEFT JOIN DTM.DIM_TRANCHE_AGE        tar
           ON FLOOR(MONTHS_BETWEEN(SYSDATE, tr.TR_DATE_NAISSANCE)/12) BETWEEN tar.INF AND tar.SUP
    WHERE tr.CLICHE = :1
    GROUP BY
        sp.DR_NO,
        tr.SP_NO,
        emp.SA_NO,
        emp.EMP_REGIME,
        tr.TR_SEXE,
        tar.TAG_CODE,
        fj.FJ_CODE,
        fj.FJ_CODE_SUP,
        fj.SECT_CODE
),

-- ── ASSEMBLAGE ─────────────────────
base AS (
    SELECT
        CAST(EXTRACT(YEAR FROM TO_DATE(:1, 'MMYYYY')) AS NUMBER(4))       AS AN_ID,
        COALESCE(i.DR_NO,      d.DR_NO,      f.DR_NO,      m.DR_NO)     AS DR_NO,
        COALESCE(i.SP_NO,      d.SP_NO,      f.SP_NO,      m.SP_NO)     AS SP_NO,
        COALESCE(i.SA_NO,      d.SA_NO,      f.SA_NO)                   AS SA_NO,
        COALESCE(i.TR_SEXE,    d.TR_SEXE,    f.TR_SEXE,    m.TR_SEXE)   AS TR_SEXE,
        CASE COALESCE(i.TR_SEXE, d.TR_SEXE, f.TR_SEXE, m.TR_SEXE)
            WHEN 1 THEN 'Masculin'
            WHEN 2 THEN 'Feminin'
            ELSE        'Inconnu'
        END                                                              AS TR_SEXE_LIBELLE,
        COALESCE(i.TAG_CODE,   d.TAG_CODE,   f.TAG_CODE,   m.TAG_CODE)  AS TAG_CODE,
        COALESCE(i.EMP_FJ_CODE,d.EMP_FJ_CODE,f.EMP_FJ_CODE,m.EMP_FJ_CODE) AS EMP_FJ_CODE,
        COALESCE(i.EMP_FJ_CODE_SUP, d.EMP_FJ_CODE_SUP, f.EMP_FJ_CODE_SUP) AS EMP_FJ_CODE_SUP,
        COALESCE(i.EMP_REGIME, d.EMP_REGIME, f.EMP_REGIME)              AS EMP_REGIME,
        CASE COALESCE(i.SECT_CODE, d.SECT_CODE, f.SECT_CODE)
            WHEN 'PB' THEN 'PUBLIC'
            WHEN 'PV' THEN 'PRIVE'
            ELSE COALESCE(i.SECT_CODE, d.SECT_CODE, f.SECT_CODE)
        END                                                              AS EMP_CAT_LIBELLE,
        d.NB_DEBUT                                                       AS TR_EFFECTIFS_DEBUT,
        i.NB_IMM                                                         AS TR_IMMATRICULES,
        m.NB_RET                                                         AS TR_RETRAITES,
        m.NB_DEC                                                         AS TR_DECEDES,
        m.NB_LIC                                                         AS TR_LICENCIES,
        m.NB_DEM                                                         AS TR_DEMISSIONS,
        m.NB_AUT                                                         AS TR_AUTRES,
        f.NB_FIN                                                         AS TR_EFFECTIFS_FIN,
        :1                                                               AS CLICHE
    FROM flux_imm i
    FULL OUTER JOIN flux_debut d
           ON d.DR_NO       = i.DR_NO
          AND d.SP_NO       = i.SP_NO
          AND d.SA_NO       = i.SA_NO
          AND d.EMP_REGIME  = i.EMP_REGIME
          AND d.TR_SEXE     = i.TR_SEXE
          AND d.TAG_CODE    = i.TAG_CODE
          AND d.EMP_FJ_CODE = i.EMP_FJ_CODE
    FULL OUTER JOIN flux_fin f
           ON f.DR_NO       = COALESCE(i.DR_NO,      d.DR_NO)
          AND f.SP_NO       = COALESCE(i.SP_NO,      d.SP_NO)
          AND f.SA_NO       = COALESCE(i.SA_NO,      d.SA_NO)
          AND f.EMP_REGIME  = COALESCE(i.EMP_REGIME, d.EMP_REGIME)
          AND f.TR_SEXE     = COALESCE(i.TR_SEXE,    d.TR_SEXE)
          AND f.TAG_CODE    = COALESCE(i.TAG_CODE,   d.TAG_CODE)
          AND f.EMP_FJ_CODE = COALESCE(i.EMP_FJ_CODE, d.EMP_FJ_CODE)
    FULL OUTER JOIN flux_mutations m
           ON m.SA_NO      = COALESCE(i.SA_NO,      d.SA_NO,      f.SA_NO)
          AND m.DR_NO      = COALESCE(i.DR_NO,      d.DR_NO,      f.DR_NO)
          AND m.SP_NO      = COALESCE(i.SP_NO,      d.SP_NO,      f.SP_NO)
          AND m.TR_SEXE    = COALESCE(i.TR_SEXE,    d.TR_SEXE,    f.TR_SEXE)
          AND m.TAG_CODE   = COALESCE(i.TAG_CODE,   d.TAG_CODE,   f.TAG_CODE)
          AND m.EMP_FJ_CODE = COALESCE(i.EMP_FJ_CODE, d.EMP_FJ_CODE, f.EMP_FJ_CODE)
          AND m.EMP_REGIME  = COALESCE(i.EMP_REGIME,  d.EMP_REGIME,  f.EMP_REGIME)
)

-- ── TABLEAU 7 : agrégation multi-axes + ligne TOTAL ──────────────────────
SELECT * FROM base;