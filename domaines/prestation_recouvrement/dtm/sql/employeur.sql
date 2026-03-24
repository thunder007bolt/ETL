-- DTM_EMPLOYEUR — Indicateurs employeur par mois d'immatriculation, géographie, régime, periodicité, état et tranche d'effectif
-- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
SELECT
    -- ── Grain ──────────────────────────────────────────────────────────────
    t.ID_TEMPS,
    dpt.PA_NO,
    dpt.PR_NO,
    dpt.DPT_NO,
    sp.DR_NO                      AS DR_NO,
    e.SP_NO                       AS SP_NO,
    e.EMP_REGIME                  AS EMP_REGIME,
    e.SA_NO                       AS SA_NO,
    dp.ID_PERIODICITE             AS ID_PERIODICITE,
    e.EMP_ETAT                    AS EMP_ETAT,
    tef.TEF_CODE                  AS TEF_CODE,
    fj.FJ_CODE                        AS EMP_FJ_CODE,
    fj.FJ_CODE_SUP                    AS EMP_FJ_CODE_SUP,
    CASE fj.SECT_CODE
        WHEN 'PB' THEN 'PUBLIC'
        WHEN 'PV' THEN 'PRIVE'
        ELSE fj.SECT_CODE
    END                               AS EMP_CAT_LIBELLE,
    -- ── Mesures flux ───────────────────────────────────────────────────────
    -- NB_NOUVELLES_IMM : immatriculations dans la période
    COUNT(DISTINCT CASE
        WHEN TO_NUMBER(TO_CHAR(TRUNC(e.EMP_DATE_IMM,'MM'),'YYYYMMDD')) = t.ID_TEMPS
        THEN e.EMP_ID END)                        AS NB_NOUVELLES_IMM,
    -- NB_REACTIVES : changement état vers A dans la période
    COUNT(DISTINCT CASE
        WHEN TO_NUMBER(TO_CHAR(TRUNC(e.EMP_DATE_CHANGE_ETAT,'MM'),'YYYYMMDD')) = t.ID_TEMPS
          AND e.EMP_ETAT = 'A'
        THEN e.EMP_ID END)                        AS NB_REACTIVES,
    -- NB_SUSPENDUS : changement état vers S dans la période
    COUNT(DISTINCT CASE
        WHEN TO_NUMBER(TO_CHAR(TRUNC(e.EMP_DATE_CHANGE_ETAT,'MM'),'YYYYMMDD')) = t.ID_TEMPS
          AND e.EMP_ETAT = 'S'
        THEN e.EMP_ID END)                        AS NB_SUSPENDUS,
    -- NB_RADIES : changement état vers R dans la période
    COUNT(DISTINCT CASE
        WHEN TO_NUMBER(TO_CHAR(TRUNC(e.EMP_DATE_CHANGE_ETAT,'MM'),'YYYYMMDD')) = t.ID_TEMPS
          AND e.EMP_ETAT = 'R'
        THEN e.EMP_ID END)                        AS NB_RADIES,
    -- NB_BERNE : mise en berne dans la période
    COUNT(DISTINCT CASE
        WHEN TO_NUMBER(TO_CHAR(TRUNC(e.EMP_DATE_CHANGE_ETAT,'MM'),'YYYYMMDD')) = t.ID_TEMPS
          AND e.EMP_ETAT = 'B'
        THEN e.EMP_ID END)                        AS NB_BERNE,
    -- ── Mesures stock ──────────────────────────────────────────────────────
    -- EMP_EFFECTIFS_ACTIFS : actifs dans le snapshot courant
    COUNT(DISTINCT CASE
        WHEN e.EMP_ETAT = 'A'
        THEN e.EMP_ID END)                        AS EMP_EFFECTIFS_ACTIFS,
    -- NB_ACTIFS_FIN_PERIODE : actifs à la fin du mois ID_TEMPS
    COUNT(DISTINCT CASE
        WHEN e.EMP_DATE_IMM <=
             LAST_DAY(TO_DATE(TO_CHAR(t.ID_TEMPS),'YYYYMMDD'))
          AND (e.EMP_ETAT != 'R'
            OR (e.EMP_ETAT = 'R'
               AND e.EMP_DATE_CHANGE_ETAT >
                   LAST_DAY(TO_DATE(TO_CHAR(t.ID_TEMPS),'YYYYMMDD'))))
        THEN e.EMP_ID END)                        AS NB_ACTIFS_FIN_PERIODE,
    -- ── Audit ──────────────────────────────────────────────────────────────
    :1                                            AS CLICHE

FROM DWH.FAIT_EMPLOYEUR                    e
-- Dimension temps : 1 ligne par mois depuis EMP_DATE_IMM
INNER JOIN DTM.DIM_TEMPS                   t
        ON t.ID_TEMPS =
           TO_NUMBER(TO_CHAR(TRUNC(e.EMP_DATE_IMM,'MM'),'YYYYMMDD'))
-- Géographie
LEFT JOIN DTM.DIM_SERVICE_PROVINCIAL       sp
       ON sp.SP_NO          = e.SP_NO
LEFT JOIN DTM.DIM_DEPARTEMENT              dpt
       ON dpt.DPT_NO        = e.DPT_NO
-- Forme juridique
LEFT JOIN DTM.DIM_FORME_JURIDIQUE          fj
       ON fj.FJ_CODE        = e.EMP_FORME_JURIDIQUE
-- Periodicité
LEFT JOIN DTM.DIM_PERIODICITE_VERSEMENT    dp
       ON dp.CODE_PERIODICITE = e.EMP_PERIODICITE
-- Tranche effectif
LEFT JOIN DTM.DIM_TRANCHE_EFFECTIF         tef
       ON e.EMP_NO_TR_DECLAR BETWEEN tef.INF AND tef.SUP

WHERE e.CLICHE          = :1
  AND e.EMP_DATE_IMM   IS NOT NULL

GROUP BY
    t.ID_TEMPS,
    dpt.PA_NO,
    dpt.PR_NO,
    dpt.DPT_NO,
    sp.DR_NO,
    e.SP_NO,
    e.EMP_REGIME,
    e.SA_NO,
    dp.ID_PERIODICITE,
    e.EMP_ETAT,
    tef.TEF_CODE,
    fj.FJ_CODE,
    fj.FJ_CODE_SUP,
    fj.SECT_CODE   -- CASE résolu dans le SELECT via EMP_cat_libelle
