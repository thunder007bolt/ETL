-- DTM_GRH_MOUVEMENT — Indicateurs de mouvement de personnel par exercice, type, nature, sens, qualification et sexe
-- :1 = CLICHE (MMYYYY) — filtre les snapshots DWH source et étiquette les lignes insérées
WITH
-- ── Mapping codes mouvements ──────────────────────────────────────────────
MVT_BASE AS (
    SELECT
        m.MVT_ID,
        m.PERS_ID,
        m.SITU_ID,
        TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM m.DATE_DEBUT)) || '1231') AS ID_TEMPS,
        CASE UPPER(TRIM(m.NATURE_MVT))
            WHEN 'DIS' THEN 'DISP'
            WHEN 'DET' THEN 'DETAC'
            ELSE UPPER(TRIM(m.NATURE_MVT))
        END                                                            AS CODE_TYPE_MVT
    FROM DWH.FAIT_GRH_MOUVEMENT m
    WHERE m.CLICHE       = :1
      AND m.DATE_DEBUT  IS NOT NULL
      AND m.VALIDE = 'O'
),
-- ── Jointure avec type mouvement ──────────────────────────────────────────
MVT_AVEC_TYPE AS (
    SELECT
        b.MVT_ID,
        b.PERS_ID,
        b.SITU_ID,
        b.ID_TEMPS,
        b.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT
            WHEN 'ENTREES' THEN 'PRISE'
            WHEN 'SORTIES' THEN 'CESSA'
            ELSE NULL
        END                                                            AS CODE_SENS_MVT
    FROM MVT_BASE b
    LEFT JOIN DTM.DIM_GRH_TYPE_MOUVEMENT tm
           ON tm.CODE_TYPE_MVT = b.CODE_TYPE_MVT
),
-- ── Enrichissement avec PERSONNE + SITUATION ──────────────────────────────
MVT_COMPLET AS (
    SELECT
        mv.MVT_ID,
        mv.PERS_ID,
        mv.ID_TEMPS,
        mv.CODE_TYPE_MVT,
        mv.CODE_NAT_MVT,
        mv.CODE_SENS_MVT,
        s.QUAL_CODE,
        p.SEXE
    FROM MVT_AVEC_TYPE mv
    LEFT JOIN DWH.FAIT_GRH_PERSONNE   p ON p.PERS_ID = mv.PERS_ID AND p.CLICHE = :1
    LEFT JOIN DWH.FAIT_GRH_SITUATION  s ON s.SITU_ID = mv.SITU_ID AND s.CLICHE = :1
)

SELECT
    mc.ID_TEMPS,
    mc.CODE_TYPE_MVT,
    mc.CODE_NAT_MVT,
    mc.CODE_SENS_MVT,
    mc.QUAL_CODE,
    mc.SEXE,
    CASE mc.SEXE
        WHEN '1' THEN 'Masculin'
        WHEN '2' THEN 'Féminin'
        ELSE NULL
    END                        AS LIBELLE_SEXE,
    COUNT(mc.MVT_ID)           AS NB_MOUVEMENTS,
    COUNT(DISTINCT mc.PERS_ID) AS NB_AGENTS,
    :1                         AS CLICHE
FROM MVT_COMPLET mc
GROUP BY
    mc.ID_TEMPS,
    mc.CODE_TYPE_MVT,
    mc.CODE_NAT_MVT,
    mc.CODE_SENS_MVT,
    mc.QUAL_CODE,
    mc.SEXE
