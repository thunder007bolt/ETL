-- ============================================================
-- DIM_ASSURE — Requête d'extraction
-- Sources  : INDIVIDU (principale) ⟕ BENEFICIAIRE (rôle)
-- Cible    : DWH.DIM_ASSURE
-- Grain    : 1 ligne par individu (IND_ID unique)
-- ============================================================
SELECT
    -- ── Identifiants sources ─────────────────────────────────
    i.IND_ID,
    i.TR_ID,
    i.IND_ID_ECNSS                                                AS ID_ECNSS,

    -- ── Identité ─────────────────────────────────────────────
    i.IND_NOM                                                     AS NOM,
    i.IND_PRENOM                                                  AS PRENOM,
    i.IND_NOM_EPOUX                                               AS NOM_EPOUX,

    -- ── Démographie ──────────────────────────────────────────
    i.IND_DATE_NAISSANCE                                          AS DATE_NAISSANCE,
    -- AGE figé à la date d'extraction ; pour l'âge au 31/12/N → calculer dans OBIEE
    -- NULL si date_naissance corrompue (age < 0 ou > 150) → évite ORA-01438 sur NUMBER(3,0)
    CASE
        WHEN TRUNC(MONTHS_BETWEEN(SYSDATE, i.IND_DATE_NAISSANCE) / 12) BETWEEN 0 AND 150
        THEN TRUNC(MONTHS_BETWEEN(SYSDATE, i.IND_DATE_NAISSANCE) / 12)
        ELSE NULL
    END                                                           AS AGE,
    i.IND_SEXE                                                    AS SEXE,

    -- ── Situation personnelle ────────────────────────────────
    i.IND_SITUATION_MATRI                                         AS SITUATION_MATRIMONIALE,
    i.IND_TYPE_ENFANT                                             AS TYPE_ENFANT,
    i.PA_NO,

    -- ── Statut et cycle de vie ───────────────────────────────
    CASE WHEN i.IND_DATE_DECES IS NOT NULL THEN 'O' ELSE 'N' END AS EST_DECEDE,
    i.IND_DATE_DECES                                              AS DATE_DECES,
    NVL(i.IND_ACTIF,  'O')                                       AS EST_ACTIF,
    NVL(i.IND_VERIFIE,'N')                                       AS EST_VERIFIE,

    -- ── Rôle bénéficiaire ─────────────────────────────────────
    -- BEN_LN_TYPE = 'M' exclu (= assuré lui-même, géré côté Python)
    -- Si un individu est bénéficiaire pour plusieurs assurés,
    -- on conserve le premier rôle alphabétiquement (MIN).
    b.BEN_LN_TYPE                                                 AS ROLE_BENEFICIAIRE,

    -- ── Audit ETL ────────────────────────────────────────────
    SYSDATE                                                       AS DATE_CHARGEMENT,
    i.IND_DATE_UPDATE                                             AS DATE_MAJ

FROM INDIVIDU i
LEFT JOIN (
    SELECT
        IND_ID_BENEF,
        MIN(BEN_LN_TYPE) AS BEN_LN_TYPE   -- 1 rôle max par individu bénéficiaire
    FROM   BENEFICIAIRE
    WHERE  BEN_LN_TYPE != 'M'             -- 'M' = assuré lui-même, exclu
    GROUP BY IND_ID_BENEF
) b ON b.IND_ID_BENEF = i.IND_ID
