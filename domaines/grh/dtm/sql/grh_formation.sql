-- DTM_GRH_FORMATION — Indicateurs de formation par exercice, catégorie, qualification et sexe
-- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
WITH
-- ── Toutes les formations terminées ───────────────────────────────────────
FORM_BASE AS (
    SELECT
        f.FORM_NO,
        TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
            COALESCE(f.DATE_FIN, f.DATE_DEBUT))) || '1231') AS ID_TEMPS,
        f.CATEGORIE                                         AS CAT_FORM_CODE,
        CASE
            WHEN REGEXP_LIKE(f.BUDGET, '^[0-9]+(\.[0-9]+)?$')
            THEN TO_NUMBER(f.BUDGET)
            ELSE NULL
        END                                               AS BUDGET
    FROM DTM.DIM_GRH_FORMATION f
    WHERE f.STATUT = 'T'
      AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) IS NOT NULL
),
-- ── Inscriptions avec détail des frais ────────────────────────────────────
INS_BASE AS (
    SELECT
        i.FORM_NO,
        i.PERS_ID,
        i.INS_ID,
        i.QUAL_CODE_INS                                    AS QUAL_CODE,
        i.DUREE                                           AS DUREE,
        i.FRAIS_INSCRIPTION
      + i.FRAIS_SCOLARITE
      + i.FRAIS_TRANSPORT
      + i.FRAIS_SEJOUR
      + i.FRAIS_DOSSIER
      + i.FRAIS_ASSURANCE
      + i.FRAIS_ACCESSOIRE                               AS COUT_REEL
    FROM USER_GRH.GRH_INSCRIPTION i
    WHERE i.FORM_NO IN (SELECT FORM_NO FROM FORM_BASE)
),
-- ── Jointure avec FAIT_GRH_PERSONNE pour le sexe ─────────────────────────
INS_COMPLET AS (
    SELECT
        fb.ID_TEMPS,
        fb.CAT_FORM_CODE,
        ib.QUAL_CODE,
        p.SEXE,
        ib.PERS_ID,
        ib.INS_ID,
        ib.DUREE,
        ib.COUT_REEL,
        fb.BUDGET
    FROM FORM_BASE fb
    INNER JOIN INS_BASE ib     ON ib.FORM_NO  = fb.FORM_NO
    LEFT  JOIN DWH.FAIT_GRH_PERSONNE p
                               ON p.PERS_ID   = ib.PERS_ID
                              AND p.CLICHE    = :1
)

SELECT
    ic.ID_TEMPS,
    ic.CAT_FORM_CODE,
    ic.QUAL_CODE,
    ic.SEXE,
    CASE ic.SEXE
        WHEN '1' THEN 'Masculin'
        WHEN '2' THEN 'Féminin'
        ELSE NULL
    END                                                   AS LIBELLE_SEXE,
    COUNT(DISTINCT ic.PERS_ID)                            AS NB_STAGIAIRES,
    COUNT(ic.INS_ID)                                      AS NB_INSCRIPTIONS,
    SUM(ic.DUREE)                                         AS NB_JOURS_FORMATION,
    SUM(ic.COUT_REEL)                                     AS COUT_TOTAL,
    SUM(ic.BUDGET)                                        AS BUDGET_FORMATION,
    :1                                                    AS CLICHE
FROM INS_COMPLET ic
GROUP BY
    ic.ID_TEMPS,
    ic.CAT_FORM_CODE,
    ic.QUAL_CODE,
    ic.SEXE
