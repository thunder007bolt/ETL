-- DTM_GRH_FORMATION — Indicateurs de formation par exercice, catégorie, qualification et sexe
-- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
--
-- Règles :
--   - formations des exercices clos, quel que soit leur statut, rattachées à l'année
--     de leur fin (à défaut, de leur début) ;
--   - qualification : celle saisie sur l'inscription ; sexe : celui de la fiche agent ;
--   - coût d'une inscription : somme des 7 frais, un frais vide comptant pour zéro ;
--   - budget vide : le champ BUDGET d'une formation contient une année budgétaire,
--     pas un montant ;
--   - durée : somme des durées saisies sur les inscriptions, sans conversion d'unité.
WITH
-- ── Formations des exercices clos ─────────────────────────────────────────
FORM_BASE AS (
    SELECT
        f.FORM_NO,
        TO_NUMBER(TO_CHAR(EXTRACT(YEAR FROM
            COALESCE(f.DATE_FIN, f.DATE_DEBUT))) || '1231') AS ID_TEMPS,
        f.CATEGORIE                                         AS CAT_FORM_CODE
    FROM DTM.DIM_GRH_FORMATION f
    WHERE COALESCE(f.DATE_FIN, f.DATE_DEBUT) IS NOT NULL
      AND EXTRACT(YEAR FROM COALESCE(f.DATE_FIN, f.DATE_DEBUT)) < EXTRACT(YEAR FROM SYSDATE)
),
-- ── Inscriptions avec détail des frais ────────────────────────────────────
INS_BASE AS (
    SELECT
        i.FORM_NO,
        i.PERS_ID,
        i.INS_ID,
        i.QUAL_CODE_INS                                    AS QUAL_CODE,
        i.DUREE                                           AS DUREE,
        NVL(i.FRAIS_INSCRIPTION, 0)
      + NVL(i.FRAIS_SCOLARITE, 0)
      + NVL(i.FRAIS_TRANSPORT, 0)
      + NVL(i.FRAIS_SEJOUR, 0)
      + NVL(i.FRAIS_DOSSIER, 0)
      + NVL(i.FRAIS_ASSURANCE, 0)
      + NVL(i.FRAIS_ACCESSOIRE, 0)                       AS COUT_REEL
    FROM USER_GRH.GRH_INSCRIPTION i
    WHERE i.FORM_NO IN (SELECT FORM_NO FROM FORM_BASE)
),
-- ── Jointure avec FAIT_GRH_PERSONNE pour le sexe ─────────────────────────
INS_COMPLET AS (
    SELECT
        fb.ID_TEMPS,
        fb.CAT_FORM_CODE,
        ib.QUAL_CODE,
        CASE
            WHEN UPPER(TRIM(TO_CHAR(p.SEXE))) IN ('M', 'MASCULIN', 'H', 'HOMME', '1')
            THEN 'M'
            WHEN UPPER(TRIM(TO_CHAR(p.SEXE))) IN ('F', 'FEMININ', 'FÉMININ', 'FEMME', '2')
            THEN 'F'
            ELSE NULL
        END                                                   AS SEXE,
        ib.PERS_ID,
        ib.INS_ID,
        ib.DUREE,
        ib.COUT_REEL
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
        WHEN 'M' THEN 'Masculin'
        WHEN 'F' THEN 'Féminin'
        ELSE 'NON RENSEIGNE'
    END                                                   AS LIBELLE_SEXE,
    COUNT(DISTINCT ic.PERS_ID)                            AS NB_STAGIAIRES,
    COUNT(ic.INS_ID)                                      AS NB_INSCRIPTIONS,
    SUM(ic.DUREE)                                         AS NB_JOURS_FORMATION,
    SUM(ic.COUT_REEL)                                     AS COUT_TOTAL,
    NULL                                                  AS BUDGET_FORMATION,
    SYSDATE                                               AS DATE_CHARGEMENT,
    :1                                                    AS CLICHE
FROM INS_COMPLET ic
GROUP BY
    ic.ID_TEMPS,
    ic.CAT_FORM_CODE,
    ic.QUAL_CODE,
    ic.SEXE
