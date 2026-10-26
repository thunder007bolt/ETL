-- DTM_GRH_FORMATION — Indicateurs de formation par exercice, catégorie, qualification et sexe
-- Tableaux : CIPRES T62 (agents formés) et partie formation de T64 (dépenses)
-- Sources  : DTM.DIM_GRH_FORMATION (formations), USER_GRH.GRH_INSCRIPTION (inscriptions,
--            lues dans la base de l'application), DWH.FAIT_GRH_PERSONNE (sexe)
-- Grain    : ID_TEMPS × CAT_FORM_CODE × QUAL_CODE × SEXE × QUALITE_EMPLOYE × NATURE_EMPLOI
-- :1 = CLICHE (MMYYYY) — filtre le snapshot DWH source et étiquette les lignes insérées
--
-- Règles :
--   - formations des exercices clos, quel que soit leur statut, rattachées à l'année
--     de leur fin (à défaut, de leur début) ;
--   - qualification : celle saisie sur l'inscription ; sexe : celui de la fiche agent ;
--   - coût d'une inscription : somme des 7 frais, un frais vide comptant pour zéro ;
--   - budget vide : le champ BUDGET d'une formation contient une année budgétaire,
--     pas un montant ;
--   - durée : somme des durées saisies sur les inscriptions, sans conversion d'unité ;
--   - type d'agent : qualité de la fiche agent (A agent CNSS, B fonction publique,
--     C affecté) et nature d'emploi de sa situation au 31/12 de l'exercice de la
--     formation, laissés en axes pour que la restitution filtre selon la DRH.
--
-- Étapes :
--   1. FORM_BASE      formations des exercices clos, rattachées à leur année
--   2. INS_BASE       inscriptions à ces formations, avec leur coût
--   3. SITU_FORM      nature d'emploi de chaque agent formé au 31/12 de l'exercice
--   4. INS_COMPLET    inscriptions complétées par l'exercice, la catégorie, le sexe
--                     et le type d'agent
--   5. SELECT final   comptages et sommes par exercice, catégorie, qualification,
--                     sexe et type d'agent
WITH
-- ── 1. Formations des exercices clos ─────────────────────────────────────────
-- Une ligne par formation, rattachée à l'année de sa fin (à défaut de son
-- début), quel que soit son statut. L'année en cours est exclue.
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
-- ── 2. Inscriptions avec leur coût ───────────────────────────────────────────
-- Une ligne par inscription à une formation retenue à l'étape 1.
INS_BASE AS (
    SELECT
        i.FORM_NO,
        i.PERS_ID,
        i.INS_ID,
        -- Qualification de l'agent au moment de l'inscription
        i.QUAL_CODE_INS                                    AS QUAL_CODE,
        -- Durée telle que saisie, sans conversion d'unité
        i.DUREE                                           AS DUREE,
        -- Coût : somme des 7 frais, un frais vide comptant pour zéro
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
-- ── 3. Nature d'emploi de l'agent formé au 31/12 de l'exercice ───────────────
-- Dernière situation commencée au plus tard le 31/12 de l'exercice de la
-- formation. Une ligne par agent et par exercice.
SITU_FORM AS (
    SELECT ID_TEMPS, PERS_ID, NATURE_EMPLOIE
    FROM (
        SELECT
            a.ID_TEMPS,
            s.PERS_ID,
            s.NATURE_EMPLOIE,
            -- RN = 1 : la situation la plus récente ; à date égale, la dernière saisie
            ROW_NUMBER() OVER (
                PARTITION BY a.ID_TEMPS, s.PERS_ID
                ORDER     BY s.DATE_DEBUT DESC, s.SITU_ID DESC
            )                                                     AS RN
        FROM (SELECT DISTINCT fb.ID_TEMPS, ib.PERS_ID
              FROM FORM_BASE fb
              INNER JOIN INS_BASE ib ON ib.FORM_NO = fb.FORM_NO) a
        INNER JOIN DWH.FAIT_GRH_SITUATION s
            ON  s.PERS_ID    = a.PERS_ID
            AND s.CLICHE     = :1
            AND s.DATE_DEBUT <= TO_DATE(TO_CHAR(a.ID_TEMPS), 'YYYYMMDD')
    )
    WHERE RN = 1
),
-- ── 4. Inscriptions avec l'exercice, la catégorie, le sexe et le type d'agent ──
-- Le sexe vient de la fiche de l'agent dans le cliché ; ses différentes
-- écritures (M, MASCULIN, H, 1, F, FEMININ, 2...) sont ramenées à M ou F.
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
        ib.COUT_REEL,
        -- Type d'agent : qualité de la fiche, nature d'emploi de la situation
        TO_CHAR(p.QUALITE)                                    AS QUALITE_EMPLOYE,
        sf.NATURE_EMPLOIE                                     AS NATURE_EMPLOI
    FROM FORM_BASE fb
    INNER JOIN INS_BASE ib     ON ib.FORM_NO  = fb.FORM_NO
    -- Jointure externe : une inscription dont l'agent manque au cliché reste
    -- comptée, avec un sexe vide
    LEFT  JOIN DWH.FAIT_GRH_PERSONNE p
                               ON p.PERS_ID   = ib.PERS_ID
                              AND p.CLICHE    = :1
    LEFT  JOIN SITU_FORM sf
                               ON sf.ID_TEMPS  = fb.ID_TEMPS
                              AND sf.PERS_ID   = ib.PERS_ID
)

-- ── 5. Comptages et sommes par exercice, catégorie, qualification, sexe et type ──
-- NB_STAGIAIRES compte chaque agent une fois par ligne : un agent inscrit dans
-- plusieurs catégories ou qualifications figure sur plusieurs lignes, le total
-- annuel ne s'obtient donc pas en les additionnant.
SELECT
    -- ── AXES ─────────────────────────────────────────────────────────
    ic.ID_TEMPS,
    ic.CAT_FORM_CODE,
    ic.QUAL_CODE,
    ic.SEXE,
    CASE ic.SEXE
        WHEN 'M' THEN 'Masculin'
        WHEN 'F' THEN 'Féminin'
        ELSE 'NON RENSEIGNE'
    END                                                   AS LIBELLE_SEXE,
    ic.QUALITE_EMPLOYE,
    ic.NATURE_EMPLOI,
    -- ── MESURES ──────────────────────────────────────────────────────
    COUNT(DISTINCT ic.PERS_ID)                            AS NB_STAGIAIRES,
    COUNT(ic.INS_ID)                                      AS NB_INSCRIPTIONS,
    SUM(ic.DUREE)                                         AS NB_JOURS_FORMATION,
    SUM(ic.COUT_REEL)                                     AS COUT_TOTAL,
    -- Vide : le champ BUDGET de la formation est une année budgétaire, pas un montant
    NULL                                                  AS BUDGET_FORMATION,
    -- ── CHARGEMENT ───────────────────────────────────────────────────
    SYSDATE                                               AS DATE_CHARGEMENT,
    :1                                                    AS CLICHE
FROM INS_COMPLET ic
GROUP BY
    ic.ID_TEMPS,
    ic.CAT_FORM_CODE,
    ic.QUAL_CODE,
    ic.SEXE,
    ic.QUALITE_EMPLOYE,
    ic.NATURE_EMPLOI
