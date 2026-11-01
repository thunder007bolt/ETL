-- =============================================================================
-- T62 — Le personnel permanent formé dans l'exercice, par catégorie, sexe et
-- finalité de la formation
-- PROPOSITION du 01/11/2026, à valider par la DRH : traduit la règle G10 du
-- README de la GRH, avec la catégorie de G3. Le résultat ne retombe pas sur
-- l'annuaire de la CNSS 2024 (tableau 123) : la question QG4 demande d'où
-- viennent ses chiffres.
-- Sources : DTM.DIM_GRH_FORMATION (formations), USER_GRH.GRH_INSCRIPTION
--           (inscriptions, lues dans la base de l'application, comme
--           grh_formation.sql), DWH.FAIT_GRH_PERSONNE, FAIT_GRH_SITUATION.
-- Grain   : une ligne par catégorie de l'annuaire de la CNSS, et une ligne
--           TOTAL. Colonnes de la maquette de l'Annuaire (p. 51) : formés avec
--           promotion et sans promotion, par sexe ; puis hommes, femmes,
--           total ; et le détail : inscriptions, formés en perfectionnement et
--           en formation diplômante.
-- :1 = CLICHE (MMYYYY), saisi sans guillemets ; :2 = exercice (AAAA), par
-- exemple 2024.
-- Réglage (parametres), en attendant la réponse de la DRH (QG1) :
--   · categorie_05 : 'MOYEN' (annuaire de la CNSS 2024) ou 'SUPERIEUR' (Kaya
--     2017), comme dans la requête de l'effectif.
-- Règles (README de la GRH) :
--   · G10 Formé : agent inscrit à une formation commencée dans l'exercice
--     (atelier de Kaya 2017 : on retient la date de début), compté une fois,
--     quel que soit le nombre de ses formations. Toutes les formations
--     comptent, quel que soit leur statut (toutes sont au statut N, lot 8).
--   · G1 Permanent : agent CNSS (qualité A) dont la dernière situation
--     commencée au 31/12 de l'exercice est de nature PERMANENT. Il n'a pas à
--     être encore présent au 31/12 : un agent formé puis retraité dans
--     l'année compte.
--   · G3 Catégorie : celle de la même situation (CATEGORIE) : 01 et 02
--     employés ; 03 agents de maîtrise ; 04 cadres moyens ; 05 selon
--     categorie_05 ; 06 cadres supérieurs ; autre ou vide : sans catégorie.
--   · Finalité « avec promotion » : au moins une formation de l'exercice après
--     laquelle le classement de l'agent a changé (CLA_AVANT différent de
--     CLA_APRES sur l'inscription) ; sinon « sans promotion ». Proposition
--     BI : le classement n'est rempli que pour les formations diplômantes.
--   · Sexe : M hommes, F femmes.
-- Contrôle attendu (cliché 102026, exercice 2024 ; lot 19, Q113, avec la
-- situation au 31/12/2024) : 409 permanents formés (employés 81, maîtrise 96,
-- cadres moyens 181, cadres supérieurs 51), 524 inscriptions, 2 avec
-- promotion. Publié (tableau 123) : 480 (132, 142, 118, 88), dont 6 avec
-- promotion.
-- =============================================================================

WITH

parametres AS (
    SELECT TO_NUMBER(:2)                                  AS annee,
           TO_DATE(:2 || '0101', 'YYYYMMDD')              AS debut,
           TO_DATE(:2 || '1231', 'YYYYMMDD')              AS fin,
           -- catégorie 05 : 'MOYEN' (annuaire 2024) ou 'SUPERIEUR' (Kaya 2017)
           'MOYEN'                                        AS categorie_05
    FROM DUAL
),

-- ── 1. Les inscriptions aux formations commencées dans l'exercice ──────────
inscriptions AS (
    SELECT i.PERS_ID, i.INS_ID,
           NVL(TO_CHAR(f.CATEGORIE), '-')                         AS finalite,
           CASE WHEN i.CLA_APRES IS NOT NULL AND i.CLA_AVANT IS NOT NULL
                 AND TO_CHAR(i.CLA_APRES) <> TO_CHAR(i.CLA_AVANT)
                THEN 1 ELSE 0 END                                 AS promotion
    FROM USER_GRH.GRH_INSCRIPTION i
    JOIN DTM.DIM_GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
    CROSS JOIN parametres pr
    WHERE f.DATE_DEBUT >= pr.debut
      AND f.DATE_DEBUT <  pr.fin + 1
),

-- ── 2. Une ligne par agent formé ───────────────────────────────────────────
formes AS (
    SELECT PERS_ID,
           MAX(promotion)                                         AS promotion,
           COUNT(*)                                               AS nb_inscriptions,
           MAX(CASE WHEN finalite = 'PERF'  THEN 1 ELSE 0 END)    AS perfectionnement,
           MAX(CASE WHEN finalite = 'DIPLO' THEN 1 ELSE 0 END)    AS diplomante
    FROM inscriptions
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),

-- ── 3. La situation de chaque agent formé au 31/12 de l'exercice ───────────
situations AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE, TO_CHAR(s.CATEGORIE) AS categorie,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM DWH.FAIT_GRH_SITUATION s
    CROSS JOIN parametres pr
    WHERE s.CLICHE     = :1
      AND s.DATE_DEBUT <= pr.fin
      AND s.PERS_ID IN (SELECT PERS_ID FROM formes)
),

-- ── 4. Les permanents formés, avec leur catégorie (G1, G3) ─────────────────
base AS (
    SELECT fo.PERS_ID, p.SEXE, fo.promotion, fo.nb_inscriptions,
           fo.perfectionnement, fo.diplomante,
           CASE
               WHEN si.categorie IN ('01', '02') THEN 1
               WHEN si.categorie = '03'          THEN 2
               WHEN si.categorie = '04'          THEN 3
               WHEN si.categorie = '05'          THEN CASE pr.categorie_05
                                                          WHEN 'SUPERIEUR' THEN 4
                                                          ELSE 3 END
               WHEN si.categorie = '06'          THEN 4
               ELSE                                   5
           END                                                    AS rang
    FROM formes fo
    CROSS JOIN parametres pr
    JOIN situations si ON si.PERS_ID = fo.PERS_ID AND si.rn = 1
    JOIN DWH.FAIT_GRH_PERSONNE p ON p.PERS_ID = fo.PERS_ID
                                AND p.CLICHE  = :1
    WHERE si.NATURE_EMPLOIE = 'PERMANENT'
      AND TO_CHAR(p.QUALITE) = 'A'
),

-- ── 5. Une ligne par catégorie, puis le TOTAL ──────────────────────────────
comptes AS (
    SELECT rang,
           SUM(CASE WHEN promotion = 1 AND SEXE = 'M' THEN 1 ELSE 0 END) AS avec_promotion_h,
           SUM(CASE WHEN promotion = 1 AND SEXE = 'F' THEN 1 ELSE 0 END) AS avec_promotion_f,
           SUM(CASE WHEN promotion = 0 AND SEXE = 'M' THEN 1 ELSE 0 END) AS sans_promotion_h,
           SUM(CASE WHEN promotion = 0 AND SEXE = 'F' THEN 1 ELSE 0 END) AS sans_promotion_f,
           SUM(CASE WHEN SEXE = 'M' THEN 1 ELSE 0 END)                   AS hommes,
           SUM(CASE WHEN SEXE = 'F' THEN 1 ELSE 0 END)                   AS femmes,
           COUNT(*)                                                      AS total,
           SUM(nb_inscriptions)                                          AS inscriptions,
           SUM(perfectionnement)                                         AS perfectionnement,
           SUM(diplomante)                                               AS diplomante
    FROM base
    GROUP BY rang
),
lignes AS (
    SELECT * FROM comptes
    UNION ALL
    SELECT 9, SUM(avec_promotion_h), SUM(avec_promotion_f), SUM(sans_promotion_h),
           SUM(sans_promotion_f), SUM(hommes), SUM(femmes), SUM(total),
           SUM(inscriptions), SUM(perfectionnement), SUM(diplomante)
    FROM comptes
)

SELECT pr.annee                                                   AS ANNEE,
       l.rang                                                     AS RANG,
       CASE l.rang WHEN 1 THEN 'Employés'
                   WHEN 2 THEN 'Agents de maîtrise'
                   WHEN 3 THEN 'Cadres moyens'
                   WHEN 4 THEN 'Cadres supérieurs'
                   WHEN 5 THEN 'Sans catégorie'
                   ELSE        'TOTAL' END                        AS CATEGORIE,
       l.avec_promotion_h                                         AS AVEC_PROMOTION_H,
       l.avec_promotion_f                                         AS AVEC_PROMOTION_F,
       l.sans_promotion_h                                         AS SANS_PROMOTION_H,
       l.sans_promotion_f                                         AS SANS_PROMOTION_F,
       l.hommes                                                   AS HOMMES,
       l.femmes                                                   AS FEMMES,
       l.total                                                    AS TOTAL,
       l.inscriptions                                             AS INSCRIPTIONS,
       l.perfectionnement                                         AS FORMES_PERFECTIONNEMENT,
       l.diplomante                                               AS FORMES_DIPLOMANTE,
       :1                                                         AS CLICHE
FROM lignes l
CROSS JOIN parametres pr
ORDER BY l.rang
