-- =============================================================================
-- T58, T59, T61, T65 — L'effectif au 31/12 : catégorie, âge, sexe, domaine,
-- ancienneté et taux d'encadrement
-- PROPOSITION du 01/11/2026, à valider par la DRH : une seule requête pour les
-- quatre tableaux de l'effectif, qui traduit les règles G1 à G7 du README de la
-- GRH, contrôlées par l'annuaire de la CNSS 2024 (lots 17 à 20).
-- Sources : DWH.FAIT_GRH_PERSONNE, FAIT_GRH_SITUATION, FAIT_GRH_DECISION,
--           FAIT_GRH_DECISION_PERSONNE (copies des tables de USER_GRH).
-- Grain   : TABLEAU × PERIMETRE × ligne (rang). Colonnes : les quatre
--           catégories de l'annuaire de la CNSS par sexe, sans catégorie,
--           hommes, femmes, total, cadres, non-cadres, taux d'encadrement. Une
--           ligne TOTAL par tableau et par périmètre (sauf T65).
--           · T58 : une ligne par tranche d'âge, pour les permanents puis pour
--             les contractuels (Kaya 2017 : le tableau est scindé en deux) ;
--           · T59 : une ligne par domaine de l'Annuaire (permanents) ;
--           · T61 : une ligne par tranche d'ancienneté (permanents) ;
--           · T65 : une ligne par exercice, sur les cinq derniers (permanents).
-- :1 = CLICHE (MMYYYY), saisi sans guillemets ; :2 = exercice (AAAA), par
-- exemple 2024.
-- Réglages (parametres), en attendant les réponses de la DRH (README, QG1 à
-- QG7) ; une réponse change le réglage, pas la requête :
--   · categorie_05 : 'MOYEN' (annuaire de la CNSS 2024) ou 'SUPERIEUR' (Kaya
--     2017, liste des qualifications) — QG1 ;
--   · duree_contrat : durée d'un contrat temporaire, en mois (18) — QG5 ;
--   · disponibilite : 'PRESENT' (l'agent mis en disponibilité reste dans
--     l'effectif, comme aujourd'hui) ou 'HORS_EFFECTIF' (comme le tableau 121
--     de l'annuaire) — QG6.
-- Règles (README de la GRH) :
--   · G1 Permanent : agent CNSS (qualité A) dont la dernière situation
--     commencée au 31/12 est de nature PERMANENT.
--   · G2 Présent au 31/12 : entré au plus tard le 31/12 (embauche, à défaut
--     prise de service), et fiche ACTIVE ou première sortie après le 31/12.
--     Sortie : première décision 02, 34, 35, 36, 52 ou 54 non annulée par une
--     décision 40, à défaut la date de décès. En réglage HORS_EFFECTIF, un
--     agent dont la dernière décision 07 (mise en disponibilité) n'est pas
--     suivie d'une décision 18 (remise en activité) n'est pas présent.
--   · G7 Contractuel : agent CNSS de nature TEMPORAIRE au 31/12, entré au
--     plus tard le 31/12, dont le dernier engagement temporaire (décision 50
--     non annulée) a moins de duree_contrat mois, sans décision de sortie
--     après cet engagement : une fin de contrat suivie d'un nouveau contrat
--     n'est pas une sortie.
--   · G3 Catégorie : catégorie de la dernière situation (CATEGORIE, grille de
--     la CNSS) : 01 et 02 employés ; 03 agents de maîtrise ; 04 cadres
--     moyens ; 05 selon categorie_05 ; 06 cadres supérieurs ; autre code ou
--     vide : sans catégorie.
--   · G4 Âge : années pleines au 31/12 ; tranches de l'Annuaire (moins de 20,
--     20-24… 55-59, 60 et plus). Date de naissance absente ou âge hors de 0 à
--     119 ans : « Âge inconnu ».
--   · Sexe : M hommes, F femmes ; un autre code ne compte que dans le total.
--   · G5 Ancienneté : années pleines au 31/12 depuis l'entrée ; tranches
--     moins de 1, 1-4, 5-9… 35-39, 40 et plus.
--   · T59 Domaine : domaine de la dernière situation (CODE_DOMAINE_ACT) rangé
--     dans les 7 domaines de l'Annuaire : IMM immatriculation, REC
--     recouvrement, CONT contentieux, PRS prestations, PREV prévention des
--     risques professionnels, ACT action sanitaire et sociale ; PIL pilotage,
--     CPT comptabilité, AUT et les autres : autres domaines. Proposition BI :
--     les libellés sont à confirmer (lot 16, Q95 ; question 11 du
--     questionnaire CIPRES). T59 n'est pas publié par l'annuaire.
--   · G6 Taux d'encadrement : cadres (moyens et supérieurs) ÷ total × 100.
-- Contrôle attendu (cliché 102026, exercice 2024 ; à quelques agents près si
-- le cliché diffère de la copie de USER_GRH du 18/02/2026), réglages par
-- défaut :
--   · T58 permanents : 742 (publié 758) ; employés 129 H / 44 F (133 / 44),
--     maîtrise 92 / 115 (94 / 114), cadres moyens 139 / 165 (143 / 168),
--     cadres supérieurs 40 / 18 (42 / 20) ; par âge 12, 28, 70, 115, 139, 155,
--     151, 72 de 20-24 à 55-59 (lot 19, Q111) ;
--   · T58 contractuels : 46 publiés ; le lot 19 (Q114) en trouve 46 sans les
--     contrats renouvelés après une fin de contrat, que G7 ajoute (4 au plus) ;
--   · T61 : moins d'un an 101 (103), 10-14 ans 224 (232) (lot 17, Q100) ;
--   · T65 : 42,3 %, 42,6 %, 42,7 %, 42,3 % de 2020 à 2023, 48,8 % en 2024 ;
--     publié 41,2 à 41,4 % puis 49,2 %. L'écart de 2020 à 2023 vient des 84
--     engagés de 2019 sans date de départ (G11, lot 19, Q112).
-- =============================================================================

WITH

parametres AS (
    SELECT TO_NUMBER(:2)    AS annee,
           -- catégorie 05 : 'MOYEN' (annuaire 2024) ou 'SUPERIEUR' (Kaya 2017)
           'MOYEN'          AS categorie_05,
           -- durée d'un contrat temporaire, en mois
           18               AS duree_contrat,
           -- agent en disponibilité : 'PRESENT' ou 'HORS_EFFECTIF'
           'PRESENT'        AS disponibilite
    FROM DUAL
),

-- ── 1. Les exercices : l'exercice demandé et les quatre d'avant (T65) ───────
exercices AS (
    SELECT pr.annee - 5 + LEVEL                                   AS annee,
           TO_DATE((pr.annee - 5 + LEVEL) || '1231', 'YYYYMMDD')  AS fin
    FROM parametres pr
    CONNECT BY LEVEL <= 5
),

-- ── 2. Les décisions de sortie et la première sortie de chaque agent (G2) ───
decisions_sortie AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt
    FROM DWH.FAIT_GRH_DECISION d
    LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp
           ON dp.DECI_NO = d.DECI_NO
          AND dp.CLICHE  = :1
    WHERE d.CLICHE = :1
      AND d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
      -- sans les décisions annulées par une décision 40 qui les cite
      AND NOT EXISTS (SELECT 1 FROM DWH.FAIT_GRH_DECISION a
                      WHERE a.CLICHE    = :1
                        AND a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    UNION ALL
    SELECT PERS_ID, DATE_DECES
    FROM DWH.FAIT_GRH_PERSONNE
    WHERE CLICHE = :1
      AND DATE_DECES IS NOT NULL
),
sorties AS (
    SELECT PERS_ID, MIN(dt) AS dt_sortie
    FROM decisions_sortie
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),

-- ── 3. Les engagements temporaires (G7) et les disponibilités (G2) ─────────
-- 50 engagement temporaire, 07 mise en disponibilité, 18 remise en activité,
-- non annulées ; pour chaque agent et chaque exercice, la dernière de chaque
-- sorte au plus tard le 31/12
decisions_position AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.CDEC_CODE, d.DATE_VIGUEUR AS dt
    FROM DWH.FAIT_GRH_DECISION d
    LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp
           ON dp.DECI_NO = d.DECI_NO
          AND dp.CLICHE  = :1
    WHERE d.CLICHE = :1
      AND d.CDEC_CODE IN ('50', '07', '18')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM DWH.FAIT_GRH_DECISION a
                      WHERE a.CLICHE    = :1
                        AND a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
positions AS (
    SELECT e.annee, e.fin, dpo.PERS_ID,
           MAX(CASE WHEN dpo.CDEC_CODE = '50' THEN dpo.dt END)    AS dt_engagement,
           MAX(CASE WHEN dpo.CDEC_CODE = '07' THEN dpo.dt END)    AS dt_disponibilite,
           MAX(CASE WHEN dpo.CDEC_CODE = '18' THEN dpo.dt END)    AS dt_remise
    FROM exercices e
    JOIN decisions_position dpo ON dpo.dt <= e.fin
    WHERE dpo.PERS_ID IS NOT NULL
    GROUP BY e.annee, e.fin, dpo.PERS_ID
),
-- contractuels sortis après leur dernier engagement
sorties_contrat AS (
    SELECT DISTINCT po.annee, po.PERS_ID
    FROM positions po
    JOIN decisions_sortie ds ON ds.PERS_ID = po.PERS_ID
                            AND ds.dt >  po.dt_engagement
                            AND ds.dt <= po.fin
),

-- ── 4. Chaque agent CNSS au 31/12 de chaque exercice ────────────────────────
situations AS (
    SELECT e.annee, e.fin, s.PERS_ID, s.NATURE_EMPLOIE, s.CATEGORIE, s.CODE_DOMAINE_ACT,
           ROW_NUMBER() OVER (PARTITION BY e.annee, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM exercices e
    JOIN DWH.FAIT_GRH_SITUATION s ON s.CLICHE     = :1
                                 AND s.DATE_DEBUT <= e.fin
),
agents AS (
    SELECT si.annee, si.fin, p.PERS_ID, p.SEXE, p.DATE_NAISS, p.ETAT_EMPLOYE,
           COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree,
           so.dt_sortie,
           si.NATURE_EMPLOIE,
           TO_CHAR(si.CATEGORIE)                                  AS categorie,
           TO_CHAR(si.CODE_DOMAINE_ACT)                           AS domaine
    FROM situations si
    JOIN DWH.FAIT_GRH_PERSONNE p ON p.PERS_ID = si.PERS_ID
                                AND p.CLICHE  = :1
    LEFT JOIN sorties so ON so.PERS_ID = si.PERS_ID
    WHERE si.rn = 1
      AND TO_CHAR(p.QUALITE) = 'A'
),

-- ── 5. Le périmètre (G1, G2, G7) et les axes (G3, G4, G5, domaine) ─────────
base AS (
    SELECT a.annee, a.PERS_ID, a.SEXE,
           CASE
               WHEN a.NATURE_EMPLOIE = 'PERMANENT'
                AND a.date_entree <= a.fin
                AND (a.ETAT_EMPLOYE = 'ACTIF' OR a.dt_sortie > a.fin)
                AND NOT (pr.disponibilite = 'HORS_EFFECTIF'
                         AND NVL(po.dt_disponibilite, DATE '1900-01-01')
                             > NVL(po.dt_remise, DATE '1900-01-01'))
               THEN 'Permanents'
               WHEN a.NATURE_EMPLOIE = 'TEMPORAIRE'
                AND a.date_entree <= a.fin
                AND po.dt_engagement > ADD_MONTHS(a.fin, -pr.duree_contrat)
                AND sc.PERS_ID IS NULL
               THEN 'Contractuels'
           END                                                    AS perimetre,
           CASE
               WHEN a.categorie IN ('01', '02') THEN 'EMPLOYES'
               WHEN a.categorie = '03'          THEN 'MAITRISE'
               WHEN a.categorie = '04'          THEN 'CADRES_MOYENS'
               WHEN a.categorie = '05'          THEN CASE pr.categorie_05
                                                         WHEN 'SUPERIEUR' THEN 'CADRES_SUP'
                                                         ELSE 'CADRES_MOYENS' END
               WHEN a.categorie = '06'          THEN 'CADRES_SUP'
               ELSE 'SANS'
           END                                                    AS categorie,
           -- âge en années pleines au 31/12 (G4), rang de la tranche
           CASE
               WHEN a.DATE_NAISS IS NULL
                 OR FLOOR((a.fin - a.DATE_NAISS) / 365.25) NOT BETWEEN 0 AND 119 THEN 99
               WHEN FLOOR((a.fin - a.DATE_NAISS) / 365.25) < 20  THEN 0
               WHEN FLOOR((a.fin - a.DATE_NAISS) / 365.25) >= 60 THEN 60
               ELSE FLOOR(FLOOR((a.fin - a.DATE_NAISS) / 365.25) / 5) * 5
           END                                                    AS rang_age,
           -- ancienneté en années pleines au 31/12 (G5), rang de la tranche
           CASE
               WHEN FLOOR((a.fin - a.date_entree) / 365.25) < 1   THEN 0
               WHEN FLOOR((a.fin - a.date_entree) / 365.25) < 5   THEN 1
               WHEN FLOOR((a.fin - a.date_entree) / 365.25) >= 40 THEN 40
               ELSE FLOOR(FLOOR((a.fin - a.date_entree) / 365.25) / 5) * 5
           END                                                    AS rang_anciennete,
           -- domaine de l'Annuaire (T59), rang de la ligne
           CASE a.domaine
               WHEN 'IMM'  THEN 1
               WHEN 'REC'  THEN 2
               WHEN 'CONT' THEN 3
               WHEN 'PRS'  THEN 4
               WHEN 'PREV' THEN 5
               WHEN 'ACT'  THEN 6
               ELSE             7
           END                                                    AS rang_domaine
    FROM agents a
    CROSS JOIN parametres pr
    LEFT JOIN positions po       ON po.annee = a.annee AND po.PERS_ID = a.PERS_ID
    LEFT JOIN sorties_contrat sc ON sc.annee = a.annee AND sc.PERS_ID = a.PERS_ID
),

-- ── 6. Une ligne par agent et par ligne de chaque tableau ──────────────────
detail AS (
    -- T58 : l'exercice demandé, permanents et contractuels, par tranche d'âge
    SELECT 'T58' AS tableau, b.perimetre, b.rang_age AS rang,
           CASE b.rang_age WHEN 0  THEN 'Moins de 20 ans'
                           WHEN 60 THEN '60 ans et plus'
                           WHEN 99 THEN 'Âge inconnu'
                           ELSE b.rang_age || ' - ' || (b.rang_age + 4) || ' ans' END AS ligne,
           b.categorie, b.SEXE
    FROM base b CROSS JOIN parametres pr
    WHERE b.annee = pr.annee AND b.perimetre IS NOT NULL
    UNION ALL
    -- T59 : l'exercice demandé, permanents, par domaine
    SELECT 'T59', b.perimetre, b.rang_domaine,
           CASE b.rang_domaine WHEN 1 THEN 'Immatriculation'
                               WHEN 2 THEN 'Recouvrement'
                               WHEN 3 THEN 'Contentieux'
                               WHEN 4 THEN 'Prestations'
                               WHEN 5 THEN 'Prévention des risques professionnels'
                               WHEN 6 THEN 'Action sanitaire et sociale'
                               ELSE        'Autres domaines' END,
           b.categorie, b.SEXE
    FROM base b CROSS JOIN parametres pr
    WHERE b.annee = pr.annee AND b.perimetre = 'Permanents'
    UNION ALL
    -- T61 : l'exercice demandé, permanents, par tranche d'ancienneté
    SELECT 'T61', b.perimetre, b.rang_anciennete,
           CASE b.rang_anciennete WHEN 0  THEN 'Moins d''un an'
                                  WHEN 1  THEN '1 - 4 ans'
                                  WHEN 40 THEN '40 ans et plus'
                                  ELSE b.rang_anciennete || ' - '
                                       || (b.rang_anciennete + 4) || ' ans' END,
           b.categorie, b.SEXE
    FROM base b CROSS JOIN parametres pr
    WHERE b.annee = pr.annee AND b.perimetre = 'Permanents'
    UNION ALL
    -- T65 : les cinq exercices, permanents
    SELECT 'T65', b.perimetre, b.annee, TO_CHAR(b.annee), b.categorie, b.SEXE
    FROM base b
    WHERE b.perimetre = 'Permanents'
),

-- ── 7. Les comptages par ligne, puis le TOTAL de chaque tableau ─────────────
comptes AS (
    SELECT tableau, perimetre, rang, ligne,
           SUM(CASE WHEN categorie = 'EMPLOYES'      AND SEXE = 'M' THEN 1 ELSE 0 END) AS employes_h,
           SUM(CASE WHEN categorie = 'EMPLOYES'      AND SEXE = 'F' THEN 1 ELSE 0 END) AS employes_f,
           SUM(CASE WHEN categorie = 'MAITRISE'      AND SEXE = 'M' THEN 1 ELSE 0 END) AS maitrise_h,
           SUM(CASE WHEN categorie = 'MAITRISE'      AND SEXE = 'F' THEN 1 ELSE 0 END) AS maitrise_f,
           SUM(CASE WHEN categorie = 'CADRES_MOYENS' AND SEXE = 'M' THEN 1 ELSE 0 END) AS cadres_moyens_h,
           SUM(CASE WHEN categorie = 'CADRES_MOYENS' AND SEXE = 'F' THEN 1 ELSE 0 END) AS cadres_moyens_f,
           SUM(CASE WHEN categorie = 'CADRES_SUP'    AND SEXE = 'M' THEN 1 ELSE 0 END) AS cadres_sup_h,
           SUM(CASE WHEN categorie = 'CADRES_SUP'    AND SEXE = 'F' THEN 1 ELSE 0 END) AS cadres_sup_f,
           SUM(CASE WHEN categorie = 'SANS' THEN 1 ELSE 0 END)                         AS sans_categorie,
           SUM(CASE WHEN SEXE = 'M' THEN 1 ELSE 0 END)                                 AS hommes,
           SUM(CASE WHEN SEXE = 'F' THEN 1 ELSE 0 END)                                 AS femmes,
           COUNT(*)                                                                    AS total,
           SUM(CASE WHEN categorie IN ('CADRES_MOYENS', 'CADRES_SUP') THEN 1 ELSE 0 END) AS cadres,
           SUM(CASE WHEN categorie IN ('EMPLOYES', 'MAITRISE') THEN 1 ELSE 0 END)      AS non_cadres
    FROM detail
    GROUP BY tableau, perimetre, rang, ligne
),
lignes AS (
    SELECT * FROM comptes
    UNION ALL
    -- une ligne TOTAL par tableau et par périmètre ; T65 compte des exercices
    -- différents, qui ne s'additionnent pas
    SELECT tableau, perimetre, 999, 'TOTAL',
           SUM(employes_h), SUM(employes_f), SUM(maitrise_h), SUM(maitrise_f),
           SUM(cadres_moyens_h), SUM(cadres_moyens_f), SUM(cadres_sup_h), SUM(cadres_sup_f),
           SUM(sans_categorie), SUM(hommes), SUM(femmes), SUM(total),
           SUM(cadres), SUM(non_cadres)
    FROM comptes
    WHERE tableau <> 'T65'
    GROUP BY tableau, perimetre
)

SELECT pr.annee                                                   AS ANNEE,
       l.tableau                                                  AS TABLEAU,
       l.perimetre                                                AS PERIMETRE,
       l.rang                                                     AS RANG,
       l.ligne                                                    AS LIGNE,
       l.employes_h                                               AS EMPLOYES_H,
       l.employes_f                                               AS EMPLOYES_F,
       l.maitrise_h                                               AS MAITRISE_H,
       l.maitrise_f                                               AS MAITRISE_F,
       l.cadres_moyens_h                                          AS CADRES_MOYENS_H,
       l.cadres_moyens_f                                          AS CADRES_MOYENS_F,
       l.cadres_sup_h                                             AS CADRES_SUP_H,
       l.cadres_sup_f                                             AS CADRES_SUP_F,
       l.sans_categorie                                           AS SANS_CATEGORIE,
       l.hommes                                                   AS HOMMES,
       l.femmes                                                   AS FEMMES,
       l.total                                                    AS TOTAL,
       l.cadres                                                   AS CADRES,
       l.non_cadres                                               AS NON_CADRES,
       ROUND(100 * l.cadres / NULLIF(l.total, 0), 1)              AS TAUX_ENCADREMENT_PCT,
       :1                                                         AS CLICHE
FROM lignes l
CROSS JOIN parametres pr
ORDER BY l.tableau,
         CASE l.perimetre WHEN 'Permanents' THEN 1 ELSE 2 END,
         l.rang
