-- =============================================================================
-- CONTRÔLE 1 — La requête de l'effectif (T58, T59, T61, T65), 17/11/2026
-- Requête contrôlée : analyses/grh/t58_t65_effectif_requete_proposee.sql (jamais lancée
-- dans sa version du 08/11/2026).
-- Exécution : mode script (F5) dans SQL Developer, rien à saisir. Comptages seulement.
-- Lit LA SOURCE (USER_GRH), pas l'entrepôt : la requête proposée lit DWH et DTM, sa
-- copie ci-dessous lit les mêmes tables dans USER_GRH, sans condition de cliché.
-- Construit par analyses/grh/controles/construire.py à partir de la requête proposée :
-- ne pas modifier ce fichier à la main, relancer le script.
-- Coller le résultat sous la ligne « Résultat » de chaque contrôle.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- C1 — T58 au 31/12/2024 : par nature, catégorie et sexe
-- Attendu  : environ 741 permanents (publié 758) : employés 129 H / 44 F, maîtrise 92 /
--            115, cadres moyens 139 / 165, cadres supérieurs 40 / 18, à quelques agents
--            près (6 agents en disponibilité en moins). Temporaires : plus de 46
--            (publié), faute de fin de contrat saisie (points 20 et 15 de la v2).
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT NATURE_EMPLOI, GROUPE_CATEGORIE, SEXE, SUM(NB_AGENTS) AS agents
FROM (
    WITH
    parametres AS (
        SELECT
               'ANNUELLE'              AS periodicite,
               'PRISE_SERVICE'         AS ordre_date_anciennete,
               'OUI'                   AS retirer_interruptions,
               'PRESENT'               AS fin_contrat_inconnue,
               'HORS_EFFECTIF'         AS disponibilite,
               'DECISIONS'             AS inactifs
        FROM DUAL
    ),
    decisions_depart AS (
        SELECT '02' AS code, 'admission à la retraite' AS libelle FROM DUAL UNION ALL
        SELECT '34', 'liquidation des droits après décès'      FROM DUAL UNION ALL
        SELECT '35', 'fin de contrat'                          FROM DUAL UNION ALL
        SELECT '36', 'démission'                               FROM DUAL UNION ALL
        SELECT '52', 'rupture du contrat'                      FROM DUAL UNION ALL
        SELECT '54', 'licenciement'                            FROM DUAL
    ),
    codes_decision AS (
        SELECT '40' AS annulation,
               '50' AS engagement_temporaire,
               '51' AS engagement_permanent,
               '07' AS mise_en_disponibilite,
               '18' AS remise_en_activite
        FROM DUAL
    ),
    regroupement_categorie AS (
        SELECT '01' AS code, 'EMPLOYES' AS groupe, 'NON' AS cadre FROM DUAL UNION ALL
        SELECT '02', 'EMPLOYES',      'NON'                       FROM DUAL UNION ALL
        SELECT '03', 'MAITRISE',      'NON'                       FROM DUAL UNION ALL
        SELECT '04', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
        SELECT '05', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
        SELECT '06', 'CADRES_SUP',    'OUI'                       FROM DUAL UNION ALL
        SELECT 'HC', 'CADRES_SUP',    'OUI'                       FROM DUAL
    ),
    correspondance_domaine AS (
        SELECT CAST(NULL AS VARCHAR2(12)) AS UA_CODE, CAST(NULL AS VARCHAR2(60)) AS DOMAINE
        FROM DUAL WHERE 1 = 0
    ),
    annees AS (
        SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS annee
        FROM USER_GRH.GRH_SITUATION s
        WHERE 1 = 1
          AND s.DATE_DEBUT IS NOT NULL
          AND EXTRACT(YEAR FROM s.DATE_DEBUT) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE) - 1
    ),
    dates AS (
        SELECT TO_DATE(TO_CHAR(annee) || '1231', 'YYYYMMDD')            AS date_arrete
        FROM annees
        UNION
        SELECT LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE, 'YYYY'), LEVEL - 13))
        FROM parametres pr
        WHERE pr.periodicite = 'MENSUELLE'
        CONNECT BY LEVEL <= 11 + EXTRACT(MONTH FROM SYSDATE)
    ),
    personnes AS (
        SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS, NVL(p.ETAT_EMPLOYE, '-') AS etat,
               TO_CHAR(p.QUALITE)                                     AS qualite,
               COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree,
               CASE pr.ordre_date_anciennete
                   WHEN 'EMBAUCHE' THEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
                   ELSE                 COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
               END                                                    AS debut_anciennete
        FROM USER_GRH.GRH_PERSONNE p
        CROSS JOIN parametres pr
        WHERE 1 = 1
    ),
    decisions AS (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               TO_CHAR(d.CDEC_CODE)       AS code,
               d.DATE_VIGUEUR             AS date_decision
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp
               ON dp.DECI_NO = d.DECI_NO
              AND 1 = 1
        CROSS JOIN codes_decision cd
        WHERE 1 = 1
          AND d.DATE_VIGUEUR IS NOT NULL
          AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
               OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                           cd.mise_en_disponibilite, cd.remise_en_activite))
          AND NOT EXISTS (SELECT 1
                          FROM USER_GRH.GRH_DECISION a
                          WHERE 1 = 1
                            AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    ),
    departs AS (
        SELECT PERS_ID, date_decision AS date_depart
        FROM decisions
        WHERE PERS_ID IS NOT NULL
          AND code IN (SELECT code FROM decisions_depart)
        UNION ALL
        SELECT PERS_ID, DATE_DECES
        FROM USER_GRH.GRH_PERSONNE
        WHERE 1 = 1
          AND DATE_DECES IS NOT NULL
    ),
    premier_depart AS (
        SELECT PERS_ID, MIN(date_depart) AS date_depart
        FROM departs
        GROUP BY PERS_ID
    ),
    engagements AS (
        SELECT e.PERS_ID, e.date_decision AS debut,
               LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                           ORDER BY e.date_decision) AS debut_suivant
        FROM decisions e
        CROSS JOIN codes_decision cd
        WHERE e.PERS_ID IS NOT NULL
          AND e.code = cd.engagement_temporaire
    ),
    contrats AS (
        SELECT g.PERS_ID, g.debut,
               LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                     NVL(g.debut_suivant, DATE '9999-12-31'))         AS fin
        FROM engagements g
        LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                             AND dep.date_depart > g.debut
        GROUP BY g.PERS_ID, g.debut, g.debut_suivant
    ),
    temporaires_sous_contrat AS (
        SELECT da.date_arrete, c.PERS_ID,
               MAX(CASE WHEN c.fin = DATE '9999-12-31' THEN 1 ELSE 0 END) AS fin_inconnue
        FROM dates da
        JOIN contrats c ON c.debut       <= da.date_arrete
                       AND da.date_arrete <  c.fin
        GROUP BY da.date_arrete, c.PERS_ID
    ),
    positions AS (
        SELECT da.date_arrete, d.PERS_ID,
               MAX(CASE WHEN d.code = cd.mise_en_disponibilite THEN d.date_decision END) AS date_dispo,
               MAX(CASE WHEN d.code = cd.remise_en_activite    THEN d.date_decision END) AS date_remise
        FROM dates da
        JOIN decisions d ON d.date_decision <= da.date_arrete
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code IN (cd.mise_en_disponibilite, cd.remise_en_activite)
        GROUP BY da.date_arrete, d.PERS_ID
    ),
    interruptions AS (
        SELECT d.PERS_ID, d.date_decision AS debut,
               (SELECT MIN(r.date_decision)
                FROM decisions r, codes_decision c2
                WHERE r.PERS_ID       = d.PERS_ID
                  AND r.code          = c2.remise_en_activite
                  AND r.date_decision > d.date_decision)          AS fin
        FROM decisions d
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.mise_en_disponibilite
        UNION ALL
        SELECT x.PERS_ID, x.debut, x.fin
        FROM (SELECT dep.PERS_ID, dep.date_depart AS debut,
                     (SELECT MIN(e.date_decision)
                      FROM decisions e, codes_decision c3
                      WHERE e.PERS_ID       = dep.PERS_ID
                        AND e.code IN (c3.engagement_temporaire, c3.engagement_permanent)
                        AND e.date_decision > dep.date_depart)    AS fin
              FROM departs dep) x
        WHERE x.fin IS NOT NULL
    ),
    interruptions_a_date AS (
        SELECT da.date_arrete, i.PERS_ID,
               SUM(GREATEST(0, LEAST(NVL(i.fin, da.date_arrete), da.date_arrete)
                               - GREATEST(i.debut, p.debut_anciennete))) AS jours
        FROM dates da
        JOIN interruptions i ON i.debut < da.date_arrete
        JOIN personnes p     ON p.PERS_ID = i.PERS_ID
        GROUP BY da.date_arrete, i.PERS_ID
    ),
    situations AS (
        SELECT da.date_arrete, s.PERS_ID,
               s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
               s.STATUT_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
               TO_CHAR(s.CATEGORIE)                                   AS categorie,
               ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM dates da
        JOIN USER_GRH.GRH_SITUATION s ON 1 = 1
                                     AND s.DATE_DEBUT <= da.date_arrete
    ),
    base AS (
        SELECT da.date_arrete, p.PERS_ID, p.SEXE,
               p.qualite                                              AS QUALITE_EMPLOYE,
               si.nature                                              AS NATURE_EMPLOI,
               si.QUAL_CODE,
               si.CODE_DOMAINE_ACT                                    AS CODE_DOMAINE,
               si.UA_CODE, si.LIEU_ID, si.FNCT_CODE, si.STATUT_EMPLOYE,
               si.categorie                                           AS CATEGORIE,
               NVL(rc.groupe, 'SANS_CATEGORIE')                       AS GROUPE_CATEGORIE,
               NVL(rc.cadre, 'NON')                                   AS cadre,
               NVL(cdo.DOMAINE, 'Domaine non renseigné')              AS DOMAINE_ACTIVITE,
               CASE WHEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
                         BETWEEN 0 AND 119
                    THEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
               END                                                    AS age,
               GREATEST(FLOOR((da.date_arrete - p.debut_anciennete
                               - CASE pr.retirer_interruptions
                                     WHEN 'OUI' THEN NVL(ia.jours, 0) ELSE 0 END)
                              / 365.25), 0)                           AS anciennete,
               CASE WHEN pr.inactifs = 'STATUT'
                    THEN CASE WHEN si.STATUT_EMPLOYE IN ('B','D','E','F','H')
                              THEN 1 ELSE 0 END
                    ELSE CASE WHEN NVL(po.date_dispo,  DATE '1900-01-01')
                                 > NVL(po.date_remise, DATE '1900-01-01')
                              THEN 1 ELSE 0 END
               END                                                    AS inactif
        FROM dates da
        CROSS JOIN personnes p
        CROSS JOIN parametres pr
        LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                             AND si.PERS_ID     = p.PERS_ID
                                             AND si.rn          = 1
        LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
        LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                             AND po.PERS_ID     = p.PERS_ID
        LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                             AND tc.PERS_ID     = p.PERS_ID
        LEFT JOIN interruptions_a_date ia     ON ia.date_arrete = da.date_arrete
                                             AND ia.PERS_ID     = p.PERS_ID
        LEFT JOIN regroupement_categorie rc   ON rc.code        = si.categorie
        LEFT JOIN correspondance_domaine cdo  ON cdo.UA_CODE    = TO_CHAR(si.UA_CODE)
        WHERE
                (NVL(si.nature, '-') <> 'TEMPORAIRE'
                 AND p.date_entree <= da.date_arrete
                 AND (p.etat <> 'INACTIF' OR pd.date_depart > da.date_arrete)
                 AND NOT (pr.disponibilite = 'HORS_EFFECTIF'
                          AND NVL(po.date_dispo, DATE '1900-01-01')
                              > NVL(po.date_remise, DATE '1900-01-01')))
             OR
                (si.nature = 'TEMPORAIRE'
                 AND tc.PERS_ID IS NOT NULL
                 AND NOT (pr.fin_contrat_inconnue = 'FICHE'
                          AND tc.fin_inconnue = 1
                          AND p.etat = 'INACTIF'))
    )
    SELECT
        TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD'))         AS ID_TEMPS,
        b.QUAL_CODE,
        ta.TRANCHE_AGE_CODE,
        tan.TRANCHE_ANC_CODE,
        b.SEXE,
        CASE b.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        b.CODE_DOMAINE,
        u.UA_NATURE,
        b.LIEU_ID,
        b.FNCT_CODE,
        b.QUALITE_EMPLOYE,
        b.NATURE_EMPLOI,
        COUNT(DISTINCT b.PERS_ID)                             AS NB_AGENTS,
        COUNT(DISTINCT CASE WHEN b.inactif = 0 THEN b.PERS_ID END) AS NB_AGENTS_ACTIFS,
        COUNT(DISTINCT CASE WHEN b.inactif = 1 THEN b.PERS_ID END) AS NB_AGENTS_INACTIFS,
        ROUND(AVG(b.age), 1)                                  AS AGE_MOYEN,
        ROUND(AVG(b.anciennete), 1)                           AS ANCIENNETE_MOYENNE,
        SYSDATE                                               AS DATE_CHARGEMENT,
        0                                                    AS CLICHE,
        b.CATEGORIE,
        b.GROUPE_CATEGORIE,
        b.UA_CODE,
        b.DOMAINE_ACTIVITE,
        COUNT(DISTINCT CASE WHEN b.cadre = 'OUI' THEN b.PERS_ID END) AS NB_CADRES
    FROM base b
    LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u
        ON  u.UA_CODE = b.UA_CODE
    LEFT JOIN (SELECT '<20' AS TRANCHE_AGE_CODE, 0 AS AGE_MIN, 19 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '20-24' AS TRANCHE_AGE_CODE, 20 AS AGE_MIN, 24 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '25-29' AS TRANCHE_AGE_CODE, 25 AS AGE_MIN, 29 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '30-34' AS TRANCHE_AGE_CODE, 30 AS AGE_MIN, 34 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '35-39' AS TRANCHE_AGE_CODE, 35 AS AGE_MIN, 39 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '40-44' AS TRANCHE_AGE_CODE, 40 AS AGE_MIN, 44 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '45-49' AS TRANCHE_AGE_CODE, 45 AS AGE_MIN, 49 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '50-54' AS TRANCHE_AGE_CODE, 50 AS AGE_MIN, 54 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '55-59' AS TRANCHE_AGE_CODE, 55 AS AGE_MIN, 59 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '60+' AS TRANCHE_AGE_CODE, 60 AS AGE_MIN, TO_NUMBER(NULL) AS AGE_MAX FROM DUAL) ta
        ON  b.age >= ta.AGE_MIN
        AND (b.age <= ta.AGE_MAX OR ta.AGE_MAX IS NULL)
    LEFT JOIN (SELECT '<1' AS TRANCHE_ANC_CODE, 0 AS ANC_MIN, 0 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '1-4' AS TRANCHE_ANC_CODE, 1 AS ANC_MIN, 4 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '5-9' AS TRANCHE_ANC_CODE, 5 AS ANC_MIN, 9 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '10-14' AS TRANCHE_ANC_CODE, 10 AS ANC_MIN, 14 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '15-19' AS TRANCHE_ANC_CODE, 15 AS ANC_MIN, 19 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '20-24' AS TRANCHE_ANC_CODE, 20 AS ANC_MIN, 24 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '25-29' AS TRANCHE_ANC_CODE, 25 AS ANC_MIN, 29 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '30-34' AS TRANCHE_ANC_CODE, 30 AS ANC_MIN, 34 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '35-39' AS TRANCHE_ANC_CODE, 35 AS ANC_MIN, 39 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '40+' AS TRANCHE_ANC_CODE, 40 AS ANC_MIN, TO_NUMBER(NULL) AS ANC_MAX FROM DUAL) tan
        ON  b.anciennete >= tan.ANC_MIN
        AND (b.anciennete <= tan.ANC_MAX OR tan.ANC_MAX IS NULL)
    GROUP BY
        TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD')),
        b.QUAL_CODE,
        ta.TRANCHE_AGE_CODE,
        tan.TRANCHE_ANC_CODE,
        b.SEXE,
        CASE b.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
        b.CODE_DOMAINE,
        u.UA_NATURE,
        b.LIEU_ID,
        b.FNCT_CODE,
        b.QUALITE_EMPLOYE,
        b.NATURE_EMPLOI,
        b.CATEGORIE,
        b.GROUPE_CATEGORIE,
        b.UA_CODE,
        b.DOMAINE_ACTIVITE
)
WHERE ID_TEMPS = 20241231
  AND QUALITE_EMPLOYE = 'A'
GROUP BY NATURE_EMPLOI, GROUPE_CATEGORIE, SEXE
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- C2 — T65 et l'effectif de 2019 à 2025 : par année et nature
-- Attendu  : permanents 2024 environ 741, taux d'encadrement 48,8 % (publié 49,2 %) ;
--            environ 42 % de 2020 à 2023 (publié 41 %) ; publié : 767 agents fin 2023,
--            758 fin 2024. Les agents inactifs valent 0 (disponibilité hors effectif).
--            Sert aussi aux points 57 (années passées) et 59 (taux des temporaires).
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS, NATURE_EMPLOI, SUM(NB_AGENTS) AS agents, SUM(NB_CADRES) AS cadres,
       ROUND(100 * SUM(NB_CADRES) / SUM(NB_AGENTS), 1) AS taux_encadrement,
       SUM(NB_AGENTS_INACTIFS) AS inactifs
FROM (
    WITH
    parametres AS (
        SELECT
               'ANNUELLE'              AS periodicite,
               'PRISE_SERVICE'         AS ordre_date_anciennete,
               'OUI'                   AS retirer_interruptions,
               'PRESENT'               AS fin_contrat_inconnue,
               'HORS_EFFECTIF'         AS disponibilite,
               'DECISIONS'             AS inactifs
        FROM DUAL
    ),
    decisions_depart AS (
        SELECT '02' AS code, 'admission à la retraite' AS libelle FROM DUAL UNION ALL
        SELECT '34', 'liquidation des droits après décès'      FROM DUAL UNION ALL
        SELECT '35', 'fin de contrat'                          FROM DUAL UNION ALL
        SELECT '36', 'démission'                               FROM DUAL UNION ALL
        SELECT '52', 'rupture du contrat'                      FROM DUAL UNION ALL
        SELECT '54', 'licenciement'                            FROM DUAL
    ),
    codes_decision AS (
        SELECT '40' AS annulation,
               '50' AS engagement_temporaire,
               '51' AS engagement_permanent,
               '07' AS mise_en_disponibilite,
               '18' AS remise_en_activite
        FROM DUAL
    ),
    regroupement_categorie AS (
        SELECT '01' AS code, 'EMPLOYES' AS groupe, 'NON' AS cadre FROM DUAL UNION ALL
        SELECT '02', 'EMPLOYES',      'NON'                       FROM DUAL UNION ALL
        SELECT '03', 'MAITRISE',      'NON'                       FROM DUAL UNION ALL
        SELECT '04', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
        SELECT '05', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
        SELECT '06', 'CADRES_SUP',    'OUI'                       FROM DUAL UNION ALL
        SELECT 'HC', 'CADRES_SUP',    'OUI'                       FROM DUAL
    ),
    correspondance_domaine AS (
        SELECT CAST(NULL AS VARCHAR2(12)) AS UA_CODE, CAST(NULL AS VARCHAR2(60)) AS DOMAINE
        FROM DUAL WHERE 1 = 0
    ),
    annees AS (
        SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS annee
        FROM USER_GRH.GRH_SITUATION s
        WHERE 1 = 1
          AND s.DATE_DEBUT IS NOT NULL
          AND EXTRACT(YEAR FROM s.DATE_DEBUT) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE) - 1
    ),
    dates AS (
        SELECT TO_DATE(TO_CHAR(annee) || '1231', 'YYYYMMDD')            AS date_arrete
        FROM annees
        UNION
        SELECT LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE, 'YYYY'), LEVEL - 13))
        FROM parametres pr
        WHERE pr.periodicite = 'MENSUELLE'
        CONNECT BY LEVEL <= 11 + EXTRACT(MONTH FROM SYSDATE)
    ),
    personnes AS (
        SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS, NVL(p.ETAT_EMPLOYE, '-') AS etat,
               TO_CHAR(p.QUALITE)                                     AS qualite,
               COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree,
               CASE pr.ordre_date_anciennete
                   WHEN 'EMBAUCHE' THEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
                   ELSE                 COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
               END                                                    AS debut_anciennete
        FROM USER_GRH.GRH_PERSONNE p
        CROSS JOIN parametres pr
        WHERE 1 = 1
    ),
    decisions AS (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               TO_CHAR(d.CDEC_CODE)       AS code,
               d.DATE_VIGUEUR             AS date_decision
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp
               ON dp.DECI_NO = d.DECI_NO
              AND 1 = 1
        CROSS JOIN codes_decision cd
        WHERE 1 = 1
          AND d.DATE_VIGUEUR IS NOT NULL
          AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
               OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                           cd.mise_en_disponibilite, cd.remise_en_activite))
          AND NOT EXISTS (SELECT 1
                          FROM USER_GRH.GRH_DECISION a
                          WHERE 1 = 1
                            AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    ),
    departs AS (
        SELECT PERS_ID, date_decision AS date_depart
        FROM decisions
        WHERE PERS_ID IS NOT NULL
          AND code IN (SELECT code FROM decisions_depart)
        UNION ALL
        SELECT PERS_ID, DATE_DECES
        FROM USER_GRH.GRH_PERSONNE
        WHERE 1 = 1
          AND DATE_DECES IS NOT NULL
    ),
    premier_depart AS (
        SELECT PERS_ID, MIN(date_depart) AS date_depart
        FROM departs
        GROUP BY PERS_ID
    ),
    engagements AS (
        SELECT e.PERS_ID, e.date_decision AS debut,
               LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                           ORDER BY e.date_decision) AS debut_suivant
        FROM decisions e
        CROSS JOIN codes_decision cd
        WHERE e.PERS_ID IS NOT NULL
          AND e.code = cd.engagement_temporaire
    ),
    contrats AS (
        SELECT g.PERS_ID, g.debut,
               LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                     NVL(g.debut_suivant, DATE '9999-12-31'))         AS fin
        FROM engagements g
        LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                             AND dep.date_depart > g.debut
        GROUP BY g.PERS_ID, g.debut, g.debut_suivant
    ),
    temporaires_sous_contrat AS (
        SELECT da.date_arrete, c.PERS_ID,
               MAX(CASE WHEN c.fin = DATE '9999-12-31' THEN 1 ELSE 0 END) AS fin_inconnue
        FROM dates da
        JOIN contrats c ON c.debut       <= da.date_arrete
                       AND da.date_arrete <  c.fin
        GROUP BY da.date_arrete, c.PERS_ID
    ),
    positions AS (
        SELECT da.date_arrete, d.PERS_ID,
               MAX(CASE WHEN d.code = cd.mise_en_disponibilite THEN d.date_decision END) AS date_dispo,
               MAX(CASE WHEN d.code = cd.remise_en_activite    THEN d.date_decision END) AS date_remise
        FROM dates da
        JOIN decisions d ON d.date_decision <= da.date_arrete
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code IN (cd.mise_en_disponibilite, cd.remise_en_activite)
        GROUP BY da.date_arrete, d.PERS_ID
    ),
    interruptions AS (
        SELECT d.PERS_ID, d.date_decision AS debut,
               (SELECT MIN(r.date_decision)
                FROM decisions r, codes_decision c2
                WHERE r.PERS_ID       = d.PERS_ID
                  AND r.code          = c2.remise_en_activite
                  AND r.date_decision > d.date_decision)          AS fin
        FROM decisions d
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.mise_en_disponibilite
        UNION ALL
        SELECT x.PERS_ID, x.debut, x.fin
        FROM (SELECT dep.PERS_ID, dep.date_depart AS debut,
                     (SELECT MIN(e.date_decision)
                      FROM decisions e, codes_decision c3
                      WHERE e.PERS_ID       = dep.PERS_ID
                        AND e.code IN (c3.engagement_temporaire, c3.engagement_permanent)
                        AND e.date_decision > dep.date_depart)    AS fin
              FROM departs dep) x
        WHERE x.fin IS NOT NULL
    ),
    interruptions_a_date AS (
        SELECT da.date_arrete, i.PERS_ID,
               SUM(GREATEST(0, LEAST(NVL(i.fin, da.date_arrete), da.date_arrete)
                               - GREATEST(i.debut, p.debut_anciennete))) AS jours
        FROM dates da
        JOIN interruptions i ON i.debut < da.date_arrete
        JOIN personnes p     ON p.PERS_ID = i.PERS_ID
        GROUP BY da.date_arrete, i.PERS_ID
    ),
    situations AS (
        SELECT da.date_arrete, s.PERS_ID,
               s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
               s.STATUT_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
               TO_CHAR(s.CATEGORIE)                                   AS categorie,
               ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM dates da
        JOIN USER_GRH.GRH_SITUATION s ON 1 = 1
                                     AND s.DATE_DEBUT <= da.date_arrete
    ),
    base AS (
        SELECT da.date_arrete, p.PERS_ID, p.SEXE,
               p.qualite                                              AS QUALITE_EMPLOYE,
               si.nature                                              AS NATURE_EMPLOI,
               si.QUAL_CODE,
               si.CODE_DOMAINE_ACT                                    AS CODE_DOMAINE,
               si.UA_CODE, si.LIEU_ID, si.FNCT_CODE, si.STATUT_EMPLOYE,
               si.categorie                                           AS CATEGORIE,
               NVL(rc.groupe, 'SANS_CATEGORIE')                       AS GROUPE_CATEGORIE,
               NVL(rc.cadre, 'NON')                                   AS cadre,
               NVL(cdo.DOMAINE, 'Domaine non renseigné')              AS DOMAINE_ACTIVITE,
               CASE WHEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
                         BETWEEN 0 AND 119
                    THEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
               END                                                    AS age,
               GREATEST(FLOOR((da.date_arrete - p.debut_anciennete
                               - CASE pr.retirer_interruptions
                                     WHEN 'OUI' THEN NVL(ia.jours, 0) ELSE 0 END)
                              / 365.25), 0)                           AS anciennete,
               CASE WHEN pr.inactifs = 'STATUT'
                    THEN CASE WHEN si.STATUT_EMPLOYE IN ('B','D','E','F','H')
                              THEN 1 ELSE 0 END
                    ELSE CASE WHEN NVL(po.date_dispo,  DATE '1900-01-01')
                                 > NVL(po.date_remise, DATE '1900-01-01')
                              THEN 1 ELSE 0 END
               END                                                    AS inactif
        FROM dates da
        CROSS JOIN personnes p
        CROSS JOIN parametres pr
        LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                             AND si.PERS_ID     = p.PERS_ID
                                             AND si.rn          = 1
        LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
        LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                             AND po.PERS_ID     = p.PERS_ID
        LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                             AND tc.PERS_ID     = p.PERS_ID
        LEFT JOIN interruptions_a_date ia     ON ia.date_arrete = da.date_arrete
                                             AND ia.PERS_ID     = p.PERS_ID
        LEFT JOIN regroupement_categorie rc   ON rc.code        = si.categorie
        LEFT JOIN correspondance_domaine cdo  ON cdo.UA_CODE    = TO_CHAR(si.UA_CODE)
        WHERE
                (NVL(si.nature, '-') <> 'TEMPORAIRE'
                 AND p.date_entree <= da.date_arrete
                 AND (p.etat <> 'INACTIF' OR pd.date_depart > da.date_arrete)
                 AND NOT (pr.disponibilite = 'HORS_EFFECTIF'
                          AND NVL(po.date_dispo, DATE '1900-01-01')
                              > NVL(po.date_remise, DATE '1900-01-01')))
             OR
                (si.nature = 'TEMPORAIRE'
                 AND tc.PERS_ID IS NOT NULL
                 AND NOT (pr.fin_contrat_inconnue = 'FICHE'
                          AND tc.fin_inconnue = 1
                          AND p.etat = 'INACTIF'))
    )
    SELECT
        TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD'))         AS ID_TEMPS,
        b.QUAL_CODE,
        ta.TRANCHE_AGE_CODE,
        tan.TRANCHE_ANC_CODE,
        b.SEXE,
        CASE b.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        b.CODE_DOMAINE,
        u.UA_NATURE,
        b.LIEU_ID,
        b.FNCT_CODE,
        b.QUALITE_EMPLOYE,
        b.NATURE_EMPLOI,
        COUNT(DISTINCT b.PERS_ID)                             AS NB_AGENTS,
        COUNT(DISTINCT CASE WHEN b.inactif = 0 THEN b.PERS_ID END) AS NB_AGENTS_ACTIFS,
        COUNT(DISTINCT CASE WHEN b.inactif = 1 THEN b.PERS_ID END) AS NB_AGENTS_INACTIFS,
        ROUND(AVG(b.age), 1)                                  AS AGE_MOYEN,
        ROUND(AVG(b.anciennete), 1)                           AS ANCIENNETE_MOYENNE,
        SYSDATE                                               AS DATE_CHARGEMENT,
        0                                                    AS CLICHE,
        b.CATEGORIE,
        b.GROUPE_CATEGORIE,
        b.UA_CODE,
        b.DOMAINE_ACTIVITE,
        COUNT(DISTINCT CASE WHEN b.cadre = 'OUI' THEN b.PERS_ID END) AS NB_CADRES
    FROM base b
    LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u
        ON  u.UA_CODE = b.UA_CODE
    LEFT JOIN (SELECT '<20' AS TRANCHE_AGE_CODE, 0 AS AGE_MIN, 19 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '20-24' AS TRANCHE_AGE_CODE, 20 AS AGE_MIN, 24 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '25-29' AS TRANCHE_AGE_CODE, 25 AS AGE_MIN, 29 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '30-34' AS TRANCHE_AGE_CODE, 30 AS AGE_MIN, 34 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '35-39' AS TRANCHE_AGE_CODE, 35 AS AGE_MIN, 39 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '40-44' AS TRANCHE_AGE_CODE, 40 AS AGE_MIN, 44 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '45-49' AS TRANCHE_AGE_CODE, 45 AS AGE_MIN, 49 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '50-54' AS TRANCHE_AGE_CODE, 50 AS AGE_MIN, 54 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '55-59' AS TRANCHE_AGE_CODE, 55 AS AGE_MIN, 59 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '60+' AS TRANCHE_AGE_CODE, 60 AS AGE_MIN, TO_NUMBER(NULL) AS AGE_MAX FROM DUAL) ta
        ON  b.age >= ta.AGE_MIN
        AND (b.age <= ta.AGE_MAX OR ta.AGE_MAX IS NULL)
    LEFT JOIN (SELECT '<1' AS TRANCHE_ANC_CODE, 0 AS ANC_MIN, 0 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '1-4' AS TRANCHE_ANC_CODE, 1 AS ANC_MIN, 4 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '5-9' AS TRANCHE_ANC_CODE, 5 AS ANC_MIN, 9 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '10-14' AS TRANCHE_ANC_CODE, 10 AS ANC_MIN, 14 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '15-19' AS TRANCHE_ANC_CODE, 15 AS ANC_MIN, 19 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '20-24' AS TRANCHE_ANC_CODE, 20 AS ANC_MIN, 24 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '25-29' AS TRANCHE_ANC_CODE, 25 AS ANC_MIN, 29 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '30-34' AS TRANCHE_ANC_CODE, 30 AS ANC_MIN, 34 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '35-39' AS TRANCHE_ANC_CODE, 35 AS ANC_MIN, 39 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '40+' AS TRANCHE_ANC_CODE, 40 AS ANC_MIN, TO_NUMBER(NULL) AS ANC_MAX FROM DUAL) tan
        ON  b.anciennete >= tan.ANC_MIN
        AND (b.anciennete <= tan.ANC_MAX OR tan.ANC_MAX IS NULL)
    GROUP BY
        TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD')),
        b.QUAL_CODE,
        ta.TRANCHE_AGE_CODE,
        tan.TRANCHE_ANC_CODE,
        b.SEXE,
        CASE b.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
        b.CODE_DOMAINE,
        u.UA_NATURE,
        b.LIEU_ID,
        b.FNCT_CODE,
        b.QUALITE_EMPLOYE,
        b.NATURE_EMPLOI,
        b.CATEGORIE,
        b.GROUPE_CATEGORIE,
        b.UA_CODE,
        b.DOMAINE_ACTIVITE
)
WHERE ID_TEMPS BETWEEN 20191231 AND 20251231
  AND MOD(ID_TEMPS, 10000) = 1231
  AND QUALITE_EMPLOYE = 'A'
GROUP BY ID_TEMPS, NATURE_EMPLOI
ORDER BY 2, 1;


-- -----------------------------------------------------------------------------
-- C3 — T61 au 31/12/2024, permanents : par tranche d'ancienneté et sexe
-- Attendu  : à comparer au tableau 122 de l'annuaire CNSS 2024. Premier calcul avec la
--            nouvelle règle (prise de service d'abord, absences retirées).
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT TRANCHE_ANC_CODE, SEXE, SUM(NB_AGENTS) AS agents
FROM (
    WITH
    parametres AS (
        SELECT
               'ANNUELLE'              AS periodicite,
               'PRISE_SERVICE'         AS ordre_date_anciennete,
               'OUI'                   AS retirer_interruptions,
               'PRESENT'               AS fin_contrat_inconnue,
               'HORS_EFFECTIF'         AS disponibilite,
               'DECISIONS'             AS inactifs
        FROM DUAL
    ),
    decisions_depart AS (
        SELECT '02' AS code, 'admission à la retraite' AS libelle FROM DUAL UNION ALL
        SELECT '34', 'liquidation des droits après décès'      FROM DUAL UNION ALL
        SELECT '35', 'fin de contrat'                          FROM DUAL UNION ALL
        SELECT '36', 'démission'                               FROM DUAL UNION ALL
        SELECT '52', 'rupture du contrat'                      FROM DUAL UNION ALL
        SELECT '54', 'licenciement'                            FROM DUAL
    ),
    codes_decision AS (
        SELECT '40' AS annulation,
               '50' AS engagement_temporaire,
               '51' AS engagement_permanent,
               '07' AS mise_en_disponibilite,
               '18' AS remise_en_activite
        FROM DUAL
    ),
    regroupement_categorie AS (
        SELECT '01' AS code, 'EMPLOYES' AS groupe, 'NON' AS cadre FROM DUAL UNION ALL
        SELECT '02', 'EMPLOYES',      'NON'                       FROM DUAL UNION ALL
        SELECT '03', 'MAITRISE',      'NON'                       FROM DUAL UNION ALL
        SELECT '04', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
        SELECT '05', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
        SELECT '06', 'CADRES_SUP',    'OUI'                       FROM DUAL UNION ALL
        SELECT 'HC', 'CADRES_SUP',    'OUI'                       FROM DUAL
    ),
    correspondance_domaine AS (
        SELECT CAST(NULL AS VARCHAR2(12)) AS UA_CODE, CAST(NULL AS VARCHAR2(60)) AS DOMAINE
        FROM DUAL WHERE 1 = 0
    ),
    annees AS (
        SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS annee
        FROM USER_GRH.GRH_SITUATION s
        WHERE 1 = 1
          AND s.DATE_DEBUT IS NOT NULL
          AND EXTRACT(YEAR FROM s.DATE_DEBUT) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE) - 1
    ),
    dates AS (
        SELECT TO_DATE(TO_CHAR(annee) || '1231', 'YYYYMMDD')            AS date_arrete
        FROM annees
        UNION
        SELECT LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE, 'YYYY'), LEVEL - 13))
        FROM parametres pr
        WHERE pr.periodicite = 'MENSUELLE'
        CONNECT BY LEVEL <= 11 + EXTRACT(MONTH FROM SYSDATE)
    ),
    personnes AS (
        SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS, NVL(p.ETAT_EMPLOYE, '-') AS etat,
               TO_CHAR(p.QUALITE)                                     AS qualite,
               COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree,
               CASE pr.ordre_date_anciennete
                   WHEN 'EMBAUCHE' THEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
                   ELSE                 COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
               END                                                    AS debut_anciennete
        FROM USER_GRH.GRH_PERSONNE p
        CROSS JOIN parametres pr
        WHERE 1 = 1
    ),
    decisions AS (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               TO_CHAR(d.CDEC_CODE)       AS code,
               d.DATE_VIGUEUR             AS date_decision
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp
               ON dp.DECI_NO = d.DECI_NO
              AND 1 = 1
        CROSS JOIN codes_decision cd
        WHERE 1 = 1
          AND d.DATE_VIGUEUR IS NOT NULL
          AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
               OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                           cd.mise_en_disponibilite, cd.remise_en_activite))
          AND NOT EXISTS (SELECT 1
                          FROM USER_GRH.GRH_DECISION a
                          WHERE 1 = 1
                            AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    ),
    departs AS (
        SELECT PERS_ID, date_decision AS date_depart
        FROM decisions
        WHERE PERS_ID IS NOT NULL
          AND code IN (SELECT code FROM decisions_depart)
        UNION ALL
        SELECT PERS_ID, DATE_DECES
        FROM USER_GRH.GRH_PERSONNE
        WHERE 1 = 1
          AND DATE_DECES IS NOT NULL
    ),
    premier_depart AS (
        SELECT PERS_ID, MIN(date_depart) AS date_depart
        FROM departs
        GROUP BY PERS_ID
    ),
    engagements AS (
        SELECT e.PERS_ID, e.date_decision AS debut,
               LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                           ORDER BY e.date_decision) AS debut_suivant
        FROM decisions e
        CROSS JOIN codes_decision cd
        WHERE e.PERS_ID IS NOT NULL
          AND e.code = cd.engagement_temporaire
    ),
    contrats AS (
        SELECT g.PERS_ID, g.debut,
               LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                     NVL(g.debut_suivant, DATE '9999-12-31'))         AS fin
        FROM engagements g
        LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                             AND dep.date_depart > g.debut
        GROUP BY g.PERS_ID, g.debut, g.debut_suivant
    ),
    temporaires_sous_contrat AS (
        SELECT da.date_arrete, c.PERS_ID,
               MAX(CASE WHEN c.fin = DATE '9999-12-31' THEN 1 ELSE 0 END) AS fin_inconnue
        FROM dates da
        JOIN contrats c ON c.debut       <= da.date_arrete
                       AND da.date_arrete <  c.fin
        GROUP BY da.date_arrete, c.PERS_ID
    ),
    positions AS (
        SELECT da.date_arrete, d.PERS_ID,
               MAX(CASE WHEN d.code = cd.mise_en_disponibilite THEN d.date_decision END) AS date_dispo,
               MAX(CASE WHEN d.code = cd.remise_en_activite    THEN d.date_decision END) AS date_remise
        FROM dates da
        JOIN decisions d ON d.date_decision <= da.date_arrete
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code IN (cd.mise_en_disponibilite, cd.remise_en_activite)
        GROUP BY da.date_arrete, d.PERS_ID
    ),
    interruptions AS (
        SELECT d.PERS_ID, d.date_decision AS debut,
               (SELECT MIN(r.date_decision)
                FROM decisions r, codes_decision c2
                WHERE r.PERS_ID       = d.PERS_ID
                  AND r.code          = c2.remise_en_activite
                  AND r.date_decision > d.date_decision)          AS fin
        FROM decisions d
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.mise_en_disponibilite
        UNION ALL
        SELECT x.PERS_ID, x.debut, x.fin
        FROM (SELECT dep.PERS_ID, dep.date_depart AS debut,
                     (SELECT MIN(e.date_decision)
                      FROM decisions e, codes_decision c3
                      WHERE e.PERS_ID       = dep.PERS_ID
                        AND e.code IN (c3.engagement_temporaire, c3.engagement_permanent)
                        AND e.date_decision > dep.date_depart)    AS fin
              FROM departs dep) x
        WHERE x.fin IS NOT NULL
    ),
    interruptions_a_date AS (
        SELECT da.date_arrete, i.PERS_ID,
               SUM(GREATEST(0, LEAST(NVL(i.fin, da.date_arrete), da.date_arrete)
                               - GREATEST(i.debut, p.debut_anciennete))) AS jours
        FROM dates da
        JOIN interruptions i ON i.debut < da.date_arrete
        JOIN personnes p     ON p.PERS_ID = i.PERS_ID
        GROUP BY da.date_arrete, i.PERS_ID
    ),
    situations AS (
        SELECT da.date_arrete, s.PERS_ID,
               s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
               s.STATUT_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
               TO_CHAR(s.CATEGORIE)                                   AS categorie,
               ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM dates da
        JOIN USER_GRH.GRH_SITUATION s ON 1 = 1
                                     AND s.DATE_DEBUT <= da.date_arrete
    ),
    base AS (
        SELECT da.date_arrete, p.PERS_ID, p.SEXE,
               p.qualite                                              AS QUALITE_EMPLOYE,
               si.nature                                              AS NATURE_EMPLOI,
               si.QUAL_CODE,
               si.CODE_DOMAINE_ACT                                    AS CODE_DOMAINE,
               si.UA_CODE, si.LIEU_ID, si.FNCT_CODE, si.STATUT_EMPLOYE,
               si.categorie                                           AS CATEGORIE,
               NVL(rc.groupe, 'SANS_CATEGORIE')                       AS GROUPE_CATEGORIE,
               NVL(rc.cadre, 'NON')                                   AS cadre,
               NVL(cdo.DOMAINE, 'Domaine non renseigné')              AS DOMAINE_ACTIVITE,
               CASE WHEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
                         BETWEEN 0 AND 119
                    THEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
               END                                                    AS age,
               GREATEST(FLOOR((da.date_arrete - p.debut_anciennete
                               - CASE pr.retirer_interruptions
                                     WHEN 'OUI' THEN NVL(ia.jours, 0) ELSE 0 END)
                              / 365.25), 0)                           AS anciennete,
               CASE WHEN pr.inactifs = 'STATUT'
                    THEN CASE WHEN si.STATUT_EMPLOYE IN ('B','D','E','F','H')
                              THEN 1 ELSE 0 END
                    ELSE CASE WHEN NVL(po.date_dispo,  DATE '1900-01-01')
                                 > NVL(po.date_remise, DATE '1900-01-01')
                              THEN 1 ELSE 0 END
               END                                                    AS inactif
        FROM dates da
        CROSS JOIN personnes p
        CROSS JOIN parametres pr
        LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                             AND si.PERS_ID     = p.PERS_ID
                                             AND si.rn          = 1
        LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
        LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                             AND po.PERS_ID     = p.PERS_ID
        LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                             AND tc.PERS_ID     = p.PERS_ID
        LEFT JOIN interruptions_a_date ia     ON ia.date_arrete = da.date_arrete
                                             AND ia.PERS_ID     = p.PERS_ID
        LEFT JOIN regroupement_categorie rc   ON rc.code        = si.categorie
        LEFT JOIN correspondance_domaine cdo  ON cdo.UA_CODE    = TO_CHAR(si.UA_CODE)
        WHERE
                (NVL(si.nature, '-') <> 'TEMPORAIRE'
                 AND p.date_entree <= da.date_arrete
                 AND (p.etat <> 'INACTIF' OR pd.date_depart > da.date_arrete)
                 AND NOT (pr.disponibilite = 'HORS_EFFECTIF'
                          AND NVL(po.date_dispo, DATE '1900-01-01')
                              > NVL(po.date_remise, DATE '1900-01-01')))
             OR
                (si.nature = 'TEMPORAIRE'
                 AND tc.PERS_ID IS NOT NULL
                 AND NOT (pr.fin_contrat_inconnue = 'FICHE'
                          AND tc.fin_inconnue = 1
                          AND p.etat = 'INACTIF'))
    )
    SELECT
        TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD'))         AS ID_TEMPS,
        b.QUAL_CODE,
        ta.TRANCHE_AGE_CODE,
        tan.TRANCHE_ANC_CODE,
        b.SEXE,
        CASE b.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        b.CODE_DOMAINE,
        u.UA_NATURE,
        b.LIEU_ID,
        b.FNCT_CODE,
        b.QUALITE_EMPLOYE,
        b.NATURE_EMPLOI,
        COUNT(DISTINCT b.PERS_ID)                             AS NB_AGENTS,
        COUNT(DISTINCT CASE WHEN b.inactif = 0 THEN b.PERS_ID END) AS NB_AGENTS_ACTIFS,
        COUNT(DISTINCT CASE WHEN b.inactif = 1 THEN b.PERS_ID END) AS NB_AGENTS_INACTIFS,
        ROUND(AVG(b.age), 1)                                  AS AGE_MOYEN,
        ROUND(AVG(b.anciennete), 1)                           AS ANCIENNETE_MOYENNE,
        SYSDATE                                               AS DATE_CHARGEMENT,
        0                                                    AS CLICHE,
        b.CATEGORIE,
        b.GROUPE_CATEGORIE,
        b.UA_CODE,
        b.DOMAINE_ACTIVITE,
        COUNT(DISTINCT CASE WHEN b.cadre = 'OUI' THEN b.PERS_ID END) AS NB_CADRES
    FROM base b
    LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u
        ON  u.UA_CODE = b.UA_CODE
    LEFT JOIN (SELECT '<20' AS TRANCHE_AGE_CODE, 0 AS AGE_MIN, 19 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '20-24' AS TRANCHE_AGE_CODE, 20 AS AGE_MIN, 24 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '25-29' AS TRANCHE_AGE_CODE, 25 AS AGE_MIN, 29 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '30-34' AS TRANCHE_AGE_CODE, 30 AS AGE_MIN, 34 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '35-39' AS TRANCHE_AGE_CODE, 35 AS AGE_MIN, 39 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '40-44' AS TRANCHE_AGE_CODE, 40 AS AGE_MIN, 44 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '45-49' AS TRANCHE_AGE_CODE, 45 AS AGE_MIN, 49 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '50-54' AS TRANCHE_AGE_CODE, 50 AS AGE_MIN, 54 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '55-59' AS TRANCHE_AGE_CODE, 55 AS AGE_MIN, 59 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '60+' AS TRANCHE_AGE_CODE, 60 AS AGE_MIN, TO_NUMBER(NULL) AS AGE_MAX FROM DUAL) ta
        ON  b.age >= ta.AGE_MIN
        AND (b.age <= ta.AGE_MAX OR ta.AGE_MAX IS NULL)
    LEFT JOIN (SELECT '<1' AS TRANCHE_ANC_CODE, 0 AS ANC_MIN, 0 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '1-4' AS TRANCHE_ANC_CODE, 1 AS ANC_MIN, 4 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '5-9' AS TRANCHE_ANC_CODE, 5 AS ANC_MIN, 9 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '10-14' AS TRANCHE_ANC_CODE, 10 AS ANC_MIN, 14 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '15-19' AS TRANCHE_ANC_CODE, 15 AS ANC_MIN, 19 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '20-24' AS TRANCHE_ANC_CODE, 20 AS ANC_MIN, 24 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '25-29' AS TRANCHE_ANC_CODE, 25 AS ANC_MIN, 29 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '30-34' AS TRANCHE_ANC_CODE, 30 AS ANC_MIN, 34 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '35-39' AS TRANCHE_ANC_CODE, 35 AS ANC_MIN, 39 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '40+' AS TRANCHE_ANC_CODE, 40 AS ANC_MIN, TO_NUMBER(NULL) AS ANC_MAX FROM DUAL) tan
        ON  b.anciennete >= tan.ANC_MIN
        AND (b.anciennete <= tan.ANC_MAX OR tan.ANC_MAX IS NULL)
    GROUP BY
        TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD')),
        b.QUAL_CODE,
        ta.TRANCHE_AGE_CODE,
        tan.TRANCHE_ANC_CODE,
        b.SEXE,
        CASE b.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
        b.CODE_DOMAINE,
        u.UA_NATURE,
        b.LIEU_ID,
        b.FNCT_CODE,
        b.QUALITE_EMPLOYE,
        b.NATURE_EMPLOI,
        b.CATEGORIE,
        b.GROUPE_CATEGORIE,
        b.UA_CODE,
        b.DOMAINE_ACTIVITE
)
WHERE ID_TEMPS = 20241231
  AND QUALITE_EMPLOYE = 'A'
  AND NATURE_EMPLOI = 'PERMANENT'
GROUP BY TRANCHE_ANC_CODE, SEXE
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- C4 — T58 au 31/12/2024, permanents : par tranche d'âge et sexe
-- Attendu  : à quelques agents près, les tranches d'âge du tableau 119 de l'annuaire
--            CNSS 2024 (lot 17).
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT TRANCHE_AGE_CODE, SEXE, SUM(NB_AGENTS) AS agents
FROM (
    WITH
    parametres AS (
        SELECT
               'ANNUELLE'              AS periodicite,
               'PRISE_SERVICE'         AS ordre_date_anciennete,
               'OUI'                   AS retirer_interruptions,
               'PRESENT'               AS fin_contrat_inconnue,
               'HORS_EFFECTIF'         AS disponibilite,
               'DECISIONS'             AS inactifs
        FROM DUAL
    ),
    decisions_depart AS (
        SELECT '02' AS code, 'admission à la retraite' AS libelle FROM DUAL UNION ALL
        SELECT '34', 'liquidation des droits après décès'      FROM DUAL UNION ALL
        SELECT '35', 'fin de contrat'                          FROM DUAL UNION ALL
        SELECT '36', 'démission'                               FROM DUAL UNION ALL
        SELECT '52', 'rupture du contrat'                      FROM DUAL UNION ALL
        SELECT '54', 'licenciement'                            FROM DUAL
    ),
    codes_decision AS (
        SELECT '40' AS annulation,
               '50' AS engagement_temporaire,
               '51' AS engagement_permanent,
               '07' AS mise_en_disponibilite,
               '18' AS remise_en_activite
        FROM DUAL
    ),
    regroupement_categorie AS (
        SELECT '01' AS code, 'EMPLOYES' AS groupe, 'NON' AS cadre FROM DUAL UNION ALL
        SELECT '02', 'EMPLOYES',      'NON'                       FROM DUAL UNION ALL
        SELECT '03', 'MAITRISE',      'NON'                       FROM DUAL UNION ALL
        SELECT '04', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
        SELECT '05', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
        SELECT '06', 'CADRES_SUP',    'OUI'                       FROM DUAL UNION ALL
        SELECT 'HC', 'CADRES_SUP',    'OUI'                       FROM DUAL
    ),
    correspondance_domaine AS (
        SELECT CAST(NULL AS VARCHAR2(12)) AS UA_CODE, CAST(NULL AS VARCHAR2(60)) AS DOMAINE
        FROM DUAL WHERE 1 = 0
    ),
    annees AS (
        SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS annee
        FROM USER_GRH.GRH_SITUATION s
        WHERE 1 = 1
          AND s.DATE_DEBUT IS NOT NULL
          AND EXTRACT(YEAR FROM s.DATE_DEBUT) BETWEEN 1900 AND EXTRACT(YEAR FROM SYSDATE) - 1
    ),
    dates AS (
        SELECT TO_DATE(TO_CHAR(annee) || '1231', 'YYYYMMDD')            AS date_arrete
        FROM annees
        UNION
        SELECT LAST_DAY(ADD_MONTHS(TRUNC(SYSDATE, 'YYYY'), LEVEL - 13))
        FROM parametres pr
        WHERE pr.periodicite = 'MENSUELLE'
        CONNECT BY LEVEL <= 11 + EXTRACT(MONTH FROM SYSDATE)
    ),
    personnes AS (
        SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS, NVL(p.ETAT_EMPLOYE, '-') AS etat,
               TO_CHAR(p.QUALITE)                                     AS qualite,
               COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree,
               CASE pr.ordre_date_anciennete
                   WHEN 'EMBAUCHE' THEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
                   ELSE                 COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
               END                                                    AS debut_anciennete
        FROM USER_GRH.GRH_PERSONNE p
        CROSS JOIN parametres pr
        WHERE 1 = 1
    ),
    decisions AS (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               TO_CHAR(d.CDEC_CODE)       AS code,
               d.DATE_VIGUEUR             AS date_decision
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp
               ON dp.DECI_NO = d.DECI_NO
              AND 1 = 1
        CROSS JOIN codes_decision cd
        WHERE 1 = 1
          AND d.DATE_VIGUEUR IS NOT NULL
          AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
               OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                           cd.mise_en_disponibilite, cd.remise_en_activite))
          AND NOT EXISTS (SELECT 1
                          FROM USER_GRH.GRH_DECISION a
                          WHERE 1 = 1
                            AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    ),
    departs AS (
        SELECT PERS_ID, date_decision AS date_depart
        FROM decisions
        WHERE PERS_ID IS NOT NULL
          AND code IN (SELECT code FROM decisions_depart)
        UNION ALL
        SELECT PERS_ID, DATE_DECES
        FROM USER_GRH.GRH_PERSONNE
        WHERE 1 = 1
          AND DATE_DECES IS NOT NULL
    ),
    premier_depart AS (
        SELECT PERS_ID, MIN(date_depart) AS date_depart
        FROM departs
        GROUP BY PERS_ID
    ),
    engagements AS (
        SELECT e.PERS_ID, e.date_decision AS debut,
               LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                           ORDER BY e.date_decision) AS debut_suivant
        FROM decisions e
        CROSS JOIN codes_decision cd
        WHERE e.PERS_ID IS NOT NULL
          AND e.code = cd.engagement_temporaire
    ),
    contrats AS (
        SELECT g.PERS_ID, g.debut,
               LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                     NVL(g.debut_suivant, DATE '9999-12-31'))         AS fin
        FROM engagements g
        LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                             AND dep.date_depart > g.debut
        GROUP BY g.PERS_ID, g.debut, g.debut_suivant
    ),
    temporaires_sous_contrat AS (
        SELECT da.date_arrete, c.PERS_ID,
               MAX(CASE WHEN c.fin = DATE '9999-12-31' THEN 1 ELSE 0 END) AS fin_inconnue
        FROM dates da
        JOIN contrats c ON c.debut       <= da.date_arrete
                       AND da.date_arrete <  c.fin
        GROUP BY da.date_arrete, c.PERS_ID
    ),
    positions AS (
        SELECT da.date_arrete, d.PERS_ID,
               MAX(CASE WHEN d.code = cd.mise_en_disponibilite THEN d.date_decision END) AS date_dispo,
               MAX(CASE WHEN d.code = cd.remise_en_activite    THEN d.date_decision END) AS date_remise
        FROM dates da
        JOIN decisions d ON d.date_decision <= da.date_arrete
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code IN (cd.mise_en_disponibilite, cd.remise_en_activite)
        GROUP BY da.date_arrete, d.PERS_ID
    ),
    interruptions AS (
        SELECT d.PERS_ID, d.date_decision AS debut,
               (SELECT MIN(r.date_decision)
                FROM decisions r, codes_decision c2
                WHERE r.PERS_ID       = d.PERS_ID
                  AND r.code          = c2.remise_en_activite
                  AND r.date_decision > d.date_decision)          AS fin
        FROM decisions d
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.mise_en_disponibilite
        UNION ALL
        SELECT x.PERS_ID, x.debut, x.fin
        FROM (SELECT dep.PERS_ID, dep.date_depart AS debut,
                     (SELECT MIN(e.date_decision)
                      FROM decisions e, codes_decision c3
                      WHERE e.PERS_ID       = dep.PERS_ID
                        AND e.code IN (c3.engagement_temporaire, c3.engagement_permanent)
                        AND e.date_decision > dep.date_depart)    AS fin
              FROM departs dep) x
        WHERE x.fin IS NOT NULL
    ),
    interruptions_a_date AS (
        SELECT da.date_arrete, i.PERS_ID,
               SUM(GREATEST(0, LEAST(NVL(i.fin, da.date_arrete), da.date_arrete)
                               - GREATEST(i.debut, p.debut_anciennete))) AS jours
        FROM dates da
        JOIN interruptions i ON i.debut < da.date_arrete
        JOIN personnes p     ON p.PERS_ID = i.PERS_ID
        GROUP BY da.date_arrete, i.PERS_ID
    ),
    situations AS (
        SELECT da.date_arrete, s.PERS_ID,
               s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
               s.STATUT_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
               TO_CHAR(s.CATEGORIE)                                   AS categorie,
               ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM dates da
        JOIN USER_GRH.GRH_SITUATION s ON 1 = 1
                                     AND s.DATE_DEBUT <= da.date_arrete
    ),
    base AS (
        SELECT da.date_arrete, p.PERS_ID, p.SEXE,
               p.qualite                                              AS QUALITE_EMPLOYE,
               si.nature                                              AS NATURE_EMPLOI,
               si.QUAL_CODE,
               si.CODE_DOMAINE_ACT                                    AS CODE_DOMAINE,
               si.UA_CODE, si.LIEU_ID, si.FNCT_CODE, si.STATUT_EMPLOYE,
               si.categorie                                           AS CATEGORIE,
               NVL(rc.groupe, 'SANS_CATEGORIE')                       AS GROUPE_CATEGORIE,
               NVL(rc.cadre, 'NON')                                   AS cadre,
               NVL(cdo.DOMAINE, 'Domaine non renseigné')              AS DOMAINE_ACTIVITE,
               CASE WHEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
                         BETWEEN 0 AND 119
                    THEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
               END                                                    AS age,
               GREATEST(FLOOR((da.date_arrete - p.debut_anciennete
                               - CASE pr.retirer_interruptions
                                     WHEN 'OUI' THEN NVL(ia.jours, 0) ELSE 0 END)
                              / 365.25), 0)                           AS anciennete,
               CASE WHEN pr.inactifs = 'STATUT'
                    THEN CASE WHEN si.STATUT_EMPLOYE IN ('B','D','E','F','H')
                              THEN 1 ELSE 0 END
                    ELSE CASE WHEN NVL(po.date_dispo,  DATE '1900-01-01')
                                 > NVL(po.date_remise, DATE '1900-01-01')
                              THEN 1 ELSE 0 END
               END                                                    AS inactif
        FROM dates da
        CROSS JOIN personnes p
        CROSS JOIN parametres pr
        LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                             AND si.PERS_ID     = p.PERS_ID
                                             AND si.rn          = 1
        LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
        LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                             AND po.PERS_ID     = p.PERS_ID
        LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                             AND tc.PERS_ID     = p.PERS_ID
        LEFT JOIN interruptions_a_date ia     ON ia.date_arrete = da.date_arrete
                                             AND ia.PERS_ID     = p.PERS_ID
        LEFT JOIN regroupement_categorie rc   ON rc.code        = si.categorie
        LEFT JOIN correspondance_domaine cdo  ON cdo.UA_CODE    = TO_CHAR(si.UA_CODE)
        WHERE
                (NVL(si.nature, '-') <> 'TEMPORAIRE'
                 AND p.date_entree <= da.date_arrete
                 AND (p.etat <> 'INACTIF' OR pd.date_depart > da.date_arrete)
                 AND NOT (pr.disponibilite = 'HORS_EFFECTIF'
                          AND NVL(po.date_dispo, DATE '1900-01-01')
                              > NVL(po.date_remise, DATE '1900-01-01')))
             OR
                (si.nature = 'TEMPORAIRE'
                 AND tc.PERS_ID IS NOT NULL
                 AND NOT (pr.fin_contrat_inconnue = 'FICHE'
                          AND tc.fin_inconnue = 1
                          AND p.etat = 'INACTIF'))
    )
    SELECT
        TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD'))         AS ID_TEMPS,
        b.QUAL_CODE,
        ta.TRANCHE_AGE_CODE,
        tan.TRANCHE_ANC_CODE,
        b.SEXE,
        CASE b.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        b.CODE_DOMAINE,
        u.UA_NATURE,
        b.LIEU_ID,
        b.FNCT_CODE,
        b.QUALITE_EMPLOYE,
        b.NATURE_EMPLOI,
        COUNT(DISTINCT b.PERS_ID)                             AS NB_AGENTS,
        COUNT(DISTINCT CASE WHEN b.inactif = 0 THEN b.PERS_ID END) AS NB_AGENTS_ACTIFS,
        COUNT(DISTINCT CASE WHEN b.inactif = 1 THEN b.PERS_ID END) AS NB_AGENTS_INACTIFS,
        ROUND(AVG(b.age), 1)                                  AS AGE_MOYEN,
        ROUND(AVG(b.anciennete), 1)                           AS ANCIENNETE_MOYENNE,
        SYSDATE                                               AS DATE_CHARGEMENT,
        0                                                    AS CLICHE,
        b.CATEGORIE,
        b.GROUPE_CATEGORIE,
        b.UA_CODE,
        b.DOMAINE_ACTIVITE,
        COUNT(DISTINCT CASE WHEN b.cadre = 'OUI' THEN b.PERS_ID END) AS NB_CADRES
    FROM base b
    LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u
        ON  u.UA_CODE = b.UA_CODE
    LEFT JOIN (SELECT '<20' AS TRANCHE_AGE_CODE, 0 AS AGE_MIN, 19 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '20-24' AS TRANCHE_AGE_CODE, 20 AS AGE_MIN, 24 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '25-29' AS TRANCHE_AGE_CODE, 25 AS AGE_MIN, 29 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '30-34' AS TRANCHE_AGE_CODE, 30 AS AGE_MIN, 34 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '35-39' AS TRANCHE_AGE_CODE, 35 AS AGE_MIN, 39 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '40-44' AS TRANCHE_AGE_CODE, 40 AS AGE_MIN, 44 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '45-49' AS TRANCHE_AGE_CODE, 45 AS AGE_MIN, 49 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '50-54' AS TRANCHE_AGE_CODE, 50 AS AGE_MIN, 54 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '55-59' AS TRANCHE_AGE_CODE, 55 AS AGE_MIN, 59 AS AGE_MAX FROM DUAL
               UNION ALL SELECT '60+' AS TRANCHE_AGE_CODE, 60 AS AGE_MIN, TO_NUMBER(NULL) AS AGE_MAX FROM DUAL) ta
        ON  b.age >= ta.AGE_MIN
        AND (b.age <= ta.AGE_MAX OR ta.AGE_MAX IS NULL)
    LEFT JOIN (SELECT '<1' AS TRANCHE_ANC_CODE, 0 AS ANC_MIN, 0 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '1-4' AS TRANCHE_ANC_CODE, 1 AS ANC_MIN, 4 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '5-9' AS TRANCHE_ANC_CODE, 5 AS ANC_MIN, 9 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '10-14' AS TRANCHE_ANC_CODE, 10 AS ANC_MIN, 14 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '15-19' AS TRANCHE_ANC_CODE, 15 AS ANC_MIN, 19 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '20-24' AS TRANCHE_ANC_CODE, 20 AS ANC_MIN, 24 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '25-29' AS TRANCHE_ANC_CODE, 25 AS ANC_MIN, 29 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '30-34' AS TRANCHE_ANC_CODE, 30 AS ANC_MIN, 34 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '35-39' AS TRANCHE_ANC_CODE, 35 AS ANC_MIN, 39 AS ANC_MAX FROM DUAL
               UNION ALL SELECT '40+' AS TRANCHE_ANC_CODE, 40 AS ANC_MIN, TO_NUMBER(NULL) AS ANC_MAX FROM DUAL) tan
        ON  b.anciennete >= tan.ANC_MIN
        AND (b.anciennete <= tan.ANC_MAX OR tan.ANC_MAX IS NULL)
    GROUP BY
        TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD')),
        b.QUAL_CODE,
        ta.TRANCHE_AGE_CODE,
        tan.TRANCHE_ANC_CODE,
        b.SEXE,
        CASE b.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
        b.CODE_DOMAINE,
        u.UA_NATURE,
        b.LIEU_ID,
        b.FNCT_CODE,
        b.QUALITE_EMPLOYE,
        b.NATURE_EMPLOI,
        b.CATEGORIE,
        b.GROUPE_CATEGORIE,
        b.UA_CODE,
        b.DOMAINE_ACTIVITE
)
WHERE ID_TEMPS = 20241231
  AND QUALITE_EMPLOYE = 'A'
  AND NATURE_EMPLOI = 'PERMANENT'
GROUP BY TRANCHE_AGE_CODE, SEXE
ORDER BY 1, 2;
