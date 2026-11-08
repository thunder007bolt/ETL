-- =============================================================================
-- DTM_GRH_EFFECTIF — PROPOSITION du 04/11/2026, corrigée le 05/11/2026 après
-- le lot 24 (T58, T59, T61, T65)
-- MÊME STRUCTURE que domaines/grh/dtm/sql/grh_effectif.sql : mêmes colonnes,
-- même grain, même paramètre :1. Seules les règles changent (réunion du
-- 03/11/2026 avec la CNSS, termes 1 à 9 de la fiche de lecture GRH, et recueil
-- des besoins v7). Cinq colonnes s'ajoutent à la fin, aucune n'est retirée.
-- Après le visa de la DRH, ce fichier remplace grh_effectif.sql (les colonnes
-- ajoutées doivent d'abord être créées dans DTM et ODS.DTM_GRH_EFFECTIF).
--
-- Sources : DWH.FAIT_GRH_PERSONNE, FAIT_GRH_SITUATION, FAIT_GRH_DECISION,
--           FAIT_GRH_DECISION_PERSONNE ; DTM.DIM_GRH_UNITE_ADMINISTRATIVE,
--           DIM_GRH_TRANCHE_AGE, DIM_GRH_TRANCHE_ANCIENNETE ; la
--           correspondance unité → domaine de la CNSS (partie 0, à brancher).
-- :1 = CLICHE (MMYYYY), saisi sans guillemets.
-- Grain   : ID_TEMPS × QUAL_CODE × TRANCHE_AGE_CODE × TRANCHE_ANC_CODE × SEXE
--           × CODE_DOMAINE × UA_NATURE × LIEU_ID × FNCT_CODE × QUALITE_EMPLOYE
--           × NATURE_EMPLOI (comme avant), plus CATEGORIE et UA_CODE.
--           Un agent est compté une seule fois par date d'arrêté : NB_AGENTS
--           et NB_CADRES s'additionnent sur tous les axes, sauf ID_TEMPS.
--
-- COLONNES : celles de grh_effectif.sql, dans le même ordre :
--   ID_TEMPS, QUAL_CODE, TRANCHE_AGE_CODE, TRANCHE_ANC_CODE, SEXE,
--   LIBELLE_SEXE, CODE_DOMAINE, UA_NATURE, LIEU_ID, FNCT_CODE,
--   QUALITE_EMPLOYE, NATURE_EMPLOI, NB_AGENTS, NB_AGENTS_ACTIFS,
--   NB_AGENTS_INACTIFS, AGE_MOYEN, ANCIENNETE_MOYENNE, DATE_CHARGEMENT, CLICHE
-- puis les colonnes AJOUTÉES :
--   CATEGORIE         catégorie de la grille (01 à 06, HC) — G3
--   GROUPE_CATEGORIE  EMPLOYES, MAITRISE, CADRES_MOYENS, CADRES_SUP ou
--                     SANS_CATEGORIE — G3
--   UA_CODE           unité administrative, pour la déclinaison par
--                     direction, agence et direction régionale
--   DOMAINE_ACTIVITE  domaine de l'unité, d'après la CNSS — G12 (T59)
--   NB_CADRES         agents des catégories cadres — G6 (T65)
-- CODE_DOMAINE garde son contenu d'avant (code de la situation) pour ne pas
-- casser l'existant ; T59 se fait désormais avec DOMAINE_ACTIVITE.
--
-- LES TABLEAUX DANS OBIEE (filtre global NATURE_EMPLOI, QUALITE_EMPLOYE = 'A') :
--   T58 : GROUPE_CATEGORIE × SEXE en colonnes, TRANCHE_AGE_CODE en lignes ;
--   T59 : GROUPE_CATEGORIE × SEXE en colonnes, DOMAINE_ACTIVITE en lignes ;
--   T61 : GROUPE_CATEGORIE × SEXE en colonnes, TRANCHE_ANC_CODE en lignes ;
--   T65 : SUM(NB_CADRES) / SUM(NB_AGENTS) par ID_TEMPS, sur 5 exercices.
--
-- CONTRÔLE dans SQL Developer (cliché 102026), en entourant la requête :
--   T58 :  SELECT GROUPE_CATEGORIE, SEXE, SUM(NB_AGENTS)
--          FROM ( …la requête… )
--          WHERE ID_TEMPS = 20241231
--            AND QUALITE_EMPLOYE = 'A' AND NATURE_EMPLOI = 'PERMANENT'
--          GROUP BY GROUPE_CATEGORIE, SEXE ORDER BY 1, 2;
--   T65 :  SELECT ID_TEMPS, NATURE_EMPLOI, SUM(NB_AGENTS), SUM(NB_CADRES),
--                 ROUND(100 * SUM(NB_CADRES) / SUM(NB_AGENTS), 1)
--          FROM ( …la requête… )
--          WHERE ID_TEMPS BETWEEN 20201231 AND 20251231
--            AND QUALITE_EMPLOYE = 'A'
--          GROUP BY ID_TEMPS, NATURE_EMPLOI ORDER BY 2, 1;
--   Le 31/12/2025 est ajouté au contrôle : c'est l'année où comptent les
--   fiches sans état (embauches récentes, G2), comptées depuis le 05/11/2026.
-- Attendu (lots 17 à 19) : 742 permanents au 31/12/2024 (publié 758) ;
--   employés 129 H / 44 F (133 / 44), maîtrise 92 / 115 (94 / 114), cadres
--   moyens 139 / 165 (143 / 168), cadres supérieurs 40 / 18 (42 / 20) ;
--   T65 permanents 48,8 % en 2024 (publié 49,2 %), environ 42 % de 2020 à 2023
--   (publié 41 %, les 84 engagés de 2019, règle G11) ; temporaires : plus de
--   46 (publié), à cause de la règle G7 ; T61 à recalculer.
--
-- RÈGLES (détail et exemples dans analyses/grh/regles_effectif.md).
-- La « date d'arrêté » est le jour où l'on photographie l'effectif
-- (souvent le 31/12 ; parfois une fin de mois).
--
--   · G1 Nature
--     Permanent ou temporaire, d'après la dernière situation commencée au
--     plus tard à la date d'arrêté. Filtre global dans OBIEE. La table garde
--     toutes les qualités (A, B, C) ; la CIPRES se lit avec qualité A.
--     Un agent sans situation suit la règle des permanents (G2).
--
--   · G2 Permanent présent
--     Compté s'il est entré au plus tard à la date d'arrêté ET s'il n'est
--     pas encore parti.
--     Entrée = date d'embauche ; si elle est vide, date de prise de service.
--     Pas encore parti = fiche différente de INACTIF (une fiche sans état
--     compte comme encore là), OU premier départ après la date d'arrêté.
--     Départs : retraite 02, décès 34, fin de contrat 35, démission 36,
--     rupture 52, licenciement 54 (liste réglable) ; une décision annulée
--     par une 40 ne compte pas ; à défaut, la date de décès.
--
--   · G7 Temporaire présent
--     Compté si la date d'arrêté tombe dans un de ses contrats.
--     Début du contrat = engagement temporaire (décision 50), non annulé.
--     Fin du contrat, dans cet ordre :
--       1. premier départ après le début ;
--       2. sinon, veille du contrat suivant ;
--       3. sinon, aucune fin connue → l'agent est quand même compté
--          (réglage fin_contrat_inconnue = PRESENT).
--
--   · G3 Catégorie
--     Catégorie de la dernière situation, regroupée ainsi :
--     01-02 employés, 03 maîtrise, 04-05 cadres moyens, 06 et HC cadres
--     supérieurs. La colonne « cadre » de ce tableau dit qui entre dans
--     NB_CADRES.
--
--   · G4 Âge
--     Années pleines à la date d'arrêté, rangées dans DIM_GRH_TRANCHE_AGE.
--     Date de naissance absente ou âge hors de 0 à 119 : l'agent reste
--     compté, avec une tranche vide.
--
--   · Sexe
--     Lu sur la fiche (M / F) ; LIBELLE_SEXE comme avant.
--
--   · G5 Ancienneté
--     Années pleines depuis la prise de service (à défaut l'embauche),
--     moins les périodes où l'agent n'était pas en activité :
--     - disponibilité (décision 07) jusqu'au retour (décision 18) ;
--     - départ suivi d'un nouvel engagement (50 ou 51).
--
--   · G12 Domaine
--     Domaine de l'unité de la dernière situation (UA_CODE), lu dans la
--     correspondance CNSS ; sinon « Domaine non renseigné ».
--
--   · G6 Taux d'encadrement
--     NB_CADRES / NB_AGENTS (inchangé).
--
--   · G13 Actifs et inactifs
--     Parmi les agents comptés : inactif = encore en disponibilité à la
--     date d'arrêté (dernière décision 07 plus récente que la dernière 18).
--     Les autres sont actifs. On ne lit plus le statut de la situation
--     (B détaché, D mis à disposition, E disponibilité, F suspendu,
--     H sous les drapeaux) : ce statut n'est pas daté. Le réglage
--     inactifs = STATUT permet de revenir à l'ancienne lecture.
--
-- RÉGLAGES (partie 0) : chacun peut changer sans réécrire la requête.
--   · periodicite           : ANNUELLE = 31/12 des exercices clos ;
--                             MENSUELLE = ajoute aussi les fins de mois
--                             clos. Avant MENSUELLE, Jules doit régler
--                             NB_AGENTS en « dernière valeur » dans OBIEE,
--                             sinon une année additionnerait ses 12 mois.
--   · ordre_date_anciennete : PRISE_SERVICE ou EMBAUCHE (début G5).
--   · retirer_interruptions : OUI = soustraire les absences de l'ancienneté ;
--                             NON = ne pas les soustraire.
--   · fin_contrat_inconnue  : PRESENT = temporaire sans fin connue compté ;
--                             FICHE = compté seulement si la fiche n'est
--                             pas INACTIF.
--   · disponibilite         : PRESENT = permanent en disponibilité reste
--                             dans l'effectif ;
--                             HORS_EFFECTIF = il en sort (QG6, pas encore
--                             tranché par la DRH) ; même valeur dans T60.
--   · inactifs              : DECISIONS = inactif si disponibilité 07 sans
--                             retour 18 après ;
--                             STATUT = inactif si statut B, D, E, F ou H.
--                             Avec HORS_EFFECTIF + DECISIONS,
--                             NB_AGENTS_INACTIFS vaut 0.
--   · decisions_depart, codes_decision, regroupement_categorie :
--     listes de codes, une ligne par code.
--   · correspondance_domaine : à brancher sur la table du fichier
--     correspondance_unite_domaine.xlsx ; tant qu'elle est vide, tous les
--     agents sont en « Domaine non renseigné ».
-- =============================================================================

WITH

-- ── 0. Les réglages ─────────────────────────────────────────────────────────
parametres AS (
    SELECT -- dates d'arrêté : 'ANNUELLE' ou 'MENSUELLE'
           'ANNUELLE'              AS periodicite,
           -- G5 : date de début de l'ancienneté, 'PRISE_SERVICE' ou 'EMBAUCHE'
           'PRISE_SERVICE'         AS ordre_date_anciennete,
           -- G5 : retirer les interruptions de l'ancienneté, 'OUI' ou 'NON'
           'OUI'                   AS retirer_interruptions,
           -- G7 : sans fin de contrat, 'PRESENT' (compté) ou 'FICHE' (si actif)
           'PRESENT'               AS fin_contrat_inconnue,
           -- QG6 : en disponibilité, 'PRESENT' = compté (inactif) ; 'HORS_EFFECTIF' = sorti
           'PRESENT'               AS disponibilite,
           -- G13 : inactifs via 'DECISIONS' (07/18) ou 'STATUT' (B,D,E,F,H)
           'DECISIONS'             AS inactifs
    FROM DUAL
),
-- G2, G7 : les décisions de départ (GRH_DECISION.CDEC_CODE) ; une ligne par code
decisions_depart AS (
    SELECT '02' AS code, 'admission à la retraite' AS libelle FROM DUAL UNION ALL
    SELECT '34', 'liquidation des droits après décès'      FROM DUAL UNION ALL
    SELECT '35', 'fin de contrat'                          FROM DUAL UNION ALL
    SELECT '36', 'démission'                               FROM DUAL UNION ALL
    SELECT '52', 'rupture du contrat'                      FROM DUAL UNION ALL
    SELECT '54', 'licenciement'                            FROM DUAL
),
-- les autres codes de décision utilisés
codes_decision AS (
    SELECT '40' AS annulation,
           '50' AS engagement_temporaire,
           '51' AS engagement_permanent,
           '07' AS mise_en_disponibilite,
           '18' AS remise_en_activite
    FROM DUAL
),
-- G3, G6 : le regroupement des catégories de la grille (GRH_SITUATION.CATEGORIE)
-- et celles qui comptent comme cadres (NB_CADRES)
regroupement_categorie AS (
    SELECT '01' AS code, 'EMPLOYES' AS groupe, 'NON' AS cadre FROM DUAL UNION ALL
    SELECT '02', 'EMPLOYES',      'NON'                       FROM DUAL UNION ALL
    SELECT '03', 'MAITRISE',      'NON'                       FROM DUAL UNION ALL
    SELECT '04', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
    SELECT '05', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
    SELECT '06', 'CADRES_SUP',    'OUI'                       FROM DUAL UNION ALL
    -- hors catégorie : proposition BI, aucun permanent en 2024
    SELECT 'HC', 'CADRES_SUP',    'OUI'                       FROM DUAL
),
-- G12 : le domaine de chaque unité, d'après le fichier de la CNSS.
-- À BRANCHER quand la table sera chargée : remplacer les deux lignes du SELECT
-- par « SELECT UA_CODE, DOMAINE FROM DTM.REF_GRH_UNITE_DOMAINE ».
correspondance_domaine AS (
    SELECT CAST(NULL AS VARCHAR2(12)) AS UA_CODE, CAST(NULL AS VARCHAR2(60)) AS DOMAINE
    FROM DUAL WHERE 1 = 0
),

-- ── 1. Les dates d'arrêté ───────────────────────────────────────────────────
-- Comme avant : le 31/12 de chaque exercice clos (une année où commence au
-- moins une situation, jusqu'à l'année dernière). En MENSUELLE, s'y ajoute la
-- fin de chaque mois clos de l'année passée et de l'année en cours.
annees AS (
    SELECT DISTINCT EXTRACT(YEAR FROM s.DATE_DEBUT) AS annee
    FROM DWH.FAIT_GRH_SITUATION s
    WHERE s.CLICHE      = :1
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

-- ── 2. Les agents et la date de début de leur ancienneté ────────────────────
personnes AS (
    SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS, NVL(p.ETAT_EMPLOYE, '-') AS etat,
           -- Qualité : A agent CNSS, B fonction publique, C agent affecté
           TO_CHAR(p.QUALITE)                                     AS qualite,
           -- G2 : la date d'entrée
           COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree,
           -- G5 : la date de début de l'ancienneté
           CASE pr.ordre_date_anciennete
               WHEN 'EMBAUCHE' THEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
               ELSE                 COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
           END                                                    AS debut_anciennete
    FROM DWH.FAIT_GRH_PERSONNE p
    CROSS JOIN parametres pr
    WHERE p.CLICHE = :1
),

-- ── 3. Les décisions utiles, sans celles annulées par une décision 40 ──────
-- L'agent est porté par la décision, ou par sa liste quand elle est collective.
decisions AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           d.DATE_VIGUEUR             AS date_decision
    FROM DWH.FAIT_GRH_DECISION d
    LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp
           ON dp.DECI_NO = d.DECI_NO
          AND dp.CLICHE  = :1
    CROSS JOIN codes_decision cd
    WHERE d.CLICHE = :1
      AND d.DATE_VIGUEUR IS NOT NULL
      AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
           OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                       cd.mise_en_disponibilite, cd.remise_en_activite))
      AND NOT EXISTS (SELECT 1
                      FROM DWH.FAIT_GRH_DECISION a
                      WHERE a.CLICHE = :1
                        AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
-- les départs : décisions de départ, et la date de décès
departs AS (
    SELECT PERS_ID, date_decision AS date_depart
    FROM decisions
    WHERE PERS_ID IS NOT NULL
      AND code IN (SELECT code FROM decisions_depart)
    UNION ALL
    SELECT PERS_ID, DATE_DECES
    FROM DWH.FAIT_GRH_PERSONNE
    WHERE CLICHE = :1
      AND DATE_DECES IS NOT NULL
),
-- G2 : le premier départ de chaque agent
premier_depart AS (
    SELECT PERS_ID, MIN(date_depart) AS date_depart
    FROM departs
    GROUP BY PERS_ID
),

-- ── 4. Les contrats des temporaires (G7) ───────────────────────────────────
-- Chaque engagement temporaire (décision 50) ouvre un contrat.
-- debut = date de cet engagement ; debut_suivant = date du suivant, s'il y en a.
engagements AS (
    SELECT e.PERS_ID, e.date_decision AS debut,
           LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                       ORDER BY e.date_decision) AS debut_suivant
    FROM decisions e
    CROSS JOIN codes_decision cd
    WHERE e.PERS_ID IS NOT NULL
      AND e.code = cd.engagement_temporaire
),
-- Fin du contrat (jour exclu) : premier départ après le début ; sinon début
-- du contrat suivant ; sinon 31/12/9999 = fin inconnue, contrat encore ouvert.
contrats AS (
    SELECT g.PERS_ID, g.debut,
           LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                 NVL(g.debut_suivant, DATE '9999-12-31'))         AS fin
    FROM engagements g
    LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                         AND dep.date_depart > g.debut
    GROUP BY g.PERS_ID, g.debut, g.debut_suivant
),
-- Temporaires encore sous contrat à la date d'arrêté (debut <= date < fin).
-- fin_inconnue = 1 s'il existe au moins un contrat sans date de fin connue.
temporaires_sous_contrat AS (
    SELECT da.date_arrete, c.PERS_ID,
           MAX(CASE WHEN c.fin = DATE '9999-12-31' THEN 1 ELSE 0 END) AS fin_inconnue
    FROM dates da
    JOIN contrats c ON c.debut       <= da.date_arrete
                   AND da.date_arrete <  c.fin
    GROUP BY da.date_arrete, c.PERS_ID
),

-- ── 5. Les disponibilités (QG6) et les interruptions de l'ancienneté (G5) ──
-- Dernière mise en disponibilité (07) et dernière remise en activité (18)
-- au plus tard à la date d'arrêté. Si date_dispo > date_remise, l'agent est
-- encore en disponibilité (inactif G13 ; éventuellement hors effectif QG6).
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
-- Périodes à retirer de l'ancienneté : disponibilité 07→18, ou départ suivi
-- d'un nouvel engagement 50/51.
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
-- Nombre de jours d'interruption entre le début de carrière et la date
-- d'arrêté (uniquement le morceau de chaque période qui chevauche cet intervalle).
interruptions_a_date AS (
    SELECT da.date_arrete, i.PERS_ID,
           SUM(GREATEST(0, LEAST(NVL(i.fin, da.date_arrete), da.date_arrete)
                           - GREATEST(i.debut, p.debut_anciennete))) AS jours
    FROM dates da
    JOIN interruptions i ON i.debut < da.date_arrete
    JOIN personnes p     ON p.PERS_ID = i.PERS_ID
    GROUP BY da.date_arrete, i.PERS_ID
),

-- ── 6. La dernière situation de chaque agent à chaque date ─────────────────
-- Dernière situation commencée au plus tard à la date d'arrêté ; à date
-- égale, la dernière saisie. Elle donne tous les axes, comme avant.
situations AS (
    SELECT da.date_arrete, s.PERS_ID,
           s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
           s.STATUT_EMPLOYE,
           TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
           TO_CHAR(s.CATEGORIE)                                   AS categorie,
           ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM dates da
    JOIN DWH.FAIT_GRH_SITUATION s ON s.CLICHE     = :1
                                 AND s.DATE_DEBUT <= da.date_arrete
),

-- ── 7. Les agents présents (G2, G7) et leurs axes ──────────────────────────
-- Une ligne par agent et par date d'arrêté. Un agent sans situation reste
-- compté avec la règle des permanents et des axes vides, comme avant.
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
           -- G4 : âge en années pleines ; vide si la date est absente ou fausse
           CASE WHEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
                     BETWEEN 0 AND 119
                THEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)
           END                                                    AS age,
           -- G5 : ancienneté en années pleines, moins les interruptions
           GREATEST(FLOOR((da.date_arrete - p.debut_anciennete
                           - CASE pr.retirer_interruptions
                                 WHEN 'OUI' THEN NVL(ia.jours, 0) ELSE 0 END)
                          / 365.25), 0)                           AS anciennete,
           -- G13 : 1 = inactif, 0 = actif
           -- STATUT : B détaché, D mis à disposition, E dispo, F suspendu, H drapeaux
           -- DECISIONS (valeur actuelle) : encore en disponibilité si
           -- dernière 07 plus récente que dernière 18 (dates absentes = 1900)
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
            -- G2 : permanent présent (ou agent sans situation)
            (NVL(si.nature, '-') <> 'TEMPORAIRE'
             AND p.date_entree <= da.date_arrete
             -- fiche pas INACTIF (vide = embauche récente), ou départ après la date
             AND (p.etat <> 'INACTIF' OR pd.date_depart > da.date_arrete)
             AND NOT (pr.disponibilite = 'HORS_EFFECTIF'
                      AND NVL(po.date_dispo, DATE '1900-01-01')
                          > NVL(po.date_remise, DATE '1900-01-01')))
         OR
            -- G7 : temporaire sous contrat
            (si.nature = 'TEMPORAIRE'
             AND tc.PERS_ID IS NOT NULL
             AND NOT (pr.fin_contrat_inconnue = 'FICHE'
                      AND tc.fin_inconnue = 1
                      AND p.etat = 'INACTIF'))
)

-- ── 8. Tranches d'âge et d'ancienneté, puis comptages par axe ──────────────
-- Comme avant : une tranche par agent (bornes incluses), un agent compté une
-- fois par date d'arrêté, sur la ligne de sa combinaison d'axes. Jointures
-- externes : un âge inconnu donne une tranche vide, l'agent reste compté.
SELECT
    -- ── AXES (comme avant) ───────────────────────────────────────────
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
    -- ── MESURES (comme avant) ────────────────────────────────────────
    COUNT(DISTINCT b.PERS_ID)                             AS NB_AGENTS,
    -- G13 : actifs = agents comptés avec inactif = 0
    COUNT(DISTINCT CASE WHEN b.inactif = 0 THEN b.PERS_ID END) AS NB_AGENTS_ACTIFS,
    -- G13 : inactifs = agents comptés avec inactif = 1 (voir réglage inactifs)
    COUNT(DISTINCT CASE WHEN b.inactif = 1 THEN b.PERS_ID END) AS NB_AGENTS_INACTIFS,
    ROUND(AVG(b.age), 1)                                  AS AGE_MOYEN,
    ROUND(AVG(b.anciennete), 1)                           AS ANCIENNETE_MOYENNE,
    -- ── CHARGEMENT (comme avant) ─────────────────────────────────────
    SYSDATE                                               AS DATE_CHARGEMENT,
    :1                                                    AS CLICHE,
    -- ── COLONNES AJOUTÉES ────────────────────────────────────────────
    b.CATEGORIE,
    b.GROUPE_CATEGORIE,
    b.UA_CODE,
    b.DOMAINE_ACTIVITE,
    COUNT(DISTINCT CASE WHEN b.cadre = 'OUI' THEN b.PERS_ID END) AS NB_CADRES
FROM base b
LEFT JOIN DTM.DIM_GRH_UNITE_ADMINISTRATIVE u
    ON  u.UA_CODE = b.UA_CODE
-- Tranche d'âge : AGE_MIN <= âge <= AGE_MAX, la dernière tranche n'a pas de borne haute
LEFT JOIN DTM.DIM_GRH_TRANCHE_AGE ta
    ON  b.age >= ta.AGE_MIN
    AND (b.age <= ta.AGE_MAX OR ta.AGE_MAX IS NULL)
-- Tranche d'ancienneté : même principe
LEFT JOIN DTM.DIM_GRH_TRANCHE_ANCIENNETE tan
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
