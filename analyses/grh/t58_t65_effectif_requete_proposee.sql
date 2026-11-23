-- =============================================================================
-- DTM_GRH_EFFECTIF — REQUÊTE PROPOSÉE : T58, T59, T61, T65
-- Version du 21/11/2026. Les règles sont celles de la version du 08/11/2026,
-- lancée sur la base le 17/11/2026 : avec les réglages tels qu'ils sont écrits
-- ici, la requête doit redonner les mêmes chiffres. Ce qui change : chaque
-- point qui attend une réponse de la CNSS est un réglage, en tête de requête.
--
-- MÊME STRUCTURE que domaines/grh/dtm/sql/grh_effectif.sql : mêmes colonnes,
-- même grain, même paramètre. Cinq colonnes s'ajoutent à la fin, aucune n'est
-- retirée. Après le visa de la DRH, ce fichier remplace grh_effectif.sql (les
-- colonnes ajoutées doivent d'abord être créées dans DTM et ODS).
--
-- Sources : DWH.FAIT_GRH_PERSONNE, FAIT_GRH_SITUATION, FAIT_GRH_DECISION,
--           FAIT_GRH_DECISION_PERSONNE ; DTM.DIM_GRH_UNITE_ADMINISTRATIVE,
--           DIM_GRH_TRANCHE_AGE, DIM_GRH_TRANCHE_ANCIENNETE ; la
--           correspondance unité → domaine de la CNSS (à brancher).
-- :1 = CLICHE (MMYYYY), saisi sans guillemets. C'est la seule valeur à saisir.
-- Grain   : ID_TEMPS × QUAL_CODE × TRANCHE_AGE_CODE × TRANCHE_ANC_CODE × SEXE
--           × CODE_DOMAINE × UA_NATURE × LIEU_ID × FNCT_CODE × QUALITE_EMPLOYE
--           × NATURE_EMPLOI (comme avant), plus CATEGORIE et UA_CODE.
--           Un agent est compté une seule fois par date d'arrêté : NB_AGENTS
--           et NB_CADRES s'additionnent sur tous les axes, sauf ID_TEMPS.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- OÙ CHANGER QUOI QUAND LA CNSS RÉPOND
-- Tout se règle dans la partie 0, en tête de requête. Les numéros sont ceux de
-- points_a_valider_v2.md. « Aujourd'hui » = la valeur écrite dans ce fichier.
--
--   Point     Sujet                              Réglage                  Aujourd'hui
--   20        temporaire sans fin de contrat     fin_contrat_inconnue     PRESENT
--   45        fiche sans état ancienne           fiche_sans_etat          PRESENT
--   64        fiche inactive après une           inactif_sans_depart      JAMAIS_COMPTE
--             disponibilité sans retour
--   50        date d'entrée de l'agent           ordre_date_entree        EMBAUCHE
--   5, 51,    disponibilité, mise à              liste absences           07 hors effectif,
--   14        disposition, autres absences                                14 présent
--   22        retraite : sens de la date         liste decisions_depart   premier jour parti,
--             d'effet, retraite refaite                                   toutes les décisions
--   21, 23    radiation, fin de détachement      liste decisions_depart   pas de code connu
--   47, 41    catégorie HC, autres codes         regroupement_categorie   HC = cadre supérieur
--   48        domaine de chaque unité (T59)      correspondance_domaine   vide (à brancher)
--   58        T59 au mois                        periodicite              ANNUELLE
--   46        où lire les agents inactifs        inactifs                 DECISIONS
--   8         ancienneté (tranché le 03/11)      ordre_date_anciennete,   PRISE_SERVICE, OUI
--                                                retirer_interruptions
--
--   Les réglages marqués « COMMUN » et les listes du socle commun sont aussi
--   dans t60_requete_proposee.sql : les deux fichiers doivent garder les mêmes
--   valeurs, sinon la fin de T60 ne retombe plus sur T58. Le script
--   controles/construire.py le vérifie et s'arrête s'ils diffèrent.
--
--   Rien à changer dans la requête pour les points 2, 3, 25, 36, 37, 43 et 62 :
--   la CNSS corrige ou saisit dans l'application (décision de départ datée,
--   date de décès, unité de l'agent), et la requête le lit toute seule.
--   Les points 15, 17, 56, 57 et 59 ne touchent pas cette requête.
-- ─────────────────────────────────────────────────────────────────────────────
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
-- casser l'existant ; T59 se fait avec DOMAINE_ACTIVITE.
--
-- LES TABLEAUX DANS OBIEE (filtre global NATURE_EMPLOI, QUALITE_EMPLOYE = 'A') :
--   T58 : GROUPE_CATEGORIE × SEXE en colonnes, TRANCHE_AGE_CODE en lignes ;
--   T59 : GROUPE_CATEGORIE × SEXE en colonnes, DOMAINE_ACTIVITE en lignes ;
--   T61 : GROUPE_CATEGORIE × SEXE en colonnes, TRANCHE_ANC_CODE en lignes ;
--   T65 : SUM(NB_CADRES) / SUM(NB_AGENTS) par ID_TEMPS, sur 5 exercices.
--
-- LES RÈGLES (détail et exemples : analyses/grh/regles_effectif.md)
-- La « date d'arrêté » est le jour où l'on photographie l'effectif (le 31/12 ;
-- une fin de mois avec periodicite = MENSUELLE).
--   G1  Nature : permanent ou temporaire, d'après la dernière situation
--       commencée au plus tard à la date d'arrêté. Un agent sans situation
--       suit la règle des permanents. La table garde toutes les qualités
--       (A, B, C) ; la CIPRES se lit avec la qualité A.
--   G2  Permanent présent : entré au plus tard à la date d'arrêté, pas dans
--       une absence qui fait sortir de l'effectif, et pas encore parti.
--       Pas encore parti = la fiche n'est pas INACTIF, ou le premier départ
--       daté tombe après la date d'arrêté.
--   G7  Temporaire présent : la date d'arrêté tombe dans un de ses contrats.
--       Début = engagement 50, non annulé. Fin = premier départ après le
--       début, sinon veille du contrat suivant, sinon fin inconnue (point 20).
--   G3  Catégorie de la dernière situation, regroupée par la liste
--       regroupement_categorie, qui dit aussi qui est cadre (G6, T65).
--   G4  Âge en années pleines à la date d'arrêté. Date de naissance absente
--       ou âge hors de 0 à 119 : l'agent reste compté, tranche vide.
--   G5  Ancienneté en années pleines depuis la prise de service (à défaut
--       l'embauche), moins les interruptions : les absences marquées RETIREE
--       jusqu'à la remise en activité, et un départ suivi d'un nouvel
--       engagement (50 ou 51).
--   G12 Domaine de l'unité de la dernière situation, lu dans la
--       correspondance de la CNSS ; sinon « Domaine non renseigné ».
--   G13 Inactif : agent compté alors qu'une absence est en cours. Avec la
--       disponibilité hors effectif, NB_AGENTS_INACTIFS vaut 0.
--
-- CONTRÔLE : controles/controle_1_effectif.sql, construit à partir de ce
-- fichier par controles/construire.py (à relancer après chaque modification).
-- Attendu avec les réglages d'aujourd'hui = résultats du 17/11/2026
-- (controles/resultats_2026-11-17.md) :
--   permanents au 31/12 : 809 (2019), 769, 734, 703, 670, 741 (2024), 883 ;
--   temporaires         : 248 (2019), 237, 244, 260, 268, 299 (2024), 338 ;
--   2024 : employés 177, maîtrise 206, cadres moyens 301, cadres supérieurs
--   57 ; 358 cadres sur 741 = 48,3 % ; aucun agent inactif.
-- Ce que les réglages doivent changer (mesuré sur la base le 17/11/2026) :
--   fin_contrat_inconnue = FICHE  : 43, 44, 45, 46, 47, 47 temporaires de
--                                   2020 à 2025 (lot 27, Q153) ;
--   inactif_sans_depart = ABSENCE : 12, 10, 9, 6 et 3 permanents de plus de
--                                   2020 à 2024 (lot 28, Q159) ;
--   fiche_sans_etat = RECENTE     : 5 permanents de moins fin 2024, tous de
--                                   60 ans et plus (lot 27, Q158).
-- =============================================================================

WITH

-- ═════════════════════════════════════════════════════════════════════════════
-- 0. LES RÉGLAGES
--    Pour appliquer une réponse de la CNSS : changer la valeur entre
--    apostrophes, ou une ligne d'une liste. Rien d'autre n'est à toucher.
-- ═════════════════════════════════════════════════════════════════════════════
parametres AS (
    SELECT
        -- ▶ POINT 20 (DRH) — COMMUN — Le temporaire dont la fin de contrat
        --   n'est pas connue (ni départ saisi, ni contrat suivant) :
        --     'PRESENT'  il reste compté (aujourd'hui : 299 fin 2024)
        --     'FICHE'    il est compté seulement si sa fiche n'est pas
        --                INACTIF (47 fin 2024, pour 46 publiés)
        --     'DUREE'    son contrat dure duree_contrat_mois mois
        'PRESENT'               AS fin_contrat_inconnue,
        18                      AS duree_contrat_mois,

        -- ▶ POINT 45 (DRH) — COMMUN — La fiche sans état (ni ACTIF ni INACTIF) :
        --     'PRESENT'  l'agent est compté comme encore là (aujourd'hui)
        --     'RECENTE'  il l'est seulement s'il est entré depuis moins de
        --                fiche_sans_etat_mois mois avant la date d'arrêté ;
        --                au-delà, la fiche est lue comme une fiche INACTIF
        'PRESENT'               AS fiche_sans_etat,
        24                      AS fiche_sans_etat_mois,

        -- ▶ POINT 64 (DRH) — COMMUN — La fiche INACTIF d'un agent parti en
        --   absence (liste absences, effet HORS_EFFECTIF) et jamais revenu,
        --   sans départ daté depuis :
        --     'JAMAIS_COMPTE'  il n'est compté à aucune date (aujourd'hui)
        --     'ABSENCE'        il est compté jusqu'au début de cette absence
        --                      (correction prévue : 12 agents de 2021 à 2025)
        'JAMAIS_COMPTE'         AS inactif_sans_depart,

        -- ▶ POINT 50 (DRH) — COMMUN — La date d'entrée de l'agent :
        --     'EMBAUCHE'       date d'embauche, à défaut prise de service
        --                      (aujourd'hui)
        --     'PRISE_SERVICE'  prise de service, à défaut date d'embauche
        'EMBAUCHE'              AS ordre_date_entree,

        -- ▶ POINT 58 (DRH) — Les dates d'arrêté :
        --     'ANNUELLE'   le 31/12 de chaque exercice clos (aujourd'hui)
        --     'MENSUELLE'  en plus, chaque fin de mois clos de l'année passée
        --                  et de l'année en cours (retenu pour T59 le 19/11).
        --                  Avant de l'activer, NB_AGENTS doit être réglé en
        --                  « dernière valeur » dans OBIEE, sinon une année
        --                  additionnerait ses 12 mois.
        'ANNUELLE'              AS periodicite,

        -- ▶ POINT 8 (tranché le 03/11) — Le début de l'ancienneté (G5) :
        --     'PRISE_SERVICE'  prise de service, à défaut embauche (aujourd'hui)
        --     'EMBAUCHE'       date d'embauche, à défaut prise de service
        'PRISE_SERVICE'         AS ordre_date_anciennete,
        --   et les interruptions : 'OUI' retirées de l'ancienneté (aujourd'hui),
        --   'NON' gardées
        'OUI'                   AS retirer_interruptions,

        -- ▶ POINT 46 (DRH) — Où lire les agents inactifs (G13) :
        --     'DECISIONS'  dans les décisions d'absence (aujourd'hui)
        --     'STATUT'     dans la position de la situation (B, D, E, F, H),
        --                  qui n'est pas datée
        'DECISIONS'             AS inactifs
    FROM DUAL
),

-- >>> SOCLE COMMUN A — même texte dans t58_t65_effectif et dans t60 (vérifié par controles/construire.py)

-- ▶ POINTS 22, 21, 23 (DRH) — Les décisions de départ (GRH_DECISION.CDEC_CODE),
--   une ligne par code.
--     type_mvt    la colonne de T60 où tombe la sortie (GRH_TYPE_MOUVEMENT)
--     date_effet  ce que veut dire la date d'effet de la décision :
--                   'PREMIER_JOUR_PARTI'      l'agent n'est déjà plus compté
--                                             ce jour-là (aujourd'hui)
--                   'DERNIER_JOUR_TRAVAILLE'  il est encore compté ce jour-là
--                                             et sort le lendemain
--     retenir     quand un agent a plusieurs décisions du même code :
--                   'TOUTES'    chacune compte : un permanent sort donc à la
--                               première (aujourd'hui)
--                   'DERNIERE'  seule la dernière compte. Prévu pour la
--                               retraite refaite (02 : 22 cas). À ne pas
--                               mettre sur 35 : un temporaire a une fin de
--                               contrat par contrat.
--   Radiation (point 21), fin de détachement (point 23) : aucune décision
--   datée n'existe aujourd'hui. Si la DRH donne un code, ajouter une ligne :
--     SELECT 'xx', 'AUTRE_SORT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'radiation' FROM DUAL
--     SELECT 'xx', 'FIN_DETACH', 'PREMIER_JOUR_PARTI', 'TOUTES', 'fin de détachement' FROM DUAL
decisions_depart AS (
    SELECT '02' AS code, 'RET' AS type_mvt, 'PREMIER_JOUR_PARTI' AS date_effet, 'TOUTES' AS retenir,
           'admission à la retraite' AS libelle                                             FROM DUAL UNION ALL
    SELECT '34', 'DEC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'liquidation des droits après décès' FROM DUAL UNION ALL
    SELECT '35', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'fin de contrat'                     FROM DUAL UNION ALL
    SELECT '36', 'DEM',       'PREMIER_JOUR_PARTI', 'TOUTES', 'démission'                          FROM DUAL UNION ALL
    SELECT '52', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'rupture du contrat'                 FROM DUAL UNION ALL
    SELECT '54', 'LIC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'licenciement'                       FROM DUAL
),

-- ▶ POINTS 5, 51, 14 (DRH) — Les absences longues, une ligne par code de
--   décision. L'absence commence à la décision et finit à la remise en
--   activité (décision 18).
--     effet       pendant l'absence :
--                   'HORS_EFFECTIF'  l'agent sort de l'effectif ; T60 compte
--                                    une sortie (type_mvt), puis une entrée
--                                    REM au retour
--                   'INACTIF'        il reste compté, dans NB_AGENTS_INACTIFS
--                   'PRESENT'        il reste compté, comme un agent actif
--     anciennete  'RETIREE' : l'absence est retirée de l'ancienneté (G5) ;
--                 'COMPTEE' : elle ne l'est pas
--     type_mvt    le code de la sortie dans T60 (GRH_TYPE_MOUVEMENT)
--   07 : tranché le 08/11 (point 5), la DRH peut encore le revoir.
--   14 : aujourd'hui l'agent mis à disposition reste compté (point 51).
--   Détaché, suspendu, sous les drapeaux (point 14) : aucune décision datée
--   n'existe ; ils restent comptés. Si la DRH donne un code, ajouter une ligne.
absences AS (
    SELECT '07' AS code, 'HORS_EFFECTIF' AS effet, 'RETIREE' AS anciennete, 'DISP' AS type_mvt,
           'mise en disponibilité' AS libelle                                 FROM DUAL UNION ALL
    SELECT '14', 'PRESENT',       'COMPTEE', 'MISE_DIS', 'mise à disposition' FROM DUAL
),

-- Les autres codes de décision utilisés
codes_decision AS (
    SELECT '40' AS annulation,
           '50' AS engagement_temporaire,
           '51' AS engagement_permanent,
           '18' AS remise_en_activite,
           '37' AS classement_detache
    FROM DUAL
),

-- <<< SOCLE COMMUN A

-- >>> CATEGORIES — regroupement pour T58/T59/T61/T65 (T62 a encore sa propre grille)

-- ▶ POINTS 47, 41 (DRH, DSI) — Le regroupement des catégories de la grille
--   (GRH_SITUATION.CATEGORIE), une ligne par code. La colonne cadre dit qui
--   entre dans NB_CADRES (G6, T65). Un code absent de la liste tombe en
--   SANS_CATEGORIE, pas cadre.
--   01 à 06 : recueil des besoins v7, RH ligne 1. HC : proposition BI
--   (2 agents, fiches inactives ; point 47).
regroupement_categorie AS (
    SELECT '01' AS code, 'EMPLOYES' AS groupe, 'NON' AS cadre FROM DUAL UNION ALL
    SELECT '02', 'EMPLOYES',      'NON'                       FROM DUAL UNION ALL
    SELECT '03', 'MAITRISE',      'NON'                       FROM DUAL UNION ALL
    SELECT '04', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
    SELECT '05', 'CADRES_MOYENS', 'OUI'                       FROM DUAL UNION ALL
    SELECT '06', 'CADRES_SUP',    'OUI'                       FROM DUAL UNION ALL
    SELECT 'HC', 'CADRES_SUP',    'OUI'                       FROM DUAL
),

-- <<< CATEGORIES

-- ▶ POINT 48 (DRH) — Le domaine de chaque unité (G12, T59), d'après le fichier
--   correspondance_unite_domaine.xlsx rempli par la CNSS.
--   À BRANCHER quand la table sera chargée : remplacer les deux lignes du
--   SELECT par « SELECT UA_CODE, DOMAINE FROM DTM.REF_GRH_UNITE_DOMAINE ».
--   Tant qu'elle est vide, tous les agents sont en « Domaine non renseigné ».
correspondance_domaine AS (
    SELECT CAST(NULL AS VARCHAR2(12)) AS UA_CODE, CAST(NULL AS VARCHAR2(60)) AS DOMAINE
    FROM DUAL WHERE 1 = 0
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 1. LES DATES D'ARRÊTÉ
--    Le 31/12 de chaque exercice clos (une année où commence au moins une
--    situation, jusqu'à l'année dernière). En MENSUELLE, s'y ajoute la fin de
--    chaque mois clos de l'année passée et de l'année en cours.
-- ═════════════════════════════════════════════════════════════════════════════
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

-- >>> SOCLE COMMUN B — même texte dans t58_t65_effectif et dans t60 (vérifié par controles/construire.py)

-- ═════════════════════════════════════════════════════════════════════════════
-- 2. LES AGENTS
-- ═════════════════════════════════════════════════════════════════════════════
personnes AS (
    SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS,
           p.DATE_EMBAUCHE                                        AS date_embauche,
           p.DATE_PRISE_SERVICE                                   AS date_prise_service,
           -- État de la fiche : ACTIF, INACTIF, ou « - » si elle n'en a pas
           NVL(p.ETAT_EMPLOYE, '-')                               AS etat,
           -- Qualité : A agent CNSS, B fonction publique, C agent affecté
           TO_CHAR(p.QUALITE)                                     AS qualite,
           -- G2, point 50 : la date d'entrée
           CASE pr.ordre_date_entree
               WHEN 'PRISE_SERVICE' THEN COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
               ELSE                      COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
           END                                                    AS date_entree
    FROM DWH.FAIT_GRH_PERSONNE p
    CROSS JOIN parametres pr
    WHERE p.CLICHE = :1
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 3. LES DÉCISIONS ET LES DÉPARTS
-- ═════════════════════════════════════════════════════════════════════════════
-- Les décisions utiles, sans celles annulées par une décision 40. L'agent est
-- porté par la décision, ou par sa liste quand elle est collective.
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
           OR TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM absences)
           OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                       cd.remise_en_activite, cd.classement_detache))
      AND NOT EXISTS (SELECT 1
                      FROM DWH.FAIT_GRH_DECISION a
                      WHERE a.CLICHE = :1
                        AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
-- Les départs : les décisions de départ, et la date de décès. Pour chacun :
-- le jour où l'agent n'est plus compté (date_depart), la colonne de T60
-- (type_mvt) et ce qui l'explique (motif).
departs AS (
    SELECT x.PERS_ID,
           -- point 22 : le sens de la date d'effet
           x.date_decision
               + CASE x.date_effet WHEN 'DERNIER_JOUR_TRAVAILLE' THEN 1 ELSE 0 END AS date_depart,
           x.type_mvt,
           'DECISION_' || x.code                                  AS motif
    FROM (SELECT dc.PERS_ID, dc.code, dc.date_decision,
                 dd.type_mvt, dd.date_effet, dd.retenir,
                 -- 1 = la dernière décision de ce code pour cet agent
                 ROW_NUMBER() OVER (PARTITION BY dc.PERS_ID, dc.code
                                    ORDER BY dc.date_decision DESC) AS rang_fin
          FROM decisions dc
          JOIN decisions_depart dd ON dd.code = dc.code
          WHERE dc.PERS_ID IS NOT NULL) x
    -- point 22 : la retraite refaite
    WHERE x.retenir = 'TOUTES'
       OR x.rang_fin = 1
    UNION ALL
    SELECT PERS_ID, DATE_DECES, 'DEC', 'DATE_DECES'
    FROM DWH.FAIT_GRH_PERSONNE
    WHERE CLICHE = :1
      AND DATE_DECES IS NOT NULL
),
-- G2 : le premier départ de chaque agent (à date égale, le premier motif)
premier_depart AS (
    SELECT PERS_ID, date_depart, type_mvt, motif
    FROM (SELECT x.PERS_ID, x.date_depart, x.type_mvt, x.motif,
                 ROW_NUMBER() OVER (PARTITION BY x.PERS_ID
                                    ORDER BY x.date_depart, x.motif) AS rn
          FROM departs x)
    WHERE rn = 1
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 4. LES CONTRATS DES TEMPORAIRES (G7)
-- ═════════════════════════════════════════════════════════════════════════════
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
-- La fin de chaque contrat (jour exclu : l'agent est compté si
-- debut <= date d'arrêté < fin).
--   fin_base : premier départ après le début ; sinon début du contrat
--              suivant ; sinon 31/12/9999 = la base ne donne aucune fin.
--   fin      : fin_base, ou ce que dit le réglage fin_contrat_inconnue quand
--              la base ne donne aucune fin. Une fin vide écarte le contrat.
contrats AS (
    SELECT x.PERS_ID, x.debut,
           CASE
               WHEN x.fin_base < DATE '9999-12-31'       THEN x.fin_base
               -- point 20 : la fin n'est pas connue
               WHEN pr.fin_contrat_inconnue = 'DUREE'    THEN ADD_MONTHS(x.debut, pr.duree_contrat_mois)
               WHEN pr.fin_contrat_inconnue = 'FICHE'
                AND p.etat = 'INACTIF'                   THEN CAST(NULL AS DATE)
               ELSE                                           x.fin_base
           END                                                    AS fin
    FROM (SELECT g.PERS_ID, g.debut,
                 LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                       NVL(g.debut_suivant, DATE '9999-12-31'))   AS fin_base
          FROM engagements g
          LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                               AND dep.date_depart > g.debut
          GROUP BY g.PERS_ID, g.debut, g.debut_suivant) x
    JOIN personnes p ON p.PERS_ID = x.PERS_ID
    CROSS JOIN parametres pr
),
-- Les temporaires sous contrat à chaque date d'arrêté
temporaires_sous_contrat AS (
    SELECT DISTINCT da.date_arrete, c.PERS_ID
    FROM dates da
    JOIN contrats c ON c.debut       <= da.date_arrete
                   AND da.date_arrete <  c.fin
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 5. LES ABSENCES (points 5, 51, 14, 64)
-- ═════════════════════════════════════════════════════════════════════════════
-- La dernière remise en activité (18) au plus tard à chaque date d'arrêté
remises AS (
    SELECT da.date_arrete, d.PERS_ID, MAX(d.date_decision) AS date_remise
    FROM dates da
    JOIN decisions d ON d.date_decision <= da.date_arrete
    CROSS JOIN codes_decision cd
    WHERE d.PERS_ID IS NOT NULL
      AND d.code = cd.remise_en_activite
    GROUP BY da.date_arrete, d.PERS_ID
),
-- Les absences en cours à chaque date d'arrêté : la dernière décision de
-- chaque code de la liste absences, si elle est plus récente que la dernière
-- remise en activité. Une ligne par date, agent et code d'absence.
absences_en_cours AS (
    SELECT x.date_arrete, x.PERS_ID, x.code, x.effet, x.type_mvt, x.date_absence
    FROM (SELECT da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt,
                 MAX(d.date_decision) AS date_absence
          FROM dates da
          JOIN decisions d ON d.date_decision <= da.date_arrete
          JOIN absences ab ON ab.code = d.code
          WHERE d.PERS_ID IS NOT NULL
          GROUP BY da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt) x
    LEFT JOIN remises r ON r.date_arrete = x.date_arrete
                       AND r.PERS_ID     = x.PERS_ID
    WHERE x.date_absence > NVL(r.date_remise, DATE '1900-01-01')
),
-- Ce que ces absences font de l'agent à chaque date d'arrêté (une ligne par
-- date et agent en absence) :
--   hors_effectif = 1 : une absence en cours le sort de l'effectif ;
--   inactif       = 1 : s'il est compté, c'est comme inactif (G13).
positions AS (
    SELECT date_arrete, PERS_ID,
           MAX(CASE WHEN effet = 'HORS_EFFECTIF' THEN 1 ELSE 0 END)              AS hors_effectif,
           MAX(CASE WHEN effet IN ('HORS_EFFECTIF', 'INACTIF') THEN 1 ELSE 0 END) AS inactif
    FROM absences_en_cours
    GROUP BY date_arrete, PERS_ID
),
-- Point 64 : la dernière absence « hors effectif » d'un agent, quand il n'en
-- est jamais revenu (aucune remise en activité depuis) et qu'aucun départ
-- n'est daté après elle. Une ligne par agent concerné.
absence_sans_retour AS (
    SELECT x.PERS_ID, x.date_absence
    FROM (SELECT d.PERS_ID, MAX(d.date_decision) AS date_absence
          FROM decisions d
          JOIN absences ab ON ab.code = d.code
          WHERE d.PERS_ID IS NOT NULL
            AND ab.effet = 'HORS_EFFECTIF'
          GROUP BY d.PERS_ID) x
    WHERE NOT EXISTS (SELECT 1
                      FROM decisions r, codes_decision cd
                      WHERE r.PERS_ID       =  x.PERS_ID
                        AND r.code          =  cd.remise_en_activite
                        AND r.date_decision >= x.date_absence)
      AND NOT EXISTS (SELECT 1
                      FROM departs dep
                      WHERE dep.PERS_ID     = x.PERS_ID
                        AND dep.date_depart > x.date_absence)
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 6. LA DERNIÈRE SITUATION DE CHAQUE AGENT À CHAQUE DATE (G1)
-- ═════════════════════════════════════════════════════════════════════════════
-- Dernière situation commencée au plus tard à la date d'arrêté ; à date
-- égale, la dernière saisie. Elle donne la nature et tous les axes.
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

-- ═════════════════════════════════════════════════════════════════════════════
-- 7. LES AGENTS PRÉSENTS À CHAQUE DATE D'ARRÊTÉ (G2, G7)
--    Une ligne par agent présent et par date. C'est la photo de l'effectif :
--    T58 la compte, T60 compare deux photos.
-- ═════════════════════════════════════════════════════════════════════════════
presents AS (
    SELECT da.date_arrete, p.PERS_ID, p.SEXE,
           p.qualite                                              AS QUALITE_EMPLOYE,
           si.nature                                              AS NATURE_EMPLOI,
           si.QUAL_CODE, si.CODE_DOMAINE_ACT, si.UA_CODE, si.LIEU_ID, si.FNCT_CODE,
           si.STATUT_EMPLOYE,
           si.categorie                                           AS CATEGORIE,
           -- G13 : 1 = compté alors qu'une absence est en cours
           NVL(po.inactif, 0)                                     AS en_absence
    FROM dates da
    CROSS JOIN personnes p
    CROSS JOIN parametres pr
    LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                         AND si.PERS_ID     = p.PERS_ID
                                         AND si.rn          = 1
    LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
    LEFT JOIN absence_sans_retour asr     ON asr.PERS_ID    = p.PERS_ID
    LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                         AND po.PERS_ID     = p.PERS_ID
    LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                         AND tc.PERS_ID     = p.PERS_ID
    WHERE
            -- G2 : le permanent (ou l'agent sans situation)
            (NVL(si.nature, '-') <> 'TEMPORAIRE'
             -- il est entré
             AND p.date_entree <= da.date_arrete
             -- il n'est pas dans une absence qui sort de l'effectif
             AND NVL(po.hors_effectif, 0) = 0
             -- il n'est pas encore parti :
             AND (   -- sa fiche ne dit pas qu'il est parti (elle n'est pas
                     -- INACTIF, ni sans état depuis trop longtemps : point 45)
                     NOT (p.etat = 'INACTIF'
                          OR (p.etat = '-'
                              AND pr.fiche_sans_etat = 'RECENTE'
                              AND p.date_entree <= ADD_MONTHS(da.date_arrete,
                                                              -pr.fiche_sans_etat_mois)))
                     -- ou son premier départ daté tombe après la date d'arrêté
                  OR pd.date_depart > da.date_arrete
                     -- ou il est parti en absence sans revenir, plus tard (point 64)
                  OR (pr.inactif_sans_depart = 'ABSENCE'
                      AND asr.date_absence > da.date_arrete)))
         OR
            -- G7 : le temporaire sous contrat
            (si.nature = 'TEMPORAIRE'
             AND tc.PERS_ID IS NOT NULL)
),

-- <<< SOCLE COMMUN B

-- ═════════════════════════════════════════════════════════════════════════════
-- 8. L'ANCIENNETÉ (G5)
-- ═════════════════════════════════════════════════════════════════════════════
-- Le début de l'ancienneté de chaque agent
debuts_anciennete AS (
    SELECT p.PERS_ID,
           CASE pr.ordre_date_anciennete
               WHEN 'EMBAUCHE' THEN COALESCE(p.date_embauche, p.date_prise_service)
               ELSE                 COALESCE(p.date_prise_service, p.date_embauche)
           END                                                    AS debut_anciennete
    FROM personnes p
    CROSS JOIN parametres pr
),
-- Les périodes à retirer de l'ancienneté : une absence marquée RETIREE, de la
-- décision à la remise en activité ; un départ suivi d'un nouvel engagement
-- (50 ou 51).
interruptions AS (
    SELECT d.PERS_ID, d.date_decision AS debut,
           (SELECT MIN(r.date_decision)
            FROM decisions r, codes_decision c2
            WHERE r.PERS_ID       = d.PERS_ID
              AND r.code          = c2.remise_en_activite
              AND r.date_decision > d.date_decision)          AS fin
    FROM decisions d
    JOIN absences ab ON ab.code = d.code
    WHERE d.PERS_ID IS NOT NULL
      AND ab.anciennete = 'RETIREE'
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
-- Le nombre de jours d'interruption entre le début de l'ancienneté et la date
-- d'arrêté (seulement le morceau de chaque période qui tombe dans cet
-- intervalle ; une absence sans retour court jusqu'à la date d'arrêté).
interruptions_a_date AS (
    SELECT da.date_arrete, i.PERS_ID,
           SUM(GREATEST(0, LEAST(NVL(i.fin, da.date_arrete), da.date_arrete)
                           - GREATEST(i.debut, an.debut_anciennete))) AS jours
    FROM dates da
    JOIN interruptions i      ON i.debut < da.date_arrete
    JOIN debuts_anciennete an ON an.PERS_ID = i.PERS_ID
    GROUP BY da.date_arrete, i.PERS_ID
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 9. LES AXES ET LES MESURES DE CHAQUE AGENT PRÉSENT
--    Une ligne par agent et par date d'arrêté.
-- ═════════════════════════════════════════════════════════════════════════════
base AS (
    SELECT pe.date_arrete, pe.PERS_ID, pe.SEXE,
           pe.QUALITE_EMPLOYE, pe.NATURE_EMPLOI,
           pe.QUAL_CODE,
           pe.CODE_DOMAINE_ACT                                    AS CODE_DOMAINE,
           pe.UA_CODE, pe.LIEU_ID, pe.FNCT_CODE,
           pe.CATEGORIE,
           -- G3 : le groupe de catégories, et s'il compte comme cadre
           NVL(rc.groupe, 'SANS_CATEGORIE')                       AS GROUPE_CATEGORIE,
           NVL(rc.cadre, 'NON')                                   AS cadre,
           -- G12 : le domaine de l'unité
           NVL(cdo.DOMAINE, 'Domaine non renseigné')              AS DOMAINE_ACTIVITE,
           -- G4 : âge en années pleines ; vide si la date est absente ou fausse
           CASE WHEN FLOOR(MONTHS_BETWEEN(pe.date_arrete, p.DATE_NAISS) / 12)
                     BETWEEN 0 AND 119
                THEN FLOOR(MONTHS_BETWEEN(pe.date_arrete, p.DATE_NAISS) / 12)
           END                                                    AS age,
           -- G5 : ancienneté en années pleines, moins les interruptions
           GREATEST(FLOOR((pe.date_arrete - an.debut_anciennete
                           - CASE pr.retirer_interruptions
                                 WHEN 'OUI' THEN NVL(ia.jours, 0) ELSE 0 END)
                          / 365.25), 0)                           AS anciennete,
           -- G13, point 46 : 1 = inactif, 0 = actif
           CASE WHEN pr.inactifs = 'STATUT'
                -- B détaché, D mis à disposition, E en disponibilité,
                -- F suspendu, H sous les drapeaux
                THEN CASE WHEN pe.STATUT_EMPLOYE IN ('B', 'D', 'E', 'F', 'H')
                          THEN 1 ELSE 0 END
                ELSE pe.en_absence
           END                                                    AS inactif
    FROM presents pe
    CROSS JOIN parametres pr
    JOIN personnes p                      ON p.PERS_ID      = pe.PERS_ID
    JOIN debuts_anciennete an             ON an.PERS_ID     = pe.PERS_ID
    LEFT JOIN interruptions_a_date ia     ON ia.date_arrete = pe.date_arrete
                                         AND ia.PERS_ID     = pe.PERS_ID
    LEFT JOIN regroupement_categorie rc   ON rc.code        = pe.CATEGORIE
    LEFT JOIN correspondance_domaine cdo  ON cdo.UA_CODE    = TO_CHAR(pe.UA_CODE)
)

-- ═════════════════════════════════════════════════════════════════════════════
-- 10. LES TRANCHES D'ÂGE ET D'ANCIENNETÉ, PUIS LES COMPTAGES PAR AXE
--     Comme avant : une tranche par agent (bornes incluses), un agent compté
--     une fois par date d'arrêté, sur la ligne de sa combinaison d'axes.
--     Jointures externes : un âge inconnu donne une tranche vide, l'agent
--     reste compté.
-- ═════════════════════════════════════════════════════════════════════════════
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
    -- G13 : inactifs = agents comptés avec inactif = 1
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
