-- =============================================================================
-- DTM_GRH_MOUVEMENT — REQUÊTE PROPOSÉE : T60
-- Version du 21/11/2026. Alignée sur t58_t65_effectif_requete_proposee.sql :
-- mêmes réglages COMMUN, même socle (présence G2/G7), pour que la fin de T60
-- retombe sur T58. Avec les réglages d'aujourd'hui, les chiffres restent ceux
-- du 17/11/2026 (controles/resultats_2026-11-17.md).
--
-- MÊME STRUCTURE que domaines/grh/dtm/sql/grh_mouvement.sql : mêmes colonnes,
-- même grain, même paramètre. Quatre colonnes s'ajoutent à la fin, aucune n'est
-- retirée. Après le visa de la DRH, ce fichier remplace grh_mouvement.sql.
--
-- Sources : DWH.FAIT_GRH_PERSONNE, FAIT_GRH_SITUATION, FAIT_GRH_DECISION,
--           FAIT_GRH_DECISION_PERSONNE, FAIT_GRH_MOUVEMENT ;
--           DTM.DIM_GRH_TYPE_MOUVEMENT.
-- :1 = CLICHE (MMYYYY), saisi sans guillemets. C'est la seule valeur à saisir.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- OÙ CHANGER QUOI QUAND LA CNSS RÉPOND
-- Les réglages COMMUN et les listes du socle (décisions, absences) sont les
-- mêmes que dans t58_t65_effectif_requete_proposee.sql. controles/construire.py
-- s'arrête s'ils diffèrent. Les numéros sont ceux de points_a_valider_v2.md.
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
--   24        entré et sorti la même année       passages_annee           NON
--   49 / G17  mouvements internes                mouvements_internes      COMME_AVANT
--
--   Rien à changer ici pour les points 2, 3, 25, 36, 37 : la CNSS corrige dans
--   l'application, la requête le lit toute seule.
-- ─────────────────────────────────────────────────────────────────────────────
--
-- COLONNES : celles de grh_mouvement.sql, dans le même ordre :
--   ID_TEMPS, CODE_TYPE_MVT, CODE_NAT_MVT, CODE_SENS_MVT, QUAL_CODE, SEXE,
--   LIBELLE_SEXE, NB_MOUVEMENTS, NB_AGENTS, DATE_CHARGEMENT, CLICHE
-- puis les colonnes AJOUTÉES :
--   QUALITE_EMPLOYE, NATURE_EMPLOI, UA_CODE, MOTIF_MVT
--
-- T60 DANS OBIEE (filtres QUALITE_EMPLOYE = 'A' et NATURE_EMPLOI, global) :
--   Effectif début = SUM(NB_AGENTS) des lignes DEBUT
--   Recrutement / autres entrées / retraite / décès / autres sorties
--   Effectif fin = début + entrées − sorties (= T58 au 31/12)
--
-- CONTRÔLE : controles/controle_2_t60.sql, construit par controles/construire.py.
-- Attendu avec les réglages d'aujourd'hui = résultats du 17/11/2026 :
--   2024 permanents : 670 · 101 · 0 · 24 · 5 · 1 · 741 (égalité chaque année).
-- RÈGLES : analyses/grh/regles_t60.md (présence = socle commun de T58).
-- =============================================================================

WITH

-- ═════════════════════════════════════════════════════════════════════════════
-- 0. LES RÉGLAGES
--    COMMUN : mêmes valeurs que t58_t65_effectif (partie 0). T60 seulement :
--    passages_annee, mouvements_internes.
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

        -- ▶ POINT 24 (DRH) — T60 seulement — Entré et sorti dans l'exercice :
        --     'NON'  pas compté (aujourd'hui)
        --     'OUI'  une entrée RECRU et une sortie (code du départ)
        'NON'                   AS passages_annee,

        -- ▶ G17 — T60 seulement — Mouvements internes de GRH_MOUVEMENT :
        --     'COMME_AVANT'  repris comme avant (aujourd'hui)
        --     'AUCUN'        retirés
        'COMME_AVANT'           AS mouvements_internes
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


-- ═════════════════════════════════════════════════════════════════════════════
-- 1. LES DATES D'ARRÊTÉ (T60)
--    Le 31/12 de chaque année, sans trou, de la première situation à l'année
--    dernière. Chaque exercice a besoin du 31/12 de l'année d'avant.
-- ═════════════════════════════════════════════════════════════════════════════
annees AS (
    SELECT 1899 + LEVEL AS annee
    FROM DUAL
    CONNECT BY LEVEL <= EXTRACT(YEAR FROM SYSDATE) - 1900
),
dates AS (
    SELECT TO_DATE(TO_CHAR(a.annee) || '1231', 'YYYYMMDD')            AS date_arrete
    FROM annees a
    WHERE a.annee >= (SELECT MIN(EXTRACT(YEAR FROM s.DATE_DEBUT))
                      FROM DWH.FAIT_GRH_SITUATION s
                      WHERE s.CLICHE      = :1
                        AND s.DATE_DEBUT IS NOT NULL
                        AND EXTRACT(YEAR FROM s.DATE_DEBUT) >= 1900)
),
-- Les exercices : d0 = 31/12 de l'année d'avant (début), d1 = 31/12 (fin)
exercices AS (
    SELECT EXTRACT(YEAR FROM da.date_arrete)                          AS annee,
           ADD_MONTHS(da.date_arrete, -12)                            AS d0,
           da.date_arrete                                             AS d1
    FROM dates da
    WHERE ADD_MONTHS(da.date_arrete, -12) IN (SELECT date_arrete FROM dates)
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
-- 8. LES DEUX PHOTOS DE CHAQUE EXERCICE (G14)
--    Présence = CTE presents du socle commun (mêmes règles que T58).
-- ═════════════════════════════════════════════════════════════════════════════
au_debut AS (
    SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
           p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
    FROM exercices e
    JOIN presents p ON p.date_arrete = e.d0
),
a_la_fin AS (
    SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
           p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
    FROM exercices e
    JOIN presents p ON p.date_arrete = e.d1
),
-- Le dernier départ daté dans chaque exercice (G9, temporaires)
dernier_depart_annee AS (
    SELECT annee, PERS_ID, type_mvt, motif
    FROM (SELECT e.annee, dep.PERS_ID, dep.type_mvt, dep.motif,
                 ROW_NUMBER() OVER (PARTITION BY e.annee, dep.PERS_ID
                                    ORDER BY dep.date_depart DESC, dep.motif DESC) AS rn
          FROM exercices e
          JOIN departs dep ON dep.date_depart >  e.d0
                          AND dep.date_depart <= e.d1)
    WHERE rn = 1
),
-- Les agents classés comme détachés (décision 37) dans chaque exercice (G8)
detaches_annee AS (
    SELECT DISTINCT e.annee, d.PERS_ID
    FROM exercices e
    JOIN decisions d ON d.date_decision >  e.d0
                    AND d.date_decision <= e.d1
    CROSS JOIN codes_decision cd
    WHERE d.PERS_ID IS NOT NULL
      AND d.code = cd.classement_detache
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 9. LES ENTRÉES (G8)
-- ═════════════════════════════════════════════════════════════════════════════
entrees AS (
    SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
           x.QUAL_CODE, x.UA_CODE,
           CASE x.cas
               WHEN 'DETACHEMENT' THEN 'DETAC'
               WHEN 'EMBAUCHE'    THEN 'RECRU'
               WHEN 'ENGAGEMENT'  THEN 'RECRU'
               WHEN 'REMISE'      THEN 'REM'
               ELSE                    'AUTRE_ENT'
           END                                                    AS CODE_TYPE_MVT,
           CASE x.cas
               WHEN 'DETACHEMENT' THEN 'DECISION_' || x.code_detache
               WHEN 'EMBAUCHE'    THEN 'EMBAUCHE'
               WHEN 'ENGAGEMENT'  THEN 'ENGAGEMENT_' || x.code_engagement
               WHEN 'REMISE'      THEN 'DECISION_' || x.code_remise
               WHEN 'NATURE'      THEN 'ETAIT_' || NVL(x.nature_debut, 'SANS_SITUATION')
               ELSE                    'SANS_MOTIF'
           END                                                    AS MOTIF_MVT
    FROM (SELECT f.*,
                 dbt.NATURE_EMPLOI          AS nature_debut,
                 cd.classement_detache      AS code_detache,
                 cd.engagement_temporaire   AS code_engagement,
                 cd.remise_en_activite      AS code_remise,
                 CASE
                     WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND dt.PERS_ID IS NOT NULL                       THEN 'DETACHEMENT'
                     WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND pe.date_entree >  f.d0
                      AND pe.date_entree <= f.d1                       THEN 'EMBAUCHE'
                     -- revenu d'une absence hors effectif (liste absences)
                     WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND NVL(po.hors_effectif, 0) = 1                 THEN 'REMISE'
                     WHEN f.NATURE_EMPLOI = 'TEMPORAIRE'
                      AND tc0.PERS_ID IS NULL                          THEN 'ENGAGEMENT'
                     WHEN dbt.PERS_ID IS NOT NULL                      THEN 'NATURE'
                     ELSE                                                   'SANS_MOTIF'
                 END                        AS cas
          FROM a_la_fin f
          JOIN personnes pe ON pe.PERS_ID = f.PERS_ID
          CROSS JOIN codes_decision cd
          LEFT JOIN au_debut dbt                 ON dbt.annee        = f.annee
                                                AND dbt.PERS_ID      = f.PERS_ID
          LEFT JOIN detaches_annee dt            ON dt.annee         = f.annee
                                                AND dt.PERS_ID       = f.PERS_ID
          -- absence hors effectif au début de l'exercice
          LEFT JOIN positions po                 ON po.date_arrete   = f.d0
                                                AND po.PERS_ID       = f.PERS_ID
          LEFT JOIN temporaires_sous_contrat tc0 ON tc0.date_arrete  = f.d0
                                                AND tc0.PERS_ID      = f.PERS_ID
          WHERE dbt.PERS_ID IS NULL
             OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')) x
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 10. LES SORTIES (G9)
-- ═════════════════════════════════════════════════════════════════════════════
sorties AS (
    SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
           x.QUAL_CODE, x.UA_CODE,
           CASE x.cas
               WHEN 'DEPART_PERMANENT'  THEN x.type_premier
               WHEN 'DEPART_TEMPORAIRE' THEN x.type_dernier
               WHEN 'ABSENCE'           THEN NVL(x.type_absence, 'DISP')
               ELSE                          'AUTRE_SORT'
           END                                                    AS CODE_TYPE_MVT,
           CASE x.cas
               WHEN 'DEPART_PERMANENT'  THEN x.motif_premier
               WHEN 'DEPART_TEMPORAIRE' THEN x.motif_dernier
               WHEN 'ABSENCE'           THEN 'DECISION_' || x.code_absence
               WHEN 'NATURE'            THEN 'DEVIENT_' || NVL(x.nature_fin, 'SANS_SITUATION')
               ELSE                          'SANS_MOTIF'
           END                                                    AS MOTIF_MVT
    FROM (SELECT dbt.*,
                 fin.NATURE_EMPLOI          AS nature_fin,
                 pd.type_mvt                AS type_premier,
                 pd.motif                   AS motif_premier,
                 dda.type_mvt               AS type_dernier,
                 dda.motif                  AS motif_dernier,
                 ae.code                    AS code_absence,
                 ae.type_mvt                AS type_absence,
                 CASE
                     WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND pd.date_depart >  dbt.d0
                      AND pd.date_depart <= dbt.d1                     THEN 'DEPART_PERMANENT'
                     WHEN dbt.NATURE_EMPLOI = 'TEMPORAIRE'
                      AND dda.PERS_ID IS NOT NULL                      THEN 'DEPART_TEMPORAIRE'
                     -- absence hors effectif à la fin (liste absences)
                     WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND NVL(po.hors_effectif, 0) = 1                 THEN 'ABSENCE'
                     WHEN fin.PERS_ID IS NOT NULL                      THEN 'NATURE'
                     ELSE                                                   'SANS_MOTIF'
                 END                        AS cas
          FROM au_debut dbt
          LEFT JOIN a_la_fin fin             ON fin.annee      = dbt.annee
                                            AND fin.PERS_ID    = dbt.PERS_ID
          LEFT JOIN premier_depart pd        ON pd.PERS_ID     = dbt.PERS_ID
          LEFT JOIN dernier_depart_annee dda ON dda.annee      = dbt.annee
                                            AND dda.PERS_ID    = dbt.PERS_ID
          LEFT JOIN positions po             ON po.date_arrete = dbt.d1
                                            AND po.PERS_ID     = dbt.PERS_ID
          LEFT JOIN absences_en_cours ae     ON ae.date_arrete = dbt.d1
                                            AND ae.PERS_ID     = dbt.PERS_ID
                                            AND ae.effet       = 'HORS_EFFECTIF'
          WHERE fin.PERS_ID IS NULL
             OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-')) x
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 11. ENTRÉ ET SORTI DANS LE MÊME EXERCICE (G16, passages_annee)
-- ═════════════════════════════════════════════════════════════════════════════
contrats_annee AS (
    SELECT DISTINCT e.annee, c.PERS_ID
    FROM exercices e
    JOIN contrats c ON c.debut >  e.d0
                   AND c.debut <= e.d1
                   AND c.fin   <= e.d1
                   AND c.fin   <  DATE '9999-12-31'
),
passages AS (
    SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite AS QUALITE_EMPLOYE,
           si.nature AS NATURE_EMPLOI, si.QUAL_CODE, si.UA_CODE,
           'EMBAUCHE' AS motif_entree, pd.type_mvt AS type_sortie, pd.motif AS motif_sortie
    FROM exercices e
    CROSS JOIN parametres pr
    JOIN personnes p       ON p.date_entree  >  e.d0
                          AND p.date_entree  <= e.d1
    JOIN premier_depart pd ON pd.PERS_ID     =  p.PERS_ID
                          AND pd.date_depart >  p.date_entree
                          AND pd.date_depart <= e.d1
    LEFT JOIN situations si ON si.date_arrete = e.d1
                           AND si.PERS_ID     = p.PERS_ID
                           AND si.rn          = 1
    LEFT JOIN au_debut dbt  ON dbt.annee      = e.annee
                           AND dbt.PERS_ID    = p.PERS_ID
    LEFT JOIN a_la_fin fin  ON fin.annee      = e.annee
                           AND fin.PERS_ID    = p.PERS_ID
    WHERE pr.passages_annee = 'OUI'
      AND p.etat = 'INACTIF'
      AND NVL(si.nature, '-') <> 'TEMPORAIRE'
      AND dbt.PERS_ID IS NULL
      AND fin.PERS_ID IS NULL
    UNION ALL
    SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite,
           si.nature, si.QUAL_CODE, si.UA_CODE,
           'ENGAGEMENT_' || cd.engagement_temporaire,
           NVL(dda.type_mvt, 'AUTRE_SORT'), NVL(dda.motif, 'SANS_MOTIF')
    FROM contrats_annee ca
    JOIN exercices e        ON e.annee        = ca.annee
    JOIN personnes p        ON p.PERS_ID      = ca.PERS_ID
    CROSS JOIN parametres pr
    CROSS JOIN codes_decision cd
    LEFT JOIN situations si ON si.date_arrete = e.d1
                           AND si.PERS_ID     = p.PERS_ID
                           AND si.rn          = 1
    LEFT JOIN dernier_depart_annee dda ON dda.annee   = ca.annee
                                      AND dda.PERS_ID = ca.PERS_ID
    LEFT JOIN au_debut dbt  ON dbt.annee      = ca.annee
                           AND dbt.PERS_ID    = ca.PERS_ID
    LEFT JOIN a_la_fin fin  ON fin.annee      = ca.annee
                           AND fin.PERS_ID    = ca.PERS_ID
    WHERE pr.passages_annee = 'OUI'
      AND si.nature = 'TEMPORAIRE'
      AND dbt.PERS_ID IS NULL
      AND fin.PERS_ID IS NULL
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 12. LES MOUVEMENTS INTERNES (G17)
-- ═════════════════════════════════════════════════════════════════════════════
internes AS (
    SELECT x.annee, x.CODE_TYPE_MVT, x.MVT_ID, x.PERS_ID, p.SEXE,
           TO_CHAR(p.QUALITE)                                     AS QUALITE_EMPLOYE,
           TO_CHAR(s.NATURE_EMPLOIE)                              AS NATURE_EMPLOI,
           s.QUAL_CODE, s.UA_CODE
    FROM (SELECT m.MVT_ID, m.PERS_ID, m.SITU_ID,
                 EXTRACT(YEAR FROM m.DATE_DEBUT)                  AS annee,
                 CASE UPPER(TRIM(m.NATURE_MVT))
                     WHEN 'DIS' THEN 'DISP'
                     WHEN 'DET' THEN 'DETAC'
                     ELSE UPPER(TRIM(m.NATURE_MVT))
                 END                                              AS CODE_TYPE_MVT
          FROM DWH.FAIT_GRH_MOUVEMENT m
          WHERE m.CLICHE      = :1
            AND m.DATE_DEBUT IS NOT NULL
            AND m.VALIDE      = 'O') x
    CROSS JOIN parametres pr
    LEFT JOIN DTM.DIM_GRH_TYPE_MOUVEMENT tm ON tm.CODE_TYPE_MVT = x.CODE_TYPE_MVT
    LEFT JOIN DWH.FAIT_GRH_PERSONNE p       ON p.PERS_ID = x.PERS_ID
                                           AND p.CLICHE  = :1
    LEFT JOIN DWH.FAIT_GRH_SITUATION s      ON s.SITU_ID = x.SITU_ID
                                           AND s.CLICHE  = :1
    WHERE pr.mouvements_internes = 'COMME_AVANT'
      AND NVL(tm.CODE_NAT_MVT, '-') NOT IN ('INITIAL', 'ENTREES', 'SORTIES')
),

-- ═════════════════════════════════════════════════════════════════════════════
-- 13. TOUS LES MOUVEMENTS
-- ═════════════════════════════════════════════════════════════════════════════
mouvements AS (
    SELECT annee, 'DEBUT' AS CODE_TYPE_MVT, PERS_ID AS id_mvt, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE,
           CAST(NULL AS VARCHAR2(30)) AS MOTIF_MVT
    FROM au_debut
    UNION ALL
    SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
    FROM entrees
    UNION ALL
    SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
    FROM sorties
    UNION ALL
    SELECT annee, 'RECRU', PERS_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_entree
    FROM passages
    UNION ALL
    SELECT annee, type_sortie, PERS_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_sortie
    FROM passages
    UNION ALL
    SELECT annee, CODE_TYPE_MVT, MVT_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, 'GRH_MOUVEMENT'
    FROM internes
)

-- ═════════════════════════════════════════════════════════════════════════════
-- 14. COMPTAGES PAR AXE
-- ═════════════════════════════════════════════════════════════════════════════
SELECT
    TO_NUMBER(TO_CHAR(m.annee) || '1231')                 AS ID_TEMPS,
    m.CODE_TYPE_MVT,
    tm.CODE_NAT_MVT,
    CASE tm.CODE_NAT_MVT
        WHEN 'ENTREES' THEN 'PRISE'
        WHEN 'SORTIES' THEN 'CESSA'
        ELSE NULL
    END                                                   AS CODE_SENS_MVT,
    m.QUAL_CODE,
    m.SEXE,
    CASE m.SEXE
        WHEN 'M' THEN 'Masculin'
        WHEN 'F' THEN 'Féminin'
        ELSE 'NON RENSEIGNE'
    END                                                   AS LIBELLE_SEXE,
    COUNT(m.id_mvt)                                       AS NB_MOUVEMENTS,
    COUNT(DISTINCT m.PERS_ID)                             AS NB_AGENTS,
    SYSDATE                                               AS DATE_CHARGEMENT,
    :1                                                    AS CLICHE,
    m.QUALITE_EMPLOYE,
    m.NATURE_EMPLOI,
    m.UA_CODE,
    m.MOTIF_MVT
FROM mouvements m
LEFT JOIN DTM.DIM_GRH_TYPE_MOUVEMENT tm
       ON tm.CODE_TYPE_MVT = m.CODE_TYPE_MVT
GROUP BY
    TO_NUMBER(TO_CHAR(m.annee) || '1231'),
    m.CODE_TYPE_MVT,
    tm.CODE_NAT_MVT,
    CASE tm.CODE_NAT_MVT WHEN 'ENTREES' THEN 'PRISE' WHEN 'SORTIES' THEN 'CESSA' ELSE NULL END,
    m.QUAL_CODE,
    m.SEXE,
    CASE m.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
    m.QUALITE_EMPLOYE,
    m.NATURE_EMPLOI,
    m.UA_CODE,
    m.MOTIF_MVT
