-- =============================================================================
-- DTM_GRH_MOUVEMENT — PROPOSITION du 08/11/2026 (T60, mouvement du personnel)
-- MÊME STRUCTURE que domaines/grh/dtm/sql/grh_mouvement.sql : mêmes colonnes,
-- même grain, même paramètre :1. Seules les règles changent. Quatre colonnes
-- s'ajoutent à la fin, aucune n'est retirée. Après le visa de la DRH, ce
-- fichier remplace grh_mouvement.sql (les colonnes ajoutées doivent d'abord
-- être créées dans DTM et ODS.DTM_GRH_MOUVEMENT).
-- Remplace la proposition du 01/11/2026, qui rendait une ligne par exercice
-- à la façon de la maquette : la mise en page des tableaux CIPRES se fait
-- dans OBIEE, pas dans la table.
--
-- Sources : DWH.FAIT_GRH_PERSONNE, FAIT_GRH_SITUATION, FAIT_GRH_DECISION,
--           FAIT_GRH_DECISION_PERSONNE, FAIT_GRH_MOUVEMENT ;
--           DTM.DIM_GRH_TYPE_MOUVEMENT (les codes de mouvement de la CNSS,
--           table GRH_TYPE_MOUVEMENT de l'application GRH).
-- :1 = CLICHE (MMYYYY), saisi sans guillemets.
--
-- POURQUOI : la requête actuelle lit GRH_MOUVEMENT, qui ne contient presque
-- que des mouvements internes (affectations, mutations, nominations) : ni
-- recrutement, ni retraite, ni décès (lot 1 ; réunion du 03/11/2026, M06).
-- Les entrées se lisent dans la fiche de l'agent (date d'embauche) et les
-- engagements ; les sorties, dans les décisions de départ.
--
-- CE QUE DEMANDE LE RECUEIL DES BESOINS v7 :
--   RH ligne 3 (T60) : « Autre : il faut le détailler (lister pour la
--   CNSS) » ; source : application GRH et paie ; déclinaison : direction,
--   agence, bureau ; périodicité : annuelle.
--   RH ligne 1 : par type d'employé, permanent ou temporaire (filtre global
--   de nature, réunion du 03/11/2026).
--
-- COLONNES : celles de grh_mouvement.sql, dans le même ordre :
--   ID_TEMPS, CODE_TYPE_MVT, CODE_NAT_MVT, CODE_SENS_MVT, QUAL_CODE, SEXE,
--   LIBELLE_SEXE, NB_MOUVEMENTS, NB_AGENTS, DATE_CHARGEMENT, CLICHE
-- puis les colonnes AJOUTÉES :
--   QUALITE_EMPLOYE  A agent CNSS, B fonction publique, C agent affecté
--   NATURE_EMPLOI    PERMANENT ou TEMPORAIRE : filtre global dans OBIEE
--   UA_CODE          unité administrative : déclinaison par direction,
--                    agence et bureau
--   MOTIF_MVT        ce qui explique le mouvement (G15) : DECISION_02,
--                    DATE_DECES, EMBAUCHE, ENGAGEMENT_50, ETAIT_TEMPORAIRE,
--                    DEVIENT_PERMANENT, SANS_MOTIF… ; GRH_MOUVEMENT pour les
--                    mouvements internes ; vide pour DEBUT
-- Grain : ID_TEMPS × CODE_TYPE_MVT × CODE_NAT_MVT × CODE_SENS_MVT × QUAL_CODE
--   × SEXE (comme avant), plus les quatre colonnes ajoutées.
--   ID_TEMPS = AAAA1231, l'exercice du mouvement. Un agent fait au plus une
--   entrée et une sortie par exercice et par nature.
--
-- LES CODES (GRH_TYPE_MOUVEMENT de la CNSS : ceux du domaine ALL sont les
-- colonnes de l'Annuaire CIPRES, ceux du domaine CNSS en sont le détail,
-- comme dans le tableau 121 de l'annuaire CNSS 2024) :
--   INITIAL  DEBUT       effectif au début de l'exercice
--   ENTREES  RECRU       recrutement
--            REM         remise en activité          (détail des autres)
--            DETAC       détaché                     (détail des autres)
--            AUTRE_ENT   autres entrées
--   SORTIES  RET         retraite
--            DEC         décès
--            LIC         licenciement                (détail des autres)
--            DEM         démission                   (détail des autres)
--            DISP        disponibilité               (détail des autres)
--            MISE_DIS    mise à disposition          (détail des autres)
--            FIN_DETACH  fin de détachement          (détail des autres)
--            FIN_CTRAT   fin de contrat              (détail des autres)
--            AUTRE_SORT  autres sorties
--   INTERNE  AFF, MUT, NOM… repris de GRH_MOUVEMENT comme avant (hors T60)
--
-- T60 DANS OBIEE (filtres QUALITE_EMPLOYE = 'A' et NATURE_EMPLOI, global) :
--   Effectif début = SUM(NB_AGENTS) des lignes DEBUT
--   Recrutement    = RECRU
--   Autres entrées = ENTREES sauf RECRU (détail : CODE_TYPE_MVT, MOTIF_MVT)
--   Retraite       = RET ; Décès = DEC
--   Autres sorties = SORTIES sauf RET et DEC (détail : idem)
--   Effectif fin   = début + entrées − sorties : c'est l'effectif de T58 au
--                    31/12 de l'exercice (mêmes règles de présence).
--   Par direction, agence ou bureau (UA_CODE) : un agent qui change d'unité
--   ne fait ni entrée ni sortie. L'effectif de fin se lit alors dans
--   DTM_GRH_EFFECTIF, et l'écart avec début + entrées − sorties donne les
--   mutations nettes de l'unité.
--
-- CONTRÔLE dans SQL Developer (cliché 102026), en entourant la requête ; à
-- refaire avec disponibilite = 'HORS_EFFECTIF' dans la CTE parametres pour
-- voir la lecture de l'annuaire :
--   SELECT ID_TEMPS,
--     SUM(CASE WHEN CODE_TYPE_MVT = 'DEBUT' THEN NB_AGENTS ELSE 0 END) AS debut,
--     SUM(CASE WHEN CODE_TYPE_MVT = 'RECRU' THEN NB_AGENTS ELSE 0 END) AS recrut,
--     SUM(CASE WHEN CODE_NAT_MVT = 'ENTREES' AND CODE_TYPE_MVT <> 'RECRU'
--              THEN NB_AGENTS ELSE 0 END)                             AS autres_ent,
--     SUM(CASE WHEN CODE_TYPE_MVT = 'RET' THEN NB_AGENTS ELSE 0 END)   AS retraite,
--     SUM(CASE WHEN CODE_TYPE_MVT = 'DEC' THEN NB_AGENTS ELSE 0 END)   AS deces,
--     SUM(CASE WHEN CODE_NAT_MVT = 'SORTIES'
--               AND CODE_TYPE_MVT NOT IN ('RET', 'DEC')
--              THEN NB_AGENTS ELSE 0 END)                             AS autres_sort,
--     SUM(CASE WHEN CODE_NAT_MVT IN ('INITIAL', 'ENTREES') THEN NB_AGENTS
--              WHEN CODE_NAT_MVT = 'SORTIES' THEN -NB_AGENTS
--              ELSE 0 END)                                            AS fin
--   FROM ( …la requête… )
--   WHERE QUALITE_EMPLOYE = 'A' AND NATURE_EMPLOI = 'PERMANENT'
--     AND ID_TEMPS BETWEEN 20201231 AND 20251231
--   GROUP BY ID_TEMPS ORDER BY 1;
-- Attendu, à quelques agents près (permanents, réglages par défaut ; lots
-- 17, 18 et 25) : début · recrut · autres_ent · retraite · deces ·
-- autres_sort · fin
--   2020 : 823 ·   1 · 0 · 39 · 3 · 0 · 782
--   2021 : 782 ·   0 · 0 · 31 · 4 · 1 · 746   (1 licenciement)
--   2022 : 746 ·   1 · 0 · 29 · 4 · 2 · 712   (2 licenciements)
--   2023 : 712 ·   0 · 0 · 30 · 3 · 2 · 677   (1 démission, 1 fin de contrat)
--   2024 : 677 · 104 · 0 · 25 · 5 · 4 · 747   (2 démissions, 1 licenciement,
--                                              1 sorti sans motif daté)
--   2025 : 747 ·   … · … ·  … · … · … · 886
--   Annuaire CNSS 2024 (tableau 121) : 767 · 104 · 7 · 23 · 3 · 94 · 758.
--   Les écarts : les 84 engagés de 2019, sans date de départ (G11) ; les
--   disponibilités et les remises en activité (réglage disponibilite) ; les
--   décès, lus 8 mois plus tard dans la liquidation des droits (34).
--   Avec disponibilite = 'HORS_EFFECTIF' : disponibilités (DISP) et remises en activité
--   (REM) proches des décisions 07 et 18 des permanents, 2, 4, 2, 5, 5 et 4,
--   3, 6, 5, 4 de 2020 à 2024 (lot 20, Q117), pour 5, 3, 2, 4, 7 et 3, 2, 3,
--   5, 6 publiées ; début et fin baissent des agents en disponibilité (6 au
--   31/12/2024).
-- Le détail des autres (B29), même filtre :
--   SELECT ID_TEMPS, CODE_NAT_MVT, CODE_TYPE_MVT, MOTIF_MVT, SUM(NB_AGENTS)
--   FROM ( …la requête… )
--   WHERE QUALITE_EMPLOYE = 'A' AND NATURE_EMPLOI = 'PERMANENT'
--     AND ID_TEMPS BETWEEN 20201231 AND 20251231
--     AND CODE_NAT_MVT IN ('ENTREES', 'SORTIES')
--   GROUP BY ID_TEMPS, CODE_NAT_MVT, CODE_TYPE_MVT, MOTIF_MVT
--   ORDER BY 1, 2, 3, 4;
--
-- RÈGLES (détail et exemples dans analyses/grh/regles_t60.md).
--
--   · G14 Les deux photos de l'exercice
--     Effectif au début de l'exercice N = les agents présents au 31/12 de
--     N − 1 ; à la fin = les présents au 31/12 de N. « Présent » suit
--     exactement les règles de T58 : G1 (nature), G2 (permanents), G7
--     (temporaires), réglages disponibilite et fin_contrat_inconnue. Les
--     parties 0 à 7 reprennent la requête de l'effectif : toute modification
--     de ces règles se fait dans les deux requêtes.
--     Un agent appartient, à chaque date, à la population de sa nature.
--
--   · G8 Entrée : présent à la fin dans une nature, absent de cette nature
--     au début. Le code, dans cet ordre :
--       permanents (et agents sans situation) :
--         DETAC      décision 37 (classement d'un agent détaché) dans
--                    l'exercice ;
--         RECRU      entrée dans l'exercice (date d'embauche, à défaut prise
--                    de service) ;
--         REM        en disponibilité au début (réglage disponibilite =
--                    HORS_EFFECTIF seulement) ;
--       temporaires :
--         RECRU      pas sous contrat au début : nouvel engagement (50) ;
--       puis pour tous :
--         AUTRE_ENT  présent au début dans une autre nature (ETAIT_TEMPORAIRE,
--                    ETAIT_PERMANENT, ETAIT_SANS_SITUATION) ; sinon
--                    SANS_MOTIF.
--
--   · G9 Sortie : présent au début dans une nature, absent de cette nature à
--     la fin. Le code, dans cet ordre :
--       le départ qui l'a fait sortir, daté dans l'exercice : pour un
--       permanent, son premier départ (comme G2) ; pour un temporaire, le
--       dernier départ de l'exercice. Le code de la décision donne le type
--       (table decisions_depart) : 02 RET, 34 DEC, 35 FIN_CTRAT, 36 DEM,
--       52 FIN_CTRAT, 54 LIC ; une date de décès donne DEC ;
--       DISP        en disponibilité à la fin (réglage HORS_EFFECTIF
--                   seulement) ;
--       AUTRE_SORT  présent à la fin dans une autre nature
--                   (DEVIENT_PERMANENT, DEVIENT_TEMPORAIRE) ; sinon
--                   SANS_MOTIF.
--
--   · G15 Le détail des « autres » (recueil v7, RH ligne 3)
--     Les codes de la CNSS (GRH_TYPE_MOUVEMENT), et la colonne MOTIF_MVT
--     pour ce que le code ne dit pas.
--
--   · G16 Entré et sorti dans le même exercice (réglage passages_annee)
--     NON : pas compté ; l'agent n'est présent à aucune des deux photos.
--     OUI : compté une fois en entrée (RECRU) et une fois en sortie (le code
--     de son départ). Début + entrées − sorties = fin reste vrai.
--
--   · G17 Les mouvements internes
--     Repris de GRH_MOUVEMENT comme avant (validés O ; AFF, MUT, NOM…). Les
--     entrées et les sorties de cette table (DIS, DET, REM) ne sont plus
--     lues : elles viennent de G8 et G9.
--
-- RÉGLAGES (partie 0, la CTE parametres) : chacun peut changer sans réécrire
-- la requête.
--   · disponibilite         : PRESENT (dans l'effectif) ou HORS_EFFECTIF ;
--                             MÊME VALEUR que la requête de l'effectif, sinon
--                             la fin ne retombe plus sur T58 (QG6).
--   · fin_contrat_inconnue  : PRESENT ou FICHE ; même valeur que l'effectif.
--   · passages_annee        : NON (proposition de la fiche T60, Q7) ou OUI.
--   · mouvements_internes   : COMME_AVANT ou AUCUN.
--   · decisions_depart      : code de décision → type de sortie ; même liste
--                             de codes que l'effectif.
--   · codes_decision        : 40 annulation ; 50 et 51 engagements ; 07 et 18
--                             disponibilité et remise ; 37 détaché.
-- =============================================================================

WITH

-- ── 0. Les réglages ─────────────────────────────────────────────────────────
parametres AS (
    SELECT -- QG6 : l'agent en disponibilité (décision 07, sans retour 18) au 31/12
           --   'PRESENT'       reste dans l'effectif : ni sortie, ni retour ;
           --   'HORS_EFFECTIF' sort de l'effectif : une sortie DISP, puis une
           --                   entrée REM au retour, comme l'annuaire CNSS 2024.
           -- Même valeur que dans la requête de l'effectif.
           'PRESENT'               AS disponibilite,
           -- G7 : sans fin de contrat, 'PRESENT' ou 'FICHE' (comme l'effectif)
           'PRESENT'               AS fin_contrat_inconnue,
           -- G16 : entré et sorti dans l'exercice, 'NON' (pas compté) ou 'OUI'
           'NON'                   AS passages_annee,
           -- G17 : mouvements internes de GRH_MOUVEMENT, 'COMME_AVANT' ou 'AUCUN'
           'COMME_AVANT'           AS mouvements_internes
    FROM DUAL
),
-- G2, G7, G9 : les décisions de départ (GRH_DECISION.CDEC_CODE) et le type de
-- sortie que chacune donne (code de GRH_TYPE_MOUVEMENT) ; une ligne par code
decisions_depart AS (
    SELECT '02' AS code, 'RET' AS type_mvt, 'admission à la retraite' AS libelle FROM DUAL UNION ALL
    SELECT '34', 'DEC',       'liquidation des droits après décès'          FROM DUAL UNION ALL
    SELECT '35', 'FIN_CTRAT', 'fin de contrat'                              FROM DUAL UNION ALL
    SELECT '36', 'DEM',       'démission'                                   FROM DUAL UNION ALL
    SELECT '52', 'FIN_CTRAT', 'rupture du contrat'                          FROM DUAL UNION ALL
    SELECT '54', 'LIC',       'licenciement'                                FROM DUAL
),
-- les autres codes de décision utilisés
codes_decision AS (
    SELECT '40' AS annulation,
           '50' AS engagement_temporaire,
           '51' AS engagement_permanent,
           '07' AS mise_en_disponibilite,
           '18' AS remise_en_activite,
           '37' AS classement_detache
    FROM DUAL
),

-- ── 1. Les dates d'arrêté : le 31/12 de chaque année, sans trou ────────────
-- De la première année où commence une situation jusqu'à l'année dernière.
-- Chaque exercice a besoin du 31/12 de l'année d'avant.
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

-- ── 2. Les agents (comme l'effectif) ────────────────────────────────────────
personnes AS (
    SELECT p.PERS_ID, p.SEXE, NVL(p.ETAT_EMPLOYE, '-') AS etat,
           -- Qualité : A agent CNSS, B fonction publique, C agent affecté
           TO_CHAR(p.QUALITE)                                     AS qualite,
           -- G2 : la date d'entrée
           COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree
    FROM DWH.FAIT_GRH_PERSONNE p
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
                                       cd.mise_en_disponibilite, cd.remise_en_activite,
                                       cd.classement_detache))
      AND NOT EXISTS (SELECT 1
                      FROM DWH.FAIT_GRH_DECISION a
                      WHERE a.CLICHE = :1
                        AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
-- les départs (décisions de départ, et la date de décès), avec le type de
-- sortie que chacun donne (G9) et son motif
departs AS (
    SELECT dc.PERS_ID, dc.date_decision AS date_depart,
           dd.type_mvt, 'DECISION_' || dc.code AS motif
    FROM decisions dc
    JOIN decisions_depart dd ON dd.code = dc.code
    WHERE dc.PERS_ID IS NOT NULL
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

-- ── 4. Les contrats des temporaires (G7, comme l'effectif) ─────────────────
-- Chaque engagement temporaire (décision 50) ouvre un contrat.
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
temporaires_sous_contrat AS (
    SELECT da.date_arrete, c.PERS_ID,
           MAX(CASE WHEN c.fin = DATE '9999-12-31' THEN 1 ELSE 0 END) AS fin_inconnue
    FROM dates da
    JOIN contrats c ON c.debut       <= da.date_arrete
                   AND da.date_arrete <  c.fin
    GROUP BY da.date_arrete, c.PERS_ID
),

-- ── 5. Les disponibilités (QG6, comme l'effectif) ──────────────────────────
-- Dernière mise en disponibilité (07) et dernière remise en activité (18) au
-- plus tard à la date d'arrêté : date_dispo > date_remise = en disponibilité.
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

-- ── 6. La dernière situation de chaque agent à chaque date ─────────────────
-- Dernière situation commencée au plus tard à la date d'arrêté ; à date
-- égale, la dernière saisie. Elle donne la nature, la qualification et l'unité.
situations AS (
    SELECT da.date_arrete, s.PERS_ID, s.QUAL_CODE, s.UA_CODE,
           TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
           ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM dates da
    JOIN DWH.FAIT_GRH_SITUATION s ON s.CLICHE     = :1
                                 AND s.DATE_DEBUT <= da.date_arrete
),

-- ── 7. Les agents présents à chaque 31/12 (G2, G7), exactement comme T58 ───
-- Une ligne par agent et par date. Même WHERE que la requête de l'effectif.
base AS (
    SELECT da.date_arrete, p.PERS_ID, p.SEXE,
           p.qualite                                              AS QUALITE_EMPLOYE,
           si.nature                                              AS NATURE_EMPLOI,
           si.QUAL_CODE, si.UA_CODE
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
),

-- ── 8. Les deux photos de chaque exercice (G14) ─────────────────────────────
au_debut AS (
    SELECT e.annee, e.d0, e.d1, b.*
    FROM exercices e
    JOIN base b ON b.date_arrete = e.d0
),
a_la_fin AS (
    SELECT e.annee, e.d0, e.d1, b.*
    FROM exercices e
    JOIN base b ON b.date_arrete = e.d1
),
-- Le dernier départ daté dans chaque exercice (G9, temporaires) ; à date
-- égale, le dernier motif.
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

-- ── 9. Les entrées (G8) : présent à la fin, absent de sa nature au début ───
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
                     -- permanents (et agents sans situation)
                     WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND dt.PERS_ID IS NOT NULL                       THEN 'DETACHEMENT'
                     WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND p.date_entree >  f.d0
                      AND p.date_entree <= f.d1                        THEN 'EMBAUCHE'
                     WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND pr.disponibilite = 'HORS_EFFECTIF'
                      AND NVL(po.date_dispo, DATE '1900-01-01')
                          > NVL(po.date_remise, DATE '1900-01-01')     THEN 'REMISE'
                     -- temporaires : pas sous contrat au début
                     WHEN f.NATURE_EMPLOI = 'TEMPORAIRE'
                      AND tc0.PERS_ID IS NULL                          THEN 'ENGAGEMENT'
                     -- présent au début, dans une autre nature
                     WHEN dbt.PERS_ID IS NOT NULL                      THEN 'NATURE'
                     ELSE                                                   'SANS_MOTIF'
                 END                        AS cas
          FROM a_la_fin f
          JOIN personnes p ON p.PERS_ID = f.PERS_ID
          CROSS JOIN parametres pr
          CROSS JOIN codes_decision cd
          LEFT JOIN au_debut dbt                 ON dbt.annee        = f.annee
                                                AND dbt.PERS_ID      = f.PERS_ID
          LEFT JOIN detaches_annee dt            ON dt.annee         = f.annee
                                                AND dt.PERS_ID       = f.PERS_ID
          -- position et contrat au début de l'exercice
          LEFT JOIN positions po                 ON po.date_arrete   = f.d0
                                                AND po.PERS_ID       = f.PERS_ID
          LEFT JOIN temporaires_sous_contrat tc0 ON tc0.date_arrete  = f.d0
                                                AND tc0.PERS_ID      = f.PERS_ID
          WHERE dbt.PERS_ID IS NULL
             OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')) x
),

-- ── 10. Les sorties (G9) : présent au début, absent de sa nature à la fin ──
sorties AS (
    SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
           x.QUAL_CODE, x.UA_CODE,
           CASE x.cas
               WHEN 'DEPART_PERMANENT'  THEN x.type_premier
               WHEN 'DEPART_TEMPORAIRE' THEN x.type_dernier
               WHEN 'DISPONIBILITE'     THEN 'DISP'
               ELSE                          'AUTRE_SORT'
           END                                                    AS CODE_TYPE_MVT,
           CASE x.cas
               WHEN 'DEPART_PERMANENT'  THEN x.motif_premier
               WHEN 'DEPART_TEMPORAIRE' THEN x.motif_dernier
               WHEN 'DISPONIBILITE'     THEN 'DECISION_' || x.code_dispo
               WHEN 'NATURE'            THEN 'DEVIENT_' || NVL(x.nature_fin, 'SANS_SITUATION')
               ELSE                          'SANS_MOTIF'
           END                                                    AS MOTIF_MVT
    FROM (SELECT dbt.*,
                 fin.NATURE_EMPLOI          AS nature_fin,
                 pd.type_mvt                AS type_premier,
                 pd.motif                   AS motif_premier,
                 dda.type_mvt               AS type_dernier,
                 dda.motif                  AS motif_dernier,
                 cd.mise_en_disponibilite   AS code_dispo,
                 CASE
                     -- permanent (ou sans situation) : son premier départ, dans l'exercice
                     WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND pd.date_depart >  dbt.d0
                      AND pd.date_depart <= dbt.d1                     THEN 'DEPART_PERMANENT'
                     -- temporaire : le dernier départ de l'exercice
                     WHEN dbt.NATURE_EMPLOI = 'TEMPORAIRE'
                      AND dda.PERS_ID IS NOT NULL                      THEN 'DEPART_TEMPORAIRE'
                     -- en disponibilité à la fin (réglage HORS_EFFECTIF seulement)
                     WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                      AND pr.disponibilite = 'HORS_EFFECTIF'
                      AND NVL(po.date_dispo, DATE '1900-01-01')
                          > NVL(po.date_remise, DATE '1900-01-01')     THEN 'DISPONIBILITE'
                     -- présent à la fin, dans une autre nature
                     WHEN fin.PERS_ID IS NOT NULL                      THEN 'NATURE'
                     ELSE                                                   'SANS_MOTIF'
                 END                        AS cas
          FROM au_debut dbt
          CROSS JOIN parametres pr
          CROSS JOIN codes_decision cd
          LEFT JOIN a_la_fin fin             ON fin.annee      = dbt.annee
                                            AND fin.PERS_ID    = dbt.PERS_ID
          LEFT JOIN premier_depart pd        ON pd.PERS_ID     = dbt.PERS_ID
          LEFT JOIN dernier_depart_annee dda ON dda.annee      = dbt.annee
                                            AND dda.PERS_ID    = dbt.PERS_ID
          -- position à la fin de l'exercice
          LEFT JOIN positions po             ON po.date_arrete = dbt.d1
                                            AND po.PERS_ID     = dbt.PERS_ID
          WHERE fin.PERS_ID IS NULL
             OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-')) x
),

-- ── 11. Entré et sorti dans le même exercice (G16, réglage passages_annee) ─
-- Absent des deux photos, mais présent entre les deux.
contrats_annee AS (
    -- un contrat temporaire commencé et fini dans l'exercice
    SELECT DISTINCT e.annee, c.PERS_ID
    FROM exercices e
    JOIN contrats c ON c.debut >  e.d0
                   AND c.debut <= e.d1
                   AND c.fin   <= e.d1
),
passages AS (
    -- permanents : entrée et premier départ dans l'exercice, fiche INACTIF
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
    -- temporaires : un contrat commencé et fini dans l'exercice
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

-- ── 12. Les mouvements internes, comme avant (G17) ─────────────────────────
-- Lus dans GRH_MOUVEMENT (validés O) ; les codes d'entrée et de sortie de
-- cette table (DIS, DET, REM) ne sont plus lus : ils viennent de G8 et G9.
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

-- ── 13. Tous les mouvements : une ligne par agent, exercice et code ────────
mouvements AS (
    -- G14 : l'effectif au début de l'exercice
    SELECT annee, 'DEBUT' AS CODE_TYPE_MVT, PERS_ID AS id_mvt, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE,
           CAST(NULL AS VARCHAR2(30)) AS MOTIF_MVT
    FROM au_debut
    UNION ALL
    -- G8 : les entrées
    SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
    FROM entrees
    UNION ALL
    -- G9 : les sorties
    SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
    FROM sorties
    UNION ALL
    -- G16 : entré et sorti dans l'exercice, l'entrée…
    SELECT annee, 'RECRU', PERS_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_entree
    FROM passages
    UNION ALL
    -- … et la sortie
    SELECT annee, type_sortie, PERS_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_sortie
    FROM passages
    UNION ALL
    -- G17 : les mouvements internes, comme avant
    SELECT annee, CODE_TYPE_MVT, MVT_ID, PERS_ID, SEXE,
           QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, 'GRH_MOUVEMENT'
    FROM internes
)

-- ── 14. Comptages par axe, comme avant ──────────────────────────────────────
-- La nature et le sens viennent de DIM_GRH_TYPE_MOUVEMENT, comme avant.
SELECT
    -- ── AXES (comme avant) ───────────────────────────────────────────
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
    -- ── MESURES (comme avant) ────────────────────────────────────────
    COUNT(m.id_mvt)                                       AS NB_MOUVEMENTS,
    COUNT(DISTINCT m.PERS_ID)                             AS NB_AGENTS,
    -- ── CHARGEMENT (comme avant) ─────────────────────────────────────
    SYSDATE                                               AS DATE_CHARGEMENT,
    :1                                                    AS CLICHE,
    -- ── COLONNES AJOUTÉES ────────────────────────────────────────────
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
