-- =============================================================================
-- T60 — Le mouvement du personnel permanent, sur les cinq derniers exercices
-- PROPOSITION du 01/11/2026, à valider par la DRH : traduit les règles G1, G2,
-- G8 et G9 du README de la GRH, contrôlées par le tableau 121 de l'annuaire de
-- la CNSS 2024 (lots 17 à 20). Remplace grh_mouvement.sql, qui lit
-- GRH_MOUVEMENT, où il n'y a ni recrutement ni départ (lot 1).
-- Sources : DWH.FAIT_GRH_PERSONNE, FAIT_GRH_SITUATION, FAIT_GRH_DECISION,
--           FAIT_GRH_DECISION_PERSONNE (copies des tables de USER_GRH).
-- Grain   : une ligne par exercice, de :2 − 4 à :2. Colonnes de la maquette de
--           l'Annuaire (p. 51) : effectif au début, entrées (recrutement,
--           autres), sorties (retraite, décès, autres), effectif à la fin ;
--           puis le détail des « autres », comme le tableau 121 de l'annuaire
--           de la CNSS ; un contrôle ; et, pour information, les décisions 07
--           et 18 de l'année.
-- :1 = CLICHE (MMYYYY), saisi sans guillemets ; :2 = exercice (AAAA), par
-- exemple 2024.
-- Réglage (parametres), en attendant la réponse de la DRH (QG6) :
--   · disponibilite : 'PRESENT' (l'agent mis en disponibilité reste dans
--     l'effectif, comme aujourd'hui : ni sortie ni entrée) ou 'HORS_EFFECTIF'
--     (comme le tableau 121 : la mise en disponibilité est une sortie, la
--     remise en activité une entrée). Même réglage que la requête de
--     l'effectif (t58_t65_effectif_requete_proposee.sql).
-- Règles (README de la GRH) :
--   · G1, G2 L'effectif au 31/12 : les permanents présents, comme dans la
--     requête de l'effectif. L'effectif au début d'un exercice est celui du
--     31/12 de l'exercice d'avant.
--   · Les entrées et les sorties se lisent par différence : une entrée est un
--     permanent présent à la fin et absent au début, une sortie l'inverse.
--     Début + entrées − sorties = fin, toujours (colonne CONTROLE à zéro). Un
--     agent entré et sorti dans la même année n'apparaît pas.
--   · G8 Entrée : recrutement si l'agent est entré (embauche, à défaut prise
--     de service) dans l'exercice ; remise en activité s'il était en
--     disponibilité au début (réglage HORS_EFFECTIF) ; sinon autre entrée
--     (temporaire devenu permanent, retour sans décision…).
--   · G9 Sortie, d'après la première sortie de l'agent, datée dans
--     l'exercice : retraite = décision 02 ; décès = date de décès, à défaut
--     liquidation des droits (34), qui vient environ 8 mois après le décès
--     (proposition BI, QG3) ; démission = 36 ; licenciement = 54 ; fin de
--     contrat = 35, 52. Sans sortie datée dans l'exercice : disponibilité si
--     l'agent est en disponibilité à la fin (réglage HORS_EFFECTIF) ; sinon
--     autre sortie (permanent devenu temporaire…).
--   · Disponibilité : dernière décision 07 non suivie d'une décision 18.
--   · Les détachements (2 et 1 publiés en 2023 et 2024) ne sont pas
--     distingués : aucune décision 37 chez les permanents (QG7).
-- Contrôle attendu (cliché 102026, exercice 2024, réglage PRESENT ; lot 17,
-- Q99 ; lot 18, Q109) :
--   2020 : 818 au début, 1 recrutement, 39 retraites, 3 décès, 777 à la fin ;
--   2021 : 777, 0, 31, 4, 1 licenciement, 741 ;
--   2022 : 741, 1, 29, 4, 2 licenciements, 707 ;
--   2023 : 707, 0, 30, 3, 1 démission, 1 fin de contrat, 672 ;
--   2024 : 672, 104 recrutements, 25 retraites, 5 décès, 2 démissions,
--          1 licenciement, 742 à la fin.
--   Publié (tableau 121) : 2024, 767 au début, 104 recrutements, 23 retraites,
--   3 décès, 758 à la fin. Les débuts de 2020 à 2024 sont sous l'annuaire
--   d'environ 84 + 8 à 16 agents : les 84 engagés de 2019 sans date de départ
--   (G11, QG2).
-- =============================================================================

WITH

parametres AS (
    SELECT TO_NUMBER(:2)    AS annee,
           -- agent en disponibilité : 'PRESENT' ou 'HORS_EFFECTIF'
           'PRESENT'        AS disponibilite
    FROM DUAL
),

-- ── 1. Les exercices : les cinq demandés et celui d'avant (effectif de début)
exercices AS (
    SELECT pr.annee - 6 + LEVEL                                   AS annee,
           TO_DATE((pr.annee - 6 + LEVEL) || '1231', 'YYYYMMDD')  AS fin
    FROM parametres pr
    CONNECT BY LEVEL <= 6
),

-- ── 2. La première sortie de chaque agent, avec son motif (G2, G9) ─────────
decisions_sortie AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt,
           d.CDEC_CODE                AS motif
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
    SELECT PERS_ID, DATE_DECES, 'DECES'
    FROM DWH.FAIT_GRH_PERSONNE
    WHERE CLICHE = :1
      AND DATE_DECES IS NOT NULL
),
sorties AS (
    SELECT PERS_ID, dt AS dt_sortie, motif
    FROM (SELECT PERS_ID, dt, motif,
                 ROW_NUMBER() OVER (PARTITION BY PERS_ID ORDER BY dt, motif) AS rn
          FROM decisions_sortie
          WHERE PERS_ID IS NOT NULL)
    WHERE rn = 1
),

-- ── 3. Les mises en disponibilité (07) et remises en activité (18) ─────────
decisions_position AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.CDEC_CODE, d.DATE_VIGUEUR AS dt
    FROM DWH.FAIT_GRH_DECISION d
    LEFT JOIN DWH.FAIT_GRH_DECISION_PERSONNE dp
           ON dp.DECI_NO = d.DECI_NO
          AND dp.CLICHE  = :1
    WHERE d.CLICHE = :1
      AND d.CDEC_CODE IN ('07', '18')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM DWH.FAIT_GRH_DECISION a
                      WHERE a.CLICHE    = :1
                        AND a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
positions AS (
    SELECT e.annee, dpo.PERS_ID,
           MAX(CASE WHEN dpo.CDEC_CODE = '07' THEN dpo.dt END)    AS dt_disponibilite,
           MAX(CASE WHEN dpo.CDEC_CODE = '18' THEN dpo.dt END)    AS dt_remise,
           -- décisions datées dans l'exercice, pour information
           MAX(CASE WHEN dpo.CDEC_CODE = '07'
                     AND dpo.dt > ADD_MONTHS(e.fin, -12) THEN 1 ELSE 0 END) AS decision_07,
           MAX(CASE WHEN dpo.CDEC_CODE = '18'
                     AND dpo.dt > ADD_MONTHS(e.fin, -12) THEN 1 ELSE 0 END) AS decision_18
    FROM exercices e
    JOIN decisions_position dpo ON dpo.dt <= e.fin
    WHERE dpo.PERS_ID IS NOT NULL
    GROUP BY e.annee, dpo.PERS_ID
),

-- ── 4. Chaque agent CNSS de nature PERMANENT au 31/12 de chaque exercice ────
situations AS (
    SELECT e.annee, e.fin, s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY e.annee, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM exercices e
    JOIN DWH.FAIT_GRH_SITUATION s ON s.CLICHE     = :1
                                 AND s.DATE_DEBUT <= e.fin
),
etats AS (
    SELECT si.annee, si.fin, p.PERS_ID,
           COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)        AS date_entree,
           so.dt_sortie, so.motif,
           CASE WHEN NVL(po.dt_disponibilite, DATE '1900-01-01')
                     > NVL(po.dt_remise, DATE '1900-01-01')
                THEN 1 ELSE 0 END                                 AS en_disponibilite,
           -- présent au 31/12 (G2), selon le réglage de la disponibilité
           CASE WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= si.fin
                 AND (p.ETAT_EMPLOYE = 'ACTIF' OR so.dt_sortie > si.fin)
                 AND NOT (pr.disponibilite = 'HORS_EFFECTIF'
                          AND NVL(po.dt_disponibilite, DATE '1900-01-01')
                              > NVL(po.dt_remise, DATE '1900-01-01'))
                THEN 1 ELSE 0 END                                 AS present,
           NVL(po.decision_07, 0)                                 AS decision_07,
           NVL(po.decision_18, 0)                                 AS decision_18
    FROM situations si
    JOIN DWH.FAIT_GRH_PERSONNE p ON p.PERS_ID = si.PERS_ID
                                AND p.CLICHE  = :1
    CROSS JOIN parametres pr
    LEFT JOIN sorties so   ON so.PERS_ID = si.PERS_ID
    LEFT JOIN positions po ON po.annee   = si.annee
                          AND po.PERS_ID = si.PERS_ID
    WHERE si.rn = 1
      AND si.NATURE_EMPLOIE = 'PERMANENT'
      AND TO_CHAR(p.QUALITE) = 'A'
),
presents AS (
    SELECT * FROM etats WHERE present = 1
),

-- ── 5. Les entrées et les sorties, par différence d'une fin d'année à l'autre
entrees AS (
    SELECT cur.annee,
           CASE
               WHEN cur.date_entree > ADD_MONTHS(cur.fin, -12)    THEN 'RECRUTEMENT'
               WHEN pr.disponibilite = 'HORS_EFFECTIF'
                AND av.en_disponibilite = 1                        THEN 'REMISE'
               ELSE                                                     'AUTRE'
           END                                                    AS motif
    FROM presents cur
    CROSS JOIN parametres pr
    LEFT JOIN presents av_present ON av_present.PERS_ID = cur.PERS_ID
                                 AND av_present.annee   = cur.annee - 1
    LEFT JOIN etats av            ON av.PERS_ID = cur.PERS_ID
                                 AND av.annee   = cur.annee - 1
    WHERE av_present.PERS_ID IS NULL
),
sorties_annee AS (
    SELECT av.annee + 1                                           AS annee,
           CASE
               WHEN av.dt_sortie > av.fin
                AND av.dt_sortie <= ADD_MONTHS(av.fin, 12)
               THEN CASE
                        WHEN av.motif = '02'              THEN 'RETRAITE'
                        WHEN av.motif IN ('DECES', '34')  THEN 'DECES'
                        WHEN av.motif = '36'              THEN 'DEMISSION'
                        WHEN av.motif = '54'              THEN 'LICENCIEMENT'
                        ELSE                                   'FIN_CONTRAT'
                    END
               WHEN pr.disponibilite = 'HORS_EFFECTIF'
                AND cur.en_disponibilite = 1                       THEN 'DISPONIBILITE'
               ELSE                                                     'AUTRE'
           END                                                    AS motif
    FROM presents av
    CROSS JOIN parametres pr
    LEFT JOIN presents cur_present ON cur_present.PERS_ID = av.PERS_ID
                                  AND cur_present.annee   = av.annee + 1
    LEFT JOIN etats cur            ON cur.PERS_ID = av.PERS_ID
                                  AND cur.annee   = av.annee + 1
    WHERE cur_present.PERS_ID IS NULL
      AND av.annee < pr.annee
),

-- ── 6. Une ligne par exercice ───────────────────────────────────────────────
annees AS (
    SELECT e.annee FROM exercices e CROSS JOIN parametres pr WHERE e.annee > pr.annee - 5
),
effectifs AS (
    SELECT annee, COUNT(*) AS nb FROM presents GROUP BY annee
),
nb_entrees AS (
    SELECT annee,
           SUM(CASE WHEN motif = 'RECRUTEMENT' THEN 1 ELSE 0 END) AS recrutements,
           SUM(CASE WHEN motif = 'REMISE'      THEN 1 ELSE 0 END) AS remises,
           SUM(CASE WHEN motif = 'AUTRE'       THEN 1 ELSE 0 END) AS autres,
           COUNT(*)                                               AS total
    FROM entrees
    GROUP BY annee
),
nb_sorties AS (
    SELECT annee,
           SUM(CASE WHEN motif = 'RETRAITE'      THEN 1 ELSE 0 END) AS retraites,
           SUM(CASE WHEN motif = 'DECES'         THEN 1 ELSE 0 END) AS deces,
           SUM(CASE WHEN motif = 'DEMISSION'     THEN 1 ELSE 0 END) AS demissions,
           SUM(CASE WHEN motif = 'LICENCIEMENT'  THEN 1 ELSE 0 END) AS licenciements,
           SUM(CASE WHEN motif = 'FIN_CONTRAT'   THEN 1 ELSE 0 END) AS fins_contrat,
           SUM(CASE WHEN motif = 'DISPONIBILITE' THEN 1 ELSE 0 END) AS disponibilites,
           SUM(CASE WHEN motif = 'AUTRE'         THEN 1 ELSE 0 END) AS autres,
           COUNT(*)                                                 AS total
    FROM sorties_annee
    GROUP BY annee
),
nb_decisions AS (
    SELECT annee, SUM(decision_07) AS decisions_07, SUM(decision_18) AS decisions_18
    FROM etats
    GROUP BY annee
)

SELECT a.annee                                                    AS ANNEE,
       NVL(d.nb, 0)                                               AS EFFECTIF_DEBUT,
       -- maquette de l'Annuaire
       NVL(ne.recrutements, 0)                                    AS ENTREES_RECRUTEMENT,
       NVL(ne.remises, 0) + NVL(ne.autres, 0)                     AS ENTREES_AUTRES,
       NVL(ns.retraites, 0)                                       AS SORTIES_RETRAITE,
       NVL(ns.deces, 0)                                           AS SORTIES_DECES,
       NVL(ns.demissions, 0) + NVL(ns.licenciements, 0) + NVL(ns.fins_contrat, 0)
         + NVL(ns.disponibilites, 0) + NVL(ns.autres, 0)          AS SORTIES_AUTRES,
       NVL(f.nb, 0)                                               AS EFFECTIF_FIN,
       -- détail des « autres », comme le tableau 121 de l'annuaire de la CNSS
       NVL(ne.remises, 0)                                         AS REMISES_EN_ACTIVITE,
       NVL(ne.autres, 0)                                          AS AUTRES_ENTREES,
       NVL(ns.demissions, 0)                                      AS DEMISSIONS,
       NVL(ns.licenciements, 0)                                   AS LICENCIEMENTS,
       NVL(ns.fins_contrat, 0)                                    AS FINS_DE_CONTRAT,
       NVL(ns.disponibilites, 0)                                  AS DISPONIBILITES,
       NVL(ns.autres, 0)                                          AS AUTRES_SORTIES,
       -- début + entrées − sorties − fin : toujours zéro
       NVL(d.nb, 0) + NVL(ne.total, 0) - NVL(ns.total, 0) - NVL(f.nb, 0) AS CONTROLE,
       -- pour information : permanents qui ont une décision 07 ou 18 dans
       -- l'exercice (comptées dans le mouvement en réglage HORS_EFFECTIF)
       NVL(nd.decisions_07, 0)                                    AS DECISIONS_07,
       NVL(nd.decisions_18, 0)                                    AS DECISIONS_18,
       :1                                                         AS CLICHE
FROM annees a
LEFT JOIN effectifs    d  ON d.annee  = a.annee - 1
LEFT JOIN effectifs    f  ON f.annee  = a.annee
LEFT JOIN nb_entrees   ne ON ne.annee = a.annee
LEFT JOIN nb_sorties   ns ON ns.annee = a.annee
LEFT JOIN nb_decisions nd ON nd.annee = a.annee
ORDER BY a.annee
