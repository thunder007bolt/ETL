-- =============================================================================
-- LOT 25 — Les agents partis puis revenus, face à la règle G2
-- Rédigé le 07/11/2026 — EXÉCUTÉ le 07/11/2026 sur USER_GRH.
-- Exécution : en mode script (F5) dans SQL Developer. Comptages seulement :
-- aucune donnée nominative.
-- Pourquoi ce lot : la requête de l'effectif (t58_t65_effectif_requete_
-- proposee.sql) compte un permanent présent au 31/12 (règle G2) s'il est
-- entré au plus tard ce jour-là et s'il n'est pas encore parti, c'est-à-dire :
--   - sa fiche n'est pas INACTIF aujourd'hui,
--   - ou son PREMIER départ tombe après le 31/12.
-- Elle ne regarde ni les départs suivants ni les retours. Or un agent peut
-- partir puis revenir (un départ suivi d'un engagement 50 ou 51) : la
-- requête prévoit déjà ce cas pour l'ancienneté (G5), il existe donc. Deux
-- erreurs sont alors possibles pour une année passée :
--   1. compté pendant son absence : la fiche est active aujourd'hui, et le
--      31/12 tombe entre son départ et son retour ;
--   2. oublié après son retour : la fiche est INACTIF aujourd'hui, et le
--      31/12 tombe après son premier départ, alors qu'il était revenu. Cas
--      possible aussi pour un temporaire titularisé, si la fin de son contrat
--      (35) est restée comme un départ.
-- Un troisième cas vient de la date d'embauche : si elle a été remplacée par
-- la date du retour, la première période de l'agent n'est pas comptée.
-- Les temporaires ne sont pas en cause : au 31/12 où leur situation est
-- TEMPORAIRE, la requête suit leurs contrats (G7), pas G2.
-- Ce que le lot doit trancher :
--   - combien d'allers-retours, de quelle sorte : vraie absence, ou simple
--     passage d'un contrat à l'autre le même jour (Q140) ;
--   - quels agents : nature pendant l'absence, fiche aujourd'hui, place de la
--     date d'embauche (Q141) ;
--   - combien d'agents G2 compte à tort ou oublie à chaque 31/12 de 2015 à
--     2025 (Q142). Si c'est négligeable, G2 reste telle quelle ; sinon, la
--     présence se lit dans la suite des décisions (dernier mouvement avant
--     le 31/12 : un départ ou un engagement).
-- Bilan (07/11/2026) : G2 suffit, la requête de l'effectif ne change pas.
--   - Les seuls allers-retours (41, pour 30 agents) sont des temporaires dont
--     le contrat a pris fin (35) et qui ont été réengagés plus tard sur un
--     nouveau contrat temporaire (50). Aucun permanent n'est parti puis
--     revenu. Aucun départ n'est suivi directement d'une titularisation (51).
--   - 27 de ces agents étaient temporaires pendant leur absence : leurs
--     contrats sont suivis par G7, pas par G2.
--   - G2 ne compte personne pendant une absence. Elle n'oublie qu'un agent,
--     sans situation, de 2015 à 2019. Les exercices lus par T65 (2020 à
--     2025) ne changent pas.
--   - Au passage : 747 permanents au 31/12/2024, pour 742 aux lots 17 et 18.
--     L'écart vient très probablement des fiches sans état, comptées par G2
--     depuis le 05/11 (le lot 17 ne prenait que les fiches ACTIF).
-- Mouvements pris en compte, comme dans la requête de l'effectif :
--   - départs : décisions 02 retraite, 34 décès, 35 fin de contrat,
--     36 démission, 52 rupture, 54 licenciement, et la date de décès de la
--     fiche ;
--   - engagements : décisions 50 temporaire, 51 permanent ;
--   - avec une date de vigueur, et non annulées par une décision 40 (qui cite
--     la décision annulée dans NO_DECI_MODIF). L'agent est porté par la
--     décision, ou par sa liste quand elle est collective
--     (GRH_DECISION_PERSONNE).
-- Aller-retour : un départ dont le mouvement suivant de l'agent est un
-- engagement. Le même jour, le départ est rangé avant l'engagement. Les
-- dates sont ramenées au jour (TRUNC) pour mesurer les délais.
-- Numérotation : suite du lot 24, Q140 à Q142.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q140 — Les allers-retours : départ, retour et délai entre les deux
-- Objectif : compter les allers-retours (un départ suivi, comme mouvement
--            suivant, d'un engagement 50 ou 51) par code de départ, code du
--            retour et délai entre les deux.
-- Tableaux : T58, T59, T61, T65 (effectif des années passées, règle G2) ;
--            T61 (ancienneté, G5).
-- Lecture  : « a. même jour » avec 35 puis 51 = un temporaire titularisé,
--            sans absence. Les délais d'un jour et plus sont de vraies
--            absences. La colonne agents compte chaque agent une fois par
--            ligne.
-- Résultat (07/11/2026) : départ · retour · délai · allers-retours · agents
--            35 · 50 · 1 à 31 jours · 3 · 3
--            35 · 50 · 1 à 12 mois · 23 · 15
--            35 · 50 · plus d'un an · 15 · 15
--            Total : 41 allers-retours. Aucune autre ligne : ni retour par
--            une 51, ni départ 02, 34, 36, 52 ou 54 suivi d'un engagement,
--            ni passage le même jour.
-- Constat  : - les seuls allers-retours sont des fins de contrat (35)
--              suivies, plus tard, d'un nouvel engagement temporaire (50) ;
--            - aucun permanent n'est parti puis revenu ;
--            - aucun départ n'est suivi directement d'une titularisation
--              (51) : le cas d'un temporaire titularisé dont la fin de
--              contrat compterait comme premier départ n'existe pas.
-- -----------------------------------------------------------------------------
WITH dec AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54', '50', '51')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
evts AS (
    SELECT DISTINCT PERS_ID, dt,
           CASE WHEN code IN ('50', '51') THEN 'E' ELSE 'D' END AS sens, code
    FROM dec
    WHERE PERS_ID IS NOT NULL
    UNION
    SELECT PERS_ID, TRUNC(DATE_DECES), 'D', 'deces'
    FROM USER_GRH.GRH_PERSONNE
    WHERE DATE_DECES IS NOT NULL
),
suite AS (
    SELECT PERS_ID, dt, sens, code,
           LEAD(sens) OVER (PARTITION BY PERS_ID
                            ORDER BY dt, CASE sens WHEN 'D' THEN 0 ELSE 1 END, code) AS sens_suivant,
           LEAD(code) OVER (PARTITION BY PERS_ID
                            ORDER BY dt, CASE sens WHEN 'D' THEN 0 ELSE 1 END, code) AS code_suivant,
           LEAD(dt)   OVER (PARTITION BY PERS_ID
                            ORDER BY dt, CASE sens WHEN 'D' THEN 0 ELSE 1 END, code) AS dt_suivant
    FROM evts
),
ar AS (
    SELECT PERS_ID, code AS code_depart, code_suivant AS code_retour,
           dt_suivant - dt AS jours
    FROM suite
    WHERE sens = 'D' AND sens_suivant = 'E'
)
SELECT code_depart, code_retour,
       CASE WHEN jours = 0   THEN 'a. même jour'
            WHEN jours <= 31 THEN 'b. 1 à 31 jours'
            WHEN jours <= 365 THEN 'c. 1 à 12 mois'
            ELSE                   'd. plus d''un an' END        AS delai,
       COUNT(*)                                                    AS allers_retours,
       COUNT(DISTINCT PERS_ID)                                     AS agents
FROM ar
GROUP BY code_depart, code_retour,
         CASE WHEN jours = 0   THEN 'a. même jour'
              WHEN jours <= 31 THEN 'b. 1 à 31 jours'
              WHEN jours <= 365 THEN 'c. 1 à 12 mois'
              ELSE                   'd. plus d''un an' END
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- Q141 — Les agents partis puis revenus : nature, fiche et date d'embauche
-- Objectif : pour chaque agent qui a au moins un aller-retour, d'après son
--            premier aller-retour :
--            - la nature de sa situation pendant l'absence : la dernière
--              situation commencée avant le jour du départ (les départs ne
--              créent presque jamais de situation, lot 24 ; un engagement 51
--              le jour même en crée une, qui ne doit pas compter ici) ;
--            - sa fiche aujourd'hui (ACTIF, INACTIF, vide) ;
--            - la place de sa date d'entrée (embauche, à défaut prise de
--              service) : avant le départ, pendant l'absence, le jour du
--              retour, après ;
--            - absence d'au moins un jour, ou passage le même jour ;
--            par qualité (A agent CNSS, B fonction publique, C affecté).
-- Tableaux : T58, T59, T61, T65 (G2).
-- Lecture  : seules les lignes dont la nature n'est pas TEMPORAIRE relèvent
--            de G2. Fiche ACTIF ou vide : risque d'erreur 1 (compté pendant
--            l'absence). Fiche INACTIF : risque d'erreur 2 (oublié après le
--            retour). Entrée le jour du retour ou après : la date d'embauche
--            a été remplacée, la première période n'est pas comptée.
-- Résultat (07/11/2026) : qualité · nature pendant l'absence · absence ·
--            fiche · entrée · agents
--            A · TEMPORAIRE · au moins un jour · ACTIF · avant le départ · 4
--            A · TEMPORAIRE · au moins un jour · INACTIF · avant le départ · 23
--            A · sans situation · au moins un jour · INACTIF · pendant
--            l'absence · 3
--            Total : 30 agents, tous agents CNSS (qualité A).
-- Constat  : - 27 agents sur 30 étaient temporaires pendant leur absence :
--              la requête suit leurs contrats (G7), G2 ne les touche pas ;
--            - les 3 autres n'ont aucune situation avant leur départ, et
--              leur date d'embauche tombe pendant leur absence. Sans
--              situation, G2 s'applique à eux ; leur fiche INACTIF peut les
--              faire oublier (Q142).
-- -----------------------------------------------------------------------------
WITH dec AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54', '50', '51')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
evts AS (
    SELECT DISTINCT PERS_ID, dt,
           CASE WHEN code IN ('50', '51') THEN 'E' ELSE 'D' END AS sens, code
    FROM dec
    WHERE PERS_ID IS NOT NULL
    UNION
    SELECT PERS_ID, TRUNC(DATE_DECES), 'D', 'deces'
    FROM USER_GRH.GRH_PERSONNE
    WHERE DATE_DECES IS NOT NULL
),
suite AS (
    SELECT PERS_ID, dt, sens,
           LEAD(sens) OVER (PARTITION BY PERS_ID
                            ORDER BY dt, CASE sens WHEN 'D' THEN 0 ELSE 1 END, code) AS sens_suivant,
           LEAD(dt)   OVER (PARTITION BY PERS_ID
                            ORDER BY dt, CASE sens WHEN 'D' THEN 0 ELSE 1 END, code) AS dt_suivant
    FROM evts
),
premier_ar AS (
    SELECT PERS_ID, dt_depart, dt_retour
    FROM (SELECT PERS_ID, dt AS dt_depart, dt_suivant AS dt_retour,
                 ROW_NUMBER() OVER (PARTITION BY PERS_ID ORDER BY dt) AS rn
          FROM suite
          WHERE sens = 'D' AND sens_suivant = 'E')
    WHERE rn = 1
),
nature_absence AS (
    SELECT PERS_ID, NATURE_EMPLOIE
    FROM (SELECT s.PERS_ID, TO_CHAR(s.NATURE_EMPLOIE) AS NATURE_EMPLOIE,
                 ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                                    ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
          FROM USER_GRH.GRH_SITUATION s
          JOIN premier_ar pa ON pa.PERS_ID = s.PERS_ID
          WHERE TRUNC(s.DATE_DEBUT) < pa.dt_depart)
    WHERE rn = 1
),
agents AS (
    SELECT TO_CHAR(p.QUALITE)                                  AS qualite,
           NVL(na.NATURE_EMPLOIE, 'sans situation')            AS nature_absence,
           NVL(TO_CHAR(p.ETAT_EMPLOYE), 'vide')                AS fiche,
           CASE WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) IS NULL
                     THEN 'e. sans date'
                WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= pa.dt_depart
                     THEN 'a. avant le départ'
                WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <  pa.dt_retour
                     THEN 'b. pendant l''absence'
                WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) =  pa.dt_retour
                     THEN 'c. le jour du retour'
                ELSE      'd. après le retour' END             AS entree,
           CASE WHEN pa.dt_retour = pa.dt_depart THEN 'même jour'
                ELSE 'au moins un jour' END                    AS absence
    FROM premier_ar pa
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = pa.PERS_ID
    LEFT JOIN nature_absence na ON na.PERS_ID = pa.PERS_ID
)
SELECT qualite, nature_absence, absence, fiche, entree, COUNT(*) AS agents
FROM agents
GROUP BY qualite, nature_absence, absence, fiche, entree
ORDER BY 1, 2, 3, 4, 5;


-- -----------------------------------------------------------------------------
-- Q142 — À chaque 31/12 de 2015 à 2025 : ce que G2 compte à tort ou oublie
-- Objectif : pour les agents CNSS (qualité A) dont la situation au 31/12
--            n'est pas TEMPORAIRE (le champ de G2), comparer deux lectures :
--            - G2, comme dans la requête : entré au plus tard le 31/12, et
--              fiche pas INACTIF ou premier départ après le 31/12 ;
--            - la suite des décisions : présent si son dernier mouvement au
--              plus tard le 31/12 est un engagement (50, 51), absent si c'est
--              un départ ; sans mouvement, présent s'il est entré.
--            Les écarts ne sont comptés que pour les agents qui ont au moins
--            un aller-retour : c'est l'effet de ce seul cas. Les autres écarts
--            sont déjà connus (les 84 engagés de 2019, règle G11).
-- Tableaux : T58, T59, T61, T65 (années passées) ; T65 lit 5 exercices.
-- Lecture  : comptes_g2 = permanents et agents sans situation comptés par
--            G2 ; dont_permanents doit être proche de 742 au 31/12/2024
--            (lots 17 et 18 : 742, avant que la fiche vide compte, 05/11).
--            comptes_absents = erreur 1 ; oublies_presents = erreurs 2 et 3 ;
--            comptes_corriges = comptes_g2 - comptes_absents
--            + oublies_presents. Un écart de quelques agents par an ne change
--            pas G2.
-- Résultat (07/11/2026) : 31/12 · comptes_g2 · dont_permanents ·
--            dont_partis_revenus · comptes_absents · oublies_presents ·
--            comptes_corriges
--            2015 · 934 · 931 · 0 · 0 · 1 · 935
--            2016 · 963 · 958 · 0 · 0 · 1 · 964
--            2017 · 916 · 911 · 0 · 0 · 1 · 917
--            2018 · 871 · 867 · 0 · 0 · 1 · 872
--            2019 · 827 · 823 · 0 · 0 · 1 · 828
--            2020 · 786 · 782 · 0 · 0 · 0 · 786
--            2021 · 750 · 746 · 0 · 0 · 0 · 750
--            2022 · 716 · 712 · 0 · 0 · 0 · 716
--            2023 · 681 · 677 · 0 · 0 · 0 · 681
--            2024 · 751 · 747 · 0 · 0 · 0 · 751
--            2025 · 890 · 886 · 0 · 0 · 0 · 890
-- Constat  : - aucun agent parti puis revenu n'est compté par G2
--              (dont_partis_revenus = 0) : personne n'est compté pendant une
--              absence ;
--            - G2 oublie 1 agent par an de 2015 à 2019, sans doute l'un des
--              3 agents sans situation de Q141, puis plus aucun. Les
--              exercices de T65 (2020 à 2025) ne changent pas : G2 reste
--              telle quelle ;
--            - 747 permanents au 31/12/2024, pour 742 aux lots 17 et 18 :
--              très probablement les fiches sans état, comptées depuis le
--              05/11 (le lot 17 ne prenait que les fiches ACTIF). L'écart avec
--              l'annuaire de la CNSS (758) passe de 16 à 11 ;
--            - 886 permanents au 31/12/2025, soit 139 de plus qu'en 2024 : à
--              rapprocher des recrutements de 2025 (T60) quand l'annuaire de
--              2025 paraîtra.
-- -----------------------------------------------------------------------------
WITH dates AS (
    SELECT TO_DATE(TO_CHAR(2014 + LEVEL) || '1231', 'YYYYMMDD') AS dt
    FROM DUAL
    CONNECT BY LEVEL <= 11
),
dec AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54', '50', '51')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
evts AS (
    SELECT DISTINCT PERS_ID, dt,
           CASE WHEN code IN ('50', '51') THEN 'E' ELSE 'D' END AS sens, code
    FROM dec
    WHERE PERS_ID IS NOT NULL
    UNION
    SELECT PERS_ID, TRUNC(DATE_DECES), 'D', 'deces'
    FROM USER_GRH.GRH_PERSONNE
    WHERE DATE_DECES IS NOT NULL
),
-- les agents qui ont au moins un aller-retour
concernes AS (
    SELECT DISTINCT PERS_ID
    FROM (SELECT PERS_ID, sens,
                 LEAD(sens) OVER (PARTITION BY PERS_ID
                                  ORDER BY dt, CASE sens WHEN 'D' THEN 0 ELSE 1 END, code) AS sens_suivant
          FROM evts)
    WHERE sens = 'D' AND sens_suivant = 'E'
),
-- G2 : le premier départ
premier_depart AS (
    SELECT PERS_ID, MIN(dt) AS dt_depart
    FROM evts
    WHERE sens = 'D'
    GROUP BY PERS_ID
),
-- la suite des décisions : le dernier mouvement au plus tard le 31/12
-- (le même jour, l'engagement passe après le départ)
dernier_mvt AS (
    SELECT da.dt AS date_arrete, e.PERS_ID, e.sens,
           ROW_NUMBER() OVER (PARTITION BY da.dt, e.PERS_ID
                              ORDER BY e.dt DESC,
                                       CASE e.sens WHEN 'E' THEN 0 ELSE 1 END) AS rn
    FROM dates da
    JOIN evts e ON e.dt <= da.dt
),
-- la dernière situation commencée au plus tard le 31/12
situ AS (
    SELECT da.dt AS date_arrete, s.PERS_ID, TO_CHAR(s.NATURE_EMPLOIE) AS nature,
           ROW_NUMBER() OVER (PARTITION BY da.dt, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM dates da
    JOIN USER_GRH.GRH_SITUATION s ON s.DATE_DEBUT <= da.dt
),
lectures AS (
    SELECT da.dt AS date_arrete,
           NVL(si.nature, 'sans situation')                    AS nature,
           CASE WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= da.dt
                 AND (NVL(TO_CHAR(p.ETAT_EMPLOYE), '-') <> 'INACTIF'
                      OR pd.dt_depart > da.dt)
                THEN 1 ELSE 0 END                              AS g2,
           CASE WHEN dm.sens = 'E' THEN 1
                WHEN dm.sens = 'D' THEN 0
                WHEN COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= da.dt THEN 1
                ELSE 0 END                                     AS selon_decisions,
           CASE WHEN c.PERS_ID IS NOT NULL THEN 1 ELSE 0 END   AS concerne
    FROM dates da
    CROSS JOIN USER_GRH.GRH_PERSONNE p
    LEFT JOIN situ si           ON si.date_arrete = da.dt
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
    LEFT JOIN premier_depart pd ON pd.PERS_ID     = p.PERS_ID
    LEFT JOIN dernier_mvt dm    ON dm.date_arrete = da.dt
                               AND dm.PERS_ID     = p.PERS_ID
                               AND dm.rn          = 1
    LEFT JOIN concernes c       ON c.PERS_ID      = p.PERS_ID
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND NVL(si.nature, '-') <> 'TEMPORAIRE'
)
SELECT TO_CHAR(date_arrete, 'DD/MM/YYYY')                      AS arrete,
       SUM(g2)                                                 AS comptes_g2,
       SUM(CASE WHEN nature = 'PERMANENT' THEN g2 ELSE 0 END)  AS dont_permanents,
       SUM(CASE WHEN concerne = 1 THEN g2 ELSE 0 END)          AS dont_partis_revenus,
       SUM(CASE WHEN concerne = 1 AND g2 = 1 AND selon_decisions = 0
                THEN 1 ELSE 0 END)                             AS comptes_absents,
       SUM(CASE WHEN concerne = 1 AND g2 = 0 AND selon_decisions = 1
                THEN 1 ELSE 0 END)                             AS oublies_presents,
       SUM(g2)
       - SUM(CASE WHEN concerne = 1 AND g2 = 1 AND selon_decisions = 0
                  THEN 1 ELSE 0 END)
       + SUM(CASE WHEN concerne = 1 AND g2 = 0 AND selon_decisions = 1
                  THEN 1 ELSE 0 END)                           AS comptes_corriges
FROM lectures
GROUP BY date_arrete
ORDER BY date_arrete;
