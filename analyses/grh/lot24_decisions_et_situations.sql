-- =============================================================================
-- LOT 24 — Les décisions face aux situations : entrées, sorties et retours
-- Rédigé le 05/11/2026 — EXÉCUTÉ le 05/11/2026 sur USER_GRH.
-- Exécution : en mode script (F5) dans SQL Developer. Comptages seulement :
-- aucune donnée nominative.
-- Pourquoi ce lot : la requête de l'effectif lit trois tables.
--   - La fiche de l'agent (GRH_PERSONNE) donne son entrée : date d'embauche,
--     à défaut prise de service (G2).
--   - Les décisions (GRH_DECISION) donnent les départs (G2), les contrats des
--     temporaires (G7), les disponibilités et les remises en activité (QG6,
--     et le temps retiré de l'ancienneté, G5).
--   - La dernière situation (GRH_SITUATION) donne le classement de l'agent,
--     et son statut, qui sert à compter les agents inactifs
--     (NB_AGENTS_INACTIFS : statuts B, D, E, F, H).
-- Le lot 2 (Q08, Q09) a montré que la nature de la situation n'a aucun code
-- pour une sortie ou un retour. Le lot 18 (Q109) a vu, chez les permanents,
-- 2 passages au statut E (disponibilité) en six ans, pour 18 disponibilités
-- publiées. Ce lot vérifie, agent par agent, que les trois tables racontent
-- la même histoire :
--   1. le classement à l'embauche (CLA) : combien par agent (Q134), et à
--      quelle date par rapport à l'embauche (Q135) ;
--   2. les décisions d'engagement et de position créent-elles une situation,
--      et de quelle nature (Q136) ;
--   3. la disponibilité : le statut de la situation après une décision 07 ou
--      18 (Q137), les passages au statut E avec ou sans décision 07 (Q138), et
--      ce que cela change au 31/12/2024 pour les permanents (Q139).
-- Ce que le lot doit trancher :
--   - si CLA peut servir à contrôler la date d'entrée (Q134, Q135) ;
--   - si les agents inactifs doivent se lire dans les décisions 07 et 18
--     plutôt que dans le statut de la situation, comme la disponibilité de
--     QG6 et l'ancienneté de G5 (Q137 à Q139).
-- Bilan (05/11/2026) :
--   - l'entrée reste la date d'embauche. Le CLA ne la suit que depuis 2019
--     (9 agents sur 10) ; avant 2000, c'est une reprise des données. Il peut
--     au mieux servir de contrôle pour les embauches récentes ;
--   - les disponibilités, les mises à disposition, les remises en activité et
--     les renouvellements de contrat ne créent presque jamais de situation :
--     on les lit seulement dans les décisions. Seul l'engagement permanent
--     (51) en crée une, quatre fois sur cinq ;
--   - le statut de la situation n'est pas daté : il est modifié sur la
--     situation en cours quand l'agent part ou s'absente, au lieu d'ouvrir une
--     nouvelle situation (à confirmer par la DSI). Il dit l'état
--     d'aujourd'hui, pas celui de la date d'arrêté. Au 31/12/2024, les 6
--     permanents en disponibilité selon les décisions sont au statut A, donc
--     comptés actifs par NB_AGENTS_INACTIFS ;
--   - proposition BI, à valider : compter les agents en disponibilité par les
--     décisions 07 et 18, comme QG6 et G5, et non par le statut.
-- Décisions prises en compte : avec une date de vigueur, et non annulées par
-- une décision 40 (qui cite la décision annulée dans NO_DECI_MODIF). L'agent
-- est porté par la décision, ou par sa liste quand elle est collective
-- (GRH_DECISION_PERSONNE).
-- Codes de décision (lot 3, Q15) : 07 mise en disponibilité, 14 mise à
-- disposition, 18 remise en activité, 37 engagement de détaché, 50
-- engagement temporaire, 51 engagement permanent.
-- Statuts de situation (lot 2, Q08) : A activité normale, B détachement,
-- C stage, D mise à disposition, E disponibilité, F suspendu, G congé payé,
-- H sous les drapeaux, R retraite, X radié, Y décédé.
-- Numérotation : suite du lot 23, Q134 à Q139.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q134 — Combien de classements à l'embauche (CLA) par agent
-- Objectif : répartir tous les agents selon leur nombre de situations de
--            nature CLA (0, 1, 2, 3 et plus), par qualité (A agent CNSS,
--            B fonction publique, C agent affecté) et par décisions
--            d'engagement (50 temporaire, 51 permanent).
-- Tableaux : T58, T59, T61, T65 (date d'entrée de l'effectif).
-- Lecture  : le lot 2 compte 3 418 CLA pour 3 180 agents. Si les agents à
--            2 CLA ou plus sont des temporaires à plusieurs contrats (lot 13,
--            Q87 : un sur trois enchaîne trois contrats ou plus), un CLA est
--            créé à chaque engagement ; la colonne meme_nb_cla_que_engagements
--            le mesure. Les agents à 0 CLA sont-ils des agents sans aucune
--            situation (colonne sans_aucune_situation) ?
-- Résultat (05/11/2026) : CLA · qualité · engagement · agents · même nombre
--            de CLA que d'engagements · sans aucune situation
--            0 · A : aucune 228 · 0 · 111 ; 50 seulement 31 · 0 · 3 ;
--                    51 seulement 185 · 0 · 0
--            0 · B : aucune 13 · 0 · 7.   0 · C : aucune 2 · 0 · 0
--            1 · A : aucune 1 213 ; 50 seulement 523 · 252 ; 51 seulement
--                    534 · 497
--            1 · B : aucune 22 ; 51 seulement 2 · 1.   1 · C : aucune 17.
--            1 · - : 50 seulement 1 · 1
--            2 · A : aucune 119 ; 50 seulement 38 · 8 ; 51 seulement 72 · 17
--            2 · B : aucune 1.   2 · C : aucune 2
--            3 et plus · A : aucune 118 ; 50 seulement 14 · 2 ; 51 seulement
--                    43 · 0.   3 et plus · B : aucune 2
--            Total 3 180 agents : 459 sans CLA, 2 312 avec un, 232 avec deux,
--            177 avec trois ou plus. Aucun agent n'a à la fois une 50 et une 51.
-- Constat  : - le CLA n'est pas recréé à chaque contrat : 523 temporaires
--              (50) n'ont qu'un CLA, dont 271 ont pourtant plusieurs
--              engagements 50. Les agents à plusieurs CLA sont surtout des
--              agents sans décision d'engagement (242 sur 409), donc anciens ;
--            - un engagement permanent (51) va avec un CLA : 497 des 534
--              permanents engagés qui ont un CLA ont une seule 51 ;
--            - 459 agents n'ont pas de CLA : 121 n'ont aucune situation, 185
--              sont des permanents engagés par une 51 dont la première
--              situation a une autre nature (affectation, nomination : Q136).
-- -----------------------------------------------------------------------------
WITH situ_agent AS (
    SELECT PERS_ID,
           COUNT(*) AS nb_situ,
           SUM(CASE WHEN UPPER(TRIM(NATURE_SITUATION)) = 'CLA' THEN 1 ELSE 0 END) AS nb_cla
    FROM USER_GRH.GRH_SITUATION
    GROUP BY PERS_ID
),
engagements AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           COUNT(DISTINCT CASE WHEN d.CDEC_CODE = '50' THEN d.DECI_NO END) AS nb_50,
           COUNT(DISTINCT CASE WHEN d.CDEC_CODE = '51' THEN d.DECI_NO END) AS nb_51
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('50', '51')
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
),
agents AS (
    SELECT CASE WHEN NVL(sa.nb_cla, 0) >= 3 THEN '3 et plus'
                ELSE TO_CHAR(NVL(sa.nb_cla, 0)) END                  AS nb_cla,
           NVL(TO_CHAR(p.QUALITE), '-')                              AS qualite,
           CASE WHEN NVL(e.nb_50, 0) > 0 AND NVL(e.nb_51, 0) > 0 THEN '50 et 51'
                WHEN NVL(e.nb_50, 0) > 0                         THEN '50 seulement'
                WHEN NVL(e.nb_51, 0) > 0                         THEN '51 seulement'
                ELSE 'aucune' END                                    AS engagement,
           CASE WHEN NVL(sa.nb_cla, 0) > 0
                 AND NVL(sa.nb_cla, 0) = NVL(e.nb_50, 0) + NVL(e.nb_51, 0)
                THEN 1 ELSE 0 END                                    AS meme_nb,
           CASE WHEN sa.PERS_ID IS NULL THEN 1 ELSE 0 END             AS sans_situ
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN situ_agent sa  ON sa.PERS_ID = p.PERS_ID
    LEFT JOIN engagements e  ON e.PERS_ID  = p.PERS_ID
)
SELECT nb_cla, qualite, engagement,
       COUNT(*)      AS agents,
       SUM(meme_nb)  AS meme_nb_cla_que_engagements,
       SUM(sans_situ) AS sans_aucune_situation
FROM agents
GROUP BY nb_cla, qualite, engagement
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- Q135 — La date du classement à l'embauche face à la date d'embauche
-- Objectif : pour chaque agent, comparer la date de début de son premier CLA
--            à sa date d'entrée (date d'embauche, à défaut prise de service) :
--            même jour, 1 à 31 jours d'écart, 32 jours à un an, plus d'un an
--            avant ou après, date du CLA aberrante (année avant 1950 ou après
--            2030), par période d'embauche. La dernière colonne compte, à
--            part, les CLA qui tombent le jour de la prise de service.
-- Tableaux : T58, T59, T61, T65 (date d'entrée de l'effectif).
-- Lecture  : si le premier CLA tombe le jour de l'embauche, au moins pour les
--            embauches récentes (2019 et après), il peut servir de contrôle
--            de la date d'entrée. Le lot 2 a vu des années aberrantes dans les
--            CLA (212, 3015).
-- Résultat (05/11/2026) : période d'embauche · écart · agents · dont CLA le
--            jour de la prise de service
--            avant 2000 (1 297) : aucun CLA 223 ; même jour 157 · 85 ; 1 à 31
--              jours 35 · 25 ; 32 jours à un an 51 · 5 ; plus d'un an après
--              830 · 1 ; plus d'un an avant 1 · 0
--            de 2000 à 2018 (1 380) : aucun CLA 180 ; même jour 479 · 218 ;
--              1 à 31 jours 323 · 119 ; 32 jours à un an 371 · 163 ; plus d'un
--              an après 4 · 0 ; plus d'un an avant 22 · 1 ; aberrante 1 · 0
--            2019 et après (503) : aucun CLA 56 ; même jour 278 · 132 ; 1 à 31
--              jours 117 · 89 ; 32 jours à un an 3 · 0 ; plus d'un an avant
--              49 · 30
-- Constat  : - avant 2000, le CLA vient plus d'un an après l'embauche pour
--              830 agents sur 1 297 : c'est la reprise des données dans
--              l'application, pas le classement du jour de l'embauche ;
--            - depuis 2019, le CLA tombe le jour de l'embauche (278) ou le
--              jour de la prise de service (89 + 30) pour 397 des 447 agents
--              qui en ont un, soit 9 sur 10 ;
--            - en tout, 868 CLA tombent le jour de la prise de service : quand
--              il ne suit pas l'embauche, le CLA suit la prise de service ;
--            - 49 agents embauchés depuis 2019 ont un CLA plus d'un an avant
--              leur embauche, 30 d'entre eux le jour de leur prise de
--              service : leur prise de service précède l'embauche de plus d'un
--              an (pris d'abord sous un autre statut ?) ;
--            - le CLA ne remplace pas la date d'embauche, qui retrouve les 104
--              recrutements de 2024 (lot 17). Il peut au mieux contrôler les
--              embauches récentes.
-- -----------------------------------------------------------------------------
WITH cla AS (
    SELECT PERS_ID, MIN(DATE_DEBUT) AS debut_cla
    FROM USER_GRH.GRH_SITUATION
    WHERE UPPER(TRIM(NATURE_SITUATION)) = 'CLA'
    GROUP BY PERS_ID
),
agents AS (
    SELECT COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) AS date_entree,
           p.DATE_PRISE_SERVICE                            AS date_prise_service,
           c.debut_cla
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN cla c ON c.PERS_ID = p.PERS_ID
),
ecarts AS (
    SELECT CASE WHEN date_entree IS NULL                     THEN '0. sans date d''entree'
                WHEN EXTRACT(YEAR FROM date_entree) < 2000   THEN '1. avant 2000'
                WHEN EXTRACT(YEAR FROM date_entree) < 2019   THEN '2. de 2000 a 2018'
                ELSE                                              '3. 2019 et apres' END AS periode_embauche,
           CASE WHEN debut_cla IS NULL                       THEN '0. aucun CLA'
                WHEN EXTRACT(YEAR FROM debut_cla) < 1950
                  OR EXTRACT(YEAR FROM debut_cla) > 2030     THEN '7. date du CLA aberrante'
                WHEN date_entree IS NULL                     THEN '8. pas de date d''entree'
                WHEN TRUNC(debut_cla) = TRUNC(date_entree)   THEN '1. meme jour'
                WHEN ABS(debut_cla - date_entree) <= 31      THEN '2. 1 a 31 jours'
                WHEN ABS(debut_cla - date_entree) <= 365     THEN '3. 32 jours a un an'
                WHEN debut_cla > date_entree                 THEN '4. plus d''un an apres'
                ELSE                                              '5. plus d''un an avant' END AS ecart,
           CASE WHEN TRUNC(debut_cla) = TRUNC(date_prise_service) THEN 1 ELSE 0 END AS jour_ps
    FROM agents
)
SELECT periode_embauche, ecart,
       COUNT(*)     AS agents,
       SUM(jour_ps) AS dont_cla_jour_prise_service
FROM ecarts
GROUP BY periode_embauche, ecart
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q136 — Les décisions d'engagement et de position créent-elles une situation ?
-- Objectif : pour chaque décision 07, 14, 18, 37, 50 et 51 (non annulée,
--            datée), chercher la situation de l'agent dont la date de début
--            est la plus proche de la date de vigueur, à 60 jours près.
--            Compter, par code de décision, la nature et le statut de cette
--            situation, ou « AUCUNE A 60 JOURS ».
-- Tableaux : T58, T61, T65 (inactifs, ancienneté) ; T58 à T65 (entrée).
-- Lecture  : une décision qui crée toujours une situation d'une même nature
--            (par exemple 14 et DIS, mise à disposition) laisse une trace dans
--            les situations. Une décision suivie de natures diverses ou
--            d'aucune situation n'en laisse pas : elle se lit seulement dans
--            les décisions. Le lot 2 n'a trouvé aucune nature pour la
--            disponibilité, la remise en activité ou le réengagement.
-- Résultat (05/11/2026) : décision · nature de la situation la plus proche ·
--            statut · décisions (« aucune » compte aussi 6 situations sans
--            nature, après une 50)
--            07 (54)    : aucune 53 ; CLA E 1
--            14 (53)    : aucune 43 ; AVA 5 (A 3, vide 2) ; AFF A 3 ; REC A 1 ;
--                         NOM A 1. Jamais DIS
--            18 (78)    : aucune 55 ; REC 6 (A 4, C 2) ; AFF A 5 ; REM A 4 ;
--                         AVA A 4 ; CLA A 2 ; NOM A 2
--            37 (6)     : aucune 3 ; CLA B 2 ; DET B 1
--            50 (1 494) : aucune 1 095 ; CLA 381 (A 232, X 145, vide 2, R 1,
--                         D 1) ; REM 11 (A 10, X 1) ; AFF 5 (X 3, A 2) ; MUT A 1 ;
--                         REC X 1
--            51 (920)   : CLA 533 (A 503, X 13, Y 6, H 5, R 4, E 1, F 1) ;
--                         aucune 185 ; AFF 129 (A 119, X 6, E 3, R 1) ; NOM 37
--                         (A 33, R 3, E 1) ; REC 15 (A 9, H 6) ; MUT 14 (A 11,
--                         X 1, R 1, E 1) ; AVA R 4 ; BON R 1 ; ANN A 1 ; ABA A 1
-- Constat  : - la mise en disponibilité (07), la mise à disposition (14) et la
--              remise en activité (18) ne créent presque jamais de situation
--              (53 sur 54, 43 sur 53, 55 sur 78 sans situation à 60 jours). La
--              mise à disposition ne donne jamais la nature DIS. Ces mouvements
--              se lisent seulement dans les décisions ;
--            - un engagement temporaire (50) crée une situation une fois sur
--              quatre (CLA 381 sur 1 494) : le premier contrat, pas les
--              renouvellements (Q134) ;
--            - un engagement permanent (51) crée une situation quatre fois sur
--              cinq : un CLA (533), ou une affectation ou une nomination (166) ;
--            - un détaché accueilli (37) reçoit un CLA ou un DET au statut B ;
--            - le statut lu aujourd'hui n'est pas celui du jour de la
--              décision : 145 CLA d'engagements temporaires sont au statut X
--              (radié), des CLA d'engagements permanents aux statuts R, Y, H.
--              Le départ semble inscrit sur la situation en cours ;
--            - natures absentes de la liste du lot 2 : REM (remise en
--              activité ? le même code existe dans GRH_MOUVEMENT, lot 1), DET
--              (détachement), ANN et ABA (une fois chacune, sens inconnu).
-- -----------------------------------------------------------------------------
WITH decisions AS (
    SELECT d.DECI_NO, NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE) AS code, d.DATE_VIGUEUR AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('07', '14', '18', '37', '50', '51')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
proches AS (
    SELECT de.code,
           UPPER(TRIM(s.NATURE_SITUATION)) AS nature,
           TO_CHAR(s.STATUT_EMPLOYE)       AS statut,
           ROW_NUMBER() OVER (PARTITION BY de.DECI_NO, de.PERS_ID
                              ORDER BY ABS(s.DATE_DEBUT - de.dt), s.SITU_ID DESC) AS rn
    FROM decisions de
    LEFT JOIN USER_GRH.GRH_SITUATION s
           ON s.PERS_ID    = de.PERS_ID
          AND s.DATE_DEBUT BETWEEN de.dt - 60 AND de.dt + 60
    WHERE de.PERS_ID IS NOT NULL
)
SELECT code                             AS decision,
       NVL(nature, 'AUCUNE A 60 JOURS') AS nature_situation,
       NVL(statut, '-')                 AS statut,
       COUNT(*)                         AS decisions
FROM proches
WHERE rn = 1
GROUP BY code, NVL(nature, 'AUCUNE A 60 JOURS'), NVL(statut, '-')
ORDER BY 1, 4 DESC;


-- -----------------------------------------------------------------------------
-- Q137 — Le statut en vigueur après une mise en disponibilité ou un retour
-- Objectif : pour chaque décision 07 (mise en disponibilité) et 18 (remise
--            en activité), non annulée et datée, lire le statut de la dernière
--            situation commencée au plus tard 30 jours après la date de
--            vigueur, avant 2019 et depuis 2019.
-- Tableaux : T58, T61, T65 (agents inactifs).
-- Lecture  : si la saisie concorde, on lit E après une 07 et A après une 18.
--            Une 07 suivie du statut A veut dire que l'agent en disponibilité
--            est compté actif par le statut, alors que la décision le met en
--            disponibilité.
-- Résultat (05/11/2026) : décision · période · statut en vigueur · décisions
--            07 avant 2019 (24)   : A 10 ; E 5 ; R 3 ; X 3 ; vide 2 ; Y 1
--            07 depuis 2019 (30)  : E 14 ; A 9 ; X 4 ; Y 2 ; R 1
--            18 avant 2019 (44)   : A 33 ; vide 3 ; X 2 ; D 2 ; R 2 ; C 2
--            18 depuis 2019 (34)  : A 21 ; X 4 ; E 3 ; R 3 ; Y 2 ; B 1
-- Constat  : - après une mise en disponibilité, le statut E n'est lu que 19
--              fois sur 54 (14 sur 30 depuis 2019) ; 19 fois, le statut est
--              resté A ;
--            - 14 décisions 07 et 13 décisions 18 montrent aujourd'hui R, X ou
--              Y : le départ de l'agent, plus tard, a remplacé le statut ;
--            - après une remise en activité, le statut est A dans 54 cas sur
--              78.
-- -----------------------------------------------------------------------------
WITH decisions AS (
    SELECT d.DECI_NO, NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE) AS code, d.DATE_VIGUEUR AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('07', '18')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
en_vigueur AS (
    SELECT de.code,
           CASE WHEN EXTRACT(YEAR FROM de.dt) < 2019 THEN 'avant 2019'
                ELSE '2019 et apres' END       AS periode,
           TO_CHAR(s.STATUT_EMPLOYE)           AS statut,
           ROW_NUMBER() OVER (PARTITION BY de.DECI_NO, de.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM decisions de
    LEFT JOIN USER_GRH.GRH_SITUATION s
           ON s.PERS_ID    = de.PERS_ID
          AND s.DATE_DEBUT <= de.dt + 30
    WHERE de.PERS_ID IS NOT NULL
)
SELECT code                                AS decision,
       periode,
       NVL(statut, 'vide ou sans situation') AS statut_en_vigueur,
       COUNT(*)                            AS decisions
FROM en_vigueur
WHERE rn = 1
GROUP BY code, periode, NVL(statut, 'vide ou sans situation')
ORDER BY 1, 2, 4 DESC;


-- -----------------------------------------------------------------------------
-- Q138 — Les passages au statut E, avec ou sans décision 07
-- Objectif : chaque fois qu'une situation passe au statut E (disponibilité)
--            alors que la situation précédente de l'agent ne l'était pas,
--            chercher une décision 07 non annulée du même agent : à 60 jours
--            près, à une autre date, ou aucune. Avant 2019 et depuis 2019.
-- Tableaux : T58, T61, T65 (agents inactifs).
-- Lecture  : avec Q137, dit si le statut E et la décision 07 désignent les
--            mêmes disponibilités, ou si chacune en voit d'autres.
-- Résultat (05/11/2026) : période · concordance · passages au statut E
--            avant 2019 : décision 07 à 60 jours 1 ; à une autre date 11 ;
--              aucune décision 07 3
--            depuis 2019 : décision 07 à une autre date 2
-- Constat  : 17 passages au statut E dans toute la base, un seul à moins de
--            60 jours d'une décision 07. Le statut E ne s'ouvre presque jamais
--            par une nouvelle situation : quand il est saisi, c'est sur la
--            situation en cours (Q137).
-- -----------------------------------------------------------------------------
WITH situ AS (
    SELECT s.PERS_ID, s.SITU_ID, s.DATE_DEBUT,
           TO_CHAR(s.STATUT_EMPLOYE) AS statut,
           LAG(TO_CHAR(s.STATUT_EMPLOYE)) OVER (PARTITION BY s.PERS_ID
                                                ORDER BY s.DATE_DEBUT, s.SITU_ID) AS statut_avant
    FROM USER_GRH.GRH_SITUATION s
),
debuts_e AS (
    SELECT PERS_ID, SITU_ID, DATE_DEBUT
    FROM situ
    WHERE statut = 'E'
      AND NVL(statut_avant, '-') <> 'E'
),
d07 AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE = '07'
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
croise AS (
    SELECT e.SITU_ID,
           CASE WHEN EXTRACT(YEAR FROM e.DATE_DEBUT) < 2019 THEN 'avant 2019'
                ELSE '2019 et apres' END                                        AS periode,
           MAX(CASE WHEN ABS(d.dt - e.DATE_DEBUT) <= 60 THEN 1 ELSE 0 END)     AS a_07_proche,
           MAX(CASE WHEN d.PERS_ID IS NOT NULL THEN 1 ELSE 0 END)              AS a_07
    FROM debuts_e e
    LEFT JOIN d07 d ON d.PERS_ID = e.PERS_ID
    GROUP BY e.SITU_ID,
             CASE WHEN EXTRACT(YEAR FROM e.DATE_DEBUT) < 2019 THEN 'avant 2019'
                  ELSE '2019 et apres' END
),
classes AS (
    SELECT periode,
           CASE WHEN a_07_proche = 1 THEN '1. decision 07 a 60 jours'
                WHEN a_07 = 1        THEN '2. decision 07 a une autre date'
                ELSE                      '3. aucune decision 07' END AS concordance
    FROM croise
)
SELECT periode, concordance, COUNT(*) AS passages_au_statut_e
FROM classes
GROUP BY periode, concordance
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q139 — Les permanents en disponibilité au 31/12/2024 : décision ou statut
-- Objectif : pour les permanents présents au 31/12/2024 (règle des lots 17 et
--            18 : 742 agents), croiser deux lectures de la disponibilité :
--            - selon les décisions : une décision 07 au plus tard le
--              31/12/2024, sans décision 18 à la même date ou après ;
--            - selon le statut de la dernière situation commencée au plus
--              tard le 31/12/2024 (A ou autre statut actif, B, D, E, F, H
--              inactifs, ou vide).
-- Tableaux : T58, T61, T65 (agents inactifs, disponibilité de QG6).
-- Lecture  : le total doit faire 742. La ligne OUI × A (actif) compte les
--            agents en disponibilité selon les décisions mais comptés actifs
--            par NB_AGENTS_INACTIFS. La ligne NON × E (inactif) compte
--            l'inverse.
-- Résultat (05/11/2026) : disponibilité selon les décisions · statut de la
--            situation · permanents
--            NON · A (actif) 710 ; NON · R (actif) 24 ; NON · E (inactif) 1 ;
--            NON · F (inactif) 1 ; OUI · A (actif) 6. Total 742.
-- Constat  : - les 6 permanents en disponibilité au 31/12/2024 selon les
--              décisions sont tous au statut A : NB_AGENTS_INACTIFS, qui lit
--              le statut, les compte actifs ;
--            - le statut ne trouve que 2 inactifs (E 1, F 1), sans décision 07
--              ouverte ;
--            - 24 permanents présents au 31/12/2024 portent déjà le statut R
--              (retraite) : leur départ, après cette date, a été inscrit sur
--              une situation commencée avant. Le statut d'une situation dit
--              l'état d'aujourd'hui, pas celui de la date d'arrêté.
-- -----------------------------------------------------------------------------
WITH sorties AS (
    SELECT PERS_ID, MIN(dt) AS dt_sortie
    FROM (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
        WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
          AND d.DATE_VIGUEUR IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                          WHERE a.CDEC_CODE = '40'
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
        UNION ALL
        SELECT PERS_ID, DATE_DECES FROM USER_GRH.GRH_PERSONNE WHERE DATE_DECES IS NOT NULL
    )
    WHERE PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
situ AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE, TO_CHAR(s.STATUT_EMPLOYE) AS statut,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
permanents AS (
    SELECT p.PERS_ID, si.statut
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    WHERE COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND (p.ETAT_EMPLOYE = 'ACTIF' OR so.dt_sortie > DATE '2024-12-31')
      AND TO_CHAR(p.QUALITE) = 'A'
      AND si.NATURE_EMPLOIE = 'PERMANENT'
),
positions AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           MAX(CASE WHEN d.CDEC_CODE = '07' THEN d.DATE_VIGUEUR END) AS derniere_07,
           MAX(CASE WHEN d.CDEC_CODE = '18' THEN d.DATE_VIGUEUR END) AS derniere_18
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('07', '18')
      AND d.DATE_VIGUEUR <= DATE '2024-12-31'
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    GROUP BY NVL(d.PERS_ID, dp.PERS_ID)
),
lectures AS (
    SELECT CASE WHEN po.derniere_07 IS NOT NULL
                 AND (po.derniere_18 IS NULL OR po.derniere_18 < po.derniere_07)
                THEN 'OUI' ELSE 'NON' END                         AS dispo_selon_decisions,
           CASE WHEN pe.statut IS NULL                       THEN 'vide'
                WHEN pe.statut IN ('B', 'D', 'E', 'F', 'H') THEN pe.statut || ' (inactif)'
                ELSE                                              pe.statut || ' (actif)' END AS statut_situation
    FROM permanents pe
    LEFT JOIN positions po ON po.PERS_ID = pe.PERS_ID
)
SELECT dispo_selon_decisions, statut_situation, COUNT(*) AS permanents
FROM lectures
GROUP BY dispo_selon_decisions, statut_situation
ORDER BY 1, 2;
