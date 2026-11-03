-- =============================================================================
-- LOT 21 — Le classement avant et après une formation (CLA_AVANT, CLA_APRES)
-- Rédigé le 02/11/2026 — À EXÉCUTER sur USER_GRH.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages et codes
-- seulement, aucune donnée nominative.
-- Pourquoi ce lot : T62 sépare les formés « avec promotion » et « sans
-- promotion ». La règle proposée (t62_requete_proposee.sql) compte une
-- promotion quand CLA_APRES diffère de CLA_AVANT sur l'inscription. Les lots 9,
-- 17, 18 et 19 n'ont fait que compter ces champs : remplis tous les deux pour
-- 2 inscriptions depuis 2019 (deux formations diplômantes finies en 2024), et
-- changés pour les deux. On n'a jamais lu leurs valeurs. Ce lot :
--   1. donne le type des deux champs (Q118) ;
--   2. lit leurs valeurs, avec la catégorie et la qualification notées sur
--      l'inscription (Q119) ;
--   3. cherche les promotions dans la situation de l'agent avant et après
--      la formation, et dans les décisions 60 et 61 (Q120, réécrite le
--      02/11/2026 en comptages après le résultat de Q119).
-- Chiffres publiés (annuaire de la CNSS 2024, tableau 123) : 480 permanents
-- formés en 2024, dont 6 avec promotion.
-- Numérotation : suite du lot 20, Q118 à Q120.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q118 — Le type des champs de classement de l'inscription
-- Objectif : savoir si CLA_AVANT et CLA_APRES sont des nombres ou des codes,
--            et leur longueur ; de même pour CATEG_INS et QUAL_CODE_INS.
-- Tableaux : T62.
-- Lecture  : un code sur 2 caractères ferait penser à la catégorie (01 à 06) ;
--            une lettre, à l'échelle (A, B, C) ; un nombre, à un échelon.
-- Résultat (02/11/2026) : CLA_AVANT VARCHAR2(8) ; CLA_APRES VARCHAR2(8) ;
--            CATEG_INS VARCHAR2(3) ; QUAL_CODE_INS VARCHAR2(12) ; tous
--            facultatifs. Le classement tient en 8 caractères (1C04BB12) :
--            c'est un code composé, pas un nombre.
-- -----------------------------------------------------------------------------
SELECT column_name, data_type, data_length, data_precision, nullable
FROM ALL_TAB_COLUMNS
WHERE owner = 'USER_GRH'
  AND table_name = 'GRH_INSCRIPTION'
  AND column_name IN ('CLA_AVANT', 'CLA_APRES', 'CATEG_INS', 'QUAL_CODE_INS')
ORDER BY column_id;


-- -----------------------------------------------------------------------------
-- Q119 — Les valeurs de CLA_AVANT et CLA_APRES
-- Objectif : toutes les inscriptions dont l'un des deux champs est rempli, par
--            année de début et type de formation (DIPLO, PERF, CONF), avec les
--            valeurs avant et après, et la catégorie et la qualification
--            notées sur l'inscription.
-- Tableaux : T62.
-- Lecture  : attendu, 2 lignes de 2024 en DIPLO. Comparer CLA_AVANT à
--            CATEG_INS et QUAL_CODE_INS : si c'est la même chose, le
--            classement est la catégorie de l'agent.
-- Résultat (02/11/2026) : une ligne par valeur, de 1979 à 2025, et 16 lignes
--            de formations sans date de début.
--            - CLA_AVANT est rempli sur toutes les lignes, pour tous les types
--              de formation (DIPLO, PERF, CONF, et RECY en 2004 et 2005, type
--              absent de la liste des catégories de formation). Il s'écrit 1 +
--              C + catégorie + échelle doublée + échelon : 1C04BB12 =
--              catégorie 04, échelle B, échelon 12. Hors catégorie : 1HC-DG08
--              (échelle DG), 1HC-SG14 (échelle SG). Le 1 du début est le même
--              partout ; son sens reste à demander.
--            - CATEG_INS reprend la catégorie de CLA_AVANT, sauf 6 inscriptions
--              de 2024 et 2025 (1C04BB05 noté 06 ou 05, 1C04BB06 noté 05,
--              1C03BB16 noté 04, 1C04BB07 noté 05).
--            - QUAL_CODE_INS suit la catégorie jusqu'en 2023 (quelques 05 ou 06
--              notés HC). En 2024 et 2025, il s'en écarte souvent : 05 noté
--              HC, 04 ou 03, 03 noté 02, 04 noté 03, 06 noté HC. C'est la
--              qualification (GRH_QUALIFICATION), pas la catégorie (lot 18).
--            - CLA_APRES n'est rempli que 10 fois en 45 ans, toujours pour une
--              formation diplômante : 1985 1C04BB03 devient 1C05BB01 (2) ; 1991
--              1C05AA02 devient 1C06BB02 (2) ; 1993 1C05AA03 devient 1C06BB01,
--              et 1C06BB15 devient 1C06BB01 ; 1999 1C05BB04 devient « . » ;
--              2013 1C04BB15 devient 1C05BB10 ; 2023 1C04AA11 et 1C04BB10
--              deviennent « 5B ». Sauf deux cas (« . » et 06 resté 06), c'est
--              une montée de catégorie : 04 à 05 ou 05 à 06. Les deux de 2023
--              finissent en 2024 : ce sont les 2 promotions de 2024 des lots 9
--              et 17.
-- Constat  : - CLA_AVANT est le classement de l'agent dans la grille au moment
--              de l'inscription : catégorie, échelle, échelon ;
--            - CLA_APRES ne peut pas compter les promotions de T62 : il n'est
--              presque jamais saisi (10 fois en 45 ans, 2 fois depuis 2019) ;
--            - la promotion peut se lire autrement : comparer la catégorie de
--              l'agent au début de la formation et dans l'année qui suit sa
--              fin (Q120, réécrite en comptages), ou chercher une décision
--              d'avancement (60) ou de reclassement (61) ;
--            - une promotion est une montée de catégorie, pas d'échelon
--              (l'échelon monte avec l'ancienneté) : à confirmer par la DRH.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM f.DATE_DEBUT) AS annee_debut,
       f.CATEGORIE                     AS type_formation,
       TO_CHAR(i.CLA_AVANT)            AS cla_avant,
       TO_CHAR(i.CLA_APRES)            AS cla_apres,
       TO_CHAR(i.CATEG_INS)            AS categ_ins,
       TO_CHAR(i.QUAL_CODE_INS)        AS qual_code_ins,
       COUNT(*)                        AS inscriptions
FROM USER_GRH.GRH_INSCRIPTION i
JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
WHERE i.CLA_AVANT IS NOT NULL OR i.CLA_APRES IS NOT NULL
GROUP BY EXTRACT(YEAR FROM f.DATE_DEBUT), f.CATEGORIE, TO_CHAR(i.CLA_AVANT),
         TO_CHAR(i.CLA_APRES), TO_CHAR(i.CATEG_INS), TO_CHAR(i.QUAL_CODE_INS)
ORDER BY 1, 2, 3, 4;


-- -----------------------------------------------------------------------------
-- Q120 — La promotion lue dans la situation de l'agent, de 2019 à 2025
-- Objectif : Q119 montre que CLA_AVANT est rempli partout et CLA_APRES presque
--            jamais. Pour chaque année de début et type de formation, compter
--            les inscriptions dont l'agent monte de catégorie dans les 12 mois
--            qui suivent la fin de la formation : catégorie de la dernière
--            situation commencée au plus tard au début de la formation, face à
--            celle de la dernière situation commencée au plus tard 12 mois
--            après sa fin. Compter aussi les changements d'échelle, les
--            décisions d'avancement (60) ou de reclassement (61) dans la même
--            période, et vérifier que la catégorie de CLA_AVANT est celle de
--            la situation au début.
-- Tableaux : T62.
-- Lecture  : comparer à l'annuaire de la CNSS 2024 (tableau 123) : 6 formés
--            avec promotion en 2024. CLA_APRES n'en donne que 2 (formations
--            commencées en 2023). Les catégories se comparent comme du texte :
--            01 < 02 < … < 06 < HC.
-- Résultat (02/11/2026) : année · type · inscriptions · agents · CLA_AVANT
--            comme la situation · catégorie montée dans les 12 mois
--            (inscriptions · agents) · échelle changée · décision 60 ou 61 ·
--            CLA_APRES rempli
--            2019 PERF  528 · 454 · 527 ·  1 ·  1 ·  3 · 0 · 0
--            2020 PERF  435 · 350 · 434 ·  0 ·  0 ·  0 · 0 · 0
--            2021 DIPLO   2 ·   2 ·   2 ·  0 ·  0 ·  0 · 0 · 0
--            2021 PERF  554 · 492 · 553 ·  0 ·  0 ·  0 · 0 · 0
--            2022 PERF  886 · 524 · 882 ·  0 ·  0 ·  0 · 0 · 0
--            2023 DIPLO   2 ·   2 ·   2 ·  2 ·  2 ·  1 · 0 · 2
--            2023 PERF  735 · 548 · 734 · 35 · 30 · 23 · 0 · 0
--            2024 CONF    1 ·   1 ·   1 ·  0 ·  0 ·  0 · 0 · 0
--            2024 PERF  522 · 407 · 521 · 23 · 22 · 17 · 0 · 0
--            2025 PERF  691 · 608 · 689 · 50 · 36 · 37 · 0 · 0
-- Constat  : - CLA_AVANT donne bien la catégorie de la situation de l'agent au
--              début de la formation : 4 345 inscriptions sur 4 356 ;
--            - aucune décision d'avancement (60) ou de reclassement (61) dans
--              l'année qui suit une formation : ces décisions ne servent pas à
--              saisir une promotion après une formation ;
--            - aucune montée de catégorie après les formations de 2019 à 2022
--              (une seule), puis 30 agents (2023), 22 (2024) et 36 (2025), soit
--              5 à 6 % des formés chaque année, presque tous en
--              perfectionnement. Une formation de perfectionnement ne donne
--              pas de promotion : ces montées viennent sans doute de la
--              carrière (avancements de catégorie repris depuis 2023 ?), pas de
--              la formation. À comparer avec les agents non formés ;
--            - seules les 2 formations diplômantes commencées en 2023 et finies
--              en 2024 lient une promotion à une formation (CLA_APRES « 5B »,
--              catégorie montée) ;
--            - 2024 : 2 promotions liées à une formation, 22 agents formés en
--              perfectionnement qui montent de catégorie dans l'année ;
--              l'annuaire de la CNSS en publie 6. Ni l'une ni l'autre lecture
--              ne retombe ; les 6 viennent sans doute d'un registre de la DRH.
--              Proposition BI : « avec promotion » = formation diplômante dont
--              le classement après est saisi et plus haut. Question à la DRH,
--              non bloquante.
-- -----------------------------------------------------------------------------
WITH ins AS (
    SELECT i.PERS_ID, i.FORM_NO,
           EXTRACT(YEAR FROM f.DATE_DEBUT)  AS annee_debut,
           f.CATEGORIE                      AS type_formation,
           f.DATE_DEBUT                     AS debut_formation,
           NVL(f.DATE_FIN, f.DATE_DEBUT)    AS fin_formation,
           -- la catégorie lue dans le classement : 1C04BB12 donne 04, 1HC-DG08 HC
           CASE WHEN SUBSTR(TO_CHAR(i.CLA_AVANT), 2, 1) = 'C'
                THEN SUBSTR(TO_CHAR(i.CLA_AVANT), 3, 2)
                ELSE SUBSTR(TO_CHAR(i.CLA_AVANT), 2, 2) END AS cat_cla,
           CASE WHEN TRIM(TO_CHAR(i.CLA_APRES)) IS NOT NULL
                 AND TRIM(TO_CHAR(i.CLA_APRES)) <> '.' THEN 1 ELSE 0 END AS cla_apres_rempli
    FROM USER_GRH.GRH_INSCRIPTION i
    JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
    WHERE f.DATE_DEBUT >= DATE '2019-01-01'
),
-- la situation de l'agent au début de la formation
avant AS (
    SELECT n.PERS_ID, n.FORM_NO, s.CATEGORIE, s.ECHELLE,
           ROW_NUMBER() OVER (PARTITION BY n.PERS_ID, n.FORM_NO
                              ORDER BY s.DATE_DEBUT DESC) AS rn
    FROM ins n
    JOIN USER_GRH.GRH_SITUATION s ON s.PERS_ID = n.PERS_ID
                                 AND s.DATE_DEBUT <= n.debut_formation
),
-- sa situation 12 mois après la fin de la formation
apres AS (
    SELECT n.PERS_ID, n.FORM_NO, s.CATEGORIE, s.ECHELLE,
           ROW_NUMBER() OVER (PARTITION BY n.PERS_ID, n.FORM_NO
                              ORDER BY s.DATE_DEBUT DESC) AS rn
    FROM ins n
    JOIN USER_GRH.GRH_SITUATION s ON s.PERS_ID = n.PERS_ID
                                 AND s.DATE_DEBUT <= ADD_MONTHS(n.fin_formation, 12)
),
-- une ligne par inscription, avec ses indicateurs
lignes AS (
    SELECT n.annee_debut, n.type_formation, n.PERS_ID, n.cla_apres_rempli,
           CASE WHEN n.cat_cla = a.CATEGORIE THEN 1 ELSE 0 END      AS cla_comme_situation,
           CASE WHEN p.CATEGORIE > a.CATEGORIE THEN 1 ELSE 0 END    AS categorie_montee,
           CASE WHEN p.ECHELLE <> a.ECHELLE THEN 1 ELSE 0 END       AS echelle_changee,
           CASE WHEN EXISTS (
                    SELECT 1
                    FROM USER_GRH.GRH_DECISION d
                    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
                    WHERE d.CDEC_CODE IN ('60', '61')
                      AND NVL(d.PERS_ID, dp.PERS_ID) = n.PERS_ID
                      AND d.DATE_VIGUEUR BETWEEN n.debut_formation
                                             AND ADD_MONTHS(n.fin_formation, 12))
                THEN 1 ELSE 0 END                                    AS decision_60_61
    FROM ins n
    LEFT JOIN avant a ON a.PERS_ID = n.PERS_ID AND a.FORM_NO = n.FORM_NO AND a.rn = 1
    LEFT JOIN apres p ON p.PERS_ID = n.PERS_ID AND p.FORM_NO = n.FORM_NO AND p.rn = 1
)
SELECT annee_debut, type_formation,
       COUNT(*)                                                AS inscriptions,
       COUNT(DISTINCT PERS_ID)                                 AS agents,
       SUM(cla_comme_situation)                                AS cla_comme_situation,
       SUM(categorie_montee)                                   AS categorie_montee_12_mois,
       COUNT(DISTINCT CASE WHEN categorie_montee = 1 THEN PERS_ID END)
                                                               AS agents_montes_12_mois,
       SUM(echelle_changee)                                    AS echelle_changee_12_mois,
       SUM(decision_60_61)                                     AS decision_60_61_12_mois,
       SUM(cla_apres_rempli)                                   AS cla_apres_rempli
FROM lignes
GROUP BY annee_debut, type_formation
ORDER BY 1, 2;
