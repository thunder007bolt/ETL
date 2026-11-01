-- =============================================================================
-- LOT 18 — Expliquer les écarts avec l'annuaire de la CNSS 2024
-- Rédigé le 01/11/2026 — À EXÉCUTER sur USER_GRH.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement,
-- aucune donnée nominative (dates et sexe ne servent qu'à compter ou à
-- rapprocher deux fiches, jamais affichés).
-- Pourquoi ce lot : le lot 17 retrouve l'effectif permanent au 31/12/2024 (742
-- pour 758 publiés), les âges et l'ancienneté de l'annuaire. Quatre écarts
-- restent à expliquer avant d'écrire les règles GRH :
--   1. la catégorie : la CNSS ne classe pas ses agents par le niveau de la
--      qualification (cadres moyens 209 pour 311, cadres supérieurs 113 pour
--      62) : Q103 et Q104 cherchent son champ ;
--   2. les années passées : de 2020 à 2023, il manque chaque année de 92 à 99
--      permanents, sans doute les 84 engagés de 2019 radiés sans date (lot 13,
--      Q88) : Q105 ;
--   3. les 16 permanents qui manquent en 2024, tous de 35 ans ou plus, et les
--      46 contractuels publiés pour 7 temporaires : Q106, et Q107 qui teste la
--      date d'état de la fiche (DATE_ETAT_EMPLOYE, jamais lue jusqu'ici) pour
--      dater les départs ;
--   4. les formés de 2024 : 480 publiés, 410 agents et 525 inscriptions dans
--      la base : Q108 ;
--   5. les mouvements (T60) : Q99, relancée, retrouve les recrutements, les
--      retraites et les démissions ; Q109 cherche les décès, les
--      disponibilités et les remises en activité de l'annuaire.
-- Règles reprises du lot 17 : présent au 31/12 = entré au plus tard le 31/12
-- et encore ACTIF ou sorti après le 31/12 ; sortie = première décision 02,
-- 34, 35, 36, 52 ou 54 non annulée par une 40, à défaut le décès ; l'agent est
-- décrit par sa dernière situation commencée au 31/12 ; permanent = qualité A
-- et nature PERMANENT.
-- Numérotation : suite du lot 17, Q103 à Q109.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q103 — Les permanents au 31/12/2024 selon chaque champ de classement
-- Objectif : pour les 742 permanents du lot 17, répartir les valeurs de chaque
--            champ de classement de la situation (qualification, catégorie,
--            échelle, cadre, catégories professionnelles) entre les quatre
--            niveaux de la qualification.
-- Tableaux : T58, T62, T65 (catégorie).
-- Lecture  : chercher le champ dont les valeurs se regroupent en 177
--            employés, 208 agents de maîtrise, 311 cadres moyens et 62 cadres
--            supérieurs (annuaire de la CNSS 2024, tableau 119), à 16 agents
--            près. Une piste : la qualification HC (hors catégorie, rangée en
--            cadre supérieur) comptée en cadre moyen par la CNSS.
-- Résultat (01/11/2026) : champ · valeur · employés · maîtrise · cadres
--            moyens · cadres supérieurs (niveau de la qualification) · total
--            QUAL_CODE : HC 0 · 0 · 0 · 52 · 52 ; 01 18 · 0 · 0 · 0 · 18 ;
--              02 186 · 0 · 0 · 0 · 186 ; 03 0 · 216 · 0 · 0 · 216 ;
--              04 0 · 0 · 209 · 0 · 209 ; 05 0 · 0 · 0 · 34 · 34 ;
--              06 0 · 0 · 0 · 27 · 27.
--            CATEGORIE : 01 20 · 0 · 0 · 0 · 20 ; 02 148 · 4 · 1 · 0 · 153 ;
--              03 29 · 173 · 4 · 1 · 207 ; 04 3 · 29 · 187 · 1 · 220 ;
--              05 3 · 6 · 15 · 60 · 84 ; 06 1 · 4 · 2 · 51 · 58.
--            ECHELLE : A 343 ; B 376 ; C 23.  CADRE : vide pour les 742.
--            CODE_CATPROF_STRUCT : A 131 ; I 158 ; M 149 ; S 298 ; vide 6.
--            CODE_CATPROF_UNIV : E 131 ; M 307 ; C 298 ; vide 6.
-- Constat  : - la CNSS classe ses agents par la catégorie de la situation
--              (GRH_SITUATION.CATEGORIE, la catégorie de la grille de
--              salaire), regroupée ainsi : 01 et 02 = employés (173 pour 177
--              publiés), 03 = agents de maîtrise (207 pour 208), 04 et 05 =
--              cadres moyens (304 pour 311), 06 = cadres supérieurs (58 pour
--              62). L'écart total (16) est celui de l'effectif ;
--            - cadres (04, 05, 06) : 362 sur 742, soit 48,8 % (49 % publié) ;
--            - la qualification ne donne pas la catégorie : 60 agents de
--              catégorie 05 sont qualifiés « cadres supérieurs », 29 agents de
--              catégorie 03 sont qualifiés « employés » ;
--            - l'atelier de Kaya (2017) rangeait 5 et 6 en cadres supérieurs ;
--              l'annuaire de 2024 range 5 en cadres moyens. Question à la DRH,
--              non bloquante (un paramètre : la catégorie 05) ;
--            - ECHELLE, CADRE et les deux catégories professionnelles ne
--              retombent pas sur l'annuaire.
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
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
permanents AS (
    SELECT NVL(q.NIVEAU, '-')                                     AS niveau,
           NVL(TO_CHAR(si.QUAL_CODE), '-')                        AS qual_code,
           NVL(TO_CHAR(si.CATEGORIE), '-')                        AS categorie,
           NVL(TO_CHAR(si.ECHELLE), '-')                          AS echelle,
           NVL(TO_CHAR(si.CADRE), '-')                            AS cadre,
           NVL(TO_CHAR(si.CODE_CATPROF_STRUCT), '-')              AS catprof_struct,
           NVL(TO_CHAR(si.CODE_CATPROF_UNIV), '-')                AS catprof_univ
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = si.QUAL_CODE
    WHERE COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND (p.ETAT_EMPLOYE = 'ACTIF' OR so.dt_sortie > DATE '2024-12-31')
      AND TO_CHAR(p.QUALITE) = 'A'
      AND si.NATURE_EMPLOIE = 'PERMANENT'
),
valeurs AS (
    SELECT '1. QUAL_CODE' AS champ, qual_code AS valeur, niveau FROM permanents
    UNION ALL
    SELECT '2. CATEGORIE', categorie, niveau FROM permanents
    UNION ALL
    SELECT '3. ECHELLE', echelle, niveau FROM permanents
    UNION ALL
    SELECT '4. CADRE', cadre, niveau FROM permanents
    UNION ALL
    SELECT '5. CODE_CATPROF_STRUCT', catprof_struct, niveau FROM permanents
    UNION ALL
    SELECT '6. CODE_CATPROF_UNIV', catprof_univ, niveau FROM permanents
)
SELECT champ, valeur,
       SUM(CASE WHEN niveau = 'A' THEN 1 ELSE 0 END)              AS employes,
       SUM(CASE WHEN niveau = 'I' THEN 1 ELSE 0 END)              AS agents_maitrise,
       SUM(CASE WHEN niveau = 'M' THEN 1 ELSE 0 END)              AS cadres_moyens,
       SUM(CASE WHEN niveau = 'S' THEN 1 ELSE 0 END)              AS cadres_superieurs,
       COUNT(*)                                                   AS permanents
FROM valeurs
GROUP BY champ, valeur
ORDER BY champ, valeur;


-- -----------------------------------------------------------------------------
-- Q104 — Les tables et listes de codes de classement
-- Objectif : trouver le libellé des valeurs de Q103 : tables de USER_GRH dont
--            le nom parle de catégorie, classe, échelle, échelon, grille, cadre
--            ou corps, et listes de codes de GRH_REF_CODES du même genre.
-- Tableaux : T58, T62, T65 (catégorie).
-- Lecture  : une table de catégories professionnelles avec un libellé
--            « cadre moyen », « cadre supérieur »… donnerait la classification
--            de la CNSS.
-- Résultat (01/11/2026) : listes de codes de GRH_REF_CODES : CATEGORIE
--            (17 codes), CATEGORIE_SUPERIEUR (4), QUALIFICATION (4), ECHELLE
--            (6), ECHELON (27), RECLASSEMENT (3), CATEGORIE_FORMATION (2)…
--            Tables et vues : V_GRH_CATEGORIE (CODE_CATEGORIE, LIB_CATEGORIE,
--            RANG_CATEGORIE), V_GRH_CATEGORIE_PROF_STRUCT et _UNIV, V_GRH_ECHELLE,
--            V_GRH_ECHELON, V_CLASSEMENT, V_GRH_TABGRILLE, V_STAT_RMSCAT
--            (ANNEE, CATEGORIE, MONTANT, NOM), GRH_TYPE_ENCADREMENT.
-- Constat  : - les libellés des catégories sont dans V_GRH_CATEGORIE et dans
--              la liste CATEGORIE : à lire au lot 19 (Q110) ;
--            - V_STAT_RMSCAT (montant par année et catégorie) est une piste
--              pour la masse salariale de T64, à regarder plus tard ;
--            - V_IDENTIFICATION contient des données nominatives : à ne
--              jamais lire.
-- -----------------------------------------------------------------------------
SELECT 'Table'          AS source,
       TABLE_NAME       AS nom,
       COLUMN_NAME      AS detail
FROM ALL_TAB_COLUMNS
WHERE OWNER = 'USER_GRH'
  AND TABLE_NAME NOT LIKE 'BIN$%'
  AND (TABLE_NAME LIKE '%CAT%'    OR TABLE_NAME LIKE '%CLASS%'
       OR TABLE_NAME LIKE '%ECHEL%' OR TABLE_NAME LIKE '%GRILLE%'
       OR TABLE_NAME LIKE '%CADRE%' OR TABLE_NAME LIKE '%CORPS%')
UNION ALL
SELECT 'Liste de codes',
       RV_DOMAIN,
       COUNT(*) || ' codes'
FROM USER_GRH.GRH_REF_CODES
WHERE RV_DOMAIN LIKE '%CAT%'    OR RV_DOMAIN LIKE '%CLASS%'
   OR RV_DOMAIN LIKE '%ECHEL%'  OR RV_DOMAIN LIKE '%CADRE%'
   OR RV_DOMAIN LIKE '%NIVEAU%'
GROUP BY RV_DOMAIN
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- Q105 — Les 84 permanents engagés en 2019 (décision 51)
-- Objectif : décrire les agents engagés comme permanents en 2019 : engagement
--            annulé ou non par une décision 40, état de la fiche, nature de la
--            dernière situation, année d'entrée, année de la date d'état
--            (DATE_ETAT_EMPLOYE), sortie datée ou non, nouvel engagement 51 en
--            2024, et fiche d'une recrue de 2024 née le même jour et du même
--            sexe (même personne sous une nouvelle fiche ?).
-- Tableaux : T60, T58 à T65 pour les années passées.
-- Lecture  : l'annuaire compte 84 « autres entrées » en 2019 et 84 « autres
--            sorties » en 2024 (tableau 121), et de 2020 à 2023 il compte de 92
--            à 99 permanents de plus que la base (lot 17, Q102). Si les 84 sont
--            inactifs, sans sortie datée, avec une date d'état en 2024, ce sont
--            les mêmes : la date d'état donnerait leur date de sortie. Q99
--            (lot 17) l'appuie déjà : en 2019, 84 recrutés permanents, et 84
--            agents de moins que prévu en fin d'année.
-- Résultat (01/11/2026) : annulé · état · nature · entrée · date d'état ·
--            sortie · 51 en 2024 · recrue 2024 née le même jour · agents
--            non · INACTIF · PERMANENT · 2019 · 2018 · sans date · non · non · 82
--            non · INACTIF · PERMANENT · 2019 · 2018 · sans date · non · oui ·  2
-- Constat  : - ce sont bien 84 agents, tous du même lot : engagement de 2019
--              non annulé, fiche inactive, sans aucune sortie datée, date d'état
--              en 2018, aucun nouvel engagement en 2024 ;
--            - 2 seulement ont une recrue de 2024 née le même jour et du même
--              sexe : les 104 recrues de 2024 ne sont pas ces 84 agents ;
--            - la base ne dit ni quand ni pourquoi ils sont partis ; l'annuaire
--              les sort en 2024 (« autres sorties »). Question à la DRH, non
--              bloquante : elle ne touche que les années passées (2019 à
--              2023), pas les exercices calculés à partir de 2025.
-- -----------------------------------------------------------------------------
WITH engagements AS (
    -- toutes les décisions 51, comme au lot 13 (Q88), avec leur annulation
    -- éventuelle par une décision 40
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR,
           CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                             WHERE a.CDEC_CODE = '40'
                               AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
                THEN 1 ELSE 0 END     AS annulee
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE = '51'
),
eng_2019 AS (
    SELECT PERS_ID, MIN(annulee) AS annulee
    FROM engagements
    WHERE PERS_ID IS NOT NULL
      AND DATE_VIGUEUR >= DATE '2019-01-01' AND DATE_VIGUEUR < DATE '2020-01-01'
    GROUP BY PERS_ID
),
eng_2024 AS (
    SELECT DISTINCT PERS_ID FROM engagements
    WHERE PERS_ID IS NOT NULL AND annulee = 0
      AND DATE_VIGUEUR >= DATE '2024-01-01' AND DATE_VIGUEUR < DATE '2025-01-01'
),
sorties AS (
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
derniere AS (
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
),
-- les fiches des agents CNSS entrés en 2024
recrues_2024 AS (
    SELECT PERS_ID, DATE_NAISS, SEXE
    FROM USER_GRH.GRH_PERSONNE
    WHERE TO_CHAR(QUALITE) = 'A'
      AND COALESCE(DATE_EMBAUCHE, DATE_PRISE_SERVICE) >= DATE '2024-01-01'
      AND COALESCE(DATE_EMBAUCHE, DATE_PRISE_SERVICE) <  DATE '2025-01-01'
),
-- une ligne par engagé de 2019 (Oracle refuse un EXISTS dans le GROUP BY)
engages AS (
    SELECT CASE e.annulee WHEN 1 THEN 'oui' ELSE 'non' END        AS engagement_annule,
           NVL(p.ETAT_EMPLOYE, '-')                               AS etat_fiche,
           NVL(TO_CHAR(de.NATURE_EMPLOIE), '-')                   AS nature_derniere,
           NVL(TO_CHAR(EXTRACT(YEAR FROM COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE))), '-')
                                                                  AS annee_entree,
           NVL(TO_CHAR(EXTRACT(YEAR FROM p.DATE_ETAT_EMPLOYE)), '-') AS annee_date_etat,
           NVL(TO_CHAR(EXTRACT(YEAR FROM so.dt_sortie)), 'sans date') AS annee_sortie,
           CASE WHEN e24.PERS_ID IS NOT NULL THEN 'oui' ELSE 'non' END AS engage_51_en_2024,
           CASE WHEN EXISTS (SELECT 1 FROM recrues_2024 r
                             WHERE r.PERS_ID   <> p.PERS_ID
                               AND r.DATE_NAISS = p.DATE_NAISS
                               AND r.SEXE       = p.SEXE)
                THEN 'oui' ELSE 'non' END                         AS recrue_2024_meme_naiss
    FROM eng_2019 e
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = e.PERS_ID
    LEFT JOIN sorties  so  ON so.PERS_ID  = e.PERS_ID
    LEFT JOIN derniere de  ON de.PERS_ID  = e.PERS_ID AND de.rn = 1
    LEFT JOIN eng_2024 e24 ON e24.PERS_ID = e.PERS_ID
)
SELECT engagement_annule, etat_fiche, nature_derniere, annee_entree,
       annee_date_etat, annee_sortie, engage_51_en_2024, recrue_2024_meme_naiss,
       COUNT(*)                                                   AS agents
FROM engages
GROUP BY engagement_annule, etat_fiche, nature_derniere, annee_entree,
         annee_date_etat, annee_sortie, engage_51_en_2024, recrue_2024_meme_naiss
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8;


-- -----------------------------------------------------------------------------
-- Q106 — Les agents CNSS entrés avant 2025 que le lot 17 ne compte pas
-- Objectif : agents CNSS (qualité A) entrés au plus tard le 31/12/2024 mais
--            non comptés présents au 31/12/2024 (fiche non ACTIVE, et sortie
--            datée au plus tard le 31/12/2024 ou pas datée), par nature
--            d'emploi au 31/12/2024, état de la fiche, année de la date d'état
--            et date de sortie ; avec ceux de 55 ans et plus au 31/12/2024.
-- Tableaux : T58 à T61, T65 (périmètre de l'effectif).
-- Lecture  : - permanents dont la date d'état est en 2025 ou 2026, ou sortis
--              en décembre 2024 : candidats pour les 16 permanents manquants
--              (lot 17, Q98 : tous de 35 ans ou plus, 7 de 55 à 59 ans) ;
--            - temporaires dont la date d'état est en 2025 ou 2026 : candidats
--              pour les 46 contractuels publiés (7 temporaires actifs
--              seulement dans la base).
-- Résultat (01/11/2026) : nature · état · date d'état · sortie · agents ·
--            dont 55 ans et plus au 31/12/2024
--            sans situation : vide · - · sans date 3 · 2 ; INACTIF · avant 2023 ·
--              avant 2024 8 · 7 ; INACTIF · avant 2023 · sans date 103 · 77
--            PERMANENT : vide · avant 2023 · sans date 5 · 5
--              INACTIF · - · avant 2024 1 · 1 ; janv.-nov. 2024 2 · 0 ;
--              sans date 10 · 8
--              INACTIF · avant 2023 · avant 2024 1 000 · 955 ; déc. 2024 10 · 9 ;
--              janv.-nov. 2024 21 · 17 ; sans date 212 · 103
--            TEMPORAIRE : vide · - · avant 2024 3 · 1 ; sans date 44 · 13
--              INACTIF · - · avant 2024 3 · 0 ; déc. 2024 3 · 0 ;
--              janv.-nov. 2024 4 · 0 ; sans date 52 · 1
--              INACTIF · avant 2023 · avant 2024 317 · 79 ; janv.-nov. 2024 1 · 1 ;
--              sans date 361 · 130
-- Constat  : - aucune date d'état après 2022 (Q107 : la dernière est de 2020) :
--              elle ne sert pas à repérer les départs récents ;
--            - candidats pour les 16 permanents manquants : 10 sortis en
--              décembre 2024 (9 de 55 ans et plus), et 15 sans sortie datée
--              (10 inactifs sans aucune date, 5 à l'état vide). L'âge (aucun
--              de 60 ans et plus dans l'annuaire) permettra de trier : lot 19
--              (Q115) ;
--            - temporaires : 8 sortis en 2024 et 96 sans date ni date d'état
--              (44 + 52). Les 46 contractuels publiés sont peut-être les
--              temporaires sous contrat (décision 50) au 31/12/2024 : lot 19
--              (Q114).
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
    SELECT s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
non_comptes AS (
    SELECT NVL(TO_CHAR(si.NATURE_EMPLOIE), '-')                   AS nature,
           NVL(p.ETAT_EMPLOYE, '-')                               AS etat_fiche,
           CASE WHEN p.DATE_ETAT_EMPLOYE IS NULL THEN '-'
                WHEN p.DATE_ETAT_EMPLOYE < DATE '2023-01-01' THEN 'avant 2023'
                ELSE TO_CHAR(EXTRACT(YEAR FROM p.DATE_ETAT_EMPLOYE)) END AS annee_date_etat,
           CASE WHEN so.dt_sortie IS NULL THEN 'sans date'
                WHEN so.dt_sortie < DATE '2024-01-01' THEN 'avant 2024'
                WHEN so.dt_sortie < DATE '2024-12-01' THEN 'janv.-nov. 2024'
                ELSE 'déc. 2024' END                              AS sortie,
           CASE WHEN FLOOR((DATE '2024-12-31' - p.DATE_NAISS) / 365.25) >= 55
                THEN 1 ELSE 0 END                                 AS age_55_plus
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
    LEFT JOIN situ si ON si.PERS_ID = p.PERS_ID AND si.rn = 1
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) <= DATE '2024-12-31'
      AND NVL(p.ETAT_EMPLOYE, '-') <> 'ACTIF'
      AND (so.dt_sortie IS NULL OR so.dt_sortie <= DATE '2024-12-31')
)
SELECT nature, etat_fiche, annee_date_etat, sortie,
       COUNT(*)                                                   AS agents,
       SUM(age_55_plus)                                           AS dont_55_ans_et_plus
FROM non_comptes
GROUP BY nature, etat_fiche, annee_date_etat, sortie
ORDER BY nature, etat_fiche, annee_date_etat, sortie;


-- -----------------------------------------------------------------------------
-- Q107 — La date d'état de la fiche peut-elle dater les départs ?
-- Objectif : pour chaque année de la date d'état (DATE_ETAT_EMPLOYE), compter
--            les fiches actives, les fiches inactives avec une sortie datée
--            (et parmi elles celles dont la date d'état tombe la même année,
--            ou à 31 jours près de la sortie), et les fiches inactives sans
--            sortie datée.
-- Tableaux : T60 (sorties), T58 à T65 pour les années passées.
-- Lecture  : si, pour les sorties datées, la date d'état tombe presque
--            toujours à moins d'un mois de la sortie, elle peut dater les 817
--            départs sans date (lot 7, Q44) et rendre l'effectif des années
--            passées reconstituable.
-- Résultat (01/11/2026) : 3 180 fiches. Date d'état de 1979 à 2020, jamais
--            après ; vide pour 437 fiches (258 actives, 18 inactives avec
--            sortie, 161 inactives sans sortie).
--            Fiches actives : 879 (621 avec une date d'état).
--            Inactives avec sortie datée : 1 419, dont 537 avec la date d'état
--            la même année que la sortie (38 %), et 390 à 31 jours près (27 %).
--            Inactives sans sortie datée : 882, dont 301 avec une date d'état
--            en 2003, 208 en 2019 et 84 en 2018.
-- Constat  : - la date d'état ne date pas les départs : elle ne tombe à moins
--              d'un mois de la sortie qu'une fois sur quatre, et elle n'est
--              plus remplie depuis 2020 ;
--            - les 84 dates d'état de 2018 sont les 84 engagés de 2019 (Q105) ;
--              les 301 de 2003 ressemblent à une reprise de données ;
--            - les 817 départs sans date (lot 7, Q44) le restent : l'effectif
--              des années passées ne se reconstitue pas mieux.
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
fiches AS (
    SELECT NVL(TO_CHAR(EXTRACT(YEAR FROM p.DATE_ETAT_EMPLOYE)), '-') AS annee_date_etat,
           CASE WHEN p.ETAT_EMPLOYE = 'ACTIF' THEN 1 ELSE 0 END    AS actif,
           so.dt_sortie,
           p.DATE_ETAT_EMPLOYE                                     AS date_etat
    FROM USER_GRH.GRH_PERSONNE p
    LEFT JOIN sorties so ON so.PERS_ID = p.PERS_ID
)
SELECT annee_date_etat,
       SUM(actif)                                                 AS actives,
       SUM(CASE WHEN actif = 0 AND dt_sortie IS NOT NULL THEN 1 ELSE 0 END) AS inactives_sortie_datee,
       SUM(CASE WHEN actif = 0 AND dt_sortie IS NOT NULL
                 AND EXTRACT(YEAR FROM dt_sortie) = EXTRACT(YEAR FROM date_etat)
                THEN 1 ELSE 0 END)                                AS dont_meme_annee,
       SUM(CASE WHEN actif = 0 AND dt_sortie IS NOT NULL
                 AND ABS(date_etat - dt_sortie) <= 31
                THEN 1 ELSE 0 END)                                AS dont_a_31_jours,
       SUM(CASE WHEN actif = 0 AND dt_sortie IS NULL THEN 1 ELSE 0 END) AS inactives_sans_sortie,
       COUNT(*)                                                   AS fiches
FROM fiches
GROUP BY annee_date_etat
ORDER BY annee_date_etat;


-- -----------------------------------------------------------------------------
-- Q108 — Les formés de 2024 : agents ou inscriptions ?
-- Objectif : pour les formations qui touchent 2024, par qualification et
--            niveau (situation au 31/12/2024) : inscriptions, couples agent et
--            formation distincts, agents distincts, les mêmes pour les seuls
--            permanents, formations distinctes et inscriptions dont le
--            classement change.
-- Tableaux : T62.
-- Lecture  : l'annuaire publie 480 permanents formés en 2024 (employés 132,
--            agents de maîtrise 142, cadres moyens 118, cadres supérieurs 88),
--            dont 6 avec promotion. Chercher la colonne qui retombe sur 480,
--            et le regroupement des qualifications qui retombe sur la
--            répartition (même piste que Q103).
-- Résultat (01/11/2026) : niveau · qualification · inscriptions · couples
--            agent-formation · agents · inscriptions de permanents · couples
--            de permanents · agents permanents · formations · classement
--            changé
--            A · 01 ·   6 ·   6 ·   6 ·   6 ·   6 ·   6 ·  3 · 0
--            A · 02 · 111 · 111 ·  93 · 111 · 111 ·  93 · 13 · 0
--            I · 03 · 107 · 107 ·  99 · 107 · 107 ·  99 · 16 · 0
--            M · 04 · 144 · 144 · 112 · 144 · 144 · 112 · 25 · 2
--            S · HC ·  67 ·  67 ·  48 ·  66 ·  66 ·  47 · 25 · 0
--            S · 05 ·  43 ·  43 ·  28 ·  43 ·  43 ·  28 · 17 · 0
--            S · 06 ·  47 ·  47 ·  24 ·  47 ·  47 ·  24 · 22 · 0
--            Total : 525 inscriptions (524 de permanents), 410 agents (409).
-- Constat  : - aucun doublon d'inscription (une inscription par agent et par
--              formation) ;
--            - 480 publiés : ni les agents (409), ni les inscriptions (524) des
--              permanents. Le regroupement par la catégorie de la situation
--              (Q103) peut changer la répartition : lot 19 (Q113) ;
--            - 2 classements changés pour 6 « avec promotion » publiés.
-- -----------------------------------------------------------------------------
WITH situ AS (
    SELECT s.PERS_ID, s.QUAL_CODE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= DATE '2024-12-31'
),
inscrits AS (
    SELECT i.PERS_ID, i.FORM_NO,
           NVL(TO_CHAR(si.QUAL_CODE), '-')                        AS qual_code,
           NVL(q.NIVEAU, '-')                                     AS niveau,
           CASE WHEN TO_CHAR(p.QUALITE) = 'A' AND si.NATURE_EMPLOIE = 'PERMANENT'
                THEN 1 ELSE 0 END                                 AS permanent,
           CASE WHEN i.CLA_APRES IS NOT NULL AND i.CLA_AVANT IS NOT NULL
                 AND TO_CHAR(i.CLA_APRES) <> TO_CHAR(i.CLA_AVANT)
                THEN 1 ELSE 0 END                                 AS classement_change
    FROM USER_GRH.GRH_INSCRIPTION i
    JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
    LEFT JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = i.PERS_ID
    LEFT JOIN situ si ON si.PERS_ID = i.PERS_ID AND si.rn = 1
    LEFT JOIN USER_GRH.GRH_QUALIFICATION q ON q.QUAL_CODE = si.QUAL_CODE
    WHERE f.DATE_DEBUT < DATE '2025-01-01'
      AND COALESCE(f.DATE_FIN, f.DATE_DEBUT) >= DATE '2024-01-01'
)
SELECT niveau, qual_code,
       COUNT(*)                                                   AS inscriptions,
       COUNT(DISTINCT PERS_ID || '-' || FORM_NO)                  AS agent_formation,
       COUNT(DISTINCT PERS_ID)                                    AS agents,
       SUM(permanent)                                             AS inscriptions_permanents,
       COUNT(DISTINCT CASE WHEN permanent = 1
                           THEN PERS_ID || '-' || FORM_NO END)    AS agent_formation_perm,
       COUNT(DISTINCT CASE WHEN permanent = 1 THEN PERS_ID END)   AS agents_permanents,
       COUNT(DISTINCT FORM_NO)                                    AS formations,
       SUM(classement_change)                                     AS classement_change
FROM inscrits
GROUP BY niveau, qual_code
ORDER BY niveau, qual_code;


-- -----------------------------------------------------------------------------
-- Q109 — Les mouvements des permanents ligne à ligne, comme le tableau 121
-- Objectif : pour chaque année de 2019 à 2024, compter chez les permanents
--            (agents CNSS de nature PERMANENT au 31/12 de l'année, comme Q99)
--            la première sortie par code (décision 02, 34, 35, 36, 52, 54, ou
--            date de décès) ; et, d'une situation à la suivante, les passages
--            d'un statut actif à un statut d'absence (B, D, E, F ou H) et les
--            retours en activité, par code de statut.
-- Tableaux : T60.
-- Lecture  : Q99 (lot 17) retrouve les recrutements, les retraites et les
--            démissions de l'annuaire. Restent à lire :
--            - les décès publiés (4, 2, 3, 5, 3 de 2020 à 2024) : la base n'a
--              plus de date de décès depuis 2010, ils devraient être dans la
--              liquidation des droits (34) (lot 7, Q42) ;
--            - les disponibilités publiées en sorties (5, 3, 2, 4, 7) et les
--              remises en activité en entrées (3, 2, 3, 5, 6), les fins de
--              détachement (2 en 2023) et les détachements (2 en 2023, 1 en
--              2024) : quels codes de statut leur répondent ?
-- Résultat (01/11/2026) : mouvement · 2019 · 2020 · 2021 · 2022 · 2023 · 2024
--            Sortie : décision 02 (retraite)        39 · 39 · 31 · 29 · 30 · 25
--            Sortie : décision 34 (liquidation)      2 ·  3 ·  4 ·  4 ·  3 ·  5
--            Sortie : décision 35 (fin de contrat)   0 ·  0 ·  0 ·  0 ·  1 ·  0
--            Sortie : décision 36 (démission)        0 ·  0 ·  0 ·  0 ·  1 ·  2
--            Sortie : décision 54 (licenciement)     3 ·  0 ·  1 ·  2 ·  0 ·  1
--            Passage au statut E (disponibilité)     2 ·  0 ·  0 ·  0 ·  0 ·  0
--            Retour du statut E                      0 ·  0 ·  0 ·  0 ·  0 ·  1
--            Aucune sortie par date de décès ni par rupture de contrat (52),
--            aucun passage aux statuts B, D, F ou H.
-- Constat  : - décès publiés (4, 2, 3, 5, 3 de 2020 à 2024) : la liquidation
--              des droits (34) donne 3, 4, 4, 3, 5, soit 19 pour 17 sur cinq
--              ans, mais pas année par année : elle vient environ 8 mois après
--              le décès (lot 7, Q42). Décalée d'un an, elle retombe trois fois
--              sur quatre (4, 3, 5 pour 4, 3, 5) ;
--            - les disponibilités (5, 3, 2, 4, 7 publiées) et les remises en
--              activité (3, 2, 3, 5, 6) ne passent pas par le statut des
--              situations (2 passages et 1 retour en six ans). Le lot 3 (Q15)
--              a trouvé les décisions 07 (mise en disponibilité) et 18 (remise
--              en activité, 3 à 6 agents par an au lot 5) : lot 20 (Q117).
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2018 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
decisions AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID, d.DATE_VIGUEUR AS dt,
           d.CDEC_CODE                AS motif
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    UNION ALL
    SELECT PERS_ID, DATE_DECES, 'DECES' FROM USER_GRH.GRH_PERSONNE WHERE DATE_DECES IS NOT NULL
),
-- la première sortie de chaque agent, avec son motif
sorties AS (
    SELECT PERS_ID, dt AS dt_sortie, motif
    FROM (SELECT PERS_ID, dt, motif,
                 ROW_NUMBER() OVER (PARTITION BY PERS_ID ORDER BY dt, motif) AS rn
          FROM decisions WHERE PERS_ID IS NOT NULL)
    WHERE rn = 1
),
nature AS (
    SELECT a.annee, s.PERS_ID, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY a.annee, s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM annees a
    JOIN USER_GRH.GRH_SITUATION s ON s.DATE_DEBUT <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
),
-- les sorties des permanents, l'année de la sortie
sorties_permanents AS (
    SELECT n.annee, so.motif
    FROM nature n
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = n.PERS_ID
    JOIN sorties so ON so.PERS_ID = n.PERS_ID
    WHERE n.rn = 1
      AND n.NATURE_EMPLOIE = 'PERMANENT'
      AND TO_CHAR(p.QUALITE) = 'A'
      AND EXTRACT(YEAR FROM so.dt_sortie) = n.annee
),
-- le statut de chaque situation et celui de la situation d'avant
statuts AS (
    SELECT s.PERS_ID, s.DATE_DEBUT, s.NATURE_EMPLOIE,
           NVL(TO_CHAR(s.STATUT_EMPLOYE), '-')                    AS statut,
           LAG(NVL(TO_CHAR(s.STATUT_EMPLOYE), '-'))
               OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT, s.SITU_ID) AS statut_avant
    FROM USER_GRH.GRH_SITUATION s
),
lignes AS (
    SELECT CASE motif WHEN 'DECES' THEN '1. Sortie : date de décès'
                      ELSE '1. Sortie : décision ' || motif END   AS mouvement,
           annee
    FROM sorties_permanents
    UNION ALL
    SELECT CASE WHEN st.statut IN ('B', 'D', 'E', 'F', 'H')
                THEN '2. Passage au statut ' || st.statut
                ELSE '3. Retour du statut ' || st.statut_avant END,
           EXTRACT(YEAR FROM st.DATE_DEBUT)
    FROM statuts st
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = st.PERS_ID
    WHERE TO_CHAR(p.QUALITE) = 'A'
      AND st.NATURE_EMPLOIE = 'PERMANENT'
      AND st.statut_avant IS NOT NULL
      AND st.DATE_DEBUT >= DATE '2019-01-01'
      AND st.DATE_DEBUT <  DATE '2025-01-01'
      AND (    (st.statut IN ('B', 'D', 'E', 'F', 'H')
                AND st.statut_avant NOT IN ('B', 'D', 'E', 'F', 'H'))
           OR  (st.statut NOT IN ('B', 'D', 'E', 'F', 'H')
                AND st.statut_avant IN ('B', 'D', 'E', 'F', 'H')))
)
SELECT mouvement,
       SUM(CASE WHEN annee = 2019 THEN 1 ELSE 0 END)              AS a2019,
       SUM(CASE WHEN annee = 2020 THEN 1 ELSE 0 END)              AS a2020,
       SUM(CASE WHEN annee = 2021 THEN 1 ELSE 0 END)              AS a2021,
       SUM(CASE WHEN annee = 2022 THEN 1 ELSE 0 END)              AS a2022,
       SUM(CASE WHEN annee = 2023 THEN 1 ELSE 0 END)              AS a2023,
       SUM(CASE WHEN annee = 2024 THEN 1 ELSE 0 END)              AS a2024
FROM lignes
GROUP BY mouvement
ORDER BY mouvement;
