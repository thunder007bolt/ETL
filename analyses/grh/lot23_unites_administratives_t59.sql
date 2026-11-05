-- =============================================================================
-- LOT 23 — Les unités administratives, pour le domaine d'activité (T59)
-- Rédigé le 04/11/2026 — EXÉCUTÉ le 04/11/2026 sur USER_GRH.
-- Exécution : en mode script (F5) dans SQL Developer. Comptages, codes et noms
-- d'unités seulement : aucune donnée nominative. Dans la table des unités, on
-- ne lit jamais l'adresse, le téléphone, le fax, le courriel ni le compte
-- bancaire (ADRESSE, TELEPHONE, TELECOPIEUR, COURRIEL, NO_COMPTE,
-- INTITULE_COMPTE).
-- Pourquoi ce lot : à la réunion du 03/11/2026 (terme 8 de la fiche de
-- lecture), la CNSS a dit que le code de domaine de la situation
-- (GRH_SITUATION.CODE_DOMAINE_ACT) n'est pas valable : le domaine d'un agent
-- vient de son unité administrative (UA_CODE). La CNSS tiendra un fichier
-- Excel de correspondance « unité → domaine », avec les 5 domaines du recueil
-- des besoins v7 (recouvrement, prestations, prévention des risques
-- professionnels, action sanitaire et sociale, gestion administrative et
-- financière). Ce lot :
--   1. vérifie les colonnes d'unité de la base, dont un éventuel « UAT »
--      (Q124) ;
--   2. décrit les natures d'unités (Q125) ;
--   3. mesure si chaque agent actif a une unité connue (Q126) ;
--   4. donne la liste des unités, avec leur rattachement, base du fichier
--      Excel à pré-remplir (Q127) ;
--   5. regarde les sections, plus fines que l'unité (Q128) ;
--   6. chiffre le manque de fiabilité du code de domaine (Q129).
-- Remplace le lot 16 (Q95, Q96, jamais exécuté), qui partait du code de
-- domaine de la situation.
-- Agents pris en compte : agents de la CNSS (qualité A), fiche ACTIVE, décrits
-- par leur dernière situation commencée au plus tard aujourd'hui (copie de la
-- base du 18/02/2026).
-- Numérotation : suite du lot 22, Q124 à Q129.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q124 — Les colonnes d'unité et de section dans la base
-- Objectif : toutes les colonnes dont le nom commence par UA (UA_CODE,
--            UA_CODE_SUP… et un éventuel UAT) ou par SECT, et toutes les
--            colonnes des tables d'unités et de sections, avec le nombre de
--            lignes des tables.
-- Tableaux : T59.
-- Lecture  : vérifier que le « code UAT » cité en réunion est bien UA_CODE,
--            et repérer les colonnes de rattachement (UA_CODE_SUP, REGION,
--            DR_NO) et de libellé (NOM, DESIGNATION, UA_SIGLE).
-- Résultat (04/11/2026) : aucune colonne « UAT » : le code d'unité est
--            UA_CODE (VARCHAR2 12), dans GRH_SITUATION (colonne 24) et dans
--            les décisions, mouvements, formations, frais et sanctions.
--            GRH_UNITE_ADMINISTRATIVE : 722 lignes ; colonnes lues : UA_CODE,
--            NOM, UA_CODE_SUP, UA_NATURE, DESIGNATION, UA_CATEGORIE, LIEU_ID,
--            NO_EMPLOYEUR, IMPUTATION_COMPTABLE, REGION, DR_NO, GP_CODE,
--            UA_STATUT, UA_RANG, UA_SIGLE, UA_DATE_VIGUEUR, UA_GROUPE,
--            UA_LIEU_CNSS_CODE, UA_QUALIF_REGION, UA_ORGANIGRAMME.
--            GRH_SECTION : 100 lignes (SECT_CODE, SECT_NOM, UA_CODE_SUP…).
--            GRH_SECTION_ANALYTIQUE : 26 lignes (CODE_ANA, DESCRIPTION,
--            CODE_PROGR). GRH_UNITE_ADMIN_NATURE : 3 lignes. La situation porte
--            aussi SECT_CODE et SECTION_ANALYTIQUE.
-- Constat  : le « code UAT » de la réunion est UA_CODE.
-- -----------------------------------------------------------------------------
SELECT c.table_name, c.column_id, c.column_name, c.data_type, c.data_length,
       t.num_rows
FROM ALL_TAB_COLUMNS c
JOIN ALL_TABLES t ON t.owner = c.owner AND t.table_name = c.table_name
WHERE c.owner = 'USER_GRH'
  AND (c.column_name LIKE 'UA%' OR c.column_name LIKE 'SECT%'
       OR c.table_name LIKE '%UNITE%' OR c.table_name LIKE '%SECTION%')
  AND c.column_name NOT IN ('ADRESSE', 'TELEPHONE', 'TELECOPIEUR', 'COURRIEL',
                            'NO_COMPTE', 'INTITULE_COMPTE')
ORDER BY c.table_name, c.column_id;


-- -----------------------------------------------------------------------------
-- Q125 — Les natures d'unités
-- Objectif : pour chaque nature d'unité (direction, service, agence…), son
--            libellé, le nombre d'unités, le nombre d'unités qui ont des agents
--            actifs, et le nombre d'agents actifs.
-- Tableaux : T59.
-- Lecture  : savoir à quel niveau la CNSS range ses agents (direction,
--            service, bureau), pour choisir le niveau du fichier Excel.
-- Résultat (04/11/2026) : nature · libellé · unités · unités avec
--            agents · agents actifs
--            Service · Service d'une direction/Siège · 376 · 114 · 590
--            Direction · Direction · 56 · 25 · 285
--            Organisme · Organisme de formation · 290 · 0 · 0
-- Constat  : - deux niveaux seulement : la direction (centrale ou régionale)
--              et le service (dont les agences et les bureaux) ;
--            - les « organismes » sont les organismes de formation (CIPRES,
--              EN3S, cabinets…), rangés dans la même table : à exclure.
-- -----------------------------------------------------------------------------
WITH der AS (
    SELECT s.PERS_ID, s.UA_CODE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
actifs AS (
    SELECT d.PERS_ID, d.UA_CODE
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
),
natures AS (
    SELECT TO_CHAR(UA_NATURE) AS nature, MAX(TO_CHAR(REMARQUE)) AS libelle
    FROM USER_GRH.GRH_UNITE_ADMIN_NATURE
    GROUP BY TO_CHAR(UA_NATURE)
)
SELECT NVL(TO_CHAR(u.UA_NATURE), '-')                            AS nature,
       MAX(n.libelle)                                            AS libelle,
       COUNT(DISTINCT u.UA_CODE)                                 AS unites,
       COUNT(DISTINCT CASE WHEN a.PERS_ID IS NOT NULL
                           THEN u.UA_CODE END)                   AS unites_avec_agents,
       COUNT(a.PERS_ID)                                          AS agents_actifs
FROM USER_GRH.GRH_UNITE_ADMINISTRATIVE u
LEFT JOIN natures n ON n.nature = TO_CHAR(u.UA_NATURE)
LEFT JOIN actifs a  ON a.UA_CODE = u.UA_CODE
GROUP BY NVL(TO_CHAR(u.UA_NATURE), '-')
ORDER BY agents_actifs DESC;


-- -----------------------------------------------------------------------------
-- Q126 — Chaque agent actif a-t-il une unité connue ?
-- Objectif : compter les agents actifs selon l'unité de leur dernière
--            situation : vide, introuvable dans la table des unités, ou
--            trouvée ; par nature d'emploi (permanent, temporaire), avec le
--            nombre d'unités différentes et le nombre d'agents qui ont aussi
--            une section.
-- Tableaux : T59.
-- Lecture  : les agents « vide » et « introuvable » iront en « Domaine non
--            renseigné » ; s'ils sont nombreux, le fichier Excel ne suffira
--            pas, il faudra corriger les situations.
-- Résultat (04/11/2026) : état de l'unité · nature d'emploi · agents ·
--            unités · avec section
--            3 unité trouvée · PERMANENT · 869 · 139 · 46
--            3 unité trouvée · TEMPORAIRE · 6 · 2 · 0
-- Constat  : tous les agents actifs ont une unité, et elle existe dans la
--            table : aucun agent n'ira en « Domaine non renseigné » faute
--            d'unité.
-- -----------------------------------------------------------------------------
WITH der AS (
    SELECT s.PERS_ID, s.UA_CODE, s.SECT_CODE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
actifs AS (
    SELECT d.PERS_ID, d.UA_CODE, d.SECT_CODE, d.NATURE_EMPLOIE
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
),
lignes AS (
    SELECT a.PERS_ID, a.UA_CODE, a.SECT_CODE,
           NVL(TO_CHAR(a.NATURE_EMPLOIE), '-')                  AS nature_emploi,
           CASE WHEN a.UA_CODE IS NULL THEN '1 unité vide'
                WHEN u.UA_CODE IS NULL THEN '2 unité introuvable'
                ELSE '3 unité trouvée' END                      AS etat_unite
    FROM actifs a
    LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u ON u.UA_CODE = a.UA_CODE
)
SELECT etat_unite, nature_emploi,
       COUNT(*)                                                 AS agents,
       COUNT(DISTINCT UA_CODE)                                  AS unites,
       SUM(CASE WHEN SECT_CODE IS NOT NULL THEN 1 ELSE 0 END)   AS avec_section
FROM lignes
GROUP BY etat_unite, nature_emploi
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- Q127 — La liste des unités, base du fichier Excel « unité → domaine »
-- Objectif : une ligne par unité administrative : code, sigle, nom,
--            désignation, nature, catégorie, unité de rattachement, niveau et
--            chemin dans l'organigramme (de la racine à l'unité), région,
--            direction régionale, statut, date de vigueur, et le nombre
--            d'agents actifs (permanents, temporaires).
-- Tableaux : T59 (et la déclinaison par direction, agence et bureau demandée
--            par le recueil des besoins v7).
-- Lecture  : ne pas coller le résultat dans le chat s'il est long : l'exporter
--            en Excel (clic droit sur la grille, Exporter, format xlsx) et le
--            déposer dans cnss_files sous le nom
--            unites_administratives_lot23.xlsx. Il servira à pré-remplir le
--            fichier de correspondance que la CNSS complétera.
-- Résultat (04/11/2026) : 722 unités, collées dans le chat (non gardées
--            dans le projet : la liste des organismes contient des noms de
--            personnes). Trois générations d'organigramme :
--            - 1993 : codes à 4 chiffres (1000 DG, 3200 DRO…), statut N, date
--              01/01/93 ; 2 agents encore sur 3284 (Agence de Dassasgho) ;
--            - 2012, codes à 6 chiffres (100090 DCP, 110100 DRO…) : statut N,
--              presque vides ; 8 agents encore rattachés (100010 DG, 100070
--              SG, 100090 DCP, 100140 DRH 2, 110103 SFC-DRO, 120103 FC-DRF,
--              130105 RC-DRN) ;
--            - 2012 et 2018, codes à 8 chiffres (10008000 DCR, 11010000
--              DRO…) : statut O, l'organigramme en vigueur, presque tous les
--              agents ;
--            - 291 organismes de formation (10000 à 10288, 8000), sans agent.
--            UA_CATEGORIE : C central, R régional. REGION et DR_NO : 000 siège ;
--            100 et 10 DRO ; 500 et 20 DRB ; 300 et 30 DRN ; 200 et 40 DRF ;
--            400 et 50 DRD. Les agences et les bureaux sont des services
--            rattachés à leur direction régionale.
-- Constat  : - le domaine se lit sans difficulté dans le nom de la plupart des
--              services (recouvrement, cotisations, immatriculation,
--              prestations, assurance vieillesse, AT/MP, prévention, SMI,
--              comptabilité, personnel…) ;
--            - restent à trancher par la CNSS : les agences et les bureaux
--              (guichets mixtes, recouvrement et prestations), les directions
--              régionales elles-mêmes (45 agents à la DRO, 23 à la DRB…) et
--              la DPASS (prévention et action sanitaire et sociale) ;
--            - la même table donne la déclinaison du recueil des besoins :
--              direction régionale (REGION, DR_NO), direction (unité de
--              niveau 1), agence ou bureau (service de niveau 2) ;
--            - 10 agents ont pour dernière situation une unité fermée (statut
--              N) : point d'attention à signaler à la DRH.
-- -----------------------------------------------------------------------------
WITH arbre AS (
    SELECT u.UA_CODE,
           LEVEL                                                AS niveau,
           SYS_CONNECT_BY_PATH(NVL(TO_CHAR(u.UA_SIGLE), TO_CHAR(u.UA_CODE)),
                               ' > ')                           AS chemin
    FROM USER_GRH.GRH_UNITE_ADMINISTRATIVE u
    START WITH u.UA_CODE_SUP IS NULL OR u.UA_CODE_SUP = u.UA_CODE
    CONNECT BY NOCYCLE PRIOR u.UA_CODE = u.UA_CODE_SUP
               AND u.UA_CODE <> u.UA_CODE_SUP
),
der AS (
    SELECT s.PERS_ID, s.UA_CODE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
agents AS (
    SELECT d.UA_CODE,
           COUNT(*)                                             AS actifs,
           SUM(CASE WHEN d.NATURE_EMPLOIE = 'PERMANENT' THEN 1 ELSE 0 END) AS permanents,
           SUM(CASE WHEN d.NATURE_EMPLOIE = 'TEMPORAIRE' THEN 1 ELSE 0 END) AS temporaires
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
    GROUP BY d.UA_CODE
)
SELECT u.UA_CODE,
       TO_CHAR(u.UA_SIGLE)                                      AS sigle,
       TO_CHAR(u.NOM)                                           AS nom,
       TO_CHAR(u.DESIGNATION)                                   AS designation,
       TO_CHAR(u.UA_NATURE)                                     AS nature,
       TO_CHAR(u.UA_CATEGORIE)                                  AS categorie,
       TO_CHAR(u.UA_CODE_SUP)                                   AS code_rattachement,
       TO_CHAR(sup.NOM)                                         AS nom_rattachement,
       ar.niveau,
       ar.chemin,
       TO_CHAR(u.REGION)                                        AS region,
       TO_CHAR(u.DR_NO)                                         AS dr_no,
       TO_CHAR(u.UA_STATUT)                                     AS statut,
       u.UA_DATE_VIGUEUR,
       NVL(g.actifs, 0)                                         AS agents_actifs,
       NVL(g.permanents, 0)                                     AS permanents,
       NVL(g.temporaires, 0)                                    AS temporaires
FROM USER_GRH.GRH_UNITE_ADMINISTRATIVE u
LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE sup ON sup.UA_CODE = u.UA_CODE_SUP
LEFT JOIN arbre  ar ON ar.UA_CODE = u.UA_CODE
LEFT JOIN agents g  ON g.UA_CODE = u.UA_CODE
ORDER BY ar.chemin NULLS LAST, u.UA_CODE;


-- -----------------------------------------------------------------------------
-- Q128 — Les sections, plus fines que l'unité
-- Objectif : combien de sections existent, combien ont des agents actifs, et
--            si la section d'un agent appartient bien à son unité (la section
--            est rattachée à une unité par GRH_SECTION.UA_CODE_SUP).
-- Tableaux : T59.
-- Lecture  : si les sections sont remplies et cohérentes, le fichier Excel
--            pourrait descendre au niveau de la section ; sinon, on reste à
--            l'unité.
-- Résultat (04/11/2026) : sections 100 · avec agents 1 · agents actifs
--            875 · avec section 46 · section introuvable 0 · section dans son
--            unité 0.
-- Constat  : les sections ne servent pas : 46 agents ont la même section,
--            qui n'appartient pas à leur unité. Le fichier reste au niveau de
--            l'unité.
-- -----------------------------------------------------------------------------
WITH der AS (
    SELECT s.PERS_ID, s.UA_CODE, s.SECT_CODE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
actifs AS (
    SELECT d.PERS_ID, d.UA_CODE, d.SECT_CODE
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
),
nb AS (
    SELECT COUNT(*) AS sections FROM USER_GRH.GRH_SECTION
)
SELECT MAX(nb.sections)                                         AS sections,
       COUNT(DISTINCT a.SECT_CODE)                              AS sections_avec_agents,
       COUNT(*)                                                 AS agents_actifs,
       SUM(CASE WHEN a.SECT_CODE IS NOT NULL THEN 1 ELSE 0 END) AS avec_section,
       SUM(CASE WHEN a.SECT_CODE IS NOT NULL
                 AND se.SECT_CODE IS NULL THEN 1 ELSE 0 END)    AS section_introuvable,
       SUM(CASE WHEN se.UA_CODE_SUP = a.UA_CODE THEN 1 ELSE 0 END) AS section_dans_son_unite
FROM actifs a
CROSS JOIN nb
LEFT JOIN USER_GRH.GRH_SECTION se ON se.SECT_CODE = a.SECT_CODE;


-- -----------------------------------------------------------------------------
-- Q129 — Le code de domaine face aux unités
-- Objectif : pour les unités qui ont des agents actifs, combien n'ont qu'un
--            seul code de domaine (CODE_DOMAINE_ACT) parmi leurs agents, et
--            combien en ont plusieurs ; et combien d'agents actifs n'ont pas
--            de code de domaine.
-- Tableaux : T59.
-- Lecture  : chiffre pour expliquer à la CNSS pourquoi on abandonne le code
--            de domaine : si des agents d'une même unité ont des domaines
--            différents, le code ne suit pas l'unité.
-- Résultat (04/11/2026) : codes dans l'unité · unités · agents · sans code
--            1 seul code · 73 · 197 · 0
--            2 codes ou plus · 66 · 678 · 0
-- Constat  : dans 66 unités sur 139, qui regroupent 678 agents actifs sur 875
--            (77 %), les agents d'une même unité n'ont pas le même code de
--            domaine : le code ne suit pas l'unité. Cela confirme ce qu'a dit
--            la CNSS en réunion.
-- -----------------------------------------------------------------------------
WITH der AS (
    SELECT s.PERS_ID, s.UA_CODE, s.CODE_DOMAINE_ACT,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
actifs AS (
    SELECT d.PERS_ID, d.UA_CODE, d.CODE_DOMAINE_ACT
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
),
par_unite AS (
    SELECT UA_CODE,
           COUNT(*)                                             AS agents,
           COUNT(DISTINCT CODE_DOMAINE_ACT)                     AS codes_differents,
           SUM(CASE WHEN CODE_DOMAINE_ACT IS NULL THEN 1 ELSE 0 END) AS sans_code
    FROM actifs
    WHERE UA_CODE IS NOT NULL
    GROUP BY UA_CODE
)
SELECT CASE WHEN codes_differents = 0 THEN '0 code'
            WHEN codes_differents = 1 THEN '1 seul code'
            ELSE '2 codes ou plus' END                          AS codes_dans_l_unite,
       COUNT(*)                                                 AS unites,
       SUM(agents)                                              AS agents,
       SUM(sans_code)                                           AS agents_sans_code
FROM par_unite
GROUP BY CASE WHEN codes_differents = 0 THEN '0 code'
              WHEN codes_differents = 1 THEN '1 seul code'
              ELSE '2 codes ou plus' END
ORDER BY 1;


-- =============================================================================
-- LOT 23, PARTIE B — Rédigée le 04/11/2026 après la partie A — EXÉCUTÉE le
-- 04/11/2026.
-- Q130 donne la liste des unités de la CNSS seulement, sans les organismes de
-- formation (leur liste contient des noms de personnes) : c'est elle qu'on
-- exporte en Excel pour pré-remplir le fichier « unité → domaine ». Q131 lit
-- les sections analytiques, rattachées à un code de programme : une autre
-- piste de classement à regarder avant de remplir le fichier.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q130 — Les unités de la CNSS, à exporter en Excel
-- Objectif : la même liste que Q127, sans les organismes de formation, les
--            unités en vigueur (statut O) d'abord.
-- Tableaux : T59 (et la déclinaison par direction, agence et bureau).
-- Lecture  : exporter le résultat en Excel (clic droit sur la grille,
--            Exporter, format xlsx) et le déposer dans cnss_files sous le nom
--            unites_cnss_lot23.xlsx. Je le transforme en fichier de
--            correspondance pré-rempli, avec une liste déroulante des 5
--            domaines.
-- Résultat (04/11/2026) : 432 unités de la CNSS (sans les organismes).
--            157 en vigueur (statut O, codes à 8 chiffres) : 69 au siège
--            (16 directions et leurs services) et 88 en région (DRO 23, DRB
--            25, DRN 14, DRF 13, DRD 13, direction comprise) ; elles portent
--            865 des 875 agents actifs. 275 fermées (statut N) : l'ancien
--            organigramme de 2012 (codes à 6 chiffres) et celui de 1993
--            (4 chiffres) ; 10 agents encore rattachés (100010 DG, 100070 SG,
--            100090 DCP, 100140 DRH 2, 110103 SFC-DRO, 120103 FC-DRF, 130105
--            RC-DRN, 3284 agence de Dassasgho 2).
-- -----------------------------------------------------------------------------
WITH arbre AS (
    SELECT u.UA_CODE,
           LEVEL                                                AS niveau,
           SYS_CONNECT_BY_PATH(NVL(TO_CHAR(u.UA_SIGLE), TO_CHAR(u.UA_CODE)),
                               ' > ')                           AS chemin
    FROM USER_GRH.GRH_UNITE_ADMINISTRATIVE u
    START WITH u.UA_CODE_SUP IS NULL OR u.UA_CODE_SUP = u.UA_CODE
    CONNECT BY NOCYCLE PRIOR u.UA_CODE = u.UA_CODE_SUP
               AND u.UA_CODE <> u.UA_CODE_SUP
),
der AS (
    SELECT s.PERS_ID, s.UA_CODE, s.NATURE_EMPLOIE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
agents AS (
    SELECT d.UA_CODE,
           COUNT(*)                                             AS actifs,
           SUM(CASE WHEN d.NATURE_EMPLOIE = 'PERMANENT' THEN 1 ELSE 0 END) AS permanents,
           SUM(CASE WHEN d.NATURE_EMPLOIE = 'TEMPORAIRE' THEN 1 ELSE 0 END) AS temporaires
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
    GROUP BY d.UA_CODE
)
SELECT u.UA_CODE,
       TO_CHAR(u.UA_SIGLE)                                      AS sigle,
       TO_CHAR(u.NOM)                                           AS nom,
       TO_CHAR(u.UA_NATURE)                                     AS nature,
       TO_CHAR(u.UA_CATEGORIE)                                  AS categorie,
       TO_CHAR(u.UA_STATUT)                                     AS statut,
       TO_CHAR(u.REGION)                                        AS region,
       TO_CHAR(u.DR_NO)                                         AS dr_no,
       TO_CHAR(u.UA_CODE_SUP)                                   AS code_rattachement,
       TO_CHAR(sup.NOM)                                         AS nom_rattachement,
       ar.niveau,
       ar.chemin,
       u.UA_DATE_VIGUEUR,
       NVL(g.actifs, 0)                                         AS agents_actifs,
       NVL(g.permanents, 0)                                     AS permanents,
       NVL(g.temporaires, 0)                                    AS temporaires
FROM USER_GRH.GRH_UNITE_ADMINISTRATIVE u
LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE sup ON sup.UA_CODE = u.UA_CODE_SUP
LEFT JOIN arbre  ar ON ar.UA_CODE = u.UA_CODE
LEFT JOIN agents g  ON g.UA_CODE = u.UA_CODE
WHERE NVL(TO_CHAR(u.UA_NATURE), '-') <> 'Organisme'
ORDER BY TO_CHAR(u.UA_STATUT) DESC, ar.chemin, u.UA_CODE;


-- -----------------------------------------------------------------------------
-- Q131 — Les sections analytiques et leur programme
-- Objectif : les 26 sections analytiques (code, description, code de
--            programme) et le nombre d'agents actifs dont la dernière
--            situation porte chacune ; plus le nombre d'agents actifs sans
--            section analytique.
-- Tableaux : T59.
-- Lecture  : si les sections analytiques sont remplies et que leurs
--            programmes ressemblent aux 5 domaines (recouvrement, prestations,
--            prévention, action sanitaire et sociale, gestion administrative
--            et financière), elles peuvent aider à remplir le fichier.
-- Résultat (04/11/2026) : code · description · programme · agents actifs
--            0101 Prestations familiales et de maternité · 01 · 132
--            0102 Gestion des risques professionnels · 01 · 24
--            0103 Gestion des pensions · 01 · 20
--            0104 Gestion de l'action sanitaire et sociale · 01 · 94
--            0106 Application et vulgarisation des textes en vigueur · 01 · 29
--            0111 Dépenses communes du programme 1 · 01 · 3
--            0201 Immatriculation · 02 · 14 ; 0202 Cotisations sociales · 02
--            · 68 ; 0203 Contrôle employeur · 02 · 44 ; 0204 Gestion du
--            contentieux du recouvrement des cotisations · 02 · 14 ; 0211
--            Dépenses communes du programme 2 · 02 · 0
--            0301 Gestion des fonds de sécurité sociale · 03 · 33 ; 0302
--            Opérations comptables · 03 · 15 ; 0311 · 03 · 0
--            0401 Pilotage et coordination · 04 · 1 ; 0402 Planification et
--            suivi-évaluation · 04 · 33 ; 0403 Gestion immobilière, de
--            l'approvisionnement et de la logistique · 04 · 4 ; 0404 Gestion
--            des ressources humaines · 04 · 20 ; 0405 Communication · 04 · 3 ;
--            0406 à 0411 (système d'information, archives, juridique,
--            qualité, audit, dépenses communes) · 04 · 0
--            05 Inconnu · 01 · 1 ; (vide) · 0
-- Constat  : - tous les agents actifs ont une section analytique, et les
--              sections suivent les 4 programmes du budget-programme :
--              01 prestations (y compris risques professionnels et action
--              sanitaire et sociale), 02 recouvrement, 03 gestion financière,
--              04 pilotage et soutien. C'est un classement officiel de la
--              CNSS, très proche des 5 domaines du recueil des besoins ;
--            - 552 agents seulement retombent sur une section de la table :
--              323 ont une section qu'elle ne connaît pas (Q132) ;
--            - deux sections sont à trancher : 0102 (gestion des risques
--              professionnels : prestations AT/MP ou prévention ?) et 0106
--              (application et vulgarisation des textes).
-- -----------------------------------------------------------------------------
WITH der AS (
    SELECT s.PERS_ID, TRIM(TO_CHAR(s.SECTION_ANALYTIQUE)) AS section_ana,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
actifs AS (
    SELECT d.PERS_ID, d.section_ana
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
)
SELECT TO_CHAR(a.CODE_ANA)                                      AS code_ana,
       TO_CHAR(a.DESCRIPTION)                                   AS description,
       TO_CHAR(a.CODE_PROGR)                                    AS code_programme,
       COUNT(x.PERS_ID)                                         AS agents_actifs
FROM USER_GRH.GRH_SECTION_ANALYTIQUE a
LEFT JOIN actifs x ON x.section_ana = TRIM(TO_CHAR(a.CODE_ANA))
GROUP BY TO_CHAR(a.CODE_ANA), TO_CHAR(a.DESCRIPTION), TO_CHAR(a.CODE_PROGR)
UNION ALL
SELECT '(vide)', 'agents actifs sans section analytique', NULL, COUNT(*)
FROM actifs
WHERE section_ana IS NULL
ORDER BY 1;


-- =============================================================================
-- LOT 23, PARTIE C — Rédigée le 04/11/2026 après Q131 — MISE DE CÔTÉ, NON
-- EXÉCUTÉE : la demande est le fichier Excel « unité → domaine »
-- (correspondance_unite_domaine.xlsx) ; la section analytique reste une piste
-- à présenter à la CNSS, pas un travail en cours.
-- Q131 montre que chaque agent porte une section analytique du budget-
-- programme, proche des 5 domaines. Avant de choisir entre le fichier Excel
-- « unité → domaine » (décision de la réunion) et la section analytique de
-- l'agent, deux vérifications : les sections inconnues de la table (Q132), et
-- la section dominante de chaque unité (Q133).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q132 — Les sections analytiques que la table ne connaît pas
-- Objectif : les valeurs de section analytique des agents actifs absentes de
--            GRH_SECTION_ANALYTIQUE (323 agents), avec leur longueur et leur
--            nombre d'agents.
-- Tableaux : T59.
-- Lecture  : si ce sont les mêmes codes écrits autrement (« 101 » pour
--            « 0101 », un espace…), on les relie ; si ce sont d'anciens codes,
--            il faut leur correspondance.
-- Résultat : à remplir.
-- -----------------------------------------------------------------------------
WITH der AS (
    SELECT s.PERS_ID, TRIM(TO_CHAR(s.SECTION_ANALYTIQUE)) AS section_ana,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
actifs AS (
    SELECT d.PERS_ID, d.section_ana
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
)
SELECT x.section_ana,
       LENGTH(x.section_ana)                                    AS longueur,
       COUNT(*)                                                 AS agents_actifs
FROM actifs x
LEFT JOIN USER_GRH.GRH_SECTION_ANALYTIQUE a
       ON TRIM(TO_CHAR(a.CODE_ANA)) = x.section_ana
WHERE a.CODE_ANA IS NULL
GROUP BY x.section_ana, LENGTH(x.section_ana)
ORDER BY agents_actifs DESC;


-- -----------------------------------------------------------------------------
-- Q133 — La section analytique dominante de chaque unité
-- Objectif : pour chaque unité qui a des agents actifs : sa section
--            analytique la plus fréquente, la part des agents qui la portent,
--            et le nombre de sections différentes dans l'unité.
-- Tableaux : T59.
-- Lecture  : si chaque unité a presque une seule section, unité et section
--            disent la même chose, et la section dominante pré-remplit le
--            fichier Excel ; si les sections se mélangent dans une unité, la
--            section de l'agent est plus fine que son unité.
-- Résultat : à remplir.
-- -----------------------------------------------------------------------------
WITH der AS (
    SELECT s.PERS_ID, s.UA_CODE, TRIM(TO_CHAR(s.SECTION_ANALYTIQUE)) AS section_ana,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
actifs AS (
    SELECT d.PERS_ID, d.UA_CODE, d.section_ana
    FROM der d
    JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
    WHERE d.rn = 1
      AND p.ETAT_EMPLOYE = 'ACTIF'
      AND TO_CHAR(p.QUALITE) = 'A'
),
par AS (
    SELECT UA_CODE, section_ana,
           COUNT(*)                                             AS agents,
           SUM(COUNT(*)) OVER (PARTITION BY UA_CODE)            AS agents_unite,
           COUNT(*) OVER (PARTITION BY UA_CODE)                 AS sections_unite,
           ROW_NUMBER() OVER (PARTITION BY UA_CODE
                              ORDER BY COUNT(*) DESC, section_ana) AS rn
    FROM actifs
    GROUP BY UA_CODE, section_ana
)
SELECT p.UA_CODE,
       TO_CHAR(u.UA_SIGLE)                                      AS sigle,
       TO_CHAR(u.UA_STATUT)                                     AS statut,
       p.section_ana                                            AS section_dominante,
       TO_CHAR(a.DESCRIPTION)                                   AS description,
       p.agents,
       p.agents_unite,
       ROUND(100 * p.agents / p.agents_unite)                   AS part_pct,
       p.sections_unite
FROM par p
JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u ON u.UA_CODE = p.UA_CODE
LEFT JOIN USER_GRH.GRH_SECTION_ANALYTIQUE a
       ON TRIM(TO_CHAR(a.CODE_ANA)) = p.section_ana
WHERE p.rn = 1
ORDER BY TO_CHAR(u.UA_STATUT) DESC, p.UA_CODE;
