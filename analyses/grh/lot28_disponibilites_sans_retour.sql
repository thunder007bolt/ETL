-- =============================================================================
-- LOT 28 — Les agents partis en disponibilité et jamais revenus
-- Rédigé le 17/11/2026 — EXÉCUTÉ le 17/11/2026 sur USER_GRH, la base source.
-- Exécution : en mode script (F5) dans SQL Developer. Rien à saisir. Comptage
-- seulement : aucune donnée nominative.
-- Pourquoi ce lot : T60 ne montre que 0 à 1 mise en disponibilité par an,
-- alors que la base a 2 à 6 décisions 07 par an, comme l'annuaire CNSS (lot
-- 27, Q156). Piste : l'agent qui ne revient pas a une fiche inactive sans
-- départ daté ; la règle G2 ne le compte alors à aucune date, même avant sa
-- disponibilité, et sa sortie n'apparaît jamais dans T60.
-- Numérotation : suite du lot 27, Q159.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q159 — La dernière mise en disponibilité de chaque agent, et ce qu'il devient
-- Objectif : pour les agents de la CNSS dont la dernière mise en
--            disponibilité (07) date de 2015 ou après, compter par année ceux
--            qui sont revenus (18 après), ceux qui ont un départ daté après,
--            ceux dont la fiche est inactive sans retour ni départ daté, et
--            ceux dont la fiche n'est pas inactive.
-- Tableaux : T58, T60 (sorties DISP ; points 25, 49 et 64 de la v2).
-- Lecture  : la colonne inactif_sans_depart_date donne les agents que la
--            règle ne compte à aucune date. Si elle explique l'écart avec
--            l'annuaire (5, 3, 2, 4 et 7 disponibilités de 2020 à 2024), la
--            date de la mise en disponibilité peut servir de date de départ
--            pour ces agents : ils seraient comptés avant, et sortiraient en
--            DISP.
-- Résultat (17/11/2026) : année de la dernière disponibilité · agents ·
--            revenus · partis avec une date · fiche inactive sans départ
--            daté · fiche non inactive, sans retour
--            2015 · 1 · 0 · 0 · 0 · 1
--            2017 · 2 · 0 · 2 · 0 · 0
--            2018 · 3 · 2 · 0 · 0 · 1
--            2019 · 5 · 5 · 0 · 0 · 0
--            2020 · 2 · 2 · 0 · 0 · 0
--            2021 · 3 · 1 · 0 · 2 · 0
--            2022 · 1 · 0 · 0 · 1 · 0
--            2023 · 4 · 1 · 0 · 3 · 0
--            2024 · 5 · 2 · 0 · 3 · 0
--            2025 · 3 · 0 · 0 · 3 · 0
-- Constat  : - la piste est confirmée : 12 agents mis en disponibilité de
--              2021 à 2025 ne sont pas revenus, et leur fiche est inactive
--              sans aucun départ daté ;
--            - la fiche d'un agent en disponibilité passe donc à INACTIF :
--              « inactif » ne veut pas dire « parti » ;
--            - la règle G2 (une fiche inactive sans départ daté n'est comptée
--              à aucune date) efface ces 12 agents de toutes les années, même
--              celles où ils travaillaient : il manque 12 permanents fin
--              2020, 10 fin 2021, 9 fin 2022, 6 fin 2023 et 3 fin 2024, et
--              T60 ne voit pas leur sortie ;
--            - correction à faire dans les requêtes de l'effectif et de T60
--              (passe unique) : pour une fiche inactive sans départ daté, la
--              dernière mise en disponibilité sans retour sert de date de
--              sortie. T60 donnerait alors 2, 1, 3 et 3 disponibilités de
--              plus de 2021 à 2024, au niveau de l'annuaire (3, 2, 4, 7) ;
--            - avant 2021, tous ceux qui ne sont pas revenus ont un départ
--              daté ou une fiche non inactive (2 agents, depuis 2015 et
--              2018).
-- -----------------------------------------------------------------------------
WITH decs AS (
    SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
           TO_CHAR(d.CDEC_CODE)       AS code,
           TRUNC(d.DATE_VIGUEUR)      AS dt
    FROM USER_GRH.GRH_DECISION d
    LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp ON dp.DECI_NO = d.DECI_NO
    WHERE d.CDEC_CODE IN ('07', '18', '02', '34', '35', '36', '52', '54')
      AND d.DATE_VIGUEUR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM USER_GRH.GRH_DECISION a
                      WHERE a.CDEC_CODE = '40'
                        AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
),
dispo AS (
    SELECT PERS_ID, MAX(dt) AS dt
    FROM decs
    WHERE code = '07' AND PERS_ID IS NOT NULL
    GROUP BY PERS_ID
),
suite AS (
    SELECT di.PERS_ID, di.dt,
           MIN(CASE WHEN x.code = '18' THEN x.dt END)  AS retour,
           MIN(CASE WHEN x.code <> '18' THEN x.dt END) AS depart
    FROM dispo di
    LEFT JOIN decs x ON x.PERS_ID = di.PERS_ID
                    AND x.dt > di.dt
                    AND x.code <> '07'
    GROUP BY di.PERS_ID, di.dt
)
SELECT EXTRACT(YEAR FROM su.dt)                                            AS annee_disponibilite,
       COUNT(*)                                                            AS agents,
       SUM(CASE WHEN su.retour IS NOT NULL THEN 1 ELSE 0 END)              AS revenus,
       SUM(CASE WHEN su.retour IS NULL
                 AND COALESCE(su.depart, TRUNC(p.DATE_DECES)) IS NOT NULL
                THEN 1 ELSE 0 END)                                         AS partis_avec_une_date,
       SUM(CASE WHEN su.retour IS NULL
                 AND COALESCE(su.depart, TRUNC(p.DATE_DECES)) IS NULL
                 AND NVL(p.ETAT_EMPLOYE, '-') = 'INACTIF'
                THEN 1 ELSE 0 END)                                         AS inactif_sans_depart_date,
       SUM(CASE WHEN su.retour IS NULL
                 AND COALESCE(su.depart, TRUNC(p.DATE_DECES)) IS NULL
                 AND NVL(p.ETAT_EMPLOYE, '-') <> 'INACTIF'
                THEN 1 ELSE 0 END)                                         AS encore_en_disponibilite
FROM suite su
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = su.PERS_ID
WHERE TO_CHAR(p.QUALITE) = 'A'
  AND su.dt >= DATE '2015-01-01'
GROUP BY EXTRACT(YEAR FROM su.dt)
ORDER BY 1;
