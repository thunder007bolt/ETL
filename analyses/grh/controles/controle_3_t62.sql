-- =============================================================================
-- CONTRÔLE 3 — La requête de T62 (personnel formé), exercice 2024, 17/11/2026
-- Requête contrôlée : analyses/grh/t62_requete_proposee.sql (jamais lancée).
-- Exécution : mode script (F5) dans SQL Developer, rien à saisir. Comptages seulement.
-- Lit LA SOURCE (USER_GRH), pas l'entrepôt : la requête proposée lit DWH et DTM, sa
-- copie ci-dessous lit les mêmes tables dans USER_GRH, sans condition de cliché.
-- Construit par analyses/grh/controles/construire.py à partir de la requête proposée :
-- ne pas modifier ce fichier à la main, relancer le script.
-- Coller le résultat sous la ligne « Résultat » de chaque contrôle.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- C1 — T62, exercice 2024 : la maquette
-- Attendu  : 409 permanents formés (employés 81, maîtrise 96, cadres moyens 181, cadres
--            supérieurs 51), 524 inscriptions, 2 avec promotion.
--            Annuaire CNSS 2024 (tableau 123) : 480 (132, 142, 118, 88), 6 avec promotion.
--            L'écart est le point 4 de la v2 ; Q149 et Q150 du lot 26 le creusent.
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT *
FROM (
    WITH
    parametres AS (
        SELECT TO_NUMBER(2024)                                  AS annee,
               TO_DATE(2024 || '0101', 'YYYYMMDD')              AS debut,
               TO_DATE(2024 || '1231', 'YYYYMMDD')              AS fin,
               'MOYEN'                                        AS categorie_05
        FROM DUAL
    ),
    inscriptions AS (
        SELECT i.PERS_ID, i.INS_ID,
               NVL(TO_CHAR(f.CATEGORIE), '-')                         AS finalite,
               CASE WHEN i.CLA_APRES IS NOT NULL AND i.CLA_AVANT IS NOT NULL
                     AND TO_CHAR(i.CLA_APRES) <> TO_CHAR(i.CLA_AVANT)
                    THEN 1 ELSE 0 END                                 AS promotion
        FROM USER_GRH.GRH_INSCRIPTION i
        JOIN USER_GRH.GRH_FORMATION f ON f.FORM_NO = i.FORM_NO
        CROSS JOIN parametres pr
        WHERE f.DATE_DEBUT >= pr.debut
          AND f.DATE_DEBUT <  pr.fin + 1
    ),
    formes AS (
        SELECT PERS_ID,
               MAX(promotion)                                         AS promotion,
               COUNT(*)                                               AS nb_inscriptions,
               MAX(CASE WHEN finalite = 'PERF'  THEN 1 ELSE 0 END)    AS perfectionnement,
               MAX(CASE WHEN finalite = 'DIPLO' THEN 1 ELSE 0 END)    AS diplomante
        FROM inscriptions
        WHERE PERS_ID IS NOT NULL
        GROUP BY PERS_ID
    ),
    situations AS (
        SELECT s.PERS_ID, s.NATURE_EMPLOIE, TO_CHAR(s.CATEGORIE) AS categorie,
               ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM USER_GRH.GRH_SITUATION s
        CROSS JOIN parametres pr
        WHERE 1 = 1
          AND s.DATE_DEBUT <= pr.fin
          AND s.PERS_ID IN (SELECT PERS_ID FROM formes)
    ),
    base AS (
        SELECT fo.PERS_ID, p.SEXE, fo.promotion, fo.nb_inscriptions,
               fo.perfectionnement, fo.diplomante,
               CASE
                   WHEN si.categorie IN ('01', '02') THEN 1
                   WHEN si.categorie = '03'          THEN 2
                   WHEN si.categorie = '04'          THEN 3
                   WHEN si.categorie = '05'          THEN CASE pr.categorie_05
                                                              WHEN 'SUPERIEUR' THEN 4
                                                              ELSE 3 END
                   WHEN si.categorie = '06'          THEN 4
                   ELSE                                   5
               END                                                    AS rang
        FROM formes fo
        CROSS JOIN parametres pr
        JOIN situations si ON si.PERS_ID = fo.PERS_ID AND si.rn = 1
        JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = fo.PERS_ID
                                    AND 1 = 1
        WHERE si.NATURE_EMPLOIE = 'PERMANENT'
          AND TO_CHAR(p.QUALITE) = 'A'
    ),
    comptes AS (
        SELECT rang,
               SUM(CASE WHEN promotion = 1 AND SEXE = 'M' THEN 1 ELSE 0 END) AS avec_promotion_h,
               SUM(CASE WHEN promotion = 1 AND SEXE = 'F' THEN 1 ELSE 0 END) AS avec_promotion_f,
               SUM(CASE WHEN promotion = 0 AND SEXE = 'M' THEN 1 ELSE 0 END) AS sans_promotion_h,
               SUM(CASE WHEN promotion = 0 AND SEXE = 'F' THEN 1 ELSE 0 END) AS sans_promotion_f,
               SUM(CASE WHEN SEXE = 'M' THEN 1 ELSE 0 END)                   AS hommes,
               SUM(CASE WHEN SEXE = 'F' THEN 1 ELSE 0 END)                   AS femmes,
               COUNT(*)                                                      AS total,
               SUM(nb_inscriptions)                                          AS inscriptions,
               SUM(perfectionnement)                                         AS perfectionnement,
               SUM(diplomante)                                               AS diplomante
        FROM base
        GROUP BY rang
    ),
    lignes AS (
        SELECT * FROM comptes
        UNION ALL
        SELECT 9, SUM(avec_promotion_h), SUM(avec_promotion_f), SUM(sans_promotion_h),
               SUM(sans_promotion_f), SUM(hommes), SUM(femmes), SUM(total),
               SUM(inscriptions), SUM(perfectionnement), SUM(diplomante)
        FROM comptes
    )
    SELECT pr.annee                                                   AS ANNEE,
           l.rang                                                     AS RANG,
           CASE l.rang WHEN 1 THEN 'Employés'
                       WHEN 2 THEN 'Agents de maîtrise'
                       WHEN 3 THEN 'Cadres moyens'
                       WHEN 4 THEN 'Cadres supérieurs'
                       WHEN 5 THEN 'Sans catégorie'
                       ELSE        'TOTAL' END                        AS CATEGORIE,
           l.avec_promotion_h                                         AS AVEC_PROMOTION_H,
           l.avec_promotion_f                                         AS AVEC_PROMOTION_F,
           l.sans_promotion_h                                         AS SANS_PROMOTION_H,
           l.sans_promotion_f                                         AS SANS_PROMOTION_F,
           l.hommes                                                   AS HOMMES,
           l.femmes                                                   AS FEMMES,
           l.total                                                    AS TOTAL,
           l.inscriptions                                             AS INSCRIPTIONS,
           l.perfectionnement                                         AS FORMES_PERFECTIONNEMENT,
           l.diplomante                                               AS FORMES_DIPLOMANTE,
           0                                                         AS CLICHE
    FROM lignes l
    CROSS JOIN parametres pr
    ORDER BY l.rang
)
ORDER BY RANG;
