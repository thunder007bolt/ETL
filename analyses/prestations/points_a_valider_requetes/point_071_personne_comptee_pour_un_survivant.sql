-- =============================================================================
-- Point 71. La personne comptée pour un survivant
-- Fiche : points_a_valider_v2.md, rubrique « À trancher en priorité ».
-- Ce que dit le point : « l'ayant droit est dans PRESTATION_ESP
--   (IND_ID_PREST), le titulaire dans DOSSIER (IND_ID) : c'est le mari décédé
--   pour 13 104 pensions de veuve sur 25 541. » ; « son tableau 77 range les
--   nouveaux rentiers selon qui ils sont, pas selon la victime ; 44 orphelins
--   de rente en 2024 dans la base pour 44 publiés. »
-- Requêtes :
--   1. Les pensions de survivant rangées dans le dossier du défunt.
--   2. Contrôle direct : le titulaire du dossier est-il l'ayant droit, ou une
--      autre personne, décédée ?
--   3. Les nouveaux rentiers survivants de 2024, face au tableau 77.
-- Reprise de : lot3b_controles_t25.sql, P51 (exécutée le 30/10/2026) pour la
--   requête 1 ; lot7_nouveaux_beneficiaires.sql, P82 (exécutée le
--   01/11/2026) pour la requête 3. La requête 2 est nouvelle.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t32_t41_requete_proposee.sql (blocs en_cours et
--   beneficiaire : la personne comptée est IND_ID_PREST, sauf en T41 où
--   c'est le titulaire du dossier) et
--   t42_t49_requete_proposee.sql (blocs pe, premiere et entree). Fiche des
--   règles : règle 63.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Pensions de survivant (RPSV veuve, RPSF veuf, RPSE orphelin, RPA
-- ascendant) rangées dans un dossier qui porte aussi un droit propre de
-- l'assuré (RPV, RPRA, RPI ou RAV) : le dossier du défunt.
-- -----------------------------------------------------------------------------
WITH dossier_v AS (
    SELECT DOS_CODE,
           MAX(CASE WHEN TPE_CODE IN ('RPV', 'RPRA', 'RPI', 'RAV')
                    THEN 1 ELSE 0 END)                            AS a_droit_propre
    FROM USER_DWH.PRESTATION_ESP
    GROUP BY DOS_CODE
)
SELECT pe.TPE_CODE                                                AS code,
       COUNT(*)                                                   AS nb_pensions,
       SUM(dv.a_droit_propre)                                     AS dans_le_dossier_du_defunt
FROM USER_DWH.PRESTATION_ESP pe
JOIN USER_DWH.DOSSIER d  ON d.DOS_CODE  = pe.DOS_CODE
JOIN dossier_v       dv ON dv.DOS_CODE = pe.DOS_CODE
WHERE d.TDOS_CODE = 'V'
  AND pe.TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA')
GROUP BY pe.TPE_CODE
ORDER BY nb_pensions DESC;

-- Résultat obtenu (P51, exécutée le 30/10/2026) :
--   Code   Pensions   Dans le dossier du défunt
--   RPSE   29 689      9 977   (34 %)
--   RPSV   25 541     13 104   (51 %)
--   RPSF      424        183
--   RPA        81          7
-- Lecture : 13 104 pensions de veuve sur 25 541 sont dans le dossier du mari,
--   dont il est le titulaire. Compter le titulaire ferait compter le mari.


-- -----------------------------------------------------------------------------
-- Requête 2. Contrôle direct, pour les pensions (TDOS_CODE V) et les rentes
-- (A) de survivant : le titulaire du dossier (DOSSIER.IND_ID) est-il la
-- personne qui touche (PRESTATION_ESP.IND_ID_PREST) ?
-- -----------------------------------------------------------------------------
SELECT pe.TPE_CODE                                                AS code,
       COUNT(*)                                                   AS nb_prestations,
       SUM(CASE WHEN d.IND_ID = pe.IND_ID_PREST
                THEN 1 ELSE 0 END)                                AS titulaire_est_l_ayant_droit,
       SUM(CASE WHEN d.IND_ID <> pe.IND_ID_PREST
                THEN 1 ELSE 0 END)                                AS titulaire_autre_personne,
       SUM(CASE WHEN d.IND_ID <> pe.IND_ID_PREST
                 AND t.IND_DATE_DECES IS NOT NULL
                THEN 1 ELSE 0 END)                                AS titulaire_autre_et_decede,
       SUM(CASE WHEN d.IND_ID IS NULL OR pe.IND_ID_PREST IS NULL
                THEN 1 ELSE 0 END)                                AS identifiant_manquant
FROM USER_DWH.PRESTATION_ESP pe
JOIN      USER_DWH.DOSSIER  d ON d.DOS_CODE = pe.DOS_CODE
LEFT JOIN USER_DWH.INDIVIDU t ON t.IND_ID   = d.IND_ID
WHERE pe.TPE_CODE IN ('RPSV', 'RPSF', 'RPSE', 'RPA', 'ARSV', 'ARSF', 'ARSE', 'ARSA')
GROUP BY pe.TPE_CODE
ORDER BY nb_prestations DESC;

-- Résultat : pas encore exécutée.
-- Lecture attendue : si « titulaire autre personne et décédé » est élevé, la
--   lecture à la lettre de « Idem. Tab 41 » (compter le titulaire) ferait
--   compter des morts, avec l'âge et le sexe du défunt.


-- -----------------------------------------------------------------------------
-- Requête 3. Les nouveaux rentiers survivants de 2024, face au tableau 77
-- T46 orphelins (ARSE), T47 ascendants (ARSA), T49 veuves et veufs (ARSV,
-- ARSF). On compte l'ayant droit (IND_ID_PREST) : personnes dont une rente
-- est saisie en 2024 (PE_DATE_INSERT), dont c'est la première rente du
-- tableau, et personnes notifiées en 2024 (PE_DATE_NOTIFICATION).
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T46' AS tableau, 'ARSE' AS code FROM DUAL
    UNION ALL SELECT 'T47', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T49', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T49', 'ARSF' FROM DUAL
),
publie AS (
    -- Annuaire CNSS 2024, tableau 77 (p. 61) : nouveaux bénéficiaires de
    -- rentes d'ayant droit en 2024, selon la nature du bénéficiaire.
    SELECT 'T46' AS tableau, 'Orphelins' AS nature, 44 AS publie_2024 FROM DUAL
    UNION ALL SELECT 'T47', 'Ascendants',      13 FROM DUAL
    UNION ALL SELECT 'T49', 'Veuves et veufs',  4 FROM DUAL
),
pe AS (
    SELECT pm.tableau,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           TRUNC(p.PE_DATE_NOTIFICATION)                          AS date_notification
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre        pm ON pm.code    = p.TPE_CODE
    JOIN USER_DWH.DOSSIER d  ON d.DOS_CODE = p.DOS_CODE
),
premiere AS (
    SELECT tableau, IND_ID_PREST, MIN(date_saisie) AS premiere_saisie
    FROM pe
    GROUP BY tableau, IND_ID_PREST
),
compte AS (
    SELECT pe.tableau,
           COUNT(DISTINCT CASE WHEN EXTRACT(YEAR FROM pe.date_saisie) = 2024
                               THEN pe.IND_ID_PREST END)          AS personnes_saisies,
           COUNT(DISTINCT CASE WHEN EXTRACT(YEAR FROM pe.date_saisie) = 2024
                                AND EXTRACT(YEAR FROM pr.premiere_saisie) = 2024
                               THEN pe.IND_ID_PREST END)          AS premiere_fois,
           COUNT(DISTINCT CASE WHEN EXTRACT(YEAR FROM pe.date_notification) = 2024
                               THEN pe.IND_ID_PREST END)          AS personnes_notifiees
    FROM pe
    JOIN premiere pr ON pr.tableau      = pe.tableau
                    AND pr.IND_ID_PREST = pe.IND_ID_PREST
    GROUP BY pe.tableau
)
SELECT pu.tableau,
       pu.nature,
       c.personnes_saisies,
       c.premiere_fois,
       c.personnes_notifiees,
       pu.publie_2024
FROM publie pu
LEFT JOIN compte c ON c.tableau = pu.tableau
ORDER BY pu.tableau;

-- Résultat obtenu (P82, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P82_entrees_2020_2025.tsv) :
--   Tableau  Nature            Saisies 2024   Première fois   Notifiées 2024   Publiés (tableau 77)
--   T46      Orphelins         44             44              47               44
--   T47      Ascendants        15             15              15               13
--   T49      Veuves et veufs    4              4               4                4
-- Lecture : l'annuaire compte les ayants droit selon ce qu'ils sont
--   (orphelins, ascendants, veuves), pas la victime : 44 orphelins dans la
--   base pour 44 publiés, 4 veuves pour 4. Les ascendants font 15 pour 13
--   (écart à expliquer, regles_t42_t49_nouveaux.md, Q7).
