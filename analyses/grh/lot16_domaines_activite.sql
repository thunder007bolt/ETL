-- =============================================================================
-- LOT 16 — Domaines d'activité des agents (T59)
-- Rédigé le 26/10/2026 — À EXÉCUTER sur USER_GRH.
-- Exécution : en mode script (F5) ; comptages seulement (aucune donnée
-- nominative).
-- Pourquoi ce lot : la question 11 du questionnaire range les codes de domaine
-- de la GRH dans les 7 domaines de l'Annuaire CIPRES. Il faut les libellés des
-- codes pour confirmer la correspondance, et vérifier que le domaine suit bien
-- la situation de l'agent.
-- Rappel : le domaine d'un agent est le champ CODE_DOMAINE_ACT de sa situation
-- (GRH_SITUATION) ; DTM_GRH_EFFECTIF prend celui de la dernière situation
-- commencée au plus tard le 31/12. Lot 6, Q37 : PIL 431 actifs, PRS 158,
-- REC 125, CPT 79, ACT 50, IMM 29, PREV 5, CONT 1.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q95 — Libellé de chaque domaine et nombre d'agents en activité
-- Objectif : lire les libellés officiels des codes, pour la correspondance avec
--            les domaines CIPRES (question 11).
-- Tableaux : T59.
-- Résultat : (à compléter)
-- -----------------------------------------------------------------------------
WITH derniere_situation AS (
    SELECT s.PERS_ID, s.CODE_DOMAINE_ACT,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT d.CODE_DOMAINE, d.LIB_DOMAINE, d.RANG_DOMAINE,
       COUNT(p.PERS_ID) AS agents_en_activite
FROM USER_GRH.GRH_DOMAINE_ACTIVITE d
LEFT JOIN derniere_situation ds ON ds.CODE_DOMAINE_ACT = d.CODE_DOMAINE AND ds.rn = 1
LEFT JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = ds.PERS_ID AND p.ETAT_EMPLOYE = 'ACTIF'
GROUP BY d.CODE_DOMAINE, d.LIB_DOMAINE, d.RANG_DOMAINE
ORDER BY d.RANG_DOMAINE;


-- -----------------------------------------------------------------------------
-- Q96 — Le domaine change-t-il avec la situation ?
-- Objectif : voir si le domaine est saisi à chaque situation (affectation,
--            nomination…) ou s'il reste figé, et s'il dépend de la nature de
--            l'unité (direction, service…), ce qui expliquerait les 49 % en
--            pilotage.
-- Tableaux : T59.
-- Résultat : (à compléter)
-- -----------------------------------------------------------------------------
WITH der AS (
    SELECT s.PERS_ID, s.CODE_DOMAINE_ACT, s.UA_CODE,
           ROW_NUMBER() OVER (PARTITION BY s.PERS_ID
                              ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
    FROM USER_GRH.GRH_SITUATION s
    WHERE s.DATE_DEBUT <= SYSDATE
),
nb_domaines AS (
    SELECT s.PERS_ID, COUNT(DISTINCT s.CODE_DOMAINE_ACT) AS domaines_differents
    FROM USER_GRH.GRH_SITUATION s
    GROUP BY s.PERS_ID
)
SELECT NVL(r.CODE_DOMAINE_ACT, '-') AS domaine,
       NVL(TO_CHAR(u.UA_NATURE), '-') AS nature_unite,
       COUNT(*) AS agents_en_activite,
       SUM(CASE WHEN n.domaines_differents > 1 THEN 1 ELSE 0 END) AS ont_change_de_domaine
FROM USER_GRH.GRH_PERSONNE p
JOIN der r ON r.PERS_ID = p.PERS_ID AND r.rn = 1
LEFT JOIN nb_domaines n ON n.PERS_ID = p.PERS_ID
LEFT JOIN USER_GRH.GRH_UNITE_ADMINISTRATIVE u ON u.UA_CODE = r.UA_CODE
WHERE p.ETAT_EMPLOYE = 'ACTIF'
GROUP BY NVL(r.CODE_DOMAINE_ACT, '-'), NVL(TO_CHAR(u.UA_NATURE), '-')
ORDER BY 1, 2;
