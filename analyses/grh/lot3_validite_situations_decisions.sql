-- =============================================================================
-- LOT 3 — Sens de VALIDE = 'E', date de sortie, catégories de décisions
-- Exécuté le 21/10/2026 sur USER_GRH.
-- Tableaux CIPRES visés : T60 ; indirectement T58, T59, T61, T65.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q13 — Les mouvements E sont-ils appliqués ?
-- Objectif : savoir si un mouvement E est suivi d'une situation de même nature
--            (à ±60 jours), comme un mouvement O.
-- Résultat : O 966 / 4 026 (24 %) · N 49 / 342 (14 %) · E 164 / 1 595 (10 %) ·
--            A 0 / 2 · vide 0 / 7.
-- Constat  : E se comporte moins comme un mouvement appliqué que N ; le filtre
--            VALIDE = 'O' est justifié (à confirmer par la DRH).
-- -----------------------------------------------------------------------------
SELECT m.VALIDE, COUNT(*) AS nb,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_GRH.GRH_SITUATION s
                             WHERE s.PERS_ID = m.PERS_ID
                               AND UPPER(TRIM(s.NATURE_SITUATION)) = UPPER(TRIM(m.NATURE_MVT))
                               AND ABS(s.DATE_DEBUT - m.DATE_DEBUT) <= 60)
                THEN 1 ELSE 0 END) AS avec_situation_proche
FROM USER_GRH.GRH_MOUVEMENT m
WHERE m.DATE_DEBUT IS NOT NULL
GROUP BY m.VALIDE;


-- -----------------------------------------------------------------------------
-- Q14 — Dernière situation de chaque agent : statut, état, date de fin
-- Objectif : savoir si la date de fin de la dernière situation peut servir de
--            date de sortie.
-- Résultat : A/ACTIF 875 · A/INACTIF 691 (dont 26 avec date de décès) ·
--            R/INACTIF 584 (aucune date de fin) · Y/INACTIF 409 (171 dates de
--            fin, 32 dates de décès) · X/INACTIF 387 · A/vide 56 · autres 56.
--            Environ 122 agents n'ont aucune situation datée avant aujourd'hui.
-- Constat  : pas de date de sortie exploitable ; statut non mis à jour au départ.
-- -----------------------------------------------------------------------------
WITH der AS (
  SELECT s.PERS_ID, s.STATUT_EMPLOYE, s.DATE_FIN,
         ROW_NUMBER() OVER (PARTITION BY s.PERS_ID ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
  FROM USER_GRH.GRH_SITUATION s
  WHERE s.DATE_DEBUT <= SYSDATE
)
SELECT d.STATUT_EMPLOYE, p.ETAT_EMPLOYE, COUNT(*) AS nb_agents,
       COUNT(d.DATE_FIN) AS avec_date_fin, COUNT(p.DATE_DECES) AS avec_date_deces,
       MIN(EXTRACT(YEAR FROM d.DATE_FIN)) AS an_fin_min,
       MAX(EXTRACT(YEAR FROM d.DATE_FIN)) AS an_fin_max
FROM der d
JOIN USER_GRH.GRH_PERSONNE p ON p.PERS_ID = d.PERS_ID
WHERE d.rn = 1
GROUP BY d.STATUT_EMPLOYE, p.ETAT_EMPLOYE
ORDER BY nb_agents DESC;


-- -----------------------------------------------------------------------------
-- Q15 — Décisions RH par catégorie
-- Objectif : trouver les motifs de départ et d'entrée datés.
-- Résultat : 29 115 décisions. Utiles pour T60 : 02 admission à la retraite
--            (838), 51 engagement d'agent (310), 50 engagement temporaire
--            (1 504), 18 remise en activité (82), 54 licenciement (28),
--            36 démission (22), 52 rupture de contrat (1), 35 indemnité de fin
--            de contrat (397), 14 mise à disposition (59), 07 mise en
--            disponibilité (54), 75 frais funéraires (3), 34 liquidation des
--            droits (213). Annulations 40 (83), modifications 41 (665).
--            Dates de vigueur aberrantes : années 5, 200 à 209.
-- -----------------------------------------------------------------------------
SELECT d.CDEC_CODE, c.OBJET, COUNT(*) AS nb,
       MIN(EXTRACT(YEAR FROM d.DATE_VIGUEUR)) AS an_min,
       MAX(EXTRACT(YEAR FROM d.DATE_VIGUEUR)) AS an_max
FROM USER_GRH.GRH_DECISION d
LEFT JOIN USER_GRH.GRH_CAT_DECISION c ON c.CDEC_CODE = d.CDEC_CODE
GROUP BY d.CDEC_CODE, c.OBJET
ORDER BY nb DESC;
