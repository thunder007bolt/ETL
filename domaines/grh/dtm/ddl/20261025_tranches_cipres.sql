-- =============================================================================
-- 25/10/2026 — Tranches d'âge (T58) et d'ancienneté (T61) conformes à l'Annuaire
-- CIPRES (pages 50 et 51)
-- Suivi : domaines/grh/dtm/SUIVI_MODIFICATIONS_DWH.md (modifications 8 et 9)
--
--   Âge (T58)       : moins de 20, 20-24, 25-29, 30-34, 35-39, 40-44, 45-49,
--                     50-54, 55-59, 60 et plus
--   Ancienneté (T61): moins de 1, 1-4, 5-9, 10-14, 15-19, 20-24, 25-29, 30-34,
--                     35-39, 40 et plus
--
-- Les requêtes ne changent pas : grh_effectif.sql rattache chaque agent à sa
-- tranche par AGE_MIN <= âge <= AGE_MAX (AGE_MAX vide pour la dernière tranche),
-- même principe pour l'ancienneté. Les bornes sont donc incluses.
-- Les anciennes tranches sont supprimées : en les gardant, un agent tomberait
-- dans deux tranches qui se chevauchent et serait compté deux fois.
-- À exécuter avec le user DTM (ou DBA), puis recharger DTM_GRH_EFFECTIF et
-- DTM_INDICATEURS_FINANCES_GRH. Les clichés déjà chargés gardent les
-- anciens codes de tranche.
-- =============================================================================


-- 0. Avant toute modification : structure et contenu actuels
--    Si la table a d'autres colonnes obligatoires que celles des INSERT
--    ci-dessous (un libellé par exemple), compléter les INSERT avant de lancer
--    l'étape 2.
SELECT table_name, column_name, data_type, data_length, nullable
FROM   all_tab_columns
WHERE  owner = 'DTM'
  AND  table_name IN ('DIM_GRH_TRANCHE_AGE', 'DIM_GRH_TRANCHE_ANCIENNETE')
ORDER  BY table_name, column_id;

SELECT * FROM DTM.DIM_GRH_TRANCHE_AGE        ORDER BY AGE_MIN;
SELECT * FROM DTM.DIM_GRH_TRANCHE_ANCIENNETE ORDER BY ANC_MIN;


-- 1. Sauvegarde des tables actuelles
CREATE TABLE DTM.DIM_GRH_TRANCHE_AGE_BAK20261025 AS SELECT * FROM DTM.DIM_GRH_TRANCHE_AGE;
CREATE TABLE DTM.DIM_GRH_TRANCHE_ANC_BAK20261025 AS SELECT * FROM DTM.DIM_GRH_TRANCHE_ANCIENNETE;


-- 2. Nouvelles tranches d'âge (T58)
DELETE FROM DTM.DIM_GRH_TRANCHE_AGE;
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('<20',    0,   19,  1);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('20-24', 20,   24,  2);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('25-29', 25,   29,  3);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('30-34', 30,   34,  4);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('35-39', 35,   39,  5);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('40-44', 40,   44,  6);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('45-49', 45,   49,  7);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('50-54', 50,   54,  8);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('55-59', 55,   59,  9);
INSERT INTO DTM.DIM_GRH_TRANCHE_AGE (TRANCHE_AGE_CODE, AGE_MIN, AGE_MAX, RANG) VALUES ('60+',   60, NULL, 10);


-- 3. Nouvelles tranches d'ancienneté (T61)
DELETE FROM DTM.DIM_GRH_TRANCHE_ANCIENNETE;
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('<1',     0,    0,  1);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('1-4',    1,    4,  2);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('5-9',    5,    9,  3);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('10-14', 10,   14,  4);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('15-19', 15,   19,  5);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('20-24', 20,   24,  6);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('25-29', 25,   29,  7);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('30-34', 30,   34,  8);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('35-39', 35,   39,  9);
INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE (TRANCHE_ANC_CODE, ANC_MIN, ANC_MAX, RANG) VALUES ('40+',   40, NULL, 10);

COMMIT;


-- 4. Contrôle : 10 tranches chacune, sans trou ni chevauchement
SELECT 'AGE' AS dimension, COUNT(*) AS tranches, MIN(AGE_MIN) AS borne_basse,
       SUM(CASE WHEN AGE_MAX IS NULL THEN 1 ELSE 0 END) AS sans_borne_haute
FROM DTM.DIM_GRH_TRANCHE_AGE
UNION ALL
SELECT 'ANCIENNETE', COUNT(*), MIN(ANC_MIN),
       SUM(CASE WHEN ANC_MAX IS NULL THEN 1 ELSE 0 END)
FROM DTM.DIM_GRH_TRANCHE_ANCIENNETE;
-- Attendu : AGE 10 · 0 · 1 ; ANCIENNETE 10 · 0 · 1.


-- Retour arrière :
-- DELETE FROM DTM.DIM_GRH_TRANCHE_AGE;
-- INSERT INTO DTM.DIM_GRH_TRANCHE_AGE SELECT * FROM DTM.DIM_GRH_TRANCHE_AGE_BAK20261025;
-- DELETE FROM DTM.DIM_GRH_TRANCHE_ANCIENNETE;
-- INSERT INTO DTM.DIM_GRH_TRANCHE_ANCIENNETE SELECT * FROM DTM.DIM_GRH_TRANCHE_ANC_BAK20261025;
-- COMMIT;
