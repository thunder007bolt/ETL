-- =============================================================================
-- LOT 1 — Profilage de USER_GRH.GRH_MOUVEMENT
-- Exécuté le 21/10/2026 sur USER_GRH (base source GRH), par l'équipe BI.
-- Tableau CIPRES visé : T60 (mouvement du personnel ; « T3 » dans la doc DataMart).
-- Objectif du lot : vérifier que la table source de DTM_GRH_MOUVEMENT contient
-- bien les entrées et sorties du personnel, et que ses jointures sont fiables.
-- Toutes les requêtes ne renvoient que des comptages (aucune donnée nominative).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q01 — Structure des colonnes de mouvement
-- Objectif : connaître la longueur réelle de NATURE_MVT (codes tronqués ?) et
--            chercher une colonne « sens » dans le schéma.
-- Résultat : NATURE_MVT = VARCHAR2(3), alors que CODE_TYPE_MVT = VARCHAR2(10).
--            Aucune colonne de sens hors du référentiel GRH_SENS_MOUVEMENT.
-- Constat  : les codes de plus de 3 caractères (RECRU, PROMO, FIN_DETACH…)
--            ne peuvent pas être saisis dans GRH_MOUVEMENT.
-- -----------------------------------------------------------------------------
SELECT table_name, column_name, data_type, data_length, nullable
FROM ALL_TAB_COLUMNS
WHERE owner = 'USER_GRH'
  AND (table_name IN ('GRH_MOUVEMENT','GRH_TYPE_MOUVEMENT','GRH_NATURE_MOUVEMENT','GRH_SENS_MOUVEMENT')
       OR column_name LIKE '%SENS%')
ORDER BY table_name, column_id;


-- -----------------------------------------------------------------------------
-- Q02 — Référentiels de mouvement et valeurs du sexe
-- Objectif : vérifier si NOM existe dans le référentiel, et quelles valeurs
--            prend le sexe.
-- Résultat : 18 types de mouvement. Ceux du domaine ALL (DEBUT, RECRU,
--            AUTRE_ENT, RET, DEC, AUTRE_SORT) correspondent aux lignes CIPRES
--            de T60 ; ceux du domaine CNSS sont un détail interne.
--            Pas de NOM. Sens : CESSA, PRISE. Sexe : M 1 857, F 1 323.
-- -----------------------------------------------------------------------------
SELECT t.CODE_TYPE_MVT, t.LIB_TYPE_MVT, t.CODE_NAT_MVT, n.LIB_NAT_MVT, t.DOMAINE_TYPE_MVT, t.RANG_TYPE_MVT
FROM USER_GRH.GRH_TYPE_MOUVEMENT t
LEFT JOIN USER_GRH.GRH_NATURE_MOUVEMENT n ON n.CODE_NAT_MVT = t.CODE_NAT_MVT
ORDER BY t.CODE_NAT_MVT, t.RANG_TYPE_MVT;

SELECT * FROM USER_GRH.GRH_SENS_MOUVEMENT;

SELECT SEXE, COUNT(*) AS nb FROM USER_GRH.GRH_PERSONNE GROUP BY SEXE;


-- -----------------------------------------------------------------------------
-- Q03 — Correspondance des codes de mouvement, par valeur de VALIDE
-- Objectif : repérer les codes non reconnus, le volume de NOM et le poids de
--            chaque valeur de VALIDE.
-- Résultat : 6 127 lignes, dont 4 040 validées (O). Codes présents : AFF
--            (2 792 validés), NOM (812, sans correspondance), MUT (323),
--            REM (63), DIS (30), DET (7), vide (13). Aucun RECRU, RET, DEC,
--            LIC, DEM. VALIDE : O 4 040, E 1 621, N 456, A 2, vide 8.
-- Constat  : la table ne contient presque que des mouvements internes.
-- -----------------------------------------------------------------------------
SELECT UPPER(TRIM(m.NATURE_MVT)) AS code_source, m.VALIDE,
       tm.CODE_TYPE_MVT, tm.LIB_TYPE_MVT, tm.CODE_NAT_MVT,
       COUNT(*) AS nb,
       MIN(EXTRACT(YEAR FROM m.DATE_DEBUT)) AS an_min,
       MAX(EXTRACT(YEAR FROM m.DATE_DEBUT)) AS an_max
FROM USER_GRH.GRH_MOUVEMENT m
LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm
       ON tm.CODE_TYPE_MVT = CASE UPPER(TRIM(m.NATURE_MVT))
                               WHEN 'DIS' THEN 'DISP' WHEN 'DET' THEN 'DETAC'
                               ELSE UPPER(TRIM(m.NATURE_MVT)) END
GROUP BY UPPER(TRIM(m.NATURE_MVT)), m.VALIDE, tm.CODE_TYPE_MVT, tm.LIB_TYPE_MVT, tm.CODE_NAT_MVT
ORDER BY code_source, m.VALIDE;


-- -----------------------------------------------------------------------------
-- Q04 — Dates de début hors bornes (mouvements validés)
-- Objectif : détecter les dates aberrantes ou futures.
-- Résultat : 4 026 dates correctes, du 20/08/1975 au 19/01/2026 ; 14 dates vides.
-- Constat  : aucun mouvement validé après le 19/01/2026 (base figée ou saisie
--            arrêtée ? question DSI).
-- -----------------------------------------------------------------------------
SELECT tranche, COUNT(*) AS nb, MIN(DATE_DEBUT) AS dmin, MAX(DATE_DEBUT) AS dmax
FROM (
  SELECT DATE_DEBUT,
         CASE WHEN DATE_DEBUT IS NULL THEN 'NULL'
              WHEN EXTRACT(YEAR FROM DATE_DEBUT) < 1950 THEN '< 1950'
              WHEN EXTRACT(YEAR FROM DATE_DEBUT) > EXTRACT(YEAR FROM SYSDATE) THEN 'futur'
              ELSE 'ok' END AS tranche
  FROM USER_GRH.GRH_MOUVEMENT
  WHERE VALIDE = 'O'
)
GROUP BY tranche;


-- -----------------------------------------------------------------------------
-- Q05 — Lien mouvement → situation → personne, par nature
-- Objectif : vérifier les jointures de la requête DTM (qualification, sexe).
-- Résultat : toutes les jointures sont complètes (SITU_ID toujours renseigné,
--            même agent, qualification présente et connue, sexe M ou F).
--            La situation liée est antérieure au mouvement dans environ 2 cas
--            sur 3 et postérieure dans 1 cas sur 4.
-- -----------------------------------------------------------------------------
WITH mv AS (
  SELECT x.MVT_ID, x.PERS_ID, x.SITU_ID, x.DATE_DEBUT,
         CASE UPPER(TRIM(x.NATURE_MVT)) WHEN 'DIS' THEN 'DISP' WHEN 'DET' THEN 'DETAC'
              ELSE UPPER(TRIM(x.NATURE_MVT)) END AS CODE_TYPE_MVT
  FROM USER_GRH.GRH_MOUVEMENT x
  WHERE x.VALIDE = 'O'
)
SELECT tm.CODE_NAT_MVT,
       COUNT(*) AS nb_mvt,
       SUM(CASE WHEN mv.SITU_ID IS NULL THEN 1 ELSE 0 END)                             AS sans_situ_id,
       SUM(CASE WHEN mv.SITU_ID IS NOT NULL AND s.SITU_ID IS NULL THEN 1 ELSE 0 END)    AS situ_introuvable,
       SUM(CASE WHEN s.PERS_ID <> mv.PERS_ID THEN 1 ELSE 0 END)                        AS situ_autre_agent,
       SUM(CASE WHEN s.DATE_DEBUT = mv.DATE_DEBUT THEN 1 ELSE 0 END)                   AS situ_meme_date,
       SUM(CASE WHEN s.DATE_DEBUT < mv.DATE_DEBUT THEN 1 ELSE 0 END)                   AS situ_anterieure,
       SUM(CASE WHEN s.DATE_DEBUT > mv.DATE_DEBUT THEN 1 ELSE 0 END)                   AS situ_posterieure,
       SUM(CASE WHEN s.QUAL_CODE IS NULL THEN 1 ELSE 0 END)                            AS qual_null,
       SUM(CASE WHEN s.QUAL_CODE IS NOT NULL AND q.QUAL_CODE IS NULL THEN 1 ELSE 0 END) AS qual_hors_ref,
       SUM(CASE WHEN p.PERS_ID IS NULL THEN 1 ELSE 0 END)                              AS pers_introuvable,
       SUM(CASE WHEN p.SEXE IS NULL OR p.SEXE NOT IN ('M','F') THEN 1 ELSE 0 END)      AS sexe_hors_mf
FROM mv
LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm ON tm.CODE_TYPE_MVT = mv.CODE_TYPE_MVT
LEFT JOIN USER_GRH.GRH_SITUATION      s  ON s.SITU_ID  = mv.SITU_ID
LEFT JOIN USER_GRH.GRH_PERSONNE       p  ON p.PERS_ID  = mv.PERS_ID
LEFT JOIN USER_GRH.GRH_QUALIFICATION  q  ON q.QUAL_CODE = s.QUAL_CODE
GROUP BY tm.CODE_NAT_MVT;


-- -----------------------------------------------------------------------------
-- Q06 — Doubles saisies (même agent, même code, même date, validés)
-- Objectif : mesurer les mouvements comptés deux fois dans NB_MOUVEMENTS.
-- Résultat : 25 paires, soit 25 mouvements en trop (0,6 %).
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS groupes_doublons, SUM(nb - 1) AS mvt_en_trop
FROM (SELECT PERS_ID, UPPER(TRIM(NATURE_MVT)) AS code, DATE_DEBUT, COUNT(*) AS nb
      FROM USER_GRH.GRH_MOUVEMENT
      WHERE VALIDE = 'O'
      GROUP BY PERS_ID, UPPER(TRIM(NATURE_MVT)), DATE_DEBUT
      HAVING COUNT(*) > 1);


-- -----------------------------------------------------------------------------
-- Q07 — Agents ayant plusieurs types de mouvement de même nature la même année
-- Objectif : mesurer le double comptage si OBIEE additionne NB_AGENTS sur les types.
-- Résultat : 155 agents-années au total, uniquement en INTERNE (AFF + MUT),
--            jusqu'à 40 en 2016, 27 en 2017, 24 en 2024.
-- Note     : la première version utilisait FETCH FIRST et a échoué (ORA-00933) :
--            la base est antérieure à Oracle 12c, on limite avec ROWNUM.
-- -----------------------------------------------------------------------------
SELECT * FROM (
  SELECT annee, CODE_NAT_MVT, COUNT(*) AS agents_multi_types
  FROM (SELECT EXTRACT(YEAR FROM m.DATE_DEBUT) AS annee, tm.CODE_NAT_MVT, m.PERS_ID
        FROM USER_GRH.GRH_MOUVEMENT m
        LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm
               ON tm.CODE_TYPE_MVT = CASE UPPER(TRIM(m.NATURE_MVT))
                                       WHEN 'DIS' THEN 'DISP' WHEN 'DET' THEN 'DETAC'
                                       ELSE UPPER(TRIM(m.NATURE_MVT)) END
        WHERE m.VALIDE = 'O' AND m.DATE_DEBUT IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM m.DATE_DEBUT), tm.CODE_NAT_MVT, m.PERS_ID
        HAVING COUNT(DISTINCT UPPER(TRIM(m.NATURE_MVT))) > 1)
  GROUP BY annee, CODE_NAT_MVT
  ORDER BY annee DESC
) WHERE ROWNUM <= 30;
