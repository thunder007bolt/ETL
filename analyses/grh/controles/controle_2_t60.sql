-- =============================================================================
-- CONTRÔLE 2 — La requête de T60 (mouvement du personnel), 21/11/2026
-- Requête contrôlée : analyses/grh/t60_requete_proposee.sql (version du 21/11/2026,
-- socle commun avec l'effectif ; absence 07 = HORS_EFFECTIF).
-- Exécution : mode script (F5) dans SQL Developer, rien à saisir. Comptages seulement.
-- Lit LA SOURCE (USER_GRH), pas l'entrepôt : la requête proposée lit DWH et DTM, sa
-- copie ci-dessous lit les mêmes tables dans USER_GRH, sans condition de cliché.
-- Construit par analyses/grh/controles/construire.py à partir de la requête proposée :
-- ne pas modifier ce fichier à la main, relancer le script.
-- Coller le résultat sous la ligne « Résultat » de chaque contrôle.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- C1 — T60, permanents, 2020 à 2025 : la maquette
-- Attendu  : début · recrut · autres_ent · retraite · deces · autres_sort · fin
--            2024 : environ 672 · 104 · 4 · 25 · 5 · 9 · 741.
--            Annuaire CNSS 2024 (tableau 121) : 767 · 104 · 7 · 23 · 3 · 94 · 758.
--            Chaque année, la fin doit être le début de l'année suivante.
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS,
       SUM(CASE WHEN CODE_TYPE_MVT = 'DEBUT' THEN NB_AGENTS ELSE 0 END) AS debut,
       SUM(CASE WHEN CODE_TYPE_MVT = 'RECRU' THEN NB_AGENTS ELSE 0 END) AS recrut,
       SUM(CASE WHEN CODE_NAT_MVT = 'ENTREES' AND CODE_TYPE_MVT <> 'RECRU'
                THEN NB_AGENTS ELSE 0 END)                             AS autres_ent,
       SUM(CASE WHEN CODE_TYPE_MVT = 'RET' THEN NB_AGENTS ELSE 0 END)   AS retraite,
       SUM(CASE WHEN CODE_TYPE_MVT = 'DEC' THEN NB_AGENTS ELSE 0 END)   AS deces,
       SUM(CASE WHEN CODE_NAT_MVT = 'SORTIES'
                 AND CODE_TYPE_MVT NOT IN ('RET', 'DEC')
                THEN NB_AGENTS ELSE 0 END)                             AS autres_sort,
       SUM(CASE WHEN CODE_NAT_MVT IN ('INITIAL', 'ENTREES') THEN NB_AGENTS
                WHEN CODE_NAT_MVT = 'SORTIES' THEN -NB_AGENTS
                ELSE 0 END)                                            AS fin
FROM (
    WITH
    parametres AS (
        SELECT
            'PRESENT'               AS fin_contrat_inconnue,
            18                      AS duree_contrat_mois,
            'PRESENT'               AS fiche_sans_etat,
            24                      AS fiche_sans_etat_mois,
            'JAMAIS_COMPTE'         AS inactif_sans_depart,
            'EMBAUCHE'              AS ordre_date_entree,
            'NON'                   AS passages_annee,
            'COMME_AVANT'           AS mouvements_internes
        FROM DUAL
    ),
    decisions_depart AS (
        SELECT '02' AS code, 'RET' AS type_mvt, 'PREMIER_JOUR_PARTI' AS date_effet, 'TOUTES' AS retenir,
               'admission à la retraite' AS libelle                                             FROM DUAL UNION ALL
        SELECT '34', 'DEC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'liquidation des droits après décès' FROM DUAL UNION ALL
        SELECT '35', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'fin de contrat'                     FROM DUAL UNION ALL
        SELECT '36', 'DEM',       'PREMIER_JOUR_PARTI', 'TOUTES', 'démission'                          FROM DUAL UNION ALL
        SELECT '52', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'rupture du contrat'                 FROM DUAL UNION ALL
        SELECT '54', 'LIC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'licenciement'                       FROM DUAL
    ),
    absences AS (
        SELECT '07' AS code, 'HORS_EFFECTIF' AS effet, 'RETIREE' AS anciennete, 'DISP' AS type_mvt,
               'mise en disponibilité' AS libelle                                 FROM DUAL UNION ALL
        SELECT '14', 'PRESENT',       'COMPTEE', 'MISE_DIS', 'mise à disposition' FROM DUAL
    ),
    codes_decision AS (
        SELECT '40' AS annulation,
               '50' AS engagement_temporaire,
               '51' AS engagement_permanent,
               '18' AS remise_en_activite,
               '37' AS classement_detache
        FROM DUAL
    ),
    annees AS (
        SELECT 1899 + LEVEL AS annee
        FROM DUAL
        CONNECT BY LEVEL <= EXTRACT(YEAR FROM SYSDATE) - 1900
    ),
    dates AS (
        SELECT TO_DATE(TO_CHAR(a.annee) || '1231', 'YYYYMMDD')            AS date_arrete
        FROM annees a
        WHERE a.annee >= (SELECT MIN(EXTRACT(YEAR FROM s.DATE_DEBUT))
                          FROM USER_GRH.GRH_SITUATION s
                          WHERE 1 = 1
                            AND s.DATE_DEBUT IS NOT NULL
                            AND EXTRACT(YEAR FROM s.DATE_DEBUT) >= 1900)
    ),
    exercices AS (
        SELECT EXTRACT(YEAR FROM da.date_arrete)                          AS annee,
               ADD_MONTHS(da.date_arrete, -12)                            AS d0,
               da.date_arrete                                             AS d1
        FROM dates da
        WHERE ADD_MONTHS(da.date_arrete, -12) IN (SELECT date_arrete FROM dates)
    ),
    personnes AS (
        SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS,
               p.DATE_EMBAUCHE                                        AS date_embauche,
               p.DATE_PRISE_SERVICE                                   AS date_prise_service,
               NVL(p.ETAT_EMPLOYE, '-')                               AS etat,
               TO_CHAR(p.QUALITE)                                     AS qualite,
               CASE pr.ordre_date_entree
                   WHEN 'PRISE_SERVICE' THEN COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
                   ELSE                      COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
               END                                                    AS date_entree
        FROM USER_GRH.GRH_PERSONNE p
        CROSS JOIN parametres pr
        WHERE 1 = 1
    ),
    decisions AS (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               TO_CHAR(d.CDEC_CODE)       AS code,
               d.DATE_VIGUEUR             AS date_decision
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp
               ON dp.DECI_NO = d.DECI_NO
              AND 1 = 1
        CROSS JOIN codes_decision cd
        WHERE 1 = 1
          AND d.DATE_VIGUEUR IS NOT NULL
          AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
               OR TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM absences)
               OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                           cd.remise_en_activite, cd.classement_detache))
          AND NOT EXISTS (SELECT 1
                          FROM USER_GRH.GRH_DECISION a
                          WHERE 1 = 1
                            AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    ),
    departs AS (
        SELECT x.PERS_ID,
               x.date_decision
                   + CASE x.date_effet WHEN 'DERNIER_JOUR_TRAVAILLE' THEN 1 ELSE 0 END AS date_depart,
               x.type_mvt,
               'DECISION_' || x.code                                  AS motif
        FROM (SELECT dc.PERS_ID, dc.code, dc.date_decision,
                     dd.type_mvt, dd.date_effet, dd.retenir,
                     ROW_NUMBER() OVER (PARTITION BY dc.PERS_ID, dc.code
                                        ORDER BY dc.date_decision DESC) AS rang_fin
              FROM decisions dc
              JOIN decisions_depart dd ON dd.code = dc.code
              WHERE dc.PERS_ID IS NOT NULL) x
        WHERE x.retenir = 'TOUTES'
           OR x.rang_fin = 1
        UNION ALL
        SELECT PERS_ID, DATE_DECES, 'DEC', 'DATE_DECES'
        FROM USER_GRH.GRH_PERSONNE
        WHERE 1 = 1
          AND DATE_DECES IS NOT NULL
    ),
    premier_depart AS (
        SELECT PERS_ID, date_depart, type_mvt, motif
        FROM (SELECT x.PERS_ID, x.date_depart, x.type_mvt, x.motif,
                     ROW_NUMBER() OVER (PARTITION BY x.PERS_ID
                                        ORDER BY x.date_depart, x.motif) AS rn
              FROM departs x)
        WHERE rn = 1
    ),
    engagements AS (
        SELECT e.PERS_ID, e.date_decision AS debut,
               LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                           ORDER BY e.date_decision) AS debut_suivant
        FROM decisions e
        CROSS JOIN codes_decision cd
        WHERE e.PERS_ID IS NOT NULL
          AND e.code = cd.engagement_temporaire
    ),
    contrats AS (
        SELECT x.PERS_ID, x.debut,
               CASE
                   WHEN x.fin_base < DATE '9999-12-31'       THEN x.fin_base
                   WHEN pr.fin_contrat_inconnue = 'DUREE'    THEN ADD_MONTHS(x.debut, pr.duree_contrat_mois)
                   WHEN pr.fin_contrat_inconnue = 'FICHE'
                    AND p.etat = 'INACTIF'                   THEN CAST(NULL AS DATE)
                   ELSE                                           x.fin_base
               END                                                    AS fin
        FROM (SELECT g.PERS_ID, g.debut,
                     LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                           NVL(g.debut_suivant, DATE '9999-12-31'))   AS fin_base
              FROM engagements g
              LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                                   AND dep.date_depart > g.debut
              GROUP BY g.PERS_ID, g.debut, g.debut_suivant) x
        JOIN personnes p ON p.PERS_ID = x.PERS_ID
        CROSS JOIN parametres pr
    ),
    temporaires_sous_contrat AS (
        SELECT DISTINCT da.date_arrete, c.PERS_ID
        FROM dates da
        JOIN contrats c ON c.debut       <= da.date_arrete
                       AND da.date_arrete <  c.fin
    ),
    remises AS (
        SELECT da.date_arrete, d.PERS_ID, MAX(d.date_decision) AS date_remise
        FROM dates da
        JOIN decisions d ON d.date_decision <= da.date_arrete
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.remise_en_activite
        GROUP BY da.date_arrete, d.PERS_ID
    ),
    absences_en_cours AS (
        SELECT x.date_arrete, x.PERS_ID, x.code, x.effet, x.type_mvt, x.date_absence
        FROM (SELECT da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt,
                     MAX(d.date_decision) AS date_absence
              FROM dates da
              JOIN decisions d ON d.date_decision <= da.date_arrete
              JOIN absences ab ON ab.code = d.code
              WHERE d.PERS_ID IS NOT NULL
              GROUP BY da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt) x
        LEFT JOIN remises r ON r.date_arrete = x.date_arrete
                           AND r.PERS_ID     = x.PERS_ID
        WHERE x.date_absence > NVL(r.date_remise, DATE '1900-01-01')
    ),
    positions AS (
        SELECT date_arrete, PERS_ID,
               MAX(CASE WHEN effet = 'HORS_EFFECTIF' THEN 1 ELSE 0 END)              AS hors_effectif,
               MAX(CASE WHEN effet IN ('HORS_EFFECTIF', 'INACTIF') THEN 1 ELSE 0 END) AS inactif
        FROM absences_en_cours
        GROUP BY date_arrete, PERS_ID
    ),
    absence_sans_retour AS (
        SELECT x.PERS_ID, x.date_absence
        FROM (SELECT d.PERS_ID, MAX(d.date_decision) AS date_absence
              FROM decisions d
              JOIN absences ab ON ab.code = d.code
              WHERE d.PERS_ID IS NOT NULL
                AND ab.effet = 'HORS_EFFECTIF'
              GROUP BY d.PERS_ID) x
        WHERE NOT EXISTS (SELECT 1
                          FROM decisions r, codes_decision cd
                          WHERE r.PERS_ID       =  x.PERS_ID
                            AND r.code          =  cd.remise_en_activite
                            AND r.date_decision >= x.date_absence)
          AND NOT EXISTS (SELECT 1
                          FROM departs dep
                          WHERE dep.PERS_ID     = x.PERS_ID
                            AND dep.date_depart > x.date_absence)
    ),
    situations AS (
        SELECT da.date_arrete, s.PERS_ID,
               s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
               s.STATUT_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
               TO_CHAR(s.CATEGORIE)                                   AS categorie,
               ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM dates da
        JOIN USER_GRH.GRH_SITUATION s ON 1 = 1
                                     AND s.DATE_DEBUT <= da.date_arrete
    ),
    presents AS (
        SELECT da.date_arrete, p.PERS_ID, p.SEXE,
               p.qualite                                              AS QUALITE_EMPLOYE,
               si.nature                                              AS NATURE_EMPLOI,
               si.QUAL_CODE, si.CODE_DOMAINE_ACT, si.UA_CODE, si.LIEU_ID, si.FNCT_CODE,
               si.STATUT_EMPLOYE,
               si.categorie                                           AS CATEGORIE,
               NVL(po.inactif, 0)                                     AS en_absence
        FROM dates da
        CROSS JOIN personnes p
        CROSS JOIN parametres pr
        LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                             AND si.PERS_ID     = p.PERS_ID
                                             AND si.rn          = 1
        LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
        LEFT JOIN absence_sans_retour asr     ON asr.PERS_ID    = p.PERS_ID
        LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                             AND po.PERS_ID     = p.PERS_ID
        LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                             AND tc.PERS_ID     = p.PERS_ID
        WHERE
                (NVL(si.nature, '-') <> 'TEMPORAIRE'
                 AND p.date_entree <= da.date_arrete
                 AND NVL(po.hors_effectif, 0) = 0
                 AND (
                         NOT (p.etat = 'INACTIF'
                              OR (p.etat = '-'
                                  AND pr.fiche_sans_etat = 'RECENTE'
                                  AND p.date_entree <= ADD_MONTHS(da.date_arrete,
                                                                  -pr.fiche_sans_etat_mois)))
                      OR pd.date_depart > da.date_arrete
                      OR (pr.inactif_sans_depart = 'ABSENCE'
                          AND asr.date_absence > da.date_arrete)))
             OR
                (si.nature = 'TEMPORAIRE'
                 AND tc.PERS_ID IS NOT NULL)
    ),
    au_debut AS (
        SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
               p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
        FROM exercices e
        JOIN presents p ON p.date_arrete = e.d0
    ),
    a_la_fin AS (
        SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
               p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
        FROM exercices e
        JOIN presents p ON p.date_arrete = e.d1
    ),
    dernier_depart_annee AS (
        SELECT annee, PERS_ID, type_mvt, motif
        FROM (SELECT e.annee, dep.PERS_ID, dep.type_mvt, dep.motif,
                     ROW_NUMBER() OVER (PARTITION BY e.annee, dep.PERS_ID
                                        ORDER BY dep.date_depart DESC, dep.motif DESC) AS rn
              FROM exercices e
              JOIN departs dep ON dep.date_depart >  e.d0
                              AND dep.date_depart <= e.d1)
        WHERE rn = 1
    ),
    detaches_annee AS (
        SELECT DISTINCT e.annee, d.PERS_ID
        FROM exercices e
        JOIN decisions d ON d.date_decision >  e.d0
                        AND d.date_decision <= e.d1
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.classement_detache
    ),
    entrees AS (
        SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
               x.QUAL_CODE, x.UA_CODE,
               CASE x.cas
                   WHEN 'DETACHEMENT' THEN 'DETAC'
                   WHEN 'EMBAUCHE'    THEN 'RECRU'
                   WHEN 'ENGAGEMENT'  THEN 'RECRU'
                   WHEN 'REMISE'      THEN 'REM'
                   ELSE                    'AUTRE_ENT'
               END                                                    AS CODE_TYPE_MVT,
               CASE x.cas
                   WHEN 'DETACHEMENT' THEN 'DECISION_' || x.code_detache
                   WHEN 'EMBAUCHE'    THEN 'EMBAUCHE'
                   WHEN 'ENGAGEMENT'  THEN 'ENGAGEMENT_' || x.code_engagement
                   WHEN 'REMISE'      THEN 'DECISION_' || x.code_remise
                   WHEN 'NATURE'      THEN 'ETAIT_' || NVL(x.nature_debut, 'SANS_SITUATION')
                   ELSE                    'SANS_MOTIF'
               END                                                    AS MOTIF_MVT
        FROM (SELECT f.*,
                     dbt.NATURE_EMPLOI          AS nature_debut,
                     cd.classement_detache      AS code_detache,
                     cd.engagement_temporaire   AS code_engagement,
                     cd.remise_en_activite      AS code_remise,
                     CASE
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND dt.PERS_ID IS NOT NULL                       THEN 'DETACHEMENT'
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND pe.date_entree >  f.d0
                          AND pe.date_entree <= f.d1                       THEN 'EMBAUCHE'
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND NVL(po.hors_effectif, 0) = 1                 THEN 'REMISE'
                         WHEN f.NATURE_EMPLOI = 'TEMPORAIRE'
                          AND tc0.PERS_ID IS NULL                          THEN 'ENGAGEMENT'
                         WHEN dbt.PERS_ID IS NOT NULL                      THEN 'NATURE'
                         ELSE                                                   'SANS_MOTIF'
                     END                        AS cas
              FROM a_la_fin f
              JOIN personnes pe ON pe.PERS_ID = f.PERS_ID
              CROSS JOIN codes_decision cd
              LEFT JOIN au_debut dbt                 ON dbt.annee        = f.annee
                                                    AND dbt.PERS_ID      = f.PERS_ID
              LEFT JOIN detaches_annee dt            ON dt.annee         = f.annee
                                                    AND dt.PERS_ID       = f.PERS_ID
              LEFT JOIN positions po                 ON po.date_arrete   = f.d0
                                                    AND po.PERS_ID       = f.PERS_ID
              LEFT JOIN temporaires_sous_contrat tc0 ON tc0.date_arrete  = f.d0
                                                    AND tc0.PERS_ID      = f.PERS_ID
              WHERE dbt.PERS_ID IS NULL
                 OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')) x
    ),
    sorties AS (
        SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
               x.QUAL_CODE, x.UA_CODE,
               CASE x.cas
                   WHEN 'DEPART_PERMANENT'  THEN x.type_premier
                   WHEN 'DEPART_TEMPORAIRE' THEN x.type_dernier
                   WHEN 'ABSENCE'           THEN NVL(x.type_absence, 'DISP')
                   ELSE                          'AUTRE_SORT'
               END                                                    AS CODE_TYPE_MVT,
               CASE x.cas
                   WHEN 'DEPART_PERMANENT'  THEN x.motif_premier
                   WHEN 'DEPART_TEMPORAIRE' THEN x.motif_dernier
                   WHEN 'ABSENCE'           THEN 'DECISION_' || x.code_absence
                   WHEN 'NATURE'            THEN 'DEVIENT_' || NVL(x.nature_fin, 'SANS_SITUATION')
                   ELSE                          'SANS_MOTIF'
               END                                                    AS MOTIF_MVT
        FROM (SELECT dbt.*,
                     fin.NATURE_EMPLOI          AS nature_fin,
                     pd.type_mvt                AS type_premier,
                     pd.motif                   AS motif_premier,
                     dda.type_mvt               AS type_dernier,
                     dda.motif                  AS motif_dernier,
                     ae.code                    AS code_absence,
                     ae.type_mvt                AS type_absence,
                     CASE
                         WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND pd.date_depart >  dbt.d0
                          AND pd.date_depart <= dbt.d1                     THEN 'DEPART_PERMANENT'
                         WHEN dbt.NATURE_EMPLOI = 'TEMPORAIRE'
                          AND dda.PERS_ID IS NOT NULL                      THEN 'DEPART_TEMPORAIRE'
                         WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND NVL(po.hors_effectif, 0) = 1                 THEN 'ABSENCE'
                         WHEN fin.PERS_ID IS NOT NULL                      THEN 'NATURE'
                         ELSE                                                   'SANS_MOTIF'
                     END                        AS cas
              FROM au_debut dbt
              LEFT JOIN a_la_fin fin             ON fin.annee      = dbt.annee
                                                AND fin.PERS_ID    = dbt.PERS_ID
              LEFT JOIN premier_depart pd        ON pd.PERS_ID     = dbt.PERS_ID
              LEFT JOIN dernier_depart_annee dda ON dda.annee      = dbt.annee
                                                AND dda.PERS_ID    = dbt.PERS_ID
              LEFT JOIN positions po             ON po.date_arrete = dbt.d1
                                                AND po.PERS_ID     = dbt.PERS_ID
              LEFT JOIN absences_en_cours ae     ON ae.date_arrete = dbt.d1
                                                AND ae.PERS_ID     = dbt.PERS_ID
                                                AND ae.effet       = 'HORS_EFFECTIF'
              WHERE fin.PERS_ID IS NULL
                 OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-')) x
    ),
    contrats_annee AS (
        SELECT DISTINCT e.annee, c.PERS_ID
        FROM exercices e
        JOIN contrats c ON c.debut >  e.d0
                       AND c.debut <= e.d1
                       AND c.fin   <= e.d1
                       AND c.fin   <  DATE '9999-12-31'
    ),
    passages AS (
        SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite AS QUALITE_EMPLOYE,
               si.nature AS NATURE_EMPLOI, si.QUAL_CODE, si.UA_CODE,
               'EMBAUCHE' AS motif_entree, pd.type_mvt AS type_sortie, pd.motif AS motif_sortie
        FROM exercices e
        CROSS JOIN parametres pr
        JOIN personnes p       ON p.date_entree  >  e.d0
                              AND p.date_entree  <= e.d1
        JOIN premier_depart pd ON pd.PERS_ID     =  p.PERS_ID
                              AND pd.date_depart >  p.date_entree
                              AND pd.date_depart <= e.d1
        LEFT JOIN situations si ON si.date_arrete = e.d1
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
        LEFT JOIN au_debut dbt  ON dbt.annee      = e.annee
                               AND dbt.PERS_ID    = p.PERS_ID
        LEFT JOIN a_la_fin fin  ON fin.annee      = e.annee
                               AND fin.PERS_ID    = p.PERS_ID
        WHERE pr.passages_annee = 'OUI'
          AND p.etat = 'INACTIF'
          AND NVL(si.nature, '-') <> 'TEMPORAIRE'
          AND dbt.PERS_ID IS NULL
          AND fin.PERS_ID IS NULL
        UNION ALL
        SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite,
               si.nature, si.QUAL_CODE, si.UA_CODE,
               'ENGAGEMENT_' || cd.engagement_temporaire,
               NVL(dda.type_mvt, 'AUTRE_SORT'), NVL(dda.motif, 'SANS_MOTIF')
        FROM contrats_annee ca
        JOIN exercices e        ON e.annee        = ca.annee
        JOIN personnes p        ON p.PERS_ID      = ca.PERS_ID
        CROSS JOIN parametres pr
        CROSS JOIN codes_decision cd
        LEFT JOIN situations si ON si.date_arrete = e.d1
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
        LEFT JOIN dernier_depart_annee dda ON dda.annee   = ca.annee
                                          AND dda.PERS_ID = ca.PERS_ID
        LEFT JOIN au_debut dbt  ON dbt.annee      = ca.annee
                               AND dbt.PERS_ID    = ca.PERS_ID
        LEFT JOIN a_la_fin fin  ON fin.annee      = ca.annee
                               AND fin.PERS_ID    = ca.PERS_ID
        WHERE pr.passages_annee = 'OUI'
          AND si.nature = 'TEMPORAIRE'
          AND dbt.PERS_ID IS NULL
          AND fin.PERS_ID IS NULL
    ),
    internes AS (
        SELECT x.annee, x.CODE_TYPE_MVT, x.MVT_ID, x.PERS_ID, p.SEXE,
               TO_CHAR(p.QUALITE)                                     AS QUALITE_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS NATURE_EMPLOI,
               s.QUAL_CODE, s.UA_CODE
        FROM (SELECT m.MVT_ID, m.PERS_ID, m.SITU_ID,
                     EXTRACT(YEAR FROM m.DATE_DEBUT)                  AS annee,
                     CASE UPPER(TRIM(m.NATURE_MVT))
                         WHEN 'DIS' THEN 'DISP'
                         WHEN 'DET' THEN 'DETAC'
                         ELSE UPPER(TRIM(m.NATURE_MVT))
                     END                                              AS CODE_TYPE_MVT
              FROM USER_GRH.GRH_MOUVEMENT m
              WHERE 1 = 1
                AND m.DATE_DEBUT IS NOT NULL
                AND m.VALIDE      = 'O') x
        CROSS JOIN parametres pr
        LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm ON tm.CODE_TYPE_MVT = x.CODE_TYPE_MVT
        LEFT JOIN USER_GRH.GRH_PERSONNE p       ON p.PERS_ID = x.PERS_ID
                                               AND 1 = 1
        LEFT JOIN USER_GRH.GRH_SITUATION s      ON s.SITU_ID = x.SITU_ID
                                               AND 1 = 1
        WHERE pr.mouvements_internes = 'COMME_AVANT'
          AND NVL(tm.CODE_NAT_MVT, '-') NOT IN ('INITIAL', 'ENTREES', 'SORTIES')
    ),
    mouvements AS (
        SELECT annee, 'DEBUT' AS CODE_TYPE_MVT, PERS_ID AS id_mvt, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE,
               CAST(NULL AS VARCHAR2(30)) AS MOTIF_MVT
        FROM au_debut
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
        FROM entrees
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
        FROM sorties
        UNION ALL
        SELECT annee, 'RECRU', PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_entree
        FROM passages
        UNION ALL
        SELECT annee, type_sortie, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_sortie
        FROM passages
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, MVT_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, 'GRH_MOUVEMENT'
        FROM internes
    )
    SELECT
        TO_NUMBER(TO_CHAR(m.annee) || '1231')                 AS ID_TEMPS,
        m.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT
            WHEN 'ENTREES' THEN 'PRISE'
            WHEN 'SORTIES' THEN 'CESSA'
            ELSE NULL
        END                                                   AS CODE_SENS_MVT,
        m.QUAL_CODE,
        m.SEXE,
        CASE m.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        COUNT(m.id_mvt)                                       AS NB_MOUVEMENTS,
        COUNT(DISTINCT m.PERS_ID)                             AS NB_AGENTS,
        SYSDATE                                               AS DATE_CHARGEMENT,
        0                                                    AS CLICHE,
        m.QUALITE_EMPLOYE,
        m.NATURE_EMPLOI,
        m.UA_CODE,
        m.MOTIF_MVT
    FROM mouvements m
    LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm
           ON tm.CODE_TYPE_MVT = m.CODE_TYPE_MVT
    GROUP BY
        TO_NUMBER(TO_CHAR(m.annee) || '1231'),
        m.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT WHEN 'ENTREES' THEN 'PRISE' WHEN 'SORTIES' THEN 'CESSA' ELSE NULL END,
        m.QUAL_CODE,
        m.SEXE,
        CASE m.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
        m.QUALITE_EMPLOYE,
        m.NATURE_EMPLOI,
        m.UA_CODE,
        m.MOTIF_MVT
)
WHERE QUALITE_EMPLOYE = 'A'
  AND NATURE_EMPLOI = 'PERMANENT'
  AND ID_TEMPS BETWEEN 20201231 AND 20251231
GROUP BY ID_TEMPS
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- C2 — T60, permanents, 2020 à 2025 : par code de mouvement
-- Lecture  : ne dépend pas de la table des codes. Un code dont la nature est vide
--            manque dans USER_GRH.GRH_TYPE_MOUVEMENT (voir Q152 du lot 26) : la maquette
--            du contrôle C1 l'oublie.
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS, CODE_NAT_MVT, CODE_TYPE_MVT, SUM(NB_AGENTS) AS agents
FROM (
    WITH
    parametres AS (
        SELECT
            'PRESENT'               AS fin_contrat_inconnue,
            18                      AS duree_contrat_mois,
            'PRESENT'               AS fiche_sans_etat,
            24                      AS fiche_sans_etat_mois,
            'JAMAIS_COMPTE'         AS inactif_sans_depart,
            'EMBAUCHE'              AS ordre_date_entree,
            'NON'                   AS passages_annee,
            'COMME_AVANT'           AS mouvements_internes
        FROM DUAL
    ),
    decisions_depart AS (
        SELECT '02' AS code, 'RET' AS type_mvt, 'PREMIER_JOUR_PARTI' AS date_effet, 'TOUTES' AS retenir,
               'admission à la retraite' AS libelle                                             FROM DUAL UNION ALL
        SELECT '34', 'DEC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'liquidation des droits après décès' FROM DUAL UNION ALL
        SELECT '35', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'fin de contrat'                     FROM DUAL UNION ALL
        SELECT '36', 'DEM',       'PREMIER_JOUR_PARTI', 'TOUTES', 'démission'                          FROM DUAL UNION ALL
        SELECT '52', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'rupture du contrat'                 FROM DUAL UNION ALL
        SELECT '54', 'LIC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'licenciement'                       FROM DUAL
    ),
    absences AS (
        SELECT '07' AS code, 'HORS_EFFECTIF' AS effet, 'RETIREE' AS anciennete, 'DISP' AS type_mvt,
               'mise en disponibilité' AS libelle                                 FROM DUAL UNION ALL
        SELECT '14', 'PRESENT',       'COMPTEE', 'MISE_DIS', 'mise à disposition' FROM DUAL
    ),
    codes_decision AS (
        SELECT '40' AS annulation,
               '50' AS engagement_temporaire,
               '51' AS engagement_permanent,
               '18' AS remise_en_activite,
               '37' AS classement_detache
        FROM DUAL
    ),
    annees AS (
        SELECT 1899 + LEVEL AS annee
        FROM DUAL
        CONNECT BY LEVEL <= EXTRACT(YEAR FROM SYSDATE) - 1900
    ),
    dates AS (
        SELECT TO_DATE(TO_CHAR(a.annee) || '1231', 'YYYYMMDD')            AS date_arrete
        FROM annees a
        WHERE a.annee >= (SELECT MIN(EXTRACT(YEAR FROM s.DATE_DEBUT))
                          FROM USER_GRH.GRH_SITUATION s
                          WHERE 1 = 1
                            AND s.DATE_DEBUT IS NOT NULL
                            AND EXTRACT(YEAR FROM s.DATE_DEBUT) >= 1900)
    ),
    exercices AS (
        SELECT EXTRACT(YEAR FROM da.date_arrete)                          AS annee,
               ADD_MONTHS(da.date_arrete, -12)                            AS d0,
               da.date_arrete                                             AS d1
        FROM dates da
        WHERE ADD_MONTHS(da.date_arrete, -12) IN (SELECT date_arrete FROM dates)
    ),
    personnes AS (
        SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS,
               p.DATE_EMBAUCHE                                        AS date_embauche,
               p.DATE_PRISE_SERVICE                                   AS date_prise_service,
               NVL(p.ETAT_EMPLOYE, '-')                               AS etat,
               TO_CHAR(p.QUALITE)                                     AS qualite,
               CASE pr.ordre_date_entree
                   WHEN 'PRISE_SERVICE' THEN COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
                   ELSE                      COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
               END                                                    AS date_entree
        FROM USER_GRH.GRH_PERSONNE p
        CROSS JOIN parametres pr
        WHERE 1 = 1
    ),
    decisions AS (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               TO_CHAR(d.CDEC_CODE)       AS code,
               d.DATE_VIGUEUR             AS date_decision
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp
               ON dp.DECI_NO = d.DECI_NO
              AND 1 = 1
        CROSS JOIN codes_decision cd
        WHERE 1 = 1
          AND d.DATE_VIGUEUR IS NOT NULL
          AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
               OR TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM absences)
               OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                           cd.remise_en_activite, cd.classement_detache))
          AND NOT EXISTS (SELECT 1
                          FROM USER_GRH.GRH_DECISION a
                          WHERE 1 = 1
                            AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    ),
    departs AS (
        SELECT x.PERS_ID,
               x.date_decision
                   + CASE x.date_effet WHEN 'DERNIER_JOUR_TRAVAILLE' THEN 1 ELSE 0 END AS date_depart,
               x.type_mvt,
               'DECISION_' || x.code                                  AS motif
        FROM (SELECT dc.PERS_ID, dc.code, dc.date_decision,
                     dd.type_mvt, dd.date_effet, dd.retenir,
                     ROW_NUMBER() OVER (PARTITION BY dc.PERS_ID, dc.code
                                        ORDER BY dc.date_decision DESC) AS rang_fin
              FROM decisions dc
              JOIN decisions_depart dd ON dd.code = dc.code
              WHERE dc.PERS_ID IS NOT NULL) x
        WHERE x.retenir = 'TOUTES'
           OR x.rang_fin = 1
        UNION ALL
        SELECT PERS_ID, DATE_DECES, 'DEC', 'DATE_DECES'
        FROM USER_GRH.GRH_PERSONNE
        WHERE 1 = 1
          AND DATE_DECES IS NOT NULL
    ),
    premier_depart AS (
        SELECT PERS_ID, date_depart, type_mvt, motif
        FROM (SELECT x.PERS_ID, x.date_depart, x.type_mvt, x.motif,
                     ROW_NUMBER() OVER (PARTITION BY x.PERS_ID
                                        ORDER BY x.date_depart, x.motif) AS rn
              FROM departs x)
        WHERE rn = 1
    ),
    engagements AS (
        SELECT e.PERS_ID, e.date_decision AS debut,
               LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                           ORDER BY e.date_decision) AS debut_suivant
        FROM decisions e
        CROSS JOIN codes_decision cd
        WHERE e.PERS_ID IS NOT NULL
          AND e.code = cd.engagement_temporaire
    ),
    contrats AS (
        SELECT x.PERS_ID, x.debut,
               CASE
                   WHEN x.fin_base < DATE '9999-12-31'       THEN x.fin_base
                   WHEN pr.fin_contrat_inconnue = 'DUREE'    THEN ADD_MONTHS(x.debut, pr.duree_contrat_mois)
                   WHEN pr.fin_contrat_inconnue = 'FICHE'
                    AND p.etat = 'INACTIF'                   THEN CAST(NULL AS DATE)
                   ELSE                                           x.fin_base
               END                                                    AS fin
        FROM (SELECT g.PERS_ID, g.debut,
                     LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                           NVL(g.debut_suivant, DATE '9999-12-31'))   AS fin_base
              FROM engagements g
              LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                                   AND dep.date_depart > g.debut
              GROUP BY g.PERS_ID, g.debut, g.debut_suivant) x
        JOIN personnes p ON p.PERS_ID = x.PERS_ID
        CROSS JOIN parametres pr
    ),
    temporaires_sous_contrat AS (
        SELECT DISTINCT da.date_arrete, c.PERS_ID
        FROM dates da
        JOIN contrats c ON c.debut       <= da.date_arrete
                       AND da.date_arrete <  c.fin
    ),
    remises AS (
        SELECT da.date_arrete, d.PERS_ID, MAX(d.date_decision) AS date_remise
        FROM dates da
        JOIN decisions d ON d.date_decision <= da.date_arrete
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.remise_en_activite
        GROUP BY da.date_arrete, d.PERS_ID
    ),
    absences_en_cours AS (
        SELECT x.date_arrete, x.PERS_ID, x.code, x.effet, x.type_mvt, x.date_absence
        FROM (SELECT da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt,
                     MAX(d.date_decision) AS date_absence
              FROM dates da
              JOIN decisions d ON d.date_decision <= da.date_arrete
              JOIN absences ab ON ab.code = d.code
              WHERE d.PERS_ID IS NOT NULL
              GROUP BY da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt) x
        LEFT JOIN remises r ON r.date_arrete = x.date_arrete
                           AND r.PERS_ID     = x.PERS_ID
        WHERE x.date_absence > NVL(r.date_remise, DATE '1900-01-01')
    ),
    positions AS (
        SELECT date_arrete, PERS_ID,
               MAX(CASE WHEN effet = 'HORS_EFFECTIF' THEN 1 ELSE 0 END)              AS hors_effectif,
               MAX(CASE WHEN effet IN ('HORS_EFFECTIF', 'INACTIF') THEN 1 ELSE 0 END) AS inactif
        FROM absences_en_cours
        GROUP BY date_arrete, PERS_ID
    ),
    absence_sans_retour AS (
        SELECT x.PERS_ID, x.date_absence
        FROM (SELECT d.PERS_ID, MAX(d.date_decision) AS date_absence
              FROM decisions d
              JOIN absences ab ON ab.code = d.code
              WHERE d.PERS_ID IS NOT NULL
                AND ab.effet = 'HORS_EFFECTIF'
              GROUP BY d.PERS_ID) x
        WHERE NOT EXISTS (SELECT 1
                          FROM decisions r, codes_decision cd
                          WHERE r.PERS_ID       =  x.PERS_ID
                            AND r.code          =  cd.remise_en_activite
                            AND r.date_decision >= x.date_absence)
          AND NOT EXISTS (SELECT 1
                          FROM departs dep
                          WHERE dep.PERS_ID     = x.PERS_ID
                            AND dep.date_depart > x.date_absence)
    ),
    situations AS (
        SELECT da.date_arrete, s.PERS_ID,
               s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
               s.STATUT_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
               TO_CHAR(s.CATEGORIE)                                   AS categorie,
               ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM dates da
        JOIN USER_GRH.GRH_SITUATION s ON 1 = 1
                                     AND s.DATE_DEBUT <= da.date_arrete
    ),
    presents AS (
        SELECT da.date_arrete, p.PERS_ID, p.SEXE,
               p.qualite                                              AS QUALITE_EMPLOYE,
               si.nature                                              AS NATURE_EMPLOI,
               si.QUAL_CODE, si.CODE_DOMAINE_ACT, si.UA_CODE, si.LIEU_ID, si.FNCT_CODE,
               si.STATUT_EMPLOYE,
               si.categorie                                           AS CATEGORIE,
               NVL(po.inactif, 0)                                     AS en_absence
        FROM dates da
        CROSS JOIN personnes p
        CROSS JOIN parametres pr
        LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                             AND si.PERS_ID     = p.PERS_ID
                                             AND si.rn          = 1
        LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
        LEFT JOIN absence_sans_retour asr     ON asr.PERS_ID    = p.PERS_ID
        LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                             AND po.PERS_ID     = p.PERS_ID
        LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                             AND tc.PERS_ID     = p.PERS_ID
        WHERE
                (NVL(si.nature, '-') <> 'TEMPORAIRE'
                 AND p.date_entree <= da.date_arrete
                 AND NVL(po.hors_effectif, 0) = 0
                 AND (
                         NOT (p.etat = 'INACTIF'
                              OR (p.etat = '-'
                                  AND pr.fiche_sans_etat = 'RECENTE'
                                  AND p.date_entree <= ADD_MONTHS(da.date_arrete,
                                                                  -pr.fiche_sans_etat_mois)))
                      OR pd.date_depart > da.date_arrete
                      OR (pr.inactif_sans_depart = 'ABSENCE'
                          AND asr.date_absence > da.date_arrete)))
             OR
                (si.nature = 'TEMPORAIRE'
                 AND tc.PERS_ID IS NOT NULL)
    ),
    au_debut AS (
        SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
               p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
        FROM exercices e
        JOIN presents p ON p.date_arrete = e.d0
    ),
    a_la_fin AS (
        SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
               p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
        FROM exercices e
        JOIN presents p ON p.date_arrete = e.d1
    ),
    dernier_depart_annee AS (
        SELECT annee, PERS_ID, type_mvt, motif
        FROM (SELECT e.annee, dep.PERS_ID, dep.type_mvt, dep.motif,
                     ROW_NUMBER() OVER (PARTITION BY e.annee, dep.PERS_ID
                                        ORDER BY dep.date_depart DESC, dep.motif DESC) AS rn
              FROM exercices e
              JOIN departs dep ON dep.date_depart >  e.d0
                              AND dep.date_depart <= e.d1)
        WHERE rn = 1
    ),
    detaches_annee AS (
        SELECT DISTINCT e.annee, d.PERS_ID
        FROM exercices e
        JOIN decisions d ON d.date_decision >  e.d0
                        AND d.date_decision <= e.d1
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.classement_detache
    ),
    entrees AS (
        SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
               x.QUAL_CODE, x.UA_CODE,
               CASE x.cas
                   WHEN 'DETACHEMENT' THEN 'DETAC'
                   WHEN 'EMBAUCHE'    THEN 'RECRU'
                   WHEN 'ENGAGEMENT'  THEN 'RECRU'
                   WHEN 'REMISE'      THEN 'REM'
                   ELSE                    'AUTRE_ENT'
               END                                                    AS CODE_TYPE_MVT,
               CASE x.cas
                   WHEN 'DETACHEMENT' THEN 'DECISION_' || x.code_detache
                   WHEN 'EMBAUCHE'    THEN 'EMBAUCHE'
                   WHEN 'ENGAGEMENT'  THEN 'ENGAGEMENT_' || x.code_engagement
                   WHEN 'REMISE'      THEN 'DECISION_' || x.code_remise
                   WHEN 'NATURE'      THEN 'ETAIT_' || NVL(x.nature_debut, 'SANS_SITUATION')
                   ELSE                    'SANS_MOTIF'
               END                                                    AS MOTIF_MVT
        FROM (SELECT f.*,
                     dbt.NATURE_EMPLOI          AS nature_debut,
                     cd.classement_detache      AS code_detache,
                     cd.engagement_temporaire   AS code_engagement,
                     cd.remise_en_activite      AS code_remise,
                     CASE
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND dt.PERS_ID IS NOT NULL                       THEN 'DETACHEMENT'
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND pe.date_entree >  f.d0
                          AND pe.date_entree <= f.d1                       THEN 'EMBAUCHE'
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND NVL(po.hors_effectif, 0) = 1                 THEN 'REMISE'
                         WHEN f.NATURE_EMPLOI = 'TEMPORAIRE'
                          AND tc0.PERS_ID IS NULL                          THEN 'ENGAGEMENT'
                         WHEN dbt.PERS_ID IS NOT NULL                      THEN 'NATURE'
                         ELSE                                                   'SANS_MOTIF'
                     END                        AS cas
              FROM a_la_fin f
              JOIN personnes pe ON pe.PERS_ID = f.PERS_ID
              CROSS JOIN codes_decision cd
              LEFT JOIN au_debut dbt                 ON dbt.annee        = f.annee
                                                    AND dbt.PERS_ID      = f.PERS_ID
              LEFT JOIN detaches_annee dt            ON dt.annee         = f.annee
                                                    AND dt.PERS_ID       = f.PERS_ID
              LEFT JOIN positions po                 ON po.date_arrete   = f.d0
                                                    AND po.PERS_ID       = f.PERS_ID
              LEFT JOIN temporaires_sous_contrat tc0 ON tc0.date_arrete  = f.d0
                                                    AND tc0.PERS_ID      = f.PERS_ID
              WHERE dbt.PERS_ID IS NULL
                 OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')) x
    ),
    sorties AS (
        SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
               x.QUAL_CODE, x.UA_CODE,
               CASE x.cas
                   WHEN 'DEPART_PERMANENT'  THEN x.type_premier
                   WHEN 'DEPART_TEMPORAIRE' THEN x.type_dernier
                   WHEN 'ABSENCE'           THEN NVL(x.type_absence, 'DISP')
                   ELSE                          'AUTRE_SORT'
               END                                                    AS CODE_TYPE_MVT,
               CASE x.cas
                   WHEN 'DEPART_PERMANENT'  THEN x.motif_premier
                   WHEN 'DEPART_TEMPORAIRE' THEN x.motif_dernier
                   WHEN 'ABSENCE'           THEN 'DECISION_' || x.code_absence
                   WHEN 'NATURE'            THEN 'DEVIENT_' || NVL(x.nature_fin, 'SANS_SITUATION')
                   ELSE                          'SANS_MOTIF'
               END                                                    AS MOTIF_MVT
        FROM (SELECT dbt.*,
                     fin.NATURE_EMPLOI          AS nature_fin,
                     pd.type_mvt                AS type_premier,
                     pd.motif                   AS motif_premier,
                     dda.type_mvt               AS type_dernier,
                     dda.motif                  AS motif_dernier,
                     ae.code                    AS code_absence,
                     ae.type_mvt                AS type_absence,
                     CASE
                         WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND pd.date_depart >  dbt.d0
                          AND pd.date_depart <= dbt.d1                     THEN 'DEPART_PERMANENT'
                         WHEN dbt.NATURE_EMPLOI = 'TEMPORAIRE'
                          AND dda.PERS_ID IS NOT NULL                      THEN 'DEPART_TEMPORAIRE'
                         WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND NVL(po.hors_effectif, 0) = 1                 THEN 'ABSENCE'
                         WHEN fin.PERS_ID IS NOT NULL                      THEN 'NATURE'
                         ELSE                                                   'SANS_MOTIF'
                     END                        AS cas
              FROM au_debut dbt
              LEFT JOIN a_la_fin fin             ON fin.annee      = dbt.annee
                                                AND fin.PERS_ID    = dbt.PERS_ID
              LEFT JOIN premier_depart pd        ON pd.PERS_ID     = dbt.PERS_ID
              LEFT JOIN dernier_depart_annee dda ON dda.annee      = dbt.annee
                                                AND dda.PERS_ID    = dbt.PERS_ID
              LEFT JOIN positions po             ON po.date_arrete = dbt.d1
                                                AND po.PERS_ID     = dbt.PERS_ID
              LEFT JOIN absences_en_cours ae     ON ae.date_arrete = dbt.d1
                                                AND ae.PERS_ID     = dbt.PERS_ID
                                                AND ae.effet       = 'HORS_EFFECTIF'
              WHERE fin.PERS_ID IS NULL
                 OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-')) x
    ),
    contrats_annee AS (
        SELECT DISTINCT e.annee, c.PERS_ID
        FROM exercices e
        JOIN contrats c ON c.debut >  e.d0
                       AND c.debut <= e.d1
                       AND c.fin   <= e.d1
                       AND c.fin   <  DATE '9999-12-31'
    ),
    passages AS (
        SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite AS QUALITE_EMPLOYE,
               si.nature AS NATURE_EMPLOI, si.QUAL_CODE, si.UA_CODE,
               'EMBAUCHE' AS motif_entree, pd.type_mvt AS type_sortie, pd.motif AS motif_sortie
        FROM exercices e
        CROSS JOIN parametres pr
        JOIN personnes p       ON p.date_entree  >  e.d0
                              AND p.date_entree  <= e.d1
        JOIN premier_depart pd ON pd.PERS_ID     =  p.PERS_ID
                              AND pd.date_depart >  p.date_entree
                              AND pd.date_depart <= e.d1
        LEFT JOIN situations si ON si.date_arrete = e.d1
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
        LEFT JOIN au_debut dbt  ON dbt.annee      = e.annee
                               AND dbt.PERS_ID    = p.PERS_ID
        LEFT JOIN a_la_fin fin  ON fin.annee      = e.annee
                               AND fin.PERS_ID    = p.PERS_ID
        WHERE pr.passages_annee = 'OUI'
          AND p.etat = 'INACTIF'
          AND NVL(si.nature, '-') <> 'TEMPORAIRE'
          AND dbt.PERS_ID IS NULL
          AND fin.PERS_ID IS NULL
        UNION ALL
        SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite,
               si.nature, si.QUAL_CODE, si.UA_CODE,
               'ENGAGEMENT_' || cd.engagement_temporaire,
               NVL(dda.type_mvt, 'AUTRE_SORT'), NVL(dda.motif, 'SANS_MOTIF')
        FROM contrats_annee ca
        JOIN exercices e        ON e.annee        = ca.annee
        JOIN personnes p        ON p.PERS_ID      = ca.PERS_ID
        CROSS JOIN parametres pr
        CROSS JOIN codes_decision cd
        LEFT JOIN situations si ON si.date_arrete = e.d1
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
        LEFT JOIN dernier_depart_annee dda ON dda.annee   = ca.annee
                                          AND dda.PERS_ID = ca.PERS_ID
        LEFT JOIN au_debut dbt  ON dbt.annee      = ca.annee
                               AND dbt.PERS_ID    = ca.PERS_ID
        LEFT JOIN a_la_fin fin  ON fin.annee      = ca.annee
                               AND fin.PERS_ID    = ca.PERS_ID
        WHERE pr.passages_annee = 'OUI'
          AND si.nature = 'TEMPORAIRE'
          AND dbt.PERS_ID IS NULL
          AND fin.PERS_ID IS NULL
    ),
    internes AS (
        SELECT x.annee, x.CODE_TYPE_MVT, x.MVT_ID, x.PERS_ID, p.SEXE,
               TO_CHAR(p.QUALITE)                                     AS QUALITE_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS NATURE_EMPLOI,
               s.QUAL_CODE, s.UA_CODE
        FROM (SELECT m.MVT_ID, m.PERS_ID, m.SITU_ID,
                     EXTRACT(YEAR FROM m.DATE_DEBUT)                  AS annee,
                     CASE UPPER(TRIM(m.NATURE_MVT))
                         WHEN 'DIS' THEN 'DISP'
                         WHEN 'DET' THEN 'DETAC'
                         ELSE UPPER(TRIM(m.NATURE_MVT))
                     END                                              AS CODE_TYPE_MVT
              FROM USER_GRH.GRH_MOUVEMENT m
              WHERE 1 = 1
                AND m.DATE_DEBUT IS NOT NULL
                AND m.VALIDE      = 'O') x
        CROSS JOIN parametres pr
        LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm ON tm.CODE_TYPE_MVT = x.CODE_TYPE_MVT
        LEFT JOIN USER_GRH.GRH_PERSONNE p       ON p.PERS_ID = x.PERS_ID
                                               AND 1 = 1
        LEFT JOIN USER_GRH.GRH_SITUATION s      ON s.SITU_ID = x.SITU_ID
                                               AND 1 = 1
        WHERE pr.mouvements_internes = 'COMME_AVANT'
          AND NVL(tm.CODE_NAT_MVT, '-') NOT IN ('INITIAL', 'ENTREES', 'SORTIES')
    ),
    mouvements AS (
        SELECT annee, 'DEBUT' AS CODE_TYPE_MVT, PERS_ID AS id_mvt, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE,
               CAST(NULL AS VARCHAR2(30)) AS MOTIF_MVT
        FROM au_debut
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
        FROM entrees
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
        FROM sorties
        UNION ALL
        SELECT annee, 'RECRU', PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_entree
        FROM passages
        UNION ALL
        SELECT annee, type_sortie, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_sortie
        FROM passages
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, MVT_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, 'GRH_MOUVEMENT'
        FROM internes
    )
    SELECT
        TO_NUMBER(TO_CHAR(m.annee) || '1231')                 AS ID_TEMPS,
        m.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT
            WHEN 'ENTREES' THEN 'PRISE'
            WHEN 'SORTIES' THEN 'CESSA'
            ELSE NULL
        END                                                   AS CODE_SENS_MVT,
        m.QUAL_CODE,
        m.SEXE,
        CASE m.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        COUNT(m.id_mvt)                                       AS NB_MOUVEMENTS,
        COUNT(DISTINCT m.PERS_ID)                             AS NB_AGENTS,
        SYSDATE                                               AS DATE_CHARGEMENT,
        0                                                    AS CLICHE,
        m.QUALITE_EMPLOYE,
        m.NATURE_EMPLOI,
        m.UA_CODE,
        m.MOTIF_MVT
    FROM mouvements m
    LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm
           ON tm.CODE_TYPE_MVT = m.CODE_TYPE_MVT
    GROUP BY
        TO_NUMBER(TO_CHAR(m.annee) || '1231'),
        m.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT WHEN 'ENTREES' THEN 'PRISE' WHEN 'SORTIES' THEN 'CESSA' ELSE NULL END,
        m.QUAL_CODE,
        m.SEXE,
        CASE m.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
        m.QUALITE_EMPLOYE,
        m.NATURE_EMPLOI,
        m.UA_CODE,
        m.MOTIF_MVT
)
WHERE QUALITE_EMPLOYE = 'A'
  AND NATURE_EMPLOI = 'PERMANENT'
  AND ID_TEMPS BETWEEN 20201231 AND 20251231
GROUP BY ID_TEMPS, CODE_NAT_MVT, CODE_TYPE_MVT
ORDER BY 1, 2, 3;


-- -----------------------------------------------------------------------------
-- C3 — T60, permanents, 2024 : le détail des « autres » par motif
-- Attendu  : REM 4 ; 2 démissions, 1 licenciement, 5 disponibilités, 1 sorti sans motif
--            daté. C'est la liste que demande le recueil (point 49 de la v2).
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT CODE_TYPE_MVT, MOTIF_MVT, SUM(NB_AGENTS) AS agents
FROM (
    WITH
    parametres AS (
        SELECT
            'PRESENT'               AS fin_contrat_inconnue,
            18                      AS duree_contrat_mois,
            'PRESENT'               AS fiche_sans_etat,
            24                      AS fiche_sans_etat_mois,
            'JAMAIS_COMPTE'         AS inactif_sans_depart,
            'EMBAUCHE'              AS ordre_date_entree,
            'NON'                   AS passages_annee,
            'COMME_AVANT'           AS mouvements_internes
        FROM DUAL
    ),
    decisions_depart AS (
        SELECT '02' AS code, 'RET' AS type_mvt, 'PREMIER_JOUR_PARTI' AS date_effet, 'TOUTES' AS retenir,
               'admission à la retraite' AS libelle                                             FROM DUAL UNION ALL
        SELECT '34', 'DEC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'liquidation des droits après décès' FROM DUAL UNION ALL
        SELECT '35', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'fin de contrat'                     FROM DUAL UNION ALL
        SELECT '36', 'DEM',       'PREMIER_JOUR_PARTI', 'TOUTES', 'démission'                          FROM DUAL UNION ALL
        SELECT '52', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'rupture du contrat'                 FROM DUAL UNION ALL
        SELECT '54', 'LIC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'licenciement'                       FROM DUAL
    ),
    absences AS (
        SELECT '07' AS code, 'HORS_EFFECTIF' AS effet, 'RETIREE' AS anciennete, 'DISP' AS type_mvt,
               'mise en disponibilité' AS libelle                                 FROM DUAL UNION ALL
        SELECT '14', 'PRESENT',       'COMPTEE', 'MISE_DIS', 'mise à disposition' FROM DUAL
    ),
    codes_decision AS (
        SELECT '40' AS annulation,
               '50' AS engagement_temporaire,
               '51' AS engagement_permanent,
               '18' AS remise_en_activite,
               '37' AS classement_detache
        FROM DUAL
    ),
    annees AS (
        SELECT 1899 + LEVEL AS annee
        FROM DUAL
        CONNECT BY LEVEL <= EXTRACT(YEAR FROM SYSDATE) - 1900
    ),
    dates AS (
        SELECT TO_DATE(TO_CHAR(a.annee) || '1231', 'YYYYMMDD')            AS date_arrete
        FROM annees a
        WHERE a.annee >= (SELECT MIN(EXTRACT(YEAR FROM s.DATE_DEBUT))
                          FROM USER_GRH.GRH_SITUATION s
                          WHERE 1 = 1
                            AND s.DATE_DEBUT IS NOT NULL
                            AND EXTRACT(YEAR FROM s.DATE_DEBUT) >= 1900)
    ),
    exercices AS (
        SELECT EXTRACT(YEAR FROM da.date_arrete)                          AS annee,
               ADD_MONTHS(da.date_arrete, -12)                            AS d0,
               da.date_arrete                                             AS d1
        FROM dates da
        WHERE ADD_MONTHS(da.date_arrete, -12) IN (SELECT date_arrete FROM dates)
    ),
    personnes AS (
        SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS,
               p.DATE_EMBAUCHE                                        AS date_embauche,
               p.DATE_PRISE_SERVICE                                   AS date_prise_service,
               NVL(p.ETAT_EMPLOYE, '-')                               AS etat,
               TO_CHAR(p.QUALITE)                                     AS qualite,
               CASE pr.ordre_date_entree
                   WHEN 'PRISE_SERVICE' THEN COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
                   ELSE                      COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
               END                                                    AS date_entree
        FROM USER_GRH.GRH_PERSONNE p
        CROSS JOIN parametres pr
        WHERE 1 = 1
    ),
    decisions AS (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               TO_CHAR(d.CDEC_CODE)       AS code,
               d.DATE_VIGUEUR             AS date_decision
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp
               ON dp.DECI_NO = d.DECI_NO
              AND 1 = 1
        CROSS JOIN codes_decision cd
        WHERE 1 = 1
          AND d.DATE_VIGUEUR IS NOT NULL
          AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
               OR TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM absences)
               OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                           cd.remise_en_activite, cd.classement_detache))
          AND NOT EXISTS (SELECT 1
                          FROM USER_GRH.GRH_DECISION a
                          WHERE 1 = 1
                            AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    ),
    departs AS (
        SELECT x.PERS_ID,
               x.date_decision
                   + CASE x.date_effet WHEN 'DERNIER_JOUR_TRAVAILLE' THEN 1 ELSE 0 END AS date_depart,
               x.type_mvt,
               'DECISION_' || x.code                                  AS motif
        FROM (SELECT dc.PERS_ID, dc.code, dc.date_decision,
                     dd.type_mvt, dd.date_effet, dd.retenir,
                     ROW_NUMBER() OVER (PARTITION BY dc.PERS_ID, dc.code
                                        ORDER BY dc.date_decision DESC) AS rang_fin
              FROM decisions dc
              JOIN decisions_depart dd ON dd.code = dc.code
              WHERE dc.PERS_ID IS NOT NULL) x
        WHERE x.retenir = 'TOUTES'
           OR x.rang_fin = 1
        UNION ALL
        SELECT PERS_ID, DATE_DECES, 'DEC', 'DATE_DECES'
        FROM USER_GRH.GRH_PERSONNE
        WHERE 1 = 1
          AND DATE_DECES IS NOT NULL
    ),
    premier_depart AS (
        SELECT PERS_ID, date_depart, type_mvt, motif
        FROM (SELECT x.PERS_ID, x.date_depart, x.type_mvt, x.motif,
                     ROW_NUMBER() OVER (PARTITION BY x.PERS_ID
                                        ORDER BY x.date_depart, x.motif) AS rn
              FROM departs x)
        WHERE rn = 1
    ),
    engagements AS (
        SELECT e.PERS_ID, e.date_decision AS debut,
               LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                           ORDER BY e.date_decision) AS debut_suivant
        FROM decisions e
        CROSS JOIN codes_decision cd
        WHERE e.PERS_ID IS NOT NULL
          AND e.code = cd.engagement_temporaire
    ),
    contrats AS (
        SELECT x.PERS_ID, x.debut,
               CASE
                   WHEN x.fin_base < DATE '9999-12-31'       THEN x.fin_base
                   WHEN pr.fin_contrat_inconnue = 'DUREE'    THEN ADD_MONTHS(x.debut, pr.duree_contrat_mois)
                   WHEN pr.fin_contrat_inconnue = 'FICHE'
                    AND p.etat = 'INACTIF'                   THEN CAST(NULL AS DATE)
                   ELSE                                           x.fin_base
               END                                                    AS fin
        FROM (SELECT g.PERS_ID, g.debut,
                     LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                           NVL(g.debut_suivant, DATE '9999-12-31'))   AS fin_base
              FROM engagements g
              LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                                   AND dep.date_depart > g.debut
              GROUP BY g.PERS_ID, g.debut, g.debut_suivant) x
        JOIN personnes p ON p.PERS_ID = x.PERS_ID
        CROSS JOIN parametres pr
    ),
    temporaires_sous_contrat AS (
        SELECT DISTINCT da.date_arrete, c.PERS_ID
        FROM dates da
        JOIN contrats c ON c.debut       <= da.date_arrete
                       AND da.date_arrete <  c.fin
    ),
    remises AS (
        SELECT da.date_arrete, d.PERS_ID, MAX(d.date_decision) AS date_remise
        FROM dates da
        JOIN decisions d ON d.date_decision <= da.date_arrete
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.remise_en_activite
        GROUP BY da.date_arrete, d.PERS_ID
    ),
    absences_en_cours AS (
        SELECT x.date_arrete, x.PERS_ID, x.code, x.effet, x.type_mvt, x.date_absence
        FROM (SELECT da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt,
                     MAX(d.date_decision) AS date_absence
              FROM dates da
              JOIN decisions d ON d.date_decision <= da.date_arrete
              JOIN absences ab ON ab.code = d.code
              WHERE d.PERS_ID IS NOT NULL
              GROUP BY da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt) x
        LEFT JOIN remises r ON r.date_arrete = x.date_arrete
                           AND r.PERS_ID     = x.PERS_ID
        WHERE x.date_absence > NVL(r.date_remise, DATE '1900-01-01')
    ),
    positions AS (
        SELECT date_arrete, PERS_ID,
               MAX(CASE WHEN effet = 'HORS_EFFECTIF' THEN 1 ELSE 0 END)              AS hors_effectif,
               MAX(CASE WHEN effet IN ('HORS_EFFECTIF', 'INACTIF') THEN 1 ELSE 0 END) AS inactif
        FROM absences_en_cours
        GROUP BY date_arrete, PERS_ID
    ),
    absence_sans_retour AS (
        SELECT x.PERS_ID, x.date_absence
        FROM (SELECT d.PERS_ID, MAX(d.date_decision) AS date_absence
              FROM decisions d
              JOIN absences ab ON ab.code = d.code
              WHERE d.PERS_ID IS NOT NULL
                AND ab.effet = 'HORS_EFFECTIF'
              GROUP BY d.PERS_ID) x
        WHERE NOT EXISTS (SELECT 1
                          FROM decisions r, codes_decision cd
                          WHERE r.PERS_ID       =  x.PERS_ID
                            AND r.code          =  cd.remise_en_activite
                            AND r.date_decision >= x.date_absence)
          AND NOT EXISTS (SELECT 1
                          FROM departs dep
                          WHERE dep.PERS_ID     = x.PERS_ID
                            AND dep.date_depart > x.date_absence)
    ),
    situations AS (
        SELECT da.date_arrete, s.PERS_ID,
               s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
               s.STATUT_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
               TO_CHAR(s.CATEGORIE)                                   AS categorie,
               ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM dates da
        JOIN USER_GRH.GRH_SITUATION s ON 1 = 1
                                     AND s.DATE_DEBUT <= da.date_arrete
    ),
    presents AS (
        SELECT da.date_arrete, p.PERS_ID, p.SEXE,
               p.qualite                                              AS QUALITE_EMPLOYE,
               si.nature                                              AS NATURE_EMPLOI,
               si.QUAL_CODE, si.CODE_DOMAINE_ACT, si.UA_CODE, si.LIEU_ID, si.FNCT_CODE,
               si.STATUT_EMPLOYE,
               si.categorie                                           AS CATEGORIE,
               NVL(po.inactif, 0)                                     AS en_absence
        FROM dates da
        CROSS JOIN personnes p
        CROSS JOIN parametres pr
        LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                             AND si.PERS_ID     = p.PERS_ID
                                             AND si.rn          = 1
        LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
        LEFT JOIN absence_sans_retour asr     ON asr.PERS_ID    = p.PERS_ID
        LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                             AND po.PERS_ID     = p.PERS_ID
        LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                             AND tc.PERS_ID     = p.PERS_ID
        WHERE
                (NVL(si.nature, '-') <> 'TEMPORAIRE'
                 AND p.date_entree <= da.date_arrete
                 AND NVL(po.hors_effectif, 0) = 0
                 AND (
                         NOT (p.etat = 'INACTIF'
                              OR (p.etat = '-'
                                  AND pr.fiche_sans_etat = 'RECENTE'
                                  AND p.date_entree <= ADD_MONTHS(da.date_arrete,
                                                                  -pr.fiche_sans_etat_mois)))
                      OR pd.date_depart > da.date_arrete
                      OR (pr.inactif_sans_depart = 'ABSENCE'
                          AND asr.date_absence > da.date_arrete)))
             OR
                (si.nature = 'TEMPORAIRE'
                 AND tc.PERS_ID IS NOT NULL)
    ),
    au_debut AS (
        SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
               p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
        FROM exercices e
        JOIN presents p ON p.date_arrete = e.d0
    ),
    a_la_fin AS (
        SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
               p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
        FROM exercices e
        JOIN presents p ON p.date_arrete = e.d1
    ),
    dernier_depart_annee AS (
        SELECT annee, PERS_ID, type_mvt, motif
        FROM (SELECT e.annee, dep.PERS_ID, dep.type_mvt, dep.motif,
                     ROW_NUMBER() OVER (PARTITION BY e.annee, dep.PERS_ID
                                        ORDER BY dep.date_depart DESC, dep.motif DESC) AS rn
              FROM exercices e
              JOIN departs dep ON dep.date_depart >  e.d0
                              AND dep.date_depart <= e.d1)
        WHERE rn = 1
    ),
    detaches_annee AS (
        SELECT DISTINCT e.annee, d.PERS_ID
        FROM exercices e
        JOIN decisions d ON d.date_decision >  e.d0
                        AND d.date_decision <= e.d1
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.classement_detache
    ),
    entrees AS (
        SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
               x.QUAL_CODE, x.UA_CODE,
               CASE x.cas
                   WHEN 'DETACHEMENT' THEN 'DETAC'
                   WHEN 'EMBAUCHE'    THEN 'RECRU'
                   WHEN 'ENGAGEMENT'  THEN 'RECRU'
                   WHEN 'REMISE'      THEN 'REM'
                   ELSE                    'AUTRE_ENT'
               END                                                    AS CODE_TYPE_MVT,
               CASE x.cas
                   WHEN 'DETACHEMENT' THEN 'DECISION_' || x.code_detache
                   WHEN 'EMBAUCHE'    THEN 'EMBAUCHE'
                   WHEN 'ENGAGEMENT'  THEN 'ENGAGEMENT_' || x.code_engagement
                   WHEN 'REMISE'      THEN 'DECISION_' || x.code_remise
                   WHEN 'NATURE'      THEN 'ETAIT_' || NVL(x.nature_debut, 'SANS_SITUATION')
                   ELSE                    'SANS_MOTIF'
               END                                                    AS MOTIF_MVT
        FROM (SELECT f.*,
                     dbt.NATURE_EMPLOI          AS nature_debut,
                     cd.classement_detache      AS code_detache,
                     cd.engagement_temporaire   AS code_engagement,
                     cd.remise_en_activite      AS code_remise,
                     CASE
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND dt.PERS_ID IS NOT NULL                       THEN 'DETACHEMENT'
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND pe.date_entree >  f.d0
                          AND pe.date_entree <= f.d1                       THEN 'EMBAUCHE'
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND NVL(po.hors_effectif, 0) = 1                 THEN 'REMISE'
                         WHEN f.NATURE_EMPLOI = 'TEMPORAIRE'
                          AND tc0.PERS_ID IS NULL                          THEN 'ENGAGEMENT'
                         WHEN dbt.PERS_ID IS NOT NULL                      THEN 'NATURE'
                         ELSE                                                   'SANS_MOTIF'
                     END                        AS cas
              FROM a_la_fin f
              JOIN personnes pe ON pe.PERS_ID = f.PERS_ID
              CROSS JOIN codes_decision cd
              LEFT JOIN au_debut dbt                 ON dbt.annee        = f.annee
                                                    AND dbt.PERS_ID      = f.PERS_ID
              LEFT JOIN detaches_annee dt            ON dt.annee         = f.annee
                                                    AND dt.PERS_ID       = f.PERS_ID
              LEFT JOIN positions po                 ON po.date_arrete   = f.d0
                                                    AND po.PERS_ID       = f.PERS_ID
              LEFT JOIN temporaires_sous_contrat tc0 ON tc0.date_arrete  = f.d0
                                                    AND tc0.PERS_ID      = f.PERS_ID
              WHERE dbt.PERS_ID IS NULL
                 OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')) x
    ),
    sorties AS (
        SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
               x.QUAL_CODE, x.UA_CODE,
               CASE x.cas
                   WHEN 'DEPART_PERMANENT'  THEN x.type_premier
                   WHEN 'DEPART_TEMPORAIRE' THEN x.type_dernier
                   WHEN 'ABSENCE'           THEN NVL(x.type_absence, 'DISP')
                   ELSE                          'AUTRE_SORT'
               END                                                    AS CODE_TYPE_MVT,
               CASE x.cas
                   WHEN 'DEPART_PERMANENT'  THEN x.motif_premier
                   WHEN 'DEPART_TEMPORAIRE' THEN x.motif_dernier
                   WHEN 'ABSENCE'           THEN 'DECISION_' || x.code_absence
                   WHEN 'NATURE'            THEN 'DEVIENT_' || NVL(x.nature_fin, 'SANS_SITUATION')
                   ELSE                          'SANS_MOTIF'
               END                                                    AS MOTIF_MVT
        FROM (SELECT dbt.*,
                     fin.NATURE_EMPLOI          AS nature_fin,
                     pd.type_mvt                AS type_premier,
                     pd.motif                   AS motif_premier,
                     dda.type_mvt               AS type_dernier,
                     dda.motif                  AS motif_dernier,
                     ae.code                    AS code_absence,
                     ae.type_mvt                AS type_absence,
                     CASE
                         WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND pd.date_depart >  dbt.d0
                          AND pd.date_depart <= dbt.d1                     THEN 'DEPART_PERMANENT'
                         WHEN dbt.NATURE_EMPLOI = 'TEMPORAIRE'
                          AND dda.PERS_ID IS NOT NULL                      THEN 'DEPART_TEMPORAIRE'
                         WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND NVL(po.hors_effectif, 0) = 1                 THEN 'ABSENCE'
                         WHEN fin.PERS_ID IS NOT NULL                      THEN 'NATURE'
                         ELSE                                                   'SANS_MOTIF'
                     END                        AS cas
              FROM au_debut dbt
              LEFT JOIN a_la_fin fin             ON fin.annee      = dbt.annee
                                                AND fin.PERS_ID    = dbt.PERS_ID
              LEFT JOIN premier_depart pd        ON pd.PERS_ID     = dbt.PERS_ID
              LEFT JOIN dernier_depart_annee dda ON dda.annee      = dbt.annee
                                                AND dda.PERS_ID    = dbt.PERS_ID
              LEFT JOIN positions po             ON po.date_arrete = dbt.d1
                                                AND po.PERS_ID     = dbt.PERS_ID
              LEFT JOIN absences_en_cours ae     ON ae.date_arrete = dbt.d1
                                                AND ae.PERS_ID     = dbt.PERS_ID
                                                AND ae.effet       = 'HORS_EFFECTIF'
              WHERE fin.PERS_ID IS NULL
                 OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-')) x
    ),
    contrats_annee AS (
        SELECT DISTINCT e.annee, c.PERS_ID
        FROM exercices e
        JOIN contrats c ON c.debut >  e.d0
                       AND c.debut <= e.d1
                       AND c.fin   <= e.d1
                       AND c.fin   <  DATE '9999-12-31'
    ),
    passages AS (
        SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite AS QUALITE_EMPLOYE,
               si.nature AS NATURE_EMPLOI, si.QUAL_CODE, si.UA_CODE,
               'EMBAUCHE' AS motif_entree, pd.type_mvt AS type_sortie, pd.motif AS motif_sortie
        FROM exercices e
        CROSS JOIN parametres pr
        JOIN personnes p       ON p.date_entree  >  e.d0
                              AND p.date_entree  <= e.d1
        JOIN premier_depart pd ON pd.PERS_ID     =  p.PERS_ID
                              AND pd.date_depart >  p.date_entree
                              AND pd.date_depart <= e.d1
        LEFT JOIN situations si ON si.date_arrete = e.d1
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
        LEFT JOIN au_debut dbt  ON dbt.annee      = e.annee
                               AND dbt.PERS_ID    = p.PERS_ID
        LEFT JOIN a_la_fin fin  ON fin.annee      = e.annee
                               AND fin.PERS_ID    = p.PERS_ID
        WHERE pr.passages_annee = 'OUI'
          AND p.etat = 'INACTIF'
          AND NVL(si.nature, '-') <> 'TEMPORAIRE'
          AND dbt.PERS_ID IS NULL
          AND fin.PERS_ID IS NULL
        UNION ALL
        SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite,
               si.nature, si.QUAL_CODE, si.UA_CODE,
               'ENGAGEMENT_' || cd.engagement_temporaire,
               NVL(dda.type_mvt, 'AUTRE_SORT'), NVL(dda.motif, 'SANS_MOTIF')
        FROM contrats_annee ca
        JOIN exercices e        ON e.annee        = ca.annee
        JOIN personnes p        ON p.PERS_ID      = ca.PERS_ID
        CROSS JOIN parametres pr
        CROSS JOIN codes_decision cd
        LEFT JOIN situations si ON si.date_arrete = e.d1
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
        LEFT JOIN dernier_depart_annee dda ON dda.annee   = ca.annee
                                          AND dda.PERS_ID = ca.PERS_ID
        LEFT JOIN au_debut dbt  ON dbt.annee      = ca.annee
                               AND dbt.PERS_ID    = ca.PERS_ID
        LEFT JOIN a_la_fin fin  ON fin.annee      = ca.annee
                               AND fin.PERS_ID    = ca.PERS_ID
        WHERE pr.passages_annee = 'OUI'
          AND si.nature = 'TEMPORAIRE'
          AND dbt.PERS_ID IS NULL
          AND fin.PERS_ID IS NULL
    ),
    internes AS (
        SELECT x.annee, x.CODE_TYPE_MVT, x.MVT_ID, x.PERS_ID, p.SEXE,
               TO_CHAR(p.QUALITE)                                     AS QUALITE_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS NATURE_EMPLOI,
               s.QUAL_CODE, s.UA_CODE
        FROM (SELECT m.MVT_ID, m.PERS_ID, m.SITU_ID,
                     EXTRACT(YEAR FROM m.DATE_DEBUT)                  AS annee,
                     CASE UPPER(TRIM(m.NATURE_MVT))
                         WHEN 'DIS' THEN 'DISP'
                         WHEN 'DET' THEN 'DETAC'
                         ELSE UPPER(TRIM(m.NATURE_MVT))
                     END                                              AS CODE_TYPE_MVT
              FROM USER_GRH.GRH_MOUVEMENT m
              WHERE 1 = 1
                AND m.DATE_DEBUT IS NOT NULL
                AND m.VALIDE      = 'O') x
        CROSS JOIN parametres pr
        LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm ON tm.CODE_TYPE_MVT = x.CODE_TYPE_MVT
        LEFT JOIN USER_GRH.GRH_PERSONNE p       ON p.PERS_ID = x.PERS_ID
                                               AND 1 = 1
        LEFT JOIN USER_GRH.GRH_SITUATION s      ON s.SITU_ID = x.SITU_ID
                                               AND 1 = 1
        WHERE pr.mouvements_internes = 'COMME_AVANT'
          AND NVL(tm.CODE_NAT_MVT, '-') NOT IN ('INITIAL', 'ENTREES', 'SORTIES')
    ),
    mouvements AS (
        SELECT annee, 'DEBUT' AS CODE_TYPE_MVT, PERS_ID AS id_mvt, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE,
               CAST(NULL AS VARCHAR2(30)) AS MOTIF_MVT
        FROM au_debut
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
        FROM entrees
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
        FROM sorties
        UNION ALL
        SELECT annee, 'RECRU', PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_entree
        FROM passages
        UNION ALL
        SELECT annee, type_sortie, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_sortie
        FROM passages
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, MVT_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, 'GRH_MOUVEMENT'
        FROM internes
    )
    SELECT
        TO_NUMBER(TO_CHAR(m.annee) || '1231')                 AS ID_TEMPS,
        m.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT
            WHEN 'ENTREES' THEN 'PRISE'
            WHEN 'SORTIES' THEN 'CESSA'
            ELSE NULL
        END                                                   AS CODE_SENS_MVT,
        m.QUAL_CODE,
        m.SEXE,
        CASE m.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        COUNT(m.id_mvt)                                       AS NB_MOUVEMENTS,
        COUNT(DISTINCT m.PERS_ID)                             AS NB_AGENTS,
        SYSDATE                                               AS DATE_CHARGEMENT,
        0                                                    AS CLICHE,
        m.QUALITE_EMPLOYE,
        m.NATURE_EMPLOI,
        m.UA_CODE,
        m.MOTIF_MVT
    FROM mouvements m
    LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm
           ON tm.CODE_TYPE_MVT = m.CODE_TYPE_MVT
    GROUP BY
        TO_NUMBER(TO_CHAR(m.annee) || '1231'),
        m.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT WHEN 'ENTREES' THEN 'PRISE' WHEN 'SORTIES' THEN 'CESSA' ELSE NULL END,
        m.QUAL_CODE,
        m.SEXE,
        CASE m.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
        m.QUALITE_EMPLOYE,
        m.NATURE_EMPLOI,
        m.UA_CODE,
        m.MOTIF_MVT
)
WHERE QUALITE_EMPLOYE = 'A'
  AND NATURE_EMPLOI = 'PERMANENT'
  AND ID_TEMPS = 20241231
GROUP BY CODE_TYPE_MVT, MOTIF_MVT
ORDER BY 1, 2;


-- -----------------------------------------------------------------------------
-- C4 — T60, temporaires, 2020 à 2025 : la maquette
-- Lecture  : premier chiffrage du tableau des temporaires. La fin d'une année doit être
--            le début de la suivante, et l'effectif de fin doit être celui des
--            temporaires du contrôle 1 (C2).
-- Résultat :
-- -----------------------------------------------------------------------------
SELECT ID_TEMPS,
       SUM(CASE WHEN CODE_TYPE_MVT = 'DEBUT' THEN NB_AGENTS ELSE 0 END) AS debut,
       SUM(CASE WHEN CODE_TYPE_MVT = 'RECRU' THEN NB_AGENTS ELSE 0 END) AS recrut,
       SUM(CASE WHEN CODE_NAT_MVT = 'ENTREES' AND CODE_TYPE_MVT <> 'RECRU'
                THEN NB_AGENTS ELSE 0 END)                             AS autres_ent,
       SUM(CASE WHEN CODE_TYPE_MVT = 'RET' THEN NB_AGENTS ELSE 0 END)   AS retraite,
       SUM(CASE WHEN CODE_TYPE_MVT = 'DEC' THEN NB_AGENTS ELSE 0 END)   AS deces,
       SUM(CASE WHEN CODE_NAT_MVT = 'SORTIES'
                 AND CODE_TYPE_MVT NOT IN ('RET', 'DEC')
                THEN NB_AGENTS ELSE 0 END)                             AS autres_sort,
       SUM(CASE WHEN CODE_NAT_MVT IN ('INITIAL', 'ENTREES') THEN NB_AGENTS
                WHEN CODE_NAT_MVT = 'SORTIES' THEN -NB_AGENTS
                ELSE 0 END)                                            AS fin
FROM (
    WITH
    parametres AS (
        SELECT
            'PRESENT'               AS fin_contrat_inconnue,
            18                      AS duree_contrat_mois,
            'PRESENT'               AS fiche_sans_etat,
            24                      AS fiche_sans_etat_mois,
            'JAMAIS_COMPTE'         AS inactif_sans_depart,
            'EMBAUCHE'              AS ordre_date_entree,
            'NON'                   AS passages_annee,
            'COMME_AVANT'           AS mouvements_internes
        FROM DUAL
    ),
    decisions_depart AS (
        SELECT '02' AS code, 'RET' AS type_mvt, 'PREMIER_JOUR_PARTI' AS date_effet, 'TOUTES' AS retenir,
               'admission à la retraite' AS libelle                                             FROM DUAL UNION ALL
        SELECT '34', 'DEC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'liquidation des droits après décès' FROM DUAL UNION ALL
        SELECT '35', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'fin de contrat'                     FROM DUAL UNION ALL
        SELECT '36', 'DEM',       'PREMIER_JOUR_PARTI', 'TOUTES', 'démission'                          FROM DUAL UNION ALL
        SELECT '52', 'FIN_CTRAT', 'PREMIER_JOUR_PARTI', 'TOUTES', 'rupture du contrat'                 FROM DUAL UNION ALL
        SELECT '54', 'LIC',       'PREMIER_JOUR_PARTI', 'TOUTES', 'licenciement'                       FROM DUAL
    ),
    absences AS (
        SELECT '07' AS code, 'HORS_EFFECTIF' AS effet, 'RETIREE' AS anciennete, 'DISP' AS type_mvt,
               'mise en disponibilité' AS libelle                                 FROM DUAL UNION ALL
        SELECT '14', 'PRESENT',       'COMPTEE', 'MISE_DIS', 'mise à disposition' FROM DUAL
    ),
    codes_decision AS (
        SELECT '40' AS annulation,
               '50' AS engagement_temporaire,
               '51' AS engagement_permanent,
               '18' AS remise_en_activite,
               '37' AS classement_detache
        FROM DUAL
    ),
    annees AS (
        SELECT 1899 + LEVEL AS annee
        FROM DUAL
        CONNECT BY LEVEL <= EXTRACT(YEAR FROM SYSDATE) - 1900
    ),
    dates AS (
        SELECT TO_DATE(TO_CHAR(a.annee) || '1231', 'YYYYMMDD')            AS date_arrete
        FROM annees a
        WHERE a.annee >= (SELECT MIN(EXTRACT(YEAR FROM s.DATE_DEBUT))
                          FROM USER_GRH.GRH_SITUATION s
                          WHERE 1 = 1
                            AND s.DATE_DEBUT IS NOT NULL
                            AND EXTRACT(YEAR FROM s.DATE_DEBUT) >= 1900)
    ),
    exercices AS (
        SELECT EXTRACT(YEAR FROM da.date_arrete)                          AS annee,
               ADD_MONTHS(da.date_arrete, -12)                            AS d0,
               da.date_arrete                                             AS d1
        FROM dates da
        WHERE ADD_MONTHS(da.date_arrete, -12) IN (SELECT date_arrete FROM dates)
    ),
    personnes AS (
        SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS,
               p.DATE_EMBAUCHE                                        AS date_embauche,
               p.DATE_PRISE_SERVICE                                   AS date_prise_service,
               NVL(p.ETAT_EMPLOYE, '-')                               AS etat,
               TO_CHAR(p.QUALITE)                                     AS qualite,
               CASE pr.ordre_date_entree
                   WHEN 'PRISE_SERVICE' THEN COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE)
                   ELSE                      COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
               END                                                    AS date_entree
        FROM USER_GRH.GRH_PERSONNE p
        CROSS JOIN parametres pr
        WHERE 1 = 1
    ),
    decisions AS (
        SELECT NVL(d.PERS_ID, dp.PERS_ID) AS PERS_ID,
               TO_CHAR(d.CDEC_CODE)       AS code,
               d.DATE_VIGUEUR             AS date_decision
        FROM USER_GRH.GRH_DECISION d
        LEFT JOIN USER_GRH.GRH_DECISION_PERSONNE dp
               ON dp.DECI_NO = d.DECI_NO
              AND 1 = 1
        CROSS JOIN codes_decision cd
        WHERE 1 = 1
          AND d.DATE_VIGUEUR IS NOT NULL
          AND (TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM decisions_depart)
               OR TO_CHAR(d.CDEC_CODE) IN (SELECT code FROM absences)
               OR TO_CHAR(d.CDEC_CODE) IN (cd.engagement_temporaire, cd.engagement_permanent,
                                           cd.remise_en_activite, cd.classement_detache))
          AND NOT EXISTS (SELECT 1
                          FROM USER_GRH.GRH_DECISION a
                          WHERE 1 = 1
                            AND TO_CHAR(a.CDEC_CODE) = cd.annulation
                            AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))
    ),
    departs AS (
        SELECT x.PERS_ID,
               x.date_decision
                   + CASE x.date_effet WHEN 'DERNIER_JOUR_TRAVAILLE' THEN 1 ELSE 0 END AS date_depart,
               x.type_mvt,
               'DECISION_' || x.code                                  AS motif
        FROM (SELECT dc.PERS_ID, dc.code, dc.date_decision,
                     dd.type_mvt, dd.date_effet, dd.retenir,
                     ROW_NUMBER() OVER (PARTITION BY dc.PERS_ID, dc.code
                                        ORDER BY dc.date_decision DESC) AS rang_fin
              FROM decisions dc
              JOIN decisions_depart dd ON dd.code = dc.code
              WHERE dc.PERS_ID IS NOT NULL) x
        WHERE x.retenir = 'TOUTES'
           OR x.rang_fin = 1
        UNION ALL
        SELECT PERS_ID, DATE_DECES, 'DEC', 'DATE_DECES'
        FROM USER_GRH.GRH_PERSONNE
        WHERE 1 = 1
          AND DATE_DECES IS NOT NULL
    ),
    premier_depart AS (
        SELECT PERS_ID, date_depart, type_mvt, motif
        FROM (SELECT x.PERS_ID, x.date_depart, x.type_mvt, x.motif,
                     ROW_NUMBER() OVER (PARTITION BY x.PERS_ID
                                        ORDER BY x.date_depart, x.motif) AS rn
              FROM departs x)
        WHERE rn = 1
    ),
    engagements AS (
        SELECT e.PERS_ID, e.date_decision AS debut,
               LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID
                                           ORDER BY e.date_decision) AS debut_suivant
        FROM decisions e
        CROSS JOIN codes_decision cd
        WHERE e.PERS_ID IS NOT NULL
          AND e.code = cd.engagement_temporaire
    ),
    contrats AS (
        SELECT x.PERS_ID, x.debut,
               CASE
                   WHEN x.fin_base < DATE '9999-12-31'       THEN x.fin_base
                   WHEN pr.fin_contrat_inconnue = 'DUREE'    THEN ADD_MONTHS(x.debut, pr.duree_contrat_mois)
                   WHEN pr.fin_contrat_inconnue = 'FICHE'
                    AND p.etat = 'INACTIF'                   THEN CAST(NULL AS DATE)
                   ELSE                                           x.fin_base
               END                                                    AS fin
        FROM (SELECT g.PERS_ID, g.debut,
                     LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),
                           NVL(g.debut_suivant, DATE '9999-12-31'))   AS fin_base
              FROM engagements g
              LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID
                                   AND dep.date_depart > g.debut
              GROUP BY g.PERS_ID, g.debut, g.debut_suivant) x
        JOIN personnes p ON p.PERS_ID = x.PERS_ID
        CROSS JOIN parametres pr
    ),
    temporaires_sous_contrat AS (
        SELECT DISTINCT da.date_arrete, c.PERS_ID
        FROM dates da
        JOIN contrats c ON c.debut       <= da.date_arrete
                       AND da.date_arrete <  c.fin
    ),
    remises AS (
        SELECT da.date_arrete, d.PERS_ID, MAX(d.date_decision) AS date_remise
        FROM dates da
        JOIN decisions d ON d.date_decision <= da.date_arrete
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.remise_en_activite
        GROUP BY da.date_arrete, d.PERS_ID
    ),
    absences_en_cours AS (
        SELECT x.date_arrete, x.PERS_ID, x.code, x.effet, x.type_mvt, x.date_absence
        FROM (SELECT da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt,
                     MAX(d.date_decision) AS date_absence
              FROM dates da
              JOIN decisions d ON d.date_decision <= da.date_arrete
              JOIN absences ab ON ab.code = d.code
              WHERE d.PERS_ID IS NOT NULL
              GROUP BY da.date_arrete, d.PERS_ID, ab.code, ab.effet, ab.type_mvt) x
        LEFT JOIN remises r ON r.date_arrete = x.date_arrete
                           AND r.PERS_ID     = x.PERS_ID
        WHERE x.date_absence > NVL(r.date_remise, DATE '1900-01-01')
    ),
    positions AS (
        SELECT date_arrete, PERS_ID,
               MAX(CASE WHEN effet = 'HORS_EFFECTIF' THEN 1 ELSE 0 END)              AS hors_effectif,
               MAX(CASE WHEN effet IN ('HORS_EFFECTIF', 'INACTIF') THEN 1 ELSE 0 END) AS inactif
        FROM absences_en_cours
        GROUP BY date_arrete, PERS_ID
    ),
    absence_sans_retour AS (
        SELECT x.PERS_ID, x.date_absence
        FROM (SELECT d.PERS_ID, MAX(d.date_decision) AS date_absence
              FROM decisions d
              JOIN absences ab ON ab.code = d.code
              WHERE d.PERS_ID IS NOT NULL
                AND ab.effet = 'HORS_EFFECTIF'
              GROUP BY d.PERS_ID) x
        WHERE NOT EXISTS (SELECT 1
                          FROM decisions r, codes_decision cd
                          WHERE r.PERS_ID       =  x.PERS_ID
                            AND r.code          =  cd.remise_en_activite
                            AND r.date_decision >= x.date_absence)
          AND NOT EXISTS (SELECT 1
                          FROM departs dep
                          WHERE dep.PERS_ID     = x.PERS_ID
                            AND dep.date_depart > x.date_absence)
    ),
    situations AS (
        SELECT da.date_arrete, s.PERS_ID,
               s.QUAL_CODE, s.CODE_DOMAINE_ACT, s.UA_CODE, s.LIEU_ID, s.FNCT_CODE,
               s.STATUT_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS nature,
               TO_CHAR(s.CATEGORIE)                                   AS categorie,
               ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID
                                  ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn
        FROM dates da
        JOIN USER_GRH.GRH_SITUATION s ON 1 = 1
                                     AND s.DATE_DEBUT <= da.date_arrete
    ),
    presents AS (
        SELECT da.date_arrete, p.PERS_ID, p.SEXE,
               p.qualite                                              AS QUALITE_EMPLOYE,
               si.nature                                              AS NATURE_EMPLOI,
               si.QUAL_CODE, si.CODE_DOMAINE_ACT, si.UA_CODE, si.LIEU_ID, si.FNCT_CODE,
               si.STATUT_EMPLOYE,
               si.categorie                                           AS CATEGORIE,
               NVL(po.inactif, 0)                                     AS en_absence
        FROM dates da
        CROSS JOIN personnes p
        CROSS JOIN parametres pr
        LEFT JOIN situations si               ON si.date_arrete = da.date_arrete
                                             AND si.PERS_ID     = p.PERS_ID
                                             AND si.rn          = 1
        LEFT JOIN premier_depart pd           ON pd.PERS_ID     = p.PERS_ID
        LEFT JOIN absence_sans_retour asr     ON asr.PERS_ID    = p.PERS_ID
        LEFT JOIN positions po                ON po.date_arrete = da.date_arrete
                                             AND po.PERS_ID     = p.PERS_ID
        LEFT JOIN temporaires_sous_contrat tc ON tc.date_arrete = da.date_arrete
                                             AND tc.PERS_ID     = p.PERS_ID
        WHERE
                (NVL(si.nature, '-') <> 'TEMPORAIRE'
                 AND p.date_entree <= da.date_arrete
                 AND NVL(po.hors_effectif, 0) = 0
                 AND (
                         NOT (p.etat = 'INACTIF'
                              OR (p.etat = '-'
                                  AND pr.fiche_sans_etat = 'RECENTE'
                                  AND p.date_entree <= ADD_MONTHS(da.date_arrete,
                                                                  -pr.fiche_sans_etat_mois)))
                      OR pd.date_depart > da.date_arrete
                      OR (pr.inactif_sans_depart = 'ABSENCE'
                          AND asr.date_absence > da.date_arrete)))
             OR
                (si.nature = 'TEMPORAIRE'
                 AND tc.PERS_ID IS NOT NULL)
    ),
    au_debut AS (
        SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
               p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
        FROM exercices e
        JOIN presents p ON p.date_arrete = e.d0
    ),
    a_la_fin AS (
        SELECT e.annee, e.d0, e.d1, p.PERS_ID, p.SEXE, p.QUALITE_EMPLOYE,
               p.NATURE_EMPLOI, p.QUAL_CODE, p.UA_CODE
        FROM exercices e
        JOIN presents p ON p.date_arrete = e.d1
    ),
    dernier_depart_annee AS (
        SELECT annee, PERS_ID, type_mvt, motif
        FROM (SELECT e.annee, dep.PERS_ID, dep.type_mvt, dep.motif,
                     ROW_NUMBER() OVER (PARTITION BY e.annee, dep.PERS_ID
                                        ORDER BY dep.date_depart DESC, dep.motif DESC) AS rn
              FROM exercices e
              JOIN departs dep ON dep.date_depart >  e.d0
                              AND dep.date_depart <= e.d1)
        WHERE rn = 1
    ),
    detaches_annee AS (
        SELECT DISTINCT e.annee, d.PERS_ID
        FROM exercices e
        JOIN decisions d ON d.date_decision >  e.d0
                        AND d.date_decision <= e.d1
        CROSS JOIN codes_decision cd
        WHERE d.PERS_ID IS NOT NULL
          AND d.code = cd.classement_detache
    ),
    entrees AS (
        SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
               x.QUAL_CODE, x.UA_CODE,
               CASE x.cas
                   WHEN 'DETACHEMENT' THEN 'DETAC'
                   WHEN 'EMBAUCHE'    THEN 'RECRU'
                   WHEN 'ENGAGEMENT'  THEN 'RECRU'
                   WHEN 'REMISE'      THEN 'REM'
                   ELSE                    'AUTRE_ENT'
               END                                                    AS CODE_TYPE_MVT,
               CASE x.cas
                   WHEN 'DETACHEMENT' THEN 'DECISION_' || x.code_detache
                   WHEN 'EMBAUCHE'    THEN 'EMBAUCHE'
                   WHEN 'ENGAGEMENT'  THEN 'ENGAGEMENT_' || x.code_engagement
                   WHEN 'REMISE'      THEN 'DECISION_' || x.code_remise
                   WHEN 'NATURE'      THEN 'ETAIT_' || NVL(x.nature_debut, 'SANS_SITUATION')
                   ELSE                    'SANS_MOTIF'
               END                                                    AS MOTIF_MVT
        FROM (SELECT f.*,
                     dbt.NATURE_EMPLOI          AS nature_debut,
                     cd.classement_detache      AS code_detache,
                     cd.engagement_temporaire   AS code_engagement,
                     cd.remise_en_activite      AS code_remise,
                     CASE
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND dt.PERS_ID IS NOT NULL                       THEN 'DETACHEMENT'
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND pe.date_entree >  f.d0
                          AND pe.date_entree <= f.d1                       THEN 'EMBAUCHE'
                         WHEN NVL(f.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND NVL(po.hors_effectif, 0) = 1                 THEN 'REMISE'
                         WHEN f.NATURE_EMPLOI = 'TEMPORAIRE'
                          AND tc0.PERS_ID IS NULL                          THEN 'ENGAGEMENT'
                         WHEN dbt.PERS_ID IS NOT NULL                      THEN 'NATURE'
                         ELSE                                                   'SANS_MOTIF'
                     END                        AS cas
              FROM a_la_fin f
              JOIN personnes pe ON pe.PERS_ID = f.PERS_ID
              CROSS JOIN codes_decision cd
              LEFT JOIN au_debut dbt                 ON dbt.annee        = f.annee
                                                    AND dbt.PERS_ID      = f.PERS_ID
              LEFT JOIN detaches_annee dt            ON dt.annee         = f.annee
                                                    AND dt.PERS_ID       = f.PERS_ID
              LEFT JOIN positions po                 ON po.date_arrete   = f.d0
                                                    AND po.PERS_ID       = f.PERS_ID
              LEFT JOIN temporaires_sous_contrat tc0 ON tc0.date_arrete  = f.d0
                                                    AND tc0.PERS_ID      = f.PERS_ID
              WHERE dbt.PERS_ID IS NULL
                 OR NVL(dbt.NATURE_EMPLOI, '-') <> NVL(f.NATURE_EMPLOI, '-')) x
    ),
    sorties AS (
        SELECT x.annee, x.PERS_ID, x.SEXE, x.QUALITE_EMPLOYE, x.NATURE_EMPLOI,
               x.QUAL_CODE, x.UA_CODE,
               CASE x.cas
                   WHEN 'DEPART_PERMANENT'  THEN x.type_premier
                   WHEN 'DEPART_TEMPORAIRE' THEN x.type_dernier
                   WHEN 'ABSENCE'           THEN NVL(x.type_absence, 'DISP')
                   ELSE                          'AUTRE_SORT'
               END                                                    AS CODE_TYPE_MVT,
               CASE x.cas
                   WHEN 'DEPART_PERMANENT'  THEN x.motif_premier
                   WHEN 'DEPART_TEMPORAIRE' THEN x.motif_dernier
                   WHEN 'ABSENCE'           THEN 'DECISION_' || x.code_absence
                   WHEN 'NATURE'            THEN 'DEVIENT_' || NVL(x.nature_fin, 'SANS_SITUATION')
                   ELSE                          'SANS_MOTIF'
               END                                                    AS MOTIF_MVT
        FROM (SELECT dbt.*,
                     fin.NATURE_EMPLOI          AS nature_fin,
                     pd.type_mvt                AS type_premier,
                     pd.motif                   AS motif_premier,
                     dda.type_mvt               AS type_dernier,
                     dda.motif                  AS motif_dernier,
                     ae.code                    AS code_absence,
                     ae.type_mvt                AS type_absence,
                     CASE
                         WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND pd.date_depart >  dbt.d0
                          AND pd.date_depart <= dbt.d1                     THEN 'DEPART_PERMANENT'
                         WHEN dbt.NATURE_EMPLOI = 'TEMPORAIRE'
                          AND dda.PERS_ID IS NOT NULL                      THEN 'DEPART_TEMPORAIRE'
                         WHEN NVL(dbt.NATURE_EMPLOI, '-') <> 'TEMPORAIRE'
                          AND NVL(po.hors_effectif, 0) = 1                 THEN 'ABSENCE'
                         WHEN fin.PERS_ID IS NOT NULL                      THEN 'NATURE'
                         ELSE                                                   'SANS_MOTIF'
                     END                        AS cas
              FROM au_debut dbt
              LEFT JOIN a_la_fin fin             ON fin.annee      = dbt.annee
                                                AND fin.PERS_ID    = dbt.PERS_ID
              LEFT JOIN premier_depart pd        ON pd.PERS_ID     = dbt.PERS_ID
              LEFT JOIN dernier_depart_annee dda ON dda.annee      = dbt.annee
                                                AND dda.PERS_ID    = dbt.PERS_ID
              LEFT JOIN positions po             ON po.date_arrete = dbt.d1
                                                AND po.PERS_ID     = dbt.PERS_ID
              LEFT JOIN absences_en_cours ae     ON ae.date_arrete = dbt.d1
                                                AND ae.PERS_ID     = dbt.PERS_ID
                                                AND ae.effet       = 'HORS_EFFECTIF'
              WHERE fin.PERS_ID IS NULL
                 OR NVL(fin.NATURE_EMPLOI, '-') <> NVL(dbt.NATURE_EMPLOI, '-')) x
    ),
    contrats_annee AS (
        SELECT DISTINCT e.annee, c.PERS_ID
        FROM exercices e
        JOIN contrats c ON c.debut >  e.d0
                       AND c.debut <= e.d1
                       AND c.fin   <= e.d1
                       AND c.fin   <  DATE '9999-12-31'
    ),
    passages AS (
        SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite AS QUALITE_EMPLOYE,
               si.nature AS NATURE_EMPLOI, si.QUAL_CODE, si.UA_CODE,
               'EMBAUCHE' AS motif_entree, pd.type_mvt AS type_sortie, pd.motif AS motif_sortie
        FROM exercices e
        CROSS JOIN parametres pr
        JOIN personnes p       ON p.date_entree  >  e.d0
                              AND p.date_entree  <= e.d1
        JOIN premier_depart pd ON pd.PERS_ID     =  p.PERS_ID
                              AND pd.date_depart >  p.date_entree
                              AND pd.date_depart <= e.d1
        LEFT JOIN situations si ON si.date_arrete = e.d1
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
        LEFT JOIN au_debut dbt  ON dbt.annee      = e.annee
                               AND dbt.PERS_ID    = p.PERS_ID
        LEFT JOIN a_la_fin fin  ON fin.annee      = e.annee
                               AND fin.PERS_ID    = p.PERS_ID
        WHERE pr.passages_annee = 'OUI'
          AND p.etat = 'INACTIF'
          AND NVL(si.nature, '-') <> 'TEMPORAIRE'
          AND dbt.PERS_ID IS NULL
          AND fin.PERS_ID IS NULL
        UNION ALL
        SELECT e.annee, p.PERS_ID, p.SEXE, p.qualite,
               si.nature, si.QUAL_CODE, si.UA_CODE,
               'ENGAGEMENT_' || cd.engagement_temporaire,
               NVL(dda.type_mvt, 'AUTRE_SORT'), NVL(dda.motif, 'SANS_MOTIF')
        FROM contrats_annee ca
        JOIN exercices e        ON e.annee        = ca.annee
        JOIN personnes p        ON p.PERS_ID      = ca.PERS_ID
        CROSS JOIN parametres pr
        CROSS JOIN codes_decision cd
        LEFT JOIN situations si ON si.date_arrete = e.d1
                               AND si.PERS_ID     = p.PERS_ID
                               AND si.rn          = 1
        LEFT JOIN dernier_depart_annee dda ON dda.annee   = ca.annee
                                          AND dda.PERS_ID = ca.PERS_ID
        LEFT JOIN au_debut dbt  ON dbt.annee      = ca.annee
                               AND dbt.PERS_ID    = ca.PERS_ID
        LEFT JOIN a_la_fin fin  ON fin.annee      = ca.annee
                               AND fin.PERS_ID    = ca.PERS_ID
        WHERE pr.passages_annee = 'OUI'
          AND si.nature = 'TEMPORAIRE'
          AND dbt.PERS_ID IS NULL
          AND fin.PERS_ID IS NULL
    ),
    internes AS (
        SELECT x.annee, x.CODE_TYPE_MVT, x.MVT_ID, x.PERS_ID, p.SEXE,
               TO_CHAR(p.QUALITE)                                     AS QUALITE_EMPLOYE,
               TO_CHAR(s.NATURE_EMPLOIE)                              AS NATURE_EMPLOI,
               s.QUAL_CODE, s.UA_CODE
        FROM (SELECT m.MVT_ID, m.PERS_ID, m.SITU_ID,
                     EXTRACT(YEAR FROM m.DATE_DEBUT)                  AS annee,
                     CASE UPPER(TRIM(m.NATURE_MVT))
                         WHEN 'DIS' THEN 'DISP'
                         WHEN 'DET' THEN 'DETAC'
                         ELSE UPPER(TRIM(m.NATURE_MVT))
                     END                                              AS CODE_TYPE_MVT
              FROM USER_GRH.GRH_MOUVEMENT m
              WHERE 1 = 1
                AND m.DATE_DEBUT IS NOT NULL
                AND m.VALIDE      = 'O') x
        CROSS JOIN parametres pr
        LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm ON tm.CODE_TYPE_MVT = x.CODE_TYPE_MVT
        LEFT JOIN USER_GRH.GRH_PERSONNE p       ON p.PERS_ID = x.PERS_ID
                                               AND 1 = 1
        LEFT JOIN USER_GRH.GRH_SITUATION s      ON s.SITU_ID = x.SITU_ID
                                               AND 1 = 1
        WHERE pr.mouvements_internes = 'COMME_AVANT'
          AND NVL(tm.CODE_NAT_MVT, '-') NOT IN ('INITIAL', 'ENTREES', 'SORTIES')
    ),
    mouvements AS (
        SELECT annee, 'DEBUT' AS CODE_TYPE_MVT, PERS_ID AS id_mvt, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE,
               CAST(NULL AS VARCHAR2(30)) AS MOTIF_MVT
        FROM au_debut
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
        FROM entrees
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, MOTIF_MVT
        FROM sorties
        UNION ALL
        SELECT annee, 'RECRU', PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_entree
        FROM passages
        UNION ALL
        SELECT annee, type_sortie, PERS_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, motif_sortie
        FROM passages
        UNION ALL
        SELECT annee, CODE_TYPE_MVT, MVT_ID, PERS_ID, SEXE,
               QUALITE_EMPLOYE, NATURE_EMPLOI, QUAL_CODE, UA_CODE, 'GRH_MOUVEMENT'
        FROM internes
    )
    SELECT
        TO_NUMBER(TO_CHAR(m.annee) || '1231')                 AS ID_TEMPS,
        m.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT
            WHEN 'ENTREES' THEN 'PRISE'
            WHEN 'SORTIES' THEN 'CESSA'
            ELSE NULL
        END                                                   AS CODE_SENS_MVT,
        m.QUAL_CODE,
        m.SEXE,
        CASE m.SEXE
            WHEN 'M' THEN 'Masculin'
            WHEN 'F' THEN 'Féminin'
            ELSE 'NON RENSEIGNE'
        END                                                   AS LIBELLE_SEXE,
        COUNT(m.id_mvt)                                       AS NB_MOUVEMENTS,
        COUNT(DISTINCT m.PERS_ID)                             AS NB_AGENTS,
        SYSDATE                                               AS DATE_CHARGEMENT,
        0                                                    AS CLICHE,
        m.QUALITE_EMPLOYE,
        m.NATURE_EMPLOI,
        m.UA_CODE,
        m.MOTIF_MVT
    FROM mouvements m
    LEFT JOIN USER_GRH.GRH_TYPE_MOUVEMENT tm
           ON tm.CODE_TYPE_MVT = m.CODE_TYPE_MVT
    GROUP BY
        TO_NUMBER(TO_CHAR(m.annee) || '1231'),
        m.CODE_TYPE_MVT,
        tm.CODE_NAT_MVT,
        CASE tm.CODE_NAT_MVT WHEN 'ENTREES' THEN 'PRISE' WHEN 'SORTIES' THEN 'CESSA' ELSE NULL END,
        m.QUAL_CODE,
        m.SEXE,
        CASE m.SEXE WHEN 'M' THEN 'Masculin' WHEN 'F' THEN 'Féminin' ELSE 'NON RENSEIGNE' END,
        m.QUALITE_EMPLOYE,
        m.NATURE_EMPLOI,
        m.UA_CODE,
        m.MOTIF_MVT
)
WHERE QUALITE_EMPLOYE = 'A'
  AND NATURE_EMPLOI = 'TEMPORAIRE'
  AND ID_TEMPS BETWEEN 20201231 AND 20251231
GROUP BY ID_TEMPS
ORDER BY 1;
