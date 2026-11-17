-- =============================================================================
-- Point 137. Les décès immédiats d'accident (T57)
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « DOSSIER, DOS_DECES_IMMEDIAT : rempli pour 2 % des
--   accidents de 2024 et 65 % de ceux de 2025. TSINISTRE a un champ du même
--   nom, pas encore lu. » ; « pour les accidents de 2024, 9 victimes mortes
--   dans l'année dans la base. L'annuaire CNSS 2024 compte autrement : la
--   mort sur le coup, 14 décès immédiats (son tableau 49). »
-- Requête : les dossiers d'accident du travail par année de l'accident et
--   valeur de DOS_DECES_IMMEDIAT.
-- Reprise de : lot8_evolution_5_ans.sql, P100 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t50_t57_requete_proposee.sql, bloc victimes (une
--   victime morte dans l'année qui suit son accident). Fiche des règles :
--   règle 76.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Dossiers d'accident du travail (TDOS_CODE = A) par année de l'accident et
-- valeur de DOS_DECES_IMMEDIAT : dossiers, victime décédée, décédée dans
-- l'année, avec une rente de survivant.
-- -----------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM d.DOS_DATE_ACCIDENT)                     AS annee_accident,
       NVL(TO_CHAR(d.DOS_DECES_IMMEDIAT), '(vide)')               AS deces_immediat,
       COUNT(*)                                                   AS dossiers,
       SUM(CASE WHEN i.IND_DATE_DECES IS NOT NULL THEN 1 ELSE 0 END) AS victime_decedee,
       SUM(CASE WHEN i.IND_DATE_DECES >= d.DOS_DATE_ACCIDENT
                 AND i.IND_DATE_DECES <  d.DOS_DATE_ACCIDENT + 366
                THEN 1 ELSE 0 END)                                AS decedee_dans_l_annee,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM USER_DWH.PRESTATION_ESP p
                             WHERE p.DOS_CODE = d.DOS_CODE
                               AND p.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA'))
                THEN 1 ELSE 0 END)                                AS avec_rente_survivant
FROM USER_DWH.DOSSIER d
LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = d.IND_ID
WHERE d.TDOS_CODE = 'A'
  AND d.DOS_DATE_ACCIDENT >= DATE '2020-01-01'
  AND d.DOS_DATE_ACCIDENT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM d.DOS_DATE_ACCIDENT), NVL(TO_CHAR(d.DOS_DECES_IMMEDIAT), '(vide)')
ORDER BY annee_accident, deces_immediat;

-- Résultat obtenu (P100, exécutée le 01/11/2026 ; détail dans
-- resultats/lot8_P100_deces_immediat.tsv) :
--   Année   N      O    vide    Décédées dans l'année
--   2024    22     3    1 167   9 (1 + 8)
--   2025    753    6    411     1
-- Face au tableau 49 de l'annuaire CNSS 2024 : 14 décès immédiats en 2024.
-- Lecture (calculé ici) : DOS_DECES_IMMEDIAT est rempli pour 25 accidents de
--   2024 sur 1 192 (2 %) et 759 de 2025 sur 1 170 (65 %).
