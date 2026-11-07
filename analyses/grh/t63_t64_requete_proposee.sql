-- =============================================================================
-- DTM_INDICATEURS_FINANCES_GRH — PROPOSITION du 07/11/2026 (T63, T64)
-- MÊME STRUCTURE que domaines/grh/dtm/sql/indicateurs_finances_grh.sql : mêmes
-- colonnes, même grain (AN_ID, une ligne par année), même paramètre :1.
-- TX_FRAIS_PERSONNEL et TX_FORMATION, vides jusqu'ici, sont enfin calculés.
-- Sept colonnes s'ajoutent à la fin, aucune n'est retirée.
-- Après le visa de la DFC, ce fichier remplace indicateurs_finances_grh.sql.
-- Avant : déployer t63_t64_deploiement.sql (copie des montants dans le DWH,
-- colonnes ajoutées dans DTM et ODS), et que la DSI ait chargé
-- STG_GL.BI_GRH_SOLDE_COMPTABLE (voir t63_t64_structure_comptable.sql).
--
-- Sources : DWH.FAIT_GRH_SOLDE_COMPTABLE (copie de la table de la DSI, même
--           cliché) ; DTM.DTM_GRH_EFFECTIF (même cliché, chargé avant) ;
--           DTM.DIM_ANNEE.
-- :1 = CLICHE (MMYYYY), saisi sans guillemets.
-- Grain   : AN_ID (une ligne par exercice).
--
-- CE QUE DISENT LES SOURCES (dans l'ordre de priorité)
--   Recueil des besoins v7, RH lignes 6 et 7 (T63, T64) : « Voir la DFC »,
--     « Source : Oracle GL », déclinaison Direction, Agence, Bureau,
--     périodicité annuelle ; frais de personnel = 65 des gestions G et S ;
--     dépense de formation = « 63, 86 et 87 » des gestions G et S ; masse
--     salariale = 651 et 652. Indicateur lié : « Ratio des dépenses de
--     formation rapportées à la masse salariale annuelle (CIPRES) », source
--     DFC, R = formation / masse salariale × 100.
--   Recueil v7, Finance ligne 1 (T66) : charges de fonctionnement = 61xxx à
--     66xxx, « uniquement la gestion G ».
--   Kaya 2017 : données extraites de GL ; masse salariale = compte 651 seul ;
--     T64 se faisait à la main sous Excel, à intégrer dans la BI.
--   Annuaire CIPRES : 5 dernières années, fin d'exercice ; T63 = FP / CF
--     (norme : 85 % au plus) ; T64 = FOR / MSAL (norme : 1 à 5 %) et
--     FOR / BUD ; « *Comptes 651 et 652 » pour la masse salariale.
--   Grille « Evaluation des Ratios CIPRES 2025 » de la CNSS (non classée) :
--     formation = 63812, 6386 et 6577 ; CF = 61 à 68, gestion G ou 7.
--
-- LES TABLEAUX DANS OBIEE (filtre EXERCICE_CLOS = 'O', 5 derniers AN_ID) :
--   T63 : AN_ID en lignes ; MT_CHARGES_FONCT, MT_FRAIS_PERSONNEL,
--         TX_FRAIS_PERSONNEL en colonnes ;
--   T64 : AN_ID en lignes ; MT_FORMATION, MT_MASSE_SALARIALE, MT_TOTAL_BUDGET,
--         TX_FORMATION, TX_FORMATION_BUDGET en colonnes.
--   Les taux sont des fractions arrondies à 4 décimales (0,4880), comme
--   TX_ENCADREMENT : OBIEE les affiche en % avec 2 décimales (recueil, B35).
--
-- COLONNES : celles de indicateurs_finances_grh.sql, dans le même ordre :
--   AN_ID, TX_ENCADREMENT, TX_FRAIS_PERSONNEL, TX_CHARGES_ADMIN, TX_FORMATION,
--   TX_EXECUTION_BUDGETAIRE, NB_CADRES, NB_EFFECTIF_TOTAL, OBSERVATION,
--   CLICHE, DATE_CHARGEMENT
-- puis les colonnes AJOUTÉES :
--   MT_CHARGES_FONCT     charges de fonctionnement (CF), FCFA — T63
--   MT_FRAIS_PERSONNEL   frais de personnel (FP), FCFA — T63
--   MT_FORMATION         dépenses de formation (FOR), FCFA — T64
--   MT_MASSE_SALARIALE   masse salariale (MSAL), FCFA — T64
--   MT_TOTAL_BUDGET      total budget (BUD), FCFA — T64
--   TX_FORMATION_BUDGET  FOR / BUD — T64 (second ratio)
--   EXERCICE_CLOS        O si les 12 mois de l'exercice sont clos, sinon N
-- Ce qui ne change pas :
--   - TX_ENCADREMENT, NB_CADRES, NB_EFFECTIF_TOTAL : calcul d'avant, à
--     l'identique. T65 se lit désormais dans DTM_GRH_EFFECTIF (NB_CADRES,
--     règle G6 de t58_t65_effectif_requete_proposee.sql) ;
--   - TX_CHARGES_ADMIN, TX_EXECUTION_BUDGETAIRE : restent vides. Ce sont des
--     ratios des tableaux de la Finance (T66, T69), hors du domaine GRH.
--
-- RÈGLES
--   · F1 Montant d'un compte
--     Montant net = débit − crédit, sur tous les mois de l'exercice (1 à 12,
--     et 13 pour les écritures d'inventaire s'il y en a).
--     Réalisé pour FP, CF, FOR, MSAL ; budget pour BUD.
--
--   · F2 Rubriques
--     Une rubrique = une liste de débuts de compte et une liste de gestions
--     (réglages rubriques et comptes_rubrique). Un compte qui commence par
--     deux débuts de la même rubrique n'est compté qu'une fois.
--     Valeurs par défaut, d'après le recueil des besoins v7 :
--       FP   comptes 65                  gestions G et S (G, 7, S, 5)
--       MSAL comptes 651 et 652          gestions G et S
--       FOR  comptes 63, 86, 87 (à la    gestions G et S
--            lettre, question QF2)
--       CF   comptes 61 à 66             gestion G (G, 7)
--       BUD  comptes 6 et 2 budgétés     toutes les gestions (proposition
--            (fonctionnement et           BI, question QF4)
--            investissement)
--
--   · F3 Exercice clos
--     EXERCICE_CLOS = 'O' quand les mois 1 à 12 du réalisé sont tous clos
--     dans Oracle GL (PERIODE_CLOSE = 'O'). Sinon les montants sont donnés
--     (suivi en cours d'année), mais les taux restent vides (réglage
--     taux_exercice_ouvert = VIDES), car T63 et T64 sont de fin d'exercice.
--
--   · F4 Taux
--     TX_FRAIS_PERSONNEL = FP / CF ; TX_FORMATION = FOR / MSAL ;
--     TX_FORMATION_BUDGET = FOR / BUD. Vides si le dénominateur est nul ou
--     absent.
--
--   · F5 Années
--     Les années de DTM_GRH_EFFECTIF (comme avant) et celles qui ont des
--     montants comptables, si DIM_ANNEE les connaît.
--
-- RÉGLAGES (partie 0) : chacun peut changer sans réécrire la requête.
--   · comptes_rubrique   : une ligne par rubrique et début de compte. Pour la
--                          lecture de la grille, remplacer les lignes de FOR
--                          par 63812, 6386 et 6577, et celles de CF par 61
--                          à 68 ; pour Kaya, retirer la ligne MSAL 652.
--   · rubriques          : la nature du montant et les gestions (liste
--                          séparée par des virgules ; * = toutes).
--   · taux_exercice_ouvert : VIDES (taux seulement pour un exercice clos) ou
--                          CALCULES (taux aussi en cours d'exercice).
--
-- QUESTIONS À LA DFC (non bloquantes : les réglages ci-dessus en attendant)
--   QF1 Charges de fonctionnement : 61 à 66 (recueil, T66) ou 61 à 68
--       (grille) ? gestion G seule, ou G et S comme les frais de personnel ?
--       Avec FP sur G et S et CF sur G seule, T63 compare deux périmètres.
--   QF2 Formation : que veut dire « 63, 86 et 87 » ? À la lettre, le compte
--       63 compte tous les services consommés. Est-ce 6386 et 6387 ? Ou les
--       comptes de la grille, 63812, 6386 et 6577 ?
--   QF3 Masse salariale : 651 et 652 (recueil, Annuaire) ou 651 seul (Kaya) ?
--       quelles gestions ?
--   QF4 Total budget : quelles gestions ? fonctionnement seul, ou aussi
--       l'investissement ? budget définitif ou initial ?
--   QF5 Déclinaison par direction, agence et bureau (recueil) : les charges
--       portent-elles une section analytique ? Si oui, une table DTM de plus,
--       au grain exercice × unité, viendra à côté de celle-ci (celle-ci garde
--       une ligne par année, pour ne rien casser dans OBIEE).
--
-- CONTRÔLE dans SQL Developer (cliché du premier chargement), en entourant la
-- requête :
--   SELECT AN_ID, EXERCICE_CLOS, MT_CHARGES_FONCT, MT_FRAIS_PERSONNEL,
--          TX_FRAIS_PERSONNEL, MT_FORMATION, MT_MASSE_SALARIALE,
--          TX_FORMATION, MT_TOTAL_BUDGET, TX_FORMATION_BUDGET, OBSERVATION
--   FROM ( …la requête… ) ORDER BY AN_ID;
-- Attendu : les mêmes montants que le contrôle C3 de
--   t63_t64_structure_comptable.sql, lecture « recueil » ; FP / CF au plus
--   0,85 ; FOR / MSAL entre 0,01 et 0,05 (sinon, revoir QF2 avec la DFC).
-- =============================================================================

WITH

-- ── 0. Les réglages ─────────────────────────────────────────────────────────
parametres AS (
    SELECT -- F3 : taux d'un exercice pas encore clos, 'VIDES' ou 'CALCULES'
           'VIDES'                 AS taux_exercice_ouvert
    FROM DUAL
),
-- F1, F2 : la nature du montant et les gestions de chaque rubrique
-- (gestions séparées par des virgules, lettre ou chiffre ; * = toutes)
rubriques AS (
    SELECT 'FP'   AS rubrique, 'REALISE' AS nature, 'G,7,S,5' AS gestions FROM DUAL UNION ALL
    SELECT 'MSAL',             'REALISE',           'G,7,S,5'             FROM DUAL UNION ALL
    SELECT 'FOR',              'REALISE',           'G,7,S,5'             FROM DUAL UNION ALL
    SELECT 'CF',               'REALISE',           'G,7'                 FROM DUAL UNION ALL
    SELECT 'BUD',              'BUDGET',            '*'                   FROM DUAL
),
-- F2 : les débuts de compte de chaque rubrique ; une ligne par début
comptes_rubrique AS (
    -- FP : frais de personnel (recueil v7, RH ligne 6)
    SELECT 'FP'   AS rubrique, '65'  AS prefixe FROM DUAL UNION ALL
    -- MSAL : masse salariale (recueil v7, RH ligne 6 ; Annuaire) ; Kaya : 651 seul
    SELECT 'MSAL', '651' FROM DUAL UNION ALL
    SELECT 'MSAL', '652' FROM DUAL UNION ALL
    -- FOR : dépenses de formation, « 63, 86 et 87 » à la lettre (recueil v7,
    -- RH ligne 6) ; grille de la CNSS : 63812, 6386, 6577 (question QF2)
    SELECT 'FOR',  '63'  FROM DUAL UNION ALL
    SELECT 'FOR',  '86'  FROM DUAL UNION ALL
    SELECT 'FOR',  '87'  FROM DUAL UNION ALL
    -- CF : charges de fonctionnement 61 à 66 (recueil v7, Finance ligne 1) ;
    -- grille : 61 à 68 (question QF1)
    SELECT 'CF',   '61'  FROM DUAL UNION ALL
    SELECT 'CF',   '62'  FROM DUAL UNION ALL
    SELECT 'CF',   '63'  FROM DUAL UNION ALL
    SELECT 'CF',   '64'  FROM DUAL UNION ALL
    SELECT 'CF',   '65'  FROM DUAL UNION ALL
    SELECT 'CF',   '66'  FROM DUAL UNION ALL
    -- BUD : total budget, charges (6) et investissements (2) budgétés
    -- (proposition BI, question QF4)
    SELECT 'BUD',  '6'   FROM DUAL UNION ALL
    SELECT 'BUD',  '2'   FROM DUAL
),

-- ── 1. Les montants de chaque rubrique, par exercice (F1, F2) ──────────────
-- Une ligne de la table compte dans une rubrique si sa nature, sa gestion et
-- le début de son compte conviennent. EXISTS : un compte n'est compté qu'une
-- fois, même s'il commence par deux débuts de la rubrique.
montants AS (
    SELECT s.EXERCICE, r.rubrique,
           SUM(s.MONTANT_DEBIT - s.MONTANT_CREDIT)                AS montant
    FROM DWH.FAIT_GRH_SOLDE_COMPTABLE s
    JOIN rubriques r ON r.nature = s.NATURE_MONTANT
                    AND (r.gestions = '*'
                         OR INSTR(',' || r.gestions || ',',
                                  ',' || s.CODE_GESTION || ',') > 0)
    WHERE s.CLICHE = :1
      AND EXISTS (SELECT 1
                  FROM comptes_rubrique c
                  WHERE c.rubrique = r.rubrique
                    AND s.COMPTE LIKE c.prefixe || '%')
    GROUP BY s.EXERCICE, r.rubrique
),

-- ── 2. Les exercices clos (F3) ─────────────────────────────────────────────
-- Nombre de mois 1 à 12 du réalisé clos dans Oracle GL.
clotures AS (
    SELECT EXERCICE,
           COUNT(DISTINCT CASE WHEN PERIODE_CLOSE = 'O' AND MOIS BETWEEN 1 AND 12
                               THEN MOIS END)                     AS mois_clos
    FROM DWH.FAIT_GRH_SOLDE_COMPTABLE
    WHERE CLICHE = :1
      AND NATURE_MONTANT = 'REALISE'
    GROUP BY EXERCICE
),

-- ── 3. Une ligne par exercice : les cinq montants ──────────────────────────
finances AS (
    SELECT m.EXERCICE                                             AS annee,
           SUM(CASE WHEN m.rubrique = 'CF'   THEN m.montant END)  AS mt_cf,
           SUM(CASE WHEN m.rubrique = 'FP'   THEN m.montant END)  AS mt_fp,
           SUM(CASE WHEN m.rubrique = 'FOR'  THEN m.montant END)  AS mt_for,
           SUM(CASE WHEN m.rubrique = 'MSAL' THEN m.montant END)  AS mt_msal,
           SUM(CASE WHEN m.rubrique = 'BUD'  THEN m.montant END)  AS mt_bud
    FROM montants m
    GROUP BY m.EXERCICE
),

-- ── 4. Les années à publier (F5) ───────────────────────────────────────────
-- Comme avant, les années de DTM_GRH_EFFECTIF ; en plus, celles qui ont des
-- montants comptables. Seulement si DIM_ANNEE les connaît (clé AN_ID).
ANNEES AS (
    SELECT annee
    FROM (SELECT TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))       AS annee
          FROM DTM.DTM_GRH_EFFECTIF
          WHERE CLICHE = :1
          UNION
          SELECT annee FROM finances)
    WHERE annee IN (SELECT AN_ID FROM DTM.DIM_ANNEE)
),

-- ── 5. Cadres et effectif total par année (inchangé) ───────────────────────
EFFECTIF AS (
    SELECT
        TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))                             AS annee,
        -- Cadres supérieurs : qualifications HC, 05, 06
        SUM(CASE WHEN QUAL_CODE IN ('HC','05','06') THEN NB_AGENTS ELSE 0 END) AS nb_cadres,
        -- Tous les agents présents, actifs ou non
        SUM(NB_AGENTS)                                                         AS nb_total
    FROM   DTM.DTM_GRH_EFFECTIF
    WHERE  CLICHE = :1
    GROUP  BY TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))
),

-- ── 6. Chaque année avec ses montants et son état de clôture ───────────────
base AS (
    SELECT a.annee, e.nb_cadres, e.nb_total,
           f.mt_cf, f.mt_fp, f.mt_for, f.mt_msal, f.mt_bud,
           NVL(c.mois_clos, 0)                                    AS mois_clos,
           CASE WHEN NVL(c.mois_clos, 0) = 12 THEN 'O' ELSE 'N' END AS clos,
           -- F3 : 1 = on publie les taux de cette année
           CASE WHEN NVL(c.mois_clos, 0) = 12
                  OR pr.taux_exercice_ouvert = 'CALCULES'
                THEN 1 ELSE 0 END                                 AS publier_taux,
           CASE WHEN f.annee IS NULL THEN 1 ELSE 0 END            AS sans_montant
    FROM ANNEES a
    CROSS JOIN parametres pr
    LEFT JOIN EFFECTIF e ON e.annee = a.annee
    LEFT JOIN finances f ON f.annee = a.annee
    LEFT JOIN clotures c ON c.EXERCICE = a.annee
)

-- ── 7. Les colonnes de la table ────────────────────────────────────────────
SELECT
    b.annee                                                       AS AN_ID,
    -- inchangé : cadres supérieurs / effectif total
    ROUND(NVL(b.nb_cadres, 0) / NULLIF(b.nb_total, 0), 4)        AS TX_ENCADREMENT,
    -- T63 (F4) : frais de personnel / charges de fonctionnement
    CASE WHEN b.publier_taux = 1
         THEN ROUND(b.mt_fp / NULLIF(b.mt_cf, 0), 4) END          AS TX_FRAIS_PERSONNEL,
    -- ratio de la Finance (T66) : hors GRH, reste vide
    CAST(NULL AS NUMBER)                                          AS TX_CHARGES_ADMIN,
    -- T64 (F4) : dépenses de formation / masse salariale
    CASE WHEN b.publier_taux = 1
         THEN ROUND(b.mt_for / NULLIF(b.mt_msal, 0), 4) END       AS TX_FORMATION,
    -- ratio de la Finance (T69) : hors GRH, reste vide
    CAST(NULL AS NUMBER)                                          AS TX_EXECUTION_BUDGETAIRE,
    NVL(b.nb_cadres, 0)                                           AS NB_CADRES,
    NVL(b.nb_total,  0)                                           AS NB_EFFECTIF_TOTAL,
    -- sans accent : la taille de la colonne est en octets dans la table
    CASE WHEN b.sans_montant = 1
         THEN 'T63-T64 : aucun montant comptable pour cet exercice'
         WHEN b.clos = 'N'
         THEN 'T63-T64 : exercice non clos (' || b.mois_clos
              || ' mois clos sur 12), taux vides'
         ELSE 'T63-T64 : comptes proposes par la BI, a valider par la DFC (QF1 a QF4)'
    END                                                           AS OBSERVATION,
    :1                                                            AS CLICHE,
    SYSDATE                                                       AS DATE_CHARGEMENT,
    -- ── COLONNES AJOUTÉES ────────────────────────────────────────────
    b.mt_cf                                                       AS MT_CHARGES_FONCT,
    b.mt_fp                                                       AS MT_FRAIS_PERSONNEL,
    b.mt_for                                                      AS MT_FORMATION,
    b.mt_msal                                                     AS MT_MASSE_SALARIALE,
    b.mt_bud                                                      AS MT_TOTAL_BUDGET,
    -- T64 (F4) : dépenses de formation / total budget
    CASE WHEN b.publier_taux = 1
         THEN ROUND(b.mt_for / NULLIF(b.mt_bud, 0), 4) END        AS TX_FORMATION_BUDGET,
    b.clos                                                        AS EXERCICE_CLOS
FROM base b
