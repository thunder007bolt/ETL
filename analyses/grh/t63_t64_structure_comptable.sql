-- =============================================================================
-- T63 ET T64 — LA STRUCTURE COMPTABLE À FAIRE ALIMENTER PAR LA DSI
-- Proposition du 07/11/2026, à valider par la DFC et la DSI.
--
-- POURQUOI
-- T63 (frais de personnel / charges de fonctionnement) et T64 (dépenses de
-- formation / masse salariale, et / total budget) ne se calculent pas avec la
-- GRH : la paie n'est pas dans la copie (lot 8, Q55). Il faut des montants
-- comptables.
--
-- CE QUE DIT LE RECUEIL DES BESOINS v7 (source qui prime, RH lignes 6 et 7)
--   - T63 et T64 : « Voir la DFC », « Source : Oracle GL » ;
--   - « Déclinaison : Direction, Agence (pour la CNSS), Bureau » ;
--   - comptes (T63) : frais de personnel = 65 des gestions G et S ;
--     dépense de formation = « 63, 86 et 87 » des gestions G et S ;
--     masse salariale = 651 et 652 ;
--   - périodicité annuelle.
-- Indicateur lié, plus bas dans le recueil : « Ratio des dépenses de formation
-- rapportées à la masse salariale annuelle (CIPRES) », source DFC.
-- Le recueil ne donne pas de comptes pour les charges de fonctionnement (CF)
-- de T63 ni pour le total budget (BUD) de T64. Ailleurs :
--   - CF : comptes 61 à 66 de la gestion G (recueil, Finance, ligne 1, T66) ;
--     comptes 61 à 68 de la gestion G ou 7 (grille « Evaluation des Ratios
--     CIPRES 2025 » de la CNSS) ;
--   - formation : comptes 63812, 6386 et 6577 dans la grille. « 63, 86 et
--     87 » est à faire préciser : le compte 63 entier compterait tous les
--     services consommés (remarque B22) ;
--   - masse salariale : compte 651 seul pour Kaya 2017 ;
--   - budget : module budget d'Oracle GL (recueil, Finance, T69).
--
-- LE CHOIX : DES MONTANTS PAR COMPTE, PAS DES RUBRIQUES DÉJÀ CALCULÉES
-- Les comptes de CF, de la formation et du budget ne sont pas arrêtés. La
-- CNSS fournit donc les montants par compte, et la BI choisit les comptes de
-- chaque rubrique par un réglage. Quand la DFC aura tranché, on change le
-- réglage, sans nouvelle extraction. C'est aussi ce que demande le recueil :
-- des données « décomposées, traquées jusqu'au niveau de granularité le plus
-- élémentaire », « sans intervention manuelle » ou presque.
--
-- CE QUE LA DSI FOURNIT
-- La table STG_GL.BI_GRH_SOLDE_COMPTABLE (schéma à confirmer par la DSI),
-- remplie à partir d'Oracle GL de l'une de ces deux façons :
--   A. par un traitement de la DSI : pour chaque exercice rechargé, supprimer
--      ses lignes puis les insérer. C'est la seule façon de garder des lignes
--      saisies à la main par la DFC (SOURCE = 'SAISIE_DFC') ;
--   B. par une vue matérialisée posée sur cette table (ON PREBUILT TABLE,
--      partie 3), rafraîchie par la DSI. Toutes les lignes viennent alors
--      d'Oracle GL.
-- Dans les deux cas, les colonnes et les règles ne changent pas : c'est le
-- contrat entre la DSI et la BI.
--
-- GRAIN : une ligne par exercice × mois × gestion × compte × section
--         analytique × nature du montant (réalisé ou budget).
--
-- RÈGLES D'ALIMENTATION
--   R1 Montants agrégés par compte : ni écriture, ni libellé d'écriture, ni
--      tiers, ni nom. Aucune donnée nominative, aucun numéro bancaire.
--   R2 Mouvements du mois, pas de cumul depuis le début de l'année. Le mois 13
--      reçoit les écritures d'inventaire de clôture, s'il y en a.
--   R3 Réalisé (NATURE_MONTANT = 'REALISE') : écritures comptabilisées des
--      comptes de charges, classes 6 et 8, de toutes les gestions.
--   R4 Budget (NATURE_MONTANT = 'BUDGET') : tous les comptes budgétés,
--      fonctionnement et investissement, budget définitif de l'exercice
--      (voté plus modifications). Par mois s'il est mensualisé, sinon en
--      entier sur le mois 12.
--   R5 Débit et crédit séparés, en francs CFA. Pour le budget, le montant va
--      au débit pour une charge ou un investissement, au crédit pour un
--      produit.
--   R6 Gestion : la valeur d'Oracle GL (G, I, S, T, E, ou 7, 6, 5, 8, 9).
--   R7 Section analytique : celle de l'écriture (centre de coût), '-' si elle
--      n'en a pas. Elle sert à la déclinaison par direction, agence et bureau.
--      UA_CODE : l'unité administrative de la GRH correspondante, si la DSI
--      la connaît. Sinon, la colonne reste vide et la BI la déduira (lot 23,
--      Q133 : section analytique dominante de chaque unité).
--   R8 Exercices : les cinq derniers clos et l'exercice en cours. Un exercice
--      rechargé remplace toutes ses lignes.
--   R9 PERIODE_CLOSE = 'O' quand la période est close dans Oracle GL. La BI
--      ne publie T63 et T64 que pour un exercice dont les 12 mois sont clos.
--
-- CE QUE LA BI EN FERA (requête T63-T64, à écrire après le premier chargement)
--   - L'ETL copie la table dans le DWH, avec le cliché, comme les autres faits.
--   - Montant réalisé d'un compte = débit − crédit, sur les mois de l'exercice.
--   - Rubriques, en réglages (valeur par défaut = recueil des besoins v7) :
--       FP   frais de personnel        comptes 65, gestions G et S
--       MSAL masse salariale            comptes 651 et 652
--       FOR  dépenses de formation      « 63, 86 et 87 », gestions G et S,
--                                       à préciser (grille : 63812, 6386, 6577)
--       CF   charges de fonctionnement  comptes 61 à 66, gestion G (recueil,
--                                       T66) ; grille : 61 à 68
--       BUD  total budget               lignes BUDGET, périmètre à préciser
--   - T63 = FP / CF (norme : 85 % au plus).
--     T64 = FOR / MSAL (norme : de 1 à 5 %) et FOR / BUD.
--     Par direction, agence et bureau, grâce à la section analytique.
--   - Les taux iront dans DTM_INDICATEURS_FINANCES_GRH (TX_FRAIS_PERSONNEL,
--     TX_FORMATION, aujourd'hui vides), sans changer ses colonnes. La
--     déclinaison par unité demandera des colonnes en plus : à voir avec
--     Jules (OAC).
--
-- QUESTIONS À LA DFC (non bloquantes : chacune deviendra un réglage)
--   1. Charges de fonctionnement : comptes 61 à 66 ou 61 à 68 ? gestion G
--      seule, ou G et S comme les frais de personnel ?
--   2. Formation : que veut dire « 63, 86 et 87 » ? 63812, 6386 et 6577 ?
--   3. Masse salariale : 651 et 652, ou 651 seul ? quelles gestions ?
--   4. Total budget : budget de quelles gestions, fonctionnement seul ou avec
--      l'investissement ? budget définitif ou initial ?
--   5. Les charges de personnel portent-elles une section analytique, pour
--      la déclinaison par direction, agence et bureau ?
--
-- Exécution :
--   parties 1 et 2 : utilisateur STG_GL (ou DBA) ;
--   partie 3 : la DSI, seulement pour la façon B ;
--   partie 4 : après chaque alimentation, en mode script (F5) dans SQL
--   Developer.
-- =============================================================================


-- ── 1. La table ─────────────────────────────────────────────────────────────
CREATE TABLE STG_GL.BI_GRH_SOLDE_COMPTABLE (
    EXERCICE            NUMBER(4)           NOT NULL,
    MOIS                NUMBER(2)           NOT NULL,
    CODE_GESTION        VARCHAR2(5 CHAR)    NOT NULL,
    COMPTE              VARCHAR2(20 CHAR)   NOT NULL,
    LIBELLE_COMPTE      VARCHAR2(240 CHAR),
    SECTION_ANALYTIQUE  VARCHAR2(30 CHAR)   NOT NULL,
    UA_CODE             VARCHAR2(12 CHAR),
    NATURE_MONTANT      VARCHAR2(10 CHAR)   NOT NULL,
    MONTANT_DEBIT       NUMBER(18,2)        DEFAULT 0 NOT NULL,
    MONTANT_CREDIT      NUMBER(18,2)        DEFAULT 0 NOT NULL,
    PERIODE_CLOSE       CHAR(1 CHAR)        NOT NULL,
    SOURCE              VARCHAR2(20 CHAR)   NOT NULL,
    DATE_EXTRACTION     DATE                NOT NULL,
    CONSTRAINT PK_BI_GRH_SOLDE_COMPTA
        PRIMARY KEY (EXERCICE, MOIS, CODE_GESTION, COMPTE, SECTION_ANALYTIQUE,
                     NATURE_MONTANT),
    CONSTRAINT CK_BI_GRH_SOLDE_MOIS    CHECK (MOIS BETWEEN 1 AND 13),
    CONSTRAINT CK_BI_GRH_SOLDE_NATURE  CHECK (NATURE_MONTANT IN ('REALISE', 'BUDGET')),
    CONSTRAINT CK_BI_GRH_SOLDE_CLOSE   CHECK (PERIODE_CLOSE IN ('O', 'N')),
    CONSTRAINT CK_BI_GRH_SOLDE_SOURCE  CHECK (SOURCE IN ('ORACLE_GL', 'SAISIE_DFC'))
);


-- ── 2. Les commentaires et le droit de lecture ──────────────────────────────
COMMENT ON TABLE STG_GL.BI_GRH_SOLDE_COMPTABLE IS
    'Montants comptables par exercice, mois, gestion, compte et section analytique (realise et budget), alimentes par la DSI depuis Oracle GL. Sert a T63 et T64 (CIPRES). Aucune donnee nominative.';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.EXERCICE           IS 'Exercice comptable (AAAA)';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.MOIS               IS 'Mois de la periode, 1 a 12 ; 13 = ecritures d''inventaire de cloture';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.CODE_GESTION       IS 'Gestion d''Oracle GL : G ou 7 administrative, I ou 6, S ou 5 action sanitaire et sociale, T ou 8, E ou 9';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.COMPTE             IS 'Compte de comptabilite generale, au niveau le plus fin';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.LIBELLE_COMPTE     IS 'Intitule du compte dans le plan comptable';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.SECTION_ANALYTIQUE IS 'Section analytique (centre de cout) de l''ecriture ; - si aucune';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.UA_CODE            IS 'Unite administrative de la GRH correspondant a la section, si connue';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.NATURE_MONTANT     IS 'REALISE = ecritures comptabilisees ; BUDGET = budget definitif de l''exercice';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.MONTANT_DEBIT      IS 'Total des debits du mois (FCFA) ; budget d''une charge ou d''un investissement';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.MONTANT_CREDIT     IS 'Total des credits du mois (FCFA) ; budget d''un produit';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.PERIODE_CLOSE      IS 'O si la periode est close dans Oracle GL, N sinon';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.SOURCE             IS 'ORACLE_GL = extrait d''Oracle GL ; SAISIE_DFC = saisi par la DFC';
COMMENT ON COLUMN STG_GL.BI_GRH_SOLDE_COMPTABLE.DATE_EXTRACTION    IS 'Date de l''extraction ou de la saisie';

-- DWH : l'utilisateur de l'ETL, qui copiera la table dans le DWH.
GRANT SELECT ON STG_GL.BI_GRH_SOLDE_COMPTABLE TO DWH;


-- ── 3. Façon B : la vue matérialisée de la DSI (exemple à adapter) ──────────
-- Exemple sur les tables standard d'Oracle E-Business Suite R12. La DSI doit
-- dire quel segment porte la gestion, le compte et la section analytique
-- (SEGMENTx ci-dessous), et l'identifiant du grand livre de la CNSS. Si elle
-- part plutôt de la copie STG_GL (STG_BALANCES, STG_BUDGET), le principe est
-- le même. Laissé en commentaire : à lancer par la DSI une fois adapté.
--
-- CREATE MATERIALIZED VIEW STG_GL.BI_GRH_SOLDE_COMPTABLE
-- ON PREBUILT TABLE
-- REFRESH COMPLETE ON DEMAND
-- AS
-- SELECT b.PERIOD_YEAR                                       AS EXERCICE,
--        b.PERIOD_NUM                                        AS MOIS,
--        cc.SEGMENT2                                         AS CODE_GESTION,
--        cc.SEGMENT3                                         AS COMPTE,
--        CAST(NULL AS VARCHAR2(240 CHAR))                    AS LIBELLE_COMPTE,
--        NVL(cc.SEGMENT4, '-')                               AS SECTION_ANALYTIQUE,
--        CAST(NULL AS VARCHAR2(12 CHAR))                     AS UA_CODE,
--        CASE b.ACTUAL_FLAG WHEN 'A' THEN 'REALISE' ELSE 'BUDGET' END AS NATURE_MONTANT,
--        SUM(b.PERIOD_NET_DR)                                AS MONTANT_DEBIT,
--        SUM(b.PERIOD_NET_CR)                                AS MONTANT_CREDIT,
--        MAX(CASE WHEN ps.CLOSING_STATUS IN ('C', 'P') THEN 'O' ELSE 'N' END) AS PERIODE_CLOSE,
--        'ORACLE_GL'                                         AS SOURCE,
--        SYSDATE                                             AS DATE_EXTRACTION
-- FROM GL.GL_BALANCES b
-- JOIN GL.GL_CODE_COMBINATIONS cc ON cc.CODE_COMBINATION_ID = b.CODE_COMBINATION_ID
-- JOIN GL.GL_PERIOD_STATUSES ps   ON ps.LEDGER_ID      = b.LEDGER_ID
--                                AND ps.PERIOD_NAME    = b.PERIOD_NAME
--                                AND ps.APPLICATION_ID = 101
-- LEFT JOIN GL.GL_BUDGET_VERSIONS bv ON bv.BUDGET_VERSION_ID = b.BUDGET_VERSION_ID
-- WHERE b.LEDGER_ID     = 1   -- identifiant du grand livre de la CNSS, à remplacer
--   AND b.CURRENCY_CODE = 'XOF'
--   AND b.PERIOD_YEAR  >= EXTRACT(YEAR FROM SYSDATE) - 5
--   AND (   (b.ACTUAL_FLAG = 'A' AND SUBSTR(cc.SEGMENT3, 1, 1) IN ('6', '8'))
--        OR (b.ACTUAL_FLAG = 'B' AND bv.STATUS = 'C'))   -- budget en vigueur
-- GROUP BY b.PERIOD_YEAR, b.PERIOD_NUM, cc.SEGMENT2, cc.SEGMENT3,
--          NVL(cc.SEGMENT4, '-'),
--          CASE b.ACTUAL_FLAG WHEN 'A' THEN 'REALISE' ELSE 'BUDGET' END;
--
-- Rafraîchissement, par la DSI, après chaque clôture mensuelle :
-- EXEC DBMS_MVIEW.REFRESH('STG_GL.BI_GRH_SOLDE_COMPTABLE', 'C');


-- ── 4. Les contrôles, après chaque alimentation ─────────────────────────────

-- C1 — Ce qui a été chargé : par exercice et nature, les mois présents, les
--      mois clos, les lignes, la date d'extraction.
--      Attendu : 12 mois (13 avec l'inventaire) pour un exercice clos, tous
--      clos ; les cinq derniers exercices clos et l'exercice en cours.
SELECT EXERCICE, NATURE_MONTANT,
       COUNT(DISTINCT MOIS)                                      AS mois_presents,
       COUNT(DISTINCT CASE WHEN PERIODE_CLOSE = 'O' THEN MOIS END) AS mois_clos,
       COUNT(*)                                                  AS lignes,
       SUM(CASE WHEN SOURCE = 'SAISIE_DFC' THEN 1 ELSE 0 END)    AS lignes_saisies,
       MIN(DATE_EXTRACTION)                                      AS extraction_debut,
       MAX(DATE_EXTRACTION)                                      AS extraction_fin
FROM STG_GL.BI_GRH_SOLDE_COMPTABLE
GROUP BY EXERCICE, NATURE_MONTANT
ORDER BY EXERCICE, NATURE_MONTANT;

-- C2 — Les totaux par gestion et par compte à deux chiffres, à comparer par la
--      DFC avec sa balance générale de l'exercice.
--      Attendu : les mêmes montants que la balance, au franc près.
SELECT EXERCICE, CODE_GESTION, SUBSTR(COMPTE, 1, 2)              AS compte_2,
       SUM(CASE WHEN NATURE_MONTANT = 'REALISE'
                THEN MONTANT_DEBIT - MONTANT_CREDIT ELSE 0 END)  AS realise_net,
       SUM(CASE WHEN NATURE_MONTANT = 'BUDGET'
                THEN MONTANT_DEBIT - MONTANT_CREDIT ELSE 0 END)  AS budget_net
FROM STG_GL.BI_GRH_SOLDE_COMPTABLE
GROUP BY EXERCICE, CODE_GESTION, SUBSTR(COMPTE, 1, 2)
ORDER BY EXERCICE, CODE_GESTION, compte_2;

-- C3 — Les rubriques de T63 et T64 selon chaque lecture des comptes, pour
--      que la DFC voie l'effet de ses réponses aux questions 1 à 4.
--      Réalisé net, gestions G et S (G ou 7, S ou 5), sauf indication.
--      Attendu : FOR / MSAL entre 1 et 5 % ; FP / CF au plus 85 %.
WITH realise AS (
    SELECT EXERCICE, CODE_GESTION, COMPTE,
           MONTANT_DEBIT - MONTANT_CREDIT                        AS net
    FROM STG_GL.BI_GRH_SOLDE_COMPTABLE
    WHERE NATURE_MONTANT = 'REALISE'
),
rubriques AS (
    SELECT EXERCICE,
           -- FP : comptes 65, gestions G et S (recueil)
           SUM(CASE WHEN COMPTE LIKE '65%' AND CODE_GESTION IN ('G', '7', 'S', '5')
                    THEN net ELSE 0 END)                         AS fp_65_g_s,
           -- MSAL : 651 et 652 (recueil, Annuaire) ou 651 seul (Kaya 2017)
           SUM(CASE WHEN (COMPTE LIKE '651%' OR COMPTE LIKE '652%')
                     AND CODE_GESTION IN ('G', '7', 'S', '5')
                    THEN net ELSE 0 END)                         AS msal_651_652,
           SUM(CASE WHEN COMPTE LIKE '651%' AND CODE_GESTION IN ('G', '7', 'S', '5')
                    THEN net ELSE 0 END)                         AS msal_651,
           -- FOR : grille de la CNSS (63812, 6386, 6577)
           SUM(CASE WHEN (COMPTE LIKE '63812%' OR COMPTE LIKE '6386%'
                          OR COMPTE LIKE '6577%')
                     AND CODE_GESTION IN ('G', '7', 'S', '5')
                    THEN net ELSE 0 END)                         AS for_grille,
           -- FOR : « 63, 86 et 87 » pris à la lettre (recueil), pour mesurer
           SUM(CASE WHEN (COMPTE LIKE '63%' OR COMPTE LIKE '86%'
                          OR COMPTE LIKE '87%')
                     AND CODE_GESTION IN ('G', '7', 'S', '5')
                    THEN net ELSE 0 END)                         AS for_63_86_87,
           -- CF : 61 à 66 gestion G (recueil, T66) ou 61 à 68 (grille)
           SUM(CASE WHEN SUBSTR(COMPTE, 1, 2) BETWEEN '61' AND '66'
                     AND CODE_GESTION IN ('G', '7')
                    THEN net ELSE 0 END)                         AS cf_61_66_g,
           SUM(CASE WHEN SUBSTR(COMPTE, 1, 2) BETWEEN '61' AND '68'
                     AND CODE_GESTION IN ('G', '7')
                    THEN net ELSE 0 END)                         AS cf_61_68_g
    FROM realise
    GROUP BY EXERCICE
),
budget AS (
    SELECT EXERCICE,
           SUM(MONTANT_DEBIT - MONTANT_CREDIT)                   AS bud_total,
           SUM(CASE WHEN CODE_GESTION IN ('G', '7')
                    THEN MONTANT_DEBIT - MONTANT_CREDIT ELSE 0 END) AS bud_g
    FROM STG_GL.BI_GRH_SOLDE_COMPTABLE
    WHERE NATURE_MONTANT = 'BUDGET'
      AND SUBSTR(COMPTE, 1, 1) IN ('2', '6')
    GROUP BY EXERCICE
)
SELECT r.EXERCICE,
       r.fp_65_g_s, r.cf_61_66_g, r.cf_61_68_g,
       ROUND(100 * r.fp_65_g_s / NULLIF(r.cf_61_66_g, 0), 1)     AS t63_fp_cf_61_66,
       ROUND(100 * r.fp_65_g_s / NULLIF(r.cf_61_68_g, 0), 1)     AS t63_fp_cf_61_68,
       r.for_grille, r.for_63_86_87, r.msal_651_652, r.msal_651,
       ROUND(100 * r.for_grille / NULLIF(r.msal_651_652, 0), 2)  AS t64_for_msal,
       b.bud_total, b.bud_g,
       ROUND(100 * r.for_grille / NULLIF(b.bud_total, 0), 2)     AS t64_for_bud
FROM rubriques r
LEFT JOIN budget b ON b.EXERCICE = r.EXERCICE
ORDER BY r.EXERCICE;

-- C4 — La déclinaison par unité : part des frais de personnel (comptes 65)
--      sans section analytique, ou sans unité de la GRH.
--      Attendu : une part faible. Sinon, T63 et T64 ne se déclineront pas par
--      direction, agence et bureau (question 5).
SELECT EXERCICE,
       SUM(MONTANT_DEBIT - MONTANT_CREDIT)                       AS fp_total,
       SUM(CASE WHEN SECTION_ANALYTIQUE = '-'
                THEN MONTANT_DEBIT - MONTANT_CREDIT ELSE 0 END)  AS fp_sans_section,
       SUM(CASE WHEN UA_CODE IS NULL
                THEN MONTANT_DEBIT - MONTANT_CREDIT ELSE 0 END)  AS fp_sans_unite,
       COUNT(DISTINCT SECTION_ANALYTIQUE)                        AS sections
FROM STG_GL.BI_GRH_SOLDE_COMPTABLE
WHERE NATURE_MONTANT = 'REALISE'
  AND COMPTE LIKE '65%'
  AND CODE_GESTION IN ('G', '7', 'S', '5')
GROUP BY EXERCICE
ORDER BY EXERCICE;
