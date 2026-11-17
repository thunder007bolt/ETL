-- =============================================================================
-- Point 131. Le délai des soins (T26)
-- Fiche : points_a_valider_v2.md, rubrique « Plutôt pour la DSI ».
-- Ce que dit le point : « ces deux dates n'ont pas été trouvées. Champs
--   proches à tester : les pièces CME reçues au guichet (339 réceptions), la
--   date de réception de l'avis du médecin conseil (DOS_DATE_REC_AVIS_MC), la
--   date de rendez-vous de chaque réception. »
-- Requête : les pièces des réceptions ANCIEN « autres pièces » des accidents
--   du travail.
-- Reprise de : lot4_controles_t26_t27.sql, P70 (exécutée le 30/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t26_requete_proposee.sql, bloc demande (les soins
--   sont liquidés au premier remboursement saisi ; pas d'indicateur de délai
--   des soins). Fiche des règles : règle 31.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Réceptions ANCIEN des accidents du travail sans pièce de consolidation, de
-- décès ni de déclaration : leurs pièces (RECEPTION_DOS_PIECE), par type.
-- -----------------------------------------------------------------------------
WITH anc AS (
    SELECT r.RD_ID
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE = 'A'
      AND r.RD_TYPE_RECEPTION = 'ANCIEN'
      AND r.RD_DATE_RECEPTION >= DATE '2019-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
      AND NOT EXISTS (
              SELECT 1
              FROM USER_DWH.RECEPTION_DOS_PIECE q
              WHERE q.RD_ID = r.RD_ID
                AND TRIM(q.TPJ_CODE) IN ('CD', 'CGM', 'CHER', 'CMCON', 'CMRECHUTE',
                                         'DECLAT', 'DECLMP', 'REAT', 'NERDAT', 'CMC',
                                         'CAT', 'CMAT', 'CMP', 'CMG', 'CRT'))
)
SELECT *
FROM (
    SELECT TRIM(p.TPJ_CODE)                                       AS code_piece,
           t.TPJ_DESCRIPTION                                      AS libelle,
           COUNT(DISTINCT p.RD_ID)                                AS receptions,
           COUNT(*)                                               AS pieces
    FROM USER_DWH.RECEPTION_DOS_PIECE p
    JOIN anc a ON a.RD_ID = p.RD_ID
    LEFT JOIN USER_DWH.TYPE_PIECE t ON t.TPJ_CODE = p.TPJ_CODE
    GROUP BY TRIM(p.TPJ_CODE), t.TPJ_DESCRIPTION
    ORDER BY receptions DESC
)
WHERE ROWNUM <= 25;

-- Résultat obtenu (P70, exécutée le 30/10/2026) : pièces des 724 réceptions
-- ANCIEN « autres pièces » : CME certificat médical 339 ; CV certificat de
-- vie 185 ; AN acte de naissance 107 ; CMI certificat médical d'incapacité
-- 24 ; CNR 22 ; CM 17 ; CS 17 ; RIB 14 ; BS 12 ; CMINAP 8 ; CTR 6 ; 12 autres
-- pièces. Aucune facture ni ordonnance.
-- Lecture : 339 réceptions portent un certificat médical (CME) : un champ à
--   tester pour la date du certificat du médecin traitant.
