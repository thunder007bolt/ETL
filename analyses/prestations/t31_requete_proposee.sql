-- =============================================================================
-- T31 — Prestations indues par branche et par mois
-- PROPOSITION du 01/11/2026, à valider après le lot 9 (P102 à P106) : la
-- maquette de l'Annuaire (p. 36), une ligne par mois et une ligne TOTAL, et
-- pour chaque branche (PVID, AT/MP, PF) puis au total : payées, trop-perçus,
-- moins-perçus ; plus le taux de prestations indues (norme CIPRES : 2 % au
-- plus).
-- Sources : DWH.FAIT_AJUSTEMENT, FAIT_DOSSIER, FAIT_DEBOURS (copies des tables
--           de USER_DWH).
-- :1      = CLICHE (MMYYYY) ; :2 = exercice (AAAA), par exemple 2025.
-- Date du calcul (R13, proposition) : l'exercice N se calcule sur le cliché
-- de fin mars N+1, la date à laquelle la CNSS arrête son annuaire (lot 8 P99).
-- Date des montants (R4, en attente de la CNSS, question posée au point du
-- 02/11/2026) : un seul réglage, date_montant dans parametres. 'MOIS' (le mois
-- payé) aujourd'hui ; 'SAISIE' (la saisie du paiement, rappels compris) si la
-- CNSS compte à la date du versement (lot 8 P98).
-- Règles (I1 du README, et l'atelier de Kaya 2017) :
--   · Branche (R1) : celle du dossier ; V = PVID ; A = AT/MP ; F et M = PF (la
--     maternité, que le DTM oubliait, comprise).
--   · Payées : « la somme décaissée au profit de l'assuré » (Kaya 2017) : les
--     débours de paiement (PM) au statut D ou P (R3), au mois du réglage
--     date_montant.
--   · Trop-perçus : les ajustements TP (trop-perçu) et TP-CONV (trop-perçu sur
--     convention) établis dans le mois (AJ_DATE_ETABLISSEMENT). « Les
--     prestations indues sont des manques à gagner pour la CNSS » (Kaya 2017).
--     Le remboursement d'un trop-perçu (REM-TP) n'est pas un nouvel indu.
--   · Moins-perçus : les ajustements MP (moins-perçu ou rappel) et RC (rappel
--     sur convention) établis dans le mois. Ils restent dans la maquette, mais
--     « les moins perçus ne sont pas des prestations indues » (Kaya 2017).
--   · Hors du tableau : avances (AV, AV-COVID19), mois de pension gratuits
--     (MPG), dons (DON-COVID19), rachats de rente (RR), reversements d'avance
--     (REV-AV), oppositions (OI, OE).
--   · Statut : tous sauf R (rejeté ou annulé : ses moins-perçus n'ont presque
--     rien payé, lot 9 P103). A = en cours, C = soldé ; depuis 2017, les
--     trop-perçus restent au statut A, repris par précomptes.
--   · Taux de prestations indues : trop-perçus ÷ payées × 100, sur l'année et
--     par branche (Kaya 2017 : indicateur CIPRES, norme 2 % au plus).
-- Contrôle (lot 9 P105) : en 2024, 229,5 M de trop-perçus pour 58,7 Md payés,
-- taux 0,39 % (PVID 0,47 %, AT/MP 0,47 %, PF 0,05 %), quelle que soit la date
-- des payées. L'annuaire de la CNSS ne publie pas d'indus.
-- Choix encore à valider : les payées sans les rappels ni les avances versés
-- par débours d'ajustement (P104) ; la date des montants (R4) ; les
-- trop-perçus comptés au mois de leur établissement.
-- =============================================================================

WITH

parametres AS (
    SELECT TO_NUMBER(:2)                    AS annee,
           TO_DATE(:2 || '0101', 'YYYYMMDD') AS debut,
           TO_DATE(:2 || '1231', 'YYYYMMDD') AS fin,
           -- date d'un paiement (règle R4, en attente de la CNSS) : 'MOIS' = le
           -- mois payé, 'SAISIE' = la saisie du paiement (rappels compris)
           'MOIS'                           AS date_montant
    FROM   DUAL
),

mois AS (
    SELECT LEVEL AS mois FROM DUAL CONNECT BY LEVEL <= 12
),

-- ── 1. Les payées, par branche et par mois ─────────────────────────────────
payees AS (
    SELECT CASE deb.CODE_BRANCHE WHEN 'V' THEN 'PVID'
                                 WHEN 'A' THEN 'AT/MP'
                                 ELSE          'PF' END           AS branche,
           EXTRACT(MONTH FROM CASE pr.date_montant
                                  WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
                                  ELSE deb.DEB_DATE_EFFET END)   AS mois,
           deb.DEB_MONTANT                                        AS montant
    FROM DWH.FAIT_DEBOURS deb
    CROSS JOIN parametres pr
    WHERE deb.CLICHE   = :1
      AND deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.CODE_BRANCHE IN ('V', 'A', 'F', 'M')
      AND CASE pr.date_montant WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
                               ELSE deb.DEB_DATE_EFFET END >= pr.debut
      AND CASE pr.date_montant WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
                               ELSE deb.DEB_DATE_EFFET END <  pr.fin + 1
),

-- ── 2. Les trop-perçus et moins-perçus, établis dans le mois ───────────────
ajustements AS (
    SELECT CASE d.TDOS_CODE WHEN 'V' THEN 'PVID'
                            WHEN 'A' THEN 'AT/MP'
                            ELSE          'PF' END                AS branche,
           EXTRACT(MONTH FROM aj.AJ_DATE_ETABLISSEMENT)           AS mois,
           CASE WHEN aj.TAJ_CODE IN ('TP', 'TP-CONV') THEN aj.AJ_MONTANT ELSE 0 END AS trop_percu,
           CASE WHEN aj.TAJ_CODE IN ('MP', 'RC')      THEN aj.AJ_MONTANT ELSE 0 END AS moins_percu
    FROM DWH.FAIT_AJUSTEMENT aj
    JOIN DWH.FAIT_DOSSIER    d  ON d.DOS_CODE = aj.DOS_CODE
                               AND d.CLICHE   = :1
    CROSS JOIN parametres    pr
    WHERE aj.CLICHE = :1
      AND aj.TAJ_CODE IN ('TP', 'TP-CONV', 'MP', 'RC')
      AND aj.AJ_STATUT <> 'R'
      AND d.TDOS_CODE IN ('V', 'A', 'F', 'M')
      AND aj.AJ_DATE_ETABLISSEMENT >= pr.debut
      AND aj.AJ_DATE_ETABLISSEMENT <  pr.fin + 1
),

-- ── 3. Une ligne par mois et par branche ───────────────────────────────────
par_branche AS (
    SELECT branche, mois, SUM(montant) AS payees, 0 AS trop_percus, 0 AS moins_percus
    FROM payees
    GROUP BY branche, mois
    UNION ALL
    SELECT branche, mois, 0, SUM(trop_percu), SUM(moins_percu)
    FROM ajustements
    GROUP BY branche, mois
),

lignes AS (
    SELECT TO_CHAR(m.mois, 'FM00')                                AS mois,
           SUM(CASE WHEN p.branche = 'PVID'  THEN p.payees       ELSE 0 END) AS payees_pvid,
           SUM(CASE WHEN p.branche = 'PVID'  THEN p.trop_percus  ELSE 0 END) AS tp_pvid,
           SUM(CASE WHEN p.branche = 'PVID'  THEN p.moins_percus ELSE 0 END) AS mp_pvid,
           SUM(CASE WHEN p.branche = 'AT/MP' THEN p.payees       ELSE 0 END) AS payees_atmp,
           SUM(CASE WHEN p.branche = 'AT/MP' THEN p.trop_percus  ELSE 0 END) AS tp_atmp,
           SUM(CASE WHEN p.branche = 'AT/MP' THEN p.moins_percus ELSE 0 END) AS mp_atmp,
           SUM(CASE WHEN p.branche = 'PF'    THEN p.payees       ELSE 0 END) AS payees_pf,
           SUM(CASE WHEN p.branche = 'PF'    THEN p.trop_percus  ELSE 0 END) AS tp_pf,
           SUM(CASE WHEN p.branche = 'PF'    THEN p.moins_percus ELSE 0 END) AS mp_pf,
           SUM(NVL(p.payees, 0))                                  AS payees_total,
           SUM(NVL(p.trop_percus, 0))                             AS tp_total,
           SUM(NVL(p.moins_percus, 0))                            AS mp_total
    FROM mois m
    LEFT JOIN par_branche p ON p.mois = m.mois
    GROUP BY m.mois
)

SELECT mois                                                       AS MOIS,
       ROUND(payees_pvid)  AS PAYEES_PVID,  ROUND(tp_pvid)  AS TROP_PERCUS_PVID,
       ROUND(mp_pvid)      AS MOINS_PERCUS_PVID,
       ROUND(payees_atmp)  AS PAYEES_ATMP,  ROUND(tp_atmp)  AS TROP_PERCUS_ATMP,
       ROUND(mp_atmp)      AS MOINS_PERCUS_ATMP,
       ROUND(payees_pf)    AS PAYEES_PF,    ROUND(tp_pf)    AS TROP_PERCUS_PF,
       ROUND(mp_pf)        AS MOINS_PERCUS_PF,
       ROUND(payees_total) AS PAYEES_TOTAL, ROUND(tp_total) AS TROP_PERCUS_TOTAL,
       ROUND(mp_total)     AS MOINS_PERCUS_TOTAL,
       -- taux de prestations indues du mois (norme CIPRES : 2 % au plus)
       ROUND(100 * tp_total / NULLIF(payees_total, 0), 3)         AS TAUX_INDUS_PCT,
       :1                                                         AS CLICHE
FROM lignes
UNION ALL
SELECT 'TOTAL',
       ROUND(SUM(payees_pvid)),  ROUND(SUM(tp_pvid)),  ROUND(SUM(mp_pvid)),
       ROUND(SUM(payees_atmp)),  ROUND(SUM(tp_atmp)),  ROUND(SUM(mp_atmp)),
       ROUND(SUM(payees_pf)),    ROUND(SUM(tp_pf)),    ROUND(SUM(mp_pf)),
       ROUND(SUM(payees_total)), ROUND(SUM(tp_total)), ROUND(SUM(mp_total)),
       ROUND(100 * SUM(tp_total) / NULLIF(SUM(payees_total), 0), 3),
       :1
FROM lignes
ORDER BY 1
