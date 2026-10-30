-- =============================================================================
-- LOT 4 — Contrôles pour T26 (dossiers AT/MP) et T27 (dossiers PF)
-- Rédigé et exécuté le 30/10/2026 (P59 à P68), puis la partie B (P69 à
-- P73), ajoutée après les résultats et exécutée le même jour.
-- Sur la base source, schéma USER_DWH, comme les lots 1 à 3.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages seulement.
-- Tables lues : RECEPTION_DOSSIER, RECEPTION_DOS_PIECE, DOSSIER,
-- DOSSIER_REJETE, PRESTATION_ESP, NOTIFICATION_PREST ; DEBOURS dans P59, P63
-- et P68 seulement (remboursements de soins des dossiers AT/MP : quelques
-- minutes chacune).
-- Données nominatives : jamais lues (nom, adresse, téléphone du déposant,
-- remarques). Aucun champ libre. L'identifiant de la mère (P67) ne sert qu'à
-- compter.
-- Pourquoi ce lot : T25 sert de modèle. On y compte des demandes (un dossier
-- × une ligne), avec les règles D1 à D8 (schéma regles_prestations). T26 et
-- T27 ont la même maquette (Annuaire p. 34), mais leurs dossiers ne vivent
-- pas comme un dossier de pension :
--   · un dossier AT/MP naît d'une déclaration d'accident ou de maladie ; il
--     peut donner des indemnités journalières (IJ), des soins, une rente
--     après la consolidation, puis des rentes de survivants ;
--   · un dossier PF est celui d'un allocataire : chaque enfant ajouté est une
--     prestation de plus, notifiée plus tard ;
--   · une grossesse ouvre un dossier de maternité (M), rattaché aux PF.
-- Chaque requête mesure ce qu'est une demande dans la branche, et d'où part
-- la durée de liquidation :
--   T26 Q1 qu'est-ce qu'une demande AT/MP ?        → P59, P60
--   T26 Q2 qu'est-ce qui est liquidé (notifié) ?   → P61
--   T26 Q3 d'où part la durée de liquidation ?     → P62
--   T26 Q4 les frais médicaux                       → P63
--   T27 Q1 un enfant de plus, une demande de plus ? → P64, P65
--   T27 Q2 les allocations prénatales               → P66
--   T27 Q3 la maternité                             → P65, P67
--   T26 Q5 et T27 Q4 refus et stock en instance     → P68
-- Déjà connu (lot 1) : en 2025, reçus / ouverts / notifiés : AT/MP 1 481 /
-- 1 237 / 357 ; PF 9 968 / 9 205 / 16 075 ; maternité 4 338 / 4 238 / 4 265
-- (P09). Durée médiane réception → notification : AT/MP 156 j, PF 13,5 j,
-- maternité 17 j (P10). Soins payés en 2025 : AT/MP seulement, 826 débours
-- (P05).
-- Atelier de Kaya 2017 : l'allocation prénatale « ne fait l'objet ni de
-- réception ni de notification » (tableau 1, T27). La durée d'une rente AT/MP
-- part de la réception du certificat médical de consolidation, celle des
-- soins du certificat du médecin traitant (tableau 2, indicateurs).
-- Chaque en-tête a une ligne « Lecture » : ce que le résultat change.
-- Numérotation : suite du lot 3 B, P59 à P68, puis P69 à P73 (partie B).
-- =============================================================================


-- =============================================================================
-- T26 Q1. QU'EST-CE QU'UNE DEMANDE AT/MP ?
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P59 — Ce que porte un dossier AT/MP
-- Question : T26 Q1. Un dossier AT/MP est-il une demande, ou plusieurs ?
-- Objectif : pour les dossiers AT/MP ouverts de 2019 à 2025, la combinaison
--            de prestations qu'ils portent (IJ, rente viagère, allocation
--            d'incapacité, rentes de survivants, soins remboursés, rien), et
--            leur nombre de dates de notification.
-- Lecture  : si presque tous les dossiers ne portent qu'une prestation, un
--            dossier = une demande. Si « IJ + rente » est fréquent, on compte
--            comme en T25 : une demande par ligne (dossier × ligne), chacune
--            avec sa liquidation. Un gros bloc « soins » ou « rien » dit que
--            T26 doit décider de ces dossiers à part (Q4, Q5).
-- Tableaux : T26.
-- Résultat : 9 106 dossiers AT/MP ouverts de 2019 à 2025 (1 237 en 2025).
--            Profils (dossiers, dont 2025) : rien 4 158 (821) ; soins seuls
--            2 831 (274) ; IJ + soins 1 560 (116), dont 122 à plusieurs dates
--            de notification ; IJ + allocation + soins 231 (9), dont 191 à
--            plusieurs dates ; IJ + rente + soins 90 (1), dont 77 ; survivants
--            + soins 59 ; IJ seules 56 ; allocation + soins 42 ; rente + soins
--            35 ; survivants seuls 26 ; rente seule 8 ; 10 autres dossiers dans
--            5 combinaisons. Aucun code hors des 4 lignes de T26.
-- Constat  : Un dossier AT/MP porte souvent plusieurs prestations liquidées à
--            des dates différentes : les IJ d'abord, la rente ou l'allocation
--            des années après (191 dossiers sur 231, 77 sur 90). On compte donc
--            comme en T25 : une demande par ligne (dossier × ligne). Deux blocs
--            pèsent lourd : 46 % des dossiers ne donnent rien, simple
--            déclaration (Q5), et 31 % ne donnent que des soins (Q4). Les soins
--            accompagnent presque toutes les autres prestations.
-- -----------------------------------------------------------------------------
WITH dos AS (
    SELECT DOS_CODE,
           EXTRACT(YEAR FROM DOS_DATE_OUVERTURE)                  AS annee
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'A'
      AND DOS_DATE_OUVERTURE >= DATE '2019-01-01'
      AND DOS_DATE_OUVERTURE <  DATE '2026-01-01'
),
esp AS (
    SELECT pe.DOS_CODE,
           MAX(CASE WHEN pe.TPE_CODE = 'AIJ' THEN 1 ELSE 0 END)  AS ij,
           MAX(CASE WHEN pe.TPE_CODE = 'ARI' THEN 1 ELSE 0 END)  AS rente,
           MAX(CASE WHEN pe.TPE_CODE = 'AAI' THEN 1 ELSE 0 END)  AS alloc,
           MAX(CASE WHEN pe.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA')
                    THEN 1 ELSE 0 END)                            AS survivant,
           MAX(CASE WHEN pe.TPE_CODE IN ('AIJ', 'ARI', 'AAI',
                                         'ARSV', 'ARSF', 'ARSE', 'ARSA')
                    THEN 0 ELSE 1 END)                            AS autre,
           COUNT(*)                                               AS nb_prest,
           COUNT(DISTINCT TRUNC(pe.PE_DATE_NOTIFICATION))         AS nb_dates_notif
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN dos ON dos.DOS_CODE = pe.DOS_CODE
    GROUP BY pe.DOS_CODE
),
soins AS (
    SELECT deb.DOS_CODE
    FROM USER_DWH.DEBOURS deb
    JOIN dos ON dos.DOS_CODE = deb.DOS_CODE
    WHERE deb.TPN_CODE IS NOT NULL
    GROUP BY deb.DOS_CODE
),
profil AS (
    SELECT dos.annee,
           CASE WHEN e.DOS_CODE IS NULL AND s.DOS_CODE IS NULL THEN 'rien'
                ELSE NVL(RTRIM(   CASE WHEN e.ij        = 1 THEN 'IJ + '          END
                               || CASE WHEN e.rente     = 1 THEN 'rente + '       END
                               || CASE WHEN e.alloc     = 1 THEN 'allocation + '  END
                               || CASE WHEN e.survivant = 1 THEN 'survivants + '  END
                               || CASE WHEN e.autre     = 1 THEN 'autre code + '  END
                               || CASE WHEN s.DOS_CODE IS NOT NULL THEN 'soins + ' END,
                               ' +'), 'code vide')
           END                                                    AS profil,
           NVL(e.nb_prest, 0)                                     AS nb_prest,
           NVL(e.nb_dates_notif, 0)                               AS nb_dates_notif
    FROM dos
    LEFT JOIN esp   e ON e.DOS_CODE = dos.DOS_CODE
    LEFT JOIN soins s ON s.DOS_CODE = dos.DOS_CODE
)
SELECT profil,
       COUNT(*)                                                   AS dossiers_2019_2025,
       SUM(CASE WHEN annee = 2025 THEN 1 ELSE 0 END)              AS ouverts_2025,
       SUM(CASE WHEN nb_dates_notif = 0 THEN 1 ELSE 0 END)        AS sans_notification,
       SUM(CASE WHEN nb_dates_notif = 1 THEN 1 ELSE 0 END)        AS une_date_notification,
       SUM(CASE WHEN nb_dates_notif >= 2 THEN 1 ELSE 0 END)       AS plusieurs_dates_notif,
       ROUND(AVG(nb_prest), 1)                                    AS prestations_par_dossier
FROM profil
GROUP BY profil
ORDER BY dossiers_2019_2025 DESC;


-- -----------------------------------------------------------------------------
-- P60 — Réceptions AT/MP : type et pièces fournies
-- Question : T26 Q1.
-- Objectif : pour les réceptions AT/MP de 2019 à 2025, le type (NOUVEAU,
--            ANCIEN) et ce que disent les pièces : déclaration d'accident ou
--            de maladie, arrêt de travail, rechute, consolidation (demande de
--            rente), décès (rentes de survivants), guérison ou reprise. Une
--            réception est classée par sa pièce la plus avancée (décès avant
--            consolidation, avant rechute, avant déclaration…).
-- Lecture  : les pièces disent ce qui est demandé. Si la rente arrive par une
--            réception ANCIEN avec un certificat de consolidation, la rente
--            est une demande à part, avec son propre dépôt, comme la réversion
--            en T25 (règle D3). Si tout arrive avec la déclaration, un dossier
--            = un dépôt.
-- Tableaux : T26.
-- Résultat : 10 618 réceptions AT/MP de 2019 à 2025 (1 481 en 2025). NOUVEAU
--            9 496 : déclaration 9 334 (1 313 en 2025 ; 8 973 ouvrent un
--            dossier et 8 697 portent son numéro), consolidation 36, décès 33,
--            autres 93. ANCIEN 1 097 (136 en 2025), toutes sur un dossier
--            existant, 6 seulement en ouvrent un : consolidation 233, décès 53,
--            déclaration 30, guérison ou reprise 6, autres pièces 724, sans
--            pièce 51. Type vide : 25.
-- Constat  : Comme en T25 (P52), NOUVEAU est la déclaration, qui ouvre le
--            dossier, et ANCIEN est déposée sur un dossier existant. Les
--            demandes de rente (certificat de consolidation, 233) et de rentes
--            de survivants (certificat de décès, 53) arrivent par une réception
--            ANCIEN : ce sont des dépôts à part, à rattacher comme la réversion
--            de T25 (règle D3). Les 724 réceptions ANCIEN « autres pièces »
--            restent à lire (P70, P71) : sans doute des demandes de
--            remboursement de soins. Aucune réception pour une IJ : elle est
--            demandée avec la déclaration.
-- -----------------------------------------------------------------------------
WITH recep0 AS (
    SELECT r.RD_ID,
           r.DOS_CODE,
           NVL(r.RD_TYPE_RECEPTION, '(vide)')                     AS type_reception,
           EXTRACT(YEAR FROM r.RD_DATE_RECEPTION)                 AS annee
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE = 'A'
      AND r.RD_DATE_RECEPTION >= DATE '2019-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
),
pieces AS (
    SELECT p.RD_ID,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CD', 'CGM', 'CHER')
                    THEN 1 ELSE 0 END)                            AS deces,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) = 'CMCON'
                    THEN 1 ELSE 0 END)                            AS consolidation,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) = 'CMRECHUTE'
                    THEN 1 ELSE 0 END)                            AS rechute,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('DECLAT', 'DECLMP', 'REAT',
                                              'NERDAT', 'CMC')
                    THEN 1 ELSE 0 END)                            AS declaration,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CAT', 'CMAT', 'CMP')
                    THEN 1 ELSE 0 END)                            AS arret,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CMG', 'CRT')
                    THEN 1 ELSE 0 END)                            AS guerison
    FROM USER_DWH.RECEPTION_DOS_PIECE p
    JOIN recep0 r ON r.RD_ID = p.RD_ID
    GROUP BY p.RD_ID
),
ouvre AS (
    SELECT DISTINCT o.RD_ID
    FROM USER_DWH.DOSSIER o
    JOIN recep0 r ON r.RD_ID = o.RD_ID
),
recep AS (
    SELECT r.type_reception,
           r.annee,
           CASE WHEN p.RD_ID IS NULL          THEN '8 sans pièce'
                WHEN p.deces         = 1      THEN '1 décès'
                WHEN p.consolidation = 1      THEN '2 consolidation'
                WHEN p.rechute       = 1      THEN '3 rechute'
                WHEN p.declaration   = 1      THEN '4 déclaration'
                WHEN p.arret         = 1      THEN '5 arrêt de travail'
                WHEN p.guerison      = 1      THEN '6 guérison ou reprise'
                ELSE                               '7 autres pièces'
           END                                                    AS motif,
           CASE WHEN o.RD_ID IS NOT NULL THEN 1 ELSE 0 END        AS ouvre_dossier,
           CASE WHEN r.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END     AS vise_dossier
    FROM recep0 r
    LEFT JOIN pieces p ON p.RD_ID = r.RD_ID
    LEFT JOIN ouvre  o ON o.RD_ID = r.RD_ID
)
SELECT type_reception,
       motif,
       COUNT(*)                                                   AS receptions_2019_2025,
       SUM(CASE WHEN annee = 2025 THEN 1 ELSE 0 END)              AS receptions_2025,
       SUM(ouvre_dossier)                                         AS ouvrent_un_dossier,
       SUM(vise_dossier)                                          AS visent_un_dossier
FROM recep
GROUP BY type_reception, motif
ORDER BY type_reception, motif;


-- =============================================================================
-- T26 Q2 ET Q3. CE QUI EST LIQUIDÉ, ET D'OÙ PART LA DURÉE
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P61 — Prestations AT/MP : lesquelles sont notifiées ?
-- Question : T26 Q2. Qu'est-ce qui est liquidé dans un dossier AT/MP ?
-- Objectif : pour chaque code de prestation des dossiers AT/MP : prestations
--            et dossiers, avec une date de demande, avec une date de
--            notification, avec une notification imprimée (NOTIFICATION_PREST),
--            saisies et notifiées en 2025, délai médian demande →
--            notification en 2025, prestations par dossier.
-- Lecture  : en T25, la liquidation est la première notification de la
--            demande (règle D4). Si les IJ ne sont pas notifiées, il faut une
--            autre date pour elles (premier paiement, Proposition BI) ou les
--            sortir des « liquidés ». Plusieurs IJ par dossier (une par arrêt
--            ou prolongation ?) poseraient la même question qu'en PF : une
--            demande par dossier, ou par prestation ?
-- Tableaux : T26.
-- Résultat : Par code (prestations / dossiers / avec date de notification /
--            notifiées en 2025 / délai médian demande → notification en 2025) :
--            AIJ 13 201 / 6 963 / 12 776 / 421 / 77 j ; ARI 2 349 / 1 970 /
--            1 853 / 96 / 782 j ; AAI 1 233 / 1 233 / 537 / 61 / 283 j ; ARSE
--            1 121 / 415 / 763 / 34 / 451 j ; ARSV 388 / 355 / 342 / 2 /
--            888 j ; ARSA 350 / 266 / 274 / 6 / 534 j ; ARSF 4 / 4 / 4 / 1 /
--            734 j. Toutes ont une date de demande. Notification imprimée
--            (NOTIFICATION_PREST) : AIJ jamais, ARI 1 244, AAI 999, ARSE 357,
--            ARSA 120, ARSV 58, ARSF 2. 1,9 IJ par dossier.
-- Constat  : Les IJ sont notifiées (97 %) : la règle D4 de T25 (liquidation =
--            première notification) vaut pour elles. Un dossier a 1,9 IJ (une
--            par arrêt ou prolongation), notifiées à des dates différentes : on
--            ne garde que la première, comme en T25. L'allocation d'incapacité
--            n'a une date de notification que dans 44 % des cas (537 sur
--            1 233), alors que 999 ont une notification imprimée : sa date est
--            à chercher là (P72). La date de demande d'une rente précède sa
--            notification de 2 ans en médiane : elle ne peut pas servir de date
--            de dépôt.
-- -----------------------------------------------------------------------------
WITH notif AS (
    SELECT DISTINCT DOS_CODE, TPE_CODE
    FROM USER_DWH.NOTIFICATION_PREST
)
SELECT pe.TPE_CODE,
       COUNT(*)                                                   AS prestations,
       COUNT(DISTINCT pe.DOS_CODE)                                AS dossiers,
       ROUND(COUNT(*) / COUNT(DISTINCT pe.DOS_CODE), 1)           AS prestations_par_dossier,
       SUM(CASE WHEN pe.PE_DATE_DEMANDE IS NOT NULL
                THEN 1 ELSE 0 END)                                AS avec_date_demande,
       SUM(CASE WHEN pe.PE_DATE_NOTIFICATION IS NOT NULL
                THEN 1 ELSE 0 END)                                AS avec_date_notification,
       SUM(CASE WHEN n.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END)    AS avec_notification_imprimee,
       SUM(CASE WHEN pe.PE_DATE_INSERT >= DATE '2025-01-01'
                 AND pe.PE_DATE_INSERT <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                                AS saisies_2025,
       SUM(CASE WHEN pe.PE_DATE_NOTIFICATION >= DATE '2025-01-01'
                 AND pe.PE_DATE_NOTIFICATION <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                                AS notifiees_2025,
       ROUND(MEDIAN(CASE WHEN pe.PE_DATE_NOTIFICATION >= DATE '2025-01-01'
                          AND pe.PE_DATE_NOTIFICATION <  DATE '2026-01-01'
                         THEN TRUNC(pe.PE_DATE_NOTIFICATION)
                              - TRUNC(pe.PE_DATE_DEMANDE) END))   AS med_demande_notif_2025_j
FROM USER_DWH.PRESTATION_ESP pe
JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
LEFT JOIN notif n ON n.DOS_CODE = pe.DOS_CODE
                 AND n.TPE_CODE = pe.TPE_CODE
WHERE d.TDOS_CODE = 'A'
GROUP BY pe.TPE_CODE
ORDER BY prestations DESC;


-- -----------------------------------------------------------------------------
-- P62 — Durée de liquidation AT/MP : quelle date de départ ?
-- Question : T26 Q3. D'où part la durée de liquidation d'une demande AT/MP ?
-- Objectif : pour les demandes AT/MP (dossier × ligne) liquidées en 2025
--            (première notification de la ligne), comparer les dates de
--            départ possibles : réception qui a ouvert le dossier, dernière
--            réception du dossier avant la liquidation, date de l'accident,
--            réception du certificat médical pour la rente
--            (DOS_DATE_REC_CM_RENTE), réception de l'avis du médecin conseil
--            (DOS_DATE_REC_AVIS_MC), date de demande de la prestation. Pour
--            chacune : combien de demandes l'ont, durée médiane, plus de 45 j.
-- Lecture  : la médiane de 156 j (P10) part de la déclaration : elle compte
--            les soins et l'attente de la consolidation, que la CNSS ne
--            maîtrise pas. Kaya 2017 fait partir la durée d'une rente du
--            certificat de consolidation. Si DOS_DATE_REC_CM_RENTE est rempli
--            pour les rentes, c'est la date de départ à proposer pour elles ;
--            pour les IJ, la dernière réception avant la liquidation.
-- Tableaux : T26.
-- Résultat : Demandes liquidées en 2025 (première notification de la ligne) :
--            rente viagère 43, survivants 13, IJ 226, allocation d'incapacité
--            61. Durée médiane selon la date de départ (rente / survivants /
--            IJ / allocation) : réception d'ouverture 986 / 619 / 251 / 444 j ;
--            dernière réception avant la liquidation 14 / 35 / 183 / 13 j, plus
--            de 45 j pour 12 / 6 / 193 / 16 demandes ; accident 1 535 / 857 /
--            294 / 560 j ; certificat médical pour la rente, rempli pour 28 /
--            1 / 41 / 52 demandes, 64 / 595 / 98 / 107 j, plus de 45 j pour
--            18 / 1 / 29 / 40 ; avis du médecin conseil, rempli pour 29 / 1 /
--            40 / 51, 12 / 125 / 14 / 15 j ; date de demande 803 / 562 / 156 /
--            283 j.
-- Constat  : Pour les rentes, les rentes de survivants et l'allocation, la
--            réception ANCIEN qui précède la liquidation (consolidation, décès)
--            donne une durée courte et plausible : 13 à 35 j en médiane. C'est
--            la règle D3 de T25 appliquée à la demande, pas au dossier. La date
--            du certificat pour la rente, retenue par Kaya 2017, est remplie
--            pour 65 % des rentes et 85 % des allocations et part plus tôt (64
--            à 107 j) : à présenter à la CNSS comme l'autre choix possible.
--            Pour les IJ, aucune date ne convient encore : la seule réception
--            est la déclaration, et ses 183 j comptent la durée de l'arrêt de
--            travail, pas le travail de la CNSS. À mesurer avec la date de
--            reprise et la dernière pièce (P69).
--            Correction après P69 : l'arrêt ne dure que 27 j en médiane. Les
--            183 j des IJ ne sont donc pas l'arrêt, mais une attente d'environ
--            7 mois après sa fin, que la base n'explique pas.
-- -----------------------------------------------------------------------------
WITH prest AS (
    SELECT pe.DOS_CODE,
           CASE WHEN pe.TPE_CODE = 'ARI'                            THEN '1 rente viagère'
                WHEN pe.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA') THEN '2 rentes de survivants'
                WHEN pe.TPE_CODE = 'AIJ'                            THEN '3 IJ'
                WHEN pe.TPE_CODE = 'AAI'                            THEN '4 allocation d''incapacité'
                ELSE                                                     '5 autres'
           END                                                    AS ligne,
           pe.PE_DATE_NOTIFICATION,
           pe.PE_DATE_DEMANDE
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE = 'A'
),
demande AS (
    SELECT DOS_CODE,
           ligne,
           TRUNC(MIN(PE_DATE_NOTIFICATION))                       AS date_liq,
           TRUNC(MIN(PE_DATE_DEMANDE))                            AS date_demande
    FROM prest
    GROUP BY DOS_CODE, ligne
),
recep AS (
    SELECT d.DOS_CODE, TRUNC(r.RD_DATE_RECEPTION) AS date_reception, 1 AS ouvre
    FROM USER_DWH.DOSSIER d
    JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID = d.RD_ID
    WHERE d.TDOS_CODE = 'A'
    UNION ALL
    SELECT r.DOS_CODE, TRUNC(r.RD_DATE_RECEPTION), 0
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE = 'A'
      AND r.DOS_CODE IS NOT NULL
),
base AS (
    SELECT dm.ligne,
           dm.date_liq,
           dm.date_demande,
           TRUNC(d.DOS_DATE_ACCIDENT)                             AS date_accident,
           TRUNC(d.DOS_DATE_REC_CM_RENTE)                         AS date_cm_rente,
           TRUNC(d.DOS_DATE_REC_AVIS_MC)                          AS date_avis_mc,
           (SELECT MIN(x.date_reception)
            FROM recep x
            WHERE x.DOS_CODE = dm.DOS_CODE
              AND x.ouvre    = 1)                                 AS date_recep_ouverture,
           (SELECT MAX(x.date_reception)
            FROM recep x
            WHERE x.DOS_CODE        = dm.DOS_CODE
              AND x.date_reception <= dm.date_liq)                AS date_recep_proche
    FROM demande dm
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = dm.DOS_CODE
    WHERE dm.date_liq >= DATE '2025-01-01'
      AND dm.date_liq <  DATE '2026-01-01'
)
SELECT ligne,
       COUNT(*)                                                   AS demandes_liquidees_2025,
       COUNT(date_recep_ouverture)                                AS avec_recep_ouverture,
       ROUND(MEDIAN(date_liq - date_recep_ouverture))             AS med_depuis_recep_ouverture,
       COUNT(date_recep_proche)                                   AS avec_recep_avant_liq,
       ROUND(MEDIAN(date_liq - date_recep_proche))                AS med_depuis_recep_proche,
       SUM(CASE WHEN date_liq - date_recep_proche > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_recep_proche,
       COUNT(date_accident)                                       AS avec_date_accident,
       ROUND(MEDIAN(date_liq - date_accident))                    AS med_depuis_accident,
       COUNT(date_cm_rente)                                       AS avec_cm_rente,
       ROUND(MEDIAN(date_liq - date_cm_rente))                    AS med_depuis_cm_rente,
       SUM(CASE WHEN date_liq - date_cm_rente > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_cm_rente,
       SUM(CASE WHEN date_cm_rente > date_liq
                THEN 1 ELSE 0 END)                                AS cm_rente_apres_liq,
       COUNT(date_avis_mc)                                        AS avec_avis_mc,
       ROUND(MEDIAN(date_liq - date_avis_mc))                     AS med_depuis_avis_mc,
       COUNT(date_demande)                                        AS avec_date_demande,
       ROUND(MEDIAN(date_liq - date_demande))                     AS med_depuis_demande
FROM base
GROUP BY ligne
ORDER BY ligne;


-- =============================================================================
-- T26 Q4. LES FRAIS MÉDICAUX
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P63 — Remboursements de soins des dossiers AT/MP
-- Question : T26 Q4. Un dossier de soins seuls est-il une demande liquidée ?
--            À quelle date ?
-- Objectif : par année du premier remboursement approuvé (2019 à 2025) :
--            dossiers AT/MP avec des soins, dont soins seuls (aucune
--            prestation en espèces), remboursements par dossier, jamais
--            approuvés, avec un paiement (D ou P), délai médian de la
--            réception d'ouverture, de l'accident et du soin au premier
--            remboursement saisi et approuvé.
-- Lecture  : Kaya 2017 mesure la liquidation des soins du certificat du
--            médecin traitant à la convocation, une date que la base n'a pas.
--            Si les soins seuls sont nombreux, il faut choisir avec la CNSS :
--            les compter en « autres prestations de la branche », liquidés à
--            la première approbation (Proposition BI, déjà dans la DTM_DOSSIER
--            proposée), ou les laisser hors de T26.
-- Tableaux : T26.
-- Résultat : Par année du premier remboursement de soins (dossiers / soins
--            seuls / remboursements par dossier / délai médian réception →
--            première approbation / plus de 45 j) : 2019 756 / 406 / 10,4 /
--            77 j / 453 ; 2020 840 / 431 / 10,4 / 91 j / 587 ; 2021 960 / 547 /
--            10,0 / 84 j / 655 ; 2022 855 / 512 / 10,2 / 78 j / 565 ;
--            2023 699 / 375 / 9,6 / 125 j / 554 ; 2024 1 477 / 780 / 7,8 /
--            150 j / 426 ; 2025 659 / 423 / 7,7 / 161 j / 573. Jamais
--            approuvés : 3 à 23 par an ; presque tous ont un paiement. Délai
--            médian accident → approbation 98 à 195 j, soin → approbation 59 à
--            104 j, sauf 2024 : 973 dossiers sans réception, accident et soin
--            17 ans avant l'approbation en médiane (6 479 et 6 435 j).
-- Constat  : Les soins sont un flux de remboursements (8 à 10 par dossier), pas
--            une demande unique, et plus de la moitié de ces dossiers n'ont que
--            des soins. Si T26 les compte en « autres prestations de la
--            branche », liquidés au premier remboursement approuvé (Proposition
--            BI), 650 à 960 dossiers par an y entrent, liquidés 3 à 5 mois
--            après la déclaration. Le pic de 2024 vient de vieux dossiers sans
--            réception (accidents d'il y a 17 ans) : soins réels de rentiers ou
--            reprise de données ? À vérifier (P73) avant de les compter.
-- -----------------------------------------------------------------------------
WITH dosa AS (
    SELECT DOS_CODE, RD_ID, TRUNC(DOS_DATE_ACCIDENT) AS date_accident
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'A'
),
soins AS (
    SELECT deb.DOS_CODE,
           COUNT(*)                                               AS nb_rb,
           SUM(CASE WHEN deb.DEB_STATUT IN ('D', 'P')
                    THEN 1 ELSE 0 END)                            AS nb_payes,
           TRUNC(MIN(deb.DEB_DATE_INSERT))                        AS premiere_saisie,
           TRUNC(MIN(deb.DEB_DATE_APPROBATION))                   AS premiere_approbation,
           TRUNC(MIN(deb.DEB_DATE_PRESTATION))                    AS premier_soin
    FROM USER_DWH.DEBOURS deb
    JOIN dosa ON dosa.DOS_CODE = deb.DOS_CODE
    WHERE deb.TPN_CODE IS NOT NULL
    GROUP BY deb.DOS_CODE
),
esp AS (
    SELECT DISTINCT pe.DOS_CODE
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN dosa ON dosa.DOS_CODE = pe.DOS_CODE
),
base AS (
    SELECT s.nb_rb,
           s.nb_payes,
           s.premiere_saisie,
           s.premiere_approbation,
           s.premier_soin,
           a.date_accident,
           TRUNC(r.RD_DATE_RECEPTION)                             AS date_reception,
           CASE WHEN e.DOS_CODE IS NULL THEN 1 ELSE 0 END         AS soins_seuls,
           EXTRACT(YEAR FROM NVL(s.premiere_approbation,
                                 s.premiere_saisie))              AS annee
    FROM soins s
    JOIN dosa a ON a.DOS_CODE = s.DOS_CODE
    LEFT JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID    = a.RD_ID
    LEFT JOIN esp                        e ON e.DOS_CODE = s.DOS_CODE
)
SELECT annee,
       COUNT(*)                                                   AS dossiers_avec_soins,
       SUM(soins_seuls)                                           AS dont_soins_seuls,
       ROUND(AVG(nb_rb), 1)                                       AS remboursements_par_dossier,
       SUM(CASE WHEN premiere_approbation IS NULL
                THEN 1 ELSE 0 END)                                AS jamais_approuves,
       SUM(CASE WHEN nb_payes > 0 THEN 1 ELSE 0 END)              AS avec_un_paiement,
       COUNT(date_reception)                                      AS avec_reception,
       ROUND(MEDIAN(premiere_saisie - date_reception))            AS med_recep_a_saisie_j,
       ROUND(MEDIAN(premiere_approbation - date_reception))       AS med_recep_a_approbation_j,
       SUM(CASE WHEN premiere_approbation - date_reception > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_recep_approbation,
       ROUND(MEDIAN(premiere_approbation - date_accident))        AS med_accident_a_approbation_j,
       ROUND(MEDIAN(premiere_approbation - premier_soin))         AS med_soin_a_approbation_j
FROM base
WHERE annee BETWEEN 2019 AND 2025
GROUP BY annee
ORDER BY annee;


-- =============================================================================
-- T27 Q1. UN ENFANT DE PLUS, UNE DEMANDE DE PLUS ?
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P64 — Dossiers PF : première notification et notifications suivantes
-- Question : T27 Q1. Quand un allocataire ajoute un enfant, est-ce une
--            nouvelle demande ?
-- Objectif : par année de notification (2019 à 2025) : prestations PF
--            notifiées (FAF, FAP), dont celles notifiées avec la première
--            notification de leur dossier et celles notifiées après ;
--            dossiers à leur première notification, dossiers renotifiés ;
--            écart médian avec la première notification ; dossiers ouverts
--            dans l'année.
-- Lecture  : en 2025, 16 075 dossiers PF ont une notification pour 9 205
--            ouverts (P09). Si l'écart vient d'enfants ajoutés à un dossier
--            existant, la question Q4 de T25 revient sous une autre forme :
--            l'enfant ajouté reste sur la ligne « allocations familiales »,
--            donc compter par dossier × ligne ne le voit pas. Il faudra
--            choisir : une demande = un dossier (sa première notification),
--            ou une demande = chaque ajout notifié.
-- Tableaux : T27.
-- Résultat : Par année (dossiers ouverts / prestations notifiées / avec la 1re
--            notification du dossier / après / dossiers à leur 1re
--            notification / dossiers renotifiés / écart médian avec la 1re
--            notification) : 2019 9 447 / 31 667 / 21 267 / 10 400 / 9 992 /
--            9 595 / 1 785 j ; 2020 8 261 / 26 852 / 17 462 / 9 390 / 8 505 /
--            8 693 / 1 759 j ; 2021 8 876 / 29 579 / 19 153 / 10 426 / 9 179 /
--            9 637 / 1 706 j ; 2022 8 527 / 27 438 / 17 595 / 9 843 / 8 617 /
--            9 082 / 1 868 j ; 2023 8 958 / 28 423 / 18 154 / 10 269 / 9 049 /
--            9 479 / 1 959 j ; 2024 9 682 / 29 415 / 19 587 / 9 828 / 9 653 /
--            8 980 / 1 980 j ; 2025 9 205 / 25 786 / 16 806 / 8 980 / 8 246 /
--            8 124 / 2 098 j. Presque toutes sont des FAF (FAP : 33 en 2025).
-- Constat  : Chaque année, environ 9 000 dossiers d'allocataires reçoivent un
--            enfant de plus, en médiane 5 ans après leur première
--            notification : autant que de nouveaux allocataires (8 246 en 2025,
--            2 enfants chacun en moyenne). Les 16 075 dossiers notifiés de 2025
--            (P09) sont ces deux groupes. Compter par dossier × ligne ne voit
--            pas l'enfant ajouté, qui reste en ligne « allocations
--            familiales ». La CNSS doit choisir (T27 Q1) : les nouveaux
--            allocataires seulement (8 246 liquidés en 2025), ou aussi chaque
--            ajout notifié (environ 16 400).
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2018 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 7
),
pf AS (
    SELECT pe.DOS_CODE,
           pe.TPE_CODE,
           TRUNC(pe.PE_DATE_NOTIFICATION)                         AS date_notif,
           EXTRACT(YEAR FROM pe.PE_DATE_NOTIFICATION)             AS annee_notif,
           MIN(TRUNC(pe.PE_DATE_NOTIFICATION))
               OVER (PARTITION BY pe.DOS_CODE)                    AS premiere_notif
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE = 'F'
),
ouverts AS (
    SELECT EXTRACT(YEAR FROM DOS_DATE_OUVERTURE)                  AS annee,
           COUNT(*)                                               AS nb
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'F'
    GROUP BY EXTRACT(YEAR FROM DOS_DATE_OUVERTURE)
)
SELECT a.annee,
       MAX(o.nb)                                                  AS dossiers_ouverts,
       COUNT(p.DOS_CODE)                                          AS prestations_notifiees,
       SUM(CASE WHEN p.TPE_CODE = 'FAF' THEN 1 ELSE 0 END)        AS dont_faf,
       SUM(CASE WHEN p.TPE_CODE = 'FAP' THEN 1 ELSE 0 END)        AS dont_fap,
       SUM(CASE WHEN p.date_notif = p.premiere_notif
                THEN 1 ELSE 0 END)                                AS avec_1re_notif_du_dossier,
       SUM(CASE WHEN p.date_notif > p.premiere_notif
                THEN 1 ELSE 0 END)                                AS apres_1re_notif_du_dossier,
       COUNT(DISTINCT CASE WHEN p.date_notif = p.premiere_notif
                           THEN p.DOS_CODE END)                   AS dossiers_1re_notif,
       COUNT(DISTINCT CASE WHEN p.date_notif > p.premiere_notif
                           THEN p.DOS_CODE END)                   AS dossiers_renotifies,
       ROUND(MEDIAN(CASE WHEN p.date_notif > p.premiere_notif
                         THEN p.date_notif - p.premiere_notif END)) AS med_ecart_1re_notif_j
FROM annees a
LEFT JOIN pf      p ON p.annee_notif = a.annee
LEFT JOIN ouverts o ON o.annee       = a.annee
GROUP BY a.annee
ORDER BY a.annee;


-- -----------------------------------------------------------------------------
-- P65 — Réceptions PF et maternité : type et pièces fournies
-- Question : T27 Q1 et Q3.
-- Objectif : pour les réceptions PF (F) et maternité (M) de 2019 à 2025, le
--            type (NOUVEAU, ANCIEN) et ce que disent les pièces :
--            accouchement, congé ou arrêt de travail, examens prénataux, acte
--            de naissance, scolarité ou certificat de vie. Une réception est
--            classée par sa première pièce dans cet ordre.
-- Lecture  : un enfant ajouté arrive-t-il par une réception ANCIEN sur le
--            dossier de l'allocataire, avec un acte de naissance ? Si oui,
--            c'est un dépôt de plus, comme la réversion en T25 (règle D3).
--            Pour la maternité : une réception par grossesse, avec quelles
--            pièces ?
-- Tableaux : T27.
-- Résultat : Réceptions de 2019 à 2025. PF : NOUVEAU 65 731, dont 65 204 avec
--            un acte de naissance (9 930 en 2025 ; 63 146 ouvrent un dossier) ;
--            ANCIEN 300 seulement (6 en 2025), surtout des certificats de
--            scolarité ou de vie (153) ; type vide 62. Maternité : 27 381
--            réceptions, toutes NOUVEAU sauf 18 vides, classées par leur pièce
--            la plus avancée : accouchement 5 352, congé ou arrêt 9 772,
--            examens prénataux 12 090, autres 167 ; 98 % ouvrent un dossier.
-- Constat  : Un enfant ajouté ne passe pas par le guichet : 6 réceptions ANCIEN
--            en 2025 pour 8 980 enfants ajoutés (P64). S'il compte comme une
--            demande, sa date de dépôt sera la date de demande de la
--            prestation, comme la réversion sans réception en T25 (règle D3).
--            La maternité a une réception par grossesse, avec les examens
--            prénataux, le congé ou l'accouchement : c'est une demande NOUVEAU
--            qui ouvre son dossier (règle D3).
-- -----------------------------------------------------------------------------
WITH recep0 AS (
    SELECT r.RD_ID,
           r.TDOS_CODE,
           r.DOS_CODE,
           NVL(r.RD_TYPE_RECEPTION, '(vide)')                     AS type_reception,
           EXTRACT(YEAR FROM r.RD_DATE_RECEPTION)                 AS annee
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE IN ('F', 'M')
      AND r.RD_DATE_RECEPTION >= DATE '2019-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
),
pieces AS (
    SELECT p.RD_ID,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) = 'CAC'
                    THEN 1 ELSE 0 END)                            AS accouchement,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('AC', 'CAT', 'CMAT')
                    THEN 1 ELSE 0 END)                            AS conge,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CEP1', 'CEP2', 'CEP3', 'CG')
                    THEN 1 ELSE 0 END)                            AS prenatal,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('AN', 'ANENFANT')
                    THEN 1 ELSE 0 END)                            AS naissance,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CS', 'CV', 'CVENFANT')
                    THEN 1 ELSE 0 END)                            AS scolarite_vie
    FROM USER_DWH.RECEPTION_DOS_PIECE p
    JOIN recep0 r ON r.RD_ID = p.RD_ID
    GROUP BY p.RD_ID
),
ouvre AS (
    SELECT DISTINCT o.RD_ID
    FROM USER_DWH.DOSSIER o
    JOIN recep0 r ON r.RD_ID = o.RD_ID
),
recep AS (
    SELECT r.TDOS_CODE,
           r.type_reception,
           r.annee,
           CASE WHEN p.RD_ID IS NULL         THEN '7 sans pièce'
                WHEN p.accouchement  = 1     THEN '1 accouchement'
                WHEN p.conge         = 1     THEN '2 congé ou arrêt'
                WHEN p.prenatal      = 1     THEN '3 examens prénataux'
                WHEN p.naissance     = 1     THEN '4 acte de naissance'
                WHEN p.scolarite_vie = 1     THEN '5 scolarité ou vie'
                ELSE                              '6 autres pièces'
           END                                                    AS motif,
           CASE WHEN o.RD_ID IS NOT NULL THEN 1 ELSE 0 END        AS ouvre_dossier,
           CASE WHEN r.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END     AS vise_dossier
    FROM recep0 r
    LEFT JOIN pieces p ON p.RD_ID = r.RD_ID
    LEFT JOIN ouvre  o ON o.RD_ID = r.RD_ID
)
SELECT TDOS_CODE,
       type_reception,
       motif,
       COUNT(*)                                                   AS receptions_2019_2025,
       SUM(CASE WHEN annee = 2025 THEN 1 ELSE 0 END)              AS receptions_2025,
       SUM(ouvre_dossier)                                         AS ouvrent_un_dossier,
       SUM(vise_dossier)                                          AS visent_un_dossier
FROM recep
GROUP BY TDOS_CODE, type_reception, motif
ORDER BY TDOS_CODE, type_reception, motif;


-- =============================================================================
-- T27 Q2 ET Q3. ALLOCATIONS PRÉNATALES ET MATERNITÉ
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P66 — Prestations PF et maternité : dates de demande et de notification
-- Question : T27 Q2. Kaya 2017 : l'allocation prénatale « ne fait l'objet ni
--            de réception ni de notification ». Est-ce encore vrai ?
-- Objectif : pour FAF, FAP, MIJ et MAP : prestations, saisies en 2025, avec
--            date de demande, avec date de notification, avec date de
--            grossesse ou d'accouchement probable, dans un dossier ouvert par
--            une réception, dans un dossier qui porte aussi une FAF ou une
--            MIJ, notifiées en 2025, délai médian demande → notification.
-- Lecture  : si FAP et MAP ont une date de notification, la règle D4 de T25
--            s'applique. Sinon, il faut une autre date de liquidation (calcul
--            ou premier paiement, Proposition BI) ou les sortir de la durée
--            de liquidation, comme le laissait entendre Kaya 2017. Une FAP
--            toujours dans un dossier avec une FAF n'est pas une demande à
--            part : elle suit le dossier de l'allocataire.
-- Tableaux : T27.
-- Résultat : Par code (prestations / saisies en 2025 / avec date de
--            notification / dossier ouvert par une réception / dans un dossier
--            avec FAF ou MIJ / notifiées en 2025 / délai médian demande →
--            notification en 2025) : FAF 614 811 / 29 794 / 419 898 / 293 468 /
--            614 811 / 25 753 / 11 j ; FAP 9 600 / 37 / 3 869 / 498 / 8 275 /
--            33 / 2 j ; MAP 52 716 / 4 236 / 31 950 / 41 506 / 52 708 / 4 245 /
--            15 j ; MIJ 56 426 / 4 260 / 45 611 / 41 772 / 56 426 / 4 273 /
--            15 j. Toutes ont une date de demande. Date de grossesse : toutes
--            les FAP, un tiers des MAP et des MIJ.
-- Constat  : Kaya 2017 ne vaut plus ici : l'allocation prénatale est
--            aujourd'hui la MAP de la branche maternité, déposée au guichet
--            avec le dossier de grossesse et notifiée avec l'IJ, 15 j après la
--            demande en médiane. L'ancienne FAP de la branche PF est presque
--            éteinte (33 en 2025). La règle D4 (première notification)
--            s'applique à toutes les lignes de T27. Les prestations anciennes
--            n'ont pas toutes une date de notification (FAF : 68 %), sans effet
--            sur les années récentes.
-- -----------------------------------------------------------------------------
WITH prest AS (
    SELECT pe.TPE_CODE,
           pe.PE_DATE_INSERT,
           pe.PE_DATE_DEMANDE,
           pe.PE_DATE_NOTIFICATION,
           pe.PE_DATE_GROSSESSE,
           pe.PE_DATEPROBABLE_ACC,
           d.RD_ID,
           MAX(CASE WHEN pe.TPE_CODE IN ('FAF', 'MIJ') THEN 1 ELSE 0 END)
               OVER (PARTITION BY pe.DOS_CODE)                    AS avec_faf_ou_mij
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE IN ('F', 'M')
)
SELECT TPE_CODE,
       COUNT(*)                                                   AS prestations,
       SUM(CASE WHEN PE_DATE_INSERT >= DATE '2025-01-01'
                 AND PE_DATE_INSERT <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                                AS saisies_2025,
       SUM(CASE WHEN PE_DATE_DEMANDE IS NOT NULL
                THEN 1 ELSE 0 END)                                AS avec_date_demande,
       SUM(CASE WHEN PE_DATE_NOTIFICATION IS NOT NULL
                THEN 1 ELSE 0 END)                                AS avec_date_notification,
       SUM(CASE WHEN PE_DATE_GROSSESSE IS NOT NULL
                  OR PE_DATEPROBABLE_ACC IS NOT NULL
                THEN 1 ELSE 0 END)                                AS avec_date_grossesse,
       SUM(CASE WHEN RD_ID IS NOT NULL THEN 1 ELSE 0 END)         AS dossier_ouvert_par_reception,
       SUM(avec_faf_ou_mij)                                       AS dossier_avec_faf_ou_mij,
       SUM(CASE WHEN PE_DATE_NOTIFICATION >= DATE '2025-01-01'
                 AND PE_DATE_NOTIFICATION <  DATE '2026-01-01'
                THEN 1 ELSE 0 END)                                AS notifiees_2025,
       ROUND(MEDIAN(CASE WHEN PE_DATE_NOTIFICATION >= DATE '2025-01-01'
                          AND PE_DATE_NOTIFICATION <  DATE '2026-01-01'
                         THEN TRUNC(PE_DATE_NOTIFICATION)
                              - TRUNC(PE_DATE_DEMANDE) END))      AS med_demande_notif_2025_j
FROM prest
WHERE TPE_CODE IN ('FAF', 'FAP', 'MIJ', 'MAP')
GROUP BY TPE_CODE
ORDER BY TPE_CODE;


-- -----------------------------------------------------------------------------
-- P67 — Dossiers de maternité : un dossier par grossesse ?
-- Question : T27 Q3.
-- Objectif : par année d'ouverture (2019 à 2025) des dossiers de maternité
--            (M) : dossiers, mères distinctes, dossiers d'une mère qui en a
--            deux ou plus la même année, dossiers avec IJ seules, allocation
--            prénatale seule, les deux, aucune prestation ; délai médian
--            réception → première notification ; réception avant ou après la
--            date probable d'accouchement.
-- Lecture  : si une mère a un dossier par grossesse, avec l'IJ et
--            l'allocation prénatale dans le même dossier, on compte comme en
--            T25 : une demande par ligne (MIJ → IJ de maternité, MAP →
--            allocations prénatales). Si la réception arrive après
--            l'accouchement, la durée ne compte pas l'attente de la
--            naissance : la date de réception convient (règle D5).
-- Tableaux : T27.
-- Résultat : Par année d'ouverture (dossiers / mères / dossiers d'une mère qui
--            en a deux la même année / IJ et prénatale dans le même dossier /
--            délai médian réception → notification / reçus avant la date
--            probable d'accouchement / après / écart médian réception −
--            accouchement probable) : 2019 3 248 / 3 244 / 8 / 3 223 / 7 j ;
--            2020 3 560 / 3 554 / 12 / 3 543 / 8 j ; 2021 3 823 / 3 822 / 2 /
--            3 813 / 8 j / 1 733 / 480 / −22 j ; 2022 3 976 / 3 974 / 4 /
--            3 954 / 12 j / 3 164 / 807 / −24 j ; 2023 4 060 / 4 058 / 4 /
--            4 042 / 8 j / 3 228 / 826 / −23 j ; 2024 3 938 / 3 934 / 8 /
--            3 914 / 11 j / 3 151 / 782 / −24 j ; 2025 4 238 / 4 233 / 9 /
--            4 211 / 16 j / 3 461 / 768 / −24 j. IJ seules 3 à 19 par an,
--            prénatale seule 0 ou 1, sans prestation 4 à 9. Date probable
--            d'accouchement remplie depuis 2021 ; date de congé et date de
--            grossesse du dossier jamais remplies.
-- Constat  : Un dossier de maternité = une grossesse : une mère n'en a qu'un
--            par an, déposé environ 3 semaines avant l'accouchement, et il
--            porte l'IJ et l'allocation prénatale dans 99,4 % des cas,
--            notifiées ensemble 1 à 2 semaines après. Compter par dossier ×
--            ligne fait de chaque grossesse deux demandes, une sur chaque
--            ligne : la CNSS doit dire si c'est ce qu'elle attend, ou si la
--            grossesse ne compte qu'une fois (T27 Q3).
-- -----------------------------------------------------------------------------
WITH dosm AS (
    SELECT d.DOS_CODE,
           d.IND_ID,
           d.RD_ID,
           EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE)                AS annee,
           TRUNC(d.DOS_DATE_GROSSESSE)                            AS date_grossesse
    FROM USER_DWH.DOSSIER d
    WHERE d.TDOS_CODE = 'M'
      AND d.DOS_DATE_OUVERTURE >= DATE '2019-01-01'
      AND d.DOS_DATE_OUVERTURE <  DATE '2026-01-01'
),
prest AS (
    SELECT pe.DOS_CODE,
           MAX(CASE WHEN pe.TPE_CODE = 'MIJ' THEN 1 ELSE 0 END)  AS ij,
           MAX(CASE WHEN pe.TPE_CODE = 'MAP' THEN 1 ELSE 0 END)  AS prenat,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))                    AS premiere_notif,
           TRUNC(MIN(pe.PE_DATEPROBABLE_ACC))                     AS date_probable_acc,
           TRUNC(MIN(pe.PE_DATE_CONGE))                           AS date_conge
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN dosm ON dosm.DOS_CODE = pe.DOS_CODE
    GROUP BY pe.DOS_CODE
),
base AS (
    SELECT dm.annee,
           dm.IND_ID,
           COUNT(*) OVER (PARTITION BY dm.IND_ID, dm.annee)       AS nb_dossiers_mere,
           NVL(p.ij, 0)                                           AS ij,
           NVL(p.prenat, 0)                                       AS prenat,
           CASE WHEN p.DOS_CODE IS NULL THEN 1 ELSE 0 END         AS sans_prest,
           p.premiere_notif,
           p.date_probable_acc,
           p.date_conge,
           dm.date_grossesse,
           TRUNC(r.RD_DATE_RECEPTION)                             AS date_reception
    FROM dosm dm
    LEFT JOIN prest p ON p.DOS_CODE = dm.DOS_CODE
    LEFT JOIN USER_DWH.RECEPTION_DOSSIER r ON r.RD_ID = dm.RD_ID
)
SELECT annee,
       COUNT(*)                                                   AS dossiers,
       COUNT(DISTINCT IND_ID)                                     AS meres,
       SUM(CASE WHEN nb_dossiers_mere >= 2 THEN 1 ELSE 0 END)     AS dossiers_meme_mere_annee,
       SUM(CASE WHEN ij = 1 AND prenat = 0 THEN 1 ELSE 0 END)     AS ij_seules,
       SUM(CASE WHEN ij = 0 AND prenat = 1 THEN 1 ELSE 0 END)     AS prenatale_seule,
       SUM(CASE WHEN ij = 1 AND prenat = 1 THEN 1 ELSE 0 END)     AS ij_et_prenatale,
       SUM(sans_prest)                                            AS sans_prestation,
       ROUND(MEDIAN(premiere_notif - date_reception))             AS med_recep_a_notif_j,
       SUM(CASE WHEN date_reception <  date_probable_acc
                THEN 1 ELSE 0 END)                                AS recus_avant_acc_probable,
       SUM(CASE WHEN date_reception >= date_probable_acc
                THEN 1 ELSE 0 END)                                AS recus_apres_acc_probable,
       ROUND(MEDIAN(date_reception - date_probable_acc))          AS med_acc_probable_a_recep_j,
       COUNT(date_conge)                                          AS avec_date_conge,
       COUNT(date_grossesse)                                      AS avec_date_grossesse
FROM base
GROUP BY annee
ORDER BY annee;


-- =============================================================================
-- T26 Q5 ET T27 Q4. REFUS ET STOCK EN INSTANCE
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P68 — AT/MP, PF et maternité : refus et dossiers sans suite
-- Question : T26 Q5 et T27 Q4. Où sont les refus, et quel est le stock en
--            instance ?
-- Objectif : par branche et par année (2019 à 2025) : réceptions,
--            réceptions sans dossier ni rejet ni dossier visé, dossiers
--            ouverts, dossiers ouverts sans prestation en espèces ni soins,
--            dont fermés, dossiers avec un rejet, rejets enregistrés (année
--            du rejet).
-- Lecture  : en T25, le module des rejets sert peu, et un dossier fermé sans
--            notification n'est pas un refus (P55 à P57). Si c'est pareil
--            ici, les règles D6 (stock) et D7 (refus) de T25 s'appliquent
--            telles quelles ; sinon, il faudra une règle par branche. Une
--            déclaration d'accident sans arrêt ni soin ne donne aucune
--            prestation : ce n'est pas une demande en attente (à confirmer
--            avec la CNSS).
-- Tableaux : T26, T27.
-- Résultat : Par branche et par année (réceptions / sans suite / dossiers
--            ouverts / sans aucune prestation / dont fermés / avec un rejet /
--            rejets de l'année). AT/MP : 2019 1 638 / 108 / 1 328 / 556 / 14 /
--            2 / 1 ; 2020 1 639 / 49 / 1 403 / 538 / 7 / 5 / 7 ; 2021 1 545 /
--            35 / 1 353 / 469 / 8 / 3 / 2 ; 2022 1 542 / 35 / 1 370 / 541 / 2 /
--            3 / 4 ; 2023 1 379 / 34 / 1 226 / 604 / 2 / 6 / 7 ; 2024 1 394 /
--            70 / 1 189 / 629 / 1 / 0 / 0 ; 2025 1 481 / 104 / 1 237 / 821 /
--            0 / 0 / 1. PF : de 8 261 à 9 682 dossiers ouverts par an, 20 à 57
--            sans prestation, 196 à 511 réceptions sans suite (483 en 2025), 2
--            à 62 rejets. Maternité : de 3 248 à 4 238 dossiers ouverts, 4 à 9
--            sans prestation, 37 à 141 réceptions sans suite, 0 à 17 rejets.
-- Constat  : Le module des rejets sert aussi peu qu'en T25 : 0 à 7 rejets par
--            an en AT/MP, 2 à 62 en PF et en maternité (règle D7 telle quelle).
--            En PF et en maternité, presque tous les dossiers ouverts donnent
--            une prestation : la règle du stock de T25 (D6) s'applique. En
--            AT/MP, 35 à 53 % des dossiers ouverts de 2019 à 2024 ne donnent
--            jamais rien, et ils ne sont presque jamais fermés : avec la règle
--            D6, plus de 4 000 resteraient en instance. La CNSS doit dire si
--            une déclaration sans arrêt ni soin est une demande à liquider (T26
--            Q5).
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2018 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 7
),
branches AS (
    SELECT 'A' AS tdos FROM DUAL
    UNION ALL SELECT 'F' FROM DUAL
    UNION ALL SELECT 'M' FROM DUAL
),
recep0 AS (
    SELECT r.RD_ID,
           r.TDOS_CODE                                            AS tdos,
           EXTRACT(YEAR FROM r.RD_DATE_RECEPTION)                 AS annee,
           r.DOS_CODE
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE IN ('A', 'F', 'M')
      AND r.RD_DATE_RECEPTION >= DATE '2019-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
),
ouvre AS (
    SELECT DISTINCT RD_ID FROM USER_DWH.DOSSIER WHERE RD_ID IS NOT NULL
),
rejete_rd AS (
    SELECT DISTINCT RD_ID FROM USER_DWH.DOSSIER_REJETE WHERE RD_ID IS NOT NULL
),
rec_agg AS (
    SELECT r.tdos,
           r.annee,
           COUNT(*)                                               AS nb,
           SUM(CASE WHEN o.RD_ID IS NULL
                     AND x.RD_ID IS NULL
                     AND r.DOS_CODE IS NULL
                    THEN 1 ELSE 0 END)                            AS sans_suite
    FROM recep0 r
    LEFT JOIN ouvre     o ON o.RD_ID = r.RD_ID
    LEFT JOIN rejete_rd x ON x.RD_ID = r.RD_ID
    GROUP BY r.tdos, r.annee
),
dos AS (
    SELECT d.DOS_CODE,
           d.TDOS_CODE                                            AS tdos,
           d.DOS_STATUT,
           EXTRACT(YEAR FROM d.DOS_DATE_OUVERTURE)                AS annee
    FROM USER_DWH.DOSSIER d
    WHERE d.TDOS_CODE IN ('A', 'F', 'M')
      AND d.DOS_DATE_OUVERTURE >= DATE '2019-01-01'
      AND d.DOS_DATE_OUVERTURE <  DATE '2026-01-01'
),
esp AS (
    SELECT DISTINCT pe.DOS_CODE
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN dos ON dos.DOS_CODE = pe.DOS_CODE
),
soins AS (
    SELECT DISTINCT deb.DOS_CODE
    FROM USER_DWH.DEBOURS deb
    JOIN dos ON dos.DOS_CODE = deb.DOS_CODE
    WHERE dos.tdos = 'A'
      AND deb.TPN_CODE IS NOT NULL
),
rejete_dos AS (
    SELECT DISTINCT DOS_CODE FROM USER_DWH.DOSSIER_REJETE WHERE DOS_CODE IS NOT NULL
),
dos_agg AS (
    SELECT d.tdos,
           d.annee,
           COUNT(*)                                               AS ouverts,
           SUM(CASE WHEN e.DOS_CODE IS NULL AND s.DOS_CODE IS NULL
                    THEN 1 ELSE 0 END)                            AS sans_prestation,
           SUM(CASE WHEN e.DOS_CODE IS NULL AND s.DOS_CODE IS NULL
                     AND d.DOS_STATUT = 'F'
                    THEN 1 ELSE 0 END)                            AS sans_prestation_fermes,
           SUM(CASE WHEN j.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END) AS avec_rejet
    FROM dos d
    LEFT JOIN esp        e ON e.DOS_CODE = d.DOS_CODE
    LEFT JOIN soins      s ON s.DOS_CODE = d.DOS_CODE
    LEFT JOIN rejete_dos j ON j.DOS_CODE = d.DOS_CODE
    GROUP BY d.tdos, d.annee
),
rej_agg AS (
    SELECT TDOS_CODE                                              AS tdos,
           EXTRACT(YEAR FROM DOSRE_DATE_REJET)                    AS annee,
           COUNT(*)                                               AS nb
    FROM USER_DWH.DOSSIER_REJETE
    WHERE TDOS_CODE IN ('A', 'F', 'M')
    GROUP BY TDOS_CODE, EXTRACT(YEAR FROM DOSRE_DATE_REJET)
)
SELECT b.tdos                                                     AS branche,
       a.annee,
       NVL(rc.nb, 0)                                              AS receptions,
       NVL(rc.sans_suite, 0)                                      AS receptions_sans_suite,
       NVL(dg.ouverts, 0)                                         AS dossiers_ouverts,
       NVL(dg.sans_prestation, 0)                                 AS ouverts_sans_prestation,
       NVL(dg.sans_prestation_fermes, 0)                          AS dont_fermes,
       NVL(dg.avec_rejet, 0)                                      AS ouverts_avec_rejet,
       NVL(rj.nb, 0)                                              AS rejets_de_l_annee
FROM branches b
CROSS JOIN annees a
LEFT JOIN rec_agg rc ON rc.tdos = b.tdos AND rc.annee = a.annee
LEFT JOIN dos_agg dg ON dg.tdos = b.tdos AND dg.annee = a.annee
LEFT JOIN rej_agg rj ON rj.tdos = b.tdos AND rj.annee = a.annee
ORDER BY b.tdos, a.annee;


-- =============================================================================
-- PARTIE B — SUITES DU LOT 4 (rédigée et exécutée le 30/10/2026 après les
-- résultats de P59 à P68)
-- Cinq points que la partie A n'a pas tranchés :
--   T26 Q3 date de départ des IJ                    → P69
--   T26 Q1 réceptions ANCIEN « autres pièces »      → P70, P71
--   T26 Q2 allocation d'incapacité sans date        → P72
--   T26 Q4 pic des soins de 2024                    → P73
-- P71 et P73 lisent DEBOURS, filtré sur les dossiers AT/MP concernés.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P69 — IJ AT/MP : quelle date de départ pour la durée ?
-- Question : T26 Q3. Les 183 j de P62 comptent l'arrêt de travail : d'où
--            partir pour mesurer le travail de la CNSS ?
-- Objectif : pour les IJ liquidées en 2025 (première IJ notifiée du dossier),
--            comparer les dates de départ possibles : reprise du travail
--            (DOS_DATE_REPRISE), début de l'IJ (PE_DATE_EFFET) et fin de
--            l'arrêt (début + PE_DUREE_ARRET), date de traitement de l'IJ,
--            dernière pièce saisie sur une réception du dossier avant la
--            liquidation, et dernière pièce de guérison, de reprise ou de
--            consolidation. Pour chacune : combien d'IJ l'ont, durée médiane,
--            plus de 45 j.
-- Lecture  : si l'IJ est notifiée peu après la reprise ou après la dernière
--            pièce, la CNSS liquide l'IJ à la fin de l'arrêt, sur un dossier
--            complété par les certificats : la durée de T26 doit partir de
--            cette date (le dossier complet, comme dit Kaya 2017), pas de la
--            déclaration.
-- Tableaux : T26.
-- Résultat : 226 IJ liquidées en 2025 (première IJ notifiée du dossier). Date
--            de reprise du travail et date de traitement : jamais remplies.
--            Début de l'IJ : toujours rempli, 266 j avant la liquidation en
--            médiane. Durée de l'arrêt : toujours remplie, 27 j en médiane ; de
--            la fin de l'arrêt à la liquidation : 211 j. Dernière pièce saisie
--            avant la liquidation : 225 IJ, 183 j avant en médiane, plus de
--            45 j pour 192. Pièce de guérison, de reprise ou de consolidation :
--            14 IJ seulement, saisie la veille de la liquidation en médiane
--            (plus de 45 j avant pour 2).
-- Constat  : L'arrêt de travail ne dure qu'un mois en médiane : les 183 j de
--            P62 ne sont pas l'arrêt (correction de P62). Une IJ est notifiée
--            environ 7 mois après la fin de l'arrêt, et aucune date de la base
--            n'explique cette attente : la reprise du travail n'est jamais
--            saisie, et le certificat de guérison ou de reprise ne l'est que
--            pour 14 IJ sur 226 (quand il l'est, la liquidation suit le
--            lendemain). Proposition BI : garder la déclaration comme dépôt de
--            l'IJ (règle D3) et publier la durée telle qu'elle est. Question à
--            la CNSS : qu'attend une IJ pendant ces mois (certificat de
--            guérison non saisi, attestation de salaire de l'employeur) ?
-- -----------------------------------------------------------------------------
WITH ij AS (
    SELECT pe.DOS_CODE,
           TRUNC(MIN(pe.PE_DATE_NOTIFICATION))                    AS date_liq,
           MIN(TRUNC(pe.PE_DATE_EFFET))
               KEEP (DENSE_RANK FIRST ORDER BY pe.PE_DATE_NOTIFICATION) AS debut_ij,
           MIN(pe.PE_DUREE_ARRET)
               KEEP (DENSE_RANK FIRST ORDER BY pe.PE_DATE_NOTIFICATION) AS duree_arret,
           MIN(TRUNC(pe.PE_DATE_TRAITEMENT))
               KEEP (DENSE_RANK FIRST ORDER BY pe.PE_DATE_NOTIFICATION) AS date_traitement
    FROM USER_DWH.PRESTATION_ESP pe
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = pe.DOS_CODE
    WHERE d.TDOS_CODE = 'A'
      AND pe.TPE_CODE = 'AIJ'
      AND pe.PE_DATE_NOTIFICATION IS NOT NULL
    GROUP BY pe.DOS_CODE
),
recep_dos AS (
    SELECT DOS_CODE, RD_ID
    FROM USER_DWH.DOSSIER
    WHERE TDOS_CODE = 'A'
      AND RD_ID IS NOT NULL
    UNION
    SELECT DOS_CODE, RD_ID
    FROM USER_DWH.RECEPTION_DOSSIER
    WHERE TDOS_CODE = 'A'
      AND DOS_CODE IS NOT NULL
),
pieces AS (
    SELECT x.DOS_CODE,
           TRUNC(p.RDP_DATE_INSERT)                               AS date_piece,
           CASE WHEN TRIM(p.TPJ_CODE) IN ('CMG', 'CRT', 'CMCON')
                THEN 1 ELSE 0 END                                 AS fin_arret
    FROM recep_dos x
    JOIN USER_DWH.RECEPTION_DOS_PIECE p ON p.RD_ID = x.RD_ID
),
base AS (
    SELECT ij.date_liq,
           ij.debut_ij,
           ij.duree_arret,
           ij.debut_ij + ij.duree_arret                           AS fin_arret,
           ij.date_traitement,
           TRUNC(d.DOS_DATE_REPRISE)                              AS date_reprise,
           (SELECT MAX(pc.date_piece)
            FROM pieces pc
            WHERE pc.DOS_CODE    = ij.DOS_CODE
              AND pc.date_piece <= ij.date_liq)                   AS derniere_piece,
           (SELECT MAX(pc.date_piece)
            FROM pieces pc
            WHERE pc.DOS_CODE    = ij.DOS_CODE
              AND pc.fin_arret   = 1
              AND pc.date_piece <= ij.date_liq)                   AS piece_fin_arret
    FROM ij
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = ij.DOS_CODE
    WHERE ij.date_liq >= DATE '2025-01-01'
      AND ij.date_liq <  DATE '2026-01-01'
)
SELECT COUNT(*)                                                   AS ij_liquidees_2025,
       COUNT(date_reprise)                                        AS avec_date_reprise,
       ROUND(MEDIAN(date_liq - date_reprise))                     AS med_depuis_reprise_j,
       SUM(CASE WHEN date_liq - date_reprise > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_reprise,
       SUM(CASE WHEN date_reprise > date_liq
                THEN 1 ELSE 0 END)                                AS reprise_apres_liq,
       COUNT(debut_ij)                                            AS avec_debut_ij,
       ROUND(MEDIAN(date_liq - debut_ij))                         AS med_depuis_debut_ij_j,
       COUNT(duree_arret)                                         AS avec_duree_arret,
       ROUND(MEDIAN(duree_arret))                                 AS med_duree_arret,
       ROUND(MEDIAN(date_liq - fin_arret))                        AS med_depuis_fin_arret_j,
       COUNT(date_traitement)                                     AS avec_date_traitement,
       ROUND(MEDIAN(date_liq - date_traitement))                  AS med_depuis_traitement_j,
       COUNT(derniere_piece)                                      AS avec_piece_avant_liq,
       ROUND(MEDIAN(date_liq - derniere_piece))                   AS med_depuis_derniere_piece_j,
       SUM(CASE WHEN date_liq - derniere_piece > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_derniere_piece,
       COUNT(piece_fin_arret)                                     AS avec_piece_fin_arret,
       ROUND(MEDIAN(date_liq - piece_fin_arret))                  AS med_depuis_piece_fin_j,
       SUM(CASE WHEN date_liq - piece_fin_arret > 45
                THEN 1 ELSE 0 END)                                AS plus_45j_piece_fin
FROM base;


-- -----------------------------------------------------------------------------
-- P70 — Réceptions ANCIEN AT/MP « autres pièces » : quelles pièces ?
-- Question : T26 Q1. Que demandent les 724 réceptions ANCIEN de P60 qui
--            n'ont ni déclaration, ni arrêt, ni rechute, ni consolidation, ni
--            décès, ni guérison ?
-- Objectif : les 25 pièces les plus fréquentes de ces réceptions (2019 à
--            2025), avec leur libellé et le nombre de réceptions qui les
--            portent.
-- Lecture  : des ordonnances ou des factures = des demandes de remboursement
--            de soins, donc des dépôts pour la ligne « autres prestations »
--            (Q4). Des pièces d'état civil = une mise à jour du dossier, pas
--            une demande.
-- Tableaux : T26.
-- Résultat : Pièces des 724 réceptions ANCIEN « autres pièces » (réceptions) :
--            CME certificat médical 339 ; CV certificat de vie 185 ; AN acte de
--            naissance 107 ; CMI certificat médical d'incapacité 24 ; CNR
--            certificat de non-remariage 22 ; CM certificat de mariage 17 ; CS
--            certificat de scolarité 17 ; RIB 14 ; BS bulletin de salaire 12 ;
--            CMINAP certificat d'inaptitude 8 ; CTR certificat de travail 6 ;
--            12 autres pièces, de 1 à 5 réceptions chacune. Aucune facture ni
--            ordonnance.
-- Constat  : Ces réceptions portent deux sortes de pièces : un certificat
--            médical (CME, CMI), qui accompagne une demande de rente ou
--            d'allocation, et des pièces de maintien des droits des rentiers et
--            des survivants (certificat de vie, de scolarité, de non-remariage,
--            état civil). Les soins ne passent pas par le guichet : aucune
--            réception ne porte de facture ni d'ordonnance.
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


-- -----------------------------------------------------------------------------
-- P71 — Ce qui suit une réception ANCIEN AT/MP
-- Question : T26 Q1.
-- Objectif : pour chaque réception ANCIEN AT/MP de 2019 à 2025, classée comme
--            dans P60, ce qui arrive sur le dossier visé dans l'année qui
--            suit : rente ou allocation notifiée, rente de survivant notifiée,
--            IJ notifiée, soins approuvés, rien. Une réception est rangée à la
--            première suite dans cet ordre.
-- Lecture  : comme P58 pour T25. Si la consolidation est suivie d'une rente
--            ou d'une allocation, le décès d'une rente de survivant, et les
--            « autres pièces » de soins, chaque réception ANCIEN est le dépôt
--            d'une demande de sa ligne (règle D3 de T25).
-- Tableaux : T26.
-- Résultat : Réceptions ANCIEN de 2019 à 2025 par pièce (réceptions / suivies
--            dans l'année d'une rente ou d'une allocation / d'une rente de
--            survivant / d'une IJ / de soins / de rien) : décès 53 / 0 / 43 /
--            0 / 0 / 10 ; consolidation 233 / 212 / 0 / 6 / 4 / 11 ;
--            déclaration, arrêt, rechute, reprise 36 / 22 / 2 / 3 / 3 / 6 ;
--            sans pièce 51 / 43 / 0 / 1 / 0 / 7 ; autres pièces 724 / 487 /
--            49 / 7 / 14 / 167. Total 1 097 : rente ou allocation 764 (70 %),
--            rente de survivant 94 (9 %), IJ 17, soins 21, rien 201 (18 %). 136
--            réceptions en 2025.
-- Constat  : Une réception ANCIEN AT/MP est le dépôt d'une demande de rente ou
--            d'allocation (après une consolidation, 91 % sont suivies d'une
--            rente ou d'une allocation) ou de rente de survivant (après un
--            décès, 81 %), comme la réversion en T25 (règle D3) : la règle D9
--            est confirmée. Les 201 sans suite, une trentaine par an, mêlent
--            des demandes en cours et des pièces de maintien des droits
--            (certificat de vie) : elles pèsent peu sur le stock.
-- -----------------------------------------------------------------------------
WITH anc0 AS (
    SELECT r.RD_ID,
           r.DOS_CODE,
           TRUNC(r.RD_DATE_RECEPTION)                             AS date_reception
    FROM USER_DWH.RECEPTION_DOSSIER r
    WHERE r.TDOS_CODE = 'A'
      AND r.RD_TYPE_RECEPTION = 'ANCIEN'
      AND r.DOS_CODE IS NOT NULL
      AND r.RD_DATE_RECEPTION >= DATE '2019-01-01'
      AND r.RD_DATE_RECEPTION <  DATE '2026-01-01'
),
pieces AS (
    SELECT p.RD_ID,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CD', 'CGM', 'CHER')
                    THEN 1 ELSE 0 END)                            AS deces,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) = 'CMCON'
                    THEN 1 ELSE 0 END)                            AS consolidation,
           MAX(CASE WHEN TRIM(p.TPJ_CODE) IN ('CMRECHUTE', 'DECLAT', 'DECLMP',
                                              'REAT', 'NERDAT', 'CMC',
                                              'CAT', 'CMAT', 'CMP', 'CMG', 'CRT')
                    THEN 1 ELSE 0 END)                            AS autre_classe
    FROM USER_DWH.RECEPTION_DOS_PIECE p
    JOIN anc0 a ON a.RD_ID = p.RD_ID
    GROUP BY p.RD_ID
),
anc AS (
    SELECT a.RD_ID,
           a.DOS_CODE,
           a.date_reception,
           CASE WHEN p.RD_ID IS NULL          THEN '4 sans pièce'
                WHEN p.deces         = 1      THEN '1 décès'
                WHEN p.consolidation = 1      THEN '2 consolidation'
                WHEN p.autre_classe  = 1      THEN '3 déclaration, arrêt, rechute, reprise'
                ELSE                               '5 autres pièces'
           END                                                    AS motif
    FROM anc0 a
    LEFT JOIN pieces p ON p.RD_ID = a.RD_ID
),
esp AS (
    SELECT pe.DOS_CODE,
           CASE WHEN pe.TPE_CODE IN ('ARI', 'AAI')                   THEN 'R'
                WHEN pe.TPE_CODE IN ('ARSV', 'ARSF', 'ARSE', 'ARSA') THEN 'S'
                WHEN pe.TPE_CODE = 'AIJ'                            THEN 'I'
                ELSE                                                     'X'
           END                                                    AS groupe,
           TRUNC(pe.PE_DATE_NOTIFICATION)                         AS date_notif
    FROM USER_DWH.PRESTATION_ESP pe
    WHERE pe.DOS_CODE IN (SELECT DOS_CODE FROM anc0)
      AND pe.PE_DATE_NOTIFICATION IS NOT NULL
),
soins AS (
    SELECT deb.DOS_CODE,
           TRUNC(deb.DEB_DATE_APPROBATION)                        AS date_approbation
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DOS_CODE IN (SELECT DOS_CODE FROM anc0)
      AND deb.TPN_CODE IS NOT NULL
      AND deb.DEB_DATE_APPROBATION IS NOT NULL
),
suite_esp AS (
    SELECT a.RD_ID,
           MAX(CASE WHEN e.groupe = 'R' THEN 1 ELSE 0 END)        AS rente,
           MAX(CASE WHEN e.groupe = 'S' THEN 1 ELSE 0 END)        AS survivant,
           MAX(CASE WHEN e.groupe = 'I' THEN 1 ELSE 0 END)        AS ij
    FROM anc a
    JOIN esp e ON e.DOS_CODE   = a.DOS_CODE
              AND e.date_notif >= a.date_reception
              AND e.date_notif <= a.date_reception + 365
    GROUP BY a.RD_ID
),
suite_soins AS (
    SELECT DISTINCT a.RD_ID
    FROM anc a
    JOIN soins s ON s.DOS_CODE          = a.DOS_CODE
                AND s.date_approbation >= a.date_reception
                AND s.date_approbation <= a.date_reception + 365
),
suite AS (
    SELECT a.motif,
           a.date_reception,
           CASE WHEN se.rente     = 1          THEN 'rente'
                WHEN se.survivant = 1          THEN 'survivant'
                WHEN se.ij        = 1          THEN 'ij'
                WHEN ss.RD_ID IS NOT NULL      THEN 'soins'
                ELSE                                'rien'
           END                                                    AS premiere_suite
    FROM anc a
    LEFT JOIN suite_esp   se ON se.RD_ID = a.RD_ID
    LEFT JOIN suite_soins ss ON ss.RD_ID = a.RD_ID
)
SELECT motif,
       COUNT(*)                                                   AS receptions_ancien,
       SUM(CASE WHEN premiere_suite = 'rente'     THEN 1 ELSE 0 END) AS rente_ou_allocation,
       SUM(CASE WHEN premiere_suite = 'survivant' THEN 1 ELSE 0 END) AS rente_de_survivant,
       SUM(CASE WHEN premiere_suite = 'ij'        THEN 1 ELSE 0 END) AS ij,
       SUM(CASE WHEN premiere_suite = 'soins'     THEN 1 ELSE 0 END) AS soins,
       SUM(CASE WHEN premiere_suite = 'rien'      THEN 1 ELSE 0 END) AS rien_dans_l_annee,
       SUM(CASE WHEN date_reception >= DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS receptions_2025
FROM suite
GROUP BY motif
ORDER BY motif;


-- -----------------------------------------------------------------------------
-- P72 — Allocation d'incapacité (AAI) : où est sa date de liquidation ?
-- Question : T26 Q2. 537 AAI sur 1 233 ont une date de notification, mais 999
--            ont une notification imprimée (P61).
-- Objectif : par année de saisie de l'AAI (2019 à 2025) : AAI saisies, avec
--            date de notification, avec notification imprimée, avec une date
--            de signature de la notification imprimée, avec un paiement (D ou
--            P), sans date de notification mais avec une signature ; délai
--            médian signature → date de notification quand les deux existent ;
--            taux d'incapacité médian.
-- Lecture  : si les AAI sans date de notification ont une notification
--            imprimée signée, la date de signature peut servir de date de
--            liquidation quand la date de notification manque (Proposition
--            BI), à condition qu'elle tombe le même jour ou presque quand les
--            deux existent.
-- Tableaux : T26.
-- Résultat : AAI par année de saisie (saisies / avec date de notification /
--            avec notification imprimée / avec signature / avec paiement /
--            délai médian signature → notification / taux d'incapacité
--            médian) : 2019 58 / 58 / 58 / 58 / 58 / 0 j / 7 ; 2020 68 / 68 /
--            68 / 68 / 68 / 0 j / 7 ; 2021 76 / 76 / 76 / 76 / 75 / 0 j / 5 ;
--            2022 43 / 43 / 43 / 43 / 43 / 0 j / 10 ; 2023 49 / 49 / 49 / 49 /
--            49 / 0 j / 7 ; 2024 55 / 54 / 54 / 54 / 53 / 0 j / 6 ; 2025 67 /
--            64 / 65 / 64 / 59 / 0 j / 5. Aucune AAI avec une signature mais
--            sans date de notification.
-- Constat  : Depuis 2019, toute allocation d'incapacité a sa date de
--            notification, signée le même jour : les AAI sans date de P61 sont
--            d'avant 2019. La règle D4 (première notification) vaut pour elles,
--            sans date de remplacement. C'est l'allocation des petits taux
--            d'incapacité (5 à 10 en médiane), 43 à 76 par an.
-- -----------------------------------------------------------------------------
WITH aai AS (
    SELECT pe.PE_ID,
           pe.DOS_CODE,
           EXTRACT(YEAR FROM pe.PE_DATE_INSERT)                   AS annee,
           TRUNC(pe.PE_DATE_NOTIFICATION)                         AS date_notif,
           pe.PE_TAUX_INCAPACITE
    FROM USER_DWH.PRESTATION_ESP pe
    WHERE pe.TPE_CODE = 'AAI'
),
notif AS (
    SELECT n.DOS_CODE,
           TRUNC(MIN(n.NOT_DATE_SIGNATURE))                       AS date_signature
    FROM USER_DWH.NOTIFICATION_PREST n
    WHERE n.TPE_CODE = 'AAI'
    GROUP BY n.DOS_CODE
),
notif_existe AS (
    SELECT DISTINCT n.DOS_CODE
    FROM USER_DWH.NOTIFICATION_PREST n
    WHERE n.TPE_CODE = 'AAI'
),
paye AS (
    SELECT DISTINCT deb.PE_ID
    FROM USER_DWH.DEBOURS deb
    JOIN aai ON aai.PE_ID = deb.PE_ID
    WHERE deb.DEB_STATUT IN ('D', 'P')
)
SELECT a.annee,
       COUNT(*)                                                   AS aai_saisies,
       COUNT(a.date_notif)                                        AS avec_date_notification,
       SUM(CASE WHEN ne.DOS_CODE IS NOT NULL THEN 1 ELSE 0 END)   AS avec_notification_imprimee,
       COUNT(n.date_signature)                                    AS avec_date_signature,
       SUM(CASE WHEN a.date_notif IS NULL AND n.date_signature IS NOT NULL
                THEN 1 ELSE 0 END)                                AS signature_sans_date_notif,
       SUM(CASE WHEN p.PE_ID IS NOT NULL THEN 1 ELSE 0 END)       AS avec_paiement,
       ROUND(MEDIAN(a.date_notif - n.date_signature))             AS med_signature_a_notif_j,
       MEDIAN(a.PE_TAUX_INCAPACITE)                               AS taux_incapacite_median
FROM aai a
LEFT JOIN notif        n  ON n.DOS_CODE  = a.DOS_CODE
LEFT JOIN notif_existe ne ON ne.DOS_CODE = a.DOS_CODE
LEFT JOIN paye         p  ON p.PE_ID     = a.PE_ID
WHERE a.annee BETWEEN 2019 AND 2025
GROUP BY a.annee
ORDER BY a.annee;


-- -----------------------------------------------------------------------------
-- P73 — Soins AT/MP : le pic de 2024
-- Question : T26 Q4. 1 477 dossiers ont leur premier remboursement de soins
--            en 2024, dont 973 sans réception et des accidents vieux de 17 ans
--            en médiane (P63). Soins réels ou reprise de données ?
-- Objectif : pour les dossiers AT/MP dont le premier remboursement approuvé
--            est de 2024, par période de l'accident et selon qu'ils ont une
--            réception : dossiers, dont avec une rente ou une allocation,
--            remboursements, première et dernière saisie, mois de saisie le
--            plus fréquent, remboursements saisis ce mois-là.
-- Lecture  : des remboursements saisis en masse sur quelques jours pour de
--            vieux dossiers = une reprise de données : les sortir des
--            liquidés de 2024. Des saisies étalées sur l'année, sur des
--            dossiers de rentiers = des soins réels, mais pas une demande
--            nouvelle : à compter, ou non, selon la réponse de la CNSS à Q4.
-- Tableaux : T26.
-- Résultat : Dossiers AT/MP au premier remboursement approuvé en 2024, par
--            période de l'accident et selon la réception (dossiers / avec
--            rente / remboursements / première et dernière saisie / mois le
--            plus chargé) : avant 2000 sans réception 36 / 30 / 303 /
--            16/06/2005 et 15/11/2024 / 2006-11 ; avant 2000 avec réception 2 /
--            1 / 7 / 2024 ; 2000-2009 sans réception 936 / 157 / 6 428 /
--            17/05/2005 et 06/01/2026 / 2006-11 (416 saisies) ; 2010-2019 sans
--            réception 1 / 1 / 139 / 2012 à 2025 ; 2010-2019 avec réception 4 /
--            1 / 36 / 2024 et 2025 ; 2020 et après avec réception 498 / 64 /
--            4 678 / 20/10/2022 et 02/02/2026 / 2024-11 (549 saisies).
-- Constat  : Les 973 vieux dossiers du pic de 2024 ont des remboursements
--            saisis depuis 2005, surtout en novembre 2006, mais leur première
--            date d'approbation est de 2024 : ces dates ont été remplies en
--            bloc en 2024, ce n'est pas une activité de 2024. Les 500 autres
--            dossiers (accidents récents) sont le flux normal. Proposition BI
--            révisée (règle D11) : un dossier de soins est liquidé à la saisie
--            de son premier remboursement, pas à sa première approbation ; dans
--            le flux normal, les deux sont à moins de 10 j l'une de l'autre
--            (P63).
-- -----------------------------------------------------------------------------
WITH dosa AS (
    SELECT d.DOS_CODE,
           d.RD_ID,
           CASE WHEN d.DOS_DATE_ACCIDENT IS NULL               THEN '5 sans date'
                WHEN d.DOS_DATE_ACCIDENT <  DATE '2000-01-01'  THEN '1 avant 2000'
                WHEN d.DOS_DATE_ACCIDENT <  DATE '2010-01-01'  THEN '2 2000-2009'
                WHEN d.DOS_DATE_ACCIDENT <  DATE '2020-01-01'  THEN '3 2010-2019'
                ELSE                                                '4 2020 et après'
           END                                                    AS periode_accident
    FROM USER_DWH.DOSSIER d
    WHERE d.TDOS_CODE = 'A'
),
rb AS (
    SELECT deb.DOS_CODE,
           TRUNC(deb.DEB_DATE_INSERT)                             AS date_saisie,
           TRUNC(deb.DEB_DATE_APPROBATION)                        AS date_approbation
    FROM USER_DWH.DEBOURS deb
    JOIN dosa ON dosa.DOS_CODE = deb.DOS_CODE
    WHERE deb.TPN_CODE IS NOT NULL
),
premier AS (
    SELECT DOS_CODE
    FROM rb
    GROUP BY DOS_CODE
    HAVING EXTRACT(YEAR FROM NVL(MIN(date_approbation), MIN(date_saisie))) = 2024
),
rente AS (
    SELECT DISTINCT DOS_CODE
    FROM USER_DWH.PRESTATION_ESP
    WHERE TPE_CODE IN ('ARI', 'AAI', 'ARSV', 'ARSF', 'ARSE', 'ARSA')
),
detail AS (
    SELECT a.periode_accident,
           CASE WHEN a.RD_ID IS NULL THEN 0 ELSE 1 END            AS avec_reception,
           rb.DOS_CODE,
           CASE WHEN r.DOS_CODE IS NULL THEN 0 ELSE 1 END         AS avec_rente,
           rb.date_saisie,
           TO_CHAR(rb.date_saisie, 'YYYY-MM')                     AS mois_saisie
    FROM rb
    JOIN premier p ON p.DOS_CODE = rb.DOS_CODE
    JOIN dosa    a ON a.DOS_CODE = rb.DOS_CODE
    LEFT JOIN rente r ON r.DOS_CODE = rb.DOS_CODE
),
par_mois AS (
    SELECT periode_accident, avec_reception, mois_saisie,
           COUNT(*)                                               AS nb,
           ROW_NUMBER() OVER (PARTITION BY periode_accident, avec_reception
                              ORDER BY COUNT(*) DESC)             AS rang
    FROM detail
    GROUP BY periode_accident, avec_reception, mois_saisie
)
SELECT d.periode_accident,
       d.avec_reception,
       COUNT(DISTINCT d.DOS_CODE)                                 AS dossiers,
       COUNT(DISTINCT CASE WHEN d.avec_rente = 1
                           THEN d.DOS_CODE END)                   AS dont_avec_rente,
       COUNT(*)                                                   AS remboursements,
       MIN(d.date_saisie)                                         AS premiere_saisie,
       MAX(d.date_saisie)                                         AS derniere_saisie,
       MAX(m.mois_saisie)                                         AS mois_le_plus_charge,
       MAX(m.nb)                                                  AS saisies_ce_mois
FROM detail d
LEFT JOIN par_mois m ON m.periode_accident = d.periode_accident
                    AND m.avec_reception   = d.avec_reception
                    AND m.rang             = 1
GROUP BY d.periode_accident, d.avec_reception
ORDER BY d.periode_accident, d.avec_reception;
