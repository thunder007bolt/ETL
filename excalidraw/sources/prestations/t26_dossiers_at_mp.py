"""
Fiche de validation de T26 « Répartition du nombre de dossiers de prestations AT/MP de l'exercice
selon la durée de liquidation » (état au 30/10/2026).

Même gabarit que T25 : la maquette, six dossiers fictifs sur un calendrier, les lignes, les dix
règles avec leur SQL et quatre questions. Ce qui change par rapport à T25 : un dossier AT/MP
porte plusieurs demandes (IJ et soins après la déclaration, rente après la consolidation, rentes
de survivants après un décès), les soins n'ont pas de notification, et une déclaration sur deux
ne donne jamais rien.

Sources : Annuaire CIPRES (p. 12, 34, 64) ; atelier de Kaya 2017 (tableau 2, délais de
liquidation des rentes et des soins) ; analyses/prestations (P09, P10 et le lot 4, P59 à P73) ;
t26_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/prestations/t26_dossiers_at_mp.py [--force]
"""

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import C, Diagram, main  # noqa: E402
from fiche_cipres import (WIDTH, banner, cards_grid, cards_with_sql, chip, height_of,  # noqa: E402
                          line_cards, mockup, timeline, wrap)

OUTPUT = ROOT / "schemas" / "prestations" / "tableaux cipres" / "T26_dossiers_at_mp.excalidraw"

HOW_TO = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix dans une "
          "case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la carte si "
          "besoin. Enregistre avec Cmd+S : chaque version est gardée dans l'historique.\n"
          "Les parties 1 à 3 montrent le tableau et des exemples, pour comprendre les règles avant "
          "d'y répondre. Les règles suivent celles de T25 ; ce qui change pour les accidents du "
          "travail est dit sur chaque carte.")

EXAMPLES = [
    dict(nom="Dossier A", detail="des IJ, le cas courant", debut=date(2025, 1, 6),
         fin=date(2025, 7, 15), sortie="plus45",
         dates="déclaration le 06/01/2025, un mois d'arrêt, IJ notifiée le 15/07/2025 : 190 jours",
         resultat="Dans T26 de 2025 : déposé et liquidé en ligne « Indemnités journalières », "
                  "plus de 45 jours."),
    dict(nom="Dossier B", detail="des soins seulement", debut=date(2025, 2, 3),
         fin=date(2025, 5, 12), sortie="plus45",
         dates="déclaration le 03/02/2025, premier remboursement saisi le 12/05/2025 : 98 jours",
         resultat="Dans T26 de 2025 : déposé et liquidé en ligne « Autres prestations de la "
                  "branche », si les soins y entrent (Q2)."),
    dict(nom="Dossier C", detail="la rente, après la consolidation", debut=date(2025, 4, 10),
         fin=date(2025, 4, 24), sortie="moins15",
         dates="accident en 2021 ; demande de rente le 10/04/2025, notifiée le 24/04/2025 : 14 jours",
         resultat="Dans T26 de 2025 : une demande de plus, déposée et liquidée en ligne « Rentes "
                  "viagères », moins de 15 jours, même si l'accident date de 2021."),
    dict(nom="Dossier D", detail="la rente de la veuve", debut=date(2025, 6, 3),
         fin=date(2025, 7, 8), sortie="15a45",
         dates="rentier décédé ; la veuve dépose le 03/06/2025, notifiée le 08/07/2025 : 35 jours",
         resultat="Dans T26 de 2025 : déposée et liquidée en ligne « Rentes de survivants », 15 "
                  "à 45 jours."),
    dict(nom="Dossier E", detail="à cheval sur deux années", debut=date(2025, 10, 20),
         fin=date(2026, 2, 5), sortie="plus45",
         dates="déclaration le 20/10/2025, IJ notifiée le 05/02/2026 : 108 jours",
         resultat="Dans T26 de 2025 : déposé (ligne IJ), en instance au 31/12. Dans T26 de 2026 : "
                  "liquidé, plus de 45 jours."),
    dict(nom="Dossier F", detail="une déclaration sans suite", debut=date(2025, 9, 15),
         fin=date(2026, 2, 28), sortie="instance",
         dates="déclaration le 15/09/2025 ; ni arrêt, ni soins, ni rente",
         resultat="Dans T26 de 2025 : déposé, sans ligne (Total seulement), en instance au 31/12. "
                  "Sorti du stock en septembre 2027 s'il n'a toujours rien donné (Q1)."),
]

LINES = [
    (1, "Rentes viagères", "la rente viagère d'incapacité, après la consolidation", None, None),
    (2, "Rentes de survivants", "les rentes de veuve, de veuf, d'orphelin et d'ascendant",
     "veuve et orphelins d'un même décès comptent une fois, comme en T25 ; l'atelier de Kaya "
     "regroupe les rentes de survivants (T29)", "Kaya 2017"),
    (3, "Indemnités journalières", "les IJ d'un arrêt de travail",
     "un arrêt prolongé donne plusieurs IJ (1,9 par dossier) : la demande compte une fois, à la "
     "première", "Proposition BI"),
    (4, "Allocation AT/MP", "l'allocation d'incapacité, pour les petits taux d'incapacité",
     "43 à 76 par an, notifiées depuis 2019", "Données"),
    (5, "Autres prestations de la branche",
     "les remboursements de soins : consultations, pharmacie, examens, radios",
     "si la CNSS les garde dans T26 : voir la question Q2", "Proposition BI"),
]

RULES = [
    dict(n="1", titre="Les dossiers concernés",
         regle="Tous les dossiers d'accident du travail et de maladie professionnelle : la "
               "déclaration ouvre le dossier de la victime, qui garde ensuite toutes ses "
               "prestations.",
         sources=[("Base", "type de dossier A, branche des risques professionnels"),
                  ("Annuaire", "p. 12 : T26 répartit les dossiers AT/MP par type de "
                               "prestation : rentes, indemnités"),
                  ("Données", "9 106 dossiers ouverts de 2019 à 2025, 1 237 en 2025")]),
    dict(n="2", titre="Déposés",
         regle="Les demandes reçues au guichet du 1er janvier au 31 décembre. La déclaration "
               "d'accident ou de maladie est le dépôt des IJ et des soins. Une rente, une "
               "allocation ou une rente de survivant est déposée à part, par sa demande sur le "
               "dossier existant.",
         sources=[("Annuaire", "p. 12 : les dossiers « déposés » ; p. 64 : la durée part de la "
                               "date de dépôt"),
                  ("Données", "en 2025 : 1 345 déclarations et 136 demandes sur un dossier "
                              "existant ; ces demandes sont suivies à 70 % d'une rente ou d'une "
                              "allocation, à 9 % d'une rente de survivant"),
                  ("Proposition BI", "une rente sans demande au guichet prend la date du "
                                     "certificat pour la rente, sinon celle de la déclaration ; "
                                     "une déclaration qui donne des IJ et des soins compte sur "
                                     "les deux lignes")],
         chiffre="En 2025 : 1 481 réceptions AT/MP."),
    dict(n="3", titre="Liquidés",
         regle="Les demandes dont les droits ont été notifiés du 1er janvier au 31 décembre, "
               "quelle que soit l'année du dépôt : la première notification de chaque demande. "
               "Les soins, qui n'ont pas de notification, sont liquidés au premier remboursement "
               "saisi.",
         sources=[("Annuaire", "p. 64 : la liquidation va jusqu'à la « notification des droits au "
                               "bénéficiaire »"),
                  ("Données", "97 % des IJ ont une date de notification ; une IJ prolongée en "
                              "donne plusieurs ; l'allocation d'incapacité est notifiée depuis "
                              "2019 ; les dates d'approbation des vieux remboursements ont été "
                              "remplies en bloc en 2024 : la saisie est plus sûre"),
                  ("Proposition BI", "les soins au premier remboursement saisi ; voir la question "
                                     "Q2")],
         chiffre="En 2025 : 343 demandes liquidées hors soins (IJ 226, rentes 43, survivants 13, "
                 "allocations 61), et environ 650 dossiers de soins."),
    dict(n="4", titre="La durée de liquidation",
         regle="Le nombre de jours entre le dépôt de la demande et sa liquidation.",
         sources=[("Annuaire", "p. 64 : du dépôt à la notification des droits"),
                  ("Kaya 2017", "rentes : du certificat de consolidation à la notification ; "
                                "soins : du certificat du médecin traitant à la convocation, une "
                                "date que la base n'a pas"),
                  ("Données", "rentes : 13 à 35 jours depuis leur demande au guichet, 64 à 107 "
                              "jours depuis le certificat ; IJ : 251 jours depuis la "
                              "déclaration, pour un arrêt d'un mois"),
                  ("Proposition BI", "partir du dépôt au guichet, comme en T25 ; voir les "
                                     "questions Q3 et Q4")],
         chiffre="En 2025 (médiane) : rentes 14 jours, allocations 13, survivants 35, IJ 251, "
                 "soins environ 155."),
    dict(n="5", titre="Les trois colonnes de durée",
         regle="Moins de 15 jours (0 à 14), de 15 à 45 jours, plus de 45 jours (46 et plus). "
               "Ensemble, les trois colonnes font les liquidés.",
         sources=[("Annuaire", "p. 12 et p. 34 : « moins de 15 jours, 15 à 45 jours, plus de 45 "
                               "jours »"),
                  ("Proposition BI", "15 et 45 jours vont dans la colonne du milieu, comme en "
                                     "T25")],
         chiffre="En 2025 : plus de 45 jours pour au moins 85 % des IJ, pour 30 % des rentes, "
                 "allocations et rentes de survivants."),
    dict(n="6", titre="En instance",
         regle="Les demandes reçues, ni liquidées ni rejetées au 31 décembre. Sans les dossiers "
               "fermés jamais liquidés, ni les demandes toujours sans suite 24 mois après leur "
               "dépôt.",
         sources=[("Annuaire", "p. 12 : « le nombre de dossiers restants à traiter en fin "
                               "d'exercice »"),
                  ("Données", "46 % des déclarations ne donnent jamais rien et ne sont jamais "
                              "fermées : sans limite, plus de 4 000 resteraient en instance pour "
                              "toujours ; des rentes et des allocations d'avant 2019 n'ont pas de "
                              "date de notification"),
                  ("Proposition BI", "la limite de 24 mois ; voir la question Q1")],
         chiffre="Pas encore mesuré avec ces règles."),
    dict(n="7", titre="Rejetés",
         regle="Les dossiers dont le refus a été enregistré du 1er janvier au 31 décembre.",
         sources=[("Annuaire", "p. 34 : la colonne « Dossiers rejetés »"),
                  ("Kaya 2017", "le module des rejets existe mais n'est pas utilisé"),
                  ("Données", "0 à 7 rejets AT/MP par an, 1 en 2025 ; une déclaration sans suite "
                              "n'est ni fermée ni rejetée")]),
    dict(n="8", titre="La durée moyenne",
         regle="La moyenne des durées des demandes liquidées, ligne par ligne, comparée à la "
               "norme de 45 jours. On donne aussi la médiane.",
         sources=[("Annuaire", "p. 12 : la durée moyenne, comparée à la norme de moins de 45 "
                               "jours"),
                  ("Proposition BI", "ajouter la médiane, que quelques dossiers très lents ne "
                                     "faussent pas")]),
    dict(n="9", titre="Le Total",
         regle="La somme des lignes : chaque demande compte une fois. Une déclaration qui donne "
               "des IJ et des soins compte donc deux fois. Les déclarations sans prestation "
               "s'ajoutent aux déposés et aux en instance. La durée moyenne du Total est "
               "recalculée sur toutes les demandes.",
         sources=[("Annuaire", "p. 34 : la ligne TOTAL"),
                  ("Proposition BI", "recalculer la moyenne du Total : une moyenne des moyennes "
                                     "des lignes serait fausse")]),
    dict(n="10", titre="Le taux de dossiers restant à traiter",
         regle="Les demandes en instance au 31 décembre, divisées par celles en instance au 1er "
               "janvier plus celles déposées dans l'année.",
         sources=[("Annuaire", "p. 12 : le nombre de dossiers restants à traiter en fin "
                               "d'exercice"),
                  ("Kaya 2017", "l'indicateur « taux des dossiers restant à traiter en fin de "
                                "période », sans formule"),
                  ("Proposition BI", "la formule, la même qu'en T25")]),
]

# Pour chaque règle, une requête courte sur les vraies tables de l'application (USER_DWH), pour
# l'année 2025, avec en commentaire le chiffre à retrouver. La règle 3 construit la liste
# demandes, la règle 4 leurs durées (durees) ; les règles 5, 8 et 9 les réutilisent.
SQL = {
    "1": """-- tous les dossiers AT/MP (type A) ; la
-- déclaration d'accident ou de maladie est
-- la réception qui a ouvert le dossier
SELECT COUNT(*)       AS dossiers,
       COUNT(RD_ID)   AS avec_declaration
FROM   USER_DWH.DOSSIER
WHERE  TDOS_CODE = 'A'""",
    "2": """-- les demandes reçues au guichet en 2025
SELECT RD_TYPE_RECEPTION, COUNT(*)
FROM   USER_DWH.RECEPTION_DOSSIER
WHERE  TDOS_CODE = 'A'
AND    RD_DATE_RECEPTION >= DATE '2025-01-01'
AND    RD_DATE_RECEPTION <  DATE '2026-01-01'
GROUP  BY RD_TYPE_RECEPTION
-- NOUVEAU : la déclaration, dépôt des IJ
--           et des soins
-- ANCIEN  : la demande d'une rente, d'une
--           allocation, d'une rente de
--           survivant, sur le dossier
-- 2025 : 1 481 = 1 345 NOUVEAU + 136 ANCIEN""",
    "3": """-- demandes : une par dossier et par ligne
-- du tableau, réutilisée ensuite
WITH prest AS (
  SELECT pe.DOS_CODE, pe.PE_DATE_DEMANDE,
         pe.PE_DATE_NOTIFICATION AS date_liq,
         CASE WHEN pe.TPE_CODE = 'ARI' THEN 1
              WHEN pe.TPE_CODE IN ('ARSV','ARSF',
                     'ARSE','ARSA')    THEN 2
              WHEN pe.TPE_CODE = 'AIJ' THEN 3
              WHEN pe.TPE_CODE = 'AAI' THEN 4
              ELSE 5 END AS ligne
  FROM   USER_DWH.PRESTATION_ESP pe
  JOIN   USER_DWH.DOSSIER d
         ON d.DOS_CODE = pe.DOS_CODE
  WHERE  d.TDOS_CODE = 'A'
  -- les soins (ligne 5) : liquidés au
  -- premier remboursement saisi
  UNION ALL
  SELECT deb.DOS_CODE, NULL,
         deb.DEB_DATE_INSERT, 5
  FROM   USER_DWH.DEBOURS deb
  JOIN   USER_DWH.DOSSIER d
         ON d.DOS_CODE = deb.DOS_CODE
  WHERE  d.TDOS_CODE = 'A'
  AND    deb.TPN_CODE IS NOT NULL),
demandes AS (
  -- liquidée à sa première notification
  SELECT DOS_CODE, ligne,
         MIN(PE_DATE_DEMANDE) AS date_demande,
         MIN(date_liq)        AS date_liquidation
  FROM   prest
  GROUP  BY DOS_CODE, ligne)
SELECT ligne, COUNT(*)
FROM   demandes
WHERE  date_liquidation >= DATE '2025-01-01'
AND    date_liquidation <  DATE '2026-01-01'
GROUP  BY ligne
ORDER  BY ligne
-- 2025 : rentes 43, survivants 13, IJ 226,
-- allocations 61 (343 hors soins) ;
-- soins, ligne 5 : environ 650 dossiers""",
    "4": """-- à ajouter à la suite du WITH de la
-- règle 3 : le dépôt et la durée
depots AS (
  SELECT dm.ligne, dm.date_liquidation,
         -- la déclaration
         (SELECT r.RD_DATE_RECEPTION
          FROM   USER_DWH.DOSSIER d
          JOIN   USER_DWH.RECEPTION_DOSSIER r
                 ON r.RD_ID = d.RD_ID
          WHERE  d.DOS_CODE = dm.DOS_CODE)
                             AS date_declaration,
         -- la demande au guichet d'une rente
         (SELECT MAX(r.RD_DATE_RECEPTION)
          FROM   USER_DWH.RECEPTION_DOSSIER r
          WHERE  r.DOS_CODE = dm.DOS_CODE
          AND    r.RD_TYPE_RECEPTION = 'ANCIEN'
          AND    r.RD_DATE_RECEPTION
                   <= dm.date_liquidation)
                             AS date_guichet,
         -- le certificat pour la rente
         (SELECT d.DOS_DATE_REC_CM_RENTE
          FROM   USER_DWH.DOSSIER d
          WHERE  d.DOS_CODE = dm.DOS_CODE)
                             AS date_certificat
  FROM   demandes dm
  WHERE  dm.date_liquidation >= DATE '2025-01-01'
  AND    dm.date_liquidation <  DATE '2026-01-01'),
durees AS (
  -- IJ et soins (3, 5) : la déclaration ;
  -- rentes et allocation : le guichet, sinon
  -- le certificat, sinon la déclaration
  SELECT ligne,
         GREATEST(TRUNC(date_liquidation)
           - TRUNC(CASE WHEN ligne IN (3, 5)
                        THEN date_declaration
                        ELSE COALESCE(date_guichet,
                               date_certificat,
                               date_declaration)
                   END), 0) AS jours
  FROM   depots)
SELECT ligne, MEDIAN(jours)
FROM   durees
GROUP  BY ligne
-- 2025 : rentes 14 j, survivants 35, IJ 251,
-- allocations 13, soins environ 155""",
    "5": """-- à coller sous le WITH des règles 3 et 4
SELECT ligne,
       SUM(CASE WHEN jours < 15
                THEN 1 ELSE 0 END) AS moins_15,
       SUM(CASE WHEN jours BETWEEN 15 AND 45
                THEN 1 ELSE 0 END) AS de_15_a_45,
       SUM(CASE WHEN jours > 45
                THEN 1 ELSE 0 END) AS plus_45
FROM   durees
GROUP  BY ligne
-- 2025 : plus de 45 jours pour au moins
-- 85 % des IJ, 30 % des rentes""",
    "6": """-- demandes AT/MP ni liquidées ni rejetées
-- au 31/12/2025
SELECT COUNT(*)
FROM   USER_DWH.RECEPTION_DOSSIER r
LEFT   JOIN USER_DWH.DOSSIER d
       ON (d.RD_ID = r.RD_ID          -- NOUVEAU
           OR d.DOS_CODE = r.DOS_CODE) -- ANCIEN
WHERE  r.TDOS_CODE = 'A'
AND    r.RD_DATE_RECEPTION < DATE '2026-01-01'
-- sans suite après 24 mois : sorti du stock
AND    r.RD_DATE_RECEPTION >= DATE '2024-01-01'
AND    NOT EXISTS (     -- aucune notification
         SELECT 1
         FROM   USER_DWH.PRESTATION_ESP p
         WHERE  p.DOS_CODE = d.DOS_CODE
         AND    p.PE_DATE_NOTIFICATION
                  >= TRUNC(r.RD_DATE_RECEPTION)
         AND    p.PE_DATE_NOTIFICATION
                  < DATE '2026-01-01')
AND    NOT EXISTS (     -- aucun soin remboursé
         SELECT 1
         FROM   USER_DWH.DEBOURS b
         WHERE  b.DOS_CODE = d.DOS_CODE
         AND    b.TPN_CODE IS NOT NULL
         AND    b.DEB_DATE_INSERT
                  < DATE '2026-01-01')
AND    NOT EXISTS (     -- aucun rejet
         SELECT 1
         FROM   USER_DWH.DOSSIER_REJETE x
         WHERE  x.RD_ID = r.RD_ID
         AND    x.DOSRE_DATE_REJET
                  < DATE '2026-01-01')
-- sans les dossiers fermés jamais liquidés
AND    NVL(d.DOS_STATUT, '-') <> 'F'""",
    "7": """SELECT COUNT(*)
FROM   USER_DWH.DOSSIER_REJETE
WHERE  TDOS_CODE = 'A'
AND    DOSRE_DATE_REJET >= DATE '2025-01-01'
AND    DOSRE_DATE_REJET <  DATE '2026-01-01'
-- 2025 : 1 (0 à 7 par an)""",
    "8": """-- à coller sous le WITH des règles 3 et 4
SELECT ligne,
       ROUND(AVG(jours)) AS duree_moyenne,
       MEDIAN(jours)     AS duree_mediane
FROM   durees
GROUP  BY ligne
-- la norme : moins de 45 jours""",
    "9": """-- le Total : toutes les demandes
-- ensemble, la moyenne recalculée ; une
-- déclaration qui donne des IJ et des soins
-- compte deux fois
SELECT COUNT(*)          AS liquides,
       ROUND(AVG(jours)) AS duree_moyenne,
       MEDIAN(jours)     AS duree_mediane
FROM   durees
-- les déclarations sans prestation
-- s'ajoutent aux déposés et aux en instance""",
    "10": """-- en instance au 31/12/2025, divisé par
-- (en instance au 31/12/2024 + déposés)
-- en instance : la règle 6, une fois avec
-- 2026-01-01, une fois avec 2025-01-01 ;
-- déposés : la règle 2 (1 481 en 2025)
SELECT ROUND(100 * :instance_fin_2025
         / (:instance_fin_2024 + 1481), 1)
         AS taux_restant_pct
FROM   DUAL""",
}

SQL_NOTE = ("À droite de chaque règle, une requête courte sur les vraies tables de l'application "
            "(schéma USER_DWH), pour l'année 2025, à copier dans SQL Developer. La règle 3 "
            "construit la liste des demandes (une par dossier et par ligne du tableau, avec sa "
            "première notification ; les soins au premier remboursement) ; la règle 4 y ajoute "
            "leur dépôt et leur durée (durees). Pour les règles 5, 8 et 9, on colle leur requête "
            "sous ce WITH, à la place de sa dernière requête. Le commentaire en fin de requête "
            "donne le chiffre à retrouver. La requête complète, sur les copies de l'entrepôt, "
            "est dans analyses/prestations/t26_requete_proposee.sql.")

QUESTIONS = [
    dict(n="Q1", titre="Les déclarations sans suite",
         regle="Une déclaration d'accident qui ne donne ni arrêt, ni soins, ni rente est-elle une "
               "demande à liquider ? Elle reste ouverte sans fin.",
         cas=[("Si c'est une demande",
               "elle compte dans les déposés, sans ligne, puis en instance chaque 31 décembre "
               "jusqu'à 24 mois sans suite (règle 6)."),
              ("Si ce n'est qu'une déclaration",
               "elle ne compte dans T26 que le jour où elle donne une IJ, des soins ou une "
               "rente : environ 550 dépôts de moins par an.")],
         sources=[("Données", "4 158 dossiers sur 9 106 ouverts de 2019 à 2025 n'ont rien donné "
                              "(35 à 53 % chaque année) ; presque aucun n'est fermé ni rejeté"),
                  ("Proposition BI", "la compter, avec la limite de 24 mois")]),
    dict(n="Q2", titre="Les soins",
         regle="Les remboursements de soins entrent-ils dans T26, en « autres prestations de la "
               "branche » ?",
         cas=[("Si oui",
               "chaque dossier soigné compte une demande, liquidée au premier remboursement "
               "saisi : environ 650 par an ; une déclaration qui donne des IJ et des soins compte "
               "deux demandes."),
              ("Si non",
               "T26 ne compte que les prestations en espèces (343 liquidées en 2025) ; les "
               "dossiers de soins seuls, 31 % des dossiers, n'y apparaissent pas.")],
         sources=[("Données", "8 à 10 remboursements par dossier ; aucune facture ne passe par "
                              "le guichet"),
                  ("Proposition BI", "les compter : c'est la prestation la plus fréquente de la "
                                     "branche")]),
    dict(n="Q3", titre="Le départ de la durée d'une rente",
         regle="La durée d'une rente part-elle de sa demande au guichet, ou de la réception du "
               "certificat de consolidation ?",
         cas=[("Depuis la demande au guichet",
               "14 jours en médiane pour une rente, 13 pour une allocation ; la date existe pour "
               "toutes les demandes."),
              ("Depuis le certificat",
               "64 jours pour une rente, 107 pour une allocation, comme le dit l'atelier de "
               "Kaya ; la date manque pour un tiers des rentes.")],
         sources=[("Kaya 2017", "délai de liquidation des rentes : du certificat de "
                                "consolidation à la notification"),
                  ("Proposition BI", "la demande au guichet, et le certificat quand la demande "
                                     "manque")]),
    dict(n="Q4", titre="L'attente des IJ",
         regle="Une IJ est notifiée environ 7 mois après la fin de l'arrêt de travail. Qu'attend-"
               "elle : un certificat de guérison, l'attestation de salaire de l'employeur, autre "
               "chose qui n'est pas saisi ?",
         sources=[("Données", "arrêt de 27 jours en médiane, IJ notifiée 211 jours après sa "
                              "fin ; la reprise du travail n'est jamais saisie, le certificat de "
                              "guérison l'est pour 14 IJ sur 226"),
                  ("Proposition BI", "compter depuis la déclaration tant qu'aucune date ne "
                                     "marque le dossier complet ; si la pièce attendue est "
                                     "saisie, partir d'elle")]),
]

BI_CHECKS = ("Une rente arrive des années après les IJ, dans le même dossier, avec sa propre "
             "notification : c'est une demande à part.\n"
             "Les demandes sur un dossier existant sont des demandes de rente ou d'allocation (70 %) "
             "ou de rente de survivant (9 %) ; les autres apportent des certificats de vie.\n"
             "L'allocation d'incapacité a sa date de notification depuis 2019.\n"
             "Le pic de soins de 2024 vient de dates d'approbation remplies en bloc : les soins "
             "sont datés par la saisie.")


def figures(d, x, y, w):
    """Encadré des premiers chiffres de 2025, avec la part des demandes de plus de 45 jours."""
    g = d.group()
    h = 330
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + 20, y + 16, "En 2025 : premiers chiffres, provisoires", 22, groups=g, inside=frame)
    d.text(x + 20, y + 54, "1 345 déclarations et 136 demandes sur un dossier existant ·\n"
                           "343 demandes liquidées (IJ 226, rentes 43, survivants 13,\n"
                           "allocations 61), plus environ 650 dossiers de soins · 1 rejet.",
           18, groups=g, inside=frame)
    by = y + 140
    for label, short in [("IJ", 15), ("rentes, survivants,\nallocations", 70)]:
        d.text(x + 20, by + 2, label, 16, groups=g, inside=frame)
        bx, scale = x + 220, (w - 240) / 100
        for share, fill, stroke in [(short, C["vert_moyen"], C["vert"]),
                                    (100 - short, C["rouge_moyen"], C["rouge"])]:
            d.box(bx, by, share * scale, 36, f"{share} %", 16, bg=fill, stroke=stroke, groups=g)
            bx += share * scale
        by += 52
    d.text(x + 20, by + 4, "45 jours ou moins · plus de 45 jours (IJ : depuis la dernière "
                           "réception)", 16, C["doux"], groups=g, inside=frame)
    w_chip = chip(d, x + 20, by + 40, "Données", groups=g)
    d.text(x + 20 + w_chip + 10, by + 45, "lot 4 (P59 à P73) ; à recalculer selon les règles "
                                         "validées", 16, groups=g, inside=frame)
    return y + h


def build():
    d = Diagram(seed=26)
    inner = WIDTH - 50

    d.text(0, 0, "T26 · Les dossiers d'accident du travail : combien, et en combien de temps ?", 48)
    d.text(0, 66, "« Répartition du nombre de dossiers de prestations AT/MP de l'exercice selon la "
                  "durée de liquidation » · ce que l'équipe BI a compris, à valider · état au "
                  "30/10/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    about = d.box(25, top, 760, 290, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T26", 22, groups=g, inside=about)
    text = wrap("Il dit combien de demandes la CNSS a reçues dans l'année pour des accidents du "
                "travail et des maladies professionnelles, combien elle en a traitées et en combien "
                "de temps, combien attendent encore au 31 décembre, et combien ont été refusées.",
                18, 720)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 12, maquette p. 34 ; une fois par an"),
                      ("Annuaire", "la norme CIPRES : une durée moyenne de moins de 45 jours")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(d, 25, top + 310, 760)

    bottom = mockup(
        d, 830, top, 445, (WIDTH - 25 - 830 - 445) / 7, "Type de prestation", "1",
        columns=[("Nombre de dossiers déposés", "2"), ("moins de 15 jours", "5"),
                 ("15 à 45 jours", "5"), ("plus de 45 jours", "5"),
                 ("Nombre de dossiers liquidés", "3"), ("Dossiers en instance", "6"),
                 ("Dossiers rejetés", "7")],
        groups=[(1, 3, "Durée de liquidation", "4 et 5")],
        rows=[("Rentes viagères", None, None), ("Rentes de survivants", None, None),
              ("Indemnités journalières", None, None), ("Allocation AT/MP", None, None),
              ("Autres prestations de la branche", None, None), ("TOTAL", None, "9")])
    d.text(830, bottom + 14, wrap("Calculés en plus pour chaque ligne : la durée moyenne de "
                                  "liquidation, comparée à la norme de 45 jours (règle 8), et le "
                                  "taux de dossiers restant à traiter (règle 10).", 18,
                                  WIDTH - 25 - 830), 18, C["doux"])
    sec["height"] = max(bottom + 80, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Comment une demande est comptée : six dossiers fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2024, 11, 1), date(2026, 2, 28), 2025, EXAMPLES)
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes : dans quelle ligne va une demande")
    w = chip(d, 25, y + 72, "Proposition BI")
    d.text(25 + w + 12, y + 76, "Une demande compte une seule fois, dans la ligne de sa "
                                "prestation. Un dossier AT/MP en porte souvent plusieurs : les IJ "
                                "et les soins après la déclaration, la rente après la "
                                "consolidation, les rentes de survivants après un décès.", 18)
    bottom = line_cards(d, 25, y + 120, inner, LINES)
    w = chip(d, 25, bottom + 16, "Proposition BI")
    d.text(25 + w + 12, bottom + 20, "Pas encore de ligne : une déclaration qui n'a encore donné "
                                     "aucune prestation ne compte que dans le Total (déposés, en "
                                     "instance).", 18)
    sec["height"] = bottom + 60 + 30 - sec["y"]

    # ------------------------------------------------------------------ 4. règles
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "4. Les règles, une par une : coche ta réponse")
    note = wrap(SQL_NOTE, 18, inner)
    d.text(25, y + 70, note, 18, C["doux"])
    bottom = cards_with_sql(d, 25, y + 70 + height_of(note, 18) + 20, inner,
                            [dict(rule, sql=SQL[rule["n"]]) for rule in RULES])
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 5. questions
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "5. Les questions pour le métier")
    bottom = cards_grid(d, 25, y + 70, inner, 4, QUESTIONS)
    g = d.group()
    checks = d.box(25, bottom + 20, inner, 150, bg=C["gris"], stroke=C["gris_bord"], groups=g)
    d.text(45, bottom + 36, "Ce que l'équipe BI a vérifié dans les données (30/10/2026) : pas de "
                            "réponse attendue", 20, groups=g, inside=checks)
    d.text(45, bottom + 70, BI_CHECKS, 16, groups=g, inside=checks)
    sec["height"] = bottom + 20 + 150 + 30 - sec["y"]
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
