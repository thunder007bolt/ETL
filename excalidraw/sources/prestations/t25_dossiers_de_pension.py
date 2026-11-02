"""
Fiche de validation de T25 « Répartition du nombre de dossiers de prestations PVID de l'exercice
selon la durée de liquidation » (état au 30/10/2026).

Ce que l'équipe BI a compris du tableau, pour que le métier le valide ou le corrige : la maquette,
six dossiers fictifs sur un calendrier, les lignes, les dix règles et cinq questions.

Sources : Annuaire CIPRES (p. 11, 34, 62, 64) ; atelier de Kaya 2017 (tableau 1 sur T25 et T27,
tableau 2 sur le délai de liquidation) ; analyses/prestations (P02, P03, P09, P10, P29, P33, et
le lot 3 B, P48 à P58) ; doc « Règles de calcul des tableaux des prestations » et
t25_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/prestations/t25_dossiers_de_pension.py [--force]
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

OUTPUT = ROOT / "schemas" / "prestations" / "tableaux cipres" / "T25_dossiers_de_pension.excalidraw"

HOW_TO = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix dans une "
          "case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la carte si "
          "besoin. Enregistre avec Cmd+S : chaque version est gardée dans l'historique.\n"
          "Les parties 1 à 3 montrent le tableau et des exemples, pour comprendre les règles avant "
          "d'y répondre.")

EXAMPLES = [
    dict(nom="Dossier A", detail="traité vite", debut=date(2025, 3, 3), fin=date(2025, 3, 14),
         sortie="moins15", dates="reçu le 03/03/2025, notifié le 14/03/2025 : 11 jours",
         resultat="Dans T25 de 2025 : déposé, liquidé, colonne « moins de 15 jours »."),
    dict(nom="Dossier B", detail="le cas le plus courant", debut=date(2025, 1, 10),
         fin=date(2025, 3, 13), sortie="plus45",
         dates="reçu le 10/01/2025, notifié le 13/03/2025 : 62 jours",
         resultat="Dans T25 de 2025 : déposé, liquidé, « plus de 45 jours ». 62 jours, c'est la "
                  "durée médiane de 2025."),
    dict(nom="Dossier C", detail="à cheval sur deux années", debut=date(2025, 11, 17),
         fin=date(2026, 1, 22), sortie="plus45",
         dates="reçu le 17/11/2025, notifié le 22/01/2026 : 66 jours",
         resultat="Dans T25 de 2025 : déposé, et en instance au 31/12. Dans T25 de 2026 : liquidé, "
                  "« plus de 45 jours »."),
    dict(nom="Dossier D", detail="refusé", debut=date(2025, 6, 2), fin=date(2025, 6, 30),
         sortie="rejet", dates="reçu le 02/06/2025, rejeté le 30/06/2025",
         resultat="Dans T25 de 2025 : déposé et rejeté. Ni liquidé, ni en instance : pas de durée "
                  "de liquidation."),
    dict(nom="Dossier E", detail="déposé l'année d'avant", debut=date(2024, 11, 20),
         fin=date(2025, 2, 5), sortie="plus45",
         dates="reçu le 20/11/2024, notifié le 05/02/2025 : 77 jours",
         resultat="Dans T25 de 2024 : déposé, en instance au 31/12. Dans T25 de 2025 : liquidé "
                  "(mais pas déposé), « plus de 45 jours »."),
    dict(nom="Dossier F", detail="réversion sur le dossier du défunt", debut=date(2025, 3, 10),
         fin=date(2025, 5, 9), sortie="plus45",
         dates="pensionné depuis 2012, décédé ; la veuve dépose le 10/03/2025, notifiée le "
               "09/05/2025 : 60 jours",
         resultat="Dans T25 de 2025 : une demande de plus, déposée et liquidée en ligne 4 "
                  "« Pension de survivant », « plus de 45 jours », même si le dossier date de "
                  "2012."),
]

LINES = [
    (1, "Pension de vieillesse", "la pension de vieillesse", None, None),
    (2, "Pension de retraite anticipée", "la pension de retraite anticipée",
     "ligne ajoutée à l'atelier de Kaya ; elle n'est pas dans l'Annuaire", "Kaya 2017"),
    (3, "Pension d'invalidité", "la pension d'invalidité", None, None),
    (4, "Pension de survivant", "les pensions de veuve, de veuf, d'orphelin et d'ascendant",
     "veuves et orphelins d'un même décès comptent une fois (jusqu'à 13 veuves ou 24 orphelins), "
     "qu'ils aient un nouveau dossier ou qu'ils soient ajoutés au dossier du défunt", "Données"),
    (5, "Allocations de vieillesse", "l'allocation de vieillesse",
     "versée en une seule fois (glossaire, p. 62)", "Annuaire"),
    (6, "Allocation de survivant",
     "l'allocation de survivants : parts du conjoint, des orphelins, des ascendants", None, None),
    (7, "Autres prestations de la branche",
     "le remboursement de cotisations ; un dossier qui n'a qu'un complément (bonification ou "
     "majoration pour enfant, contribution aux charges du ménage)",
     "un complément joint à une pension suit la pension : il ne change pas la ligne",
     "Proposition BI"),
]

RULES = [
    dict(n="1", titre="Les dossiers concernés",
         regle="Les dossiers de la branche des pensions : vieillesse, invalidité, survivants, y "
               "compris ceux de coordination payés par une autre caisse, que la CNSS reçoit et "
               "instruit. Sans les dossiers transférés à une autre caisse.",
         sources=[("Base", "deux catégories de dossier : « coordination payée par une autre "
                           "caisse » (3 326 dossiers) et « transféré à une autre caisse » (153)"),
                  ("Données", "la CNSS reçoit à son guichet 76 % des dossiers de coordination "
                              "payés par une autre caisse et en notifie la moitié : 238 dépôts "
                              "et 292 liquidations en 2025"),
                  ("Kaya 2017", "pour les pensions de coordination, seule la part CNSS compte "
                                "(T32, T42) ; rien sur T25"),
                  ("Proposition BI", "garder la coordination, n'exclure que les dossiers "
                                     "transférés, sans activité en 2025. Voir la question Q1")]),
    dict(n="2", titre="Déposés",
         regle="Les demandes reçues au guichet du 1er janvier au 31 décembre : une nouvelle "
               "demande, qui ouvre un dossier, ou une demande de réversion déposée sur le "
               "dossier du défunt.",
         sources=[("Annuaire", "p. 11 : les dossiers « déposés » ; p. 64 : la durée part de la "
                               "date de dépôt"),
                  ("Kaya 2017", "le dépôt, c'est la réception du dossier complet au guichet"),
                  ("Données", "le guichet distingue les nouvelles demandes (4 055 en 2025) des "
                              "demandes sur un dossier existant (616), à 94 % des réversions"),
                  ("Proposition BI", "une réversion non enregistrée au guichet prend la date "
                                     "de demande de sa pension ; une demande sans pension créée "
                                     "ne compte que dans le Total")],
         chiffre="En 2025 : 4 671 demandes reçues au guichet."),
    dict(n="3", titre="Liquidés",
         regle="Les demandes dont les droits ont été notifiés du 1er janvier au 31 décembre, "
               "quelle que soit l'année du dépôt. Une réversion est une demande à part : elle "
               "compte l'année de sa propre notification.",
         sources=[("Annuaire", "p. 64 : la liquidation va jusqu'à la « notification des droits au "
                               "bénéficiaire »"),
                  ("Kaya 2017", "la notification des droits signifiée à l'assuré"),
                  ("Données", "le statut du dossier ne peut pas servir (61 632 dossiers « actifs ») ; "
                              "une seconde notification arrive en médiane 9 ans après la "
                              "première : c'est une réversion, pas une révision"),
                  ("Proposition BI", "on retient la première notification de chaque demande")],
         chiffre="En 2025 : environ 4 800 demandes liquidées, dont environ 900 réversions."),
    dict(n="4", titre="La durée de liquidation",
         regle="Le nombre de jours entre le dépôt et la première notification des droits de la "
               "demande.",
         sources=[("Annuaire", "p. 64 : du dépôt à la notification des droits"),
                  ("Kaya 2017", "de la réception du dossier complet à la notification ; voir la "
                                "question Q2"),
                  ("Données", "96 % des pièces sont saisies le jour de la réception : la date de "
                              "réception est celle du dossier complet"),
                  ("Proposition BI", "des jours entiers ; une durée négative, erreur de saisie, "
                                     "compte 0")],
         chiffre="En 2025 : 62 jours pour la moitié des dossiers (médiane)."),
    dict(n="5", titre="Les trois colonnes de durée",
         regle="Moins de 15 jours (0 à 14), de 15 à 45 jours, plus de 45 jours (46 et plus). "
               "Ensemble, les trois colonnes font les liquidés.",
         sources=[("Annuaire", "p. 11 et p. 34 : « moins de 15 jours, 15 à 45 jours, plus de 45 "
                               "jours »"),
                  ("Proposition BI", "15 et 45 jours vont dans la colonne du milieu ; un dossier "
                                     "notifié le jour du dépôt va dans « moins de 15 jours »")],
         chiffre="En 2025 : 18 %, 22 % et 60 % des dossiers."),
    dict(n="6", titre="En instance",
         regle="Les demandes reçues, ni notifiées ni rejetées au 31 décembre, quelle que soit "
               "l'année du dépôt. Sans les dossiers fermés jamais notifiés, ni les vieilles "
               "réceptions jamais reliées à un dossier.",
         sources=[("Annuaire", "p. 11 : « le nombre de dossiers restants à traiter en fin "
                               "d'exercice »"),
                  ("Données", "telle quelle, la règle donne 23 218 demandes en instance fin "
                              "2025, cinq fois les dépôts d'une année : surtout des réceptions "
                              "d'avant 2016 sans lien vers leur dossier, et des pensions radiées "
                              "sans date de notification"),
                  ("Proposition BI", "les sortir du stock ; compter les réceptions à partir de "
                                     "2016. Voir la question Q3")],
         chiffre="Pas encore mesuré avec ces exclusions."),
    dict(n="7", titre="Rejetés",
         regle="Les dossiers dont le refus a été enregistré du 1er janvier au 31 décembre.",
         sources=[("Annuaire", "p. 34 : la colonne « Dossiers rejetés »"),
                  ("Kaya 2017", "le module des rejets existe mais n'est pas utilisé ; voir la "
                                "question Q5"),
                  ("Données", "146 rejets de pension depuis 2017, 14 en 2025, avec un motif : "
                              "prescription, pas de bénéficiaire, pièce non conforme, autre. "
                              "Aucun refus n'est noté ailleurs")]),
    dict(n="8", titre="La durée moyenne",
         regle="La moyenne des durées des demandes liquidées, ligne par ligne, comparée à la "
               "norme de 45 jours. On donne aussi la médiane.",
         sources=[("Annuaire", "p. 11 : la durée moyenne, comparée à la norme de moins de 45 "
                               "jours"),
                  ("Kaya 2017", "le délai de liquidation des pensions est la moyenne des durées"),
                  ("Proposition BI", "ajouter la médiane, que quelques dossiers très lents ne "
                                     "faussent pas")],
         chiffre="En 2025 : moyenne 82 jours, médiane 62 jours."),
    dict(n="9", titre="Le Total",
         regle="La somme des lignes : chaque demande compte une seule fois. Les demandes sans "
               "pension créée s'ajoutent aux déposés et aux en instance. La durée moyenne du "
               "Total est recalculée sur toutes les demandes.",
         sources=[("Annuaire", "p. 34 : la ligne TOTAL"),
                  ("Proposition BI", "recalculer la moyenne du Total : une moyenne des moyennes "
                                     "des lignes serait fausse")]),
    dict(n="10", titre="Le taux de dossiers restant à traiter",
         regle="Les demandes en instance au 31 décembre, divisées par celles en instance au 1er "
               "janvier plus celles déposées dans l'année.",
         sources=[("Annuaire", "p. 11 : le nombre de dossiers restants à traiter en fin "
                               "d'exercice"),
                  ("Kaya 2017", "l'indicateur « taux des dossiers restant à traiter en fin de "
                                "période », sans formule"),
                  ("Proposition BI", "la formule")]),
]

# Pour chaque règle, une requête courte sur les vraies tables de l'application (USER_DWH), pour
# l'année 2025, avec en commentaire le chiffre à retrouver. La règle 3 construit la liste
# demandes, la règle 4 leurs durées (durees) ; les règles 5, 8 et 9 les réutilisent.
SQL = {
    "1": """-- dossiers de pension (type V) par
-- catégorie ; les transférés (CNSS_AC)
-- sont écartés dans toutes les règles
SELECT NVL(DOS_CATEGORIE, 'ORDINAIRE')
         AS categorie,
       COUNT(*)
FROM   USER_DWH.DOSSIER
WHERE  TDOS_CODE = 'V'
GROUP  BY NVL(DOS_CATEGORIE, 'ORDINAIRE')
-- COOR_AC : coordination payée par une
--           autre caisse, gardée (3 326)
-- CNSS_AC : transféré à une autre
--           caisse, écarté (153)""",
    "2": """-- les demandes reçues au guichet en 2025
SELECT RD_TYPE_RECEPTION, COUNT(*)
FROM   USER_DWH.RECEPTION_DOSSIER
WHERE  TDOS_CODE = 'V'
AND    RD_DATE_RECEPTION >= DATE '2025-01-01'
AND    RD_DATE_RECEPTION <  DATE '2026-01-01'
GROUP  BY RD_TYPE_RECEPTION
-- NOUVEAU : ouvre un dossier
-- ANCIEN  : arrive sur un dossier existant
--           (à 94 % une réversion)
-- 2025 : 4 671 = 4 055 NOUVEAU + 616 ANCIEN""",
    "3": """-- demandes : une par dossier et par ligne
-- du tableau, réutilisée ensuite
WITH prest AS (
  SELECT pe.DOS_CODE, pe.PE_DATE_DEMANDE,
         pe.PE_DATE_NOTIFICATION,
         CASE WHEN pe.TPE_CODE = 'RPV'  THEN 1
              WHEN pe.TPE_CODE = 'RPRA' THEN 2
              WHEN pe.TPE_CODE = 'RPI'  THEN 3
              WHEN pe.TPE_CODE IN ('RPSV','RPSF',
                     'RPSE','RPA')      THEN 4
              WHEN pe.TPE_CODE = 'RAV'  THEN 5
              WHEN pe.TPE_CODE IN ('RAS','RASC',
                     'RASO','RASA')     THEN 6
              ELSE 7 END AS ligne
  FROM   USER_DWH.PRESTATION_ESP pe
  JOIN   USER_DWH.DOSSIER d
         ON d.DOS_CODE = pe.DOS_CODE
  WHERE  d.TDOS_CODE = 'V'
  AND    NVL(d.DOS_CATEGORIE, '-') <> 'CNSS_AC'),
demandes AS (
  -- liquidée à sa première notification
  SELECT DOS_CODE, ligne,
         MIN(ligne) OVER (PARTITION BY DOS_CODE)
                                AS ligne_principale,
         MIN(PE_DATE_DEMANDE)   AS date_demande,
         MIN(PE_DATE_NOTIFICATION)
                                AS date_liquidation
  FROM   prest
  GROUP  BY DOS_CODE, ligne)
SELECT ligne, COUNT(*)
FROM   demandes
WHERE  date_liquidation >= DATE '2025-01-01'
AND    date_liquidation <  DATE '2026-01-01'
GROUP  BY ligne
ORDER  BY ligne
-- 2025 : environ 4 800 demandes liquidées,
-- dont environ 900 réversions""",
    "4": """-- à ajouter à la suite du WITH de la
-- règle 3 : le dépôt et la durée
depots AS (
  SELECT dm.ligne, dm.date_demande,
         dm.date_liquidation,
         CASE WHEN dm.ligne = dm.ligne_principale
         -- la réception qui a ouvert le dossier
         THEN (SELECT r.RD_DATE_RECEPTION
               FROM   USER_DWH.DOSSIER d
               JOIN   USER_DWH.RECEPTION_DOSSIER r
                      ON r.RD_ID = d.RD_ID
               WHERE  d.DOS_CODE = dm.DOS_CODE)
         -- une réversion : sa réception ANCIEN
         ELSE (SELECT MAX(r.RD_DATE_RECEPTION)
               FROM   USER_DWH.RECEPTION_DOSSIER r
               WHERE  r.DOS_CODE = dm.DOS_CODE
               AND    r.RD_TYPE_RECEPTION
                        = 'ANCIEN'
               AND    r.RD_DATE_RECEPTION
                        <= dm.date_liquidation)
         END AS date_reception
  FROM   demandes dm
  WHERE  dm.date_liquidation >= DATE '2025-01-01'
  AND    dm.date_liquidation <  DATE '2026-01-01'),
durees AS (
  -- sans réception : la date de demande ;
  -- une durée négative compte 0
  SELECT ligne,
         GREATEST(TRUNC(date_liquidation)
           - TRUNC(NVL(date_reception,
                       date_demande)), 0) AS jours
  FROM   depots)
SELECT MEDIAN(jours) FROM durees
-- 2025 : 62 jours (médiane)""",
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
-- 2025, toutes lignes : 18 %, 22 %, 60 %""",
    "6": """-- réceptions de pension ni liquidées ni
-- rejetées au 31/12/2025
SELECT COUNT(*)
FROM   USER_DWH.RECEPTION_DOSSIER r
LEFT   JOIN USER_DWH.DOSSIER d
       ON (d.RD_ID = r.RD_ID          -- NOUVEAU
           OR d.DOS_CODE = r.DOS_CODE) -- ANCIEN
WHERE  r.TDOS_CODE = 'V'
AND    r.RD_DATE_RECEPTION < DATE '2026-01-01'
AND    NVL(d.DOS_CATEGORIE, '-') <> 'CNSS_AC'
AND    NOT EXISTS (     -- aucune notification
         SELECT 1
         FROM   USER_DWH.PRESTATION_ESP p
         WHERE  p.DOS_CODE = d.DOS_CODE
         AND    p.PE_DATE_NOTIFICATION
                  >= TRUNC(r.RD_DATE_RECEPTION)
         AND    p.PE_DATE_NOTIFICATION
                  < DATE '2026-01-01')
AND    NOT EXISTS (     -- aucun rejet
         SELECT 1
         FROM   USER_DWH.DOSSIER_REJETE x
         WHERE  x.RD_ID = r.RD_ID
         AND    x.DOSRE_DATE_REJET
                  < DATE '2026-01-01')
-- sans les dossiers fermés jamais notifiés
AND    NVL(d.DOS_STATUT, '-') <> 'F'
-- ni les réceptions d'avant 2016 sans dossier
AND   (r.RD_DATE_RECEPTION >= DATE '2016-01-01'
       OR d.DOS_CODE IS NOT NULL)
-- sans ces deux exclusions : 23 218""",
    "7": """SELECT COUNT(*)
FROM   USER_DWH.DOSSIER_REJETE
WHERE  TDOS_CODE = 'V'
AND    DOSRE_DATE_REJET >= DATE '2025-01-01'
AND    DOSRE_DATE_REJET <  DATE '2026-01-01'
-- 2025 : 14 (146 depuis 2017)""",
    "8": """-- à coller sous le WITH des règles 3 et 4
SELECT ligne,
       ROUND(AVG(jours)) AS duree_moyenne,
       MEDIAN(jours)     AS duree_mediane
FROM   durees
GROUP  BY ligne
-- 2025, toutes lignes : moyenne 82 jours,
-- médiane 62 ; la norme : moins de 45""",
    "9": """-- le Total : toutes les demandes
-- ensemble, la moyenne recalculée (pas la
-- moyenne des moyennes des lignes)
SELECT COUNT(*)          AS liquides,
       ROUND(AVG(jours)) AS duree_moyenne,
       MEDIAN(jours)     AS duree_mediane
FROM   durees
-- les réceptions sans pension créée
-- s'ajoutent aux déposés et aux en instance""",
    "10": """-- en instance au 31/12/2025, divisé par
-- (en instance au 31/12/2024 + déposés)
-- en instance : la règle 6, une fois avec
-- 2026-01-01, une fois avec 2025-01-01 ;
-- déposés : la règle 2 (4 671 en 2025)
SELECT ROUND(100 * :instance_fin_2025
         / (:instance_fin_2024 + 4671), 1)
         AS taux_restant_pct
FROM   DUAL""",
}

SQL_NOTE = ("À droite de chaque règle, une requête courte sur les vraies tables de l'application "
            "(schéma USER_DWH), pour l'année 2025, à copier dans SQL Developer. La règle 3 "
            "construit la liste des demandes (une par dossier et par ligne du tableau, avec sa "
            "première notification) ; la règle 4 y ajoute leur dépôt et leur durée (durees). "
            "Pour les règles 5, 8 et 9, on colle leur requête sous ce WITH, à la place de sa "
            "dernière requête. Le commentaire en fin de requête donne le chiffre à retrouver. La "
            "requête complète, sur les copies de l'entrepôt, est dans "
            "analyses/prestations/t25_requete_proposee.sql.")

QUESTIONS = [
    dict(n="Q1", titre="Les dossiers de coordination",
         regle="Les dossiers de coordination payés par une autre caisse doivent-ils compter dans "
               "T25 ? La CNSS en reçoit la plupart à son guichet et notifie les droits de la "
               "moitié.",
         sources=[("Données", "en 2025 : 238 dépôts et 292 liquidations, 6 à 8 % de T25"),
                  ("Proposition BI", "les garder, car la CNSS les instruit ; n'exclure que les "
                                     "dossiers transférés à une autre caisse")]),
    dict(n="Q2", titre="La date du dépôt",
         regle="La date de réception saisie au guichet est-elle celle du dossier complet ?",
         sources=[("Kaya 2017", "la durée part de la réception du dossier complet, et le guichet "
                                "ne doit plus recevoir de dossier incomplet"),
                  ("Données", "96 % des pièces sont saisies le jour de la réception ; 4 % "
                              "arrivent plus tard"),
                  ("Proposition BI", "prendre la date de réception saisie")]),
    dict(n="Q3", titre="Les vieilles demandes",
         regle="Une demande en attente depuis des années reste-t-elle « en instance » ? Faut-il "
               "une date de départ, une limite d'ancienneté ou une clôture ?",
         sources=[("Données", "telle quelle, la règle compte 23 218 demandes en instance fin "
                              "2025, dont 79 % reçues avant 2021 : surtout de vieilles "
                              "réceptions jamais reliées à leur dossier"),
                  ("Proposition BI", "ne compter que les réceptions depuis 2016 et sortir les "
                                     "dossiers fermés jamais notifiés : le vrai stock est de "
                                     "quelques centaines de demandes par an")]),
    dict(n="Q4", titre="La pension de la veuve",
         regle="Quand un pensionné décède, la pension de sa veuve est-elle ouverte dans un nouveau "
               "dossier, ou ajoutée au dossier du défunt ? Même question pour les orphelins.",
         cas=[("Si c'est un nouveau dossier",
               "la demande de la veuve compte comme déposée puis liquidée, sur la ligne "
               "« Pension de survivant », avec sa propre durée."),
              ("Si c'est le dossier du défunt",
               "la réversion compte quand même à part : déposée et liquidée sur la ligne "
               "« Pension de survivant », l'année de sa propre notification (règles 2 et 3)."),
              ("Si les deux existent",
               "c'est ce que montrent nos données : les deux cas sont comptés de la même "
               "façon.")],
         sources=[("Données", "la moitié des pensions de veuve sont dans le dossier du défunt ; "
                              "en 2025, 616 demandes de réversion déposées sur un dossier "
                              "existant, suivies à 94 % d'une pension de survivant"),
                  ("Proposition BI", "compter chaque demande, qu'elle ait son propre dossier "
                                     "ou non")]),
    dict(n="Q5", titre="Les refus",
         regle="Les refus de dossier sont-ils tous enregistrés dans le module des rejets ? Nous "
               "n'en trouvons qu'une quinzaine par an.",
         sources=[("Kaya 2017", "le module des rejets existe mais n'est pas utilisé"),
                  ("Données", "146 rejets de pension depuis 2017 ; ni le statut de la réception "
                              "ni la fermeture du dossier ne notent un refus"),
                  ("Proposition BI", "tant que les refus ne sont pas saisis, la colonne "
                                     "« rejetés » reste presque vide")]),
]

BI_CHECKS = ("Une demande reçue au guichet ouvre un seul dossier : oui, sauf 11 cas.\n"
             "Une seconde notification arrive en médiane 9 ans après la première : ce sont des "
             "réversions, pas des révisions.\n"
             "Les réversions sont enregistrées au guichet comme demandes sur un dossier existant : "
             "616 en 2025.\n"
             "Le stock en instance, tel quel, est faussé par de vieilles réceptions sans dossier : "
             "voir Q3.")


def build():
    d = Diagram(seed=25)
    inner = WIDTH - 50

    d.text(0, 0, "T25 · Les dossiers de pension : combien, et en combien de temps ?", 48)
    d.text(0, 66, "« Répartition du nombre de dossiers de prestations PVID de l'exercice selon la "
                  "durée de liquidation » · ce que l'équipe BI a compris, à valider · état au "
                  "30/10/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    about = d.box(25, top, 760, 290, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T25", 22, groups=g, inside=about)
    text = wrap("Il dit combien de dossiers de pension la CNSS a reçus dans l'année, combien elle en "
                "a traités et en combien de temps, combien attendent encore au 31 décembre, et "
                "combien ont été refusés.", 18, 720)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 11, maquette p. 34 ; une fois par an"),
                      ("Annuaire", "la norme CIPRES : une durée moyenne de moins de 45 jours")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36

    g = d.group()
    ftop = top + 310
    figures = d.box(25, ftop, 760, 300, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, ftop + 16, "En 2025 : premiers chiffres, provisoires", 22, groups=g, inside=figures)
    d.text(45, ftop + 54, "4 671 demandes reçues · environ 4 800 liquidées, dont 900\n"
                          "réversions · 14 rejetées. Durée moyenne : 82 jours (médiane 62) :\n"
                          "la norme de 45 jours n'est pas tenue.", 18, groups=g, inside=figures)
    bx = 45
    for share, label, fill, stroke in [(18, "18 %", C["vert_moyen"], C["vert"]),
                                       (22, "22 %", C["jaune"], C["jaune_bord"]),
                                       (60, "60 %", C["rouge_moyen"], C["rouge"])]:
        w = 7.2 * share
        d.box(bx, ftop + 140, w, 40, label, 18, bg=fill, stroke=stroke, groups=g)
        bx += w
    d.text(45, ftop + 188, "moins de 15 jours · 15 à 45 jours · plus de 45 jours", 16, C["doux"],
           groups=g, inside=figures)
    w = chip(d, 45, ftop + 228, "Données", groups=g)
    d.text(45 + w + 10, ftop + 233, "dossiers ouverts en 2025 ; à recalculer selon les règles "
                                    "validées", 16, groups=g, inside=figures)

    bottom = mockup(
        d, 830, top, 445, (WIDTH - 25 - 830 - 445) / 7, "Type de prestation", "1",
        columns=[("Nombre de dossiers déposés", "2"), ("moins de 15 jours", "5"),
                 ("15 à 45 jours", "5"), ("plus de 45 jours", "5"),
                 ("Nombre de dossiers liquidés", "3"), ("Dossiers en instance", "6"),
                 ("Dossiers rejetés", "7")],
        groups=[(1, 3, "Durée de liquidation", "4 et 5")],
        rows=[("Pension de vieillesse", None, None),
              ("Pension de retraite anticipée", "Kaya 2017", None),
              ("Pension d'invalidité", None, None), ("Pension de survivant", None, None),
              ("Allocations de vieillesse", None, None), ("Allocation de survivant", None, None),
              ("Autres prestations de la branche", None, None), ("TOTAL", None, "9")])
    d.text(830, bottom + 14, "Calculés en plus pour chaque ligne : la durée moyenne de liquidation, "
                             "comparée à la norme de 45 jours (règle 8), et le taux de dossiers "
                             "restant à traiter (règle 10).", 18, C["doux"])
    sec["height"] = max(bottom + 50, ftop + 300) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Comment un dossier est compté : six dossiers fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2024, 11, 1), date(2026, 2, 28), 2025, EXAMPLES)
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes : dans quelle ligne va un dossier")
    w = chip(d, 25, y + 72, "Proposition BI")
    d.text(25 + w + 12, y + 76, "Une demande compte une seule fois, dans la ligne de sa pension. "
                                "Un dossier porte en général une demande ; celui d'un pensionné "
                                "décédé en porte deux : sa pension et la réversion (ligne 4).", 18)
    bottom = line_cards(d, 25, y + 120, inner, LINES)
    w = chip(d, 25, bottom + 16, "Proposition BI")
    d.text(25 + w + 12, bottom + 20, "Pas encore de ligne : une demande reçue dont la pension "
                                     "n'est pas encore créée ne compte que dans le Total "
                                     "(déposés, en instance).", 18)
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
    bottom = cards_grid(d, 25, y + 70, inner, 5, QUESTIONS)
    g = d.group()
    checks = d.box(25, bottom + 20, inner, 150, bg=C["gris"], stroke=C["gris_bord"], groups=g)
    d.text(45, bottom + 36, "Ce que l'équipe BI a vérifié dans les données (30/10/2026) : pas de "
                            "réponse attendue", 20, groups=g, inside=checks)
    d.text(45, bottom + 70, BI_CHECKS, 16, groups=g, inside=checks)
    sec["height"] = bottom + 20 + 150 + 30 - sec["y"]
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
