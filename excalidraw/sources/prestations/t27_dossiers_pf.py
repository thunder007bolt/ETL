"""
Fiche de validation de T27 « Répartition du nombre de dossiers de prestations PF de l'exercice
selon la durée de liquidation » (état au 30/10/2026).

Même gabarit que T25 : la maquette, six dossiers fictifs sur un calendrier, les lignes, les dix
règles avec leur SQL et quatre questions. Ce qui change par rapport à T25 : T27 réunit les
prestations familiales (dossier de l'allocataire) et la maternité (un dossier par grossesse, qui
porte l'IJ et l'allocation prénatale), et un enfant ajouté à un dossier ne passe pas au guichet.

Sources : Annuaire CIPRES (p. 12, 34, 64) ; atelier de Kaya 2017 (tableau 1 sur T27, tableau 2
sur le délai de liquidation des PF) ; analyses/prestations (P02, P09, P10 et le lot 4, P64 à
P68) ; t27_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/prestations/t27_dossiers_pf.py [--force]
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

OUTPUT = ROOT / "schemas" / "prestations" / "tableaux cipres" / "T27_dossiers_pf.excalidraw"

HOW_TO = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix dans une "
          "case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la carte si "
          "besoin. Enregistre avec Cmd+S : chaque version est gardée dans l'historique.\n"
          "Les parties 1 à 3 montrent le tableau et des exemples, pour comprendre les règles avant "
          "d'y répondre. Les règles suivent celles de T25 ; ce qui change pour les prestations "
          "familiales et la maternité est dit sur chaque carte.")

EXAMPLES = [
    dict(nom="Dossier A", detail="un nouvel allocataire", debut=date(2025, 1, 6),
         fin=date(2025, 1, 17), sortie="moins15",
         dates="reçu le 06/01/2025, notifié le 17/01/2025 : 11 jours",
         resultat="Dans T27 de 2025 : déposé et liquidé en ligne « Allocations familiales », "
                  "moins de 15 jours."),
    dict(nom="Dossier B", detail="un enfant ajouté", debut=date(2025, 3, 3),
         fin=date(2025, 3, 14), sortie="moins15",
         dates="allocataire depuis 2019 ; nouveau-né demandé le 03/03/2025, notifié le "
               "14/03/2025 : 11 jours",
         resultat="Si l'ajout est une demande (Q1) : une demande de plus en ligne 1, moins de "
                  "15 jours. Sinon : rien dans T27."),
    dict(nom="Dossier C", detail="une grossesse", debut=date(2025, 5, 5), fin=date(2025, 5, 21),
         sortie="15a45",
         dates="reçu le 05/05/2025, 3 semaines avant l'accouchement ; notifié le 21/05/2025 : "
               "16 jours",
         resultat="Dans T27 de 2025 : deux demandes, en ligne « IJ de maternité » et en ligne "
                  "« Allocations prénatales », 15 à 45 jours (Q2)."),
    dict(nom="Dossier D", detail="refusé", debut=date(2025, 6, 2), fin=date(2025, 6, 30),
         sortie="rejet", dates="reçu le 02/06/2025, rejeté le 30/06/2025",
         resultat="Dans T27 de 2025 : déposé et rejeté. Ni liquidé, ni en instance : pas de durée "
                  "de liquidation."),
    dict(nom="Dossier E", detail="à cheval sur deux années", debut=date(2025, 12, 15),
         fin=date(2026, 1, 8), sortie="15a45",
         dates="reçu le 15/12/2025, notifié le 08/01/2026 : 24 jours",
         resultat="Dans T27 de 2025 : déposé, en instance au 31/12. Dans T27 de 2026 : liquidé, "
                  "15 à 45 jours."),
    dict(nom="Dossier F", detail="une réception sans dossier", debut=date(2025, 7, 10),
         fin=date(2026, 2, 28), sortie="instance",
         dates="reçu le 10/07/2025 ; aucun dossier ouvert depuis",
         resultat="Dans T27 de 2025 : déposé, sans ligne (Total seulement), en instance au 31/12. "
                  "Sorti du stock en juillet 2027 s'il n'a toujours pas de suite (Q3)."),
]

LINES = [
    (1, "Allocations familiales", "les allocations familiales de l'allocataire, pour tous ses "
                                  "enfants",
     "un enfant ajouté plus tard, sans passer au guichet : une demande de plus ? Voir la "
     "question Q1", "Proposition BI"),
    (2, "Indemnités journalières de maternité", "les IJ du congé de maternité",
     "une grossesse = un dossier, qui porte aussi l'allocation prénatale (99,4 %)", "Données"),
    (3, "Allocations prénatales", "l'allocation prénatale de la maternité, et l'ancienne "
                                  "allocation prénatale des PF (33 en 2025)",
     "l'atelier de Kaya disait : ni réception ni notification ; ce n'est plus vrai", "Kaya 2017"),
    (4, "Autres prestations de la branche", "aucune prestation aujourd'hui",
     "des frais de maternité sont prévus dans la base, mais jamais payés", "Données"),
]

RULES = [
    dict(n="1", titre="Les dossiers concernés",
         regle="Les dossiers des prestations familiales, un par allocataire, et ceux de la "
               "maternité, un par grossesse. La maternité est rattachée aux prestations "
               "familiales.",
         sources=[("Base", "deux types de dossier dans la branche PF : F prestations "
                           "familiales, M prestations de maternité"),
                  ("Annuaire", "p. 12 : T27 répartit les dossiers PF par type de prestation : "
                               "allocations, indemnités"),
                  ("Données", "aucun soin n'est payé en maternité : M n'est pas la maladie, "
                              "comme le dit la requête actuelle")]),
    dict(n="2", titre="Déposés",
         regle="Les demandes reçues au guichet du 1er janvier au 31 décembre : un nouvel "
               "allocataire ou une nouvelle grossesse, qui ouvre son dossier. Un enfant ajouté, "
               "qui ne passe pas au guichet, est déposé à la date de sa demande.",
         sources=[("Annuaire", "p. 12 : les dossiers « déposés » ; p. 64 : la durée part de la "
                               "date de dépôt"),
                  ("Kaya 2017", "le dépôt, c'est la réception du dossier complet au guichet"),
                  ("Données", "en 2025 : 9 968 réceptions PF et 4 338 de maternité ; 6 réceptions "
                              "seulement sur un dossier existant, avec des certificats de "
                              "scolarité ou de vie : ce ne sont pas des demandes"),
                  ("Proposition BI", "une grossesse est déposée une fois sur chacune de ses deux "
                                     "lignes ; voir les questions Q1 et Q2")],
         chiffre="En 2025 : 14 306 réceptions."),
    dict(n="3", titre="Liquidés",
         regle="Les demandes dont les droits ont été notifiés du 1er janvier au 31 décembre, "
               "quelle que soit l'année du dépôt : la première notification de chaque demande.",
         sources=[("Annuaire", "p. 64 : la liquidation va jusqu'à la « notification des droits au "
                               "bénéficiaire »"),
                  ("Kaya 2017", "T54 et T55 : on compte les IJ de maternité et les allocations "
                                "prénatales liquidées, pas celles payées"),
                  ("Données", "les deux lignes de la maternité sont notifiées, ensemble, 15 jours "
                              "après la demande ; l'atelier de Kaya disait le contraire pour "
                              "l'allocation prénatale, sous son ancien code"),
                  ("Proposition BI", "un enfant ajouté est liquidé à sa propre notification")],
         chiffre="En 2025 : 8 246 nouveaux allocataires, environ 8 100 dossiers avec un enfant "
                 "ajouté, 4 273 IJ de maternité, 4 245 allocations prénatales."),
    dict(n="4", titre="La durée de liquidation",
         regle="Le nombre de jours entre le dépôt et la première notification de la demande.",
         sources=[("Annuaire", "p. 64 : du dépôt à la notification des droits"),
                  ("Kaya 2017", "le délai de liquidation des PF va de la réception du dossier "
                                "complet au guichet à la notification des droits"),
                  ("Données", "une grossesse est déposée 3 semaines avant l'accouchement en "
                              "médiane : la durée ne compte pas l'attente de la naissance"),
                  ("Proposition BI", "un enfant ajouté : depuis la date de sa demande ; une "
                                     "durée négative compte 0")],
         chiffre="En 2025 (médiane) : 13,5 jours pour les allocataires, 17 pour la maternité, "
                 "environ 11 pour un enfant ajouté."),
    dict(n="5", titre="Les trois colonnes de durée",
         regle="Moins de 15 jours (0 à 14), de 15 à 45 jours, plus de 45 jours (46 et plus). "
               "Ensemble, les trois colonnes font les liquidés.",
         sources=[("Annuaire", "p. 12 et p. 34 : « moins de 15 jours, 15 à 45 jours, plus de 45 "
                               "jours »"),
                  ("Proposition BI", "15 et 45 jours vont dans la colonne du milieu, comme en "
                                     "T25")],
         chiffre="Dossiers ouverts en 2025 : allocataires 57 %, 24 % et 19 % ; maternité 44 %, "
                 "44 % et 12 %."),
    dict(n="6", titre="En instance",
         regle="Les demandes reçues, ni notifiées ni rejetées au 31 décembre. Sans les dossiers "
               "fermés jamais notifiés, ni les demandes toujours sans suite 24 mois après leur "
               "dépôt.",
         sources=[("Annuaire", "p. 12 : « le nombre de dossiers restants à traiter en fin "
                               "d'exercice »"),
                  ("Données", "483 réceptions PF de 2025 n'ont ni dossier ni rejet ; des "
                              "allocations anciennes n'ont pas de date de notification (68 % en "
                              "ont une)"),
                  ("Proposition BI", "la limite de 24 mois, la même qu'en T26 ; voir la question "
                                     "Q3")],
         chiffre="Pas encore mesuré avec ces règles."),
    dict(n="7", titre="Rejetés",
         regle="Les dossiers dont le refus a été enregistré du 1er janvier au 31 décembre.",
         sources=[("Annuaire", "p. 34 : la colonne « Dossiers rejetés »"),
                  ("Kaya 2017", "le module des rejets existe mais n'est pas exploité ; il revient "
                                "aux services de l'utiliser"),
                  ("Données", "2 à 62 rejets PF par an, 0 à 17 en maternité ; 21 en 2025 ; voir "
                              "la question Q4")]),
    dict(n="8", titre="La durée moyenne",
         regle="La moyenne des durées des demandes liquidées, ligne par ligne, comparée à la "
               "norme de 45 jours. On donne aussi la médiane.",
         sources=[("Annuaire", "p. 12 : la durée moyenne, comparée à la norme de moins de 45 "
                               "jours"),
                  ("Kaya 2017", "le délai de liquidation des PF est la moyenne des durées"),
                  ("Proposition BI", "ajouter la médiane, que quelques dossiers très lents ne "
                                     "faussent pas")]),
    dict(n="9", titre="Le Total",
         regle="La somme des lignes : chaque demande compte une fois. Une grossesse compte donc "
               "deux fois, un allocataire une fois, plus une fois par enfant ajouté. Les "
               "réceptions sans prestation s'ajoutent aux déposés et aux en instance.",
         sources=[("Annuaire", "p. 34 : la ligne TOTAL"),
                  ("Proposition BI", "recalculer la moyenne du Total : une moyenne des moyennes "
                                     "des lignes serait fausse")]),
    dict(n="10", titre="Le taux de dossiers restant à traiter",
         regle="Les demandes en instance au 31 décembre, divisées par celles en instance au 1er "
               "janvier plus celles déposées dans l'année.",
         sources=[("Annuaire", "p. 12 : le nombre de dossiers restants à traiter en fin "
                               "d'exercice, qui entre dans le calcul des prestations à payer"),
                  ("Proposition BI", "la formule, la même qu'en T25")]),
]

# Pour chaque règle, une requête courte sur les vraies tables de l'application (USER_DWH), pour
# l'année 2025, avec en commentaire le chiffre à retrouver. La règle 3 construit la liste
# demandes, la règle 4 leurs durées (durees) ; les règles 5, 8 et 9 les réutilisent.
SQL = {
    "1": """-- F : prestations familiales, un dossier
-- par allocataire ; M : maternité, un
-- dossier par grossesse
SELECT TDOS_CODE, COUNT(*)
FROM   USER_DWH.DOSSIER
WHERE  TDOS_CODE IN ('F', 'M')
GROUP  BY TDOS_CODE""",
    "2": """-- les demandes reçues au guichet en 2025
SELECT TDOS_CODE, COUNT(*)
FROM   USER_DWH.RECEPTION_DOSSIER
WHERE  TDOS_CODE IN ('F', 'M')
AND    RD_DATE_RECEPTION >= DATE '2025-01-01'
AND    RD_DATE_RECEPTION <  DATE '2026-01-01'
GROUP  BY TDOS_CODE
-- 2025 : 14 306 = 9 968 F + 4 338 M
-- un enfant ajouté ne passe pas au guichet :
-- il est déposé à sa PE_DATE_DEMANDE""",
    "3": """-- demandes : une par dossier et par ligne
-- du tableau, réutilisée ensuite
WITH prest AS (
  SELECT pe.DOS_CODE, pe.PE_DATE_DEMANDE,
         pe.PE_DATE_NOTIFICATION,
         CASE WHEN pe.TPE_CODE = 'FAF' THEN 1
              WHEN pe.TPE_CODE = 'MIJ' THEN 2
              WHEN pe.TPE_CODE IN ('MAP','FAP')
                                       THEN 3
              ELSE 4 END AS ligne
  FROM   USER_DWH.PRESTATION_ESP pe
  JOIN   USER_DWH.DOSSIER d
         ON d.DOS_CODE = pe.DOS_CODE
  WHERE  d.TDOS_CODE IN ('F', 'M')),
demandes AS (
  -- liquidée à sa première notification
  SELECT DOS_CODE, ligne,
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
-- 2025 : allocataires 8 246, IJ de
-- maternité 4 273, prénatales 4 245
-- enfant ajouté, sous le même WITH :
-- chaque autre date de notification FAF
SELECT COUNT(DISTINCT p.DOS_CODE)
FROM   prest p
JOIN   demandes dm
       ON  dm.DOS_CODE = p.DOS_CODE
       AND dm.ligne = 1
WHERE  p.ligne = 1
AND    p.PE_DATE_NOTIFICATION
         > dm.date_liquidation
AND    p.PE_DATE_NOTIFICATION
         >= DATE '2025-01-01'
AND    p.PE_DATE_NOTIFICATION
         <  DATE '2026-01-01'
-- 2025 : environ 8 100 dossiers""",
    "4": """-- à ajouter à la suite du WITH de la
-- règle 3 : dépôt = la réception qui a
-- ouvert le dossier, sinon la date de
-- demande ; une durée négative compte 0
durees AS (
  SELECT dm.ligne,
         GREATEST(TRUNC(dm.date_liquidation)
           - TRUNC(NVL(
               (SELECT r.RD_DATE_RECEPTION
                FROM   USER_DWH.DOSSIER d
                JOIN   USER_DWH.RECEPTION_DOSSIER r
                       ON r.RD_ID = d.RD_ID
                WHERE  d.DOS_CODE = dm.DOS_CODE),
               dm.date_demande)), 0) AS jours
  FROM   demandes dm
  WHERE  dm.date_liquidation >= DATE '2025-01-01'
  AND    dm.date_liquidation <  DATE '2026-01-01')
SELECT ligne, MEDIAN(jours)
FROM   durees
GROUP  BY ligne
-- 2025 : allocataires 13,5 j, maternité 17
-- enfant ajouté : de sa PE_DATE_DEMANDE à
-- sa notification, environ 11 j""",
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
-- dossiers ouverts en 2025 : allocataires
-- 57 %, 24 %, 19 % ; maternité 44 %,
-- 44 %, 12 %""",
    "6": """-- demandes PF ni liquidées ni rejetées
-- au 31/12/2025
SELECT COUNT(*)
FROM   USER_DWH.RECEPTION_DOSSIER r
LEFT   JOIN USER_DWH.DOSSIER d
       ON (d.RD_ID = r.RD_ID          -- NOUVEAU
           OR d.DOS_CODE = r.DOS_CODE) -- ANCIEN
WHERE  r.TDOS_CODE IN ('F', 'M')
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
AND    NOT EXISTS (     -- aucun rejet
         SELECT 1
         FROM   USER_DWH.DOSSIER_REJETE x
         WHERE  x.RD_ID = r.RD_ID
         AND    x.DOSRE_DATE_REJET
                  < DATE '2026-01-01')
-- sans les dossiers fermés jamais notifiés
AND    NVL(d.DOS_STATUT, '-') <> 'F'
-- 2025 : 483 réceptions PF sans dossier
-- ni rejet""",
    "7": """SELECT TDOS_CODE, COUNT(*)
FROM   USER_DWH.DOSSIER_REJETE
WHERE  TDOS_CODE IN ('F', 'M')
AND    DOSRE_DATE_REJET >= DATE '2025-01-01'
AND    DOSRE_DATE_REJET <  DATE '2026-01-01'
GROUP  BY TDOS_CODE
-- 2025 : 21""",
    "8": """-- à coller sous le WITH des règles 3 et 4
SELECT ligne,
       ROUND(AVG(jours)) AS duree_moyenne,
       MEDIAN(jours)     AS duree_mediane
FROM   durees
GROUP  BY ligne
-- la norme : moins de 45 jours""",
    "9": """-- le Total : toutes les demandes
-- ensemble, la moyenne recalculée ; une
-- grossesse compte deux fois (lignes 2 et
-- 3), un allocataire une fois, plus une
-- fois par enfant ajouté (règle 3)
SELECT COUNT(*)          AS liquides,
       ROUND(AVG(jours)) AS duree_moyenne,
       MEDIAN(jours)     AS duree_mediane
FROM   durees""",
    "10": """-- en instance au 31/12/2025, divisé par
-- (en instance au 31/12/2024 + déposés)
-- en instance : la règle 6, une fois avec
-- 2026-01-01, une fois avec 2025-01-01 ;
-- déposés : la règle 2 (14 306 en 2025)
SELECT ROUND(100 * :instance_fin_2025
         / (:instance_fin_2024 + 14306), 1)
         AS taux_restant_pct
FROM   DUAL""",
}

SQL_NOTE = ("À droite de chaque règle, une requête courte sur les vraies tables de l'application "
            "(schéma USER_DWH), pour l'année 2025, à copier dans SQL Developer. La règle 3 "
            "construit la liste des demandes (une par dossier et par ligne du tableau, avec sa "
            "première notification) ; la règle 4 y ajoute leur durée (durees). Pour les règles "
            "5, 8 et 9, on colle leur requête sous ce WITH, à la place de sa dernière requête. "
            "Le commentaire en fin de requête donne le chiffre à retrouver. La requête "
            "complète, sur les copies de l'entrepôt, est dans "
            "analyses/prestations/t27_requete_proposee.sql.")

QUESTIONS = [
    dict(n="Q1", titre="L'enfant ajouté",
         regle="Quand un allocataire déclare un nouvel enfant, l'allocation est ajoutée à son "
               "dossier, sans passer au guichet. Est-ce une demande à compter dans T27 ?",
         cas=[("Si c'est une demande",
               "« Allocations familiales » compte environ 16 400 liquidations en 2025 au lieu de "
               "8 246 ; chacune est déposée à la date de sa demande, liquidée en 11 jours en "
               "médiane."),
              ("Si ce n'en est pas une",
               "la ligne ne compte que les nouveaux allocataires ; le travail des ajouts, autant "
               "que celui des nouveaux dossiers, n'apparaît pas dans T27.")],
         sources=[("Données", "environ 9 000 dossiers reçoivent un enfant de plus chaque année, "
                              "5 ans après leur première notification en médiane"),
                  ("Proposition BI", "les compter : c'est la moitié du travail des PF")]),
    dict(n="Q2", titre="La grossesse sur deux lignes",
         regle="Un dossier de maternité porte l'IJ et l'allocation prénatale, notifiées ensemble. "
               "La grossesse compte-t-elle sur les deux lignes, ou une seule fois ?",
         cas=[("Sur les deux lignes",
               "4 273 IJ de maternité et 4 245 allocations prénatales liquidées en 2025 ; le "
               "Total compte la grossesse deux fois."),
              ("Une seule fois",
               "il faut dire dans quelle ligne : si c'est l'IJ, la ligne des allocations "
               "prénatales reste presque vide.")],
         sources=[("Données", "99,4 % des dossiers de maternité portent les deux ; une mère n'a "
                              "qu'un dossier par an"),
                  ("Proposition BI", "les deux lignes, comme T54 et T55 qui comptent l'une et "
                                     "l'autre")]),
    dict(n="Q3", titre="Les demandes sans suite",
         regle="Une réception qui n'ouvre aucun dossier, ou une allocation jamais notifiée, "
               "reste-t-elle « en instance » sans limite ?",
         sources=[("Données", "483 réceptions PF de 2025 n'ont ni dossier ni rejet, 196 à 511 "
                              "par an depuis 2019"),
                  ("Proposition BI", "en instance pendant 24 mois, puis hors du stock, comme en "
                                     "T26")]),
    dict(n="Q4", titre="Les refus",
         regle="Les refus sont-ils tous enregistrés dans le module des rejets ? Nous en trouvons "
               "21 en 2025, pour 14 306 réceptions.",
         sources=[("Kaya 2017", "le module des rejets n'est pas exploité ; il revient aux "
                                "services de l'utiliser"),
                  ("Proposition BI", "tant que les refus ne sont pas saisis, la colonne "
                                     "« rejetés » reste presque vide")]),
]

BI_CHECKS = ("Un dossier de maternité = une grossesse : une mère n'en a qu'un par an.\n"
             "La grossesse est déposée environ 3 semaines avant l'accouchement : la durée ne "
             "compte pas l'attente de la naissance.\n"
             "L'allocation prénatale est notifiée, en même temps que l'IJ, 15 jours après la "
             "demande.\n"
             "Un enfant ajouté ne passe pas au guichet : 6 réceptions sur un dossier existant en "
             "2025, pour environ 9 000 enfants ajoutés.")


def figures(d, x, y, w):
    """Encadré des premiers chiffres de 2025, avec la répartition par durée."""
    g = d.group()
    h = 330
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + 20, y + 16, "En 2025 : premiers chiffres, provisoires", 22, groups=g, inside=frame)
    d.text(x + 20, y + 54, "9 968 demandes PF et 4 338 de maternité reçues · 8 246\n"
                           "nouveaux allocataires, environ 8 100 enfants ajoutés, 4 273\n"
                           "grossesses liquidées · 21 rejets.", 18, groups=g, inside=frame)
    by = y + 140
    for label, shares in [("allocataires", (57, 24, 19)), ("maternité", (44, 44, 12))]:
        d.text(x + 20, by + 8, label, 16, groups=g, inside=frame)
        bx, scale = x + 220, (w - 240) / 100
        for share, fill, stroke in zip(shares, (C["vert_moyen"], C["jaune"], C["rouge_moyen"]),
                                       (C["vert"], C["jaune_bord"], C["rouge"])):
            d.box(bx, by, share * scale, 36, f"{share} %", 16, bg=fill, stroke=stroke, groups=g)
            bx += share * scale
        by += 52
    d.text(x + 20, by + 4, "moins de 15 jours · 15 à 45 jours · plus de 45 jours", 16, C["doux"],
           groups=g, inside=frame)
    w_chip = chip(d, x + 20, by + 40, "Données", groups=g)
    d.text(x + 20 + w_chip + 10, by + 45, "dossiers ouverts en 2025 (P10) ; à recalculer selon "
                                         "les règles validées", 16, groups=g, inside=frame)
    return y + h


def build():
    d = Diagram(seed=27)
    inner = WIDTH - 50

    d.text(0, 0, "T27 · Les dossiers de prestations familiales : combien, et en combien de temps ?",
           48)
    d.text(0, 66, "« Répartition du nombre de dossiers de prestations PF de l'exercice selon la "
                  "durée de liquidation » · ce que l'équipe BI a compris, à valider · état au "
                  "30/10/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    about = d.box(25, top, 760, 290, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T27", 22, groups=g, inside=about)
    text = wrap("Il dit combien de demandes de prestations familiales et de maternité la CNSS a "
                "reçues dans l'année, combien elle en a traitées et en combien de temps, combien "
                "attendent encore au 31 décembre, et combien ont été refusées.", 18, 720)
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
        rows=[("Allocations familiales", None, None),
              ("Indemnités journalières de maternité", None, None),
              ("Allocations prénatales", None, None),
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
                                "prestation. Le dossier d'un allocataire porte ses allocations "
                                "familiales ; celui d'une grossesse, l'IJ de maternité et "
                                "l'allocation prénatale.", 18)
    bottom = line_cards(d, 25, y + 120, inner, LINES)
    w = chip(d, 25, bottom + 16, "Proposition BI")
    d.text(25 + w + 12, bottom + 20, "Pas encore de ligne : une réception qui n'a encore donné "
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
