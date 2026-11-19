"""
Fiche de validation de T58 « Effectif des travailleurs de l'organisme à la clôture de l'exercice,
par catégorie professionnelle, âge et sexe » (état au 17/11/2026) : fiche pilote de la famille de
l'effectif, T58, T59, T61 et T65.

Même gabarit que les fiches des prestations : la maquette, six agents fictifs sur un calendrier,
les lignes de chaque tableau de la famille, neuf règles avec leur SQL et cinq questions.

Sources : Annuaire CIPRES (p. 17, 50 à 52) ; atelier de Kaya 2017 (T58 scindé en permanents et
temporaires, catégories) ; annuaire de la CNSS 2024 (tableaux 119, 120, 122, 124) ;
recueil des besoins v7 (RH lignes 1, 2 et 4) ; réunion du 03/11/2026 ; analyses/grh (lots 17 à
28, règles G1 à G13, points_a_valider_v2.md) ; t58_t65_effectif_requete_proposee.sql, lancée le
17/11/2026 (analyses/grh/controles).

Usage :
    python3 excalidraw/sources/grh/t58_effectif.py [--force]
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fiche_grh import HOW_TO_DEBUT, OUTPUT_DIR, SORTIES, figures  # noqa: E402
from excalidraw_builder import C, Diagram, main  # noqa: E402
from fiche_cipres import (WIDTH, banner, cards_grid, cards_with_sql, chip, height_of,  # noqa: E402
                          line_cards, mockup, timeline, wrap)

OUTPUT = OUTPUT_DIR / "T58_effectif_31_12.excalidraw"

HOW_TO = HOW_TO_DEBUT + (
    "C'est la fiche pilote de l'effectif : T59 (par domaine), T61 (par ancienneté) et T65 (taux "
    "d'encadrement) comptent le même personnel au 31 décembre, avec les mêmes règles. Valider "
    "T58, c'est valider la famille.")

EXAMPLES = [
    dict(nom="Agent A", detail="permanent, embauché en 2009", debut=date(2023, 10, 1),
         fin=date(2025, 3, 31), sortie="compte", dates="en poste toute l'année",
         resultat="Compté dans T58 de 2024, dans sa catégorie et sa tranche d'âge au 31/12 ; 15 "
                  "ans d'ancienneté dans T61."),
    dict(nom="Agent B", detail="recruté en 2024", debut=date(2024, 3, 1),
         fin=date(2025, 3, 31), sortie="compte", dates="embauché le 01/03/2024",
         resultat="Compté ; moins d'un an d'ancienneté dans T61, et un recrutement dans T60."),
    dict(nom="Agent C", detail="retraité en décembre", debut=date(2023, 10, 1),
         fin=date(2024, 12, 15), sortie="exclu", dates="retraite le 15/12/2024 (décision 02)",
         resultat="Pas compté au 31/12/2024 : il est parti avant. Il était compté au "
                  "31/12/2023."),
    dict(nom="Agent D", detail="mis en disponibilité", debut=date(2023, 10, 1),
         fin=date(2025, 3, 31), sortie="exclu",
         dates="disponibilité depuis le 01/06/2024 (décision 07)",
         resultat="Pas compté au 31/12/2024 : en disponibilité, il sort de l'effectif, comme "
                  "dans l'annuaire de la CNSS (retenu le 08/11/2026). Il reste compté au "
                  "31/12/2023 (question Q5)."),
    dict(nom="Agent E", detail="temporaire", debut=date(2024, 7, 1),
         fin=date(2025, 3, 31), sortie="compte", dates="engagé le 01/07/2024 (décision 50)",
         resultat="Compté dans le tableau des temporaires. Chaque tableau de la famille se lit "
                  "pour les permanents ou pour les temporaires. Sans fin de contrat saisie, il "
                  "resterait compté les années suivantes (question Q1)."),
    dict(nom="Agent F", detail="parti sans décision", debut=date(2023, 10, 1),
         fin=date(2023, 12, 31), sortie="exclu",
         dates="fiche inactive, aucune date de départ",
         resultat="Compté nulle part : la base ne dit pas quand il est parti. C'est le cas des "
                  "84 engagés de 2019 (question Q2)."),
]

LINES = [
    (1, "T58 : les tranches d'âge",
     "moins de 20 ans, 20-24… 55-59, 60 ans et plus ; les permanents, puis les temporaires",
     "en 2024, aucun permanent de moins de 20 ans ; 5 fiches sans état de 69 à 80 ans à "
     "écarter (question Q4)", "Données"),
    (2, "T59 : les domaines",
     "recouvrement, prestations, prévention des risques professionnels, action sanitaire et "
     "sociale, gestion administrative et financière",
     "le domaine est celui de l'unité de l'agent ; tant que le fichier des unités n'est pas "
     "rempli, tout T59 est en « Domaine non renseigné » (question Q3)", "Recueil v7"),
    (3, "T61 : l'ancienneté",
     "moins d'un an, 1-4, 5-9… 35-39, 40 ans et plus ; par sexe",
     "aucun agent de 1 à 4 ans en 2024 : presque aucun recrutement de 2020 à 2023", "Données"),
    (4, "T65 : cinq exercices",
     "une ligne par année : cadres, non-cadres, total et taux d'encadrement",
     "le taux passe de 42 % à 48 % en 2024 : 101 recrutements, dont beaucoup de cadres",
     "Données"),
]

RULES = [
    dict(n="1", titre="Le personnel compté",
         regle="Les agents de la CNSS seulement, pas ceux de la fonction publique ni les agents "
               "affectés. Un tableau pour les permanents, un pour les temporaires, selon la "
               "nature de la dernière situation au 31 décembre.",
         sources=[("Recueil v7", "RH ligne 1 : « par type employé : permanent, temporaire »"),
                  ("Kaya 2017", "T58 est scindé en deux : permanents et temporaires"),
                  ("Annuaire", "p. 17 : « l'effectif des travailleurs de l'organisme »"),
                  ("Base", "qualité A agent CNSS, B fonction publique, C affecté ; nature "
                           "PERMANENT ou TEMPORAIRE"),
                  ("Données", "758 permanents publiés au 31/12/2024, 741 dans la base (requête "
                              "lancée le 17/11/2026)")]),
    dict(n="2", titre="Présent au 31 décembre",
         regle="Entré au plus tard le 31 décembre (embauche, à défaut prise de service) et pas "
               "encore parti : sa fiche n'est pas inactive, ou son premier départ tombe après "
               "le 31 décembre. Un départ est une décision de retraite, de liquidation après "
               "décès, de fin de contrat, de démission, de rupture ou de licenciement, non "
               "annulée ; à défaut, la date de décès.",
         sources=[("Réunion", "03/11/2026 : « d'accord »"),
                  ("Annuaire", "p. 17 : l'effectif « à la clôture de l'exercice »"),
                  ("Données", "741 pour 758 en 2024 ; 5 fiches sans état anciennes sont comptées "
                              "à tort (question Q4), et 3 agents en disponibilité manquent "
                              "(question Q5)")],
         chiffre="En 2024 : 101 recrutements, 24 retraites, 5 décès, 1 démission (T60)."),
    dict(n="3", titre="La catégorie",
         regle="La catégorie de la situation de l'agent (sa catégorie dans la grille de la "
               "CNSS) : 1re et 2e catégories, employés ; 3e, agents de maîtrise ; 4e et 5e, "
               "cadres moyens ; 6e et hors catégorie, cadres supérieurs. Pour la CIPRES, les "
               "cadres sont les 4e, 5e et 6e.",
         sources=[("Recueil v7", "RH ligne 1 : cadre moyen, catégories 4 et 5 ; cadre "
                                 "supérieur, catégorie 6"),
                  ("Annuaire", "p. 50 : employés, agents de maîtrise et cadres ; chaque "
                               "organisme garde ses catégories"),
                  ("Données", "177, 206, 301 et 57 dans la base pour 177, 208, 311 et 62 "
                              "publiés ; 2 agents hors catégorie seulement, aux fiches "
                              "inactives (lot 26, Q143)")]),
    dict(n="4", titre="L'âge",
         regle="L'âge en années pleines au 31 décembre. Tranches : moins de 20 ans, 20-24… "
               "55-59, 60 ans et plus.",
         sources=[("Réunion", "03/11/2026 : « d'accord »"),
                  ("Annuaire", "p. 50 : la maquette et ses tranches"),
                  ("Données", "les tranches de 20 à 34 ans sont identiques à l'annuaire de la "
                              "CNSS, les suivantes à 2 à 9 agents près ; 5 fiches sans état de "
                              "69 à 80 ans en trop (lot 27, Q158)")],
         chiffre="Exemple : né le 15/06/1980, il a 44 ans au 31/12/2024."),
    dict(n="5", titre="Le sexe",
         regle="Hommes et femmes d'après la fiche de l'agent.",
         sources=[("Réunion", "03/11/2026 : « d'accord »"),
                  ("Base", "M et F"),
                  ("Données", "402 hommes et 339 femmes en 2024 ; 412 et 346 publiés")]),
    dict(n="6", titre="Les temporaires",
         regle="Un temporaire est présent si le 31 décembre tombe dans un de ses contrats. Un "
               "contrat commence à la décision d'engagement temporaire ; il finit au premier "
               "départ saisi, sinon la veille du contrat suivant. Sans fin connue, l'agent "
               "reste compté.",
         sources=[("Réunion", "03/11/2026 : la BI ne fixe pas de durée de contrat à la place de "
                              "la CNSS"),
                  ("Kaya 2017", "la partie « temporaires » de T58"),
                  ("Données", "299 temporaires fin 2024 pour 46 contractuels publiés, et le "
                              "nombre monte chaque année ; 47 en ne gardant que les fiches non "
                              "inactives (lot 27, Q153)"),
                  ("Proposition BI", "en attendant la saisie des fins de contrat, ne compter "
                                     "que les fiches non inactives ; question Q1")]),
    dict(n="7", titre="L'ancienneté (T61)",
         regle="Les années pleines au 31 décembre depuis la prise de service, à défaut "
               "l'embauche, moins les périodes de disponibilité et les départs suivis d'un "
               "nouvel engagement. Tranches : moins d'un an, 1-4, 5-9… 35-39, 40 ans et plus.",
         sources=[("Recueil v7", "RH ligne 4 : « prendre en compte les sorties et retours »"),
                  ("Réunion", "03/11/2026 : la prise de service d'abord"),
                  ("Annuaire", "p. 52 : la maquette de T61"),
                  ("Données", "chaque tranche à 4 agents près de l'annuaire de la CNSS : moins "
                              "d'un an 102 pour 103, 10-14 ans 225 pour 232")],
         chiffre="Exemple : prise de service le 01/01/2008, disponibilité de 2015 à 2018 : 13 "
                 "ans au 31/12/2024."),
    dict(n="8", titre="Le domaine d'activité (T59)",
         regle="Le domaine de l'unité où travaille l'agent, lu dans le fichier des unités tenu "
               "par la CNSS. Cinq domaines. Une unité sans domaine donne « Domaine non "
               "renseigné ».",
         sources=[("Recueil v7", "RH ligne 2 : cinq domaines, de recouvrement à gestion "
                                 "administrative et financière"),
                  ("Réunion", "03/11/2026 : le domaine par l'unité"),
                  ("Données", "le code de domaine de la situation n'est pas fiable : il change "
                              "d'un agent à l'autre dans 66 unités sur 139 (lot 23)"),
                  ("Proposition BI", "le fichier unité → domaine, 132 unités à décider ; "
                                     "question Q3")]),
    dict(n="9", titre="Le taux d'encadrement (T65)",
         regle="Les cadres (moyens et supérieurs) divisés par l'effectif au 31 décembre, sur "
               "les cinq derniers exercices. Norme : 10 % au plus.",
         sources=[("Réunion", "03/11/2026 : « on garde comme ça »"),
                  ("Annuaire", "p. 17 : « nombre de cadres sur total effectif »"),
                  ("Données", "48,3 % pour 49,2 % en 2024 (358 cadres sur 741) ; 41,9 % à "
                              "42,4 % de 2020 à 2023")]),
]

# Pour chaque règle, une requête courte sur les vraies tables de l'application (USER_GRH), pour
# l'année 2024, avec en commentaire le chiffre à retrouver. La règle 2 construit la liste
# permanents_2024, que les règles 3, 4, 5, 7, 8 et 9 réutilisent.
SQL = {
    "1": """-- qui est permanent au 31/12/2024 :
-- agent de la CNSS (QUALITE = 'A') dont
-- la dernière situation est PERMANENT
SELECT p.PERS_ID
FROM   USER_GRH.GRH_PERSONNE p
JOIN   USER_GRH.GRH_SITUATION s
       ON s.PERS_ID = p.PERS_ID
WHERE  p.QUALITE = 'A'
AND    s.NATURE_EMPLOIE = 'PERMANENT'
-- sa dernière situation, commencée
-- au plus tard le 31/12/2024
AND    s.DATE_DEBUT = (
         SELECT MAX(x.DATE_DEBUT)
         FROM   USER_GRH.GRH_SITUATION x
         WHERE  x.PERS_ID = p.PERS_ID
         AND    x.DATE_DEBUT <= DATE '2024-12-31')""",
    "2": """-- la liste des permanents présents
-- au 31/12/2024, réutilisée ensuite
WITH permanents_2024 AS (
  SELECT p.PERS_ID, p.SEXE, p.DATE_NAISS,
         p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE,
         s.CATEGORIE, s.UA_CODE
  FROM   USER_GRH.GRH_PERSONNE p
  JOIN   USER_GRH.GRH_SITUATION s
         ON s.PERS_ID = p.PERS_ID
  -- règle 1 : agent CNSS, permanent
  WHERE  p.QUALITE = 'A'
  AND    s.NATURE_EMPLOIE = 'PERMANENT'
  AND    s.DATE_DEBUT = (
           SELECT MAX(x.DATE_DEBUT)
           FROM   USER_GRH.GRH_SITUATION x
           WHERE  x.PERS_ID = p.PERS_ID
           AND    x.DATE_DEBUT <= DATE '2024-12-31')
  -- entré au plus tard le 31/12/2024
  AND    NVL(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE)
           <= DATE '2024-12-31'
  -- pas parti : fiche pas inactive, ou
  -- premier départ (02, 34, 35, 36, 52, 54)
  -- après le 31/12/2024
  AND   (NVL(p.ETAT_EMPLOYE, '-') <> 'INACTIF'
         OR (SELECT MIN(d.DATE_VIGUEUR)
             FROM   USER_GRH.GRH_DECISION d
             WHERE  d.PERS_ID = p.PERS_ID
             AND    d.CDEC_CODE IN
                    ('02','34','35','36','52','54'))
            > DATE '2024-12-31'))
SELECT COUNT(*) FROM permanents_2024
-- la requête complète écarte aussi les
-- décisions annulées par une 40, lit les
-- décisions collectives et sort les agents
-- en disponibilité : 741 en 2024""",
    "3": """-- à coller sous la règle 2, à la place
-- de sa dernière ligne
SELECT CATEGORIE, COUNT(*)
FROM   permanents_2024
GROUP  BY CATEGORIE
ORDER  BY CATEGORIE
-- 2024 : 01 + 02 employés      177
--        03      maîtrise      206
--        04 + 05 cadres moyens 301
--        06 + HC cadres sup.    57""",
    "4": """-- âge en années pleines au 31/12/2024,
-- par tranche de 5 ans (20 = 20 à 24 ans)
SELECT tranche, COUNT(*)
FROM  (SELECT FLOOR(FLOOR((DATE '2024-12-31'
                - DATE_NAISS) / 365.25) / 5) * 5
                AS tranche
       FROM   permanents_2024)
GROUP  BY tranche
ORDER  BY tranche
-- 2024, de 20-24 à 55-59 ans : 12, 28, 70,
-- 115, 139, 152, 150, 70 ; 5 de 60 ans et
-- plus : des fiches sans état anciennes""",
    "5": """SELECT SEXE, COUNT(*)
FROM   permanents_2024
GROUP  BY SEXE
-- 2024 : M (hommes) 402, F (femmes) 339""",
    "6": """-- les temporaires au 31/12/2024 : un
-- engagement (50), aucun départ depuis
SELECT COUNT(*) AS regle_actuelle,
       SUM(CASE WHEN NVL(p.ETAT_EMPLOYE, '-')
                     <> 'INACTIF'
                THEN 1 ELSE 0 END)
         AS fiche_non_inactive
FROM   USER_GRH.GRH_PERSONNE p
JOIN   USER_GRH.GRH_SITUATION s
       ON s.PERS_ID = p.PERS_ID
WHERE  p.QUALITE = 'A'
AND    s.NATURE_EMPLOIE = 'TEMPORAIRE'
AND    s.DATE_DEBUT = (  -- dernière situation
         SELECT MAX(x.DATE_DEBUT)
         FROM   USER_GRH.GRH_SITUATION x
         WHERE  x.PERS_ID = p.PERS_ID
         AND    x.DATE_DEBUT <= DATE '2024-12-31')
AND    EXISTS (
  SELECT 1
  FROM   USER_GRH.GRH_DECISION e
  WHERE  e.PERS_ID = p.PERS_ID
  AND    e.CDEC_CODE = '50'
  AND    e.DATE_VIGUEUR <= DATE '2024-12-31')
-- aucun départ après le dernier engagement
AND    NOT EXISTS (
  SELECT 1
  FROM   USER_GRH.GRH_DECISION f
  WHERE  f.PERS_ID = p.PERS_ID
  AND    f.CDEC_CODE IN
         ('02','34','35','36','52','54')
  AND    f.DATE_VIGUEUR <= DATE '2024-12-31'
  AND    f.DATE_VIGUEUR >= (
           SELECT MAX(e.DATE_VIGUEUR)
           FROM   USER_GRH.GRH_DECISION e
           WHERE  e.PERS_ID = f.PERS_ID
           AND    e.CDEC_CODE = '50'
           AND    e.DATE_VIGUEUR
                  <= DATE '2024-12-31'))
-- 2024 : 299, dont 47 fiches non inactives
-- (l'annuaire : 46 contractuels)""",
    "7": """-- ancienneté en années pleines au
-- 31/12/2024, depuis la prise de service
-- (à défaut, depuis l'embauche)
SELECT anciennete, COUNT(*)
FROM  (SELECT FLOOR((DATE '2024-12-31'
                - NVL(DATE_PRISE_SERVICE,
                      DATE_EMBAUCHE))
                / 365.25) AS anciennete
       FROM   permanents_2024)
GROUP  BY anciennete
ORDER  BY anciennete
-- puis regrouper : moins de 1, 1-4, 5-9…
-- 2024 : moins d'un an 102, 10-14 ans 225
-- (la requête complète retire aussi les
-- disponibilités et les départs suivis
-- d'un nouvel engagement)""",
    "8": """SELECT UA_CODE, COUNT(*)
FROM   permanents_2024
GROUP  BY UA_CODE
-- le domaine de chaque unité vient du
-- fichier unité -> domaine de la CNSS
-- (correspondance_unite_domaine.xlsx) ;
-- libellés des unités :
-- USER_GRH.GRH_UNITE_ADMINISTRATIVE""",
    "9": """SELECT ROUND(100 * SUM(CASE
         WHEN CATEGORIE IN ('04','05','06','HC')
         THEN 1 ELSE 0 END) / COUNT(*), 1)
         AS taux_encadrement
FROM   permanents_2024
-- 2024 : 48,3 % (l'annuaire : 49,2 %)
-- pour une autre année : refaire la
-- règle 2 avec le 31/12 de cette année""",
}

SQL_NOTE = ("À droite de chaque règle, une requête courte sur les vraies tables de l'application "
            "(schéma USER_GRH), pour l'année 2024, à copier dans SQL Developer. La règle 2 "
            "construit la liste des permanents présents au 31/12/2024 (permanents_2024) ; pour "
            "les règles 3, 4, 5, 7, 8 et 9, on colle leur requête sous celle de la règle 2, à la "
            "place de sa dernière ligne. Le commentaire en fin de requête donne le chiffre à "
            "retrouver. La requête complète, sur les copies de l'entrepôt, est dans "
            "analyses/grh/t58_t65_effectif_requete_proposee.sql.")

QUESTIONS = [
    dict(n="Q1", titre="Les temporaires sans fin de contrat",
         regle="La base ne garde pas la fin d'un contrat temporaire : un temporaire reste "
               "compté tant qu'aucun départ n'est saisi. En attendant la saisie des fins de "
               "contrat, compte-t-on seulement ceux dont la fiche n'est pas inactive ?",
         cas=[("Fiches non inactives seulement (proposé)",
               "47 temporaires fin 2024, pour 46 contractuels dans l'annuaire de la CNSS."),
              ("La règle actuelle",
               "299 temporaires fin 2024, et le nombre monte chaque année (338 fin 2025)."),
              ("Un contrat de 18 mois", "60 temporaires fin 2024.")],
         sources=[("Données", "lot 27, Q153 ; contrôle de l'effectif du 17/11/2026"),
                  ("Proposition BI", "le réglage fin_contrat_inconnue = FICHE")]),
    dict(n="Q2", titre="Les 84 engagés de 2019",
         regle="84 permanents, en service depuis 2018 et engagés en 2019, ont une fiche "
               "inactive sans aucun départ. L'annuaire les compte de 2019 à 2023, puis les sort "
               "en 2024. Que sont-ils devenus, et pouvez-vous saisir leur départ dans "
               "l'application ?",
         sources=[("Données", "lots 18, 20 et 27 (Q154, Q155) ; annuaire CNSS 2024, tableau "
                              "121 : 84 « autres entrées » en 2019, 84 « autres sorties » en "
                              "2024"),
                  ("Proposition BI", "sans date saisie, ils ne sont comptés dans aucune année : "
                                     "670 permanents fin 2023 pour 767 publiés")]),
    dict(n="Q3", titre="Le domaine de chaque unité (T59)",
         regle="Le domaine d'un agent est celui de son unité. Pouvez-vous remplir le domaine "
               "de chaque unité dans le fichier préparé, d'abord les 132 lignes « À décider » ?",
         sources=[("Recueil v7", "RH ligne 2 : cinq domaines"),
                  ("Données", "432 unités, dont 157 en vigueur (lot 23)"),
                  ("Proposition BI", "sans le fichier, tout T59 est en « Domaine non "
                                     "renseigné »")]),
    dict(n="Q4", titre="Les fiches sans état",
         regle="67 fiches n'ont pas d'état. La plupart sont des embauches récentes, mais au "
               "moins 11 sont anciennes, dont 5 permanents de 69 à 80 ans. Une fiche sans état "
               "entrée depuis plus de deux ans est-elle celle d'un agent parti ?",
         cas=[("Oui : on ne la compte plus (proposé)",
               "736 permanents fin 2024, aucun de 60 ans et plus, comme l'annuaire."),
              ("Non", "741 permanents fin 2024, dont 5 de 60 ans et plus.")],
         sources=[("Données", "lots 26 et 27 (Q151, Q157, Q158)"),
                  ("Proposition BI", "ne compter une fiche sans état que si l'entrée est "
                                     "récente")]),
    dict(n="Q5", titre="La fiche inactive d'un agent en disponibilité",
         regle="12 agents mis en disponibilité de 2021 à 2025 ont une fiche inactive sans "
               "aucun départ daté. La fiche passe-t-elle bien à « inactif » pendant une "
               "disponibilité, et pour quelles autres absences ?",
         sources=[("Données", "lot 28, Q159 : il manque 12 permanents fin 2020, 3 fin 2024"),
                  ("Proposition BI", "compter ces agents jusqu'à leur mise en disponibilité, "
                                     "qui devient leur date de sortie")]),
]

BI_CHECKS = ("La requête de l'effectif a été lancée le 17/11/2026 : 741 permanents fin 2024 pour "
             "758 publiés, les employés au chiffre exact (133 hommes, 44 femmes).\n"
             "Les âges retombent sur l'annuaire de la CNSS jusqu'à 34 ans, et l'ancienneté à 4 "
             "agents près par tranche.\n"
             "Le taux d'encadrement : 48,3 % pour 49,2 % en 2024.\n"
             "Les temporaires ne retombent pas : 299 pour 46 contractuels publiés (question Q1).")

# (tranche, annuaire de la CNSS, base)
AGES_2024 = [("20-24", 12, 12), ("25-29", 28, 28), ("30-34", 70, 70), ("35-39", 117, 115),
             ("40-44", 141, 139), ("45-49", 157, 152), ("50-54", 154, 150), ("55-59", 79, 70)]


def build():
    d = Diagram(seed=58)
    inner = WIDTH - 50

    d.text(0, 0, "T58 · Le personnel au 31 décembre, par catégorie, âge et sexe", 48)
    d.text(0, 66, "« Effectif des travailleurs de l'organisme à la clôture de l'exercice par "
                  "catégorie professionnelle, âge et sexe » · fiche pilote de T58, T59, T61 et "
                  "T65 · ce que l'équipe BI a compris, à valider · état au 17/11/2026", 22,
           C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    text = wrap("Il dit combien de personnes la CNSS emploie au 31 décembre, par catégorie "
                "(employés, agents de maîtrise, cadres), par tranche d'âge et par sexe : le "
                "personnel permanent d'abord, les temporaires ensuite. Les tableaux T59 "
                "(domaine), T61 (ancienneté) et T65 (taux d'encadrement) comptent le même "
                "personnel.", 18, 720)
    about_h = 54 + height_of(text, 18) + 16 + 2 * 36 + 10
    about = d.box(25, top, 760, about_h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T58", 22, groups=g, inside=about)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 17, maquettes p. 50 à 52"),
                      ("Kaya 2017", "le tableau scindé : permanents, temporaires")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(
        d, 25, top + about_h + 20, 760, "En 2024 : la base face à l'annuaire de la CNSS",
        "758 permanents au 31/12/2024 dans l'annuaire, 741 dans la base ; 46 contractuels "
        "publiés, 299 temporaires dans la base (47 aux fiches non inactives). Par catégorie : "
        "177, 208, 311, 62 publiés ; 177, 206, 301, 57 dans la base.",
        "Permanents par tranche d'âge, en 2024 :", AGES_2024,
        "annuaire CNSS 2024, tableau 120 ; requête lancée le 17/11/2026")

    cat_cols = [("Hommes", "5"), ("Femmes", "5")]
    bottom = mockup(
        d, 830, top, 260, (WIDTH - 25 - 830 - 260) / 11, "Tranches d'âge", "4",
        columns=cat_cols * 4 + [("Hommes", "5"), ("Femmes", "5"), ("Ensemble", "1")],
        groups=[(0, 1, "EMPLOYÉS", "3"), (2, 3, "MAÎTRISE", "3"),
                (4, 5, "CADRES MOYENS", "3"), (6, 7, "CADRES SUP.", "3"),
                (8, 10, "TOTAL", "2")],
        rows=[("Moins de 20 ans", None, None), ("20 – 24 ans", None, None),
              ("25 – 29 ans", None, None), ("30 – 34 ans", None, None),
              ("35 – 39 ans", None, None), ("40 – 44 ans", None, None),
              ("45 – 49 ans", None, None), ("50 – 54 ans", None, None),
              ("55 – 59 ans", None, None), ("60 ans et plus", None, None),
              ("TOTAL", None, "2")],
        row_h=46)
    d.text(830, bottom + 14, wrap("Les cases comptent les permanents présents au 31/12 (règles 1 "
                                  "et 2), par catégorie (règle 3) et par sexe (règle 5). La "
                                  "partie « temporaires » a la même forme (règle 6).", 18,
                                  WIDTH - 25 - 830), 18, C["doux"])
    sec["height"] = max(bottom + 80, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Qui est compté au 31/12/2024 : six agents fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2023, 10, 1), date(2025, 3, 31), 2024,
                      EXAMPLES, outcomes=SORTIES,
                      axis_label="Dans le calendrier : l'agent en poste, de l'entrée au départ")
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes de chaque tableau de la famille")
    w = chip(d, 25, y + 72, "Annuaire")
    d.text(25 + w + 12, y + 76, "T58 a une ligne par tranche d'âge ; T59, T61 et T65 gardent "
                                "les mêmes colonnes (catégories et sexe) avec leurs propres "
                                "lignes.", 18)
    bottom = line_cards(d, 25, y + 120, inner, LINES)
    sec["height"] = bottom + 30 - sec["y"]

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
    sec = d.section(0, y, WIDTH, 0, "5. Les questions pour la DRH")
    bottom = cards_grid(d, 25, y + 70, inner, 3, QUESTIONS)
    g = d.group()
    checks = d.box(25, bottom + 20, inner, 170, bg=C["gris"], stroke=C["gris_bord"], groups=g)
    d.text(45, bottom + 36, "Ce que l'équipe BI a vérifié dans les données (17/11/2026) : pas de "
                            "réponse attendue", 20, groups=g, inside=checks)
    d.text(45, bottom + 70, BI_CHECKS, 16, groups=g, inside=checks)
    sec["height"] = bottom + 20 + 170 + 30 - sec["y"]
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
