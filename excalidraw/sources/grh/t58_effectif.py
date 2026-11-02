"""
Fiche de validation de T58 « Effectif des travailleurs de l'organisme à la clôture de l'exercice,
par catégorie professionnelle, âge et sexe » (état au 01/11/2026) : fiche pilote de la famille de
l'effectif, T58, T59, T61 et T65.

Même gabarit que les fiches des prestations : la maquette, six agents fictifs sur un calendrier,
les lignes de chaque tableau de la famille, neuf règles avec leur SQL et cinq questions.

Sources : Annuaire CIPRES (p. 17, 50 à 52) ; atelier de Kaya 2017 (T58 scindé en permanents et
temporaires, catégories) ; annuaire de la CNSS 2024 (tableaux 119, 120, 122, 124) ;
analyses/grh (lots 17 à 20, règles G1 à G7) ; t58_t65_effectif_requete_proposee.sql.

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
         fin=date(2025, 3, 31), sortie="question",
         dates="disponibilité depuis le 01/06/2024 (décision 07)",
         resultat="Compté aujourd'hui ; pas compté si l'agent en disponibilité sort de "
                  "l'effectif, comme dans l'annuaire de la CNSS (question Q4)."),
    dict(nom="Agent E", detail="contractuel", debut=date(2024, 7, 1),
         fin=date(2025, 3, 31), sortie="compte", dates="engagé le 01/07/2024 (décision 50)",
         resultat="Compté dans la partie « contractuels » de T58 ; pas dans T59, T61 ni T65, qui "
                  "ne comptent que les permanents."),
    dict(nom="Agent F", detail="parti sans décision", debut=date(2023, 10, 1),
         fin=date(2023, 12, 31), sortie="exclu",
         dates="fiche inactive, aucune date de départ",
         resultat="Compté nulle part : la base ne dit pas quand il est parti. C'est le cas des "
                  "84 engagés de 2019 (question Q5)."),
]

LINES = [
    (1, "T58 : les tranches d'âge",
     "moins de 20 ans, 20-24… 55-59, 60 ans et plus ; les permanents, puis les contractuels",
     "en 2024, aucun permanent de moins de 20 ans ni de 60 ans et plus", "Annuaire"),
    (2, "T59 : les domaines",
     "immatriculation, recouvrement, contentieux, prestations, prévention des risques "
     "professionnels, action sanitaire et sociale, autres domaines",
     "T59 n'est pas publié par l'annuaire de la CNSS : le rangement des domaines est à valider "
     "(question Q3)", "Proposition BI"),
    (3, "T61 : l'ancienneté",
     "moins d'un an, 1-4, 5-9… 35-39, 40 ans et plus ; par sexe",
     "un seul agent de 1 à 4 ans en 2024 : aucun recrutement de 2020 à 2023", "Données"),
    (4, "T65 : cinq exercices",
     "une ligne par année : cadres, non-cadres, total et taux d'encadrement",
     "le taux passe de 41 % à 49 % en 2024 : sortie des 84 de 2019, recrutement de cadres",
     "Données"),
]

RULES = [
    dict(n="1", titre="Le personnel compté",
         regle="Le personnel permanent : un agent de la CNSS dont la dernière situation au 31 "
               "décembre est de nature permanente. Les contractuels sont comptés à part (règle "
               "6).",
         sources=[("Annuaire", "p. 17 : « l'effectif des travailleurs de l'organisme »"),
                  ("Kaya 2017", "T58 est scindé en deux : permanents et temporaires"),
                  ("Base", "qualité A agent CNSS, B fonction publique, C affecté ; nature "
                           "PERMANENT ou TEMPORAIRE"),
                  ("Données", "758 permanents publiés au 31/12/2024, 742 dans la base (lot 17, "
                              "Q97)")]),
    dict(n="2", titre="Présent au 31 décembre",
         regle="Entré au plus tard le 31 décembre (embauche, à défaut prise de service), et "
               "encore en poste, ou sorti après le 31 décembre. La sortie est la première "
               "décision de départ (retraite, démission, licenciement, fin de contrat, "
               "liquidation des droits) non annulée, à défaut la date de décès.",
         sources=[("Annuaire", "p. 17 : l'effectif « à la clôture de l'exercice »"),
                  ("Données", "742 pour 758 en 2024 (−2 %) ; les 16 manquants ne forment aucun "
                              "groupe (lot 19, Q115)"),
                  ("Proposition BI", "accepter cet écart de 2 %")],
         chiffre="En 2024 : 25 départs à la retraite, dont 9 en décembre (lot 19, Q116)."),
    dict(n="3", titre="La catégorie",
         regle="La catégorie de la situation de l'agent (sa catégorie dans la grille de la "
               "CNSS) : 1re et 2e catégories, employés ; 3e, agents de maîtrise ; 4e et 5e, "
               "cadres moyens ; 6e, cadres supérieurs.",
         sources=[("Annuaire", "p. 50 : employés, agents de maîtrise et cadres ; chaque "
                               "organisme garde ses catégories"),
                  ("Kaya 2017", "1 et 2 exécution, 3 maîtrise, 4 cadres moyens, 5 et 6 cadres "
                                "supérieurs"),
                  ("Données", "seule la catégorie de la situation retombe sur l'annuaire de la "
                              "CNSS 2024 : 173, 207, 304, 58 pour 177, 208, 311, 62, avec la 5e "
                              "en cadres moyens (lots 18 et 19, Q103, Q111)"),
                  ("Proposition BI", "la 5e en cadres moyens, comme l'annuaire de 2024 ; "
                                     "question Q1")]),
    dict(n="4", titre="L'âge",
         regle="L'âge en années pleines au 31 décembre. Tranches : moins de 20 ans, 20-24… "
               "55-59, 60 ans et plus.",
         sources=[("Annuaire", "p. 50 : la maquette et ses tranches"),
                  ("Données", "en années pleines, les tranches retombent sur l'annuaire de la "
                              "CNSS : identiques jusqu'à 34 ans, à 2 près jusqu'à 54 (lot 17, "
                              "Q98)"),
                  ("Proposition BI", "les prestations comptent autrement (année moins année "
                                     "de naissance) : la GRH suit l'annuaire")],
         chiffre="Exemple : né le 15/06/1980, il a 44 ans au 31/12/2024."),
    dict(n="5", titre="Le sexe",
         regle="Hommes et femmes d'après la fiche de l'agent.",
         sources=[("Base", "M et F"),
                  ("Données", "400 hommes et 342 femmes en 2024 ; 412 et 346 publiés (lot 17, "
                              "Q98)")]),
    dict(n="6", titre="Les contractuels",
         regle="Un agent temporaire au 31 décembre, dont le dernier contrat (décision "
               "d'engagement temporaire) a moins de 18 mois, sans départ depuis. Une fin de "
               "contrat suivie d'un nouveau contrat n'est pas un départ.",
         sources=[("Kaya 2017", "la partie « temporaires » de T58"),
                  ("Données", "46 contractuels publiés fin 2024 ; 44 engagés en 2024 et 2 au "
                              "second semestre 2023 dans la base ; 7 seulement ont une fiche "
                              "active (lot 19, Q114)"),
                  ("Proposition BI", "18 mois, faute de date de fin de contrat dans la base ; "
                                     "question Q2")]),
    dict(n="7", titre="L'ancienneté (T61)",
         regle="Les années pleines au 31 décembre depuis l'embauche, à défaut la prise de "
               "service. Tranches : moins d'un an, 1-4, 5-9… 35-39, 40 ans et plus.",
         sources=[("Annuaire", "p. 52 : la maquette de T61"),
                  ("Données", "chaque tranche à 8 près de l'annuaire de la CNSS ; moins d'un an "
                              "101 pour 103 (lot 17, Q100)")],
         chiffre="Exemple : embauché le 01/03/2024, moins d'un an au 31/12/2024."),
    dict(n="8", titre="Le domaine d'activité (T59)",
         regle="Le domaine de la situation de l'agent, rangé dans les sept domaines de "
               "l'Annuaire ; le pilotage, la comptabilité et les autres codes vont dans « autres "
               "domaines ».",
         sources=[("Annuaire", "p. 17 : sept domaines"),
                  ("Kaya 2017", "cinq domaines : recouvrement, prestations, prévention, action "
                                "sanitaire et sociale, gestion administrative"),
                  ("Données", "fin 2025, le pilotage compte 431 actifs sur 879 (lot 8, Q47)"),
                  ("Proposition BI", "le rangement des codes ; question Q3")]),
    dict(n="9", titre="Le taux d'encadrement (T65)",
         regle="Les cadres (moyens et supérieurs) divisés par l'effectif permanent au 31 "
               "décembre, sur les cinq derniers exercices.",
         sources=[("Annuaire", "p. 17 : « nombre de cadres sur total effectif »"),
                  ("Kaya 2017", "la notion de cadre restait à clarifier"),
                  ("Données", "48,8 % pour 49,2 % en 2024 ; à 0,3 point près de 2020 à 2023 en "
                              "comptant les 84 engagés de 2019 comme l'annuaire (lot 19, "
                              "Q112)")]),
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
         s.CATEGORIE, s.CODE_DOMAINE_ACT
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
  -- pas parti : fiche active, ou premier
  -- départ (02, 34, 35, 36, 52, 54) après
  AND   (p.ETAT_EMPLOYE = 'ACTIF'
         OR (SELECT MIN(d.DATE_VIGUEUR)
             FROM   USER_GRH.GRH_DECISION d
             WHERE  d.PERS_ID = p.PERS_ID
             AND    d.CDEC_CODE IN
                    ('02','34','35','36','52','54'))
            > DATE '2024-12-31'))
SELECT COUNT(*) FROM permanents_2024
-- 2024 : environ 742 (la requête complète
-- écarte aussi les décisions annulées par
-- une 40 et lit les décisions collectives)""",
    "3": """-- à coller sous la règle 2, à la place
-- de sa dernière ligne
SELECT CATEGORIE, COUNT(*)
FROM   permanents_2024
GROUP  BY CATEGORIE
ORDER  BY CATEGORIE
-- 2024 : 01 + 02 employés      173
--        03      maîtrise      207
--        04 + 05 cadres moyens 304
--        06      cadres sup.    58
-- 05 : selon la réponse à la question Q1""",
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
-- 115, 139, 155, 151, 72 ; personne de moins
-- de 20 ans ni de 60 ans et plus""",
    "5": """SELECT SEXE, COUNT(*)
FROM   permanents_2024
GROUP  BY SEXE
-- 2024 : M (hommes) 400, F (femmes) 342""",
    "6": """-- les contractuels au 31/12/2024
SELECT COUNT(*)
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
-- dernier engagement (50) après le
-- 30/06/2023 : moins de 18 mois
AND   (SELECT MAX(e.DATE_VIGUEUR)
       FROM   USER_GRH.GRH_DECISION e
       WHERE  e.PERS_ID = p.PERS_ID
       AND    e.CDEC_CODE = '50'
       AND    e.DATE_VIGUEUR <= DATE '2024-12-31')
      > DATE '2023-06-30'
-- et aucun départ après cet engagement
AND    NOT EXISTS (
  SELECT 1
  FROM   USER_GRH.GRH_DECISION f
  WHERE  f.PERS_ID = p.PERS_ID
  AND    f.CDEC_CODE IN
         ('02','34','35','36','52','54')
  AND    f.DATE_VIGUEUR <= DATE '2024-12-31'
  AND    f.DATE_VIGUEUR > (
           SELECT MAX(e.DATE_VIGUEUR)
           FROM   USER_GRH.GRH_DECISION e
           WHERE  e.PERS_ID = f.PERS_ID
           AND    e.CDEC_CODE = '50'))
-- 2024 : environ 46, comme l'annuaire""",
    "7": """-- ancienneté en années pleines au
-- 31/12/2024, depuis l'embauche (à
-- défaut, depuis la prise de service)
SELECT anciennete, COUNT(*)
FROM  (SELECT FLOOR((DATE '2024-12-31'
                - NVL(DATE_EMBAUCHE,
                      DATE_PRISE_SERVICE))
                / 365.25) AS anciennete
       FROM   permanents_2024)
GROUP  BY anciennete
ORDER  BY anciennete
-- puis regrouper : moins de 1, 1-4, 5-9…
-- 2024 : moins d'un an 101, 10-14 ans 224""",
    "8": """SELECT CODE_DOMAINE_ACT, COUNT(*)
FROM   permanents_2024
GROUP  BY CODE_DOMAINE_ACT
-- IMM immatriculation, REC recouvrement,
-- CONT contentieux, PRS prestations,
-- PREV prévention des risques,
-- ACT action sanitaire et sociale ;
-- PIL, CPT, AUT : autres domaines
-- libellés : USER_GRH.GRH_DOMAINE_ACTIVITE""",
    "9": """SELECT ROUND(100 * SUM(CASE
         WHEN CATEGORIE IN ('04','05','06')
         THEN 1 ELSE 0 END) / COUNT(*), 1)
         AS taux_encadrement
FROM   permanents_2024
-- 2024 : 48,8 % (l'annuaire : 49,2 %)
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
    dict(n="Q1", titre="La 5e catégorie",
         regle="Les agents de 5e catégorie sont-ils des cadres moyens ou des cadres supérieurs ?",
         cas=[("Cadres moyens (annuaire de la CNSS 2024)",
               "304 cadres moyens et 58 cadres supérieurs dans la base, pour 311 et 62 "
               "publiés."),
              ("Cadres supérieurs (Kaya 2017)",
               "220 cadres moyens et 142 cadres supérieurs : loin de l'annuaire.")],
         sources=[("Données", "lot 19, Q111"),
                  ("Proposition BI", "cadres moyens, comme l'annuaire")]),
    dict(n="Q2", titre="La durée d'un contrat temporaire",
         regle="La base ne garde pas la fin d'un contrat temporaire. Combien de temps dure un "
               "contrat ?",
         cas=[("18 mois", "46 contractuels fin 2024, comme l'annuaire de la CNSS."),
              ("12 mois", "44 contractuels.")],
         sources=[("Données", "lot 19, Q114"),
                  ("Proposition BI", "18 mois, à ajuster par le réglage duree_contrat")]),
    dict(n="Q3", titre="Les domaines de T59",
         regle="Le pilotage (PIL), la comptabilité (CPT) et AUT vont-ils dans « autres "
               "domaines » ? Le code ACT est-il l'action sanitaire et sociale ?",
         sources=[("Annuaire", "p. 17 : sept domaines"),
                  ("Données", "fin 2025 : PIL 431 actifs, ACT 50 dont 41 femmes (lot 8, Q47)"),
                  ("Proposition BI", "oui aux deux ; le lot 16 lira les libellés")]),
    dict(n="Q4", titre="L'agent en disponibilité",
         regle="Un agent mis en disponibilité au 31 décembre fait-il partie de l'effectif ?",
         cas=[("Oui (réglage actuel)", "il reste compté, comme dans les requêtes DTM."),
              ("Non (comme l'annuaire de la CNSS)",
               "il sort de l'effectif jusqu'à sa remise en activité (décision 18) ; T60 compte "
               "les deux mouvements.")],
         sources=[("Données", "2 à 5 mises en disponibilité par an chez les permanents (lot "
                              "20, Q117)"),
                  ("Proposition BI", "le réglage disponibilite")]),
    dict(n="Q5", titre="Les 84 engagés de 2019",
         regle="84 agents engagés en 2019 n'ont aucune date de départ dans la base. L'annuaire "
               "les compte jusqu'en 2023. Que sont-ils devenus, et quand sont-ils partis ?",
         sources=[("Données", "10 radiés dès 2019, 74 sans trace (lots 18 et 20, Q105, Q117)"),
                  ("Proposition BI", "laisser 2019 à 2023 tels que la base les donne : sans "
                                     "effet sur les exercices à partir de 2025")]),
]

BI_CHECKS = ("L'effectif permanent de 2024 retombe sur l'annuaire de la CNSS à 2 % près (742 pour "
             "758) ; les âges et l'ancienneté, à quelques agents près.\n"
             "La catégorie de la situation retombe à 7 agents près par catégorie, chaque case "
             "catégorie et sexe à 4 près ; la qualification, elle, ne retombe pas.\n"
             "Le taux d'encadrement retombe à 0,3 point près de 2020 à 2024.\n"
             "Les 46 contractuels publiés sont les temporaires sous contrat au 31/12.")

AGES_2024 = [("20-24", 12, 12), ("25-29", 28, 28), ("30-34", 70, 70), ("35-39", 117, 115),
             ("40-44", 141, 139), ("45-49", 157, 155), ("50-54", 154, 151), ("55-59", 79, 72)]


def build():
    d = Diagram(seed=58)
    inner = WIDTH - 50

    d.text(0, 0, "T58 · Le personnel au 31 décembre, par catégorie, âge et sexe", 48)
    d.text(0, 66, "« Effectif des travailleurs de l'organisme à la clôture de l'exercice par "
                  "catégorie professionnelle, âge et sexe » · fiche pilote de T58, T59, T61 et "
                  "T65 · ce que l'équipe BI a compris, à valider · état au 01/11/2026", 22,
           C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    text = wrap("Il dit combien de personnes la CNSS emploie au 31 décembre, par catégorie "
                "(employés, agents de maîtrise, cadres), par tranche d'âge et par sexe : le "
                "personnel permanent d'abord, les contractuels ensuite. Les tableaux T59 "
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
        "758 permanents au 31/12/2024 dans l'annuaire, 742 dans la base (−2 %) ; 46 "
        "contractuels des deux côtés. Par catégorie : 177, 208, 311, 62 publiés ; 173, 207, "
        "304, 58 dans la base.",
        "Permanents par tranche d'âge, en 2024 :", AGES_2024,
        "annuaire CNSS 2024, tableau 120 ; lots 17 et 19")

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
                                  "partie « contractuels » a la même forme (règle 6).", 18,
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
    d.text(45, bottom + 36, "Ce que l'équipe BI a vérifié dans les données (01/11/2026) : pas de "
                            "réponse attendue", 20, groups=g, inside=checks)
    d.text(45, bottom + 70, BI_CHECKS, 16, groups=g, inside=checks)
    sec["height"] = bottom + 20 + 170 + 30 - sec["y"]
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
