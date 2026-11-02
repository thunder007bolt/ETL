"""
Fiche de validation de T60 « Mouvement du personnel au cours des 5 dernières années » (état au
01/11/2026).

Même gabarit que les fiches des prestations : la maquette, six agents fictifs sur un calendrier,
les colonnes, huit règles avec leur SQL et cinq questions.

Sources : Annuaire CIPRES (p. 17, 51) ; atelier de Kaya 2017 (décès, prise de service) ;
annuaire de la CNSS 2024 (tableau 121) ; analyses/grh (lots 7, 17 à 20, règles G1, G2, G8, G9) ;
t60_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/grh/t60_mouvement.py [--force]
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fiche_grh import HOW_TO_DEBUT, OUTPUT_DIR, figures  # noqa: E402
from excalidraw_builder import C, Diagram, main  # noqa: E402
from fiche_cipres import (WIDTH, banner, cards_grid, cards_with_sql, chip, height_of,  # noqa: E402
                          line_cards, mockup, timeline, wrap)

OUTPUT = OUTPUT_DIR / "T60_mouvement_personnel.excalidraw"

HOW_TO = HOW_TO_DEBUT + (
    "T60 suit l'effectif permanent de T58 d'une fin d'année à l'autre : début + entrées − "
    "sorties = fin. Il remplace la requête DTM actuelle, qui lit une table sans recrutements ni "
    "départs.")

# ce qu'un agent fictif donne dans T60 de 2024
MOUVEMENTS = {
    "entree": (C["vert_moyen"], C["vert"], "une entrée"),
    "sortie": (C["rouge_moyen"], C["rouge"], "une sortie"),
    "aucun": (C["gris"], C["gris_bord"], "ni entrée ni sortie"),
    "question": (C["jaune"], C["jaune_bord"], "selon la réponse à une question"),
}

EXAMPLES = [
    dict(nom="Agent A", detail="recruté en 2024", debut=date(2024, 3, 1),
         fin=date(2025, 3, 31), sortie="entree", dates="embauché le 01/03/2024",
         resultat="Entrée « recrutement » en 2024 : absent au 31/12/2023, présent au 31/12/2024, "
                  "embauché dans l'année."),
    dict(nom="Agent B", detail="retraité", debut=date(2023, 10, 1),
         fin=date(2024, 7, 31), sortie="sortie", dates="retraite le 31/07/2024 (décision 02)",
         resultat="Sortie « retraite » en 2024 : présent au 31/12/2023, parti avant le "
                  "31/12/2024."),
    dict(nom="Agent C", detail="décédé en février", debut=date(2023, 10, 1),
         fin=date(2024, 10, 15), sortie="question",
         dates="décès le 10/02, liquidation des droits le 15/10/2024",
         resultat="Sortie « décès » en 2024, datée par la liquidation des droits : la base n'a "
                  "plus la date du décès. Un décès de fin d'année glisse sur l'année suivante "
                  "(question Q1)."),
    dict(nom="Agent D", detail="mis en disponibilité", debut=date(2023, 10, 1),
         fin=date(2025, 3, 31), sortie="question",
         dates="disponibilité le 01/06/2024 (07), remise le 01/02/2025 (18)",
         resultat="Aujourd'hui : ni sortie ni entrée. Comme l'annuaire : sortie « disponibilité » "
                  "en 2024, entrée « remise en activité » en 2025 (question Q2)."),
    dict(nom="Agent E", detail="temporaire devenu permanent", debut=date(2024, 7, 1),
         fin=date(2025, 3, 31), sortie="entree", dates="permanent depuis le 01/07/2024",
         resultat="Entrée « autre » en 2024 : il entre dans l'effectif permanent sans être "
                  "recruté dans l'année."),
    dict(nom="Agent F", detail="recruté et parti en 2024", debut=date(2024, 2, 1),
         fin=date(2024, 9, 30), sortie="aucun", dates="embauché le 01/02, démission le 30/09",
         resultat="Ni entrée ni sortie : absent au 31/12/2023 comme au 31/12/2024 (question "
                  "Q4)."),
]

LINES = [
    (1, "Effectif au début et à la fin",
     "les permanents présents au 31/12 de l'année d'avant, puis de l'année : ceux de T58",
     "de 2020 à 2023, la base compte 84 agents de moins que l'annuaire (question Q3)", "Données"),
    (2, "Entrées",
     "recrutement : embauché dans l'année ; autres : remise en activité, temporaire devenu "
     "permanent",
     "104 recrutements en 2024 dans la base comme dans l'annuaire", "Données"),
    (3, "Sorties",
     "retraite, décès ; autres : démission, licenciement, fin de contrat, mise en disponibilité",
     "le tableau 121 de l'annuaire de la CNSS détaille les « autres » ; la requête aussi",
     "Données"),
    (4, "Le contrôle",
     "début + entrées − sorties = fin, chaque année",
     "toujours vrai : entrées et sorties se lisent d'une fin d'année à l'autre",
     "Proposition BI"),
]

RULES = [
    dict(n="1", titre="L'effectif au début et à la fin",
         regle="Les permanents présents au 31 décembre, comptés comme dans T58. L'effectif au "
               "début d'une année est celui du 31 décembre de l'année d'avant.",
         sources=[("Annuaire", "p. 51 : effectif en début et en fin d'année"),
                  ("Données", "2024 : 672 au début et 742 à la fin dans la base ; 767 et 758 "
                              "publiés. L'écart du début vient des 84 engagés de 2019 (lot 17, "
                              "Q99)")]),
    dict(n="2", titre="Entrées et sorties, d'une fin d'année à l'autre",
         regle="Une entrée est un permanent présent à la fin de l'année et absent au début ; une "
               "sortie, l'inverse. Ainsi début + entrées − sorties = fin, toujours.",
         sources=[("Base", "la table des mouvements n'a ni recrutement ni départ : la requête "
                           "DTM actuelle ne peut pas servir (lot 1)"),
                  ("Proposition BI", "un agent entré et sorti la même année n'apparaît pas ; "
                                     "question Q4")]),
    dict(n="3", titre="Le recrutement",
         regle="Une entrée d'un agent embauché dans l'année (date d'embauche, à défaut prise de "
               "service).",
         sources=[("Annuaire", "p. 51 : « recrutement »"),
                  ("Kaya 2017", "la note de prise de service vaut en attendant la décision "
                                "d'engagement"),
                  ("Données", "104 en 2024, comme l'annuaire ; aucun de 2020 à 2023 (lot 17, "
                              "Q99)")]),
    dict(n="4", titre="Les autres entrées",
         regle="Une remise en activité après une disponibilité (décision 18), un temporaire "
               "devenu permanent, un retour sans décision.",
         sources=[("Données", "décisions 18 : 4, 3, 6, 5, 4 de 2020 à 2024, pour 3, 2, 3, 5, 6 "
                              "remises publiées (lot 20, Q117)"),
                  ("Proposition BI", "la remise compte selon le réglage disponibilite ; "
                                     "question Q2")]),
    dict(n="5", titre="La retraite",
         regle="Une sortie dont la première décision de départ, dans l'année, est l'admission à "
               "la retraite (décision 02), à sa date de vigueur.",
         sources=[("Base", "décision 02 « admission à la retraite »"),
                  ("Données", "39, 31, 29, 30, 25 de 2020 à 2024, pour 43, 31, 26, 32, 23 "
                              "publiées ; compter une retraite de décembre l'année suivante "
                              "ferait moins bien (lot 19, Q116)")]),
    dict(n="6", titre="Le décès",
         regle="Une sortie par décès, à la date du décès ; à défaut, à la date de la liquidation "
               "des droits (décision 34).",
         sources=[("Kaya 2017", "le communiqué nécrologique vaut en attendant l'acte de décès"),
                  ("Données", "aucune date de décès saisie depuis 2010 ; la liquidation vient "
                              "8,5 mois après le décès en moyenne (lot 7, Q42) : 19 sorties de "
                              "2020 à 2024 pour 17 décès publiés (lot 18, Q109)"),
                  ("Proposition BI", "la liquidation des droits ; question Q1")]),
    dict(n="7", titre="Les autres sorties",
         regle="Démission (36), licenciement (54), fin de contrat (35, 52), mise en "
               "disponibilité (décision 07, selon le réglage), permanent devenu temporaire.",
         sources=[("Données", "démissions exactes (1 en 2023, 2 en 2024), licenciements à 1 "
                              "près ; décisions 07 : 2, 4, 2, 5, 5 pour 5, 3, 2, 4, 7 "
                              "disponibilités publiées (lot 20, Q117)"),
                  ("Proposition BI", "les détachements restent introuvables ; question Q5")]),
    dict(n="8", titre="Cinq exercices et le contrôle",
         regle="Une ligne par exercice, des quatre années d'avant à l'année choisie. Une "
               "colonne de contrôle vérifie début + entrées − sorties − fin = 0.",
         sources=[("Annuaire", "p. 51 : « au cours des 5 dernières années »")]),
]

# Pour chaque règle, une requête courte sur les vraies tables de l'application (USER_GRH), pour
# l'année 2024, avec en commentaire le chiffre à retrouver. La règle 1 construit les listes
# permanents_2023 et permanents_2024 (règle 2 de T58 aux deux dates) ; la règle 5, sorties_2024.
SQL = {
    "1": """-- les permanents présents au 31/12/2023
-- et au 31/12/2024 : la règle 2 de T58,
-- aux deux dates, réutilisée ensuite
WITH fins AS (
  SELECT DATE '2023-12-31' AS fin FROM DUAL
  UNION ALL
  SELECT DATE '2024-12-31' FROM DUAL),
permanents AS (
  SELECT EXTRACT(YEAR FROM f.fin) AS annee,
         p.PERS_ID, p.DATE_EMBAUCHE,
         p.DATE_PRISE_SERVICE
  FROM   fins f
  CROSS  JOIN USER_GRH.GRH_PERSONNE p
  JOIN   USER_GRH.GRH_SITUATION s
         ON s.PERS_ID = p.PERS_ID
  WHERE  p.QUALITE = 'A'
  AND    s.NATURE_EMPLOIE = 'PERMANENT'
  AND    s.DATE_DEBUT = (
           SELECT MAX(x.DATE_DEBUT)
           FROM   USER_GRH.GRH_SITUATION x
           WHERE  x.PERS_ID = p.PERS_ID
           AND    x.DATE_DEBUT <= f.fin)
  AND    NVL(p.DATE_EMBAUCHE,
             p.DATE_PRISE_SERVICE) <= f.fin
  AND   (p.ETAT_EMPLOYE = 'ACTIF'
         OR (SELECT MIN(d.DATE_VIGUEUR)
             FROM   USER_GRH.GRH_DECISION d
             WHERE  d.PERS_ID = p.PERS_ID
             AND    d.CDEC_CODE IN
                    ('02','34','35','36','52','54'))
            > f.fin)),
permanents_2023 AS (
  SELECT * FROM permanents WHERE annee = 2023),
permanents_2024 AS (
  SELECT * FROM permanents WHERE annee = 2024)
SELECT annee, COUNT(*)
FROM   permanents
GROUP  BY annee
-- 2023 (début de 2024) : 672 ; 2024 : 742
-- (l'annuaire : 767 et 758 ; l'écart du
-- début vient des 84 engagés de 2019)""",
    "2": """-- entrée : dans la liste de fin, pas
-- dans celle du début ; sortie : l'inverse
SELECT
  (SELECT COUNT(*) FROM permanents_2024
   WHERE  PERS_ID NOT IN
          (SELECT PERS_ID
           FROM   permanents_2023)) AS entrees,
  (SELECT COUNT(*) FROM permanents_2023
   WHERE  PERS_ID NOT IN
          (SELECT PERS_ID
           FROM   permanents_2024)) AS sorties
FROM   DUAL
-- 2024 : entrées − sorties = 742 − 672 = 70""",
    "3": """-- une entrée embauchée en 2024 (à défaut
-- d'embauche, la prise de service)
SELECT COUNT(*)
FROM   permanents_2024
WHERE  PERS_ID NOT IN
       (SELECT PERS_ID FROM permanents_2023)
AND    NVL(DATE_EMBAUCHE, DATE_PRISE_SERVICE)
         >= DATE '2024-01-01'
-- 2024 : 104, comme l'annuaire""",
    "4": """-- les entrées sans embauche en 2024
SELECT COUNT(*)
FROM   permanents_2024
WHERE  PERS_ID NOT IN
       (SELECT PERS_ID FROM permanents_2023)
AND    NVL(DATE_EMBAUCHE, DATE_PRISE_SERVICE)
         < DATE '2024-01-01'
-- à part : les remises en activité (18)
SELECT COUNT(DISTINCT PERS_ID)
FROM   USER_GRH.GRH_DECISION
WHERE  CDEC_CODE = '18'
AND    DATE_VIGUEUR BETWEEN DATE '2024-01-01'
                        AND DATE '2024-12-31'
-- 2024 : 4 décisions 18 (6 remises publiées)""",
    "5": """-- sorties_2024 : la première décision de
-- départ des présents du 31/12/2023,
-- quand elle tombe en 2024 ; à ajouter
-- à la suite du WITH de la règle 1
sorties_2024 AS (
  SELECT d.PERS_ID, d.CDEC_CODE
  FROM   permanents_2023 p
  JOIN   USER_GRH.GRH_DECISION d
         ON d.PERS_ID = p.PERS_ID
  WHERE  d.CDEC_CODE IN
         ('02','34','35','36','52','54')
  AND    d.DATE_VIGUEUR = (
           SELECT MIN(x.DATE_VIGUEUR)
           FROM   USER_GRH.GRH_DECISION x
           WHERE  x.PERS_ID = d.PERS_ID
           AND    x.CDEC_CODE IN
                  ('02','34','35','36','52','54'))
  AND    d.DATE_VIGUEUR
         BETWEEN DATE '2024-01-01'
             AND DATE '2024-12-31')
SELECT COUNT(*) FROM sorties_2024
WHERE  CDEC_CODE = '02'  -- retraite
-- 2024 : 25 (l'annuaire : 23)""",
    "6": """-- même liste sorties_2024 (règle 5) ;
-- 34 : liquidation des droits après décès
SELECT COUNT(*) FROM sorties_2024
WHERE  CDEC_CODE = '34'
-- 2024 : 5 (l'annuaire : 3 décès)
-- GRH_PERSONNE.DATE_DECES est vide pour
-- tous les décès depuis 2010""",
    "7": """-- même liste sorties_2024 (règle 5)
SELECT CDEC_CODE, COUNT(*)
FROM   sorties_2024
WHERE  CDEC_CODE IN ('35','36','52','54')
GROUP  BY CDEC_CODE
-- 36 démission, 54 licenciement,
-- 35 et 52 fin de contrat
-- 2024 : 36 = 2 ; 54 = 1 (comme l'annuaire)
-- à part : les mises en disponibilité (07)
SELECT COUNT(DISTINCT PERS_ID)
FROM   USER_GRH.GRH_DECISION
WHERE  CDEC_CODE = '07'
AND    DATE_VIGUEUR BETWEEN DATE '2024-01-01'
                        AND DATE '2024-12-31'
-- 2024 : 5 (l'annuaire : 7)""",
    "8": """-- début + entrées − sorties − fin
SELECT (SELECT COUNT(*) FROM permanents_2023)
     + (SELECT COUNT(*) FROM permanents_2024
        WHERE  PERS_ID NOT IN
               (SELECT PERS_ID
                FROM   permanents_2023))
     - (SELECT COUNT(*) FROM permanents_2023
        WHERE  PERS_ID NOT IN
               (SELECT PERS_ID
                FROM   permanents_2024))
     - (SELECT COUNT(*) FROM permanents_2024)
       AS controle
FROM   DUAL
-- toujours 0 ; les cinq exercices : refaire
-- avec 2019 et 2020, … , 2023 et 2024""",
}

SQL_NOTE = ("À droite de chaque règle, une requête courte sur les vraies tables de l'application "
            "(schéma USER_GRH), pour l'année 2024, à copier dans SQL Developer. La règle 1 "
            "construit les permanents présents au 31/12/2023 et au 31/12/2024 (la règle 2 de la "
            "fiche T58, aux deux dates) : les listes permanents_2023 et permanents_2024. La "
            "règle 5 y ajoute la liste sorties_2024, que les règles 6 et 7 réutilisent. Pour les "
            "autres règles, on colle leur requête sous ce WITH, à la place de sa dernière "
            "requête. Le commentaire en fin de requête donne le chiffre à retrouver. La "
            "requête complète, sur les copies de l'entrepôt, est dans "
            "analyses/grh/t60_requete_proposee.sql.")

QUESTIONS = [
    dict(n="Q1", titre="La date d'un décès",
         regle="La base n'a plus de date de décès depuis 2010. Peut-on la saisir, ou celle du "
               "communiqué nécrologique ?",
         cas=[("Si la date est saisie",
               "le décès compte l'année où il a lieu, comme dans l'annuaire."),
              ("Sinon",
               "il compte à la liquidation des droits, environ 8 mois plus tard ; un décès de "
               "fin d'année glisse sur l'année suivante.")],
         sources=[("Kaya 2017", "le communiqué nécrologique vaut en attendant l'acte"),
                  ("Données", "lot 7, Q42 ; lot 18, Q109")]),
    dict(n="Q2", titre="La disponibilité",
         regle="Une mise en disponibilité est-elle une sortie, et la remise en activité une "
               "entrée ?",
         cas=[("Non (réglage actuel)",
               "l'agent reste dans l'effectif ; T60 ne montre pas les disponibilités."),
              ("Oui (comme l'annuaire de la CNSS)",
               "2 à 5 sorties « disponibilité » et 3 à 6 entrées « remise en activité » par "
               "an ; l'agent sort aussi de T58.")],
         sources=[("Données", "lot 20, Q117"),
                  ("Proposition BI", "le réglage disponibilite")]),
    dict(n="Q3", titre="Les 84 engagés de 2019",
         regle="L'annuaire compte 84 « autres entrées » en 2019 et 84 « autres sorties » en "
               "2024. La base ne date pas leur départ. Que s'est-il passé ?",
         sources=[("Données", "84 recrutés en 2019, jamais présents en fin d'année ; 10 radiés "
                              "dès 2019 (lots 17 et 20, Q99, Q117)"),
                  ("Proposition BI", "laisser 2019 à 2023 tels que la base les donne : sans "
                                     "effet à partir de 2025")]),
    dict(n="Q4", titre="Entré et sorti la même année",
         regle="Un agent recruté puis parti dans la même année n'apparaît ni en entrée ni en "
               "sortie. Faut-il le compter dans les deux ?",
         cas=[("Non (proposition)", "le calcul reste simple : début + entrées − sorties = fin."),
              ("Oui", "il faut l'ajouter à part, en entrée et en sortie.")],
         sources=[("Proposition BI", "cas rare chez les permanents")]),
    dict(n="Q5", titre="Les détachements",
         regle="L'annuaire compte 2 détachements et 2 fins de détachement en 2023, 1 "
               "détachement en 2024. Où sont-ils saisis ?",
         sources=[("Données", "aucune décision d'engagement de détaché (37) chez les "
                              "permanents (lot 20, Q117)"),
                  ("Proposition BI", "en attendant, ils tombent dans les autres entrées ou "
                                     "sorties")]),
]

BI_CHECKS = ("Les recrutements de 2024 retombent exactement sur l'annuaire de la CNSS (104) ; "
             "les retraites à 4 près par an ; les démissions exactement ; les licenciements à 1 "
             "près.\n"
             "Les mises en disponibilité (07) et les remises en activité (18) retombent à 1 à 3 "
             "près par an.\n"
             "L'effectif du début est sous l'annuaire de 84 agents de 2020 à 2023 : les engagés "
             "de 2019 sans date de départ.\n"
             "La requête DTM actuelle lit une table sans recrutements ni départs : elle ne peut "
             "pas servir.")

RETRAITES = [("2020", 43, 39), ("2021", 31, 31), ("2022", 26, 29), ("2023", 32, 30),
             ("2024", 23, 25)]


def build():
    d = Diagram(seed=60)
    inner = WIDTH - 50

    d.text(0, 0, "T60 · Le mouvement du personnel permanent, sur cinq ans", 48)
    d.text(0, 66, "« Mouvement du personnel au cours des 5 dernières années » · ce que l'équipe BI "
                  "a compris, à valider · état au 01/11/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    text = wrap("Il suit le personnel permanent d'une année à l'autre : combien au début, combien "
                "sont entrés (recrutés ou revenus), combien sont sortis (retraite, décès, "
                "autres), combien à la fin. Cinq années côte à côte montrent les vagues de "
                "recrutement et de départs.", 18, 720)
    about_h = 54 + height_of(text, 18) + 16 + 2 * 36 + 10
    about = d.box(25, top, 760, about_h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T60", 22, groups=g, inside=about)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 17, maquette p. 51"),
                      ("Données", "annuaire de la CNSS 2024, tableau 121")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(
        d, 25, top + about_h + 20, 760, "De 2020 à 2024 : la base face à l'annuaire de la CNSS",
        "2024 : 104 recrutements des deux côtés ; 767 au début et 758 à la fin dans l'annuaire, "
        "672 et 742 dans la base, qui n'a pas les 84 engagés de 2019.",
        "Départs à la retraite, de 2020 à 2024 :", RETRAITES,
        "annuaire CNSS 2024, tableau 121 ; lots 17 et 19")

    bottom = mockup(
        d, 830, top, 220, (WIDTH - 25 - 830 - 220) / 7, "Exercice", "8",
        columns=[("Effectif au début", "1"), ("Recrutement", "3"), ("Autres", "4"),
                 ("Retraite", "5"), ("Décès", "6"), ("Autres", "7"), ("Effectif à la fin", "1")],
        groups=[(1, 2, "ENTRÉES", "2"), (3, 5, "SORTIES", "2")],
        # « Année » : une ligne tout en capitales serait grisée comme un TOTAL
        rows=[("Année N − 4", None, None), ("Année N − 3", None, None),
              ("Année N − 2", None, None), ("Année N − 1", None, None),
              ("Année N", None, None)],
        row_h=54)
    d.text(830, bottom + 14, wrap("Chaque ligne est un exercice ; N est l'année choisie. Le "
                                  "début d'une ligne est la fin de la ligne d'avant (règle 1). "
                                  "Le détail des « autres » (démissions, licenciements, "
                                  "disponibilités…) suit dans la requête, comme dans le tableau "
                                  "121 de l'annuaire de la CNSS.", 18, WIDTH - 25 - 830), 18,
           C["doux"])
    sec["height"] = max(bottom + 110, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Ce que donne chaque agent dans T60 de 2024 : six agents "
                                    "fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2023, 10, 1), date(2025, 3, 31), 2024,
                      EXAMPLES, outcomes=MOUVEMENTS,
                      axis_label="Dans le calendrier : l'agent permanent, en poste")
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. colonnes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les colonnes")
    w = chip(d, 25, y + 72, "Annuaire")
    d.text(25 + w + 12, y + 76, "Effectif au début, entrées (recrutement, autres), sorties "
                                "(retraite, décès, autres), effectif à la fin.", 18)
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
