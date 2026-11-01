"""
Fiche de validation de T62 « Travailleurs en formation ou formés, par catégorie
professionnelle, sexe et finalité de la formation » (état au 01/11/2026).

Même gabarit que les fiches des prestations : la maquette, six agents fictifs sur un calendrier,
les lignes, huit règles avec leur SQL et cinq questions. Les chiffres de la base ne retombent pas
sur l'annuaire de la CNSS : la fiche le dit et le demande.

Sources : Annuaire CIPRES (p. 17, 51) ; atelier de Kaya 2017 (date de début de la formation) ;
annuaire de la CNSS 2024 (tableau 123) ; analyses/grh (lots 8, 9, 17 à 19, règle G10) ;
t62_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/grh/t62_formation.py [--force]
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fiche_grh import HOW_TO_DEBUT, OUTPUT_DIR, SORTIES, figures  # noqa: E402
from excalidraw_builder import C, Diagram, main  # noqa: E402
from fiche_cipres import (WIDTH, banner, cards_grid, cards_with_sql, chip, height_of,  # noqa: E402
                          line_cards, mockup, timeline, wrap)

OUTPUT = OUTPUT_DIR / "T62_personnel_forme.excalidraw"

HOW_TO = HOW_TO_DEBUT + (
    "Attention : les chiffres de la base ne retombent pas sur l'annuaire de la CNSS 2024 (409 "
    "formés pour 480). La question Q1 demande d'où viennent les chiffres publiés ; la règle 1 "
    "en dépend.")

EXAMPLES = [
    dict(nom="Agent A", detail="employé, une formation", debut=date(2024, 3, 4),
         fin=date(2024, 3, 15), sortie="compte", dates="formation du 04 au 15/03/2024",
         resultat="Compté une fois, en employés, sans promotion."),
    dict(nom="Agent B", detail="cadre, deux formations", debut=date(2024, 2, 5),
         fin=date(2024, 11, 29), sortie="compte", dates="une formation en février, une en novembre",
         resultat="Compté une fois (règle 1) ; ses deux inscriptions comptent dans le détail."),
    dict(nom="Agent C", detail="formation diplômante", debut=date(2024, 1, 8),
         fin=date(2024, 12, 20), sortie="compte", dates="diplôme obtenu, puis reclassé",
         resultat="Compté avec promotion : son classement a changé après la formation (règle "
                  "7)."),
    dict(nom="Agent D", detail="formation à cheval", debut=date(2023, 12, 4),
         fin=date(2024, 1, 26), sortie="exclu", dates="du 04/12/2023 au 26/01/2024",
         resultat="Compté en 2023, l'année du début (Kaya 2017) ; pas en 2024 (question Q5)."),
    dict(nom="Agent E", detail="contractuel", debut=date(2024, 5, 6),
         fin=date(2024, 5, 17), sortie="question", dates="formation en mai 2024",
         resultat="Pas compté : T62 compte le personnel permanent (question Q4)."),
    dict(nom="Agent F", detail="formé puis retraité", debut=date(2024, 4, 8),
         fin=date(2024, 4, 19), sortie="compte", dates="formation en avril, retraite en octobre",
         resultat="Compté : il était permanent quand il a été formé (règle 4)."),
]

LINES = [
    (1, "Employés", "catégories 1 et 2 de la grille", "132 publiés en 2024, 81 dans la base",
     "Données"),
    (2, "Agents de maîtrise", "catégorie 3", "142 publiés, 96 dans la base", "Données"),
    (3, "Cadres moyens", "catégories 4 et 5 (la 5e : question Q1 de la fiche T58)",
     "118 publiés, 181 dans la base", "Données"),
    (4, "Cadres supérieurs", "catégorie 6", "88 publiés, 51 dans la base", "Données"),
    (5, "TOTAL", "la somme des catégories, et le détail des inscriptions",
     "480 publiés, 409 dans la base", "Données"),
]

RULES = [
    dict(n="1", titre="Le formé",
         regle="Un agent inscrit à au moins une formation dans l'année, compté une fois, quel "
               "que soit le nombre de ses formations.",
         sources=[("Annuaire", "p. 17 : « travailleurs en formation ou formés »"),
                  ("Données", "en 2024 : 410 agents et 525 inscriptions, aucune en double (lot "
                              "18, Q108)"),
                  ("Proposition BI", "compter des agents, pas des inscriptions ; questions Q1 "
                                     "et Q3")]),
    dict(n="2", titre="L'année d'une formation",
         regle="L'année où la formation commence.",
         sources=[("Kaya 2017", "on retient la date de début de la formation"),
                  ("Données", "presque aucune formation ne chevauche le 31 décembre (lot 9, "
                              "Q67)")]),
    dict(n="3", titre="Toutes les formations",
         regle="Toutes les formations comptent, quel que soit leur statut.",
         sources=[("Données", "toutes les formations sont au statut N ; la requête DTM d'origine "
                              "ne gardait que le statut T et ne publiait rien (lot 8, Q50, "
                              "Q56)")]),
    dict(n="4", titre="Le personnel permanent",
         regle="Un agent de la CNSS de nature permanente au 31 décembre. Il n'a pas à être "
               "encore en poste : un agent formé puis retraité dans l'année compte.",
         sources=[("Données", "l'annuaire publie les permanents formés ; 409 des 410 agents "
                              "formés en 2024 sont permanents (lot 18, Q108)"),
                  ("Proposition BI", "les contractuels formés à part ; question Q4")]),
    dict(n="5", titre="La catégorie",
         regle="La catégorie de la situation de l'agent au 31 décembre, comme dans T58 : 1re et "
               "2e employés, 3e agents de maîtrise, 4e et 5e cadres moyens, 6e cadres "
               "supérieurs.",
         sources=[("Données", "81, 96, 181, 51 pour 132, 142, 118, 88 publiés : deux fois plus "
                              "de cadres moyens formés que dans l'annuaire (lot 19, Q113)"),
                  ("Proposition BI", "la même catégorie que T58")]),
    dict(n="6", titre="Le sexe",
         regle="Hommes et femmes d'après la fiche de l'agent.",
         sources=[("Base", "M et F")]),
    dict(n="7", titre="Avec ou sans promotion",
         regle="Avec promotion : le classement de l'agent a changé après une formation de "
               "l'année (classement avant et après, sur l'inscription). Sinon, sans promotion.",
         sources=[("Annuaire", "p. 51 : la finalité, « avec promotion » ou « sans promotion »"),
                  ("Données", "le classement n'est rempli que pour les formations diplômantes : "
                              "2 changements en 2024 pour 6 promotions publiées (lot 19, "
                              "Q113)"),
                  ("Proposition BI", "question Q2")]),
    dict(n="8", titre="Le Total et le détail",
         regle="Le Total est la somme des catégories. Le détail donne les inscriptions, et les "
               "formés en perfectionnement et en formation diplômante.",
         sources=[("Base", "catégories de formation : PERF perfectionnement, DIPLO diplômante "
                           "(lot 8)")]),
]

# Traduction en SQL de chaque règle : extraits de analyses/grh/t62_requete_proposee.sql
SQL = {
    "1": """formes AS (
  SELECT PERS_ID,
         MAX(promotion) AS promotion,
         COUNT(*)       AS nb_inscriptions
  FROM inscriptions
  GROUP BY PERS_ID)""",
    "2": """-- debut, fin : 01/01 et 31/12
WHERE f.DATE_DEBUT >= debut
  AND f.DATE_DEBUT <  fin + 1""",
    "3": """FROM USER_GRH.GRH_INSCRIPTION i
JOIN DTM.DIM_GRH_FORMATION f
  ON f.FORM_NO = i.FORM_NO
-- aucun filtre sur le statut""",
    "4": """-- si : dernière situation au 31/12
WHERE si.NATURE_EMPLOIE = 'PERMANENT'
  AND TO_CHAR(p.QUALITE) = 'A'""",
    "5": """CASE
  WHEN cat IN ('01', '02') THEN 1
  WHEN cat = '03' THEN 2
  WHEN cat = '04' THEN 3
  WHEN cat = '05' THEN  -- réglage
    CASE categorie_05
      WHEN 'SUPERIEUR' THEN 4 ELSE 3 END
  WHEN cat = '06' THEN 4
  ELSE 5
END AS rang""",
    "6": """SUM(CASE WHEN SEXE = 'M'
         THEN 1 ELSE 0 END) AS hommes,
SUM(CASE WHEN SEXE = 'F'
         THEN 1 ELSE 0 END) AS femmes""",
    "7": """CASE WHEN i.CLA_APRES IS NOT NULL
      AND i.CLA_AVANT IS NOT NULL
      AND TO_CHAR(i.CLA_APRES)
          <> TO_CHAR(i.CLA_AVANT)
     THEN 1 ELSE 0 END AS promotion""",
    "8": """-- ligne TOTAL
SELECT 9, SUM(avec_promotion_h), …,
       SUM(total), SUM(inscriptions)
FROM comptes""",
}

SQL_NOTE = ("À droite de chaque règle, sa traduction en SQL, pour la DSI et l'équipe BI : un "
            "extrait de la requête de T62 (analyses/grh/t62_requete_proposee.sql). Elle lit les "
            "formations de l'entrepôt (DTM.DIM_GRH_FORMATION) et les inscriptions dans la base "
            "de l'application, comme la requête DTM actuelle ; les agents viennent des copies de "
            "l'entrepôt (DWH.FAIT_GRH_…) du cliché choisi.")

QUESTIONS = [
    dict(n="Q1", titre="D'où viennent les 480 formés ?",
         regle="L'annuaire de la CNSS publie 480 permanents formés en 2024 ; la base en compte "
               "409 (524 inscriptions), avec une autre répartition par catégorie. D'où viennent "
               "les chiffres publiés ?",
         cas=[("Du module formation de l'application",
               "il faut trouver ce qui manque dans la base (formations non saisies ?)."),
              ("D'un autre registre (plan de formation…)",
               "T62 devra le lire, ou l'application devra être complétée.")],
         sources=[("Données", "lots 17 à 19, Q101, Q108, Q113"),
                  ("Proposition BI", "les agents distincts de la base, en attendant")]),
    dict(n="Q2", titre="La promotion",
         regle="Une formation « avec promotion » est-elle une formation suivie d'un changement "
               "de classement ? Où ce changement est-il saisi ?",
         sources=[("Données", "le classement avant et après n'est rempli que pour les formations "
                              "diplômantes (lot 9, Q64)"),
                  ("Proposition BI", "le classement de l'inscription")]),
    dict(n="Q3", titre="Agents ou inscriptions",
         regle="Un agent formé deux fois dans l'année compte-t-il une ou deux fois ?",
         cas=[("Une fois (proposition)", "409 permanents formés en 2024."),
              ("Deux fois", "524 inscriptions.")],
         sources=[("Annuaire", "p. 17 : on compte des « travailleurs »")]),
    dict(n="Q4", titre="Les contractuels formés",
         regle="Faut-il compter aussi les contractuels formés, comme pour T58 ?",
         cas=[("Non (proposition, comme l'annuaire)", "les permanents seulement."),
              ("Oui", "un agent de plus en 2024.")],
         sources=[("Données", "lot 18, Q108")]),
    dict(n="Q5", titre="Une formation à cheval sur deux années",
         regle="Une formation commencée en décembre et finie en janvier compte-t-elle l'année "
               "de son début ?",
         cas=[("L'année du début (Kaya 2017)", "la proposition."),
              ("L'année de la fin", "la règle de la requête DTM actuelle.")],
         sources=[("Données", "presque aucune formation n'est dans ce cas (lot 9, Q67)")]),
]

BI_CHECKS = ("En 2024, la base compte 410 agents formés (409 permanents) et 525 inscriptions ; "
             "aucune inscription en double.\n"
             "Par catégorie, la base a deux fois plus de cadres moyens formés que l'annuaire, et "
             "moins d'employés et d'agents de maîtrise : les chiffres publiés viennent sans doute "
             "d'un autre registre.\n"
             "Toutes les formations ont le statut N : la requête DTM d'origine, qui ne gardait "
             "que le statut T, ne publiait rien.\n"
             "La dépense de formation de T64 se calcule déjà (frais des inscriptions) ; la masse "
             "salariale et le budget manquent.")

FORMES_2024 = [("Employés", 132, 81), ("Maîtrise", 142, 96), ("Cadres moyens", 118, 181),
               ("Cadres sup.", 88, 51)]


def build():
    d = Diagram(seed=62)
    inner = WIDTH - 50

    d.text(0, 0, "T62 · Le personnel permanent formé dans l'année", 48)
    d.text(0, 66, "« Travailleurs en formation ou formés, par catégorie professionnelle, sexe et "
                  "finalité de la formation » · ce que l'équipe BI a compris, à valider · état "
                  "au 01/11/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    text = wrap("Il dit combien d'agents permanents ont suivi une formation dans l'année, par "
                "catégorie et par sexe, et combien ont été promus après leur formation. Avec la "
                "dépense de formation de T64, il mesure l'effort de formation de la CNSS.", 18,
                720)
    about_h = 54 + height_of(text, 18) + 16 + 2 * 36 + 10
    about = d.box(25, top, 760, about_h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T62", 22, groups=g, inside=about)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 17, maquette p. 51"),
                      ("Données", "annuaire de la CNSS 2024, tableau 123")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(
        d, 25, top + about_h + 20, 760, "En 2024 : la base face à l'annuaire de la CNSS",
        "480 permanents formés dans l'annuaire, dont 6 avec promotion ; la base compte 409 "
        "agents permanents formés et 524 inscriptions, dont 2 avec un classement changé.",
        "Permanents formés en 2024, par catégorie :", FORMES_2024,
        "annuaire CNSS 2024, tableau 123 ; lot 19, Q113")

    bottom = mockup(
        d, 830, top, 300, (WIDTH - 25 - 830 - 300) / 7, "Catégorie", "5",
        columns=[("Hommes", "6"), ("Femmes", "6"), ("Hommes", "6"), ("Femmes", "6"),
                 ("Hommes", "6"), ("Femmes", "6"), ("Ensemble", "1")],
        groups=[(0, 1, "AVEC PROMOTION", "7"), (2, 3, "SANS PROMOTION", "7"),
                (4, 6, "TOTAL", "8")],
        rows=[("Employés", None, None), ("Agents de maîtrise", None, None),
              ("Cadres moyens", None, None), ("Cadres supérieurs", None, None),
              ("TOTAL", None, "8")],
        row_h=54)
    d.text(830, bottom + 14, wrap("Les cases comptent les agents permanents formés dans l'année "
                                  "(règles 1 à 4), une fois chacun, par catégorie (règle 5) et "
                                  "par sexe (règle 6), selon qu'une formation a été suivie d'une "
                                  "promotion (règle 7).", 18, WIDTH - 25 - 830), 18, C["doux"])
    sec["height"] = max(bottom + 110, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Qui est compté dans T62 de 2024 : six agents fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2023, 11, 1), date(2025, 1, 31), 2024,
                      EXAMPLES, outcomes=SORTIES,
                      axis_label="Dans le calendrier : la formation, du début à la fin")
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes : les catégories")
    w = chip(d, 25, y + 72, "Annuaire")
    d.text(25 + w + 12, y + 76, "Employés, agents de maîtrise, cadres ; comme dans T58, la CNSS "
                                "sépare les cadres moyens et les cadres supérieurs.", 18)
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
