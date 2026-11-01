"""
Fiche de validation de T41 « Répartition des allocataires de la branche PF et montants par
tranches d'âges et par sexe » (état au 01/11/2026).

Même gabarit que T32 (fiche pilote de T32 à T41) : la maquette, six allocataires fictifs sur un
calendrier, les tranches d'âge, les huit règles avec leur SQL et cinq questions. T41 suit les
règles de T32, sauf trois points : on compte des dossiers d'allocataires, un enfant ne compte que
s'il a été payé dans l'année, et l'âge et le sexe sont ceux de l'allocataire.

Sources : Annuaire CIPRES (p. 14, 41, 62) ; atelier de Kaya 2017 (T41 : le dossier de
l'allocataire ; un tableau des enfants à prévoir) ; annuaire de la CNSS 2024 (tableaux 36, 38) ;
analyses/prestations (lot 5, P74 à P80 ; lot 6, P79 à P81) ; t32_t41_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/prestations/t41_allocataires.py [--force]
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

OUTPUT = ROOT / "schemas" / "prestations" / "tableaux cipres" / "T41_allocataires.excalidraw"

HOW_TO = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix dans une "
          "case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la carte si "
          "besoin. Enregistre avec Cmd+S : chaque version est gardée dans l'historique.\n"
          "T41 suit les règles de la fiche pilote T32, sauf trois points : on compte des dossiers "
          "d'allocataires, un enfant ne compte que s'il a été payé dans l'année, et l'âge et le "
          "sexe sont ceux de l'allocataire, pas des enfants.")

# sort d'un allocataire fictif dans le calendrier : compté ou non au 31/12
SORTIES = {
    "compte": (C["vert_moyen"], C["vert"], "compté au 31/12"),
    "exclu": (C["gris"], C["gris_bord"], "pas compté au 31/12"),
    "question": (C["jaune"], C["jaune_bord"], "selon la réponse à une question"),
}

EXAMPLES = [
    dict(nom="Allocataire A", detail="trois enfants de 4, 9 et 13 ans", debut=date(2024, 11, 1),
         fin=date(2026, 2, 28), sortie="compte",
         dates="allocations en cours toute l'année, payées en 2025",
         resultat="Dans T41 de 2025 : compté une fois, dans sa tranche d'âge ; montant : les "
                  "allocations de ses trois enfants payées en 2025."),
    dict(nom="Allocataire B", detail="un seul enfant, 22 ans en 2025", debut=date(2024, 11, 1),
         fin=date(2026, 2, 28), sortie="exclu",
         dates="allocation encore en cours dans la base ; enfant né en 2003",
         resultat="Dans T41 de 2025 : pas compté, son enfant a passé 21 ans (règle 2)."),
    dict(nom="Allocataire C", detail="enfants pas payés en 2025", debut=date(2024, 11, 1),
         fin=date(2026, 2, 28), sortie="exclu",
         dates="allocations en cours, aucun paiement en 2025 (certificat attendu)",
         resultat="Dans T41 de 2025 : pas compté, comme dans l'annuaire de la CNSS (règle 2, "
                  "question Q1)."),
    dict(nom="Allocataire D", detail="nouvel allocataire", debut=date(2025, 4, 1),
         fin=date(2026, 2, 28), sortie="compte",
         dates="premier enfant né en mars 2025, payé depuis avril",
         resultat="Dans T41 de 2025 : compté ; montant : les mois payés depuis avril."),
    dict(nom="Allocataire E", detail="allocations suspendues", debut=date(2024, 11, 1),
         fin=date(2025, 10, 31), sortie="exclu",
         dates="suspendues le 31/10/2025, pas reprises",
         resultat="Dans T41 de 2025 : pas compté, plus rien en cours au 31/12 ; ses paiements de "
                  "janvier à octobre sont hors du tableau (question Q3)."),
    dict(nom="Allocataire F", detail="seulement une allocation prénatale", debut=date(2025, 6, 1),
         fin=date(2025, 11, 30), sortie="question",
         dates="grossesse déclarée en juin 2025, pas encore d'enfant en allocations familiales",
         resultat="Dans T41 de 2025 : compté seulement si les allocations prénatales y entrent "
                  "(question Q4)."),
]

LINES = [
    (1, "Moins de 20 ans", "les allocataires très jeunes",
     "la première tranche de la maquette", "Annuaire"),
    (2, "De 20 à 79 ans", "douze tranches de 5 ans : 20-24, 25-29… 75-79",
     "l'âge de l'allocataire, titulaire du dossier, et non celui des enfants ; voir la "
     "question Q2", "Proposition BI"),
    (3, "80 ans et plus", "les allocataires les plus âgés",
     "une naissance en 1900 est une date par défaut : elle va en « âge inconnu » (P80)",
     "Données"),
    (4, "TOTAL", "la somme des tranches, hommes, femmes et ensemble",
     "avec l'allocation moyenne : montant ÷ effectif", "Annuaire"),
]

RULES = [
    dict(n="1", titre="Les allocations concernées",
         regle="Les allocations familiales versées pour les enfants. Les allocations "
               "prénatales (T55) et la maternité (T54) ont leurs propres tableaux.",
         sources=[("Annuaire", "p. 14 : T41 répartit « les bénéficiaires d'allocations » de la "
                               "branche PF"),
                  ("Base", "un code : FAF, allocations familiales ; FAP et MAP sont les "
                           "allocations prénatales"),
                  ("Proposition BI", "les allocations familiales seulement ; voir la question "
                                     "Q4")]),
    dict(n="2", titre="L'enfant compté au 31 décembre",
         regle="Une allocation en cours le 31 décembre (effet avant, ni radiée ni suspendue), "
               "pour un enfant en vie de 21 ans au plus, payée dans l'année.",
         sources=[("Annuaire", "p. 14 : un tableau « de fin d'exercice »"),
                  ("Données", "l'annuaire de la CNSS compte des enfants de 21 ans au plus "
                              "(tableau 36) ; avec la condition de paiement, nos chiffres de "
                              "2024 y retombent à 1,2 % près, sans elle à 5,6 % (P74, P79)"),
                  ("Proposition BI", "ne compter que les enfants payés ; voir la question Q1")],
         chiffre="En 2024 : 271 577 enfants comptés ; 268 481 dans l'annuaire de la CNSS."),
    dict(n="3", titre="L'allocataire compté",
         regle="Le dossier d'allocations familiales qui a au moins un enfant compté, une seule "
               "fois, quel que soit le nombre de ses enfants.",
         sources=[("Kaya 2017", "« c'est le dossier de l'allocataire qui est considéré et non le "
                                "nombre de prestataires »"),
                  ("Annuaire", "glossaire, p. 62 : l'allocataire a droit aux prestations "
                               "familiales « du chef de son activité professionnelle ou de "
                               "celle d'autrui »"),
                  ("Données", "un titulaire par dossier : 102 637 titulaires pour 102 643 "
                              "dossiers (P74)")],
         chiffre="En 2024 : 98 048 allocataires, 2,8 enfants chacun ; 97 378 dans l'annuaire de "
                 "la CNSS (+0,7 %)."),
    dict(n="4", titre="L'âge",
         regle="L'âge de l'allocataire, titulaire du dossier : l'année de l'exercice moins son "
               "année de naissance. 120 ans ou plus : « Âge inconnu ».",
         sources=[("Annuaire", "glossaire, p. 62 : « la différence entre l'année de "
                               "constatation et l'année de naissance »"),
                  ("Données", "l'année 1900 est une date de naissance par défaut dans la base "
                              "(P80)"),
                  ("Proposition BI", "le titulaire du dossier ; voir la question Q2")],
         chiffre="Exemple : né en 1985, il a 40 ans dans T41 de 2025."),
    dict(n="5", titre="Les tranches d'âge",
         regle="Moins de 20 ans, puis 20-24, 25-29… 75-79, et 80 ans et plus.",
         sources=[("Annuaire", "p. 41 : la maquette de T41")]),
    dict(n="6", titre="Le sexe",
         regle="Celui de l'allocataire, d'après sa fiche : 1 pour les hommes, 2 pour les femmes.",
         sources=[("Base", "deux valeurs, 1 et 2, sans libellé"),
                  ("Données", "1 = hommes, confirmé sur les pensionnés de vieillesse (P75)")]),
    dict(n="7", titre="Le montant",
         regle="Ce qui a été payé dans l'année pour les enfants comptés du dossier : les débours "
               "de paiement au statut D ou P, au mois payé.",
         sources=[("Annuaire", "p. 14 : « les montants servis »"),
                  ("Données", "7,69 Md en 2024, 6,7 % sous les 8,24 Md publiés par la CNSS ; "
                              "tout le payé de l'année, enfants sortis compris : 7,95 Md "
                              "(P75)"),
                  ("Proposition BI", "le montant des seuls enfants comptés ; voir la question "
                                     "Q3")],
         chiffre="En 2024 : 7,69 Md versés pour les enfants comptés."),
    dict(n="8", titre="Le Total et l'allocation moyenne",
         regle="Le Total est la somme des tranches. L'allocation moyenne est le montant divisé "
               "par l'effectif, par tranche, par sexe et au total.",
         sources=[("Annuaire", "p. 14 : la pyramide des âges et « la pension moyenne »")],
         chiffre="En 2024 : environ 78 400 FCFA par allocataire et par an, 28 300 par enfant."),
]

# Traduction en SQL de chaque règle : extraits de analyses/prestations/t32_t41_requete_proposee.sql
SQL = {
    "1": """-- T41 : allocations familiales
SELECT 'T41' AS tableau, 'FAF' AS code
FROM DUAL""",
    "2": """-- en cours au 31/12, enfant en vie
-- (comme T32), de 21 ans au plus
AND (pm.tableau <> 'T41'
     OR annee - EXTRACT(YEAR FROM
          i.IND_DATE_NAISSANCE) <= 21)
-- et payé dans l'exercice : jointure
-- sur paie, pas de LEFT JOIN
JOIN paie pa ON pa.PE_ID = ec.PE_ID""",
    "3": """-- un allocataire par dossier :
-- le titulaire du dossier
SELECT 'T41', d.IND_ID,
       SUM(pa.montant)
FROM   en_cours ec
JOIN   DWH.FAIT_DOSSIER d
       ON d.DOS_CODE = ec.DOS_CODE
JOIN   paie pa ON pa.PE_ID = ec.PE_ID
GROUP BY ec.DOS_CODE, d.IND_ID""",
    "4": """-- l'âge du titulaire du dossier
annee - EXTRACT(YEAR FROM
          i.IND_DATE_NAISSANCE) AS age""",
    "5": """-- T41 : bas = 20, haut = 80
CASE WHEN age IS NULL OR age < 0
       OR age >= 120  THEN 'Âge inconnu'
     WHEN age < bas   THEN 'Moins de 20 ans'
     WHEN age >= haut THEN '80 ans et plus'
     ELSE FLOOR(age / 5) * 5 || ' – '
          || (FLOOR(age / 5) * 5 + 4) || ' ans'
END AS libelle""",
    "6": """SUM(CASE WHEN sexe = '1'
         THEN 1 ELSE 0 END) AS eff_h,
SUM(CASE WHEN sexe = '2'
         THEN 1 ELSE 0 END) AS eff_f""",
    "7": """-- payé dans l'exercice, au mois payé
WHERE deb.DEB_TYPE = 'PM'
  AND deb.DEB_STATUT IN ('D', 'P')
  AND deb.DEB_DATE_EFFET >= debut
  AND deb.DEB_DATE_EFFET <  fin + 1""",
    "8": """-- ligne TOTAL, et la moyenne
SELECT tableau, 1000, 'TOTAL', COUNT(*),
       SUM(montant)
FROM tranche GROUP BY tableau
-- …
ROUND(mnt_t / NULLIF(eff_t, 0))
                       AS MONTANT_MOYEN""",
}

SQL_NOTE = ("À droite de chaque règle, sa traduction en SQL, pour la DSI et l'équipe BI : un "
            "extrait de la requête commune à T32-T41 (analyses/prestations/"
            "t32_t41_requete_proposee.sql). Elle lit les copies de l'entrepôt (DWH.FAIT_…) du "
            "cliché du mois, pour l'exercice choisi ; T41 y a son code (perimetre), sa grille "
            "d'âge (grille) et son propre calcul de l'allocataire (beneficiaire).")

QUESTIONS = [
    dict(n="Q1", titre="L'enfant compté",
         regle="Un enfant dont l'allocation court au 31 décembre, mais qui n'a rien perçu dans "
               "l'année (certificat attendu, par exemple), compte-t-il ?",
         cas=[("Si non : seuls les enfants payés",
               "98 048 allocataires en 2024, comme l'annuaire (97 378)."),
              ("Si oui",
               "102 643 allocataires, 5 % de plus que l'annuaire.")],
         sources=[("Données", "annuaire de la CNSS 2024, tableau 38 ; nos requêtes P74 et P79"),
                  ("Proposition BI", "ne compter que les enfants payés, comme l'annuaire")]),
    dict(n="Q2", titre="Qui est l'allocataire",
         regle="L'âge et le sexe du tableau sont-ils ceux du titulaire du dossier, l'assuré qui "
               "ouvre le droit ?",
         cas=[("Le titulaire du dossier",
               "un titulaire par dossier : 102 637 titulaires pour 102 643 dossiers."),
              ("La personne payée",
               "101 683 personnes payées distinctes ; leur âge et leur sexe ne sont pas encore "
               "mesurés.")],
         sources=[("Kaya 2017", "c'est le dossier de l'allocataire qui est considéré"),
                  ("Proposition BI", "le titulaire du dossier")]),
    dict(n="Q3", titre="Le montant",
         regle="Nos montants restent sous celui de l'annuaire de la CNSS (8,24 Md en 2024). "
               "Comptez-vous les rappels payés dans l'année pour des mois anciens ?",
         cas=[("Ce qui a été payé pour les enfants comptés",
               "7,69 Md (−6,7 %) ; l'allocation moyenne a un sens."),
              ("Tout ce qui a été payé dans l'année",
               "7,95 Md (−3,4 %), enfants sortis en cours d'année compris.")],
         sources=[("Données", "annuaire de la CNSS 2024, tableau 36 ; notre requête P75"),
                  ("Proposition BI", "le montant des enfants comptés, en attendant votre "
                                     "réponse")]),
    dict(n="Q4", titre="Les allocations prénatales",
         regle="Une femme qui ne touche qu'une allocation prénatale est-elle une allocataire de "
               "T41 ?",
         cas=[("Si oui", "chaque grossesse déclarée ajoute une allocataire pour l'année."),
              ("Si non", "elles restent dans leur propre tableau, T55 (bénéficiaires "
                          "d'allocations prénatales).")],
         sources=[("Annuaire", "p. 16 : T54 et T55 suivent les indemnités de maternité et "
                               "les allocations prénatales"),
                  ("Base", "MAP, allocation prénatale de la branche maternité ; FAP, l'ancien "
                           "code des PF, presque éteint (P66)"),
                  ("Proposition BI", "les laisser hors de T41")]),
    dict(n="Q5", titre="Un tableau des enfants",
         regle="Faut-il ajouter à T41 un tableau des enfants par âge, comme le tableau 36 de "
               "l'annuaire de la CNSS ?",
         cas=[("Si oui", "271 577 enfants en 2024, de 0 à 21 ans, par tranche et par sexe."),
              ("Si non", "les enfants ne restent qu'un contrôle de l'équipe BI.")],
         sources=[("Kaya 2017", "« prévoir un tableau statistique sur les enfants des "
                                "allocataires »"),
                  ("Proposition BI", "l'ajouter : les données sont prêtes")]),
]

BI_CHECKS = ("La règle retombe sur l'annuaire de la CNSS : 98 048 allocataires et 271 577 "
             "enfants en 2024, pour 97 378 et 268 481 publiés (lot 6, P79).\n"
             "Avant 2024, l'écart grandit (+5 % en 2023, +11 % en 2020) : sans doute les "
             "chiffres publiés à l'époque ; sans effet pour la BI.\n"
             "Un titulaire par dossier (P74) ; 1 = hommes (P75) ; 1900 = date par défaut (P80).\n"
             "Pas encore mesurés : l'âge et le sexe des allocataires eux-mêmes ; le montant reste "
             "sous l'annuaire (question Q3).")

# allocataires au 31/12 : publiés par la CNSS (annuaire 2024, tableau 38) et les nôtres avec la
# règle 2 (lot 6, P79 : enfants payés dans l'année)
ALLOCATAIRES = [("2020", 84401, 93972), ("2021", 88587, 96656), ("2022", 91085, 98570),
                ("2023", 93233, 98128), ("2024", 97378, 98048)]


def figures(d, x, y, w):
    """Encadré des premiers chiffres : les allocataires publiés par la CNSS et les nôtres."""
    g = d.group()
    h = 640
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + 20, y + 16, "La base face à l'annuaire de la CNSS", 22, groups=g, inside=frame)
    d.text(x + 20, y + 54, "En 2024 : 98 048 allocataires et 271 577 enfants chez nous,\n"
                           "97 378 et 268 481 publiés par la CNSS (+0,7 % et +1,2 %).\n"
                           "Montant : 7,69 Md chez nous, 8,24 Md publiés.", 18, groups=g,
           inside=frame)
    d.text(x + 20, y + 140, "Allocataires au 31/12 :", 16, C["doux"], groups=g, inside=frame)
    lx = x + 340
    d.box(lx, y + 142, 18, 14, bg=C["bleu_moyen"], stroke=C["bleu"], groups=g)
    d.text(lx + 26, y + 140, "annuaire de la CNSS", 16, groups=g, inside=frame)
    d.box(lx + 210, y + 142, 18, 14, bg=C["vert_moyen"], stroke=C["vert"], groups=g)
    d.text(lx + 236, y + 140, "la base (P79)", 16, groups=g, inside=frame)
    by = y + 176
    biggest = max(max(a, b) for _, a, b in ALLOCATAIRES)
    scale = (w - 260) / biggest
    for label, cnss, nous in ALLOCATAIRES:
        d.text(x + 20, by + 13, label, 16, groups=g, inside=frame)
        for k, (n, bg, stroke) in enumerate([(cnss, C["bleu_moyen"], C["bleu"]),
                                             (nous, C["vert_moyen"], C["vert"])]):
            top = by + k * 24
            d.box(x + 130, top, max(n * scale, 4), 20, bg=bg, stroke=stroke, groups=g)
            d.text(x + 140 + max(n * scale, 4), top, f"{n:,}".replace(",", " "), 15, groups=g,
                   inside=frame)
        by += 56
    note = wrap("Avant 2024, l'écart grandit : les chiffres anciens de l'annuaire sont sans doute "
                "ceux publiés à l'époque, sur la base de ce moment-là.", 16, w - 40)
    d.text(x + 20, by + 8, note, 16, C["doux"], groups=g, inside=frame)
    by += 8 + height_of(note, 16) + 12
    w_chip = chip(d, x + 20, by, "Données", groups=g)
    d.text(x + 20 + w_chip + 10, by + 5, "annuaire CNSS 2024, tableaux 36 et 38 ; P75, P79", 16,
           groups=g, inside=frame)
    return y + h


def build():
    d = Diagram(seed=41)
    inner = WIDTH - 50

    d.text(0, 0, "T41 · Les allocataires des prestations familiales au 31 décembre", 48)
    d.text(0, 66, "« Répartition des allocataires de la branche PF et montants par tranches d'âges "
                  "et par sexe » · suit la fiche pilote T32 · ce que l'équipe BI a compris, à "
                  "valider · état au 01/11/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    about = d.box(25, top, 760, 290, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T41", 22, groups=g, inside=about)
    text = wrap("Il dit combien d'allocataires des prestations familiales la CNSS compte au 31 "
                "décembre, par tranche d'âge et par sexe, et ce qu'elle a versé dans l'année pour "
                "leurs enfants. On en tire la pyramide des âges des allocataires et l'allocation "
                "moyenne.", 18, 720)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 14, maquette p. 41 ; fin d'exercice"),
                      ("Kaya 2017", "on compte le dossier, pas les enfants")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(d, 25, top + 310, 760)

    bottom = mockup(
        d, 830, top, 300, (WIDTH - 25 - 830 - 300) / 6, "Tranches d'âge", "4 et 5",
        columns=[("Effectif", "2 et 3"), ("Montant", "7"), ("Effectif", "2 et 3"),
                 ("Montant", "7"), ("Effectif", "8"), ("Montant", "8")],
        groups=[(0, 1, "HOMMES", "6"), (2, 3, "FEMMES", "6"), (4, 5, "TOTAL", "8")],
        rows=[("Moins de 20 ans", None, None), ("20 – 24 ans", None, None),
              ("25 – 29 ans", None, None), ("30 – 34 ans", None, None),
              ("35 – 39 ans", None, None), ("40 – 44 ans", None, None),
              ("45 – 49 ans", None, None), ("50 – 54 ans", None, None),
              ("55 – 59 ans", None, None), ("60 – 64 ans", None, None),
              ("65 – 69 ans", None, None), ("70 – 74 ans", None, None),
              ("75 – 79 ans", None, None), ("80 ans et plus", None, None),
              ("TOTAL", None, "8")],
        row_h=40)
    d.text(830, bottom + 14, wrap("Les colonnes Effectif comptent les allocataires, un par "
                                  "dossier, au 31/12 (règles 1 à 3) ; les colonnes Montant, ce "
                                  "qui a été payé dans l'année pour leurs enfants comptés "
                                  "(règle 7).", 18, WIDTH - 25 - 830), 18, C["doux"])
    sec["height"] = max(bottom + 80, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Qui est compté : six allocataires fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2024, 11, 1), date(2026, 2, 28), 2025,
                      EXAMPLES, outcomes=SORTIES,
                      axis_label="Dans le calendrier : les allocations, du début à la fin")
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes : les tranches d'âge de l'allocataire")
    w = chip(d, 25, y + 72, "Annuaire")
    d.text(25 + w + 12, y + 76, "Une ligne par tranche d'âge de 5 ans, de « moins de 20 ans » à "
                                "« 80 ans et plus » (maquette p. 41). C'est l'âge de "
                                "l'allocataire qui compte, pas celui des enfants.", 18)
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
    sec = d.section(0, y, WIDTH, 0, "5. Les questions pour le métier")
    bottom = cards_grid(d, 25, y + 70, inner, 3, QUESTIONS)
    g = d.group()
    checks = d.box(25, bottom + 20, inner, 190, bg=C["gris"], stroke=C["gris_bord"], groups=g)
    d.text(45, bottom + 36, "Ce que l'équipe BI a vérifié dans les données (01/11/2026) : pas de "
                            "réponse attendue", 20, groups=g, inside=checks)
    d.text(45, bottom + 70, BI_CHECKS, 16, groups=g, inside=checks)
    sec["height"] = bottom + 20 + 190 + 30 - sec["y"]
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
