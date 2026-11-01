"""
Fiche de validation de T32 « Répartition des pensionnés vieillesse et montants par tranches
d'âges et par sexe » (état au 31/10/2026) : fiche pilote de la famille T32 à T41.

Même gabarit que T25 : la maquette, six pensionnés fictifs sur un calendrier, les tranches
d'âge, les huit règles avec leur SQL et cinq questions. Les règles valent pour T33 à T41, aux
différences près dites dans la requête commune (périmètre, grille d'âge, orphelins, allocataires).

Sources : Annuaire CIPRES (p. 13, 37, 62) ; atelier de Kaya 2017 (T32 : part CNSS de la
coordination) ; annuaire de la CNSS 2024 (tableaux 18, 19) ; analyses/prestations (P14, P15,
P25, P26, P29 et le lot 5, P74 à P78) ; t32_t41_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/prestations/t32_pensionnes_vieillesse.py [--force]
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

OUTPUT = ROOT / "schemas" / "prestations" / "tableaux cipres" / "T32_pensionnes_vieillesse.excalidraw"

HOW_TO = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix dans une "
          "case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la carte si "
          "besoin. Enregistre avec Cmd+S : chaque version est gardée dans l'historique.\n"
          "C'est la fiche pilote de T32 à T41 : les mêmes règles servent aux pensions "
          "d'invalidité, de survivants, aux rentes d'accident du travail et aux allocataires. "
          "Valider T32, c'est valider la famille.")

# sort d'un pensionné fictif dans le calendrier : compté ou non au 31/12
SORTIES = {
    "compte": (C["vert_moyen"], C["vert"], "compté au 31/12"),
    "exclu": (C["gris"], C["gris_bord"], "pas compté au 31/12"),
    "question": (C["jaune"], C["jaune_bord"], "selon la réponse à une question"),
}

EXAMPLES = [
    dict(nom="Pensionné A", detail="retraité depuis 2015", debut=date(2024, 11, 1),
         fin=date(2026, 2, 28), sortie="compte",
         dates="pension en cours toute l'année, payée chaque mois",
         resultat="Dans T32 de 2025 : compté, dans sa tranche d'âge ; montant : les 12 mois payés "
                  "en 2025."),
    dict(nom="Pensionné B", detail="nouveau retraité", debut=date(2025, 4, 1),
         fin=date(2026, 2, 28), sortie="compte",
         dates="pension depuis le 01/04/2025",
         resultat="Dans T32 de 2025 : compté ; montant : les 9 mois payés, d'avril à décembre."),
    dict(nom="Pensionné C", detail="décédé en cours d'année", debut=date(2024, 11, 1),
         fin=date(2025, 8, 12), sortie="exclu",
         dates="décédé le 12/08/2025",
         resultat="Dans T32 de 2025 : pas compté. Ses paiements de janvier à juillet sont en "
                  "dehors de T32 (question Q3)."),
    dict(nom="Pensionné D", detail="pension suspendue", debut=date(2024, 11, 1),
         fin=date(2025, 10, 31), sortie="exclu",
         dates="suspendue le 31/10/2025, pas reprise",
         resultat="Dans T32 de 2025 : pas compté, la pension n'est plus en cours au 31/12."),
    dict(nom="Pensionné E", detail="pas payé en fin d'année", debut=date(2024, 11, 1),
         fin=date(2026, 2, 28), sortie="compte",
         dates="pension en cours, pas payée depuis octobre (certificat de vie attendu)",
         resultat="Dans T32 de 2025 : compté, comme dans l'annuaire de la CNSS (règle 2) ; "
                  "montant : les mois payés."),
    dict(nom="Pensionné F", detail="retraite anticipée", debut=date(2025, 2, 1),
         fin=date(2026, 2, 28), sortie="question",
         dates="pension de retraite anticipée depuis le 01/02/2025",
         resultat="Dans T32 de 2025 : compté si la retraite anticipée y entre (question Q2) ; "
                  "l'annuaire de la CNSS la compte à part."),
]

LINES = [
    (1, "Moins de 55 ans", "les pensionnés partis tôt, presque tous en retraite anticipée",
     "2 en 2024 dans l'annuaire de la CNSS, 16 chez nous : voir la question Q2", "Données"),
    (2, "De 55 à 99 ans", "neuf tranches de 5 ans : 55-59, 60-64… 95-99",
     "la plupart entre 60 et 74 ans", "Annuaire"),
    (3, "100 ans et plus", "les pensionnés très âgés",
     "56 en 2024 dans l'annuaire de la CNSS, 74 chez nous : voir la question Q4",
     "Données"),
    (4, "TOTAL", "la somme des tranches, hommes, femmes et ensemble",
     "avec la pension moyenne : montant ÷ effectif", "Annuaire"),
]

RULES = [
    dict(n="1", titre="Les pensions concernées",
         regle="Les pensions de vieillesse et les pensions de retraite anticipée.",
         sources=[("Annuaire", "p. 13 : T32 répartit « les bénéficiaires de pension de "
                               "vieillesse »"),
                  ("Base", "deux codes : RPV, pension de vieillesse ; RPRA, pension de retraite "
                           "anticipée"),
                  ("Kaya 2017", "pour la coordination, seuls les montants de la part CNSS "
                                "comptent : ce sont les seuls que paie la CNSS"),
                  ("Proposition BI", "la retraite anticipée dans T32 ; voir la question Q2")],
         chiffre="En 2024 : 31 180 pensions de vieillesse et 87 retraites anticipées en cours."),
    dict(n="2", titre="Le pensionné au 31 décembre",
         regle="Une pension en cours le 31 décembre (effet avant, ni radiée ni suspendue), dont "
               "le titulaire est en vie, qu'elle ait été payée en fin d'année ou non.",
         sources=[("Annuaire", "p. 13 : un tableau « de fin d'exercice »"),
                  ("Données", "c'est ainsi que compte l'annuaire de la CNSS : 30 808 "
                              "pensionnés publiés pour 2024 (vieillesse et anticipée), 31 093 "
                              "chez nous (+0,9 %) ; seuls 27 670 ont été payés dans l'année "
                              "(lot 5, P74)"),
                  ("Données", "les pensions suspendues, rares (253 en T32 en 2024), ne comptent "
                              "pas (lot 6, P81). T41 fait exception : un enfant ne compte que "
                              "s'il a été payé dans l'année (lot 6, P79)"),
                  ("Proposition BI", "s'aligner sur l'annuaire de la CNSS ; voir la question "
                                     "Q1")],
         chiffre="De 2020 à 2024, nos chiffres retombent sur l'annuaire à moins de 1 % près."),
    dict(n="3", titre="La personne comptée",
         regle="Le titulaire de la pension, une seule fois, même s'il a deux pensions de "
               "vieillesse (après une révision).",
         sources=[("Annuaire", "glossaire : le pensionné de droits directs, « assuré "
                               "bénéficiaire d'une pension du chef de ses droits acquis »"),
                  ("Données", "fin 2025, 10 personnes seulement ont deux pensions en cours "
                              "dans T32 ; 519 sont aussi dans un autre tableau (lot 5, P77)"),
                  ("Proposition BI", "une personne une fois par tableau ; une veuve "
                                     "retraitée compte aussi dans T34")]),
    dict(n="4", titre="L'âge",
         regle="L'année de l'exercice moins l'année de naissance, quelle que soit la date "
               "d'anniversaire. 120 ans ou plus : « Âge inconnu ».",
         sources=[("Annuaire", "glossaire, p. 62 : « la différence entre l'année de "
                               "constatation et l'année de naissance »"),
                  ("Données", "la date de naissance est remplie ; l'année 1900 est une date "
                              "par défaut : 503 bénéficiaires, dont 499 veuves de T34 et 1 "
                              "pensionné de T32 (lot 6, P80)"),
                  ("Proposition BI", "120 ans ou plus en « Âge inconnu » : cela couvre "
                                     "l'année 1900")],
         chiffre="Exemple : né en 1960, il a 65 ans dans T32 de 2025."),
    dict(n="5", titre="Les tranches d'âge",
         regle="Moins de 55 ans, puis 55-59, 60-64… 95-99, et 100 ans et plus.",
         sources=[("Annuaire", "p. 37 : la maquette de T32"),
                  ("Base", "la base a déjà 21 tranches de 5 ans : on les regroupe")]),
    dict(n="6", titre="Le sexe",
         regle="Hommes et femmes d'après la fiche de la personne : 1 pour les hommes, 2 pour les "
               "femmes.",
         sources=[("Base", "deux valeurs, 1 et 2, sans libellé"),
                  ("Données", "confirmé : 24 814 hommes et 6 279 femmes chez nous en 2024, "
                              "24 627 et 6 103 dans l'annuaire de la CNSS (lot 5, P75)")]),
    dict(n="7", titre="Le montant",
         regle="Ce qui a été payé dans l'année aux pensionnés comptés : les débours de paiement "
               "au statut D (payé par effet) ou P (payé sans effet), au mois payé.",
         sources=[("Annuaire", "p. 13 : « les montants servis »"),
                  ("Données", "l'annuaire de la CNSS (38,24 Md en 2024) tombe entre nos deux "
                              "montants : 37,67 Md aux pensionnés comptés (−1,5 %), 38,63 Md "
                              "en tout (+1,0 %) (lot 5, P75)"),
                  ("Données", "2 901 pensions de coordination payées par une autre caisse "
                              "(COOR_AC) n'ont aucun paiement CNSS : comptées, sans montant "
                              "(lot 5, P78)"),
                  ("Proposition BI", "le montant des seuls pensionnés comptés ; voir la "
                                     "question Q3")],
         chiffre="En 2024 : 37,67 Md payés aux pensionnés comptés, et 0,96 Md à 1 105 "
                 "pensionnés sortis dans l'année."),
    dict(n="8", titre="Le Total et la pension moyenne",
         regle="Le Total est la somme des tranches. La pension moyenne est le montant divisé "
               "par l'effectif, par tranche, par sexe et au total.",
         sources=[("Annuaire", "p. 13 : la pension moyenne, dont l'évolution renseigne sur le "
                               "pouvoir d'achat des pensionnés")],
         chiffre="En 2024 : environ 1,2 M FCFA par retraité et par an."),
]

# Traduction en SQL de chaque règle : extraits de analyses/prestations/t32_t41_requete_proposee.sql
SQL = {
    "1": """-- T32 : pension de vieillesse et
-- retraite anticipée
SELECT 'T32' AS tableau, 'RPV' AS code
FROM DUAL
UNION ALL SELECT 'T32', 'RPRA' FROM DUAL""",
    "2": """-- fin = le 31/12 de l'exercice
WHERE pe.PE_DATE_EFFET <= fin
  AND (pe.PE_DATE_RADIATION IS NULL
       OR pe.PE_DATE_RADIATION > fin)
  AND (   pe.PE_DATE_SUSPENSION IS NULL
       OR pe.PE_DATE_SUSPENSION > fin
       OR pe.PE_DATE_REACTIVATION
            BETWEEN pe.PE_DATE_SUSPENSION
                AND fin)
  -- titulaire en vie au 31/12
  AND (i.IND_DATE_DECES IS NULL
       OR i.IND_DATE_DECES > fin)""",
    "3": """-- une personne par tableau, avec le
-- montant de toutes ses pensions du tableau
SELECT tableau,
       IND_ID_PREST     AS personne,
       SUM(montant)     AS montant
FROM   en_cours
GROUP BY tableau, IND_ID_PREST""",
    "4": """annee - EXTRACT(YEAR FROM
                i.IND_DATE_NAISSANCE) AS age""",
    "5": """-- T32 : bas = 55, haut = 100
CASE WHEN age IS NULL OR age < 0
       OR age >= 120  THEN 'Âge inconnu'
     WHEN age < bas   THEN 'Moins de 55 ans'
     WHEN age >= haut THEN '100 ans et plus'
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
            "cliché du mois, pour l'exercice choisi ; chaque tableau a ses codes (perimetre) et "
            "sa grille d'âge (grille).")

QUESTIONS = [
    dict(n="Q1", titre="Le pensionné au 31 décembre",
         regle="Comptez-vous, comme l'annuaire de la CNSS, toute pension en cours au 31 "
               "décembre, payée ou non en fin d'année ?",
         cas=[("Si oui",
               "31 093 pensionnés en 2024, comme l'annuaire (30 808 avec la retraite "
               "anticipée)."),
              ("Si on exige un paiement dans l'année",
               "27 670, 11 % de moins : les pensions de coordination payées par une autre "
               "caisse et celles en attente d'un certificat de vie disparaissent.")],
         sources=[("Données", "annuaire de la CNSS 2024, tableau 18 ; nos requêtes A01, P74 "
                              "et P78"),
                  ("Proposition BI", "s'aligner sur l'annuaire de la CNSS")]),
    dict(n="Q2", titre="La retraite anticipée",
         regle="La retraite anticipée entre-t-elle dans T32, avec la vieillesse ?",
         cas=[("Si oui", "87 pensionnés de plus en 2024."),
              ("Si non", "elle n'apparaît dans aucun tableau des pensionnés au 31/12.")],
         sources=[("Kaya 2017", "la retraite anticipée a sa propre ligne en T25"),
                  ("Données", "l'annuaire de la CNSS la compte à part (78 en 2024)"),
                  ("Proposition BI", "la compter dans T32 : c'est une pension de vieillesse")]),
    dict(n="Q3", titre="Le montant",
         regle="Faut-il mettre en face des pensionnés du 31 décembre ce qui leur a été payé, ou "
               "tout ce qui a été payé dans l'année, décédés compris ?",
         cas=[("Ce qui a été payé aux pensionnés comptés",
               "37,67 Md en 2024 ; la pension moyenne (montant ÷ effectif) a un sens."),
              ("Tout ce qui a été payé dans l'année",
               "38,63 Md : le montant de l'année est complet, mais la moyenne est un peu "
               "gonflée.")],
         sources=[("Données", "l'annuaire de la CNSS publie 38,24 Md pour 2024, entre les "
                              "deux (lot 5, P75)"),
                  ("Proposition BI", "le montant des pensionnés comptés")]),
    dict(n="Q4", titre="Les pensionnés très âgés",
         regle="Fin 2025, 69 pensionnés payés ont 100 ans ou plus, et 215 pensions sont encore "
               "en cours alors que leur titulaire est décédé. Un contrôle d'existence est-il "
               "prévu ?",
         sources=[("Données", "en 2024, 74 pensionnés de 100 ans et plus chez nous, 56 dans "
                              "l'annuaire de la CNSS ; 12 auraient 110 ans ou plus, avec des "
                              "dates de naissance approchées (lots 5 et 6, P76, P80)"),
                  ("Proposition BI", "les compter tant que la pension est en cours, et les "
                                     "signaler à la direction des prestations")]),
    dict(n="Q5", titre="La coordination et la pension moyenne",
         regle="Fin 2025, 2 901 pensions de vieillesse de coordination sont payées par une "
               "autre caisse : la CNSS ne leur verse rien. Elles comptent dans l'effectif ; "
               "faut-il aussi les compter dans la pension moyenne ?",
         cas=[("Si oui",
               "la moyenne suit la définition de l'Annuaire (montant ÷ effectif), mais baisse "
               "de près de 9 %."),
              ("Si non",
               "la moyenne est celle des pensions que paie la CNSS ; il faut le dire sous le "
               "tableau.")],
         sources=[("Kaya 2017", "pour la coordination, seule la part CNSS des montants compte"),
                  ("Données", "sans elles, l'effectif de 2024 serait à −8 % de l'annuaire de "
                              "la CNSS : elle les compte (lot 5, P78)"),
                  ("Proposition BI", "les compter dans l'effectif, sans montant ; la moyenne "
                                     "selon votre réponse")]),
]

BI_CHECKS = ("La définition du pensionné est celle de l'annuaire de la CNSS : nos chiffres de "
             "2020 à 2024 y retombent à moins de 1 % près (lot 5, P74).\n"
             "Le sexe est confirmé (1 = hommes) ; la répartition par âge suit celle de "
             "l'annuaire à quelques centaines près.\n"
             "Le montant publié tombe entre nos deux montants (−1,5 % et +1,0 %).\n"
             "L'année de naissance 1900 est une date par défaut (lot 6, P80) : ces "
             "bénéficiaires passent en âge inconnu.")


# répartition par âge publiée par la CNSS pour 2024 (annuaire, tableau 19 : vieillesse seule) et
# la nôtre (lot 5, P76 : vieillesse et retraite anticipée)
PYRAMIDE_2024 = [("moins de 55", 2, 16), ("55-59", 3399, 3778), ("60-64", 8638, 8854),
                 ("65-69", 7757, 7737), ("70-74", 5623, 5536), ("75-79", 3078, 2997),
                 ("80-84", 1264, 1220), ("85-89", 619, 590), ("90-94", 195, 195),
                 ("95-99", 99, 97), ("100 et plus", 56, 74)]


def figures(d, x, y, w):
    """Encadré des premiers chiffres : la répartition par âge publiée par la CNSS et la nôtre."""
    g = d.group()
    h = 860
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + 20, y + 16, "En 2024 : la base face à l'annuaire de la CNSS", 22,
           groups=g, inside=frame)
    d.text(x + 20, y + 54, "31 093 pensionnés au 31/12/2024 chez nous, 30 808 publiés par la\n"
                           "CNSS (vieillesse et anticipée) : +0,9 %. Montant : 37,67 Md aux\n"
                           "pensionnés comptés, 38,24 Md publiés.", 18, groups=g, inside=frame)
    d.text(x + 20, y + 140, "Pensionnés par tranche d'âge, en 2024 :", 16, C["doux"], groups=g,
           inside=frame)
    lx = x + 340
    d.box(lx, y + 142, 18, 14, bg=C["bleu_moyen"], stroke=C["bleu"], groups=g)
    d.text(lx + 26, y + 140, "annuaire de la CNSS", 16, groups=g, inside=frame)
    d.box(lx + 210, y + 142, 18, 14, bg=C["vert_moyen"], stroke=C["vert"], groups=g)
    d.text(lx + 236, y + 140, "la base (lot 5)", 16, groups=g, inside=frame)
    by = y + 176
    biggest = max(max(a, b) for _, a, b in PYRAMIDE_2024)
    scale = (w - 260) / biggest
    for label, cnss, nous in PYRAMIDE_2024:
        d.text(x + 20, by + 13, label, 16, groups=g, inside=frame)
        for k, (n, bg, stroke) in enumerate([(cnss, C["bleu_moyen"], C["bleu"]),
                                             (nous, C["vert_moyen"], C["vert"])]):
            top = by + k * 24
            d.box(x + 130, top, max(n * scale, 4), 20, bg=bg, stroke=stroke, groups=g)
            d.text(x + 140 + max(n * scale, 4), top, f"{n:,}".replace(",", " "), 15, groups=g,
                   inside=frame)
        by += 56
    w_chip = chip(d, x + 20, by + 10, "Données", groups=g)
    d.text(x + 20 + w_chip + 10, by + 15, "annuaire CNSS 2024, tableaux 18 et 19 ; P74 à P76", 16, groups=g, inside=frame)
    return y + h


def build():
    d = Diagram(seed=32)
    inner = WIDTH - 50

    d.text(0, 0, "T32 · Les pensionnés de vieillesse au 31 décembre, par âge et par sexe", 48)
    d.text(0, 66, "« Répartition des pensionnés vieillesse et montants par tranches d'âges et par "
                  "sexe » · fiche pilote de T32 à T41 · ce que l'équipe BI a compris, à valider · "
                  "état au 31/10/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    about = d.box(25, top, 760, 290, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T32", 22, groups=g, inside=about)
    text = wrap("Il dit combien de pensionnés de vieillesse la CNSS compte au 31 décembre, par "
                "tranche d'âge et par sexe, et ce qu'elle leur a versé dans l'année. On en tire "
                "la pyramide des âges et la pension moyenne, utiles aux études actuarielles.",
                18, 720)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 13, maquette p. 37 ; fin d'exercice"),
                      ("Annuaire", "la même forme pour T33 à T41")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(d, 25, top + 310, 760)

    bottom = mockup(
        d, 830, top, 300, (WIDTH - 25 - 830 - 300) / 6, "Tranches d'âge", "4 et 5",
        columns=[("Effectif", "2"), ("Montant", "7"), ("Effectif", "2"), ("Montant", "7"),
                 ("Effectif", "8"), ("Montant", "8")],
        groups=[(0, 1, "HOMMES", "6"), (2, 3, "FEMMES", "6"), (4, 5, "TOTAL", "8")],
        rows=[("Moins de 55 ans", None, None), ("55 – 59 ans", None, None),
              ("60 – 64 ans", None, None), ("65 – 69 ans", None, None),
              ("70 – 74 ans", None, None), ("75 – 79 ans", None, None),
              ("80 – 84 ans", None, None), ("85 – 89 ans", None, None),
              ("90 – 94 ans", None, None), ("95 – 99 ans", None, None),
              ("100 ans et plus", None, None), ("TOTAL", None, "8")],
        row_h=46)
    d.text(830, bottom + 14, wrap("Les colonnes Effectif comptent les pensionnés au 31/12 "
                                  "(règles 1 à 3) ; les colonnes Montant, ce qui leur a été payé "
                                  "dans l'année (règle 7).", 18, WIDTH - 25 - 830), 18,
           C["doux"])
    sec["height"] = max(bottom + 80, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Qui est compté : six pensionnés fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2024, 11, 1), date(2026, 2, 28), 2025,
                      EXAMPLES, outcomes=SORTIES,
                      axis_label="Dans le calendrier : la pension, du début à la fin")
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes : les tranches d'âge")
    w = chip(d, 25, y + 72, "Annuaire")
    d.text(25 + w + 12, y + 76, "Une ligne par tranche d'âge de 5 ans, de « moins de 55 ans » à "
                                "« 100 ans et plus ». Les autres tableaux ont leur propre grille "
                                "(T33 jusqu'à 60 ans et plus, T35 de moins de 5 ans à 20-24 "
                                "ans…).", 18)
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
    checks = d.box(25, bottom + 20, inner, 170, bg=C["gris"], stroke=C["gris_bord"], groups=g)
    d.text(45, bottom + 36, "Ce que l'équipe BI a vérifié dans les données (31/10/2026) : pas de "
                            "réponse attendue", 20, groups=g, inside=checks)
    d.text(45, bottom + 70, BI_CHECKS, 16, groups=g, inside=checks)
    sec["height"] = bottom + 20 + 170 + 30 - sec["y"]
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
