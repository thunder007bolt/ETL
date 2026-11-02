"""
Fiche de validation de T31 « Répartition des prestations indues par branche et par mois » (état
au 01/11/2026).

Même gabarit que les autres fiches : la maquette (12 mois × payées, trop-perçus, moins-perçus de
trois branches et du total), six cas fictifs sur un calendrier, les lignes, huit règles avec leur
SQL et cinq questions. La date des paiements (règle R4) est en attente de la CNSS : la fiche
montre les deux possibilités.

Sources : Annuaire CIPRES (p. 13, 36) ; atelier de Kaya 2017 (définition des indus, colonne
« payées », taux de prestations indues) ; analyses/prestations (lot 1 P11, lot 3 P41, lot 9 P102
à P106) ; t31_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/prestations/t31_indus.py [--force]
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

OUTPUT = ROOT / "schemas" / "prestations" / "tableaux cipres" / "T31_prestations_indues.excalidraw"

HOW_TO = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix dans une "
          "case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la carte si "
          "besoin. Enregistre avec Cmd+S : chaque version est gardée dans l'historique.\n"
          "T31 suit chaque mois les prestations payées et les prestations indues de chaque "
          "branche ; il donne le taux de prestations indues, un indicateur CIPRES dont la norme "
          "est de 2 % au plus.")

# sort d'un cas fictif dans le calendrier, pour T31 de 2025
SORTIES = {
    "compte": (C["vert_moyen"], C["vert"], "trop-perçu de 2025 : dans le taux"),
    "moins": (C["bleu_moyen"], C["bleu"], "moins-perçu : dans le tableau, hors du taux"),
    "exclu": (C["gris"], C["gris_bord"], "pas dans T31 de 2025"),
    "question": (C["jaune"], C["jaune_bord"], "selon la réponse à une question"),
}

EXAMPLES = [
    dict(nom="Pensionné A", detail="trop-perçu après une révision", debut=date(2025, 1, 1),
         fin=date(2025, 3, 20), sortie="compte",
         dates="payé en trop en janvier et février, trop-perçu établi le 20/03/2025",
         resultat="T31 de 2025 : trop-perçu de mars, dans la colonne PVID ; il compte dans le "
                  "taux d'indus."),
    dict(nom="Pensionnée B", detail="rappel d'un moins-perçu", debut=date(2025, 4, 1),
         fin=date(2025, 7, 15), sortie="moins",
         dates="sous-payée d'avril à juin, moins-perçu établi en juin, payé en juillet",
         resultat="T31 de 2025 : moins-perçu de juin ; il reste dans le tableau mais n'est pas "
                  "un indu (Kaya 2017)."),
    dict(nom="Pensionné C", detail="avance sur pension", debut=date(2025, 4, 10),
         fin=date(2025, 9, 30), sortie="exclu",
         dates="avance versée en avril 2025, reprise sur le rappel en septembre",
         resultat="Hors de T31 : une avance n'est ni un trop-perçu ni un moins-perçu (règle 5)."),
    dict(nom="Allocataire D", detail="trop-perçu de 2024, repris en 2025",
         debut=date(2024, 11, 15), fin=date(2025, 6, 30), sortie="exclu",
         dates="trop-perçu établi en novembre 2024, retenu sur les paiements jusqu'en juin 2025",
         resultat="T31 de 2024 (novembre), pas de 2025 : un trop-perçu compte le mois où il est "
                  "établi, pas quand il est repris (règle 7)."),
    dict(nom="Rentier E", detail="trop-perçu annulé", debut=date(2025, 5, 5),
         fin=date(2025, 6, 20), sortie="exclu",
         dates="trop-perçu établi en mai 2025, annulé en juin (statut R)",
         resultat="Hors de T31 : un ajustement annulé ne compte pas (règle 6)."),
    dict(nom="Retraité F", detail="pension payée après le décès", debut=date(2025, 2, 10),
         fin=date(2025, 6, 30), sortie="question",
         dates="décédé en février 2025, payé jusqu'en mai, décès déclaré en juin",
         resultat="Un trop-perçu de 2025 si la CNSS l'établit ; aujourd'hui, presque jamais "
                  "(question Q3)."),
]

LINES = [
    (1, "Janvier à décembre", "une ligne par mois de l'exercice",
     "le mois où l'ajustement est établi ; pour les payées, le mois de la règle R4",
     "Proposition BI"),
    (2, "TOTAL", "la somme des 12 mois", "c'est sur cette ligne que se lit le taux de l'année",
     "Annuaire"),
    (3, "Les branches en colonnes", "PVID, AT/MP, PF, puis TOTAL",
     "la PF comprend la maternité, que le DTM oubliait", "Données"),
    (4, "Le taux d'indus", "pas dans la maquette : la BI l'ajoute",
     "trop-perçus ÷ payées ; indicateur CIPRES, norme 2 % au plus", "Kaya 2017"),
]

RULES = [
    dict(n="1", titre="Les branches",
         regle="La branche du dossier : pensions (PVID), accidents du travail (AT/MP), "
               "prestations familiales (PF), maternité comprise.",
         sources=[("Annuaire", "p. 36 : la maquette de T31, trois branches et un total"),
                  ("Base", "le type du dossier : V, A, F, M ; un trop-perçu n'a pas de "
                           "prestation liée, sa branche se lit par le dossier (P106)"),
                  ("Données", "le DTM laissait la maternité de côté : 15 trop-perçus en 2024 "
                              "(P102)")]),
    dict(n="2", titre="Les payées",
         regle="« La somme décaissée au profit de l'assuré » : les débours de paiement au statut "
               "D ou P, au mois payé ou au mois du versement.",
         sources=[("Kaya 2017", "« la colonne payées est la somme décaissée au profit de "
                                "l'assuré social »"),
                  ("Données", "58,7 Md en 2024 ; au mois payé la série est régulière, au mois "
                              "du versement elle a des pics en février, mai, août et novembre "
                              "(P105)"),
                  ("Proposition BI", "la date est en attente de la CNSS ; voir la question Q4")]),
    dict(n="3", titre="Les trop-perçus",
         regle="Les ajustements « trop-perçu » et « trop-perçu sur convention », établis dans le "
               "mois.",
         sources=[("Kaya 2017", "« les prestations indues sont des manques à gagner pour la "
                                "CNSS (prestations perçues en trop par les bénéficiaires) »"),
                  ("Base", "TP et TP-CONV"),
                  ("Données", "684 trop-perçus en 2024, 229,5 M ; 1 577 en 2025 (P102)")]),
    dict(n="4", titre="Les moins-perçus",
         regle="Les ajustements « moins-perçu ou rappel » et « rappel sur convention », établis "
               "dans le mois : ils restent dans le tableau, mais ne sont pas des indus.",
         sources=[("Kaya 2017", "« les moins perçus ne sont pas des prestations indues »"),
                  ("Base", "MP et RC"),
                  ("Données", "414 M en 2024 ; ceux des PF fondent : 56 362 en 2023, 1 384 en "
                              "2025 (P102)"),
                  ("Proposition BI", "les garder dans la maquette ; voir la question Q1")]),
    dict(n="5", titre="Ce qui n'est pas dans T31",
         regle="Le remboursement d'un trop-perçu, les avances et leur reprise, les mois de "
               "pension gratuits, les dons et avances COVID, les rachats de rente.",
         sources=[("Base", "REM-TP, AV, REV-AV, MPG, DON-COVID19, AV-COVID19, RR"),
                  ("Données", "les avances sont de loin les plus gros ajustements : 5,4 Md en "
                              "2024 (P102)")]),
    dict(n="6", titre="Les statuts",
         regle="Tous les ajustements, sauf ceux au statut R (annulés).",
         sources=[("Base", "quatre statuts sans libellé : A, C, I, R"),
                  ("Données", "C = soldé (rappel payé, trop-perçu repris) ; A = en cours ; R = "
                              "annulé (presque rien payé) ; depuis 2017, les trop-perçus restent "
                              "en cours, repris par retenues (P103)"),
                  ("Proposition BI", "écarter R ; voir la question Q5")]),
    dict(n="7", titre="Le mois",
         regle="Un trop-perçu ou un moins-perçu compte le mois où il est établi, pas le mois où "
               "il a été payé en trop, ni celui où il est repris.",
         sources=[("Base", "la date d'établissement de l'ajustement, toujours remplie (P103)"),
                  ("Proposition BI", "le mois d'établissement ; voir la question Q2")]),
    dict(n="8", titre="Le taux de prestations indues",
         regle="Les trop-perçus divisés par les payées, en pourcentage, par branche et sur "
               "l'année.",
         sources=[("Kaya 2017", "« montant des prestations indues ÷ montant des prestations "
                                "payées sur la période × 100 » ; norme CIPRES : 2 % au plus"),
                  ("Données", "0,39 % en 2024, quelle que soit la date des payées (P105)")],
         chiffre="En 2024 : PVID 0,47 %, AT/MP 0,47 %, PF 0,05 %."),
]

# Pour chaque règle, une requête courte sur les vraies tables de l'application (USER_DWH), pour
# l'année 2024, avec en commentaire le chiffre à retrouver. Chaque requête se lance seule.
SQL = {
    "1": """-- la branche vient du type de dossier
SELECT TDOS_CODE, COUNT(*)
FROM   USER_DWH.DOSSIER
WHERE  TDOS_CODE IN ('V', 'A', 'F', 'M')
GROUP  BY TDOS_CODE
-- V : PVID (pensions) ; A : AT/MP ;
-- F et M : PF (la maternité comprise)""",
    "2": """-- les payées de 2024 par branche : les
-- débours de paiement (PM) au statut D
-- (payé par effet) ou P (payé sans effet)
SELECT d.TDOS_CODE, SUM(deb.DEB_MONTANT)
FROM   USER_DWH.DEBOURS deb
JOIN   USER_DWH.DOSSIER d
       ON d.DOS_CODE = deb.DOS_CODE
WHERE  deb.DEB_TYPE = 'PM'
AND    deb.DEB_STATUT IN ('D', 'P')
-- au mois payé ; au mois du versement :
-- DEB_DATE_INSERT (réglage date_montant)
AND    deb.DEB_DATE_EFFET >= DATE '2024-01-01'
AND    deb.DEB_DATE_EFFET <  DATE '2025-01-01'
GROUP  BY d.TDOS_CODE
-- 2024 : 58,7 Md en tout""",
    "3": """SELECT d.TDOS_CODE, COUNT(*),
       SUM(aj.AJ_MONTANT) AS trop_percu
FROM   USER_DWH.AJUSTEMENT aj
JOIN   USER_DWH.DOSSIER d
       ON d.DOS_CODE = aj.DOS_CODE
-- trop-perçu, trop-perçu sur convention
WHERE  aj.TAJ_CODE IN ('TP', 'TP-CONV')
AND    aj.AJ_STATUT <> 'R'
AND    aj.AJ_DATE_ETABLISSEMENT
         >= DATE '2024-01-01'
AND    aj.AJ_DATE_ETABLISSEMENT
         <  DATE '2025-01-01'
GROUP  BY d.TDOS_CODE
-- 2024 : 684 trop-perçus, 229,5 M""",
    "4": """SELECT d.TDOS_CODE, COUNT(*),
       SUM(aj.AJ_MONTANT) AS moins_percu
FROM   USER_DWH.AJUSTEMENT aj
JOIN   USER_DWH.DOSSIER d
       ON d.DOS_CODE = aj.DOS_CODE
-- moins-perçu ou rappel, rappel sur
-- convention : montrés, pas des indus
WHERE  aj.TAJ_CODE IN ('MP', 'RC')
AND    aj.AJ_STATUT <> 'R'
AND    aj.AJ_DATE_ETABLISSEMENT
         >= DATE '2024-01-01'
AND    aj.AJ_DATE_ETABLISSEMENT
         <  DATE '2025-01-01'
GROUP  BY d.TDOS_CODE
-- 2024 : 414 M""",
    "5": """-- tous les types d'ajustement de 2024 ;
-- seuls TP, TP-CONV, MP et RC entrent
SELECT TAJ_CODE, COUNT(*), SUM(AJ_MONTANT)
FROM   USER_DWH.AJUSTEMENT
WHERE  AJ_DATE_ETABLISSEMENT
         >= DATE '2024-01-01'
AND    AJ_DATE_ETABLISSEMENT
         <  DATE '2025-01-01'
GROUP  BY TAJ_CODE
-- hors T31 : REM-TP remboursement d'un
-- trop-perçu, AV avance, MPG mois de
-- pension gratuits, …
-- libellés : USER_DWH.TYPE_AJUSTEMENT""",
    "6": """SELECT AJ_STATUT, COUNT(*)
FROM   USER_DWH.AJUSTEMENT
WHERE  TAJ_CODE IN ('TP', 'TP-CONV',
                    'MP', 'RC')
GROUP  BY AJ_STATUT
-- C soldé, A en cours : gardés
-- R annulé : écarté (AJ_STATUT <> 'R')""",
    "7": """-- un trop-perçu compte le mois où il est
-- établi, pas celui où il est repris
SELECT EXTRACT(MONTH FROM
         AJ_DATE_ETABLISSEMENT) AS mois,
       SUM(AJ_MONTANT)          AS trop_percu
FROM   USER_DWH.AJUSTEMENT
WHERE  TAJ_CODE IN ('TP', 'TP-CONV')
AND    AJ_STATUT <> 'R'
AND    AJ_DATE_ETABLISSEMENT
         >= DATE '2024-01-01'
AND    AJ_DATE_ETABLISSEMENT
         <  DATE '2025-01-01'
GROUP  BY EXTRACT(MONTH FROM
            AJ_DATE_ETABLISSEMENT)
ORDER  BY 1""",
    "8": """-- trop-perçus (règle 3) / payées
-- (règle 2), par branche, en %
SELECT branche,
       ROUND(100 * SUM(tp) / SUM(paye), 2)
         AS taux_indus_pct
FROM  (SELECT DECODE(d.TDOS_CODE, 'V', 'PVID',
                'A', 'AT/MP', 'F', 'PF',
                'M', 'PF') AS branche,
              aj.AJ_MONTANT AS tp, 0 AS paye
       FROM   USER_DWH.AJUSTEMENT aj
       JOIN   USER_DWH.DOSSIER d
              ON d.DOS_CODE = aj.DOS_CODE
       WHERE  aj.TAJ_CODE IN ('TP', 'TP-CONV')
       AND    aj.AJ_STATUT <> 'R'
       AND    EXTRACT(YEAR FROM
                aj.AJ_DATE_ETABLISSEMENT) = 2024
       UNION ALL
       SELECT DECODE(d.TDOS_CODE, 'V', 'PVID',
                'A', 'AT/MP', 'F', 'PF',
                'M', 'PF'),
              0, deb.DEB_MONTANT
       FROM   USER_DWH.DEBOURS deb
       JOIN   USER_DWH.DOSSIER d
              ON d.DOS_CODE = deb.DOS_CODE
       WHERE  deb.DEB_TYPE = 'PM'
       AND    deb.DEB_STATUT IN ('D', 'P')
       AND    EXTRACT(YEAR FROM
                deb.DEB_DATE_EFFET) = 2024)
WHERE  branche IS NOT NULL
GROUP  BY branche
-- 2024 : PVID 0,47 %, AT/MP 0,47 %,
-- PF 0,05 % ; en tout 0,39 %""",
}

SQL_NOTE = ("À droite de chaque règle, une requête courte sur les vraies tables de l'application "
            "(schéma USER_DWH), pour l'année 2024, à copier dans SQL Developer ; chacune se "
            "lance seule. Les payées viennent des débours (DEBOURS), les trop-perçus et les "
            "moins-perçus des ajustements (AJUSTEMENT), la branche du type de dossier (DOSSIER). "
            "Le commentaire en fin de requête donne le chiffre à retrouver. La requête "
            "complète, sur les copies de l'entrepôt, est dans "
            "analyses/prestations/t31_requete_proposee.sql.")

QUESTIONS = [
    dict(n="Q1", titre="Les moins-perçus",
         regle="Kaya 2017 dit que les moins-perçus ne sont pas des indus, mais la maquette leur "
               "garde une colonne. Faut-il la remplir ?",
         cas=[("La remplir", "414 M en 2024, à côté des trop-perçus, sans entrer dans le taux."),
              ("La laisser vide", "le tableau ne montre que les indus.")],
         sources=[("Annuaire", "p. 36 : colonne « moins perçus » dans chaque branche"),
                  ("Proposition BI", "la remplir, hors du taux")]),
    dict(n="Q2", titre="Le mois d'un indu",
         regle="Un trop-perçu se compte-t-il le mois où il est établi, ou les mois où la "
               "prestation a été payée en trop ?",
         cas=[("Le mois de l'établissement", "une seule date, toujours remplie."),
              ("Les mois payés en trop", "la base ne donne pas cette période.")],
         sources=[("Données", "date d'établissement toujours remplie (P103)"),
                  ("Proposition BI", "le mois de l'établissement")]),
    dict(n="Q3", titre="Les pensions payées après un décès",
         regle="Une pension versée à une personne décédée, dont le décès est déclaré tard, "
               "devient-elle un trop-perçu ?",
         cas=[("Si oui", "il manque des trop-perçus : 7 seulement en 2024 après un décès."),
              ("Si non", "comment ces sommes sont-elles reprises ?")],
         sources=[("Données", "175 décès de retraités de 2024 déclarés après mars 2025 (P99, "
                              "P106)"),
                  ("Proposition BI", "à préciser avec la direction des prestations")]),
    dict(n="Q4", titre="La date des payées",
         regle="Les payées d'un mois sont-elles celles du mois payé, ou celles versées dans le "
               "mois ? Les rappels et avances versés par ajustement en font-ils partie ?",
         cas=[("Le mois payé", "une série régulière, 3,9 Md par mois pour la PVID."),
              ("Le mois du versement", "des pics en février, mai, août et novembre.")],
         sources=[("Données", "nos requêtes P98, P104 et P105"),
                  ("Proposition BI", "en attente de la CNSS : un réglage dans la requête")]),
    dict(n="Q5", titre="Les statuts et le recouvrement",
         regle="Le statut R veut-il dire annulé ? Depuis 2017, aucun trop-perçu n'est soldé : "
               "comment savoir qu'un indu est recouvré ?",
         cas=[("R = annulé", "il sort de T31 ; c'est la règle 6."),
              ("Le recouvrement", "à suivre par les retenues, pas par le statut.")],
         sources=[("Données", "statuts lus d'après les paiements liés (P103)"),
                  ("Proposition BI", "écarter R ; le recouvrement n'entre pas dans T31")]),
]

BI_CHECKS = ("Le taux d'indus de 2024 est de 0,39 % : 229,5 M de trop-perçus pour 58,7 Md payés, "
             "très loin de la norme de 2 % (lot 9, P105).\n"
             "Les statuts se lisent d'après les paiements liés : C soldé, A en cours, R annulé "
             "(P103).\n"
             "Les paiements d'ajustement versent surtout des avances, des rappels et des rachats "
             "de rente : ils ne sont pas des indus (P104).\n"
             "L'annuaire de la CNSS ne publie pas d'indus : pas de contrôle externe.")

# taux d'indus de 2024 par branche (lot 9, P105), en % ; la norme CIPRES est 2 %
TAUX_2024 = [("PVID", 0.47), ("AT/MP", 0.47), ("PF", 0.045), ("Total", 0.39)]
NORME = 2.0


def figures(d, x, y, w):
    """Encadré des premiers chiffres : le taux d'indus de 2024 face à la norme de 2 %."""
    g = d.group()
    h = 520
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + 20, y + 16, "En 2024 : le taux d'indus face à la norme", 22, groups=g,
           inside=frame)
    d.text(x + 20, y + 54, "229,5 M de trop-perçus pour 58,7 Md payés : 0,39 %.\n"
                           "Norme CIPRES : 2 % au plus.", 18, groups=g, inside=frame)
    by = y + 130
    scale = (w - 260) / NORME
    for label, taux in TAUX_2024:
        d.text(x + 20, by + 6, label, 16, groups=g, inside=frame)
        d.box(x + 130, by, max(taux * scale, 4), 30, bg=C["vert_moyen"], stroke=C["vert"],
              groups=g)
        d.text(x + 140 + max(taux * scale, 4), by + 6, f"{taux:.2f} %".replace(".", ","), 16,
               groups=g, inside=frame)
        by += 50
    nx = x + 130 + NORME * scale
    d.arrow(nx, y + 120, nx, by, color=C["rouge"], style="dashed", head=None)
    d.text(nx - 150, by + 6, "norme : 2 %", 16, C["rouge"], groups=g, inside=frame)
    note = wrap("Le taux ne change pas selon la date retenue pour les payées : 0,39 % au mois "
                "payé comme au mois du versement.", 16, w - 40)
    d.text(x + 20, by + 44, note, 16, C["doux"], groups=g, inside=frame)
    by += 44 + height_of(note, 16) + 12
    w_chip = chip(d, x + 20, by, "Données", groups=g)
    d.text(x + 20 + w_chip + 10, by + 5, "lot 9, P102 à P106 ; pas de chiffre publié", 16,
           groups=g, inside=frame)
    return y + h


def build():
    d = Diagram(seed=31)
    inner = WIDTH - 50

    d.text(0, 0, "T31 · Les prestations indues, par branche et par mois", 48)
    d.text(0, 66, "« Répartition des prestations indues par branche et par mois » · ce que l'équipe "
                  "BI a compris, à valider · état au 01/11/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Le tableau en un coup d'œil")
    top = y + 65
    g = d.group()
    about = d.box(25, top, 760, 290, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi sert T31", 22, groups=g, inside=about)
    text = wrap("Il suit chaque mois, dans chaque branche, ce que la CNSS a payé et ce qu'elle a "
                "payé en trop (les indus) ou en moins. Le rapport des indus aux payées donne le "
                "taux de prestations indues, un indicateur CIPRES.", 18, 720)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 13, maquette p. 36 ; fin d'année"),
                      ("Kaya 2017", "indus = trop-perçus ; norme 2 % au plus")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(d, 25, top + 310, 760)

    mx, mw = 830, WIDTH - 25 - 830
    columns, groups = [], []
    for i, (branche, regle) in enumerate([("PVID", "1"), ("AT/MP", "1"), ("PF", "1"),
                                          ("TOTAL", "8")]):
        columns += [("Payées", "2"), ("Trop perçus", "3"), ("Moins perçus", "4")]
        groups.append((3 * i, 3 * i + 2, branche, regle))
    bottom = mockup(
        d, mx, top, 220, (mw - 220) / 12, "Mois", "7",
        columns=columns, groups=groups,
        rows=[(m, None, None) for m in ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
                                         "Juillet", "Août", "Septembre", "Octobre",
                                         "Novembre", "Décembre"]] + [("TOTAL", None, None)],
        row_h=40)
    d.text(mx, bottom + 14, wrap("Payées : ce que la CNSS a décaissé (règle 2) ; trop perçus : "
                                 "les indus (règle 3) ; moins perçus : les rappels, qui ne sont "
                                 "pas des indus (règle 4). La BI ajoute le taux de chaque mois "
                                 "et de l'année (règle 8).", 18, mw), 18, C["doux"])
    sec["height"] = max(bottom + 110, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Ce qui entre dans T31 : six cas fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2024, 11, 1), date(2026, 2, 28), 2025,
                      EXAMPLES, outcomes=SORTIES,
                      axis_label="Dans le calendrier : du paiement en trop ou en moins à "
                                 "l'ajustement")
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes et les colonnes")
    w = chip(d, 25, y + 72, "Annuaire")
    d.text(25 + w + 12, y + 76, "Une ligne par mois, de janvier à décembre, et une ligne TOTAL ; "
                                "trois colonnes par branche (maquette p. 36).", 18)
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
