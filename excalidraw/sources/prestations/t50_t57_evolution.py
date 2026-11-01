"""
Fiche de validation de T50 à T57 « Évolution sur 5 ans » et « Récapitulatifs par branche » (état
au 01/11/2026) : fiche pilote de la famille.

Même gabarit que T32 et T42, avec trois maquettes : T50 (une prestation suivie sur 5 ans,
effectif, montant et services passés, par sexe ; T51 à T55 ont la même forme sans services
passés), T56 (les prestations servies par branche) et T57 (les assurés décédés par branche). Six
cas fictifs sur un calendrier, les lignes, huit règles avec leur SQL et cinq questions. La date
des montants (règle R4) est en attente de la CNSS : la fiche montre les deux possibilités.

Sources : Annuaire CIPRES (p. 16, 46 à 48) ; atelier de Kaya 2017 (T54, T55 : les liquidées ;
T57 : assurés décédés déclarés) ; annuaire de la CNSS 2024 (tableaux 24, 26, 34, 35, 40, 81, 97,
99) ; analyses/prestations (lot 8, P92 à P101) ; t50_t57_requete_proposee.sql.

Usage :
    python3 excalidraw/sources/prestations/t50_t57_evolution.py [--force]
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

OUTPUT = ROOT / "schemas" / "prestations" / "tableaux cipres" / "T50_T57_evolution_5_ans.excalidraw"

HOW_TO = ("Pour chaque règle (partie 4) et chaque question (partie 5), dessine une croix dans une "
          "case : D'accord, Pas d'accord ou À discuter, et écris ta remarque sous la carte si "
          "besoin. Enregistre avec Cmd+S : chaque version est gardée dans l'historique.\n"
          "C'est la fiche pilote de T50 à T57 : T50 à T55 suivent chacun une prestation sur 5 "
          "ans (allocations de vieillesse et de survivants, IJ et allocations d'incapacité des "
          "AT/MP, IJ de maternité, allocations prénatales) ; T56 et T57 récapitulent par branche.")

# sort d'un cas fictif dans le calendrier, pour l'exercice 2025
SORTIES = {
    "compte": (C["vert_moyen"], C["vert"], "compté en 2025"),
    "montant": (C["bleu_moyen"], C["bleu"], "dans le montant de 2025 seulement"),
    "exclu": (C["gris"], C["gris_bord"], "pas compté en 2025"),
}

EXAMPLES = [
    dict(nom="Allocation A", detail="allocation de vieillesse", debut=date(2025, 3, 10),
         fin=date(2025, 4, 30), sortie="compte",
         dates="liquidée le 10/03/2025, payée en avril 2025",
         resultat="T50 de 2025 : comptée ; montant : l'allocation payée en avril ; services "
                  "passés : ses mois cotisés à la CNSS."),
    dict(nom="Allocation B", detail="liquidée en 2024, payée en 2025", debut=date(2024, 11, 20),
         fin=date(2025, 1, 31), sortie="montant",
         dates="liquidée le 20/11/2024, payée en janvier 2025",
         resultat="Effectif : T50 de 2024, l'année de la liquidation (règle 2) ; son montant va "
                  "en 2025 (règle 4)."),
    dict(nom="Mère C", detail="IJ de maternité", debut=date(2025, 6, 2), fin=date(2025, 9, 30),
         sortie="compte",
         dates="grossesse déclarée en juin, IJ liquidée en juillet 2025",
         resultat="T54 de 2025 : comptée une fois ; montant : ses IJ payées en 2025."),
    dict(nom="Mère D", detail="IJ liquidée en 2024", debut=date(2024, 11, 15),
         fin=date(2025, 2, 28), sortie="montant",
         dates="IJ liquidée en novembre 2024, payée jusqu'en février 2025",
         resultat="Effectif : T54 de 2024 (Kaya 2017 : les liquidées) ; ses paiements de 2025 "
                  "vont dans le montant de 2025."),
    dict(nom="Retraité E", detail="décès déclaré tard", debut=date(2025, 6, 12),
         fin=date(2026, 5, 10), sortie="exclu",
         dates="décédé le 12/06/2025, décès saisi en mai 2026",
         resultat="Absent de T57 de 2025 : calculé sur le cliché de fin mars 2026 (règle 8), son "
                  "décès n'est pas encore connu, comme dans l'annuaire."),
    dict(nom="Victime F", detail="accident du travail mortel", debut=date(2025, 2, 3),
         fin=date(2025, 4, 15), sortie="compte",
         dates="accident le 03/02/2025, décès le 15/04/2025",
         resultat="T57 de 2025, branche AT/MP : un sinistré, mort dans l'année de son accident "
                  "(règle 7)."),
]

LINES = [
    (1, "Les années N-4 à N", "T50 à T55 et T57 : une ligne par année ; T56 : une colonne",
     "les années passées sont recalculées sur la base d'aujourd'hui (un seul cliché)",
     "Données"),
    (2, "La ligne TOTAL", "T50 à T55 et T57 : la somme des 5 années",
     "demandée par Kaya 2017 pour T54, T55 et T57", "Kaya 2017"),
    (3, "T56 : les branches", "PF, AT/MP, AVS, Autres et Total",
     "« Autres » reste vide : tous les paiements ont une des trois branches (P95)", "Données"),
    (4, "T57 : les branches en colonnes", "PF, AT/MP, AVS, Autres, et un Total",
     "Kaya 2017 ajoute la colonne et la ligne Total", "Kaya 2017"),
]

RULES = [
    dict(n="1", titre="Les prestations de chaque tableau",
         regle="T50 allocations de vieillesse ; T51 allocations de survivants ; T52 IJ AT/MP ; "
               "T53 allocations d'incapacité ; T54 IJ de maternité ; T55 allocations prénatales.",
         sources=[("Annuaire", "p. 16 : les six tableaux d'évolution"),
                  ("Base", "RAV ; RASC, RASO, RASA (les parts : conjoint, orphelin, ascendant) ; "
                           "AIJ ; AAI ; MIJ ; MAP et FAP"),
                  ("Données", "RAS ne se liquide plus : ses parts portent les personnes (P93)")]),
    dict(n="2", titre="Le bénéficiaire de l'année",
         regle="La personne dont la prestation est liquidée dans l'année : notifiée pour la "
               "première fois, comme pour T25.",
         sources=[("Kaya 2017", "T54 et T55 : les indemnités et allocations « liquidées et non "
                                "payées »"),
                  ("Données", "c'est ainsi que compte l'annuaire de la CNSS : allocations de "
                              "vieillesse 831 pour 833, IJ de maternité 3 890 pour 3 893 en 2024 ; "
                              "les payées en donnent 25 % de plus (P92)")],
         chiffre="De 2020 à 2024, les IJ de maternité retombent à moins de 1 % près, sauf 2022."),
    dict(n="3", titre="La personne comptée",
         regle="Une fois par tableau et par année, avec son sexe ; T54 et T55 : les femmes "
               "seulement.",
         sources=[("Annuaire", "p. 47 : T54 et T55 n'ont qu'une colonne FEMMES"),
                  ("Données", "une IJ AT/MP se liquide en plusieurs fois : 371 IJ pour 195 "
                              "victimes en 2024 ; on compte la victime (P93)"),
                  ("Proposition BI", "la personne, pas la prestation ; voir la question Q5")]),
    dict(n="4", titre="Le montant",
         regle="Ce qui a été payé dans l'année : les débours de paiement au statut D ou P. "
               "L'année d'un paiement est celle du mois qu'il paie, ou celle où il est versé.",
         sources=[("Annuaire", "p. 16 : « les montants servis »"),
                  ("Données", "la date du versement retombe mieux sur l'annuaire (11 lignes sur "
                              "16 ; allocations de vieillesse : 0,0 % contre +3,9 %) (P98)"),
                  ("Proposition BI", "en attente de la CNSS : un réglage dans la requête ; voir "
                                     "la question Q1")],
         chiffre="En 2024 : 497,8 M d'allocations de vieillesse versés pour 497,7 M publiés."),
    dict(n="5", titre="Les services passés moyens (T50)",
         regle="La moyenne des mois cotisés à la CNSS de la dernière allocation liquidée de la "
               "personne, par sexe : en coordination, le total moins les mois dans l'autre caisse.",
         sources=[("Annuaire", "p. 46 : « nombre moyen de services passés »"),
                  ("Données", "toujours remplis ; 96 mois en moyenne en 2024, 8 ans (P94)")]),
    dict(n="6", titre="T56 : les prestations servies par branche",
         regle="Les paiements et remboursements de l'année, par branche : PF (familles et "
               "maternité), AT/MP, AVS (pensions et allocations). Pas les ajustements.",
         sources=[("Base", "chaque paiement porte sa branche, celle de son dossier (P07)"),
                  ("Données", "AVS +1,3 %, PF −3,8 % ; AT/MP −29 % : les prestations en nature "
                              "des AT/MP ne sont pas dans les paiements (P95, P98)"),
                  ("Proposition BI", "les ajustements corrigent des paiements (T31) ; voir la "
                                     "question Q3")]),
    dict(n="7", titre="T57 : les assurés décédés par branche",
         regle="AVS : les titulaires d'une pension directe ; AT/MP : les rentiers et les "
               "victimes mortes dans l'année de leur accident ; PF : les allocataires.",
         sources=[("Kaya 2017", "« assurés décédés déclarés » : pension directe, rentier et "
                                "sinistré, allocataire"),
                  ("Données", "en 2024 : 622 titulaires de pension directe, 16 rentiers, 10 "
                              "victimes, 194 allocataires décédés (P97)"),
                  ("Proposition BI", "voir la question Q4")]),
    dict(n="8", titre="Les 5 années et la date du calcul",
         regle="Les années N-4 à N, chacune avec les mêmes règles, sur le cliché de fin mars "
               "N+1, quand la CNSS arrête son annuaire.",
         sources=[("Données", "les décès de retraités de 2024 connus fin mars 2025 retombent "
                              "mois par mois sur l'annuaire : 444 pour 447 (P99)"),
                  ("Proposition BI", "nouvelle règle R13 ; voir la question Q2")]),
]

# Traduction en SQL de chaque règle : extraits de analyses/prestations/t50_t57_requete_proposee.sql
SQL = {
    "1": """SELECT 'T50' AS tableau, 'RAV' AS code
FROM DUAL
UNION ALL SELECT 'T51', 'RASC' FROM DUAL
UNION ALL SELECT 'T51', 'RASO' FROM DUAL
-- … T52 AIJ, T53 AAI, T54 MIJ,
-- T55 FAP et MAP""",
    "2": """-- l'année de la première notification
EXTRACT(YEAR FROM p.PE_DATE_NOTIFICATION)
                  AS annee_liquidation
-- … dans les 5 années
WHERE annee_liquidation IN
      (SELECT annee FROM annees)""",
    "3": """ROW_NUMBER() OVER (
  PARTITION BY tableau, annee_liquidation,
               IND_ID_PREST
  ORDER BY PE_DATE_NOTIFICATION DESC)
                  AS rang
-- une personne par tableau et par année
WHERE rang = 1""",
    "4": """-- date_montant : 'MOIS' ou 'SAISIE'
EXTRACT(YEAR FROM
  CASE pr.date_montant
       WHEN 'SAISIE' THEN deb.DEB_DATE_INSERT
       ELSE deb.DEB_DATE_EFFET END) AS annee
-- débours de paiement D ou P
WHERE deb.DEB_TYPE = 'PM'
  AND deb.DEB_STATUT IN ('D', 'P')""",
    "5": """AVG(CASE WHEN mois BETWEEN 1 AND 600
         THEN GREATEST(mois
              - NVL(mois_autre_caisse, 0), 0)
    END)          AS services_moyens""",
    "6": """CASE deb.CODE_BRANCHE
     WHEN 'V' THEN 'AVS'
     WHEN 'A' THEN 'AT/MP'
     WHEN 'F' THEN 'PF'
     WHEN 'M' THEN 'PF'
     ELSE          'AUTRES'
END AS branche
-- paiements et remboursements
WHERE deb.DEB_TYPE IN ('PM', 'RB')""",
    "7": """-- pension directe en cours au décès
p.TPE_CODE IN ('RPV', 'RPRA', 'RPI')
AND p.PE_DATE_EFFET <= de.date_deces
-- sinistré : mort dans l'année
ds.DOS_DATE_ACCIDENT <= de.date_deces
AND de.date_deces
    < ds.DOS_DATE_ACCIDENT + 366""",
    "8": """-- les années N-4 à N
SELECT pr.annee_n - 5 + LEVEL AS annee
FROM parametres pr
CONNECT BY LEVEL <= 5
-- :1 = le cliché de fin mars N+1""",
}

SQL_NOTE = ("À droite de chaque règle, sa traduction en SQL, pour la DSI et l'équipe BI : un "
            "extrait de la requête commune à T50-T57 (analyses/prestations/"
            "t50_t57_requete_proposee.sql), en trois parties : T50 à T55, T56, T57. Elle lit les "
            "copies de l'entrepôt (DWH.FAIT_…) du cliché choisi ; l'année N est un paramètre.")

QUESTIONS = [
    dict(n="Q1", titre="L'année d'un paiement",
         regle="Dans l'annuaire, un paiement compte-t-il pour l'année du mois qu'il paie, ou "
               "pour l'année où la CNSS le verse, rappels compris ?",
         cas=[("L'année du versement",
               "la base retombe mieux sur l'annuaire : allocations de vieillesse exactes."),
              ("L'année du mois payé",
               "un rappel de 15 mois versé en mars 2024 va pour 12 mois en 2023.")],
         sources=[("Données", "notre requête P98, ligne par ligne"),
                  ("Proposition BI", "l'année du versement ; en attendant, un réglage dans la "
                                     "requête")]),
    dict(n="Q2", titre="La date d'arrêt de l'annuaire",
         regle="L'annuaire d'une année est-il arrêté vers la fin mars de l'année suivante ?",
         cas=[("Si oui", "la BI calcule chaque exercice sur le cliché de fin mars : ses chiffres "
                         "retombent sur les vôtres."),
              ("Si non", "il faut la bonne date, sinon les décès et les radiations déclarés "
                         "tard creusent des écarts.")],
         sources=[("Données", "décès de 2024 connus fin mars 2025 : 444 pour 447 publiés (P99)"),
                  ("Proposition BI", "le cliché de fin mars N+1")]),
    dict(n="Q3", titre="T56 : ce qu'on additionne",
         regle="Où sont payées les prestations en nature des AT/MP ? Les ajustements de "
               "paiement entrent-ils dans les prestations servies ?",
         cas=[("Les prestations en nature",
               "139,7 M publiés en 2024 ; introuvables dans les paiements de la base."),
              ("Les ajustements", "0,8 Md en 2024 (surtout l'AVS) : des corrections, pas des "
                                  "prestations.")],
         sources=[("Données", "nos requêtes P95 et P98"),
                  ("Proposition BI", "paiements et remboursements, sans les ajustements")]),
    dict(n="Q4", titre="T57 : qui compter",
         regle="Comptez-vous les seuls titulaires d'une pension directe, ou tous les "
               "pensionnés décédés, veuves et orphelins compris ?",
         cas=[("Pension directe (Kaya 2017)", "622 décès en 2024 sur la base d'aujourd'hui."),
              ("Tous les pensionnés", "747 ; l'annuaire de la CNSS en publie 648, dont 89 "
                                      "ascendants pour 59 pensions d'ascendant en cours.")],
         sources=[("Kaya 2017", "pension directe, rentier et sinistré, allocataire"),
                  ("Proposition BI", "suivre Kaya 2017")]),
    dict(n="Q5", titre="Personnes ou prestations",
         regle="Un bénéficiaire qui reçoit plusieurs IJ ou allocations dans l'année compte-t-il "
               "une fois ou à chaque prestation ?",
         cas=[("Une fois", "195 victimes avec une IJ AT/MP liquidée en 2024."),
              ("À chaque prestation", "371 IJ liquidées.")],
         sources=[("Données", "notre requête P93"),
                  ("Proposition BI", "une fois : le tableau compte des bénéficiaires")]),
]

BI_CHECKS = ("Le bénéficiaire de l'année est la prestation liquidée : allocations de vieillesse "
             "et IJ de maternité à 0,2 % de l'annuaire en 2024 (lot 8, P92).\n"
             "Les montants retombent à 1 à 4 % près, sauf les AT/MP (−12 à −29 %), quelle que "
             "soit la date (P95, P98).\n"
             "L'annuaire est arrêté vers la fin mars de l'année suivante (P99) ; la base n'a pas "
             "de date de déclaration du décès (P96).\n"
             "La vue des décédés préparée par la DSI prend tous les prestataires et des données "
             "nominatives : T57 garde ses propres filtres (P101).")

# IJ de maternité, 2020 à 2024 : publiées par la CNSS (annuaire 2024, tableau 35) et liquidées
# dans la base (lot 8, P92)
MATERNITE = [("2020", 3428, 3438), ("2021", 3810, 3810), ("2022", 3973, 3834),
             ("2023", 4050, 4069), ("2024", 3893, 3890)]


def figures(d, x, y, w):
    """Encadré des premiers chiffres : les IJ de maternité publiées par la CNSS et les nôtres."""
    g = d.group()
    h = 600
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(x + 20, y + 16, "La base face à l'annuaire de la CNSS", 22, groups=g, inside=frame)
    d.text(x + 20, y + 54, "En 2024 : 831 allocations de vieillesse liquidées chez nous,\n"
                           "833 publiées ; 497,8 M versés pour 497,7 M publiés.", 18, groups=g,
           inside=frame)
    d.text(x + 20, y + 116, "Bénéficiaires d'IJ de maternité :", 16, C["doux"], groups=g,
           inside=frame)
    lx = x + 340
    d.box(lx, y + 118, 18, 14, bg=C["bleu_moyen"], stroke=C["bleu"], groups=g)
    d.text(lx + 26, y + 116, "annuaire de la CNSS", 16, groups=g, inside=frame)
    d.box(lx + 210, y + 118, 18, 14, bg=C["vert_moyen"], stroke=C["vert"], groups=g)
    d.text(lx + 236, y + 116, "la base (P92)", 16, groups=g, inside=frame)
    by = y + 152
    biggest = max(max(a, b) for _, a, b in MATERNITE)
    scale = (w - 260) / biggest
    for label, cnss, nous in MATERNITE:
        d.text(x + 20, by + 13, label, 16, groups=g, inside=frame)
        for k, (n, bg, stroke) in enumerate([(cnss, C["bleu_moyen"], C["bleu"]),
                                             (nous, C["vert_moyen"], C["vert"])]):
            top = by + k * 24
            d.box(x + 130, top, max(n * scale, 4), 20, bg=bg, stroke=stroke, groups=g)
            d.text(x + 140 + max(n * scale, 4), top, f"{n:,}".replace(",", " "), 15, groups=g,
                   inside=frame)
        by += 56
    note = wrap("Compter les IJ payées au lieu des liquidées en donnerait 25 % de plus chaque "
                "année.", 16, w - 40)
    d.text(x + 20, by + 8, note, 16, C["doux"], groups=g, inside=frame)
    by += 8 + height_of(note, 16) + 12
    w_chip = chip(d, x + 20, by, "Données", groups=g)
    d.text(x + 20 + w_chip + 10, by + 5, "annuaire CNSS 2024, tableaux 24, 34, 35 ; P92", 16,
           groups=g, inside=frame)
    return y + h


def build():
    d = Diagram(seed=50)
    inner = WIDTH - 50

    d.text(0, 0, "T50 à T57 · L'évolution sur 5 ans et les récapitulatifs par branche", 48)
    d.text(0, 66, "« Évolution du nombre des bénéficiaires et des montants au cours des 5 dernières "
                  "années » · fiche pilote de T50 à T57 · ce que l'équipe BI a compris, à valider · "
                  "état au 01/11/2026", 22, C["doux"])
    y = banner(d, 120, HOW_TO) + 40

    # ------------------------------------------------------------------ 1. coup d'œil
    sec = d.section(0, y, WIDTH, 0, "1. Les tableaux en un coup d'œil")
    top = y + 65
    g = d.group()
    about = d.box(25, top, 760, 290, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, top + 16, "À quoi servent T50 à T57", 22, groups=g, inside=about)
    text = wrap("T50 à T55 suivent chacun une prestation sur 5 ans : combien de bénéficiaires "
                "chaque année, par sexe, et ce qui leur a été servi. T56 récapitule les montants "
                "par branche, T57 les assurés décédés par branche.", 18, 720)
    d.text(45, top + 54, text, 18, groups=g, inside=about)
    cy = top + 54 + height_of(text, 18) + 16
    for tag, line in [("Annuaire", "description p. 16, maquettes p. 46 à 48"),
                      ("Kaya 2017", "T54, T55 : les liquidées ; T57 : décédés déclarés")]:
        w = chip(d, 45, cy, tag, groups=g)
        d.text(45 + w + 10, cy + 4, line, 18, groups=g, inside=about)
        cy += 36
    fbottom = figures(d, 25, top + 310, 760)

    mx, mw = 830, WIDTH - 25 - 830
    d.text(mx, top, "T50 : les allocations de vieillesse (T51 à T55 : la même forme, sans "
                    "services passés)", 20, C["doux"])
    bottom = mockup(
        d, mx, top + 40, 260, (mw - 260) / 6, "Années", "8",
        columns=[("Effectif", "2 et 3"), ("Montant", "4"), ("Services passés moyens", "5"),
                 ("Effectif", "2 et 3"), ("Montant", "4"), ("Services passés moyens", "5")],
        groups=[(0, 2, "HOMMES", "3"), (3, 5, "FEMMES", "3")],
        rows=[("N-4", None, None), ("N-3", None, None), ("N-2", None, None),
              ("N-1", None, None), ("N", None, None), ("TOTAL", None, None)],
        row_h=40)
    d.text(mx, bottom + 30, "T56 : les prestations servies par branche", 20, C["doux"])
    bottom = mockup(
        d, mx, bottom + 70, 260, (mw - 260) / 5, "Branche", "6",
        columns=[("N-4", "4"), ("N-3", "4"), ("N-2", "4"), ("N-1", "4"), ("N", "4")],
        groups=[(0, 4, "MONTANTS SERVIS", "4")],
        rows=[("PF", None, None), ("AT/MP", None, None), ("AVS", None, None),
              ("Autres", None, None), ("TOTAL", None, None)],
        row_h=40)
    d.text(mx, bottom + 30, "T57 : les assurés décédés par branche", 20, C["doux"])
    bottom = mockup(
        d, mx, bottom + 70, 260, (mw - 260) / 5, "Années", "8",
        columns=[("PF", "7"), ("AT/MP", "7"), ("AVS", "7"), ("Autres", "7"), ("Total", "7")],
        groups=[(0, 4, "DÉCÉDÉS", "7")],
        rows=[("N-4", None, None), ("N-3", None, None), ("N-2", None, None),
              ("N-1", None, None), ("N", None, None), ("TOTAL", None, None)],
        row_h=40)
    sec["height"] = max(bottom + 40, fbottom) + 30 - sec["y"]

    # ------------------------------------------------------------------ 2. exemples
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "2. Qui est compté : six cas fictifs")
    bottom = timeline(d, 25, y + 70, inner, date(2024, 11, 1), date(2026, 5, 31), 2025,
                      EXAMPLES, outcomes=SORTIES,
                      axis_label="Dans le calendrier : de la liquidation au paiement, ou du "
                                 "décès à sa saisie")
    sec["height"] = bottom + 30 - sec["y"]

    # ------------------------------------------------------------------ 3. lignes
    y = sec["y"] + sec["height"] + 40
    sec = d.section(0, y, WIDTH, 0, "3. Les lignes : les années et les branches")
    w = chip(d, 25, y + 72, "Annuaire")
    d.text(25 + w + 12, y + 76, "T50 à T55 et T57 : une ligne par année, de N-4 à N ; T56 : une "
                                "ligne par branche et une colonne par année (maquettes p. 46 à "
                                "48).", 18)
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
