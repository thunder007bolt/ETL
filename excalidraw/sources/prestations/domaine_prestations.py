"""
Schéma « Le domaine Prestations, expliqué simplement » (état au 29/10/2026).

Sources : Annuaire CIPRES (T25 à T57), analyses/prestations (lots 1 et 2, règles R1 à R12).

Usage :
    python3 excalidraw/sources/prestations/domaine_prestations.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "domaine_prestations.excalidraw"


def build():
    d = Diagram(seed=26)

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "Le domaine Prestations, expliqué simplement", 48)
    d.text(0, 66, "CNSS Burkina Faso · tableaux CIPRES T25 à T57 · état au 29/10/2026", 22, C["doux"])

    # ------------------------------------------------------------------ 1. idée de base
    d.section(0, 120, 1250, 520, "1. L'idée de base : l'argent entre, l'argent sort")
    emp = d.box(40, 230, 280, 150, "Employeurs\net travailleurs", 26, bg=C["gris"])
    cnss = d.box(485, 230, 280, 150, "CNSS\nla caisse commune", 26, bg=C["violet"])
    ben = d.box(930, 230, 280, 150, "Bénéficiaires\nretraités, veuves,\nvictimes, familles", 24,
                bg=C["jaune"])
    d.harrow(emp, cnss, stroke_width=3)
    d.harrow(cnss, ben, stroke_width=3)
    d.text((320 + 485) / 2, 268, "cotisations", 22, C["doux"], align="center")
    d.text((765 + 930) / 2, 268, "prestations", 22, C["vert"], align="center")
    d.box(222.5, 420, 360, 90, "L'argent qui ENTRE\n= domaine Recouvrement", 22, bg=C["blanc"],
          stroke=C["gris_bord"], style="dashed")
    d.box(607.5, 420, 480, 90, "L'argent qui SORT\n= domaine PRESTATIONS (le nôtre)", 22,
          bg=C["vert_clair"], stroke=C["vert"], stroke_width=3)
    d.box(40, 545, 1170, 65,
          "Une prestation, c'est l'argent que la CNSS verse à une personne qui y a droit.", 24,
          bg=C["jaune_clair"], stroke=C["jaune_bord"])

    # ------------------------------------------------------------------ 2. branches
    d.section(1300, 120, 1300, 520, "2. Les 3 branches (les familles de prestations)")
    branches = [
        ("PVID\npensions de vieillesse,\nd'invalidité et de décès", "bleu",
         "Quand ? À la retraite, en cas\nd'invalidité ou au décès\nde l'assuré\n\nExemples :\n"
         "• pension de retraite\n• pension d'invalidité\n• pension de veuf ou de veuve\n"
         "• pension d'orphelin\n• pension d'ascendant (parent)\n\nCode des dossiers : V"),
        ("AT/MP\naccidents du travail,\nmaladies professionnelles", "orange",
         "Quand ? Un accident ou une\nmaladie causés par le travail\n\nExemples :\n"
         "• indemnités journalières\n  pendant l'arrêt\n• rente à vie si séquelle\n"
         "• allocation d'incapacité\n• rentes de survivants\n  si la victime décède\n\n"
         "Code des dossiers : A"),
        ("PF\nprestations familiales\net maternité", "vert",
         "Quand ? On a des enfants,\nou on attend un bébé\n\nExemples :\n• allocations familiales\n"
         "• allocations prénatales\n• indemnités de maternité\n\nCodes des dossiers : F et M\n"
         "(M = maternité, rangée\ndans les PF)"),
    ]
    for i, (head, color, body) in enumerate(branches):
        x = 1325 + i * 420
        g = d.group()
        d.box(x, 185, 395, 100, head, 22, bg=C[f"{color}_moyen"], stroke=C[color], stroke_width=3,
              groups=g)
        b = d.box(x, 285, 395, 335, bg=C[f"{color}_clair"], stroke=C[color], groups=g)
        d.text(x + 18, 301, body, 20, groups=g, inside=b)

    # ------------------------------------------------------------------ 3. qui reçoit
    d.section(0, 690, 1250, 620, "3. Qui reçoit l'argent ?")
    insured = d.box(425, 770, 400, 130, "L'ASSURÉ\nla personne qui a travaillé\net cotisé", 22,
                    bg=C["gris"])
    receivers = [
        ("PENSIONNÉ\n\npension de retraite\nou d'invalidité\n\n(PVID)", "bleu_moyen", "bleu"),
        ("SURVIVANTS\nsi l'assuré décède :\n\nveuf ou veuve\norphelin\nascendant (père, mère)\n\n(PVID)",
         "bleu_clair", "bleu"),
        ("CRÉDIRENTIER\n\ntouche une rente\nd'accident du travail\n(la victime ou\nsa famille)\n\n(AT/MP)",
         "orange_moyen", "orange"),
        ("ALLOCATAIRE\n\ntouche les allocations\nfamiliales pour\nses enfants\n\n(PF)", "vert_moyen", "vert"),
    ]
    for i, (txt, bg, stroke) in enumerate(receivers):
        x = 40 + i * 300
        r = d.box(x, 1000, 285, 225, txt, 20, bg=C[bg], stroke=C[stroke])
        d.arrow(425 + 400 * (i + 1) / 5, 906, x + 142.5, 994, insured, r)
    d.box(40, 1245, 1185, 48,
          "Attention : pour un orphelin, l'argent est versé à son tuteur, mais c'est l'orphelin "
          "qu'il faut compter.", 18, bg=C["jaune_clair"], stroke=C["jaune_bord"])

    # ------------------------------------------------------------------ 4. vie d'un dossier
    d.section(1300, 690, 1300, 620, "4. La vie d'un dossier")
    steps = [
        "DÉPÔT\nla personne apporte\nsa demande",
        "LIQUIDATION\nla CNSS calcule les\ndroits et notifie\nsa décision",
        "PAIEMENT\nchaque mois, ou en\nune seule fois",
        "CONTRÔLE\na posteriori : on\nrevérifie après coup",
    ]
    prev = None
    for i, txt in enumerate(steps):
        r = d.box(1325 + i * 310, 765, 260, 125, txt, 20, bg=C["bleu_clair"], stroke=C["bleu"])
        if prev:
            d.harrow(prev, r, color=C["bleu"], stroke_width=3)
        prev = r
    d.box(1325, 905, 590, 75,
          "Durée de liquidation = du dépôt à la notification\nNorme CIPRES : moins de 45 jours", 20,
          bg=C["rouge_clair"], stroke=C["rouge"], color=C["rouge"])
    d.box(1945, 905, 570, 75,
          "Au 31 décembre, un dossier est liquidé,\nrejeté ou encore en instance (en attente)", 20,
          bg=C["blanc"], stroke=C["gris_bord"], style="dashed")
    d.text(1325, 1005, "Dans la base de données, la même histoire s'écrit avec 5 tables :", 22)
    tables = [
        ("DOSSIER\nla demande\n\nA = actif\nF = fermé", "solid"),
        ("PRESTATION\nle droit accordé\n(ex. une pension)\n\nA active\nS suspendue\nF radiée", "solid"),
        ("DÉBOURS\nla somme due\npour un mois\n\npayé si statut D\n(par effet) ou P\n(sans effet)",
         "solid"),
        ("EFFET\nl'ordre de\npaiement, qui\nregroupe des\ndébours", "solid"),
        ("AJUSTEMENT\nune correction\naprès coup :\ntrop perçu ou\nmoins perçu", "dashed"),
    ]
    prev = None
    for i, (txt, style) in enumerate(tables):
        r = d.box(1325 + i * 247.5, 1050, 210, 175, txt, 18, bg=C["blanc"], stroke=C["doux"],
                  style=style)
        if prev and i < 4:
            d.harrow(prev, r, color=C["doux"])
        prev = r
    d.box(1325, 1237, 1200, 58,
          "Un vrai paiement = un débours au statut D ou P. Le montant se lit sur le débours, "
          "jamais sur l'effet\n(sinon il est compté plusieurs fois). Règle proposée R3, à confirmer "
          "par la DSI.", 18, bg=C["jaune_clair"], stroke=C["jaune_bord"])

    # ------------------------------------------------------------------ 5. questions CIPRES
    d.section(0, 1360, 2600, 490, "5. Les 4 questions de la CIPRES (tableaux T25 à T57)")
    d.text(25, 1422, "La CIPRES est l'organisme régional qui contrôle les caisses de sécurité "
                     "sociale. Chaque année, la CNSS lui remplit ces tableaux (l'Annuaire statistique).",
           20, C["doux"])
    questions = [
        ("1. Traite-t-on vite ?\nT25, T26, T27",
         "Pour chaque branche, combien de dossiers :\n• déposés\n• liquidés en moins de 15 jours,\n"
         "  de 15 à 45 jours, en plus de 45 jours\n• en instance\n• rejetés",
         "Norme : moins de 45 jours", True),
        ("2. Vérifie-t-on ?\nT28, T29, T30",
         "Après le paiement, la CNSS doit revérifier\nune partie des dossiers payés.\n"
         "C'est le contrôle a posteriori.\n\nBut : repérer les erreurs et les fraudes.",
         "Norme : au moins 5 % des dossiers payés", True),
        ("3. Paie-t-on juste ?\nT31",
         "Chaque mois et pour chaque branche :\n• trop perçus : la CNSS a payé trop,\n"
         "  elle perd de l'argent\n• moins perçus : elle n'a pas payé assez,\n  l'assuré est lésé",
         "Norme : 2 % au plus", True),
        ("4. Qui paie-t-on, et combien ?\nT32 à T57",
         "Par tranche d'âge et par sexe :\n• T32 à T41 : les bénéficiaires au 31/12\n"
         "• T42 à T49 : les nouveaux de l'année\n• T50 à T56 : l'évolution sur 5 ans\n"
         "• T57 : les décès par branche\nOn compte des personnes et des montants.",
         "Chaque tableau a sa grille d'âge (11 en tout)", False),
    ]
    for i, (head, body, pill, is_norm) in enumerate(questions):
        x = 25 + i * 640
        g = d.group()
        d.box(x, 1470, 620, 80, head, 24, bg=C["jaune"], stroke=C["jaune_bord"], stroke_width=3,
              groups=g)
        b = d.box(x, 1550, 620, 220, bg=C["blanc"], stroke=C["jaune_bord"], groups=g)
        d.text(x + 20, 1565, body, 20, groups=g, inside=b)
        d.box(x, 1780, 620, 50, pill, 20,
              bg=C["rouge_moyen"] if is_norm else C["gris"],
              stroke=C["rouge"] if is_norm else C["gris_bord"],
              color=C["rouge"] if is_norm else C["doux"], groups=g)

    # ------------------------------------------------------------------ 6. données 2025
    d.section(0, 1900, 2600, 580,
              "6. Ce que montrent les données de 2025 (chiffres provisoires, à faire valider)")
    facts = [
        ("Durée de liquidation, vieillesse (médiane)", "62 jours", "rouge",
         "AT/MP : 156 j · PF : 13,5 j · maternité : 17 j\nNorme CIPRES : moins de 45 jours"),
        ("Dossiers contrôlés après paiement", "0,07 %", "rouge",
         "115 contrôles pour 155 206 dossiers payés\nNorme : au moins 5 %\n"
         "Plus aucune saisie depuis juillet 2025"),
        ("Pensionnés de vieillesse payés fin 2025", "29 721", "bleu",
         "41,56 milliards de FCFA versés en 2025, soit\nenviron 1,4 million par pensionné et par an\n"
         "(première version du tableau T32)"),
        ("Pensions d'orphelin", "8 447", "orange",
         "pensions actives sans aucun paiement au\n4e trimestre 2025 : faut-il les radier ?\n"
         "Question pour la direction des prestations"),
    ]
    for i, (title, figure, color, detail) in enumerate(facts):
        x = 25 + i * 640
        g = d.group()
        card = d.box(x, 1975, 620, 210, bg=C["blanc"], stroke=C["section_bord"], groups=g)
        d.text(x + 20, 1991, title, 20, C["doux"], groups=g, inside=card)
        d.text(x + 20, 2023, figure, 44, C[color], groups=g, inside=card)
        d.text(x + 20, 2090, detail, 18, groups=g, inside=card)

    g = d.group()
    dtm = d.box(25, 2210, 1260, 245, bg=C["rouge_clair"], stroke=C["rouge"], groups=g)
    d.text(45, 2226, "Ce qui ne va pas dans le DTM actuel", 24, C["rouge"], groups=g, inside=dtm)
    d.text(45, 2270,
           "• les montants versés sont gonflés 3,8 fois\n• seuls les dossiers déjà payés sont gardés\n"
           "• le statut F est lu « liquidé », alors qu'il veut dire « fermé »\n"
           "• la maternité (M) est appelée « Maladie »\n"
           "• pour un orphelin, on compte le tuteur payé, pas l'orphelin\n"
           "• les « contrôles » comptés ne sont qu'une case cochée à la saisie", 20, groups=g,
           inside=dtm)

    d.text(1305, 2214, "Où on en est (29/10/2026)", 24)
    project = [
        ("Lots 1 et 2\nprofilage des\ndonnées\n\nFAIT", "vert_moyen", "vert"),
        ("Règles R1 à R12\nproposées\n(qui compter,\nquand, combien)\n\nFAIT", "vert_moyen", "vert"),
        ("Validation\npar la direction\ndes prestations\net la DSI\n\nÀ FAIRE", "jaune", "jaune_bord"),
        ("Requêtes DTM\ncorrigées\n(DTM_DOSSIER\ndéjà proposée)\n\nEN COURS", "jaune", "jaune_bord"),
        ("Lot 3\ndossiers, indus,\nallocataires\n\nÀ FAIRE", "gris", "gris_bord"),
    ]
    prev = None
    for i, (txt, bg, stroke) in enumerate(project):
        r = d.box(1305 + i * 258, 2260, 228, 170, txt, 20, bg=C[bg], stroke=C[stroke])
        if prev:
            d.harrow(prev, r, color=C["doux"])
        prev = r

    return d


if __name__ == "__main__":
    main(build, OUTPUT)
