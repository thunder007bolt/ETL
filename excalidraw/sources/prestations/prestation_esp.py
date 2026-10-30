"""
Schéma « PRESTATION_ESP, expliquée simplement » (état au 29/10/2026).

Sources : analyses/prestations (P04 types et volumes, P16 ayant droit et personne payée,
P24 statut et dates, P27 et P28 notification, P29 et P30 mois cotisés ; règles R5 à R7).

Usage :
    python3 excalidraw/sources/prestations/prestation_esp.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "prestation_esp.excalidraw"


def build():
    d = Diagram(seed=31)

    def label(a, b, txt):
        """Colonne de liaison écrite au-dessus d'une flèche horizontale entre a et b."""
        d.text((a["x"] + a["width"] + b["x"]) / 2, a["y"] + a["height"] / 2 - 32, txt, 18,
               C["doux"], align="center")

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "PRESTATION_ESP, expliquée simplement", 48)
    d.text(0, 66, "Une ligne = un droit à de l'argent, ouvert dans un dossier, pour une personne · "
                  "947 953 lignes · état au 29/10/2026", 22, C["doux"])

    # ------------------------------------------------------------------ 1. espèces
    d.section(0, 120, 2600, 330, "1. ESP veut dire « en espèces » : de l'argent versé")
    d.box(40, 190, 1240, 220,
          "EN ESPÈCES : de l'argent versé à une personne\n"
          "pension, rente, allocation, indemnité journalière\n\n"
          "C'est ce que contient PRESTATION_ESP :\n947 953 droits, de 28 types (TPE_CODE)", 22,
          bg=C["blanc"], stroke=C["encre"], stroke_width=3)
    d.box(1320, 190, 1240, 220,
          "EN NATURE : des soins payés pour la personne\n"
          "pharmacie, consultation, radio… (accidents du travail)\n\n"
          "Ce ne sont pas des droits en espèces :\nils se lisent sur les débours (code TPN_CODE)", 22,
          bg=C["gris"], stroke=C["gris_bord"])

    # ------------------------------------------------------------------ 2. place
    d.section(0, 500, 2600, 440, "2. Où elle se place : entre le dossier et les paiements")
    dossier = d.box(60, 580, 620, 230,
                    "DOSSIER\nla chemise de l'assuré,\nouverte dans une branche\n\n"
                    "un dossier peut ouvrir\nplusieurs droits", 22, bg=C["gris"], stroke=C["gris_bord"])
    prest = d.box(990, 580, 620, 230,
                  "PRESTATION_ESP\nun droit accordé :\n« telle personne a droit à\n"
                  "telle prestation depuis\ntelle date »", 24, bg=C["jaune"], stroke=C["jaune_bord"],
                  stroke_width=3)
    debours = d.box(1920, 580, 620, 230,
                    "DEBOURS\nchaque somme due au titre\nde ce droit : une par mois\n"
                    "(par trimestre pour les\nallocations familiales)", 22, bg=C["gris"],
                    stroke=C["gris_bord"])
    d.harrow(dossier, prest, stroke_width=3)
    label(dossier, prest, "DOS_CODE")
    d.harrow(prest, debours, stroke_width=3)
    label(prest, debours, "PE_ID")
    d.box(60, 840, 2480, 70,
          "Pour comparer : le dossier est la chemise, la prestation est l'abonnement, "
          "et les débours sont les factures de chaque mois.", 22, bg=C["jaune_clair"],
          stroke=C["jaune_bord"])

    # ------------------------------------------------------------------ 3. exemples
    d.section(0, 990, 2600, 800, "3. Deux exemples (personnes fictives)")

    # un retraité (PVID)
    d.text(50, 1050, "Un retraité : 1 dossier, 1 droit, 1 débours par mois", 24, C["bleu"])
    dos_v = d.box(50, 1110, 330, 150, "DOSSIER V\nvieillesse\nassuré : M. A", 22,
                  bg=C["bleu_clair"], stroke=C["bleu"])
    rpv = d.box(470, 1080, 400, 210,
                "PRESTATION_ESP\nRPV · pension de vieillesse\nayant droit : M. A\n"
                "personne payée : M. A\nactive (A) depuis 01/2023", 20, bg=C["bleu_moyen"],
                stroke=C["bleu"], stroke_width=3)
    d.harrow(dos_v, rpv, color=C["bleu"])
    for i, month in enumerate(["01/2025", "02/2025", "03/2025 …"]):
        y = 1080 + i * 80
        deb = d.box(960, y, 300, 70, f"DEBOURS · {month}", 20, bg=C["blanc"], stroke=C["bleu"])
        d.arrow(876, 1185, 954, y + 35, rpv, deb, color=C["bleu"])
    d.text(50, 1320, "Chaque débours porte le mois payé (DEB_DATE_EFFET)\n"
                     "et le montant (DEB_MONTANT).", 20, C["doux"])

    # une famille (PF)
    d.text(1335, 1050, "Une salariée avec trois enfants : 1 dossier, 3 droits", 24, C["vert"])
    dos_f = d.box(1335, 1150, 330, 150, "DOSSIER F\nprestations familiales\nassurée : Mme B", 22,
                  bg=C["vert_clair"], stroke=C["vert"])
    deb_f = d.box(2265, 1080, 290, 350,
                  "DEBOURS\n\nun débours\npar trimestre\npour chaque\nenfant", 20, bg=C["blanc"],
                  stroke=C["vert"])
    for i in range(3):
        y = 1080 + i * 120
        faf = d.box(1755, y, 420, 110,
                    f"FAF · enfant {i + 1}\nayant droit : enfant {i + 1}\npersonne payée : Mme B", 20,
                    bg=C["vert_moyen"], stroke=C["vert"], stroke_width=3)
        d.arrow(1671, 1225, 1749, y + 55, dos_f, faf, color=C["vert"])
        d.arrow(2181, y + 55, 2259, y + 55, faf, deb_f, color=C["vert"])

    d.box(50, 1470, 2500, 120,
          "Attention, orphelin : la pension RPSE est ouverte pour l'orphelin (l'ayant droit, "
          "IND_ID_PREST), mais l'argent est versé à son tuteur (la personne payée, IND_ID_BENEF).\n"
          "Dans les tableaux CIPRES, on compte l'orphelin. Pour une veuve (RPSV), l'ayant droit "
          "et la personne payée sont la même personne.", 20, bg=C["rouge_clair"], stroke=C["rouge"],
          color=C["rouge"])
    d.box(50, 1610, 2500, 140,
          "Vérifié dans la base (P16) : pour les allocations familiales, l'ayant droit est l'enfant "
          "et la personne payée est l'assuré ;\npour les orphelins, la personne payée n'est jamais "
          "l'ayant droit. Les plus nombreuses : FAF 614 811 prestations, RPV 46 836 (P04).", 20,
          bg=C["jaune_clair"], stroke=C["jaune_bord"])

    # ------------------------------------------------------------------ 4. colonnes
    d.section(0, 1840, 2600, 550, "4. Les colonnes à connaître")
    g = d.group()
    frame = d.box(25, 1910, 2550, 210, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    d.text(45, 1924, "Qui et quoi", 22, groups=g, inside=frame)
    d.text(45, 1962,
           "PE_ID · la clé de la prestation (reprise par les débours et les étapes)\n"
           "DOS_CODE · le dossier\n"
           "TPE_CODE · le type de droit : RPV, RPSV, RPSE, ARI, FAF, MIJ… (28 codes)\n"
           "IND_ID_PREST · l'ayant droit : la personne pour qui le droit est ouvert\n"
           "IND_ID_BENEF · la personne qui reçoit l'argent", 20, groups=g, inside=frame)
    d.text(1330, 1962,
           "PE_FREQ_PAIEMENT · mensuel, ou trimestriel (FAF, FAP)\n"
           "PE_MOIS_COTISATION · les mois cotisés de l'assuré\n"
           "PE_SAL_MOYEN · le salaire moyen retenu pour le calcul\n"
           "PE_TAUX_INCAPACITE · pour les rentes d'accident du travail\n"
           "PE_STATUT · A active, S suspendue, F fermée", 20, groups=g, inside=frame)

    d.text(25, 2160, "La vie d'un droit : le statut et ces dates sont fiables (P24)", 22)
    steps = [
        "PE_DATE_EFFET\nle droit commence\nstatut A (active)",
        "PE_DATE_NOTIFICATION\nla décision est envoyée\n(souvent après l'effet)",
        "PE_DATE_SUSPENSION\nle droit est arrêté\nstatut S (suspendue)",
        "PE_DATE_REACTIVATION\nle droit reprend\nstatut A",
        "PE_DATE_RADIATION\nle droit prend fin\nstatut F (fermée)",
    ]
    prev = None
    for i, txt in enumerate(steps):
        box = d.box(60 + i * 500, 2210, 440, 130, txt, 20, bg=C["blanc"], stroke=C["encre"])
        if prev:
            d.harrow(prev, box)
        prev = box

    # ------------------------------------------------------------------ 5. à retenir
    d.section(0, 2440, 2600, 360, "5. Ce qu'il faut en retenir pour les tableaux CIPRES")
    cards = [
        ("Qui compter ?\n\nl'ayant droit (IND_ID_PREST),\nsauf allocations familiales :\n"
         "l'allocataire payé\n(règle R5 proposée)", "jaune_clair", "jaune_bord", "encre"),
        ("Pensionné au 31/12 ?\n\nprestation active à cette date\net payée au 4e trimestre\n\n"
         "(règle R6 proposée)", "jaune_clair", "jaune_bord", "encre"),
        ("Nouveau pensionné ?\n\nprestation notifiée dans l'année\n(PE_DATE_NOTIFICATION)\n\n"
         "(règle R7 proposée)", "jaune_clair", "jaune_bord", "encre"),
        ("Pièges\n\nune personne peut avoir plusieurs\nprestations (révisions) : la compter\n"
         "une fois ; les mois cotisés sont\nrecopiés sur chaque prestation", "rouge_clair", "rouge",
         "rouge"),
    ]
    for i, (txt, bg, stroke, color) in enumerate(cards):
        d.box(25 + i * 640, 2510, 620, 250, txt, 20, bg=C[bg], stroke=C[stroke], color=C[color])

    return d


if __name__ == "__main__":
    main(build, OUTPUT)
