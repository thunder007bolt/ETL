"""
Schéma « T60 : le mouvement du personnel, cas par cas » (état au 08/11/2026).

Ce que fait la requête proposée de T60 (analyses/grh/t60_requete_proposee.sql, règles
dans analyses/grh/regles_t60.md) : 1. l'idée des deux photos ; 2. la maquette CIPRES et
les codes de mouvement de la CNSS (le détail des « autres » demandé par le recueil des
besoins v7) ; 3. les entrées ; 4. les sorties ; 5. les cas sans mouvement ; 6. 2024 face à
l'annuaire CNSS ; puis T60 dans OBIEE, les réglages, les questions et les sources.

Chiffres repris des lots 17, 18, 20 et 25 (2024, permanents) et du tableau 121 de
l'annuaire CNSS 2024.

Usage :
    python3 excalidraw/sources/grh/cas_mouvements_t60.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "grh" / "T60 mouvements" / "cas_mouvements_t60.excalidraw"

WIDTH = 2600
GAP = 40
CARD_H = 250

# genre de carte : (fond de la carte, fond du verdict, bord, épaisseur, style)
GENRES = {
    "entree": (C["blanc"], C["vert_moyen"], C["vert"], 2, "solid"),
    "sortie": (C["blanc"], C["orange_moyen"], C["orange"], 2, "solid"),
    "rien": (C["blanc"], C["gris"], C["gris_bord"], 2, "solid"),
    "reglage": (C["blanc"], C["jaune"], C["jaune_bord"], 2, "solid"),
    "attention": (C["rouge_clair"], C["rouge_moyen"], C["rouge"], 3, "solid"),
    "absent": (C["blanc"], C["blanc"], C["gris_bord"], 2, "dashed"),
}

ENTREES = [
    ("Embauché dans l'année", "permanent : date d'embauche\n(sinon de prise de service) dans "
     "l'année\n2024 : 104, comme l'annuaire CNSS", "RECRU · RECRUTEMENT", "entree"),
    ("Nouveau contrat de temporaire", "engagement 50, alors qu'il n'avait\naucun contrat au "
     "31/12 d'avant\n(tableau des temporaires)", "RECRU · RECRUTEMENT", "entree"),
    ("Temporaire devenu permanent", "temporaire au 31/12 d'avant,\npermanent au 31/12 :\n"
     "il entre chez les permanents", "AUTRE_ENT · ÉTAIT TEMPORAIRE", "entree"),
    ("Revenu de disponibilité", "remis en activité (18) ; il était\nen disponibilité (07) au "
     "31/12 d'avant\nseulement si la dispo sort de l'effectif", "REM · SELON LE RÉGLAGE",
     "reglage"),
    ("Détaché à la CNSS", "décision 37 (classement d'un agent\ndétaché) dans l'année\n"
     "aucune chez les permanents aujourd'hui", "DETAC · DÉTACHÉ", "absent"),
    ("Embauché fin décembre,\nsans situation au 31/12", "nature vide au 31/12 : il n'est\n"
     "dans aucun tableau cette année-là ;\nl'année suivante, il entre ainsi",
     "AUTRE_ENT · ÉTAIT SANS SITUATION", "attention"),
]

SORTIES = [
    ("Parti à la retraite", "décision 02, à sa date d'effet\n2024 : 25 (23 publiés)",
     "RET · RETRAITE", "sortie"),
    ("Décédé", "date de décès ; sinon liquidation des\ndroits (34), environ 8 mois après :\n"
     "aucune date de décès depuis 2010", "DEC · DÉCÈS", "attention"),
    ("Démission", "décision 36\n2024 : 2, comme l'annuaire CNSS", "DEM · AUTRES SORTIES",
     "sortie"),
    ("Licenciement", "décision 54\n2024 : 1, comme l'annuaire CNSS", "LIC · AUTRES SORTIES",
     "sortie"),
    ("Fin de contrat", "décision 35, ou rupture du contrat 52 :\nle temporaire n'a plus de "
     "contrat\nau 31/12", "FIN_CTRAT · AUTRES SORTIES", "sortie"),
    ("Mis en disponibilité", "décision 07, pas encore de retour (18)\nau 31/12 ; seulement si "
     "la dispo\nsort de l'effectif (2024 : 5 décisions)", "DISP · SELON LE RÉGLAGE", "reglage"),
    ("Titularisé, vu des temporaires", "temporaire au 31/12 d'avant,\npermanent au 31/12 :\n"
     "il quitte les temporaires", "AUTRE_SORT · DEVIENT PERMANENT", "sortie"),
    ("Retraite, puis décès la même année", "seul le premier départ compte,\ncomme pour "
     "l'effectif (T58)", "RET · RETRAITE (PAS DÉCÈS)", "sortie"),
]

SANS_MOUVEMENT = [
    ("Changé d'unité, de qualification\nou de catégorie", "présent aux deux 31/12, dans la "
     "même\nnature : pas un mouvement de T60", "AUCUN MOUVEMENT", "rien"),
    ("Contrat renouvelé", "nouvel engagement 50, sans départ\nentre les deux : le premier "
     "contrat\nfinit la veille du suivant", "AUCUN MOUVEMENT", "rien"),
    ("Parti puis revenu dans l'année", "temporaire réengagé avant le 31/12 :\nprésent aux deux "
     "31/12 (lot 25)", "AUCUN MOUVEMENT", "rien"),
    ("Départ annulé", "la décision de départ est annulée\npar une décision 40 :\non l'ignore",
     "AUCUN MOUVEMENT", "rien"),
    ("En disponibilité (réglage actuel)", "PRESENT : l'agent reste dans\nl'effectif ; ni sortie "
     "ni retour\n(question QG6)", "AUCUN MOUVEMENT (RÉGLAGE)", "reglage"),
    ("Entré et sorti la même année", "absent des deux 31/12 ;\nréglage passages_annee : NON\n"
     "(OUI = une entrée et une sortie)", "PAS COMPTÉ (RÉGLAGE)", "reglage"),
    ("Fiche INACTIF, sans date de départ", "84 engagés de 2019 : jamais présents,\nni entrée "
     "ni sortie ; l'annuaire\nles sort tous en 2024 (G11)", "JAMAIS DANS T60", "attention"),
    ("Mis à disposition, détaché\nhors de la CNSS", "décision 14, ou rien de daté :\nl'agent "
     "reste dans l'effectif\n(questions 5 et 6)", "AUCUN MOUVEMENT", "rien"),
]

# La maquette de l'Annuaire CIPRES (p. 51) et les codes de la CNSS sous chaque colonne
MAQUETTE = [
    ("EFFECTIF\nDÉBUT", "DEBUT", C["bleu_clair"], C["bleu"]),
    ("RECRUTEMENT", "RECRU", C["vert_clair"], C["vert"]),
    ("AUTRES\nENTRÉES", "REM\nDETAC\nAUTRE_ENT", C["vert_clair"], C["vert"]),
    ("RETRAITE", "RET", C["orange_clair"], C["orange"]),
    ("DÉCÈS", "DEC", C["orange_clair"], C["orange"]),
    ("AUTRES\nSORTIES", "LIC · DEM\nDISP · MISE_DIS\nFIN_DETACH\nFIN_CTRAT\nAUTRE_SORT",
     C["orange_clair"], C["orange"]),
    ("EFFECTIF\nFIN", "début\n+ entrées\n− sorties", C["bleu_clair"], C["bleu"]),
]

# 2024, permanents : (colonne, base, annuaire CNSS 2024, pourquoi l'écart)
CHIFFRES_2024 = [
    ("Effectif début", "677", "767", "les 84 engagés de 2019, jamais comptés par la base (G11)"),
    ("Recrutement", "104", "104", "retrouvé"),
    ("Autres entrées", "0", "7", "6 remises en activité et 1 détachement : réglage QG6, "
                                 "détachement introuvable"),
    ("Retraite", "25", "23", "à quelques unités près, chaque année"),
    ("Décès", "5", "3", "liquidation des droits, 8 mois après le décès"),
    ("Autres sorties", "4", "94", "les 84 de 2019 sortis par l'annuaire ; 7 disponibilités "
                                  "(réglage)"),
    ("Effectif fin", "747", "758", "= T58 au 31/12/2024 (lot 25)"),
]


def build():
    d = Diagram(seed=60)

    def carte(x, y, w, h, titre, detail, verdict, genre):
        """Une carte : le cas, ce qu'il veut dire, et le code du mouvement en bas."""
        fond, fond_verdict, bord, sw, style = GENRES[genre]
        g = d.group()
        cadre = d.box(x, y, w, h, bg=fond, stroke=bord, stroke_width=sw, style=style, groups=g)
        d.text(x + 22, y + 16, titre, 24, groups=g, inside=cadre)
        h_titre = text_size(titre, 24)[1]
        d.text(x + 22, y + 16 + h_titre + 8, detail, 20, C["doux"], groups=g, inside=cadre)
        d.box(x + 22, y + h - 16 - 48, w - 44, 48, verdict, 20, bg=fond_verdict, stroke=bord,
              style=style, groups=g)

    def grille(cartes, top, colonnes, h=CARD_H):
        """Range les cartes en lignes ; renvoie le bas de la dernière ligne."""
        w = (WIDTH - 80 - (colonnes - 1) * GAP) / colonnes
        for i, c in enumerate(cartes):
            ligne, col = divmod(i, colonnes)
            carte(40 + col * (w + GAP), top + ligne * (h + 30), w, h, *c)
        lignes = (len(cartes) + colonnes - 1) // colonnes
        return top + lignes * h + (lignes - 1) * 30

    def bloc(x, y, w, h, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2):
        g = d.group()
        b = d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, groups=g)
        d.text(x + 22, y + 16, txt, size, groups=g, inside=b)
        return b

    # ------------------------------------------------------------------ titre et légende
    d.text(0, 0, "T60 : le mouvement du personnel, cas par cas", 48)
    d.text(0, 66, "Ce que la requête proposée de T60 fait de chaque agent, d'un 31/12 à "
                  "l'autre, et le code de mouvement de la CNSS qu'elle lui donne\n"
                  "t60_requete_proposee.sql et regles_t60.md · règles proposées, en attente "
                  "du visa de la DRH · état au 08/11/2026", 22, C["doux"])
    legende = [("entrée", "entree"), ("sortie", "sortie"), ("aucun mouvement", "rien"),
               ("selon un réglage", "reglage"), ("à surveiller", "attention")]
    for i, (txt, genre) in enumerate(legende):
        _, fond_verdict, bord, sw, style = GENRES[genre]
        d.box(1240 + i * 270, 8, 250, 44, txt, 18, bg=fond_verdict, stroke=bord, stroke_width=sw)

    # ------------------------------------------------------------------ 1. l'idée
    s1 = 170
    d.section(0, s1, WIDTH, 500, "1. L'idée : deux photos de l'effectif, d'un 31/12 à l'autre")
    debut = d.box(40, s1 + 90, 620, 250,
                  "EFFECTIF AU DÉBUT DE N\nles agents présents\nau 31/12 de N − 1\n"
                  "(mêmes règles que T58)", 22, bg=C["bleu_clair"], stroke=C["bleu"],
                  stroke_width=3)
    milieu = d.box(790, s1 + 90, 1020, 250,
                   "PENDANT L'ANNÉE N\nENTRÉE : présent à la fin, absent au début\n"
                   "SORTIE : présent au début, absent à la fin\nle code vient de la fiche "
                   "ou des décisions\n(parties 3 et 4)", 22, bg=C["blanc"], stroke=C["encre"])
    fin = d.box(1940, s1 + 90, 620, 250,
                "EFFECTIF À LA FIN DE N\nles agents présents\nau 31/12 de N\n"
                "= l'effectif de T58", 22, bg=C["bleu_clair"], stroke=C["bleu"], stroke_width=3)
    d.harrow(debut, milieu, stroke_width=3)
    d.harrow(milieu, fin, stroke_width=3)
    bloc(40, s1 + 370, 2520, 90,
         "Toujours : DÉBUT + ENTRÉES − SORTIES = FIN. Chaque agent est rangé, à chaque 31/12, "
         "dans la population de sa nature (permanents, temporaires) :\nle filtre de nature "
         "d'OBIEE donne le tableau des permanents et celui des temporaires, chacun juste.", 20,
         couleurs=(C["vert_clair"], C["vert"]))

    # ------------------------------------------------------------------ 2. maquette et codes
    s2 = s1 + 500 + 50
    sec2 = d.section(0, s2, WIDTH, 700, "2. La maquette de l'Annuaire CIPRES, et les codes de "
                                        "la CNSS sous chaque colonne")
    n = len(MAQUETTE)
    wc = (2520 - (n - 1) * 20) / n
    for i, (titre, codes, bg, bord) in enumerate(MAQUETTE):
        x = 40 + i * (wc + 20)
        d.box(x, s2 + 90, wc, 110, titre, 22, bg=bg, stroke=bord, stroke_width=3)
        d.box(x, s2 + 215, wc, 210, codes, 24, bg=C["blanc"], stroke=bord)
    bas_codes = s2 + 425
    bloc(40, bas_codes + 30, 1240, 180,
         "LE DÉTAIL DES « AUTRES » DEMANDÉ PAR LE RECUEIL\n"
         "Recueil des besoins v7, RH ligne 3 : « Autre : il faut le\n"
         "détailler (lister pour la CNSS) ». Déclinaison : direction,\n"
         "agence, bureau. Périodicité : annuelle.", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))
    bloc(1320, bas_codes + 30, 1240, 180,
         "CES CODES EXISTENT DÉJÀ À LA CNSS\n"
         "Table des types de mouvement de l'application GRH\n"
         "(GRH_TYPE_MOUVEMENT). Le tableau 121 de l'annuaire CNSS 2024\n"
         "détaille ses « autres » de la même façon.", 20)
    sec2["height"] = bas_codes + 30 + 180 + 40 - s2

    # ------------------------------------------------------------------ 3. entrées
    s3 = sec2["y"] + sec2["height"] + 50
    sec3 = d.section(0, s3, WIDTH, 700, "3. Les entrées (règle G8)")
    bloc(40, s3 + 80, 2520, 80,
         "Un agent ENTRE quand il est présent au 31/12 de N dans une nature (permanent, "
         "temporaire), et absent de cette nature au 31/12 de N − 1.\nLe code se lit dans "
         "l'ordre des cartes ; s'il n'y a aucune explication : AUTRE_ENT, motif SANS_MOTIF.", 20,
         couleurs=(C["vert_clair"], C["vert"]))
    bas3 = grille(ENTREES, s3 + 190, 3)
    sec3["height"] = bas3 + 40 - s3

    # ------------------------------------------------------------------ 4. sorties
    s4 = bas3 + 40 + 50
    sec4 = d.section(0, s4, WIDTH, 900, "4. Les sorties (règle G9)")
    bloc(40, s4 + 80, 2520, 80,
         "Un agent SORT quand il est présent au 31/12 de N − 1 dans une nature, et absent de "
         "cette nature au 31/12 de N. Le code vient du départ qui l'a fait sortir :\nle premier "
         "départ d'un permanent (comme T58), le dernier départ de l'année d'un temporaire ; "
         "sans départ daté : AUTRE_SORT, motif SANS_MOTIF.", 20,
         couleurs=(C["orange_clair"], C["orange"]))
    bas4 = grille(SORTIES, s4 + 190, 4)
    sec4["height"] = bas4 + 40 - s4

    # ------------------------------------------------------------------ 5. sans mouvement
    s5 = bas4 + 40 + 50
    sec5 = d.section(0, s5, WIDTH, 900, "5. Les cas sans mouvement dans T60")
    bas5 = grille(SANS_MOUVEMENT, s5 + 80, 4)
    sec5["height"] = bas5 + 40 - s5

    # ------------------------------------------------------------------ 6. 2024
    s6 = bas5 + 40 + 50
    sec6 = d.section(0, s6, WIDTH, 700, "6. 2024 : ce que la base devrait donner, face à "
                                        "l'annuaire CNSS 2024 (permanents)")
    cols = [("Colonne de T60", 380), ("Base", 200), ("Annuaire CNSS", 240),
            ("Pourquoi l'écart", 1640)]
    xs, x = [], 40
    for _, w in cols:
        xs.append(x)
        x += w + 20
    for (titre, w), xc in zip(cols, xs):
        d.text(xc + 4, s6 + 80, titre, 21, C["doux"])
    y = s6 + 124
    for i, ligne in enumerate(CHIFFRES_2024):
        g = d.group()
        cadre = d.box(40, y, 2520, 56, bg=C["blanc"] if i % 2 == 0 else C["section"],
                      stroke=C["gris_bord"], stroke_width=1, groups=g)
        for val, xc in zip(ligne, xs):
            d.text(xc + 16, y + 14, val, 20, groups=g, inside=cadre)
        y += 64
    sec6["height"] = y - 8 + 40 - s6

    # ------------------------------------------------------------------ OBIEE, réglages, sources
    s7 = sec6["y"] + sec6["height"] + 50
    hb = 230
    bloc(40, s7, 820, hb,
         "T60 DANS OBIEE\n"
         "· filtres : agents A, nature (global), 5 ans\n"
         "· colonnes : DEBUT, RECRU, ENTREES sauf RECRU,\n"
         "  RET, DEC, SORTIES sauf RET et DEC\n"
         "· fin = début + entrées − sorties\n"
         "· par direction ou agence : la fin vient de T58,\n"
         "  l'écart = les mutations nettes", 20)
    bloc(890, s7, 820, hb,
         "CE QUI PEUT CHANGER (CTE parametres)\n"
         "· disponibilité : reste dans l'effectif, ou sortie\n"
         "  DISP et retour REM (QG6, T58 et T60 ensemble)\n"
         "· entré et sorti la même année : non, ou oui\n"
         "· décision → type de sortie (52 = fin de contrat)\n"
         "· mouvements internes : comme avant, ou retirés", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))
    bloc(1740, s7, 820, hb,
         "D'OÙ VIENNENT CES RÈGLES\n"
         "· recueil des besoins v7, RH lignes 1 et 3\n"
         "· atelier de Kaya 2017 (prise de service,\n"
         "  communiqué nécrologique)\n"
         "· Annuaire CIPRES p. 51 ; réunion du 03/11/2026\n"
         "  (M06 à M12) ; lots 1, 17, 18, 20, 24, 25", 20)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
