"""
Schéma « La requête de l'effectif, pas à pas » (état au 07/11/2026).

Explique comment analyses/grh/t58_t65_effectif_requete_proposee.sql retrouve
l'effectif au 31/12 des années passées avec le seul cliché d'aujourd'hui :
1. l'idée ; 2. la requête partie par partie (numéros du fichier) ;
3. trois agents fictifs, au 31/12 de chaque année ; 4. ce qui en sort, et ce
que la base ne permet pas de rejouer.

Les âges et anciennetés de l'agent A sont calculés ici avec les formules de
la requête (années pleines ; disponibilité retirée de l'ancienneté).

Usage :
    python3 excalidraw/sources/grh/requete_effectif_pas_a_pas.py [--force]
"""

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main  # noqa: E402

OUTPUT = ROOT / "schemas" / "grh" / "T58 effectif" / "requete_effectif_pas_a_pas.excalidraw"

WIDTH = 2600
ANNEES = list(range(2018, 2026))
DEBUT_AXE = date(2018, 1, 1)
X0, PX = 560, 235            # le 01/01/2018 sur l'axe, et la largeur d'une année
CELL_W, CELL_H = 220, 122
ROW_H = 255

COMPTE = (C["vert_clair"], C["vert"])
INACTIF = (C["jaune_clair"], C["jaune_bord"])
NON = (C["gris"], C["gris_bord"])


def x_de(jour):
    return X0 + (jour - DEBUT_AXE).days / 365.25 * PX


def fin_annee(annee):
    return date(annee, 12, 31)


# ---------------------------------------------------------------- agent A (permanent)
A_NAISSANCE = date(1965, 4, 10)
A_ENTREE = date(2019, 9, 1)
A_SITUATIONS = [(date(2019, 9, 1), "03", "maîtrise"), (date(2021, 7, 1), "04", "cadre moyen")]
A_DISPO = (date(2022, 6, 1), date(2023, 2, 1))     # décision 07, puis 18
A_DEPART = date(2025, 6, 30)                         # décision 02


def age(naissance, jour):
    """G4 : années pleines (MONTHS_BETWEEN / 12, arrondi en dessous)."""
    return jour.year - naissance.year - ((jour.month, jour.day) < (naissance.month, naissance.day))


def anciennete(jour, retirer=True):
    """G5 : années pleines depuis l'entrée, moins la part de la disponibilité avant le 31/12."""
    jours = (jour - A_ENTREE).days
    debut, fin = A_DISPO
    if retirer and debut < jour:
        jours -= max(0, (min(fin, jour) - max(debut, A_ENTREE)).days)
    return max(int(jours / 365.25), 0)


def ans(n):
    return f"{n} an" if n < 2 else f"{n} ans"


def cellule_a(annee):
    jour = fin_annee(annee)
    if jour < A_ENTREE:
        return "pas compté\npas encore entré", NON
    if A_DEPART <= jour:
        return "pas compté\nparti le 30/06/2025\n(retraite, 02)", NON
    _, code, groupe = [s for s in A_SITUATIONS if s[0] <= jour][-1]
    lignes = ["compté"]
    couleur = COMPTE
    if A_DISPO[0] <= jour < A_DISPO[1]:
        lignes = ["compté, mais inactif", "(en disponibilité)"]
        couleur = INACTIF
    lignes += [f"{groupe} ({code})", f"{age(A_NAISSANCE, jour)} ans · anc. {ans(anciennete(jour))}"]
    if annee == 2023:
        lignes.append("(dispo retirée)")
    if annee == 2024:
        lignes.append("départ daté 2025")
    return "\n".join(lignes), couleur


# ---------------------------------------------------------------- agent B (temporaire)
B_CONTRATS = [(date(2021, 3, 1), date(2022, 3, 1), "contrat 1"),
              (date(2022, 3, 1), date(2023, 2, 28), "contrat 2"),
              (date(2024, 10, 1), None, "contrat 3")]


def cellule_b(annee):
    jour = fin_annee(annee)
    if jour < B_CONTRATS[0][0]:
        return "pas compté\npas encore engagé", NON
    for debut, fin, nom in B_CONTRATS:
        if debut <= jour and (fin is None or jour < fin):
            if fin is None:
                return f"compté\ntemporaire, {nom}\nsans fin connue :\ncompté (réglage)", COMPTE
            return f"compté\ntemporaire\n{nom}", COMPTE
    return "pas compté\ncontrat 2 fini le\n28/02/2023 (35)", NON


def build():
    d = Diagram(seed=58)

    def bloc(x, y, w, h, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2, style="solid"):
        """Cadre avec un texte aligné à gauche (une liste se lit mieux ainsi)."""
        g = d.group()
        b = d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, style=style,
                  groups=g)
        d.text(x + 22, y + 16, txt, size, groups=g, inside=b)
        return b

    def trait(x1, y1, x2, y2, couleur=C["encre"], sw=2, style="solid"):
        return d.arrow(x1, y1, x2, y2, color=couleur, stroke_width=sw, style=style, head=None)

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "La requête de l'effectif, pas à pas", 48)
    d.text(0, 66, "Comment t58_t65_effectif_requete_proposee.sql retrouve l'effectif au 31/12 "
                  "des années passées, avec la seule photo d'aujourd'hui\n"
                  "Faits du DWH, cliché 102026 · règles proposées, en attente du visa de la DRH · "
                  "agents fictifs · état au 07/11/2026", 22, C["doux"])

    # ------------------------------------------------------------------ 1. l'idée
    s1 = 160
    d.section(0, s1, WIDTH, 330, "1. L'idée : une seule photo, mais des dates")
    photo = d.box(40, s1 + 75, 780, 220,
                  "UNE SEULE PHOTO : AUJOURD'HUI\nle DWH ne garde que le dernier\n"
                  "cliché (102026)\n\nil n'y a pas de photo\nde 2020, 2021, 2022…", 22,
                  bg=C["gris"], stroke=C["gris_bord"])
    dates = d.box(910, s1 + 75, 780, 220,
                  "MAIS PRESQUE TOUT Y EST DATÉ\nla fiche : embauche, naissance, décès\n"
                  "les situations : date de début\nles décisions : date de vigueur\n"
                  "(départ, engagement, disponibilité)", 22,
                  bg=C["jaune"], stroke=C["jaune_bord"], stroke_width=3)
    rejoue = d.box(1780, s1 + 75, 780, 220,
                   "ALORS LA REQUÊTE REJOUE LE PASSÉ\npour chaque 31/12 et chaque agent :\n"
                   "était-il là ce jour-là ?\ndans quelle situation ?\npuis elle compte", 22,
                   bg=C["vert_clair"], stroke=C["vert"], stroke_width=3)
    d.harrow(photo, dates, stroke_width=3)
    d.harrow(dates, rejoue, stroke_width=3)

    # ------------------------------------------------------------------ 2. les parties
    s2 = s1 + 380
    d.section(0, s2, WIDTH, 1120, "2. La requête, partie par partie (numéros du fichier)")
    etapes = [
        "1. LES DATES D'ARRÊTÉ\nle 31/12 de chaque année close :\n…, 2023, 2024, 2025\n"
        "une année compte dès qu'une\nsituation y commence\n\nlit : FAIT_GRH_SITUATION",
        "2. LES AGENTS\nune ligne par fiche :\nsexe, naissance, qualité\n"
        "entrée = date d'embauche,\nsinon de prise de service\n\nlit : FAIT_GRH_PERSONNE",
        "3. LES DÉPARTS\ndécisions 02, 34, 35, 36, 52, 54,\nou date de décès ;\n"
        "une décision annulée par\nune 40 ne compte pas ;\non garde le premier départ\n"
        "lit : FAIT_GRH_DECISION",
        "4. LES CONTRATS\nchaque engagement 50 ouvre\nun contrat, fini au premier\n"
        "départ qui suit, sinon la veille\ndu contrat suivant, sinon\nfin inconnue\n"
        "lit : FAIT_GRH_DECISION",
        "5. LES DISPONIBILITÉS\nau 31/12 : dernière mise en\ndispo (07), dernier retour (18) ;\n"
        "07 plus récente : l'agent est\nen disponibilité. Ces périodes\nsont retirées de l'ancienneté\n"
        "lit : FAIT_GRH_DECISION",
        "6. LA SITUATION DU 31/12\nla dernière situation\ncommencée au plus tard\n"
        "le 31/12 : nature, catégorie,\nunité, fonction, lieu\n\nlit : FAIT_GRH_SITUATION",
    ]
    boites = []
    for i, txt in enumerate(etapes):
        couleurs = (C["jaune_clair"], C["jaune_bord"]) if i in (0, 5) else (C["blanc"], C["encre"])
        boites.append(d.box(40 + i * 425, s2 + 80, 395, 260, txt, 18,
                            bg=couleurs[0], stroke=couleurs[1]))

    # partie 7 : le croisement
    y7 = s2 + 410
    g7 = d.group()
    croisement = d.box(40, y7, 2520, 260, bg=C["vert_clair"], stroke=C["vert"], stroke_width=3,
                       groups=g7)
    d.text(70, y7 + 18, "7. LE CROISEMENT : chaque agent face à chaque 31/12. La situation du "
                        "jour dit s'il est permanent ou temporaire, puis on ne garde que les "
                        "présents", 22, groups=g7, inside=croisement)
    d.box(70, y7 + 70, 1210, 165,
          "PERMANENT, OU AGENT SANS SITUATION (règle G2)\n"
          "compté s'il est entré au plus tard le 31/12\n"
          "et s'il n'est pas encore parti : fiche pas INACTIF,\n"
          "ou premier départ après le 31/12", 20, bg=C["blanc"], stroke=C["vert"], groups=g7)
    d.box(1320, y7 + 70, 1210, 165,
          "TEMPORAIRE (règle G7)\n"
          "compté si le 31/12 tombe dans un de ses contrats\n"
          "(du début inclus à la fin exclue) ;\n"
          "sans fin connue, il est compté (réglage)", 20, bg=C["blanc"], stroke=C["vert"],
          groups=g7)
    for b in boites:
        cx = b["x"] + b["width"] / 2
        d.arrow(cx, b["y"] + b["height"] + 6, cx, y7 - 6, b, croisement, stroke_width=2)

    # parties 8 et OBIEE
    y8 = y7 + 330
    comptage = d.box(40, y8, 1200, 220,
                     "8. LE COMPTAGE\nune ligne par 31/12 et par combinaison d'axes :\n"
                     "catégorie, sexe, tranche d'âge, tranche d'ancienneté,\n"
                     "unité, domaine, nature, qualité, fonction, lieu\n"
                     "mesures : NB_AGENTS, NB_CADRES, actifs, inactifs,\n"
                     "âge moyen, ancienneté moyenne", 20)
    obiee = d.box(1360, y8, 1200, 220,
                  "PUIS OBIEE : LES TABLEAUX\nfiltre : qualité A (agent CNSS) et nature d'emploi\n"
                  "T58 : catégorie × sexe, par tranche d'âge, au 31/12 choisi\n"
                  "T59 : catégorie × sexe, par domaine d'activité\n"
                  "T61 : catégorie × sexe, par tranche d'ancienneté\n"
                  "T65 : NB_CADRES / NB_AGENTS, une colonne par année (5 ans)", 20,
                  bg=C["violet"], stroke=C["encre"])
    d.arrow(640, y7 + 260 + 6, 640, y8 - 6, croisement, comptage, stroke_width=3)
    d.harrow(comptage, obiee, stroke_width=3)

    bloc(40, y8 + 260, 2520, 120,
         "RÉGLAGES (partie 0) : ils changent le résultat sans réécrire la requête ; "
         "valeur actuelle en premier\n"
         "dates d'arrêté : ANNUELLE ou MENSUELLE  ·  disponibilité : PRESENT ou HORS_EFFECTIF  ·  "
         "temporaire sans fin connue : PRESENT ou FICHE\n"
         "inactifs : DECISIONS ou STATUT  ·  début de l'ancienneté : PRISE_SERVICE ou EMBAUCHE  ·  "
         "retirer les interruptions : OUI ou NON", 19, couleurs=(C["jaune_clair"], C["jaune_bord"]))

    # ------------------------------------------------------------------ 3. trois agents
    s3 = s2 + 1170
    sec3 = d.section(0, s3, WIDTH, 1080, "3. Concrètement : trois agents fictifs, au 31/12 "
                                        "de chaque année")
    for i, (txt, (bg, bord)) in enumerate([("compté", COMPTE), ("compté, mais inactif", INACTIF),
                                            ("pas compté", NON)]):
        d.box(1640 + i * 310, s3 + 22, 280, 40, txt, 17, bg=bg, stroke=bord)

    rangs = [s3 + 125, s3 + 125 + ROW_H + 25, s3 + 125 + 2 * (ROW_H + 25)]
    bas = rangs[2] + ROW_H
    for annee in ANNEES:
        x = x_de(fin_annee(annee))
        d.text(x, s3 + 82, f"31/12/{annee}", 18, align="center")
        trait(x, s3 + 112, x, bas, C["gris_bord"], 1, "dashed")

    def rang(top, recit, couleurs_recit, barres, evenements, cellules):
        bloc(40, top, 480, ROW_H, recit, 17, couleurs=couleurs_recit)
        axe = top + 100
        d.arrow(X0, axe, x_de(date(2026, 1, 1)) + 40, axe, color=C["doux"], stroke_width=1)
        for debut, fin, (bg, bord), style in barres:
            d.box(x_de(debut), axe - 8, x_de(fin) - x_de(debut), 16, bg=bg, stroke=bord,
                  stroke_width=1, style=style)
        for annee in ANNEES:
            x = x_de(fin_annee(annee))
            trait(x, axe - 12, x, axe + 12)
        for jour, txt, niveau, couleur in evenements:
            x = x_de(jour)
            y = top + 8 + niveau * 42
            d.text(x, y, txt, 15, align="center")
            if couleur:
                trait(x, y + 40, x, axe - 10, couleur, 1, "dotted")
                d.ellipse(x - 8, axe - 8, 16, 16, bg=couleur, stroke=couleur)
        for annee, (txt, (bg, bord)) in cellules:
            x = x_de(fin_annee(annee))
            d.box(x - CELL_W / 2, top + 128, CELL_W, CELL_H, txt, 15, bg=bg, stroke=bord)

    # agent A
    rang(rangs[0],
         "AGENT A : PERMANENT\nné le 10/04/1965\nentré le 01/09/2019, catégorie 03\n"
         "promu en 04 le 01/07/2021\nmis en disponibilité (07) le 01/06/2022\n"
         "de retour (18) le 01/02/2023\nretraité (02) le 30/06/2025\nfiche aujourd'hui : INACTIF",
         (C["blanc"], C["encre"]),
         [(A_ENTREE, A_DEPART, (C["vert_moyen"], C["vert"]), "solid"),
          (A_DISPO[0], A_DISPO[1], (C["jaune"], C["jaune_bord"]), "solid")],
         [(A_ENTREE, "entrée\n01/09/2019", 0, C["vert"]),
          (A_SITUATIONS[1][0], "promu en 04\n01/07/2021", 0, C["vert"]),
          (A_DISPO[0], "07 : dispo\n01/06/2022", 0, C["jaune_bord"]),
          (A_DISPO[1], "18 : retour\n01/02/2023", 1, C["jaune_bord"]),
          (A_DEPART, "02 : retraite\n30/06/2025", 0, C["rouge"])],
         [(a, cellule_a(a)) for a in ANNEES])

    # agent B
    fin_dessin = date(2026, 1, 1)
    rang(rangs[1],
         "AGENT B : TEMPORAIRE\ncontrat 1 : engagé (50) le 01/03/2021\n"
         "contrat 2 : engagé (50) le 01/03/2022 ;\nle contrat 1 finit la veille\n"
         "fin de contrat (35) le 28/02/2023\ncontrat 3 : engagé (50) le 01/10/2024,\n"
         "aucune fin connue",
         (C["blanc"], C["encre"]),
         [(B_CONTRATS[0][0], B_CONTRATS[0][1], (C["vert_moyen"], C["vert"]), "solid"),
          (B_CONTRATS[1][0], B_CONTRATS[1][1], (C["vert_moyen"], C["vert"]), "solid"),
          (B_CONTRATS[2][0], fin_dessin, (C["vert_moyen"], C["vert"]), "dashed")],
         [(B_CONTRATS[0][0], "50 : contrat 1\n01/03/2021", 0, C["vert"]),
          (B_CONTRATS[1][0], "50 : contrat 2\n01/03/2022", 1, C["vert"]),
          (B_CONTRATS[1][1], "35 : fin\n28/02/2023", 0, C["rouge"]),
          (B_CONTRATS[2][0], "50 : contrat 3\n01/10/2024", 0, C["vert"])],
         [(a, cellule_b(a)) for a in ANNEES])

    # agent C : une seule grande case pour 2019 à 2025
    c_entree = date(2019, 6, 1)
    rang(rangs[2],
         "AGENT C : PARTI SANS DATE\nengagé (51) le 01/06/2019\nfiche aujourd'hui : INACTIF\n"
         "aucune décision de départ,\npas de date de décès",
         (C["rouge_clair"], C["rouge"]),
         [(c_entree, date(2024, 1, 1), (C["gris"], C["gris_bord"]), "dashed")],
         [(c_entree, "51 : engagé\n01/06/2019", 0, C["vert"]),
          (date(2024, 1, 1), "parti quand ?\naucune date", 0, None)],
         [(2018, ("pas compté\npas encore entré", NON))])
    x19 = x_de(fin_annee(2019)) - CELL_W / 2
    x25 = x_de(fin_annee(2025)) + CELL_W / 2
    d.box(x19, rangs[2] + 128, x25 - x19, CELL_H,
          "jamais compté, à aucun 31/12 : sa fiche dit INACTIF, et rien ne dit quand il est parti.\n"
          "C'est le cas des 84 engagés de 2019 (règle G11, question QG2 à la DRH) :\n"
          "de 2020 à 2023, la base compte 92 à 99 permanents de moins que l'annuaire de la CNSS.",
          18, bg=C["rouge_clair"], stroke=C["rouge"])

    bloc(40, bas + 30, 2520, 120,
         "À RETENIR : à chaque 31/12, l'agent est rangé d'après sa situation de ce jour-là "
         "(A : maîtrise jusqu'en 2020, cadre moyen ensuite).\n"
         "Un départ daté le fait sortir au bon moment, même si sa fiche dit INACTIF aujourd'hui "
         "(A compte encore au 31/12/2024). Sa disponibilité\n"
         "le rend inactif au 31/12/2022 et sort de son ancienneté. Sans aucune date, la requête "
         "ne peut rien rejouer (C).", 20, couleurs=(C["jaune_clair"], C["jaune_bord"]))
    sec3["height"] = bas + 30 + 120 + 40 - s3

    # ------------------------------------------------------------------ 4. résultat et limites
    s4 = s3 + sec3["height"] + 50
    d.section(0, s4, WIDTH, 580, "4. Ce qui en sort, et ce que la base ne permet pas de rejouer")
    a21 = fin_annee(2021)
    bloc(40, s4 + 80, 1220, 250,
         f"OÙ TOMBE L'AGENT A AU 31/12/2021\n"
         f"dans la ligne : 20211231 · PERMANENT · qualité A · CADRES_MOYENS · M\n"
         f"· tranche de ses {age(A_NAISSANCE, a21)} ans · tranche de ses "
         f"{ans(anciennete(a21))} d'ancienneté\n"
         f"il y ajoute 1 à NB_AGENTS et 1 à NB_CADRES (04 est une catégorie de cadres)\n\n"
         f"Un agent ne compte qu'une fois par 31/12 : on additionne\n"
         f"sur tous les axes, mais jamais d'une date à l'autre.", 20)
    bloc(40, s4 + 350, 1220, 170,
         "ATTENDU AU CONTRÔLE (lots 17 à 19 ; la requête reste à exécuter)\n"
         "742 permanents au 31/12/2024 (annuaire de la CNSS : 758)\n"
         "taux d'encadrement (T65) : 48,8 % en 2024 (publié : 49,2 %),\n"
         "environ 42 % de 2020 à 2023 (publié : 41 %) : l'écart vient des\n"
         "84 engagés de 2019, comme l'agent C", 20, couleurs=(C["vert_clair"], C["vert"]))
    bloc(1340, s4 + 80, 1220, 440,
         "CE QUE LA BASE D'AUJOURD'HUI NE PERMET PAS DE REJOUER\n\n"
         "· un départ sans date : l'agent sort de toutes les années\n"
         "  (agent C, les 84 engagés de 2019 : question QG2 à la DRH)\n"
         "· le statut de la situation (détaché, en disponibilité…) n'est pas\n"
         "  daté : il est écrasé sur la situation en cours. On lit donc\n"
         "  les décisions 07 et 18 pour les inactifs (règle G13)\n"
         "· ce qui n'est pas daté vaut pour toutes les années tel qu'il est\n"
         "  aujourd'hui : la qualité (A, B, C), le domaine de l'unité\n\n"
         "Tout le reste se rejoue, car chaque fait a sa date : entrées,\n"
         "départs, contrats, disponibilités, situations (catégorie, unité…).", 20,
         couleurs=(C["rouge_clair"], C["rouge"]))
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
