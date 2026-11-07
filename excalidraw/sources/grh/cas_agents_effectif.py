"""
Schéma « Les cas d'agents dans l'effectif » (état au 07/11/2026).

Tous les cas d'agents que traite analyses/grh/t58_t65_effectif_requete_proposee.sql
(T58, T59, T61, T65), et ce que la requête en fait à une date d'arrêté (le 31/12) :
1. le parcours d'un agent ; 2. les permanents (G2) ; 3. les temporaires (G7) ;
4. les cas entre les deux ; 5. comment l'agent compté est rangé ; puis les
réglages et les sources.

Chiffres repris des lots : 67 fiches sans état (lot 2, Q10), 6 permanents en
disponibilité au 31/12/2024 (lot 24), 234 temporaires sans engagement 50
(lot 13), 30 agents réengagés et aucun permanent parti puis revenu (lot 25),
84 engagés de 2019 sans date de départ (G11).

Usage :
    python3 excalidraw/sources/grh/cas_agents_effectif.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "grh" / "cas_agents_effectif.excalidraw"

WIDTH = 2600
GAP = 40
CARD_H = 250

# genre de carte : (fond de la carte, fond du verdict, bord, épaisseur, style)
GENRES = {
    "oui": (C["blanc"], C["vert_moyen"], C["vert"], 2, "solid"),
    "inactif": (C["blanc"], C["jaune"], C["jaune_bord"], 2, "solid"),
    "non": (C["blanc"], C["gris"], C["gris_bord"], 2, "solid"),
    "attention": (C["rouge_clair"], C["rouge_moyen"], C["rouge"], 3, "solid"),
    "absent": (C["blanc"], C["blanc"], C["gris_bord"], 2, "dashed"),
}

PERMANENTS = [
    ("En poste", "entré avant le 31/12,\nfiche active, aucun départ", "COMPTÉ", "oui"),
    ("Tout juste embauché", "fiche pas encore passée à ACTIF (sans état) :\n"
     "elle compte comme encore là\n(67 fiches en janvier 2026)", "COMPTÉ", "oui"),
    ("Embauché après le 31/12", "date d'embauche (sinon de prise de service)\n"
     "après la date d'arrêté", "PAS COMPTÉ", "non"),
    ("Parti avant le 31/12, ou ce jour-là", "retraite 02, décès 34, fin de contrat 35,\n"
     "démission 36, rupture 52, licenciement 54", "PAS COMPTÉ", "non"),
    ("Parti après le 31/12", "exemple : retraite le 15/01/2025 ;\n"
     "il est encore là au 31/12/2024", "COMPTÉ CETTE ANNÉE-LÀ", "oui"),
    ("Décédé, sans décision de décès", "la date de décès de la fiche\n"
     "sert de date de départ", "SORT À SA DATE DE DÉCÈS", "non"),
    ("Départ annulé", "la décision de départ est annulée\n"
     "par une décision 40 : on l'ignore", "COMPTÉ (DÉPART IGNORÉ)", "oui"),
    ("En disponibilité", "mis en disponibilité (07),\npas encore de retour (18)\n"
     "6 permanents au 31/12/2024", "COMPTÉ, MAIS INACTIF", "inactif"),
    ("Revenu de disponibilité", "remis en activité (18) ; la disponibilité\n"
     "est retirée de son ancienneté", "COMPTÉ, ACTIF", "oui"),
    ("Détaché, mis à disposition,\nsuspendu, sous les drapeaux",
     "la base ne date ni le début\nni la fin de ces absences", "COMPTÉ, ACTIF", "oui"),
    ("Fiche INACTIF, sans aucune date de départ", "ni décision de départ, ni date de décès :\n"
     "on ne sait pas quand il est parti\n84 engagés de 2019 (G11, question QG2)",
     "JAMAIS COMPTÉ", "attention"),
    ("Cas absents de la base (lot 25)", "parti puis revenu ;\n"
     "fiche active malgré un départ saisi\n(la fiche l'emporterait : compté)",
     "AUCUN CAS AUJOURD'HUI", "absent"),
]

TEMPORAIRES = [
    ("Sous contrat le 31/12", "engagé (50) avant le 31/12,\ncontrat pas encore fini",
     "COMPTÉ", "oui"),
    ("Contrat fini avant le 31/12, ou ce jour-là", "fin de contrat 35, ou un autre départ\n"
     "(démission, rupture…)", "PAS COMPTÉ", "non"),
    ("Contrat renouvelé", "nouvel engagement 50, sans départ entre\n"
     "les deux : le premier contrat finit\nla veille du suivant", "COMPTÉ, SANS COUPURE", "oui"),
    ("Fin de contrat, puis réengagé", "pas compté entre deux contrats ;\n"
     "ce trou est retiré de l'ancienneté\n(30 agents, lot 25)", "COMPTÉ PENDANT SES CONTRATS",
     "oui"),
    ("Aucune fin de contrat saisie", "ni départ, ni contrat suivant :\n"
     "contrat considéré comme encore ouvert\nréglage FICHE : seulement si fiche active",
     "COMPTÉ (RÉGLAGE)", "oui"),
    ("Aucun engagement 50", "on ne sait pas quand son contrat\n"
     "commence ; 234 agents (lot 13),\npresque tous anciens", "PAS COMPTÉ", "attention"),
    ("Engagement annulé", "l'engagement 50 est annulé\npar une décision 40",
     "CE CONTRAT EST IGNORÉ", "non"),
]

ENTRE_LES_DEUX = [
    ("Temporaire devenu permanent", "temporaire jusqu'à sa situation de permanent,\n"
     "permanent ensuite. Aucun cas « fin de contrat,\npuis titularisation » qui gênerait (lot 25)",
     "SUIT SA SITUATION DU 31/12", "oui"),
    ("Aucune situation au 31/12", "compté avec la règle des permanents, mais\n"
     "sa nature est vide : les filtres Permanent et\nTemporaire d'OBIEE l'écartent. Nombre à compter",
     "COMPTÉ, MAIS DANS AUCUN TABLEAU", "attention"),
    ("Agents B et C", "B : fonction publique ; C : agent affecté.\n"
     "Ils restent dans la table, mais les tableaux\nCIPRES ne lisent que les agents A",
     "HORS DES TABLEAUX CIPRES", "non"),
]

RANGEMENT = [
    "CATÉGORIE\nsituation du 31/12\n01-02 employés · 03 maîtrise\n04-05 cadres moyens\n"
    "06 et HC cadres supérieurs\nsinon : sans catégorie",
    "ÂGE\nannées pleines au 31/12\ndate de naissance absente\nou fausse : compté,\nsans tranche d'âge",
    "SEXE\nlu sur la fiche\nsinon : non renseigné",
    "ANCIENNETÉ (T61)\ndepuis la prise de service\n(sinon l'embauche), moins\nles disponibilités et les\n"
    "trous entre deux contrats",
    "DOMAINE (T59)\nd'après l'unité du 31/12\n« non renseigné » tant que\nla CNSS n'a pas rendu\n"
    "le fichier unité → domaine",
]


def build():
    d = Diagram(seed=59)

    def carte(x, y, w, h, titre, detail, verdict, genre):
        """Une carte : le cas, ce qu'il veut dire, et le verdict en bas."""
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
    d.text(0, 0, "Les cas d'agents dans l'effectif", 48)
    d.text(0, 66, "Ce que la requête de l'effectif (T58, T59, T61, T65) fait de chaque agent, "
                  "à une date d'arrêté (le 31/12 de chaque année)\n"
                  "t58_t65_effectif_requete_proposee.sql · règles proposées, en attente du visa "
                  "de la DRH · état au 07/11/2026", 22, C["doux"])
    legende = [("compté", "oui"), ("compté, mais inactif", "inactif"), ("pas compté", "non"),
               ("à surveiller", "attention")]
    for i, (txt, genre) in enumerate(legende):
        _, fond_verdict, bord, sw, style = GENRES[genre]
        d.box(1460 + i * 280, 8, 260, 44, txt, 18, bg=fond_verdict, stroke=bord, stroke_width=sw)

    # ------------------------------------------------------------------ 1. le parcours
    s1 = 170
    d.section(0, s1, WIDTH, 470, "1. Le parcours d'un agent, à chaque 31/12")
    agent = d.box(40, s1 + 90, 560, 330,
                  "UN AGENT DE LA BASE\nà une date d'arrêté\n(le 31/12 de chaque année)\n\n"
                  "la requête refait ce parcours\npour chaque 31/12", 22,
                  bg=C["bleu_clair"], stroke=C["bleu"])
    situ = d.box(700, s1 + 90, 600, 330,
                 "SA SITUATION CE JOUR-LÀ\nla dernière situation\ncommencée au plus tard\n"
                 "le 31/12 dit sa nature :\npermanent ou temporaire", 22,
                 bg=C["jaune_clair"], stroke=C["jaune_bord"], stroke_width=3)
    d.harrow(agent, situ, stroke_width=3)
    branches = [
        ("PERMANENT, ou toute autre nature\n→ règle des permanents (partie 2)",
         C["vert_clair"], C["vert"]),
        ("TEMPORAIRE\n→ règle des temporaires (partie 3)", C["bleu_clair"], C["bleu"]),
        ("AUCUNE SITUATION CE JOUR-LÀ\n→ règle des permanents, mais nature vide (partie 4)",
         C["rouge_clair"], C["rouge"]),
    ]
    for i, (txt, bg, bord) in enumerate(branches):
        b = d.box(1440, s1 + 80 + i * 125, 1120, 100, txt, 22, bg=bg, stroke=bord)
        d.arrow(situ["x"] + situ["width"] + 6, situ["y"] + situ["height"] / 2,
                b["x"] - 6, b["y"] + b["height"] / 2, situ, b, stroke_width=2)

    # ------------------------------------------------------------------ 2. permanents
    s2 = s1 + 470 + 50
    sec2 = d.section(0, s2, WIDTH, 1000, "2. Les permanents (règle G2)")
    bloc(40, s2 + 80, 2520, 110,
         "RÈGLE : un permanent est compté s'il est ENTRÉ au plus tard le 31/12 (date d'embauche, "
         "sinon de prise de service) ET s'il n'est PAS ENCORE PARTI :\nsa fiche n'est pas INACTIF, "
         "ou son premier départ tombe après le 31/12. Départ = décision 02, 34, 35, 36, 52 ou 54 "
         "non annulée par une 40, sinon la date de décès.", 20,
         couleurs=(C["vert_clair"], C["vert"]))
    bas2 = grille(PERMANENTS, s2 + 220, 4)
    sec2["height"] = bas2 + 40 - s2

    # ------------------------------------------------------------------ 3. temporaires
    s3 = bas2 + 40 + 50
    sec3 = d.section(0, s3, WIDTH, 900, "3. Les temporaires (règle G7)")
    bloc(40, s3 + 80, 1220, 210,
         "RÈGLE : un temporaire est compté si le 31/12 tombe DANS UN DE SES CONTRATS\n"
         "· début : l'engagement temporaire (décision 50), non annulé\n"
         "· fin, dans cet ordre : le premier départ qui suit (35…),\n"
         "  sinon la veille du contrat suivant, sinon fin inconnue\n"
         "· la base ne garde pas la fin prévue d'un contrat", 20,
         couleurs=(C["bleu_clair"], C["bleu"]))
    # petit dessin d'un contrat
    g = d.group()
    d.box(1320, s3 + 80, 1240, 210, bg=C["blanc"], stroke=C["bleu"], groups=g)
    axe = s3 + 185
    x_debut, x_fin = 1440, 2160
    d.arrow(1360, axe, 2520, axe, color=C["doux"], stroke_width=1)
    d.box(x_debut, axe - 9, x_fin - x_debut, 18, bg=C["vert_moyen"], stroke=C["vert"],
          stroke_width=1, groups=g)
    d.text(x_debut, s3 + 100, "début : engagement 50", 18, groups=g)
    d.text(x_fin, s3 + 100, "fin : départ (35…),\nsinon veille du contrat suivant", 18,
           align="center", groups=g)
    for x, txt, couleur in [(1800, "31/12 dans le contrat\n→ compté", C["vert"]),
                            (2400, "31/12 après la fin\n→ pas compté", C["gris_bord"])]:
        d.arrow(x, axe - 30, x, axe + 30, color=couleur, stroke_width=2, style="dashed",
                head=None)
        d.text(x, axe + 36, txt, 17, couleur, align="center", groups=g)
    bas3 = grille(TEMPORAIRES, s3 + 320, 4)
    sec3["height"] = bas3 + 40 - s3

    # ------------------------------------------------------------------ 4. entre les deux
    s4 = bas3 + 40 + 50
    sec4 = d.section(0, s4, WIDTH, 400, "4. Les cas entre les deux")
    bas4 = grille(ENTRE_LES_DEUX, s4 + 80, 3, h=270)
    sec4["height"] = bas4 + 40 - s4

    # ------------------------------------------------------------------ 5. rangement
    s5 = bas4 + 40 + 50
    d.section(0, s5, WIDTH, 360, "5. Puis chaque agent compté est rangé")
    w5 = (WIDTH - 80 - 4 * 30) / 5
    for i, txt in enumerate(RANGEMENT):
        d.box(40 + i * (w5 + 30), s5 + 80, w5, 240, txt, 19, bg=C["blanc"], stroke=C["encre"])

    # ------------------------------------------------------------------ réglages et sources
    s6 = s5 + 360 + 40
    bloc(40, s6, 1240, 250,
         "CE QUI PEUT CHANGER, SANS RÉÉCRIRE LA REQUÊTE (réglages)\n"
         "· disponibilité : compté (PRESENT) ou sorti de l'effectif\n"
         "  (HORS_EFFECTIF) : à trancher par la DRH (QG6)\n"
         "· temporaire sans fin connue : compté (PRESENT),\n"
         "  ou seulement si sa fiche est active (FICHE)\n"
         "· liste des départs : 02, 34, 35, 36, 52, 54, un code en plus ou en moins", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))
    bloc(1320, s6, 1240, 250,
         "D'OÙ VIENNENT CES RÈGLES\n"
         "· recueil des besoins v7, RH ligne 1 : « par type employé :\n"
         "  Permanent, Temporaire (CNSS et CIPRES) »\n"
         "· RH ligne 4 : « prendre en compte les sorties et retours »\n"
         "  pour l'ancienneté\n"
         "· réunion du 03/11/2026 avec la CNSS ; lots 13, 24 et 25", 20)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
