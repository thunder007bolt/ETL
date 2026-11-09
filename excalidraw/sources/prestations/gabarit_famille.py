"""
Gabarit des schémas « famille de tableaux » des prestations (08/11/2026), à l'image de ce qui a
été fait pour la GRH (cas_agents_effectif, cas_mouvements_t60, gestion_administrative_2024).

Pour une famille de tableaux CIPRES (T25-T27, T28-T31, T32-T41, T42-T49, T50-T57), un schéma
se lit de haut en bas :
    0. les tableaux de la famille, en une ligne chacun ;
    1. ce que demande le recueil des besoins v7, demande par demande, et où on en est ;
    2. les règles, en clair, avec leurs sources ;
    3. les cas, carte par carte : ce que devient chaque dossier ou chaque personne ;
    4. face à l'annuaire statistique de la CNSS 2024 : ce qui confirme, tranche ou contredit ;
    5. ce qui peut changer : les réglages de la CTE parametres des requêtes proposées ;
    6. les questions ouvertes, une par point, avec ce que fait la requête en attendant ;
    7. ce qui reste à faire, et d'où viennent les règles.

Le travail existant (README, requêtes proposées, fiches de validation, fiche de lecture) n'est
pas modifié : ces schémas le reprennent et le confrontent au recueil v7 et à l'annuaire.

Chaque famille a son script (sources/prestations/famille_*.py) qui ne contient que ses données
et appelle construire(DATA).
"""

from excalidraw_builder import C, Diagram, text_size
from fiche_cipres import WIDTH, badge_width, height_of, wrap

INNER = WIDTH - 50          # largeur utile d'une section (marge de 25 de chaque côté)
X0 = 25

# Étiquettes de source : (fond, bord, ce qu'elle désigne)
SOURCES = {
    "Recueil v7": (C["bleu_moyen"], C["bleu"], "le recueil des besoins de la CNSS, version 7"),
    "Réunion": (C["bleu_clair"], C["bleu"], "une décision prise en réunion avec la CNSS"),
    "Kaya 2017": (C["violet"], C["encre"], "l'atelier de validation de la CNSS à Kaya"),
    "Annuaire CIPRES": (C["rouge_clair"], C["rouge"], "le modèle des tableaux (Annuaire CIPRES)"),
    "Annuaire CNSS 2024": (C["vert_moyen"], C["vert"], "les chiffres publiés par la CNSS pour 2024"),
    "Modèles CNSS": (C["orange_clair"], C["orange"], "les modèles de ratios de la CNSS (2025)"),
    "Données": (C["gris"], C["gris_bord"], "nos requêtes sur la base (copie du 02/02/2026)"),
    "Proposition BI": (C["jaune"], C["jaune_bord"], "notre choix, sans source écrite : à valider"),
}

# Où en est une demande du recueil : (fond, bord, libellé de la légende)
STATUTS = {
    "pris en compte": (C["vert_moyen"], C["vert"], "pris en compte"),
    "à intégrer": (C["orange_moyen"], C["orange"], "à intégrer (proposé, pas encore fait)"),
    "question": (C["bleu_moyen"], C["bleu"], "question à la CNSS"),
    "hors base": (C["gris"], C["gris_bord"], "la base n'a pas la donnée"),
    "hors BI": (C["blanc"], C["gris_bord"], "pas un calcul de la BI"),
}

# Verdict de l'annuaire CNSS 2024 : (fond, bord, libellé de la légende)
VERDICTS = {
    "confirme": (C["vert_moyen"], C["vert"], "confirme notre règle"),
    "tranche": (C["bleu_moyen"], C["bleu"], "tranche une question"),
    "precise": (C["jaune"], C["jaune_bord"], "précise une question"),
    "ecart": (C["rouge_moyen"], C["rouge"], "écart à expliquer"),
    "infirme": (C["rouge"], C["rouge"], "contredit notre règle"),
    "non publie": (C["blanc"], C["gris_bord"], "pas publié par la CNSS"),
    "non comparable": (C["gris"], C["gris_bord"], "pas comparable tel quel"),
}

# Sort d'un cas : (fond de la carte, fond du verdict, bord, épaisseur, style)
GENRES = {
    "compte": (C["blanc"], C["vert_moyen"], C["vert"], 2, "solid"),
    "pas compte": (C["blanc"], C["gris"], C["gris_bord"], 2, "solid"),
    "reglage": (C["blanc"], C["jaune"], C["jaune_bord"], 2, "solid"),
    "attention": (C["rouge_clair"], C["rouge_moyen"], C["rouge"], 3, "solid"),
    "question": (C["blanc"], C["bleu_moyen"], C["bleu"], 2, "dashed"),
}
LEGENDE_GENRES = [("compté", "compte"), ("pas compté", "pas compte"),
                  ("selon un réglage", "reglage"), ("à surveiller", "attention"),
                  ("question ouverte", "question")]

CHIP_H = 30


def chip_width(label, size=16):
    return text_size(label, size)[0] + 22


def chip(d, x, y, label, couleurs, size=16, groups=(), style="solid"):
    """Étiquette colorée ; renvoie sa largeur."""
    bg, bord = couleurs[0], couleurs[1]
    w = chip_width(label, size)
    d.box(x, y, w, CHIP_H, label, size, bg=bg, stroke=bord, style=style, groups=groups,
          color=C["blanc"] if bg == C["rouge"] else C["encre"])
    return w


def chip_source(d, x, y, tag, groups=()):
    return chip(d, x, y, tag, SOURCES[tag], groups=groups)


def chip_statut(d, x, y, statut, groups=()):
    bg, bord, _ = STATUTS[statut]
    label = "question CNSS" if statut == "question" else statut
    return chip(d, x, y, label, (bg, bord), groups=groups,
                style="dashed" if statut == "hors BI" else "solid")


def chip_verdict(d, x, y, verdict, groups=()):
    bg, bord, label = VERDICTS[verdict]
    court = {"confirme": "confirme", "tranche": "tranche", "precise": "précise",
             "ecart": "écart", "infirme": "contredit", "non publie": "non publié",
             "non comparable": "non comparable"}[verdict]
    return chip(d, x, y, court, (bg, bord), groups=groups,
                style="dashed" if verdict == "non publie" else "solid")


class Page:
    """Le schéma d'une famille : un Diagram et la position courante (y)."""

    def __init__(self, seed):
        self.d = Diagram(seed=seed)
        self.y = 0

    # ---------------------------------------------------------------- briques
    def bloc(self, x, y, w, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2, h=None):
        """Cadre de texte ; la hauteur suit le texte (coupé à la largeur). Renvoie le bas."""
        texte = wrap(txt, size, w - 44)
        h = h or height_of(texte, size) + 34
        g = self.d.group()
        b = self.d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, groups=g)
        self.d.text(x + 22, y + 16, texte, size, groups=g, inside=b)
        return y + h

    def section(self, titre):
        """Ouvre une section ; renvoie (cadre, haut du contenu)."""
        top = self.y
        cadre = self.d.section(0, top, WIDTH, 100, titre)
        return cadre, top + 80

    def fermer(self, cadre, bas):
        cadre["height"] = bas + 35 - cadre["y"]
        self.y = cadre["y"] + cadre["height"] + 50

    def legende(self, x, y, items, size=17):
        """items : [(fond, bord, libellé, style)] ; renvoie la largeur occupée."""
        lx = x
        for bg, bord, label, style in items:
            self.d.box(lx, y, 40, 22, bg=bg, stroke=bord, style=style)
            self.d.text(lx + 50, y, label, size)
            lx += 50 + text_size(label, size)[0] + 30
        return lx - x

    def tableau(self, x, y, colonnes, lignes, size=18, entete=19):
        """Tableau à hauteur de ligne variable.
        colonnes : [(titre, largeur)] ; lignes : [[cellule]], une cellule étant un texte, ou
        ("statut", clé), ("verdict", clé), ("sources", [étiquettes]).
        Renvoie le bas."""
        xs, cx = [], x
        for _, w in colonnes:
            xs.append(cx)
            cx += w
        largeur = cx - x
        # en-tête
        th = max(height_of(wrap(t, entete, w - 24), entete) for t, w in colonnes) + 22
        for (titre, w), cx in zip(colonnes, xs):
            self.d.box(cx, y, w, th, wrap(titre, entete, w - 24), entete, bg=C["gris"],
                       stroke=C["encre"])
        y += th

        def hauteur(cellule, w):
            if isinstance(cellule, tuple):
                if cellule[0] == "sources":
                    return len(cellule[1]) * (CHIP_H + 6) - 6
                return CHIP_H
            return height_of(wrap(cellule, size, w - 28), size)

        for i, ligne in enumerate(lignes):
            h = max(hauteur(c, w) for c, (_, w) in zip(ligne, colonnes)) + 26
            fond = C["blanc"] if i % 2 == 0 else C["section"]
            g = self.d.group()
            cadre = self.d.box(x, y, largeur, h, bg=fond, stroke=C["gris_bord"], stroke_width=1,
                               groups=g)
            for c, (_, w), cx in zip(ligne, colonnes, xs):
                if cx > x:
                    self.d.arrow(cx, y, cx, y + h, color=C["gris_bord"], stroke_width=1,
                                 head=None)
                if isinstance(c, tuple):
                    if c[0] == "statut":
                        chip_statut(self.d, cx + 14, y + 13, c[1], groups=g)
                    elif c[0] == "verdict":
                        chip_verdict(self.d, cx + 14, y + 13, c[1], groups=g)
                    elif c[0] == "sources":
                        for k, tag in enumerate(c[1]):
                            chip_source(self.d, cx + 14, y + 13 + k * (CHIP_H + 6), tag, groups=g)
                else:
                    self.d.text(cx + 14, y + 13, wrap(c, size, w - 28), size, groups=g,
                                inside=cadre)
            y += h
        return y

    # ---------------------------------------------------------------- en-tête
    def en_bref(self, y, points):
        """Encadré « En bref » : une puce par point ; renvoie le bas."""
        d = self.d
        lignes = [wrap("• " + p, 21, WIDTH - 64) for p in points]
        h = 16 + 40 + sum(height_of(t, 21) + 8 for t in lignes) + 12
        g = d.group()
        b = d.box(0, y, WIDTH, h, bg=C["bleu_clair"], stroke=C["bleu"], stroke_width=3, groups=g)
        d.text(22, y + 14, "En bref", 26, groups=g, inside=b)
        cy = y + 16 + 40
        for t in lignes:
            d.text(22, cy, t, 21, groups=g, inside=b)
            cy += height_of(t, 21) + 8
        return y + h

    def entete(self, titre, sous_titre, tableaux, en_bref=None):
        d = self.d
        d.text(0, 0, titre, 48)
        st = wrap(sous_titre, 22, WIDTH - 40)
        d.text(0, 66, st, 22, C["doux"])
        y = 66 + height_of(st, 22) + 24
        if en_bref:
            y = self.en_bref(y, en_bref) + 30
        # les légendes : statut du recueil, verdict de l'annuaire, sort d'un cas
        d.text(0, y, "Recueil v7 :", 17, C["doux"])
        self.legende(130, y, [(bg, bord, lab, "dashed" if k == "hors BI" else "solid")
                              for k, (bg, bord, lab) in STATUTS.items()])
        y += 34
        d.text(0, y, "Annuaire CNSS :", 17, C["doux"])
        self.legende(160, y, [(bg, bord, lab, "dashed" if k == "non publie" else "solid")
                              for k, (bg, bord, lab) in VERDICTS.items()])
        y += 34
        d.text(0, y, "Cas :", 17, C["doux"])
        self.legende(60, y, [(GENRES[k][1], GENRES[k][2], lab, GENRES[k][4])
                             for lab, k in LEGENDE_GENRES])
        y += 34
        d.text(0, y, "Sources :", 17, C["doux"])
        sx = 100
        for tag in SOURCES:
            sx += chip_source(d, sx, y - 4, tag) + 14
        y += 56
        # 0. les tableaux de la famille
        cadre, top = self.section0(y)
        bas = self.tableau(X0, top, [("Tableau", 200), ("Ce qu'il compte (Annuaire CIPRES)", 1450),
                                     ("Page", 160), ("Ce qu'on a déjà", INNER - 1810)],
                           tableaux)
        self.fermer(cadre, bas)

    def section0(self, y):
        self.y = y
        return self.section("0. Les tableaux de la famille")

    # ---------------------------------------------------------------- 1. recueil v7
    def recueil(self, demandes, note=None, titre=None):
        """demandes : [(référence, ce que dit le recueil, en clair, où c'est pris en compte,
        statut)]."""
        cadre, top = self.section(titre or "1. Ce que demande le recueil des besoins v7 (la "
                                  "source la plus forte après une décision de réunion)")
        if note:
            top = self.bloc(X0, top, INNER, note, 19, (C["bleu_clair"], C["bleu"])) + 24
        bas = self.tableau(X0, top, [("Où dans le recueil", 300),
                                     ("Ce que dit le recueil", 780),
                                     ("En clair", 640),
                                     ("Comment on le prend en compte", INNER - 300 - 780 - 640 - 220),
                                     ("Où on en est", 220)],
                           [[ref, dit, clair, ou, ("statut", st)]
                            for ref, dit, clair, ou, st in demandes])
        self.fermer(cadre, bas)

    # ---------------------------------------------------------------- 2. règles
    def regles(self, regles, par_ligne=3):
        """regles : [{id, titre, regle, exemple, sources: [(étiquette, détail)], etat}]."""
        cadre, top = self.section("2. Les règles, en clair")
        gap = 30
        w = (INNER - (par_ligne - 1) * gap) / par_ligne
        inner = w - 44
        d = self.d

        def pose(r):
            t_titre = wrap(f"{r['id']} · {r['titre']}", 22, inner)
            t_regle = wrap(r["regle"], 18, inner)
            t_ex = wrap("Exemple : " + r["exemple"], 17, inner) if r.get("exemple") else ""
            srcs = [(tag, wrap(det, 16, inner - chip_width(tag) - 10)) for tag, det in r["sources"]]
            t_etat = wrap(r["etat"], 16, inner - 20) if r.get("etat") else ""
            h = 16 + height_of(t_titre, 22) + 12 + height_of(t_regle, 18) + 14
            if t_ex:
                h += height_of(t_ex, 17) + 14
            h += sum(max(CHIP_H, height_of(det, 16) + 6) + 8 for _, det in srcs)
            if t_etat:
                h += 6 + height_of(t_etat, 16) + 20
            return t_titre, t_regle, t_ex, srcs, t_etat, h + 14

        y = top
        for debut in range(0, len(regles), par_ligne):
            rang = regles[debut:debut + par_ligne]
            poses = [pose(r) for r in rang]
            h = max(p[-1] for p in poses)
            for i, (t_titre, t_regle, t_ex, srcs, t_etat, _) in enumerate(poses):
                x = X0 + i * (w + gap)
                g = d.group()
                b = d.box(x, y, w, h, bg=C["blanc"], stroke=C["encre"], groups=g)
                cy = y + 16
                d.text(x + 22, cy, t_titre, 22, groups=g, inside=b)
                cy += height_of(t_titre, 22) + 12
                d.text(x + 22, cy, t_regle, 18, groups=g, inside=b)
                cy += height_of(t_regle, 18) + 14
                if t_ex:
                    d.text(x + 22, cy, t_ex, 17, C["doux"], groups=g, inside=b)
                    cy += height_of(t_ex, 17) + 14
                for tag, det in srcs:
                    cw = chip_source(d, x + 22, cy, tag, groups=g)
                    d.text(x + 22 + cw + 10, cy + 4, det, 16, groups=g, inside=b)
                    cy += max(CHIP_H, height_of(det, 16) + 6) + 8
                if t_etat:
                    eh = height_of(t_etat, 16) + 14
                    d.box(x + 22, y + h - 16 - eh, inner, eh, t_etat, 16, bg=C["section"],
                          stroke=C["gris_bord"], groups=g)
            y += h + gap
        self.fermer(cadre, y - gap)

    # ---------------------------------------------------------------- 3. cas
    def cas(self, cas, par_ligne=4, intro=None):
        """cas : [(titre, détail, ce qu'il donne, genre)]."""
        cadre, top = self.section("3. Les cas, carte par carte")
        if intro:
            top = self.bloc(X0, top, INNER, intro, 19, (C["vert_clair"], C["vert"])) + 24
        gap = 30
        w = (INNER - (par_ligne - 1) * gap) / par_ligne
        inner = w - 44
        d = self.d

        def pose(c):
            titre, detail, verdict, _ = c
            t = wrap(titre, 22, inner)
            dt = wrap(detail, 18, inner)
            v = wrap(verdict, 18, inner - 20)
            vh = height_of(v, 18) + 18
            return t, dt, v, vh, 16 + height_of(t, 22) + 10 + height_of(dt, 18) + 16 + vh + 16

        y = top
        for debut in range(0, len(cas), par_ligne):
            rang = cas[debut:debut + par_ligne]
            poses = [pose(c) for c in rang]
            h = max(p[-1] for p in poses)
            for i, (c, (t, dt, v, vh, _)) in enumerate(zip(rang, poses)):
                fond, fond_v, bord, sw, style = GENRES[c[3]]
                x = X0 + i * (w + gap)
                g = d.group()
                b = d.box(x, y, w, h, bg=fond, stroke=bord, stroke_width=sw, style=style, groups=g)
                d.text(x + 22, y + 16, t, 22, groups=g, inside=b)
                d.text(x + 22, y + 16 + height_of(t, 22) + 10, dt, 18, C["doux"], groups=g,
                       inside=b)
                d.box(x + 22, y + h - 16 - vh, inner, vh, v, 18, bg=fond_v, stroke=bord,
                      style=style, groups=g)
            y += h + gap
        self.fermer(cadre, y - gap)

    # ---------------------------------------------------------------- 4. annuaire
    def annuaire(self, lignes, note=None, apres=None):
        """lignes : [(tableau de l'annuaire, ce qu'on compare, nous, publié, écart, verdict,
        ce que ça change)]."""
        cadre, top = self.section("4. Face à l'annuaire statistique de la CNSS 2024")
        if note:
            top = self.bloc(X0, top, INNER, note, 19, (C["vert_clair"], C["vert"])) + 24
        largeurs = [("Tableau de l'annuaire", 300), ("Ce qu'on compare", 620), ("Nous", 220),
                    ("Publié", 220), ("Écart", 170), ("Verdict", 190)]
        reste = INNER - sum(w for _, w in largeurs)
        bas = self.tableau(X0, top, largeurs + [("Ce que ça change pour nous", reste)],
                           [[t, comp, nous, pub, ecart, ("verdict", v), effet]
                            for t, comp, nous, pub, ecart, v, effet in lignes])
        if apres:
            bas = self.bloc(X0, bas + 24, INNER, apres, 19, (C["gris"], C["gris_bord"]))
        self.fermer(cadre, bas)

    # ---------------------------------------------------------------- 5. réglages
    def reglages(self, lignes, note=None):
        """lignes : [(réglage, requête, valeur retenue, autre valeur, ce que ça change,
        qui décide)]."""
        cadre, top = self.section("5. Ce qui peut changer : les réglages (CTE parametres des "
                                  "requêtes proposées)")
        if note:
            top = self.bloc(X0, top, INNER, note, 19, (C["jaune_clair"], C["jaune_bord"])) + 24
        largeurs = [("Réglage", 300), ("Requête", 330), ("Valeur retenue", 330),
                    ("Autre valeur possible", 330)]
        reste = INNER - sum(w for _, w in largeurs) - 300
        bas = self.tableau(X0, top, largeurs + [("Ce que ça change", reste), ("Qui décide", 300)],
                           [list(l) for l in lignes])
        self.fermer(cadre, bas)

    # ---------------------------------------------------------------- 6. questions
    def questions(self, questions, par_ligne=2, note=None, titre=None):
        """questions : [(n°, question, à qui, en attendant, déjà posée où)]."""
        cadre, top = self.section(titre or "6. Les questions ouvertes (une question par point)")
        if note:
            top = self.bloc(X0, top, INNER, note, 19, (C["bleu_clair"], C["bleu"])) + 24
        gap = 30
        w = (INNER - (par_ligne - 1) * gap) / par_ligne
        inner = w - 44
        d = self.d

        def pose(q):
            n, question, qui, attente, deja = q
            tq = wrap(question, 21, inner - 70)
            tqui = wrap("À qui : " + qui, 17, inner)
            tatt = wrap("En attendant, la requête : " + attente, 17, inner - 24)
            tdeja = wrap(deja, 16, inner) if deja else ""
            h = 16 + max(44, height_of(tq, 21)) + 12 + height_of(tqui, 17) + 12
            h += height_of(tatt, 17) + 24 + 12
            if tdeja:
                h += height_of(tdeja, 16) + 8
            return tq, tqui, tatt, tdeja, h + 12

        y = top
        for debut in range(0, len(questions), par_ligne):
            rang = questions[debut:debut + par_ligne]
            poses = [pose(q) for q in rang]
            h = max(p[-1] for p in poses)
            for i, (q, (tq, tqui, tatt, tdeja, _)) in enumerate(zip(rang, poses)):
                x = X0 + i * (w + gap)
                g = d.group()
                b = d.box(x, y, w, h, bg=C["blanc"], stroke=C["bleu"], stroke_width=2, groups=g)
                bw = badge_width(q[0], 16)
                d.ellipse(x + 18, y + 14, bw, 44, q[0], 16, bg=C["bleu"], stroke=C["bleu"],
                          color=C["blanc"], groups=g)
                d.text(x + 18 + bw + 12, y + 16, tq, 21, groups=g, inside=b)
                cy = y + 16 + max(44, height_of(tq, 21)) + 12
                d.text(x + 22, cy, tqui, 17, C["doux"], groups=g, inside=b)
                cy += height_of(tqui, 17) + 12
                ah = height_of(tatt, 17) + 24
                d.box(x + 22, cy, inner, ah, bg=C["jaune_clair"], stroke=C["jaune_bord"],
                      groups=g)
                d.text(x + 34, cy + 12, tatt, 17, groups=g, inside=b)
                cy += ah + 12
                if tdeja:
                    d.text(x + 22, cy, tdeja, 16, C["doux"], groups=g, inside=b)
            y += h + gap
        self.fermer(cadre, y - gap)

    # ---------------------------------------------------------------- 7. points d'attention
    def attention(self, points, par_ligne=3, note=None, titre=None):
        """points : [(titre, ce qu'il faut savoir)]."""
        cadre, top = self.section(titre or "7. Les points d'attention")
        if note:
            top = self.bloc(X0, top, INNER, note, 19, (C["rouge_clair"], C["rouge"])) + 24
        gap = 30
        w = (INNER - (par_ligne - 1) * gap) / par_ligne
        inner = w - 44
        d = self.d
        y = top
        for debut in range(0, len(points), par_ligne):
            rang = [(wrap(t, 21, inner), wrap(txt, 18, inner))
                    for t, txt in points[debut:debut + par_ligne]]
            h = max(16 + height_of(t, 21) + 10 + height_of(txt, 18) + 18 for t, txt in rang)
            for i, (t, txt) in enumerate(rang):
                x = X0 + i * (w + gap)
                g = d.group()
                b = d.box(x, y, w, h, bg=C["blanc"], stroke=C["rouge"], stroke_width=3, groups=g)
                d.text(x + 22, y + 16, t, 21, groups=g, inside=b)
                d.text(x + 22, y + 16 + height_of(t, 21) + 10, txt, 18, C["doux"], groups=g,
                       inside=b)
            y += h + gap
        self.fermer(cadre, y - gap)

    # ---------------------------------------------------------------- 8. reste et sources
    def fin(self, reste, sources, numero=7, titre=None):
        """reste : [(tâche, qui, état)] ; sources : texte."""
        cadre, top = self.section(titre or f"{numero}. Ce qui reste à faire, et d'où viennent "
                                  "les règles")
        bas = self.tableau(X0, top, [("Tâche", 1500), ("Qui", 400), ("État", INNER - 1900)],
                           [list(r) for r in reste])
        bas = self.bloc(X0, bas + 24, INNER, sources, 19)
        self.fermer(cadre, bas)


def construire(data):
    """data : dict avec seed, titre, sous_titre, tableaux, recueil, regles, cas, annuaire,
    reglages, questions, reste, sources (et les notes facultatives *_note)."""
    p = Page(data["seed"])
    p.entete(data["titre"], data["sous_titre"], data["tableaux"], data.get("en_bref"))
    p.recueil(data["recueil"], data.get("recueil_note"))
    p.regles(data["regles"])
    p.cas(data["cas"], intro=data.get("cas_note"))
    p.annuaire(data["annuaire"], data.get("annuaire_note"), data.get("annuaire_apres"))
    p.reglages(data["reglages"], data.get("reglages_note"))
    p.questions(data["questions"], note=data.get("questions_note"))
    if data.get("attention"):
        p.attention(data["attention"], note=data.get("attention_note"))
        p.fin(data["reste"], data["sources"], numero=8)
    else:
        p.fin(data["reste"], data["sources"])
    return p.d
