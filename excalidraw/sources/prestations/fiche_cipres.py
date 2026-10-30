"""
Gabarit des fiches de validation des tableaux CIPRES : une feuille par tableau, pour que le
métier valide ou corrige ce que l'équipe BI a compris.

Une fiche se lit de haut en bas :
    bandeau « comment valider » et légende des sources ;
    1. le tableau en un coup d'œil : à quoi il sert, la maquette vide de l'Annuaire, avec le
       numéro de la règle de chaque colonne, et les premiers chiffres ;
    2. des dossiers fictifs placés sur un calendrier, pour voir comment chacun est compté ;
    3. les lignes : quelle prestation va dans quelle ligne ;
    4. une carte par règle : la règle en clair, ses sources, les cases de réponse ;
    5. les questions au métier, sur le même modèle.

Chaque justification porte l'étiquette de sa source (SOURCES) ; « Proposition BI » marque un
choix de l'équipe, sans source écrite, à valider en priorité.
"""

from datetime import date

from excalidraw_builder import C, LINE_HEIGHT, text_size

WIDTH = 2600
SOURCES = {
    # étiquette : (fond, bord, ce qu'elle désigne)
    "Annuaire": (C["rouge_clair"], C["rouge"], "l'Annuaire statistique CIPRES, avec la page"),
    "Kaya 2017": (C["violet"], C["encre"], "l'atelier de validation de la CNSS à Kaya"),
    "Base": (C["gris"], C["gris_bord"], "les libellés de la base des prestations"),
    "Données": (C["gris"], C["gris_bord"], "nos requêtes sur la base (copie du 02/02/2026)"),
    "Proposition BI": (C["jaune"], C["jaune_bord"], "notre choix, sans source écrite : à valider"),
}
ANSWERS = ["D'accord", "Pas d'accord", "À discuter"]
# sort d'un dossier fictif dans le calendrier : (fond, bord, légende)
OUTCOMES = {
    "moins15": (C["vert_moyen"], C["vert"], "liquidé en moins de 15 jours"),
    "15a45": (C["jaune"], C["jaune_bord"], "liquidé en 15 à 45 jours"),
    "plus45": (C["rouge_moyen"], C["rouge"], "liquidé en plus de 45 jours"),
    "rejet": (C["gris"], C["gris_bord"], "rejeté"),
    # montré dans la légende seulement si un dossier l'utilise
    "instance": (C["bleu_moyen"], C["bleu"], "toujours sans décision"),
}
MONTHS = ["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.",
          "déc."]


def wrap(text, size, width):
    """Coupe le texte en lignes qui tiennent dans width (les retours à la ligne sont gardés)."""
    lines = []
    for paragraph in text.split("\n"):
        line = ""
        for word in paragraph.split(" "):
            candidate = f"{line} {word}" if line else word
            if line and text_size(candidate, size)[0] > width:
                lines.append(line)
                line = word
            else:
                line = candidate
        lines.append(line)
    return "\n".join(lines)


def height_of(text, size):
    return (text.count("\n") + 1) * size * LINE_HEIGHT


BADGE_H, CHIP_H = 44, 30


def badge_width(label, size=16):
    # le texte d'un ovale tient dans le rectangle inscrit, 0,7 fois sa taille
    return max(BADGE_H, (text_size(label, size)[0] + 14) / 0.707)


def badge(d, x, y, label, size=16, groups=()):
    """Pastille foncée qui porte un numéro de règle ou de question."""
    return d.ellipse(x, y, badge_width(label, size), BADGE_H, label, size, bg=C["encre"],
                     stroke=C["encre"], color=C["blanc"], groups=groups)


def chip(d, x, y, tag, groups=()):
    """Étiquette de source ; renvoie sa largeur."""
    bg, stroke, _ = SOURCES[tag]
    w = text_size(tag, 16)[0] + 22
    d.box(x, y, w, CHIP_H, tag, 16, bg=bg, stroke=stroke, groups=groups)
    return w


def banner(d, y, how_to):
    """Bandeau du haut : comment valider, et la légende des sources. Renvoie le bas du bandeau."""
    g = d.group()
    frame = d.box(25, y, WIDTH - 50, 250, bg=C["jaune_clair"], stroke=C["jaune_bord"], groups=g)
    d.text(45, y + 16, "Comment valider cette fiche", 22, groups=g, inside=frame)
    d.text(45, y + 54, wrap(how_to, 18, 1180), 18, groups=g, inside=frame)
    d.text(1330, y + 16, "D'où vient chaque justification", 22, groups=g, inside=frame)
    for i, (tag, (_, _, meaning)) in enumerate(SOURCES.items()):
        cy = y + 54 + i * 36
        w = chip(d, 1330, cy, tag, groups=g)
        d.text(1330 + w + 12, cy + 5, meaning, 16, groups=g, inside=frame)
    return y + 250


def mockup(d, x, y, label_w, col_w, label_head, label_rule, columns, groups, rows, row_h=54):
    """Maquette vide du tableau, comme dans l'Annuaire. Au-dessus de chaque colonne, le numéro de
    la règle qui la calcule.

    columns : [(libellé, règle)] ; groups : [(première, dernière, libellé, règle)] pour un en-tête
    commun à plusieurs colonnes ; rows : [(libellé, étiquette de source ou None, règle ou None)],
    une ligne en capitales (TOTAL) étant grisée.
    Renvoie le bas de la maquette.
    """
    group_h, head_h = 48, 84
    top = y + BADGE_H + 10  # ligne des numéros de règle au-dessus
    d.text(x, y + 12, "règle n°", 16, C["doux"])

    def col_x(i):
        return x + label_w + i * col_w

    # numéros de règle : un par colonne, ou un pour tout un groupe de colonnes
    grouped = {i for first, last, _, _ in groups for i in range(first, last + 1)}
    for first, last, _, rule in groups:
        cx = (col_x(first) + col_x(last + 1)) / 2
        badge(d, cx - badge_width(rule) / 2, y, rule)
    for i, (_, rule) in enumerate(columns):
        if i not in grouped:
            badge(d, col_x(i) + (col_w - badge_width(rule)) / 2, y, rule)
    badge(d, x + (label_w - badge_width(label_rule)) / 2, y, label_rule)

    full_h = group_h + head_h
    d.box(x, top, label_w, full_h, label_head, 18, bg=C["gris"], stroke=C["encre"])
    for first, last, title, _ in groups:
        d.box(col_x(first), top, (last - first + 1) * col_w, group_h, title, 18, bg=C["gris"],
              stroke=C["encre"])
    for i, (label, _) in enumerate(columns):
        if i in grouped:
            d.box(col_x(i), top + group_h, col_w, head_h, wrap(label, 16, col_w - 20), 16,
                  bg=C["gris"], stroke=C["encre"])
        else:
            d.box(col_x(i), top, col_w, full_h, wrap(label, 16, col_w - 20), 16, bg=C["gris"],
                  stroke=C["encre"])

    ry = top + full_h
    for label, tag, rule in rows:
        is_total = label.isupper()
        bg = C["gris"] if is_total else C["blanc"]
        cell = d.box(x, ry, label_w, row_h, bg=bg, stroke=C["encre"])
        d.text(x + 14, ry + (row_h - 22.5) / 2, label, 18, inside=cell)
        if tag:
            w = text_size(tag, 16)[0] + 22
            chip(d, x + label_w - w - 10, ry + (row_h - CHIP_H) / 2, tag)
        if rule:
            badge(d, x + label_w - badge_width(rule) - 10, ry + (row_h - BADGE_H) / 2, rule)
        for i in range(len(columns)):
            d.box(col_x(i), ry, col_w, row_h, bg=bg, stroke=C["encre"])
        ry += row_h
    return ry


def _card_layout(w, card):
    """Positions relatives des éléments d'une carte de règle ou de question, et sa hauteur."""
    inner = w - 28
    ops = []
    title = wrap(card["titre"], 22, inner - badge_width(card["n"]) - 12)
    ops.append(("badge", 14, 12, card["n"]))
    ops.append(("text", 14 + badge_width(card["n"]) + 12, 22, title, 22, C["encre"]))
    y = 12 + max(BADGE_H, height_of(title, 22) + 10) + 12
    rule = wrap(card["regle"], 18, inner)
    ops.append(("text", 14, y, rule, 18, C["encre"]))
    y += height_of(rule, 18) + 14
    for label, text in card.get("cas", []):
        body = wrap(text, 16, inner - 24)
        h = 10 + 22 + height_of(body, 16) + 10
        ops.append(("case", 14, y, label, body, h))
        y += h + 8
    if card.get("cas"):
        y += 6
    for tag, text in card.get("sources", []):
        cw = text_size(tag, 16)[0] + 22
        body = wrap(text, 16, inner - cw - 10)
        ops.append(("chip", 14, y, tag))
        ops.append(("text", 14 + cw + 10, y + 5, body, 16, C["encre"]))
        y += max(CHIP_H, height_of(body, 16) + 8) + 8
    if card.get("chiffre"):
        figure = wrap(card["chiffre"], 16, inner)
        ops.append(("text", 14, y + 2, figure, 16, C["doux"]))
        y += height_of(figure, 16) + 12
    ops.append(("answers", 14, y + 6))
    y += 6 + 30 + 12
    ops.append(("text", 14, y, "Remarque :", 16, C["doux"]))
    y += 20 + 46
    return ops, y


def card_height(w, card):
    return _card_layout(w, card)[1]


def card(d, x, y, w, h, card):
    """Carte d'une règle ou d'une question : numéro, titre, la règle en clair, ce que change
    chaque réponse possible (clé « cas » : un encadré par réponse), ses sources avec leur
    étiquette, un chiffre utile, puis les cases de réponse et la place pour une remarque."""
    ops, _ = _card_layout(w, card)
    g = d.group()
    frame = d.box(x, y, w, h, bg=C["blanc"], stroke=C["gris_bord"], groups=g)
    for op in ops:
        kind, dx, dy = op[:3]
        if kind == "badge":
            badge(d, x + dx, y + dy, op[3], groups=g)
        elif kind == "text":
            d.text(x + dx, y + dy, op[3], op[4], op[5], groups=g, inside=frame)
        elif kind == "chip":
            chip(d, x + dx, y + dy, op[3], groups=g)
        elif kind == "case":
            panel = d.box(x + dx, y + dy, w - 28, op[5], bg=C["bleu_clair"], stroke=C["bleu_moyen"],
                          groups=g)
            d.text(x + dx + 12, y + dy + 10, op[3], 16, C["bleu"], groups=g, inside=panel)
            d.text(x + dx + 12, y + dy + 32, op[4], 16, C["encre"], groups=g, inside=panel)
        elif kind == "answers":
            ax = x + dx
            for label in ANSWERS:
                d.box(ax, y + dy + 2, 22, 22, bg=C["blanc"], stroke=C["encre"], groups=g)
                d.text(ax + 30, y + dy + 2, label, 16, groups=g, inside=frame)
                ax += 30 + text_size(label, 16)[0] + 26
    return frame


def sql_height(code, size=16):
    return 44 + height_of(code, size) + 20


def cards_with_sql(d, x, y, w, cards, per_row=2, gap=40, inner_gap=16, size=16):
    """Cartes de règles, chacune suivie à droite de sa traduction en SQL (clé « sql » de la carte),
    per_row règles par rangée. Renvoie le bas."""
    unit = (w - (per_row - 1) * gap) / per_row
    code_w = max(text_size(c["sql"], size, mono=True)[0] for c in cards) + 36
    card_w = unit - inner_gap - code_w
    for start in range(0, len(cards), per_row):
        row = cards[start:start + per_row]
        h = max(max(card_height(card_w, c), sql_height(c["sql"], size)) for c in row)
        for i, c in enumerate(row):
            cx = x + i * (unit + gap)
            card(d, cx, y, card_w, h, c)
            g = d.group()
            panel = d.box(cx + card_w + inner_gap, y, code_w, h, bg=C["gris"], stroke=C["gris_bord"],
                          groups=g)
            d.text(panel["x"] + 18, y + 14, f"Règle {c['n']} en SQL", 16, C["doux"], groups=g,
                   inside=panel)
            d.text(panel["x"] + 18, y + 44, c["sql"], size, groups=g, inside=panel, mono=True)
        y += h + gap / 2
    return y - gap / 2


def cards_grid(d, x, y, w, per_row, cards, gap=20):
    """Cartes rangées par lignes de per_row, chaque ligne à la hauteur de sa plus haute carte.
    Renvoie le bas de la grille."""
    cw = (w - (per_row - 1) * gap) / per_row
    for start in range(0, len(cards), per_row):
        row = cards[start:start + per_row]
        h = max(card_height(cw, c) for c in row)
        for i, c in enumerate(row):
            card(d, x + i * (cw + gap), y, cw, h, c)
        y += h + gap
    return y - gap


def timeline(d, x, y, w, start, end, year, rows, label_w=300, result_w=520, row_h=104):
    """Dossiers fictifs placés sur un calendrier, de la réception à la décision, avec ce que
    chacun donne dans le tableau. rows : [{nom, detail, debut, fin, sortie, dates, resultat}],
    sortie étant une clé de OUTCOMES. Renvoie le bas, légende comprise."""
    ax = x + label_w + 20
    aw = w - label_w - result_w - 60
    rx = ax + aw + 40
    span = (end - start).days

    def px(day):
        return ax + (day - start).days * aw / span

    d.text(x, y, "Le dossier", 20, C["doux"])
    d.text(ax, y, "Dans le calendrier : de la réception à la décision", 20, C["doux"])
    d.text(rx, y, "Ce qu'il donne dans le tableau", 20, C["doux"])
    top = y + 40
    axis_y = top + 56
    bottom = axis_y + 14 + len(rows) * row_h

    first, last = px(date(year, 1, 1)), px(date(year, 12, 31))
    d.box(first, top, last - first, bottom - top, bg=C["bleu_clair"], stroke=C["bleu"],
          style="dashed")
    d.text(first + 10, top + 6, f"Exercice {year} : du 1er janvier au 31 décembre", 16, C["bleu"])
    d.arrow(last, top, last, bottom, color=C["rouge"], style="dashed", head=None)
    d.text(last + 8, top + 6, f"31/12/{year}", 16, C["rouge"])

    d.arrow(ax, axis_y, ax + aw, axis_y, color=C["doux"], head=None)
    month = date(start.year, start.month, 1)
    while month <= end:
        if month >= start:
            label = MONTHS[month.month - 1] + (f" {month.year}" if month.month == 1 else "")
            d.arrow(px(month), axis_y - 5, px(month), axis_y + 5, color=C["doux"], head=None)
            d.text(px(month) + 3, axis_y - 24, label, 14, C["doux"])
        month = date(month.year + month.month // 12, month.month % 12 + 1, 1)

    for i, row in enumerate(rows):
        ry = axis_y + 14 + i * row_h
        d.text(x, ry + 8, row["nom"], 20)
        d.text(x, ry + 38, wrap(row["detail"], 16, label_w), 16, C["doux"])
        bx, ex = px(row["debut"]), px(row["fin"])
        bg, stroke, _ = OUTCOMES[row["sortie"]]
        d.box(bx, ry + 36, max(ex - bx, 6), 26, bg=bg, stroke=stroke)
        tw = text_size(row["dates"], 16)[0]
        d.text(min(bx, rx - 30 - tw), ry + 8, row["dates"], 16)
        d.text(rx, ry + 8, wrap(row["resultat"], 18, result_w), 18)

    lx = x
    used = {row["sortie"] for row in rows}
    for key, (bg, stroke, label) in OUTCOMES.items():
        if key == "instance" and key not in used:
            continue
        d.box(lx, bottom + 16, 40, 22, bg=bg, stroke=stroke)
        d.text(lx + 50, bottom + 16, label, 16)
        lx += 50 + text_size(label, 16)[0] + 40
    return bottom + 46


def line_cards(d, x, y, w, lines, gap=16):
    """Une carte par ligne du tableau : son nom, les prestations qu'on y compte, et ce qu'il faut
    savoir, avec la source. lines : [(n, nom, prestations comptées, à noter, étiquette)].
    Renvoie le bas des cartes."""
    cw = (w - (len(lines) - 1) * gap) / len(lines)
    inner = cw - 24
    parts = []
    for n, name, counted, note, tag in lines:
        parts.append((wrap(f"{n}. {name}", 20, inner), wrap(counted, 16, inner),
                      wrap(note, 16, inner) if note else "", tag))
    head_h = max(height_of(title, 20) for title, *_ in parts) + 24
    body_h = max(height_of(body, 16) + (14 + (CHIP_H + 8 if tag else 0) + height_of(note, 16) if note else 0)
                 for _, body, note, tag in parts) + 30
    for i, (title, body, note, tag) in enumerate(parts):
        cx = x + i * (cw + gap)
        g = d.group()
        frame = d.box(cx, y, cw, head_h + body_h, bg=C["blanc"], stroke=C["encre"], groups=g)
        head = d.box(cx, y, cw, head_h, bg=C["gris"], stroke=C["encre"], groups=g)
        d.text(cx + 12, y + 12, title, 20, groups=g, inside=head)
        d.text(cx + 12, y + head_h + 12, body, 16, groups=g, inside=frame)
        if note:
            ny = y + head_h + 12 + height_of(body, 16) + 14
            if tag:
                chip(d, cx + 12, ny, tag, groups=g)
                ny += CHIP_H + 8
            d.text(cx + 12, ny, note, 16, C["doux"], groups=g, inside=frame)
    return y + head_h + body_h
