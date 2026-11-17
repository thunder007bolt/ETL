"""
La proposition de rangement des schémas (17/11/2026) : ce qui ne va pas, l'arborescence
d'aujourd'hui face à celle proposée, les documents à archiver (déplacés, jamais supprimés), les
schémas annotés à garder, les étapes et les espaces de travail. Rien n'est déplacé par ce script :
c'est le document à valider avant de ranger.

Inventaire fait le 17/11/2026 sur schemas/ (137 schémas, 28 dossiers), sources/, historique/,
le README, le journal et la mémoire.

Usage :
    python3 excalidraw/sources/rangement/proposition_rangement.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402
from fiche_cipres import height_of, wrap  # noqa: E402

OUTPUT = ROOT / "schemas" / "rangement" / "proposition de rangement.excalidraw"
LARGEUR, GAP, PAD, TAILLE = 2700, 30, 20, 20

PROBLEMES = [
    ("Trop d'entrées à la racine", "12 entrées. Six dossiers transverses n'ont que 1 à 4 schémas : "
     "annuaire cnss, besoins cnss, regles cipres, vues cnss, reunions, lexique."),
    ("Des dossiers presque vides", "11 dossiers ne contiennent qu'un seul schéma, dont les cinq "
     "dossiers de famille des prestations."),
    ("Le même tableau, quatre écritures", "« T25 T27 dossiers », « T25_dossiers_de_pension », "
     "« T25 regles », « regles_t25_t27_dossiers ». 44 noms avec _, 64 avec des espaces."),
    ("Deux domaines, deux rangements", "En prestations, un dossier « T.. » contient les règles ; "
     "en GRH, il contient les explications (cas, requête pas à pas)."),
    ("Tout est mélangé", "Ce qu'on montre à la CNSS, les règles de travail et les schémas pour "
     "comprendre sont au même niveau."),
    ("Anciennes et nouvelles versions", "Points à valider v1, v2, v3 côte à côte ; des fiches "
     "du 30/10 à côté de celles du 17/11 ; trois essais vides."),
]

AUJOURDHUI = """schemas/                          137 schémas
├─ Immatriculation                 essai
├─ Tests/                          essai
├─ annuaire cnss/                  3
├─ besoins cnss/                   1
├─ grh/                            26
│  ├─ (à la racine)                5
│  ├─ T58 effectif/                3
│  ├─ T60 mouvements/              3
│  ├─ T63 T64 finances/            1
│  ├─ regles par tableau/          8
│  └─ tableaux cipres/             3
├─ journal/                        4
│  └─ jours/                       25
├─ lexique/                        1
├─ maitrise/                       15
├─ prestations/                    57
│  ├─ (à la racine)                8
│  ├─ T25 T27 dossiers/ … (5)      1 chacun
│  ├─ regles par tableau/          33
│  ├─ tableaux cipres/             8
│  └─ vie_dossier/                 3
├─ regles cipres/                  1
├─ reunions/                       1
└─ vues cnss/                      4"""

PROPOSE = """schemas/                          6 dossiers à la racine
├─ journal/                        inchangé
├─ commun/                         ce qui vaut pour plusieurs domaines
│  ├─ fiche regles                 les 99 règles imposées
│  ├─ remarques besoins v7         le recueil des besoins
│  ├─ lexique
│  └─ vues cnss/                   4
├─ prestations/
│  ├─ 1 pour la cnss/
│  │  ├─ regles par tableau/       33
│  │  ├─ fiches de validation/     8 (ex « tableaux cipres »)
│  │  ├─ points a valider          ta version annotée
│  │  └─ points a valider v3
│  ├─ 2 regles et sources/         les 5 familles, besoins v7,
│  │                               annuaire CNSS 2024 (2)
│  └─ 3 comprendre/                domaine, tables, prestation esp,
│     └─ vie dossier/              fiche de lecture ; vie dossier (3)
├─ grh/
│  ├─ 1 pour la cnss/
│  │  ├─ regles par tableau/       8
│  │  ├─ fiches de validation/     3
│  │  └─ points a valider v2
│  ├─ 2 regles et sources/         annuaire CNSS 2024, réunion du 03/11
│  └─ 3 comprendre/                fiche de lecture, T63 T64
│     ├─ T58 effectif/             3
│     └─ T60 mouvements/           3
├─ maitrise/                       inchangé (à mettre à jour plus tard)
└─ archives/                       rien n'est supprimé
   ├─ essais/                      Immatriculation, Tests, grh effectif
   ├─ grh/                         points a valider (v1), regles grh
   └─ prestations/                 regles prestations"""

ARCHIVES = [
    ("Dans archives/ (6 schémas)", "vert", [
        "Immatriculation, Tests/ et grh/effectif : trois essais vides.",
        "grh/points_a_valider (01/11) : remplacé par la version 2 du 17/11, sans annotation.",
        "grh/regles_grh (01/11) : G1 à G11 seulement, avant le recueil v7 et la décision du "
        "08/11. Repris par la fiche des règles, les règles par tableau et les points v2.",
        "prestations/regles_prestations (30/10) : donne encore la photo de fin mars. Repris par "
        "les 5 familles et les règles par tableau.",
    ]),
    ("Gardés, mais déplacés", "jaune", [
        "Les 11 fiches « tableaux cipres » : dans « 1 pour la cnss/fiches de validation ». "
        "La CNSS doit encore y répondre.",
        "Les 3 schémas de l'annuaire CNSS 2024 : chacun dans son domaine, avec « annuaire cnss "
        "2024 » dans le nom.",
        "La réunion GRH du 03/11 : dans grh, « 2 regles et sources ».",
        "Les dossiers d'un seul schéma disparaissent : le schéma remonte d'un niveau "
        "(les 5 familles, regles cipres, besoins cnss, lexique, T63 T64 finances).",
    ]),
    ("Gardés, à mettre à jour plus tard", "orange", [
        "maitrise/ (15 fiches du 30/10) : reste en place.",
        "prestations/prestation_esp : montre les règles d'avant le 30/10.",
        "prestations/fiche_de_lecture : parle encore de la photo de fin mars.",
        "11 schémas des prestations citent encore la photo de fin mars au lieu de fin janvier.",
    ]),
]

A_GARDER = [
    "prestations/points_a_valider : tes 42 captures, 25 flèches et tes notes (jusqu'au 17/11).",
    "grh/fiche_de_lecture : tes réponses en vert et ton post-it sur le filtre de nature.",
    "grh/T58 effectif/cas_agents_effectif : tes 3 « à mieux éclaircir ».",
    "prestations/T25 T27 dossiers/regles_t25_t27_dossiers : tes 5 pastilles de couleur.",
    "prestations/vie_dossier/cheminement_rente : l'image que tu y as collée.",
]

ETAPES = [
    ("Ce que je fais", "Je déplace environ 100 schémas par le même chemin que la visionneuse : "
     "l'historique, tes annotations et tes espaces de travail suivent. Aucun schéma n'est "
     "régénéré ni modifié.", "vert"),
    ("Ce que je mets à jour", "La sortie d'environ 45 scripts, les liens du README, du journal "
     "et des analyses. Puis je vérifie que chaque script retrouve son schéma à sa nouvelle "
     "place.", "jaune"),
    ("Ce qui change pour toi", "13 entrées à la racine deviennent 6 dossiers. Tes espaces de "
     "travail restent les mêmes. Seuls les marqueurs de lecture des schémas déplacés sont à "
     "reposer.", "orange"),
]

ESPACES = [
    ("Réunion CNSS", "les règles par tableau, les points à valider, la fiche des règles, les vues"),
    ("Prestations", "tout le dossier prestations et ses deux schémas de l'annuaire"),
    ("GRH", "tout le dossier grh, sa partie de l'annuaire et la réunion du 03/11"),
    ("Suivi du projet", "le journal"),
    ("Réviser", "les fiches de maîtrise et le lexique"),
]

NOMMAGE = [
    "Les noms des schémas ne changent pas (la visionneuse affiche déjà les _ comme des "
    "espaces), sauf quatre : les 3 de l'annuaire et le lexique.",
    "Nouveaux dossiers et schémas : minuscules, sans accent, mots séparés par des espaces.",
    "Tnn en tête pour un tableau ou une famille ; un numéro pour un ordre de lecture ; "
    "AAAA-MM-JJ pour un document daté.",
    "Pas de numéro de version dans le nom : l'ancienne version part dans archives/.",
    "Les déplacements passent par la visionneuse : l'historique, tes annotations et les espaces "
    "de travail suivent. Jamais par le Finder.",
]


def carte(d, x, y, w, titre, corps, bg, stroke, h=None):
    """Un encadré : un titre et un texte ; renvoie sa hauteur."""
    texte = wrap(corps, TAILLE, w - 2 * PAD)
    hauteur = h or 2 * PAD + 34 + height_of(texte, TAILLE)
    cadre = d.box(x, y, w, hauteur, bg=bg, stroke=stroke, stroke_width=1)
    d.text(x + PAD, y + PAD, wrap(titre, 22, w - 2 * PAD), 22, color=C["bleu"], inside=cadre)
    d.text(x + PAD, y + PAD + 34, texte, TAILLE, inside=cadre)
    return hauteur


def hauteur_carte(w, corps):
    return 2 * PAD + 34 + height_of(wrap(corps, TAILLE, w - 2 * PAD), TAILLE)


def rangee(d, y, cartes, par_ligne):
    """Des cartes (titre, corps, bg, stroke) par lignes de même hauteur ; renvoie le bas."""
    w = (LARGEUR - 50 - (par_ligne - 1) * GAP) / par_ligne
    for debut in range(0, len(cartes), par_ligne):
        ligne = cartes[debut:debut + par_ligne]
        h = max(hauteur_carte(w, corps) for _, corps, *_ in ligne)
        for i, (titre, corps, bg, stroke) in enumerate(ligne):
            carte(d, 25 + i * (w + GAP), y, w, titre, corps, bg, stroke, h)
        y += h + GAP
    return y - GAP


def puces(lignes):
    return "\n".join(f"• {ligne}" for ligne in lignes)


def build():
    d = Diagram(seed=17)
    d.text(0, 0, "Ranger les schémas : la proposition", 48)
    d.text(0, 66, "État au 17/11/2026. 138 schémas dans 29 dossiers. Rien n'est encore déplacé : "
           "dis si tu valides, et je range tout en une passe.", 22, C["doux"])
    y = 130

    sec = d.section(0, y, LARGEUR, 0, "1. Ce qui ne va pas aujourd'hui")
    bas = rangee(d, y + 80, [(t, c, C["rouge_clair"], C["rouge"]) for t, c in PROBLEMES], 3)
    sec["height"] = bas + 30 - y
    y = bas + 70

    sec = d.section(0, y, LARGEUR, 0, "2. Le rangement : aujourd'hui, et proposé")
    w = (LARGEUR - 50 - GAP) / 2
    h = max(height_of(AUJOURDHUI, 18), height_of(PROPOSE, 18)) + 2 * PAD + 44
    for i, (titre, arbre, bg, stroke) in enumerate([
            ("Aujourd'hui", AUJOURDHUI, C["blanc"], C["gris_bord"]),
            ("Proposé : le même squelette dans chaque domaine", PROPOSE, C["vert_clair"], C["vert"])]):
        x = 25 + i * (w + GAP)
        cadre = d.box(x, y + 80, w, h, bg=bg, stroke=stroke, stroke_width=1)
        d.text(x + PAD, y + 80 + PAD, titre, 24, color=C["bleu"], inside=cadre)
        d.text(x + PAD, y + 80 + PAD + 44, arbre, 18, mono=True, inside=cadre)
    bas = y + 80 + h
    regle = ("Quatre règles. 1. Un document qui ne parle que d'un domaine va dans ce domaine ; "
             "commun/ reçoit ce qui en touche au moins deux. 2. Chaque domaine a les mêmes trois "
             "dossiers : 1 pour la cnss, 2 regles et sources, 3 comprendre. 3. Pas de dossier "
             "pour un seul schéma. 4. Une version remplacée part dans archives/. Un nouveau "
             "domaine (immatriculation, recouvrement, finances) se crée sur ce modèle.")
    regle = wrap(regle, TAILLE, LARGEUR - 50 - 2 * PAD)
    cadre = d.box(25, bas + GAP, LARGEUR - 50, height_of(regle, TAILLE) + 2 * PAD,
                  bg=C["bleu_clair"], stroke=C["bleu"], stroke_width=1)
    d.text(25 + PAD, bas + GAP + PAD, regle, TAILLE, inside=cadre)
    bas += GAP + height_of(regle, TAILLE) + 2 * PAD
    sec["height"] = bas + 30 - y
    y = bas + 70

    sec = d.section(0, y, LARGEUR, 0, "3. Ce que devient chaque document (rien n'est supprimé)")
    bas = rangee(d, y + 80, [(t, puces(lignes), C[f"{couleur}_clair"],
                              C["jaune_bord"] if couleur == "jaune" else C[couleur])
                             for t, couleur, lignes in ARCHIVES], 3)
    bas = rangee(d, bas + GAP, [("À garder absolument : tes schémas annotés", puces(A_GARDER),
                                 C["bleu_clair"], C["bleu"])], 1)
    sec["height"] = bas + 30 - y
    y = bas + 70

    sec = d.section(0, y, LARGEUR, 0, "4. Le rangement, en une seule passe")
    bas = rangee(d, y + 80, [(t, c, C[f"{couleur}_clair"],
                              C["jaune_bord"] if couleur == "jaune" else C[couleur])
                             for t, c, couleur in ETAPES], 3)
    sec["height"] = bas + 30 - y
    y = bas + 70

    sec = d.section(0, y, LARGEUR, 0, "5. Les espaces de travail (déjà en place) et les noms")
    espaces = puces(f"{nom} : {contenu}." for nom, contenu in ESPACES)
    bas = rangee(d, y + 80, [
        ("Cinq espaces de départ, dans le menu en haut de la liste",
         espaces + "\nUn espace est une vue par thème : il ne déplace rien. Tu peux en créer, "
         "en renommer et en supprimer.", C["violet"], C["encre"]),
        ("La règle de nommage proposée", puces(NOMMAGE), C["blanc"], C["gris_bord"]),
    ], 2)
    sec["height"] = bas + 30 - y
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
