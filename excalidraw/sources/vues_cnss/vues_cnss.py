"""
Les vues à demander à la CNSS (proposition du 07/11/2026).

Les données dont la BI a besoin et qu'elle ne trouve pas dans les bases
sources, d'après le recueil des besoins v7, l'Annuaire CIPRES et l'atelier de
Kaya (2017). La CNSS crée les vues dans la base source, la BI les lit comme les
autres tables. Pas de SQL : des structures simples (colonnes, types, grain).

Quatre schémas, dans schemas/vues cnss/ :
  00 sommaire                 le principe, les trois vues, ce qui est déjà là
  01 balance comptable        V_BI_BALANCE_COMPTABLE (DSI, DFC) : T63, T64,
                              T66 à T76, SP1, SP2
  02 controles a posteriori   V_BI_CONTROLE_POSTERIORI (DSI, DAI) : T28 à T30
  03 reserves                 V_BI_RESERVE (DFC) : T75

Les données comptables de la copie (STG_GL) ne sont pas lues : la CNSS les dit
non valides (07/11/2026).

Usage :
    python3 excalidraw/sources/vues_cnss/vues_cnss.py [--force]
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import C, Diagram  # noqa: E402
from commun import (BALANCE_COLONNES, BALANCE_EXEMPLE_LARGEURS,  # noqa: E402
                    BALANCE_EXEMPLE_NOMS, BALANCE_LIGNE, BALANCE_NOM, BALANCE_REGLES,
                    WIDTH, bloc, entete, exemple, rangee, tableau, tableau_colonnes)

OUT_DIR = ROOT / "schemas" / "vues cnss"

BLEU = (C["bleu_clair"], C["bleu"])
JAUNE = (C["jaune_clair"], C["jaune_bord"])
VERT = (C["vert_clair"], C["vert"])
BLANC = (C["blanc"], C["encre"])
GRIS = (C["gris"], C["gris_bord"])


def section(d, top, titre):
    return d.section(0, top, WIDTH, 300, titre)


def fermer(sec, bas):
    """Ajuste la hauteur d'une section à son contenu ; renvoie le haut de la suivante."""
    sec["height"] = bas + 40 - sec["y"]
    return bas + 40 + 50


def bloc_nom(d, top, nom, ligne, alimentation):
    return bloc(d, 40, top, WIDTH - 80,
                f"NOM PROPOSÉ : {nom} (une vue dans la base source ; la CNSS choisit comment "
                f"l'alimenter : {alimentation})\nUNE LIGNE = {ligne}", 22, VERT, sw=3)


# ================================================================ 00 sommaire
def sommaire():
    d = Diagram(seed=70)
    s = entete(d, "Les vues à créer par la CNSS",
               "Les données dont la BI a besoin pour les tableaux CIPRES et qu'elle ne trouve "
               "pas dans les bases sources · prestations, gestion financière, GRH · "
               "proposition de l'équipe BI du 07/11/2026")

    sec = section(d, s, "1. Le principe")
    bas = rangee(d, s + 80, [
        ("LA CNSS CRÉE LES VUES\ndans la base source, à côté des tables que la BI lit déjà. "
         "Elle choisit comment les alimenter : Oracle GL, saisie, fichier Excel chargé dans "
         "une table…", BLEU, 2),
        ("LA BI LES LIT\ncomme les autres tables de la base source, avec le même chargement "
         "chaque mois. Les calculs des tableaux CIPRES restent du côté de la BI.", BLEU, 2),
        ("TROIS RÈGLES\n· des noms qui commencent par V_BI_\n· ni nom de personne, ni numéro "
         "bancaire\n· les 5 derniers exercices et l'exercice en cours", JAUNE, 2),
    ], size=21)
    s = fermer(sec, bas)

    sec = section(d, s, "2. Les trois vues")
    y = s + 80
    for txt, couleurs in [
        (f"VUE 1 · {BALANCE_NOM} : la balance générale d'Oracle GL, mois par mois, avec le "
         "budget\nAlimentée par : la DSI, avec la DFC.\n"
         f"Une ligne = {BALANCE_LIGNE}.\n"
         "Tableaux : GRH T63, T64 ; gestion financière T66 à T70, SP1, SP2, T72, T73, T74 ; "
         "action sanitaire et sociale T76 ; et les charges techniques de T75.\n"
         "Une seule vue, parce que tous ces tableaux se calculent avec des montants par compte. "
         "Elle remplace BI_GRH_SOLDE_COMPTABLE, proposée d'abord pour T63-T64.\n"
         "Détail : schéma « 01 balance comptable ».", VERT),
        ("VUE 2 · V_BI_CONTROLE_POSTERIORI : les contrôles a posteriori des dossiers de "
         "prestations déjà liquidés\nAlimentée par : la DSI, avec la DAI.\n"
         "Une ligne = un contrôle d'un dossier.\n"
         "Tableaux : T28 (PVID), T29 (AT/MP), T30 (PF) ; l'indu constaté aide aussi à "
         "vérifier T31.\nDétail : schéma « 02 controles a posteriori ».", VERT),
        ("VUE 3 · V_BI_RESERVE : les réserves de l'organisme, par branche et par type\n"
         "Alimentée par : la DFC (saisie ou fichier Excel chargé).\n"
         "Une ligne = un exercice × une branche × un type de réserve.\n"
         "Tableau : T75.\nDétail : schéma « 03 reserves ».", VERT),
    ]:
        b = bloc(d, 40, y, WIDTH - 80, txt, 21, couleurs, sw=3)
        y += b["height"] + 24
    s = fermer(sec, y - 24)

    sec = section(d, s, "3. Ce qui est déjà dans nos sources, ou hors de ces vues")
    bas = tableau(d, s + 80, [
        ("Tableaux", 360, {"size": 20}),
        ("Où sont les données", 1000, {"size": 19}),
        ("Ce qu'il reste à faire", 900, {"size": 19}),
    ], [
        ("T25 à T27, T32 à T57", "La base des prestations (dossiers, débours, bénéficiaires)",
         "Pas de vue à demander : requêtes proposées et fiches faites"),
        ("T58 à T62, T65", "La base de la GRH",
         "Pas de vue à demander : requêtes proposées et fiches faites"),
        ("T31 (indus)", "Les trop-perçus de la base (table AJUSTEMENT) : 0,39 % d'indus en 2024. "
         "Le recueil cite les fichiers Excel de la DAI",
         "Comparer avec le fichier de la DAI (question dans « 02 »)"),
        ("T71 (remise en banque des chèques)", "L'application de recouvrement : la date de "
         "remise semble dans TXDEPOT. Le recueil précise que l'heure n'est pas enregistrée",
         "À vérifier par la BI dans ses sources ; une vue seulement si la date manque"),
        ("T33 (invalidité)", "Le recueil demande un module pour convertir une pension "
         "d'invalidité en retraite à l'âge légal",
         "La BI peut appliquer cette règle avec l'âge de l'assuré, sans vue"),
        ("T62 bis (formation des personnes externes)", "Recueil, RH ligne 5 : fichiers Excel "
         "du bilan social", "Une petite vue de plus, si la CNSS confirme ce tableau"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "4. D'où vient cette liste")
    bas = rangee(d, s + 80, [
        ("LES SOURCES\n· recueil des besoins v7 : Prestation lignes 3 et 19 ; Finance lignes 1 "
         "à 12 ; RH lignes 5 à 7\n· Annuaire CIPRES : maquettes T28 à T30, T63 à T76\n"
         "· atelier de Kaya (2017)\n· nos lots sur la base des prestations (contrôles : "
         "lot 1, P13)", BLANC, 2),
        ("CE QUI N'EST PAS LU\nLes données comptables de la copie de la base (schéma STG_GL) : "
         "la CNSS les dit non valides. Toute la comptabilité passe donc par la vue 1, "
         "alimentée depuis Oracle GL.", JAUNE, 2),
    ], size=20)
    fermer(sec, bas)
    return d


# ================================================================ 01 balance
def balance():
    d = Diagram(seed=71)
    s = entete(d, f"{BALANCE_NOM} : la balance comptable, mois par mois",
               "Vue à créer par la CNSS dans la base source · alimentée depuis Oracle GL par la "
               "DSI, avec la DFC · sert à la GRH (T63, T64), à la gestion financière (T66 à T75, "
               "SP1, SP2) et à l'action sanitaire et sociale (T76) · proposition du 07/11/2026")

    sec = section(d, s, "1. Pourquoi une seule vue")
    bas = rangee(d, s + 80, [
        ("UNE VUE POUR TOUS LES TABLEAUX FINANCIERS\nTous se calculent avec des montants par "
         "compte : la balance suffit. La BI regroupe ensuite les comptes de chaque tableau. "
         "Si un compte change, aucune nouvelle extraction n'est nécessaire.", BLEU, 2),
        ("CE QUE DIT LE RECUEIL v7\n« Source : Oracle GL » (T63, T64, T69, T73, SP1)\n"
         "« Voir la DSI pour les formules » (T66, T74)\nT72 : comptes de placement listés\n"
         "T75 : « en attente du retour de la DSI pour la structure »", JAUNE, 2),
        ("POURQUOI CES COLONNES\n· le mois : SP1 est mensuel\n· débit et crédit : T72 a des "
         "entrées et des sorties\n· le mois 0 : T72 part du 1er janvier\n· la branche : T66 à "
         "T68 sont par branche\n· le budget : T64, T69, T70, SP1, SP2", BLEU, 2),
    ], size=20)
    s = fermer(sec, bas)

    sec = section(d, s, "2. La structure demandée")
    b = bloc_nom(d, s + 80, BALANCE_NOM, BALANCE_LIGNE, "Oracle GL")
    bas = tableau_colonnes(d, b["y"] + b["height"] + 30, BALANCE_COLONNES)
    s = fermer(sec, bas)

    sec = section(d, s, "3. À quoi ça ressemble (comptes et montants fictifs)")
    bas = exemple(d, s + 80, BALANCE_EXEMPLE_NOMS, BALANCE_EXEMPLE_LARGEURS, [
        ("2024", "0", "G", "COMMUN", "5600100", "Dépôts à terme", "-", "REALISE",
         "2 000 000 000", "0", "O", "15/01/2025"),
        ("2024", "3", "G", "COMMUN", "651100", "Salaires et appointements", "DG01", "REALISE",
         "104 000 000", "0", "O", "15/04/2024"),
        ("2024", "3", "G", "PF", "701100", "Cotisations PF", "-", "REALISE", "0",
         "950 000 000", "O", "15/04/2024"),
        ("2024", "12", "S", "COMMUN", "615000", "Entretien et réparations", "DPASS", "BUDGET",
         "25 000 000", "0", "O", "15/01/2025"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "4. Les règles d'alimentation")
    bas = rangee(d, s + 80, [(t, BLANC, 2) for t in BALANCE_REGLES], size=20)
    s = fermer(sec, bas)

    sec = section(d, s, "5. Ce que la BI calcule avec cette vue (comptes du recueil v7)")
    bas = tableau(d, s + 80, [
        ("Tableau", 260, {"size": 20}),
        ("Ce que la BI calcule", 900, {"size": 19}),
        ("Comptes et gestions", 1120, {"size": 19}),
    ], [
        ("T63", "Frais de personnel / charges de fonctionnement",
         "Frais de personnel : comptes 65 (G et S). Charges de fonctionnement : 61 à 66 (G)"),
        ("T64", "Formation / masse salariale ; formation / total budget",
         "Formation : « 63, 86 et 87 » (G et S). Masse salariale : 651 et 652. Budget : les "
         "lignes BUDGET"),
        ("T66 à T68", "Par branche (PF, AT/MP, PVID) : charges de fonctionnement / produits "
         "techniques, / charges techniques ; produits financiers / charges de fonctionnement",
         "Charges de fonctionnement : 61 à 66 (G). Produits techniques : 701, 702, 708. "
         "Charges techniques : 601, 602, 603, 605. Produits financiers : 771 à 773, 775 à 778"),
        ("T69, T70, SP2", "Réalisé / prévu, pour l'investissement (classe 2) et le "
         "fonctionnement (classe 6)",
         "Gestion G (T69), S (T70), I, immeubles de rapport (SP2)"),
        ("SP1", "Taux de réalisation du budget, compte par compte, chaque mois",
         "Tous les comptes budgétés"),
        ("T72", "Placements au 1er janvier, entrées, sorties, placements au 31/12, par type",
         "Comptes de placement du recueil : 51, 52, 5600100 à 5601000, 2501, 2502, 25601, "
         "258, 260, 27"),
        ("T73", "Produits et charges de la gestion des placements, plus les revenus des "
         "immeubles de rapport", "Produits financiers (77) et charges des placements ; "
         "gestion I pour les immeubles"),
        ("T74", "Rapport de charges brut : charges techniques / produits techniques ; net : "
         "charges techniques / (produits techniques − charges de fonctionnement)",
         "Les mêmes comptes que T66 à T68"),
        ("T75", "Charges techniques de l'année, face aux réserves de V_BI_RESERVE",
         "Charges techniques, par branche"),
        ("T76", "Action sanitaire et sociale : charges de fonctionnement / dépenses ASS, / "
         "dépenses de l'organisme ; recettes / dépenses ASS",
         "Gestion S ; dépenses de l'organisme : toutes les gestions"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "6. Les questions à la DFC")
    bas = rangee(d, s + 80, [
        ("SUR LA VUE\n1. Les écritures portent-elles la branche (PF, AT/MP, PVID) ? Sinon, "
         "quelle clé pour répartir les charges de fonctionnement entre les branches (T66 à "
         "T68, T74) ?\n2. Le réalisé du budget : l'écriture comptable ou l'engagement ? Pour "
         "l'investissement, la réception provisoire (Kaya 2017) ?\n3. Les charges de "
         "personnel ont-elles une section analytique, pour décliner T63 et T64 par direction, "
         "agence et bureau ?", JAUNE, 2),
        ("SUR LES COMPTES\n4. Placements (T72, T73) : quel type pour chaque compte de "
         "placement (immeubles de rapport, obligations, actions, DAT, autres titres, "
         "participations) ? quels comptes pour chaque ligne de T73 ?\n5. Formation (T64) : "
         "que veut dire « 63, 86 et 87 » ? 6386 et 6387 ? Masse salariale : 651 et 652, ou "
         "651 seul ?\n6. Total budget (T64) : quelles gestions, avec ou sans "
         "l'investissement ?", JAUNE, 2),
    ], size=20)
    fermer(sec, bas)
    return d


# ================================================================ 02 contrôles
def controles():
    d = Diagram(seed=72)
    s = entete(d, "V_BI_CONTROLE_POSTERIORI : les contrôles a posteriori",
               "Vue à créer par la CNSS dans la base source · alimentée par la DSI, avec la DAI "
               "· sert à T28 (PVID), T29 (AT/MP) et T30 (PF) · proposition du 07/11/2026")

    sec = section(d, s, "1. Pourquoi cette vue")
    bas = rangee(d, s + 80, [
        ("T28, T29, T30\nPar type de prestation : les dossiers payés et les dossiers contrôlés "
         "a posteriori, trimestre par trimestre. Norme : au moins 5 % des dossiers payés "
         "sont contrôlés.", BLEU, 2),
        ("CE QU'ON TROUVE DANS LA BASE\nLa table DOSSIERS_CONTROLES : 115 contrôles en 2025 "
         "pour 155 206 dossiers payés (0,07 %). Plus aucune saisie depuis juillet 2025, et "
         "des années fausses (202, 203, 2055).", JAUNE, 2),
        ("CE QUE DISENT LES SOURCES\nRecueil v7 (T28 à T30) : « Source : Fichiers Excel "
         "(DAI) ». Kaya 2017 : il manquait un module des contrôles ; détailler par direction "
         "régionale et par agence.", JAUNE, 2),
    ], size=20)
    s = fermer(sec, bas)

    sec = section(d, s, "2. La structure demandée")
    b = bloc_nom(d, s + 80, "V_BI_CONTROLE_POSTERIORI",
                 "un contrôle a posteriori d'un dossier de prestation déjà liquidé",
                 "table DOSSIERS_CONTROLES ou fichier de la DAI")
    bas = tableau_colonnes(d, b["y"] + b["height"] + 30, [
        ("ID_CONTROLE", "NUMBER(10)", "oui", "Identifiant unique du contrôle"),
        ("DOS_CODE", "VARCHAR2(11)", "oui",
         "Numéro du dossier contrôlé, tel qu'il est dans l'application des prestations"),
        ("TDOS_CODE", "VARCHAR2(1)", "oui",
         "Type de dossier, comme dans l'application : V vieillesse (PVID), A AT/MP, "
         "F prestations familiales, M maternité"),
        ("CODE_PRESTATION", "VARCHAR2(10)", "oui",
         "Prestation contrôlée, avec le code de l'application des prestations (par exemple "
         "RPV, ARI, FAF)"),
        ("DATE_CONTROLE", "DATE", "oui", "Date du contrôle"),
        ("RESULTAT", "VARCHAR2(15)", "oui", "CONFORME, ANOMALIE ou EN_COURS"),
        ("MONTANT_INDU", "NUMBER(18,2)", "non",
         "Indu constaté, en francs CFA : positif pour un trop-perçu, négatif pour un "
         "moins-perçu ; vide s'il n'y en a pas"),
        ("DATE_MISE_A_JOUR", "DATE", "oui", "Date à laquelle la ligne a été alimentée"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "3. À quoi ça ressemble (dossiers et montants fictifs)")
    bas = exemple(d, s + 80,
                  ["ID_CONTROLE", "DOS_CODE", "TDOS_CODE", "CODE_PRESTATION", "DATE_CONTROLE",
                   "RESULTAT", "MONTANT_INDU", "DATE_MISE_A_JOUR"],
                  [200, 280, 180, 240, 240, 240, 240, 260], [
                      ("1501", "V0000001234", "V", "RPV", "12/02/2026", "CONFORME", "",
                       "15/02/2026"),
                      ("1502", "F0000005678", "F", "FAF", "20/02/2026", "ANOMALIE", "45 000",
                       "21/02/2026"),
                      ("1503", "A0000000912", "A", "ARI", "03/03/2026", "EN_COURS", "",
                       "04/03/2026"),
                  ])
    s = fermer(sec, bas)

    sec = section(d, s, "4. Les règles d'alimentation")
    bas = rangee(d, s + 80, [
        ("CE QU'ON MET DEDANS\n· chaque contrôle a posteriori d'un dossier déjà liquidé ; un "
         "dossier contrôlé deux fois donne deux lignes\n· toutes les branches\n· les 5 "
         "derniers exercices et l'exercice en cours", BLANC, 2),
        ("D'OÙ VIENNENT LES LIGNES\n· la table DOSSIERS_CONTROLES, si la DAI y saisit de "
         "nouveau ses contrôles\n· ou une table chargée depuis le fichier Excel de la DAI\n"
         "Au choix de la CNSS : la vue reste la même.", BLANC, 2),
        ("CE QU'ON N'Y MET PAS\n· ni nom d'assuré, ni numéro bancaire : le numéro de dossier "
         "suffit\n\nMISE À JOUR\n· au moins à chaque fin de trimestre (T28 à T30 sont "
         "trimestriels)", BLANC, 2),
    ], size=20)
    s = fermer(sec, bas)

    sec = section(d, s, "5. Ce que la BI en fera, et les questions à la DAI")
    bas = rangee(d, s + 80, [
        ("LA BI\n· dossiers payés : déjà dans la base des prestations\n· dossiers contrôlés : "
         "cette vue, par trimestre de la date du contrôle\n· taux de contrôle = contrôlés / "
         "payés, par type de prestation\n· par direction régionale et agence, grâce au "
         "dossier (Kaya 2017)\n· T31 : l'indu constaté se compare aux trop-perçus de la "
         "base", VERT, 2),
        ("LES QUESTIONS À LA DAI\n1. Où sont enregistrés les contrôles depuis juillet "
         "2025 ?\n2. Un contrôle porte-t-il sur tout le dossier, ou sur une prestation du "
         "dossier ?\n3. Quels résultats possibles pour un contrôle ?\n4. T31 : votre "
         "fichier des indus diffère-t-il des trop-perçus de la base (table AJUSTEMENT) ?",
         JAUNE, 2),
    ], size=20)
    fermer(sec, bas)
    return d


# ================================================================ 03 réserves
def reserves():
    d = Diagram(seed=73)
    s = entete(d, "V_BI_RESERVE : les réserves par branche",
               "Vue à créer par la CNSS dans la base source · alimentée par la DFC · sert à T75 "
               "· proposition du 07/11/2026")

    sec = section(d, s, "1. Pourquoi cette vue")
    bas = rangee(d, s + 80, [
        ("T75 : LES RÉSERVES EN ANNÉES DE PRESTATIONS\nFonds de roulement, réserve de "
         "sécurité, réserve technique, face aux charges techniques de l'année. Un tableau par "
         "branche (Annuaire CIPRES).", BLEU, 2),
        ("CE QUE DIT LE RECUEIL v7 (Finance, ligne 12)\n« Non encore utilisé à la CNSS ». "
         "Pour chaque branche : PF, AT/MP, AV (PVID). Oracle GL, après traitement sous Excel ; "
         "disponible vers juillet N+1. « En attente du retour de la DSI pour la "
         "structure ».", JAUNE, 2),
        ("POURQUOI UNE VUE À PART\nLes réserves ne sont pas un simple solde de compte : la "
         "DFC les calcule. Il suffit qu'elle donne le montant de chaque réserve au 31/12, "
         "par branche.", BLEU, 2),
    ], size=20)
    s = fermer(sec, bas)

    sec = section(d, s, "2. La structure demandée")
    b = bloc_nom(d, s + 80, "V_BI_RESERVE",
                 "un exercice × une branche × un type de réserve",
                 "saisie de la DFC ou fichier Excel chargé")
    bas = tableau_colonnes(d, b["y"] + b["height"] + 30, [
        ("EXERCICE", "NUMBER(4)", "oui", "Année (réserves au 31/12)"),
        ("BRANCHE", "VARCHAR2(10)", "oui", "PF, ATMP ou PVID"),
        ("TYPE_RESERVE", "VARCHAR2(20)", "oui",
         "FONDS_ROULEMENT (réserve de trésorerie), SECURITE ou TECHNIQUE"),
        ("MONTANT", "NUMBER(18,2)", "oui", "Montant de la réserve au 31/12, en francs CFA"),
        ("DATE_MISE_A_JOUR", "DATE", "oui", "Date à laquelle la ligne a été saisie ou chargée"),
    ])
    s = fermer(sec, bas)

    sec = section(d, s, "3. À quoi ça ressemble (montants fictifs)")
    bas = exemple(d, s + 80, ["EXERCICE", "BRANCHE", "TYPE_RESERVE", "MONTANT",
                              "DATE_MISE_A_JOUR"],
                  [200, 200, 320, 320, 280], [
                      ("2024", "PF", "FONDS_ROULEMENT", "5 000 000 000", "15/07/2025"),
                      ("2024", "PF", "SECURITE", "3 200 000 000", "15/07/2025"),
                      ("2024", "PVID", "TECHNIQUE", "48 000 000 000", "15/07/2025"),
                  ])
    s = fermer(sec, bas)

    sec = section(d, s, "4. Les règles, ce que la BI en fera, et les questions à la DFC")
    bas = rangee(d, s + 80, [
        ("LES RÈGLES\n· une ligne par branche et par type de réserve, chaque année\n· le "
         "montant au 31/12, en francs CFA\n· les 5 derniers exercices\n· une mise à jour par "
         "an, après l'arrêté des comptes (vers juillet N+1)\n· ni nom, ni numéro "
         "bancaire", BLANC, 2),
        ("LA BI\n· charges techniques de l'année : vue V_BI_BALANCE_COMPTABLE, par "
         "branche\n· réserves en années de prestations = réserve / charges techniques\n· un "
         "tableau par branche, comme le demande l'Annuaire", VERT, 2),
        ("LES QUESTIONS À LA DFC\n1. Les réserves sont-elles déjà calculées chaque année ?\n"
         "2. Le recueil écrit « prestations N−1 / réserve », l'Annuaire « réserve / "
         "prestations » : lequel ?\n3. Les charges techniques en face : celles de l'année, "
         "ou de l'année d'avant ?", JAUNE, 2),
    ], size=20)
    fermer(sec, bas)
    return d


SCHEMAS = [
    ("00 sommaire", sommaire),
    ("01 balance comptable", balance),
    ("02 controles a posteriori", controles),
    ("03 reserves", reserves),
]


def main():
    parser = argparse.ArgumentParser(description="Génère les schémas des vues à demander à la CNSS.")
    parser.add_argument("--force", action="store_true", help="écraser les fichiers existants")
    args = parser.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for nom, build in SCHEMAS:
        d = build()
        path = OUT_DIR / f"{nom}.excalidraw"
        try:
            d.save(path, force=args.force)
            print(f"inchangé : {path}" if d.inchange
                  else f"{len(d.elements)} éléments écrits dans {path}")
        except FileExistsError as e:
            print(e)
        for warning in d.warnings:
            print("ATTENTION", nom, ":", warning)


if __name__ == "__main__":
    main()
