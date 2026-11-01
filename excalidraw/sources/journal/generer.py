"""
Génère le journal de bord dans schemas/journal, à partir des données de journal.py :

    00 tableau de bord     où on en est, qui doit agir, la suite, les derniers jours
    01 frise               tous les jours, semaine par semaine
    02 decisions           les décisions, dans l'ordre, avec ce qu'elles ont remplacé
    03 ou trouver quoi     les analyses, requêtes, schémas, documents et références
    jours/AAAA-MM-JJ       une fiche par jour : ses chantiers et ce qui restait le soir

Les vues utilisent le même rendu que les fiches de maîtrise (sources/maitrise/fiches.py) : des
blocs séparés, rangés en parties.

Usage :
    python3 excalidraw/sources/journal/generer.py [--force] [--seulement <texte>]
"""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ICI = Path(__file__).resolve().parent
sys.path.insert(0, str(ICI))
sys.path.insert(0, str(ICI.parent / "maitrise"))

import journal as J  # noqa: E402
from fiches import ROOT, Bloc, Fiche, Partie, generate  # noqa: E402

DOSSIER = ROOT / "schemas" / "journal"
JOURS_SEMAINE = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
MOIS = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
        "octobre", "novembre", "décembre"]


def jj_mm(iso):
    d = date.fromisoformat(iso)
    return f"{d.day:02d}/{d.month:02d}"


def en_toutes_lettres(iso):
    d = date.fromisoformat(iso)
    return f"{JOURS_SEMAINE[d.weekday()]} {d.day} {MOIS[d.month - 1]} {d.year}"


def semaine(iso):
    """Le lundi de la semaine de cette date."""
    d = date.fromisoformat(iso)
    return d - timedelta(days=d.weekday())


def titre_semaine(lundi):
    dimanche = lundi + timedelta(days=6)
    if lundi.month == dimanche.month:
        return f"Semaine du {lundi.day} au {dimanche.day} {MOIS[dimanche.month - 1]} {dimanche.year}"
    return (f"Semaine du {lundi.day} {MOIS[lundi.month - 1]} au {dimanche.day} "
            f"{MOIS[dimanche.month - 1]} {dimanche.year}")


def par_semaine(items, date_de):
    groupes = {}
    for item in items:
        groupes.setdefault(semaine(date_de(item)), []).append(item)
    return sorted(groupes.items())


def puces(lignes):
    return "\n".join(f"• {ligne}" for ligne in lignes)


def bloc_jour(jour):
    d = date.fromisoformat(jour.date)
    origine = " · reconstitué" if jour.source == "reconstitué" else ""
    return Bloc(jour.titre, [("", puces(jour.marquants))], ref=jj_mm(jour.date), genre="notion",
                pied=f"{JOURS_SEMAINE[d.weekday()]} · fiche jours/{jour.date}{origine}")


# ---------------------------------------------------------------------------------- les vues

def tableau_de_bord():
    maj = en_toutes_lettres(J.MISE_A_JOUR)
    return Fiche(
        fichier="00 tableau de bord.excalidraw",
        titre="Journal de bord : où on en est",
        sous_titre=f"La mémoire du travail sur le projet BI de la CNSS · mis à jour le {maj}",
        objectif="où en est chaque domaine, qui doit agir et sur quoi, la suite dans l'ordre, et "
                 "les derniers jours. Pour revenir en arrière : la frise (01), les décisions (02), "
                 "où trouver quoi (03) et la fiche de chaque jour (dossier jours).",
        parties=[
            Partie("Où on en est", colonnes=3, blocs=[
                Bloc(domaine, [("Fait", fait), ("Ensuite", ensuite)], genre="etape", etat=etat)
                for domaine, etat, fait, ensuite in J.ETAT
            ]),
            Partie("Ce qui attend quelqu'un", colonnes=3, blocs=[
                Bloc(qui, [("", quoi)], genre="question") for qui, quoi in J.EN_ATTENTE
            ]),
            Partie("La suite, dans l'ordre", colonnes=3, flux=True, blocs=[
                Bloc(titre, [("", quoi)], ref=str(i), genre="suite")
                for i, (titre, quoi) in enumerate(J.SUITE, 1)
            ]),
            Partie("Les derniers jours", colonnes=4, flux=True,
                   blocs=[bloc_jour(j) for j in sorted(J.JOURS, key=lambda j: j.date)[-4:]]),
            Partie("Se servir du journal", colonnes=3, blocs=[
                Bloc("Pour toi", [("", "Ouvre ce tableau de bord pour savoir où on en est. Pour "
                                       "retrouver ce qui a été fait un jour donné, ouvre la frise "
                                       "puis la fiche du jour ; pour savoir pourquoi on fait "
                                       "ainsi, les décisions.")]),
                Bloc("Pour Claude", [("", "En début de séance, lire journal.py : l'état, ce qui "
                                          "attend, la suite. À la fin de chaque étape, compléter "
                                          "le jour, les décisions et l'état, puis régénérer.")]),
                Bloc("Ajouter une journée", [("", "Ajouter un Jour à JOURS dans "
                                                  "sources/journal/journal.py, puis lancer "
                                                  "generer.py --force. Les vues se refont "
                                                  "seules.")]),
            ]),
        ],
    )


def frise():
    parties = [Partie("Avant la mission", colonnes=1, blocs=[
        Bloc("Mai à juillet 2026", [("", J.AVANT)], ref="avant", genre="notion"),
    ])]
    for lundi, jours in par_semaine(J.JOURS, lambda j: j.date):
        parties.append(Partie(titre_semaine(lundi), colonnes=4, flux=True,
                              blocs=[bloc_jour(j) for j in sorted(jours, key=lambda j: j.date)]))
    return Fiche(
        fichier="01 frise.excalidraw",
        titre="La frise : tous les jours de la mission",
        sous_titre="Journal de bord · semaine par semaine, de gauche à droite · chaque jour a sa "
                   "fiche dans le dossier jours",
        objectif="ce qui s'est passé chaque jour depuis le début de la mission, en trois ou "
                 "quatre faits, pour retrouver vite le jour qui t'intéresse.",
        parties=parties,
    )


def decisions():
    parties = []
    for lundi, items in par_semaine(J.DECISIONS, lambda x: x.date):
        blocs = []
        for dec in sorted(items, key=lambda x: x.date):
            champs = [("La décision", dec.decision), ("Pourquoi", dec.pourquoi),
                      ("Qui", dec.qui), ("Ce que ça change", dec.effet)]
            if dec.remplacee_par:
                champs.append(("Remplacée par", dec.remplacee_par))
            blocs.append(Bloc(dec.titre, champs, ref=jj_mm(dec.date), genre="regle",
                              etat=dec.statut))
        parties.append(Partie(titre_semaine(lundi), colonnes=3, blocs=blocs))
    return Fiche(
        fichier="02 decisions.excalidraw",
        titre="Les décisions, dans l'ordre",
        sous_titre="Journal de bord · ce qui a été décidé, pourquoi, par qui, et ce que ça a "
                   "changé",
        objectif="pourquoi le travail se fait comme il se fait : chaque décision avec sa date, sa "
                 "raison et son effet, et celles qui ont été remplacées.",
        parties=parties,
    )


def ou_trouver():
    return Fiche(
        fichier="03 ou trouver quoi.excalidraw",
        titre="Où trouver quoi",
        sous_titre="Journal de bord · les analyses, les requêtes, les schémas, les documents et "
                   "les références",
        objectif="où est rangé chaque livrable du projet, et ce qu'on y trouve.",
        parties=[Partie(rubrique, colonnes=3, blocs=[
            Bloc(nom, [("Où", ou), ("Ce qu'on y trouve", quoi)], genre="donnee")
            for nom, ou, quoi in items
        ]) for rubrique, items in J.LIVRABLES],
    )


def fiche_jour(jour, veille, lendemain):
    origine = ("reconstituée à partir des notes, des lots de requêtes et de git"
               if jour.source == "reconstitué" else "écrite pendant la séance")
    parties = []
    if jour.chantiers:
        blocs = []
        for c in jour.chantiers:
            champs = [(label, texte) for label, texte in [
                ("Ce que tu as demandé", c.demande), ("Ce qui a été fait", c.fait),
                ("Ce qu'on a trouvé", c.trouve), ("Ce qui a été décidé", c.decide),
                ("Les fichiers", c.fichiers)] if texte]
            blocs.append(Bloc(c.titre, champs, ref=c.domaine, genre="etape", etat=c.etat))
        parties.append(Partie("Les chantiers du jour", colonnes=3, blocs=blocs))
    if jour.reste:
        parties.append(Partie("Le soir, il restait", colonnes=3, blocs=[
            Bloc(f"À faire {i}", [("", r)], genre="suite") for i, r in enumerate(jour.reste, 1)
        ]))
    nav = []
    if veille:
        nav.append(Bloc(f"La veille · {jj_mm(veille.date)}", [("", veille.titre)],
                        genre="notion", pied=f"jours/{veille.date}"))
    if lendemain:
        nav.append(Bloc(f"Le jour suivant · {jj_mm(lendemain.date)}", [("", lendemain.titre)],
                        genre="notion", pied=f"jours/{lendemain.date}"))
    nav.append(Bloc("Vue d'ensemble", [("", "00 tableau de bord, 01 frise, 02 décisions, 03 où "
                                            "trouver quoi")], genre="notion"))
    parties.append(Partie("Pour naviguer", colonnes=3, blocs=nav))
    return Fiche(
        fichier=f"jours/{jour.date}.excalidraw",
        titre=f"{en_toutes_lettres(jour.date).capitalize()} · {jour.titre}",
        sous_titre=f"Journal de bord · fiche {origine}",
        objectif=puces(jour.marquants),
        parties=parties,
        titre_objectif="La journée en bref",
    )


def toutes_les_vues():
    jours = sorted(J.JOURS, key=lambda j: j.date)
    vues = [tableau_de_bord(), frise(), decisions(), ou_trouver()]
    for i, jour in enumerate(jours):
        veille = jours[i - 1] if i > 0 else None
        lendemain = jours[i + 1] if i + 1 < len(jours) else None
        vues.append(fiche_jour(jour, veille, lendemain))
    return vues


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère le journal de bord.")
    parser.add_argument("--force", action="store_true", help="écraser les vues existantes")
    parser.add_argument("--seulement", help="ne générer que les vues dont le chemin contient ce "
                                            "texte")
    args = parser.parse_args()
    generate(toutes_les_vues(), force=args.force, seulement=args.seulement, dossier=DOSSIER)
