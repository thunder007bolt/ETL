"""
Le rangement des schémas du 17/11/2026, validé par l'utilisateur (« Oui, range tout ») :
6 dossiers à la racine (journal, commun, prestations, grh, maitrise, archives) et, dans chaque
domaine, « 1 pour la cnss », « 2 regles et sources », « 3 comprendre ».

Chaque déplacement suit le chemin de la visionneuse (server.py, _move) : le fichier ou le dossier
est renommé, les versions de l'historique suivent, deplacements.json note l'ancienne et la
nouvelle place, et les espaces de travail (espaces.json) sont mis à jour. Aucun schéma n'est
régénéré ni modifié. Les dossiers laissés vides sont retirés.

Usage (depuis ETL) :
    python3 excalidraw/sources/rangement/ranger.py            # à blanc : liste ce qui serait fait
    python3 excalidraw/sources/rangement/ranger.py --faire    # déplace
Le script se relance sans risque : un déplacement déjà fait est sauté.
"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "lib"))

import server  # noqa: E402
from historique import move_history, record_move  # noqa: E402

SCHEMAS = server.SCHEMAS_DIR

P1, P2, P3 = "1 pour la cnss", "2 regles et sources", "3 comprendre"
FAMILLES = [("T25 T27 dossiers", "regles_t25_t27_dossiers"),
            ("T28 T31 controles indus", "regles_t28_t31_controles_indus"),
            ("T32 T41 beneficiaires", "regles_t32_t41_beneficiaires"),
            ("T42 T49 nouveaux", "regles_t42_t49_nouveaux"),
            ("T50 T57 evolution", "regles_t50_t57_evolution")]

# (ancienne place, nouvelle place) : un schéma (.excalidraw) ou un dossier entier
DEPLACEMENTS = [
    # --- archives : rien n'est supprimé
    ("Immatriculation.excalidraw", "archives/essais/Immatriculation.excalidraw"),
    ("Tests/test/ddd.excalidraw", "archives/essais/ddd.excalidraw"),
    ("grh/effectif.excalidraw", "archives/essais/grh_effectif.excalidraw"),
    ("grh/points_a_valider.excalidraw", "archives/grh/points_a_valider.excalidraw"),
    ("grh/regles_grh.excalidraw", "archives/grh/regles_grh.excalidraw"),
    ("prestations/regles_prestations.excalidraw", "archives/prestations/regles_prestations.excalidraw"),
    # --- commun : ce qui vaut pour plusieurs domaines
    ("regles cipres/fiche_regles.excalidraw", "commun/fiche_regles.excalidraw"),
    ("besoins cnss/remarques_besoins_v7.excalidraw", "commun/remarques_besoins_v7.excalidraw"),
    ("lexique/définitions.excalidraw", "commun/lexique.excalidraw"),
    ("vues cnss", "commun/vues cnss"),
    # --- prestations
    ("prestations/regles par tableau", f"prestations/{P1}/regles par tableau"),
    ("prestations/tableaux cipres", f"prestations/{P1}/fiches de validation"),
    ("prestations/points_a_valider.excalidraw", f"prestations/{P1}/points_a_valider.excalidraw"),
    ("prestations/points_a_valider_v3.excalidraw", f"prestations/{P1}/points_a_valider_v3.excalidraw"),
    *[(f"prestations/{dossier}/{nom}.excalidraw", f"prestations/{P2}/{nom}.excalidraw")
      for dossier, nom in FAMILLES],
    ("prestations/besoins_v7_prestations.excalidraw", f"prestations/{P2}/besoins_v7_prestations.excalidraw"),
    ("annuaire cnss/prestations_2024.excalidraw",
     f"prestations/{P2}/annuaire_cnss_2024_prestations.excalidraw"),
    ("annuaire cnss/pensions_2024.excalidraw", f"prestations/{P2}/annuaire_cnss_2024_pensions.excalidraw"),
    ("prestations/domaine_prestations.excalidraw", f"prestations/{P3}/domaine_prestations.excalidraw"),
    ("prestations/tables_prestations.excalidraw", f"prestations/{P3}/tables_prestations.excalidraw"),
    ("prestations/prestation_esp.excalidraw", f"prestations/{P3}/prestation_esp.excalidraw"),
    ("prestations/fiche_de_lecture.excalidraw", f"prestations/{P3}/fiche_de_lecture.excalidraw"),
    ("prestations/vie_dossier", f"prestations/{P3}/vie_dossier"),
    # --- grh
    ("grh/regles par tableau", f"grh/{P1}/regles par tableau"),
    ("grh/tableaux cipres", f"grh/{P1}/fiches de validation"),
    ("grh/points_a_valider_v2.excalidraw", f"grh/{P1}/points_a_valider_v2.excalidraw"),
    ("annuaire cnss/gestion_administrative_2024.excalidraw",
     f"grh/{P2}/annuaire_cnss_2024_gestion_administrative.excalidraw"),
    ("reunions/reunion_grh_2026-11-03.excalidraw", f"grh/{P2}/reunion_grh_2026-11-03.excalidraw"),
    ("grh/fiche_de_lecture.excalidraw", f"grh/{P3}/fiche_de_lecture.excalidraw"),
    ("grh/T58 effectif", f"grh/{P3}/T58 effectif"),
    ("grh/T60 mouvements", f"grh/{P3}/T60 mouvements"),
    ("grh/T63 T64 finances/t63_t64_donnees_a_fournir.excalidraw",
     f"grh/{P3}/t63_t64_donnees_a_fournir.excalidraw"),
    # --- la proposition, une fois appliquée
    ("rangement/proposition de rangement.excalidraw",
     "archives/rangement/proposition de rangement.excalidraw"),
]

# dossiers dont les schémas partent un par un : un espace de travail qui liste le dossier doit
# d'abord lister ses schémas, pour les suivre
DISSOUS = ["regles cipres", "besoins cnss", "lexique", "annuaire cnss", "reunions", "Tests",
           "Tests/test", "grh/T63 T64 finances", "rangement",
           *[f"prestations/{dossier}" for dossier, _ in FAMILLES]]


def schemas_de(dossier):
    return sorted(p for p in dossier.rglob("*.excalidraw") if not server._hidden(p))


def detailler_espaces(faire):
    """Remplace, dans chaque espace, un dossier dissous par la liste de ses schémas."""
    espaces, change = server.read_spaces(), False
    for espace in espaces:
        chemins = []
        for chemin in espace["chemins"]:
            if chemin in DISSOUS and (SCHEMAS / chemin).is_dir():
                change = True
                chemins += [server.relative(p) for p in schemas_de(SCHEMAS / chemin)]
            else:
                chemins.append(chemin)
        espace["chemins"] = list(dict.fromkeys(chemins))
    if change and faire:
        server.write_spaces(espaces)
    return change


def deplacer(ancien, nouveau, faire):
    src, dst = SCHEMAS / ancien, SCHEMAS / nouveau
    if not src.exists():
        return "déjà fait" if dst.exists() else "INTROUVABLE"
    if dst.exists():
        return "CONFLIT : la nouvelle place est prise"
    if not faire:
        return "à faire"
    dst.parent.mkdir(parents=True, exist_ok=True)
    fichiers = [src] if src.is_file() else schemas_de(src)
    suivis = [(p, dst / p.relative_to(src) if src.is_dir() else dst) for p in fichiers]
    os.rename(src, dst)
    for avant, apres in suivis:
        move_history(avant, apres)
        record_move(avant, apres)
    server.follow_in_spaces(ancien, nouveau)
    return f"fait ({len(suivis)} schéma{'s' if len(suivis) > 1 else ''})"


def retirer_dossiers_vides(faire):
    """Retire les dossiers de schemas/ laissés vides (un .DS_Store ne compte pas)."""
    retires = []
    for dossier in sorted((p for p in SCHEMAS.rglob("*") if p.is_dir()), reverse=True):
        if server._hidden(dossier) or any(c.name != ".DS_Store" for c in dossier.iterdir()):
            continue
        retires.append(server.relative(dossier))
        if faire:
            (dossier / ".DS_Store").unlink(missing_ok=True)
            dossier.rmdir()
            server.follow_in_spaces(retires[-1], None)
    return retires


def nettoyer_espaces(faire):
    """Dans un espace, une entrée déjà contenue dans un de ses dossiers est en double : on la retire."""
    espaces, change = server.read_spaces(), False
    for espace in espaces:
        gardes = [c for c in espace["chemins"]
                  if not any(c != d and c.startswith(f"{d}/") for d in espace["chemins"])]
        change = change or gardes != espace["chemins"]
        espace["chemins"] = gardes
    if change and faire:
        server.write_spaces(espaces)
    return espaces


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Range les schémas (17/11/2026).")
    parser.add_argument("--faire", action="store_true", help="déplacer vraiment (sinon : à blanc)")
    args = parser.parse_args()

    detailler_espaces(args.faire)
    problemes = 0
    for ancien, nouveau in DEPLACEMENTS:
        etat = deplacer(ancien, nouveau, args.faire)
        problemes += etat.isupper() or etat.startswith(("INTROUVABLE", "CONFLIT"))
        print(f"{etat:28} {ancien}  ->  {nouveau}")
    if args.faire:
        print("dossiers vides retirés :", ", ".join(retirer_dossiers_vides(True)) or "aucun")
    for espace in nettoyer_espaces(args.faire):
        print(f"espace « {espace['nom']} » :", espace["chemins"])
    sys.exit(1 if problemes else 0)
