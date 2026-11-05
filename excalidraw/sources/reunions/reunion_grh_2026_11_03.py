"""
Réunion GRH du 03/11/2026 : ce qu'on en retient (état au 04/11/2026).

Premier schéma du dossier « reunions ». Il reprend les deux enregistrements de la réunion
(cnss_files : opt.m4a, 59 min, à 13 h 47 ; Voix 261103_144325.m4a, 13 min, à 14 h 43), transcrits
sur le Mac avec whisper.cpp le 04/11. Les points sont numérotés M01 à M12 pour pouvoir y renvoyer ;
« Où » donne l'enregistrement et la minute. Rien n'est encore reporté dans les règles, les fiches
ni les requêtes.

Usage :
    python3 excalidraw/sources/reunions/reunion_grh_2026_11_03.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "sources" / "prestations"))
sys.path.insert(0, str(ROOT / "sources" / "besoins_cnss"))

from excalidraw_builder import C, Diagram, main  # noqa: E402
from remarques_besoins_v7 import cards, rows  # noqa: E402

OUTPUT = ROOT / "schemas" / "reunions" / "reunion_grh_2026-11-03.excalidraw"

EN_BREF = [
    ("Qui, quoi",
     "Toi et un interlocuteur de la CNSS qui connaît l'application GRH, devant la base. On a "
     "passé en revue T59, T60, T61, T62, T63 et T64, et regardé les unités administratives, "
     "GRH_MOUVEMENT, GRH_SITUATION et GRH_INSCRIPTION."),
    ("Les enregistrements",
     "« opt » : 59 min, de 13 h 47 (T59, T60, T61, qualité des données). « Voix » : 13 min, de "
     "14 h 43 (T62, T58, T63 et T64). Dans « Où », opt 11:16 veut dire l'enregistrement « opt », "
     "à 11 minutes 16."),
    ("Leur qualité",
     "Salle bruyante, manipulations à l'écran, passages inaudibles : les citations sont "
     "approchées. Les transcriptions restent dans le dossier de travail de la session."),
]

# (numéro, tableau, ce qui a été dit, ce que ça change chez nous, où)
DECIDE = [
    ("M01", "T62", "Toutes les formations comptent : leur statut est N partout. « On considère "
     "toutes les formations de la base. »", "Rien : c'est notre règle (terme 3 de T62).",
     "Voix 3:00"),
    ("M02", "T62", "« Avec promotion » est la finalité de la formation, pas la promotion obtenue : "
     "« ils disent juste la finalité, on ne sait pas si elle a effectivement eu la promotion ». "
     "Une formation diplômante est d'emblée « avec promotion ».", "Le terme 7 change de sens : "
     "on ne cherche plus la montée de catégorie, on regarde le type de formation (DIPLO). À "
     "confirmer avec la DRH.", "Voix 0:29 à 2:04, 8:27"),
    ("M03", "T62", "On compte à partir des inscriptions (les participants), pas de la table des "
     "formations ; l'année est celle du début de la formation.",
     "Rien : c'est notre règle (termes 1 et 2).", "Voix 5:27, 7:45"),
    ("M04", "T58, T62", "La catégorie est celle de la situation à la date demandée (l'année en "
     "paramètre), jamais la situation actuelle : une requête faite en novembre 2026 puis en "
     "janvier 2027 doit donner la même chose.", "C'est notre règle (situation au 31/12) ; à "
     "vérifier dans les requêtes DTM.", "Voix 9:00 à 10:54"),
    ("M05", "T59", "Les domaines viennent de la structure : unités administratives, services, "
     "sections. Plusieurs services font un domaine ; le service cotisation a deux sections, "
     "immatriculation et cotisation. La CNSS donne un fichier Excel de correspondance service et "
     "domaine ; on en fait une table de paramétrage (une dimension), pas du code.",
     "Notre règle prend GRH_SITUATION.CODE_DOMAINE_ACT : à revoir quand le fichier arrive.",
     "opt 2:00 à 7:00"),
    ("M06", "T60", "GRH_MOUVEMENT contient à 94 ou 95 % des mouvements internes (mutations, "
     "affectations, nominations) : on ne peut pas en tirer les entrées et les sorties.",
     "Confirme notre constat du 21/10.", "opt 38:04"),
    ("M07", "T60", "GRH_SITUATION porte les entrées et les sorties (la nature de situation : "
     "disponibilité, détachement…) et elle est plus complète que GRH_MOUVEMENT : environ 1 300 "
     "agents ont une situation sans aucun mouvement. Chaque changement qui touche la paie "
     "(mariage, enfant, promotion) crée une ligne : plusieurs par an. GRH_PERSONNE : une ligne "
     "par agent.", "C'est déjà notre source, avec les décisions.", "opt 38:00 à 52:44"),
    ("M08", "T63, T64", "Les données financières viennent du logiciel financier : la masse "
     "salariale dans les comptes 651 et 652. Le budget de formation de la GRH n'est pas tenu à "
     "jour. Nous définissons la structure des vues ; la CNSS les alimente périodiquement.",
     "Conforme à la décision des vues simples (20/10) et au lot 22.", "Voix 11:06 à 13:10"),
]

# (numéro, tableau, la question, ce qu'on a dit, ce qu'on fait en attendant)
OUVERT = [
    ("M09", "T60", "Recrutement : toute entrée, temporaire comprise, ou seulement les "
     "permanents ?", "Ton interlocuteur : « entrer, c'est recrutement », même provisoire ; les "
     "autres entrées vaudraient alors zéro. Toi : un temporaire devenu permanent serait compté "
     "deux fois. (opt 9:28 à 10:39, 16:18)", "L'entrée d'un permanent embauché dans l'année."),
    ("M10", "T60, T61", "La date d'entrée : la note de prise de service ou la décision "
     "d'engagement (dans GRH_DECISION) ?", "Il penche pour la prise de service : on peut l'avoir "
     "sans l'engagement. Engagé le 25/12/2026, prise de service le 10/01/2027 : entré en 2027. "
     "La même date doit servir à l'ancienneté. (opt 11:16 à 14:03)",
     "La date d'embauche, à défaut la prise de service."),
    ("M11", "T60", "Les autres sorties et les retours : faut-il les détailler, et un départ en "
     "formation longue est-il une sortie ?", "Retraite et décès à part ; le reste en « autres "
     "sorties » : disponibilité, départ en formation longue, fin de contrat (temporaires), "
     "démission. Le retour d'une disponibilité ou d'une formation longue serait une entrée. "
     "(opt 15:40 à 20:17)", "Le regroupement actuel ; le recueil des besoins demande aussi le "
     "détail (B29)."),
    ("M12", "T60", "La date de décès : qu'est-ce qui est le plus proche de la réalité ?",
     "Aucune date de décès n'est saisie depuis 2010 ; le communiqué nécrologique (Kaya) n'est "
     "pas dans la base. Piste : la base des prestations. Signaler le défaut de saisie à la "
     "CNSS. (opt 53:06 à 55:00)", "La décision de liquidation des droits, environ 8 mois et "
     "demi après le décès."),
]

DONNEES = [
    ("GRH_SITUATION, les conjoints",
     "Le nombre de conjoints est vide chez des agents mariés ; la colonne « conjoint salarié » "
     "semble inversée avec une autre. (opt 47:01 à 48:20)"),
    ("GRH_SITUATION, le classement",
     "Le classement donne la catégorie (catégorie, échelle, échelon) ; la qualification dit si "
     "l'agent est cadre ; le « lieu » devrait être le site. (opt 49:00 à 50:00)"),
    ("Les unités administratives",
     "Le code de l'unité (UA_CODE) relie un agent à sa structure ; plusieurs niveaux (service, "
     "section) peuvent former un domaine. (opt 2:00 à 4:00)"),
    ("Les rapports annuels",
     "Ton interlocuteur doute qu'ils soient tirés proprement de la base : à garder en tête "
     "quand on compare à l'annuaire CNSS 2024. (opt 26:07 à 27:24)"),
]

A_FAIRE = [
    ("CNSS (organisation, DRH)",
     "Envoyer le fichier de correspondance service et domaine (M05)."),
    ("DRH", "Répondre à M02 (formation diplômante = avec promotion), M09, M10, M11 et M12."),
    ("DSI et DFC",
     "Alimenter les vues financières ; préciser les comptes de formation (« 63, 86 et 87 » du "
     "recueil des besoins, B22)."),
    ("Nous",
     "Vérifier que les requêtes DTM prennent la catégorie à la date demandée (M04) ; préparer "
     "la structure des vues financières (M08)."),
    ("Toi", "Exécuter le lot 22 de la GRH (structure du grand livre) et me coller le résultat."),
]


def build():
    d = Diagram(seed=113)

    # ------------------------------------------------------------------ titre
    d.text(0, 0, "Réunion GRH du 03/11/2026 : ce qu'on en retient", 48)
    d.text(0, 66, "Deux enregistrements (cnss_files : opt.m4a et Voix 261103_144325.m4a), "
                  "transcrits sur le Mac le 04/11 · rien n'est encore reporté dans nos règles, "
                  "fiches ou requêtes", 22, C["doux"])

    y = cards(d, 130, "La réunion en bref", EN_BREF, C["blanc"], C["encre"])
    y = rows(d, y, "1. Ce qui est dit ou validé (M01 à M08)",
             ["Tableau", "Ce qui a été dit", "Ce que ça change chez nous", "Où"],
             [250, 1080, 780, 345], DECIDE, C["vert_clair"], C["vert"],
             note="Chaque point porte un numéro pour y renvoyer ; « Où » donne l'enregistrement "
                  "et la minute.")
    y = rows(d, y, "2. Ce qui reste ouvert, à poser à la CNSS (M09 à M12)",
             ["Tableau", "La question", "Ce qu'on a dit", "En attendant"],
             [250, 600, 1080, 525], OUVERT, C["orange_clair"], C["orange"],
             note="Une seule question par point. En attendant la réponse, on garde la règle de "
                  "la dernière colonne.")
    y = cards(d, y, "3. Ce qu'on a vu dans les données", DONNEES, C["gris"], C["gris_bord"],
              cols=2)
    cards(d, y, "4. Qui fait quoi", A_FAIRE, C["jaune_clair"], C["jaune_bord"])
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
