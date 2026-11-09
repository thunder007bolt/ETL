"""
Schéma « Le recueil des besoins v7 : toutes les demandes sur les prestations, et où elles en
sont » (état au 08/11/2026).

Toutes les demandes du recueil des besoins v7 qui touchent les prestations (T25 à T57, leurs
indicateurs et les demandes générales), ligne par ligne, avec le statut et la question liée
que leur donnent les cinq documents de famille :
    analyses/prestations/regles_t25_t27_dossiers.md
    analyses/prestations/regles_t28_t31_controles_indus.md
    analyses/prestations/regles_t32_t41_beneficiaires.md
    analyses/prestations/regles_t42_t49_nouveaux.md
    analyses/prestations/regles_t50_t57_evolution.md
Puis les tableaux sans aucune ligne, les questions ouvertes regroupées par direction (sans
doublon) et la façon dont la liste est vérifiée.

Le détail est dans analyses/prestations/besoins_v7_prestations.md : mêmes lignes, mêmes
statuts, mêmes questions. Rien d'existant n'est modifié (ni les documents de famille, ni le
gabarit, ni les requêtes).

Usage :
    python3 excalidraw/sources/prestations/besoins_v7_prestations.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from excalidraw_builder import C, main  # noqa: E402
from fiche_cipres import WIDTH, height_of, wrap  # noqa: E402
from gabarit_famille import INNER, STATUTS, X0, Page  # noqa: E402

OUTPUT = ROOT / "schemas" / "prestations" / "besoins_v7_prestations.excalidraw"

PC = "pris en compte"
AI = "à intégrer"
QU = "question"
HBA = "hors base"
HBI = "hors BI"
SANS = None          # aucun document de famille ne donne de statut

FAMILLES = ["T25-T27", "T28-T31", "T32-T41", "T42-T49", "T50-T57"]

# Une ligne : (réf., ce que dit le recueil, en clair, où c'est traité, statut, question liée).
# Le statut et la question liée sont ceux du document de famille, sans rien changer.
# Une même ligne du recueil traitée par plusieurs familles avec des statuts différents a une
# ligne par statut ; les renvois « Idem. Tab n » sont détaillés comme dans les familles.

PARTIES = [
    # ------------------------------------------------------------------ 1. T25-T27
    {
        "cle": "T25-T27",
        "titre": "1. T25 à T27 : les dossiers (déposés, liquidés, en instance, rejetés)",
        "note": (
            "Document : regles_t25_t27_dossiers.md. Liquidé : les droits sont calculés et "
            "notifiés à l'assuré ; en instance : déposé, ni liquidé ni rejeté. T26 n'a aucune "
            "ligne dans le recueil (partie 7). Les indicateurs de délai et de restes à traiter "
            "sont en partie 6."
        ),
        "lignes": [
            ("Prestation #1 (T27), l. 125",
             "« Ce tableau concerne les dossiers de l'exercice (déposés et liquidés) »",
             "Le tableau compte les dépôts et les liquidations de l'année. Deux lectures : "
             "chaque étape dans son année, ou seulement les dossiers déposés dans l'année",
             "T25-T27 H2, D3, D4 : chaque étape dans son année ; réglage `lecture_periode`",
             QU, "T25-T27 Q1"),
            ("Prestation #1 (T27), l. 126",
             "« Dossier en instance : dossier déposé, non liquidé et non rejeté au cours de la "
             "période »",
             "Tout ce qui attend au 31/12 (un stock), ou seulement les dépôts de l'année encore "
             "en attente",
             "T25-T27 D6 : stock au 31/12, limité par les réglages `debut_stock` et "
             "`sans_suite_mois`",
             QU, "T25-T27 Q1, Q2"),
            ("Prestation #1 (T27), l. 127",
             "« Autres prestations de la branche : Renommer par “Frais médicaux de "
             "maternité” »",
             "La dernière ligne de T27 compte les dossiers de maternité dont les frais sont pris "
             "en charge (accouchement, pharmacie…)",
             "T25-T27 N3 ; la base n'a aucun paiement de ces frais de 2020 à 2025",
             AI, "T25-T27 Q15"),
            ("Prestation #1 (T27), l. 128",
             "« Périodicité : Mensuelle, Trimestrielle, Annuelle »",
             "T27 sort pour un mois, un trimestre ou une année",
             "T25-T27 N1 (la requête t27 est annuelle aujourd'hui)",
             AI, ""),
            ("Prestation #18 (T25), l. 248",
             "« Ajouter une ligne « Pension de retraite anticipée » »",
             "Une ligne à part pour la retraite anticipée (code RPRA de l'application)",
             "T25-T27 R2 ; requête t25, ligne 2",
             PC, ""),
            ("Prestation #18 (T25), l. 249 (« Idem. Tab 27 », renvoi à l. 125)",
             "Les dossiers de l'exercice, déposés et liquidés",
             "Même lecture pour les pensions",
             "T25-T27 H2, D3, D4 ; réglage `lecture_periode`",
             QU, "T25-T27 Q1"),
            ("Prestation #18 (T25), l. 249 (renvoi à l. 126)",
             "La définition du dossier en instance",
             "Même stock pour les pensions, sans limite de 24 mois aujourd'hui",
             "T25-T27 D6 ; réglage `sans_suite_mois` à créer dans t25",
             QU, "T25-T27 Q1, Q2"),
            ("Prestation #18 (T25), l. 249 (renvoi à l. 128)",
             "La périodicité mensuelle, trimestrielle, annuelle",
             "T25 sort aussi au mois et au trimestre",
             "T25-T27 N1",
             AI, ""),
            ("Prestation #18 (T25), l. 249 (renvoi à l. 127)",
             "Le renommage en « Frais médicaux de maternité »",
             "Ne peut viser que T27 : la maternité n'est pas dans la branche des pensions",
             "Lecture BI : T25 garde « Autres prestations de la branche » (T25-T27 A1) ; à "
             "signaler à la CNSS",
             PC, ""),
        ],
    },
    # ------------------------------------------------------------------ 2. T28-T31
    {
        "cle": "T28-T31",
        "titre": "2. T28 à T31 : les contrôles a posteriori et les prestations indues",
        "note": (
            "Document : regles_t28_t31_controles_indus.md. Contrôle a posteriori : vérification "
            "d'un dossier après paiement. Indu (trop-perçu) : somme payée en trop. T29 n'a qu'un "
            "renvoi (« Idem. Tab 30 ») ; les ratios de contrôle et d'indus sont en partie 6."
        ),
        "lignes": [
            ("Prestation #2 (T30), l. 134",
             "« TOTAL : total des trimestres »",
             "La colonne TOTAL additionne les contrôles des quatre trimestres ; elle n'ajoute pas "
             "les dossiers payés",
             "T28-T31 N5 ; aucune requête T30",
             AI, ""),
            ("Prestation #2 (T30), l. 135",
             "« Nbre de bénéf. payés : nbre de dossiers payés »",
             "La colonne compte des dossiers payés, pas des personnes (pas les enfants)",
             "T28-T31 N4 ; aucune requête ; la CNSS compte autrement",
             AI, "T28-T31 Q7"),
            ("Prestation #2 (T30), l. 136",
             "« Périodicité : Trimestrielle, Semestrielle, Annuelle »",
             "T30 sort pour un trimestre, un semestre (nouveau) ou une année",
             "T28-T31 N6 ; réglage `payes_depuis`",
             AI, "T28-T31 Q8"),
            ("Prestation #8 (T29), l. 182 (« Idem. Tab 30 », renvoi à l. 134)",
             "TOTAL = total des trimestres",
             "Même colonne TOTAL pour les AT/MP (accidents du travail et maladies "
             "professionnelles)",
             "T28-T31 N5",
             AI, ""),
            ("Prestation #8 (T29), l. 182 (renvoi à l. 135)",
             "Bénéficiaires payés = dossiers payés",
             "Des dossiers payés, là où Kaya 2017 disait « prestations payées » pour T29 (lecture "
             "BI, à confirmer par la DCP)",
             "T28-T31 N4",
             AI, ""),
            ("Prestation #8 (T29), l. 182 (renvoi à l. 136)",
             "Périodicité trimestrielle, semestrielle, annuelle",
             "T29 au trimestre, au semestre, à l'année",
             "T28-T31 N6",
             AI, ""),
            ("Prestation #19 (T28), l. 255",
             "« Ajouter une ligne « Pension de retraite anticipée » »",
             "La retraite anticipée (code RPRA) a sa propre ligne dans T28",
             "T28-T31 N1 ; la correspondance du README la range encore avec la vieillesse",
             AI, ""),
            ("Prestation #19 (T28), l. 256",
             "« Source : Fichiers Excel (DAI) »",
             "Les contrôles sont tenus par la DAI (Direction de l'audit interne) dans des "
             "fichiers Excel, pas dans l'application",
             "T28-T31 N11 ; vue `V_BI_CONTROLE_POSTERIORI` proposée à la DSI",
             HBA, ""),
            ("Prestation #19 (T28), l. 257 (« Idem. Tab 30 », renvoi à l. 134)",
             "TOTAL = total des trimestres",
             "Même colonne TOTAL pour les pensions",
             "T28-T31 N5",
             AI, ""),
            ("Prestation #19 (T28), l. 257 (renvoi à l. 135)",
             "Bénéficiaires payés = dossiers payés",
             "Pour T28 aussi, on compte des dossiers payés",
             "T28-T31 N4",
             AI, ""),
            ("Prestation #19 (T28), l. 257 (renvoi à l. 136)",
             "Périodicité trimestrielle, semestrielle, annuelle",
             "T28 au trimestre, au semestre, à l'année",
             "T28-T31 N6",
             AI, ""),
            ("Prestation #3 (T31), l. 142 à 144",
             "« PVID : … (Branche pension) ; AT/MP : … (RP) ; PF : Prestation familiale »",
             "Les trois branches : pensions (PVID), risques professionnels (AT/MP, ou RP), "
             "prestations familiales (PF). La maternité n'y est pas citée",
             "T28-T31 R1 (la maternité va avec les PF) ; de même T25-T27 R1 et T32-T41 R1. Pour "
             "T56 et T57, voir la partie 5",
             PC, ""),
            ("Prestation #3 (T31), l. 145",
             "« Source : Fichiers Excel (DAI) »",
             "Aujourd'hui, la CNSS tire T31 des fichiers de la DAI",
             "T28-T31 N10 : la base par défaut, le fichier de la DAI pour comparer",
             QU, "T28-T31 Q14"),
            ("Prestation #3 (T31), l. 146",
             "« Périodicité : Mensuelle, Trimestrielle, Annuelle »",
             "T31 sort pour un mois, un trimestre ou une année",
             "T28-T31 N6 ; réglage `periode` à créer (t31 ne tourne que sur l'année)",
             AI, ""),
            ("Prestation #29 (T51), l. 326, reprise pour T28",
             "« Allocation de survivants : veufs/veuves, orphelins et ascendants »",
             "La ligne « allocation de survivant » de T28 prend aussi l'allocation d'ascendant "
             "(code RASA)",
             "T28-T31 N1 ; aucune requête T28",
             AI, ""),
            ("Prestation #1 (T27), l. 127, reprise pour T30",
             "« Autres prestations de la branche : Renommer par “Frais médicaux de "
             "maternité” »",
             "Écrit pour T27 ; T30 a la même dernière ligne pour la même branche",
             "T28-T31 N3, par analogie ; ligne vide (ces frais ne sont pas dans la base)",
             QU, "T28-T31 Q12"),
            ("Prestation #30 (T76), l. 333, reprise pour les taux",
             "« Ratio : en pourcentage (2 chiffres après la virgule) »",
             "Écrit pour le tableau 76 (action sanitaire et sociale) ; vaut-il pour les taux de "
             "contrôle et d'indus ?",
             "T28-T31 N12, par analogie ; la requête t31 arrondit à 3 chiffres",
             QU, "T28-T31 Q30"),
        ],
    },
    # ------------------------------------------------------------------ 3. T32-T41
    {
        "cle": "T32-T41",
        "titre": "3. T32 à T41 : les bénéficiaires au 31 décembre (pensionnés, rentiers, "
                 "allocataires)",
        "note": (
            "Document : regles_t32_t41_beneficiaires.md. « Idem. Tab 41 » reprend la périodicité "
            "de T41 ; la définition de l'allocataire ne s'applique pas à un survivant (on compte "
            "la veuve, l'orphelin, l'ascendant, pas l'assuré décédé). T39 et T40 n'ont aucune "
            "ligne (partie 7)."
        ),
        "lignes": [
            ("Prestation #4 (T41), l. 152",
             "« Allocataire : Titulaire du dossier (assuré) »",
             "Dans T41, on compte une fois la personne au nom de qui le dossier d'allocations "
             "familiales est ouvert (l'assuré) : pas les enfants, pas celui qui encaisse",
             "T32-T41 N2 ; réglage `allocataire` = TITULAIRE. Pour T32 à T49, partie 4 ; pour "
             "T57, partie 5",
             AI, "T32-T41 Q8"),
            ("Prestation #4 (T41), l. 153",
             "« Périodicité : Mensuelle, Trimestrielle, Annuelle »",
             "T41 se compte aussi à la fin de chaque mois et de chaque trimestre",
             "T32-T41 N1, N10 ; réglages `periodicite`, `t41_paiement`",
             AI, ""),
            ("Prestation #9 (T37), l. 188",
             "« Idem. Tab 41 »",
             "Périodicité de T41 ; pour une rente viagère, le titulaire est la victime elle-même",
             "T32-T41 N1, R5",
             AI, ""),
            ("Prestation #10 (T38), l. 194",
             "« Crédirentiers : rentiers »",
             "Dire « rentiers » dans le titre et les libellés",
             "T32-T41 N9 ; T39 et T40 par cohérence. Pour T57, partie 5",
             AI, "T32-T41 Q13"),
            ("Prestation #10 (T38), l. 195",
             "« Le cumul sans préciser un exercice »",
             "Lecture BI : tous les rentiers en cours à la date choisie, quelle que soit l'année "
             "où leur rente a commencé (un stock). Autre lecture : un montant cumulé",
             "T32-T41 R6 (stock à la date d'arrêté), N1",
             QU, "T32-T41 Q14"),
            ("Prestation #10 (T38), l. 196",
             "« Idem. Tab 41 »",
             "Périodicité de T41 ; on compte le veuf ou la veuve, pas la victime décédée",
             "T32-T41 N1, R5 (lecture BI)",
             AI, ""),
            ("Prestation #20 (T32), l. 263",
             "« Pour les pensions de coordination, les montants concernent uniquement la partie "
             "CNSS »",
             "Coordination : l'assuré a cotisé à la CNSS et dans une autre caisse. Le montant ne "
             "prend que ce que verse la CNSS",
             "T32-T41 R3 ; de même T42-T49 N6 (par analogie pour T43 et T44) et T50-T57 "
             "(montants de T56)",
             PC, "T32-T41 Q16"),
            ("Prestation #20 (T32), l. 264",
             "« Idem. Tab 41 »",
             "Périodicité de T41 ; le titulaire est le retraité lui-même",
             "T32-T41 N1",
             AI, ""),
            ("Prestation #21 (T33), l. 270",
             "« Les informaticiens doivent développer un nouveau module pour la gestion des "
             "pensions d'invalidité qui sont convertis en pension normale dès que l'assuré "
             "atteint l'âge de départ à la retraite »",
             "Un travail sur l'application, pas sur la BI. Sans ce module, un invalide reste "
             "invalide toute sa vie dans l'application",
             "T32-T41 N5 (T33 compte les invalides tels qu'ils sont) ; T42-T49 (effet sur T42)",
             HBI, "T32-T41 Q15 ; T42-T49 Q18"),
            ("Prestation #21 (T33), l. 271",
             "« Idem. Tab 41 »",
             "Périodicité de T41 ; le titulaire est l'invalide lui-même",
             "T32-T41 N1",
             AI, ""),
            ("Prestation #22 (T34), l. 277",
             "« Idem. Tab 41 »",
             "Périodicité de T41 ; on compte la veuve ou le veuf, pas l'assuré décédé",
             "T32-T41 N1, R5",
             AI, ""),
            ("Prestation #23 (T35), l. 283",
             "« Supprimer la tranche d'âge « 25 ans et plus » »",
             "Un orphelin n'est compté que jusqu'à 24 ans : la dernière ligne de T35 devient "
             "« 20-24 ans »",
             "T32-T41 R6 ; réglage `age_max_orphelin`. Pour T44 et T46, partie 4",
             PC, ""),
            ("Prestation #23 (T35), l. 284",
             "« Idem. Tab 41 »",
             "Périodicité de T41 ; on compte l'orphelin, pas le tuteur qui reçoit le paiement",
             "T32-T41 N1, R5",
             AI, ""),
            ("Prestation #24 (T36), l. 290",
             "« Idem. Tab 41 »",
             "Périodicité de T41 ; on compte l'ascendant (père ou mère), pas l'assuré décédé",
             "T32-T41 N1, R5",
             AI, ""),
        ],
    },
    # ------------------------------------------------------------------ 4. T42-T49
    {
        "cle": "T42-T49",
        "titre": "4. T42 à T49 : les nouveaux bénéficiaires de l'année (pensionnés et rentiers)",
        "note": (
            "Document : regles_t42_t49_nouveaux.md. T46, T47 et T49 n'ont qu'un renvoi "
            "(« Idem. Tab 45 », l. 210, 216, 230), rattaché ici à la ligne qu'il reprend ; T48 "
            "a le sien en l. 224. Les lignes de T32, T33 et T50, T51 qu'utilise aussi cette "
            "famille sont en parties 3 et 5 (même statut)."
        ),
        "lignes": [
            ("Prestation #25 (T42), l. 296",
             "« Service passé : total des périodes d'activités salarié en nombre de mois »",
             "La colonne « Total services passés » additionne les mois de travail salarié des "
             "nouveaux retraités",
             "T42-T49 R9 : mois cotisés (`PE_MOIS_COTISATION`), toujours remplis",
             PC, ""),
            ("Prestation #25 (T42), l. 297",
             "« La modalité « total services passés » est à estimer en nombre de mois »",
             "En mois, pas en années ; une valeur approchée suffit",
             "T42-T49 R9",
             PC, ""),
            ("Prestation #25 (T42), l. 298",
             "« Pour les pensions de coordination, prendre uniquement la partie CNSS »",
             "On ne garde que la part de la CNSS ; la phrase ne dit pas si c'est pour les mois, "
             "pour le montant, ou pour les deux",
             "T42-T49 R9 (mois sans l'autre caisse), N6 (montant payé par la CNSS). Pour T50, "
             "partie 5",
             QU, "T42-T49 Q10, Q11"),
            ("Prestation #25 (T42), l. 299",
             "« Périodicité : Mensuelle, Trimestrielle, Annuelle »",
             "T42 sort par mois, par trimestre et par année",
             "T42-T49 N1 ; la requête n'est qu'annuelle",
             AI, ""),
            ("Prestation #11 (T45), l. 203 ; #26 (T43), l. 305 ; #27 (T44), l. 312 ; repris "
             "pour T46 à T49 (l. 210, 216, 224, 230)",
             "« Indiquer la période (début, fin) en paramètre »",
             "L'utilisateur choisit une date de début et une date de fin ; le tableau compte les "
             "personnes entrées entre les deux",
             "T42-T49 N1, N2, N3 ; réglages `debut` et `fin` à rendre libres",
             AI, ""),
            ("Prestation #11 (T45), l. 202 ; repris pour T46 à T49",
             "« Nouveaux crédirentiers : rentiers »",
             "Un crédirentier est un rentier : T45 compte toutes les nouvelles rentes d'AT/MP",
             "T42-T49 R2",
             PC, ""),
            ("#11 (T45), l. 204 ; #26 (T43), l. 306 ; #27 (T44), l. 313 ; repris pour T46 à "
             "T49 (renvoi à l. 153)",
             "« Idem. Tab 41 » : « Périodicité : Mensuelle, Trimestrielle, Annuelle »",
             "T43 à T49 sortent aussi par mois, par trimestre et par année",
             "T42-T49 N1",
             AI, ""),
            ("Mêmes lignes, et T32 à T38 (l. 188, 196, 264, 271, 277, 284, 290) : renvoi à l. 152",
             "« Idem. Tab 41 » : « Allocataire : Titulaire du dossier (assuré) »",
             "Lu à la lettre pour un survivant, on compterait l'assuré décédé à la place de sa "
             "veuve, de son orphelin ou de son parent",
             "T42-T49 N4 et T32-T41 R5 : on garde l'ayant droit ; une seule question pour T32 à "
             "T49",
             QU, "T42-T49 Q1"),
            ("Prestation #14 (T48), l. 222",
             "« Dégré IPP : moyenne des taux IPP (somme des taux diviser par le total de la "
             "tranche) »",
             "IPP : incapacité permanente partielle, le taux d'incapacité de la victime. Pour "
             "chaque tranche, la somme des taux divisée par le nombre de rentiers",
             "T42-T49 N7",
             PC, "T42-T49 Q15"),
            ("Prestation #14 (T48), l. 223",
             "« Chaque rentier possède un taux dans l'application »",
             "Le taux est toujours saisi",
             "Vérifié : jamais vide ni nul, de 15 à 100, de 2020 à 2025 (P87)",
             PC, ""),
            ("Prestation #18 (T25), l. 248 ; #19 (T28), l. 255, pour T42",
             "« Ajouter une ligne « Pension de retraite anticipée » »",
             "Demandé pour T25 et T28 ; le recueil ne dit rien pour T42",
             "T42-T49 R2 : dans T42 avec la vieillesse (proposition BI)",
             QU, "T42-T49 Q13"),
            ("Prestation #23 (T35), l. 283, pour T44 et T46",
             "« Supprimer la tranche d'âge « 25 ans et plus » »",
             "Écrit pour le stock des orphelins (T35) ; les grilles de T44 et T46 s'arrêtent à "
             "« 20 ans et plus »",
             "Réglage `orphelin_age_max` (à créer)",
             QU, "T42-T49 Q14"),
        ],
    },
    # ------------------------------------------------------------------ 5. T50-T57
    {
        "cle": "T50-T57",
        "titre": "5. T50 à T57 : l'évolution sur cinq ans, les prestations servies par branche, "
                 "les décédés",
        "note": (
            "Document : regles_t50_t57_evolution.md. T56 n'a aucune ligne (partie 7) ; T53 et "
            "T55 n'ont qu'un renvoi. Montant liquidé : ce qui est dû pour les prestations "
            "accordées dans l'année, même payé plus tard."
        ),
        "lignes": [
            ("Prestation #28 (T50), l. 319",
             "« Service passé : total des périodes d'activités salarié en nombre de mois »",
             "Les services passés d'un allocataire de vieillesse se comptent en mois ; T50 en "
             "donne la moyenne",
             "T50-T57 R9, N10 ; de même T42-T49 R2 (frontière de T42, l. 319 et 320)",
             PC, ""),
            ("Prestation #28 (T50), l. 320",
             "« Périodicité : Annuelle »",
             "T50 sort une fois par an",
             "T50-T57, requête 1",
             PC, ""),
            ("Prestation #29 (T51), l. 326",
             "« Allocation de survivants : veufs/veuves, orphelins et ascendants »",
             "T51 compte aussi les ascendants, que l'Annuaire CIPRES ne cite pas",
             "T50-T57 R2 (codes RASC, RASO, RASA) ; de même T42-T49 R2 (frontière de T43 et "
             "T44). Pour T28, partie 2",
             PC, ""),
            ("Prestation #29 (T51), l. 327",
             "« Idem. Tab 52 »",
             "T51 sort une fois par an",
             "T50-T57, requête 1",
             PC, ""),
            ("Prestation #16 (T52), l. 236",
             "« Périodicité : Annuelle »",
             "T52 sort une fois par an",
             "T50-T57, requête 1",
             PC, ""),
            ("Prestation #17 (T53), l. 242",
             "« Idem. Tab 52 »",
             "T53 sort une fois par an",
             "T50-T57, requête 1",
             PC, ""),
            ("Prestation #5 (T54), l. 159",
             "« Montant : Montant liquidé »",
             "Le montant de T54 est ce qui est dû pour les indemnités de maternité liquidées "
             "dans l'année, pas ce qui a été payé dans l'année",
             "T50-T57 N1 ; réglage `montant_t54_t55` (à créer)",
             AI, "T50-T57 Q3"),
            ("Prestation #5 (T54), l. 160",
             "« Périodicité : Annuelle »",
             "T54 sort une fois par an",
             "T50-T57, requête 1",
             PC, ""),
            ("Prestation #6 (T55), l. 166 (« Idem Tab. 54 », renvoi à l. 159)",
             "Le montant liquidé",
             "Même règle pour les allocations prénatales",
             "T50-T57 N1, comme T54",
             AI, "T50-T57 Q3"),
            ("Prestation #6 (T55), l. 166 (renvoi à l. 160)",
             "La périodicité annuelle",
             "T55 sort une fois par an",
             "T50-T57, requête 1",
             PC, ""),
            ("Prestation #7 (T57), l. 172",
             "« Assurés décédés »",
             "T57 compte des assurés décédés : titulaire d'une pension directe, rentier et "
             "sinistré (victime d'accident), allocataire. L'assuré mort en activité en est-il ?",
             "T50-T57 Fiche 7, N2",
             QU, "T50-T57 Q4"),
            ("Prestation #7 (T57), l. 173",
             "« Remplacer « Autres » par « TOTAL » »",
             "Colonnes PF, AT/MP, AVS et TOTAL ; « Autres » disparaît",
             "T50-T57 N3 (la requête 3 sort encore une colonne AUTRES)",
             AI, ""),
            ("Prestation #7 (T57), l. 174",
             "« Désagréger les branches par type de bénéficiaires »",
             "Chaque branche se détaille : retraité, veuve, orphelin… ; rentier, sinistré, "
             "survivant ; allocataire",
             "T50-T57 N2 (la requête 3 ne garde que la branche)",
             AI, ""),
            ("Prestation #7 (T57), l. 175",
             "« Ajouter un autre filtre : Assuré ou ayant droit »",
             "Montrer les assurés décédés, les ayants droit décédés (veuves, orphelins…), ou les "
             "deux",
             "T50-T57 N2 ; réglage `qualite_deces` (à créer)",
             AI, "T50-T57 Q10"),
            ("Prestation #7 (T57), l. 176",
             "« Périodicité : Annuelle »",
             "T57 sort une fois par an",
             "T50-T57, requête 3",
             PC, ""),
            ("Prestation #4 (T41), l. 152, reprise pour T57",
             "« Allocataire : Titulaire du dossier (assuré) »",
             "Dans T57, branche PF : l'allocataire décédé est le titulaire du dossier",
             "T50-T57 Fiche 7",
             PC, ""),
            ("Prestation #10 (T38), l. 194, reprise pour T57",
             "« Crédirentiers : rentiers »",
             "Le titulaire d'une rente de survivant est aussi un « rentier »",
             "T50-T57 Fiche 7, N2",
             PC, ""),
            ("Prestation #25 (T42), l. 298, reprise pour T50",
             "« Pour les pensions de coordination, prendre uniquement la partie CNSS »",
             "Pour T50, seuls les mois cotisés à la CNSS",
             "T50-T57 R9 : mois cotisés moins `PE_COT_AUTRE` (0 à 4 allocations par an) ; le sens "
             "de ce champ est la question T42-T49 Q10, comme pour T42",
             QU, "T42-T49 Q10"),
            ("Prestation #3 (T31), l. 142 à 144, reprise pour T56 et T57",
             "« PVID : … (Branche pension) ; AT/MP : … (RP) ; PF : Prestation familiale »",
             "Les noms que la CNSS donne à ses trois branches",
             "T50-T57 R1 : écrire « AVS (PVID) » et « AT/MP (RP) » dans T56 et T57",
             AI, ""),
            ("Prestation #1 (T27), l. 127, reprise pour T56",
             "« Autres prestations de la branche : Renommer par “Frais médicaux de "
             "maternité” »",
             "Les frais médicaux de maternité sont une prestation des PF : ils vont dans la "
             "ligne PF de T56",
             "T50-T57 R1, Fiche 6 ; la base n'en paie aucun",
             QU, "T50-T57 Q17"),
        ],
    },
    # ------------------------------------------------------------------ 6.1 PF
    {
        "cle": "6",
        "titre": "6.1 Indicateurs des prestations familiales (PF), maternité comprise",
        "note": (
            "Les formules des indicateurs sont des équations Word, absentes du texte : les "
            "familles les ont relues dans le document Word. DES, DR, DAI : structures écrites "
            "dans le recueil (sigles en partie 8)."
        ),
        "lignes": [
            ("Indicateurs PF, l. 1093 à 1103",
             "« Le délai de liquidation d'un dossier de prestations familiales » : « la moyenne "
             "des durées de liquidation » ; trimestriel, annuel ; par DR et agence ; DES, DR",
             "Le nombre moyen de jours pour liquider un dossier de PF",
             "T25-T27 D5, N1, N2, N5",
             AI, ""),
            ("Indicateurs PF, l. 1097",
             "« NB : La durée de liquidation est la différence entre la date à laquelle un "
             "dossier complet de PF a été réceptionné au guichet, et la date de validation »",
             "Pour les PF, le délai finit à la validation (Kaya 2017 disait : à la notification)",
             "T25-T27 D4 ; réglage `fin_duree_pf` : la notification en repli",
             QU, "T25-T27 Q16"),
            ("Indicateurs PF, l. 1102 (pour le délai)",
             "« Par type de prestations (Allocations familiales et Indemnités Journalière de "
             "Maternité) »",
             "L'indicateur de délai ne prend que deux types : l'allocation prénatale n'y est pas",
             "T25-T27 D14, N5",
             QU, "T25-T27 Q14"),
            ("Indicateurs PF, l. 1102 (pour le rangement)",
             "« Par type de prestations (Allocations familiales et Indemnités Journalière de "
             "Maternité) »",
             "Le recueil range lui-même les IJ (indemnités journalières) de maternité avec les PF",
             "T50-T57 R1 (T54 et la ligne PF de T56)",
             PC, ""),
            ("Indicateurs PF, l. 1104 à 1110",
             "« Le taux des dossiers restant à traiter en fin de période (CIPRES) » : en "
             "instance à la fin ÷ dossiers complets reçus × 100 ; trimestriel, annuel ; par "
             "type, DR et agence",
             "La part des dossiers de PF encore en attente à la fin de la période",
             "T25-T27 N4, N1, N2 ; réglage `formule_taux`",
             AI, "T25-T27 Q19"),
            ("Indicateurs PF, l. 1111 et 1116 à 1119",
             "« Le ratio de contrôle a posteriori » : dossiers contrôlés après paiement ÷ "
             "dossiers liquidés et payés × 100 ; mensuel, trimestriel, annuel ; par type, DR "
             "et agence ; DAI, DR",
             "La part des dossiers de PF vérifiés après paiement",
             "T28-T31 C1, N5, N6, N9 ; aucun contrôle enregistré depuis juillet 2025",
             HBA, ""),
            ("Indicateurs PF, l. 1120 et 1125 à 1128",
             "« Le ratio des prestations indues » : montant des prestations indues ÷ montant "
             "des prestations payées × 100 ; mensuel, trimestriel, annuel ; par type, DR et "
             "agence ; DAI, DR",
             "Le taux d'indus des PF (sommes payées en trop)",
             "T28-T31 : fiche T31, règle 8 ; N9 ; le taux par branche manque à la requête t31",
             AI, "T28-T31 Q27"),
            ("Indicateurs PF, l. 1129 à 1141 ; AT/MP l. 1210 à 1214 ; pensions l. 1287 à 1294",
             "« Le ratio des ressources de cotisations allouées aux prestations » : prestations "
             "ordonnancées ÷ cotisations ordonnancées × 100 ; par type, DR et agence",
             "La part des cotisations consommée par les prestations",
             "T50-T57 N11 (les montants viennent de T56)",
             AI, "T50-T57 Q20"),
            ("Indicateurs PF, l. 1133 (la note)",
             "« Le montant total des prestations ordonnancées au titre de la période est "
             "l'ensemble des prestations ayant fait l'objet d'une notification »",
             "Pour la CNSS, une prestation est ordonnancée quand elle est notifiée",
             "T25-T27 D4 ; T32-T41 (versée aux questions du montant et de sa date) ; T42-T49 "
             "(R7 garde la saisie)",
             PC, "T32-T41 Q1, Q2"),
            ("Indicateurs PF, l. 1137 à 1139",
             "Ratio des ressources des PF : « Trimestriel / Annuel »",
             "Les montants des PF doivent aussi se lire au trimestre",
             "T50-T57 N8 : garder le mois de chaque paiement",
             AI, ""),
        ],
    },
    # ------------------------------------------------------------------ 6.2 AT/MP
    {
        "cle": "6",
        "titre": "6.2 Indicateurs des accidents du travail et maladies professionnelles (AT/MP)",
        "note": None,
        "lignes": [
            ("Indicateurs AT/MP, l. 1146 à 1156",
             "« Taux de variation du nombre de déclarations d'AT/MP » : (déclarations de n − "
             "déclarations de n−1) ÷ déclarations de n−1 × 100 ; annuel ; par DR et agence ; DP, "
             "DR",
             "De combien les déclarations d'accident ou de maladie professionnelle montent ou "
             "baissent d'une année à l'autre",
             "T25-T27 N8",
             AI, ""),
            ("Indicateurs AT/MP, l. 1157 à 1169",
             "« Le délai de liquidation des remboursements de soins » : « la différence entre la "
             "date de réception du certificat médical du medecin traitant et la date de "
             "convocation » ; trimestriel, annuel ; par DR et agence",
             "Du jour où la CNSS reçoit le certificat du médecin traitant au jour de la "
             "convocation",
             "T25-T27 D11 ; ces deux dates ne sont pas trouvées dans la base",
             QU, "T25-T27 Q17"),
            ("Indicateurs AT/MP, l. 1170 à 1179",
             "« Le délai de liquidation des rentes de survivants » : « la différence entre la "
             "date de réception du certificat médical de consolidation et la date de "
             "notification » ; annuel ; par DR et agence",
             "Le certificat de consolidation concerne la victime, pas ses survivants : la phrase "
             "vise sans doute la rente viagère",
             "T25-T27 : réglage `depart_rente` (la demande au guichet en attendant) ; T42-T49, "
             "même statut : sa population est celle des nouvelles rentes de T45 à T49 (R7)",
             QU, "T25-T27 Q8, Q9"),
            ("Indicateurs AT/MP, l. 1180 à 1188",
             "Le taux des dossiers restant à traiter, même formule ; annuel ; par type, DR et "
             "agence ; DR, DCP",
             "La part des dossiers AT/MP en attente au 31/12",
             "T25-T27 N4 ; il dépend beaucoup des déclarations sans suite (D12)",
             AI, "T25-T27 Q19"),
            ("Indicateurs AT/MP, l. 1190 et 1196 à 1198",
             "Le ratio de contrôle a posteriori ; annuel ; par type, DR et agence ; DR, DCP",
             "La part des dossiers AT/MP vérifiés après paiement",
             "T28-T31 C1 ; contrôles à obtenir de la DAI",
             HBA, ""),
            ("Indicateurs AT/MP, l. 1199 et 1207 à 1209",
             "Le ratio des prestations indues ; annuel ; par type, DR et agence ; DR, DCP",
             "Le taux d'indus des AT/MP",
             "T28-T31 : fiche T31, règle 8 ; N7, N9",
             AI, ""),
        ],
    },
    # ------------------------------------------------------------------ 6.3 pensions
    {
        "cle": "6",
        "titre": "6.3 Indicateurs des pensions",
        "note": None,
        "lignes": [
            ("Indicateurs pensions, l. 1216 à 1238 (note l. 1224)",
             "« Délai de liquidation des dossiers de pension » : de la réception au guichet d'un "
             "dossier complet à la notification des droits ; trimestriel, annuel ; par type, DR "
             "et agence ; DCP, DR",
             "Le nombre moyen de jours entre le dépôt d'un dossier de pension complet et la "
             "notification",
             "T25-T27 D3, D4, D5, N1, N2, N5 ; T42-T49, même statut : il porte sur les mêmes "
             "premières pensions que T42 à T44, datées à la notification, contre-indice pour R7 "
             "qui compte à la saisie",
             AI, "T42-T49 Q2"),
            ("Indicateurs pensions, l. 1239 à 1245",
             "Le taux des dossiers restant à traiter ; annuel ; par type, DR et agence",
             "La part des dossiers de pension en attente au 31/12",
             "T25-T27 N4 ; le stock des pensions reste gonflé (D6)",
             AI, "T25-T27 Q19"),
            ("Indicateur « rapport démographique », l. 1247 à 1254",
             "Rapport = cotisants actifs ÷ retraités ; annuel ; par DR et agence",
             "Le nombre de cotisants pour un retraité ; les retraités sont ceux de T32 (retraite "
             "anticipée comprise) et de T33",
             "T32-T41 N7, N3",
             AI, "T32-T41 Q17"),
            ("Indicateurs pensions, l. 1251 et 1262 (la phrase sur les pensions directes)",
             "« Le nombre de retraités est le nombre de bénéficiaires de pensions directes "
             "(Pension vieillesse, Pension anticipé, pension d'invalidité) »",
             "Un retraité touche une pension directe ; l'allocation de vieillesse n'en est pas",
             "T25-T27 (comparer les départs à la retraite) ; T42-T49 R2 (pas reprise pour T42) ; "
             "T50-T57 Fiche 7 (branche AVS de T57)",
             PC, "T25-T27 Q21"),
            ("Indicateur « taux de remplacement », l. 1256 à 1265",
             "Taux = montant de la pension ÷ dernier revenu d'activité × 100 ; « Le dernier "
             "revenu d'activité est le salaire moyen des cinq meilleures années » ; pensions "
             "directes ; annuel ; par DR et agence",
             "La part du salaire que la pension remplace",
             "T32-T41 et T42-T49 : pas encore calculé ; le salaire moyen de l'application "
             "(`PE_SAL_MOYEN`) n'est pas encore étudié",
             QU, "T32-T41 Q18, Q19 ; T42-T49 Q19"),
            ("Indicateurs pensions, l. 1267, 1274 et 1275",
             "Le ratio de contrôle a posteriori ; annuel ; par type, DR et agence ; sans "
             "structure responsable",
             "La part des dossiers de pension vérifiés après paiement",
             "T28-T31 C1 ; contrôles à obtenir de la DAI",
             HBA, ""),
            ("Indicateurs pensions, l. 1277, 1284 et 1285",
             "Le ratio des prestations indues ; annuel ; par type, DR et agence",
             "Le taux d'indus des pensions",
             "T28-T31 : fiche T31, règle 8 ; N9",
             AI, ""),
        ],
    },
    # ------------------------------------------------------------------ 6.4 ASS
    {
        "cle": "6",
        "titre": "6.4 Indicateurs de l'action sanitaire et sociale (ASS)",
        "note": (
            "Deux autres indicateurs de l'ASS (normes sanitaires, réussite au CQP) sont en partie "
            "6.6, avec les autres indicateurs rangés dans la famille T50-T57."
        ),
        "lignes": [
            ("Indicateurs ASS, l. 1318 à 1337",
             "Charges administratives de l'ASS sur ses charges totales, puis sur celles de la "
             "CNSS ; taux de couverture des dépenses de l'ASS ; annuel ; DCG",
             "L'ASS a ses propres indicateurs ; le montant de l'ASS de la ligne « Autres » de "
             "T56 doit être le même",
             "T50-T57 N5",
             QU, "T50-T57 Q18"),
            ("Prestation #30 (T76), l. 333 et 334",
             "« Ratio : en pourcentage (2 chiffres après la virgule) » ; « Périodicité : "
             "Annuelle »",
             "T76 est le tableau des dépenses et recettes de l'ASS, hors T25 à T57 ; Kaya 2017 : "
             "il vient d'un fichier Excel",
             "T50-T57 N5 : lien seulement (même montant d'ASS que la ligne « Autres » de T56)",
             HBA, ""),
        ],
    },
    # ------------------------------------------------------------------ 6.5 autres
    {
        "cle": "6",
        "titre": "6.5 Les autres demandes : toutes les branches, la finance, les lignes générales",
        "note": (
            "Une ligne générale vaut pour toutes les familles. Quand les familles lui donnent "
            "des statuts différents, elle a une ligne par statut (voir partie 9)."
        ),
        "lignes": [
            ("Indicateur 2, l. 1085 à 1091",
             "« La proportion des prestations payées par virement bancaire » : payées par "
             "virement ÷ payées × 100 ; annuel, trimestriel, mensuel ; par type, DR, agence et "
             "mode de paiement ; DES, DCP, DR",
             "La part des paiements faits par virement",
             "T32-T41 (hors de la famille : mêmes paiements que R3) ; les autres familles le "
             "citent parmi les lignes qui ne changent rien",
             AI, ""),
            ("Indicateurs prestations, l. 1357, 1369 et 1373 à 1375",
             "« Le taux de prestations indues (CIPRES) » : même formule ; trimestriel, annuel ; "
             "par DR et agence",
             "Le taux d'indus de toute la CNSS",
             "T28-T31 : taux total de la requête t31 ; trimestre et DR à ajouter (N6, N9)",
             AI, ""),
            ("Indicateurs du recouvrement, l. 1608 à 1614",
             "« Le taux de prestations indues (CIPRES) » ; annuel ; par DR et agences ; DCR, DR",
             "Le même indicateur, rangé aussi sous le recouvrement",
             "T28-T31 : même calcul que la l. 1369",
             QU, "T28-T31 Q29"),
            ("Structures responsables, l. 1119, 1128, 1198 et 1209",
             "« DAI, DR » pour les ratios des PF ; « DR, DCP » pour ceux des AT/MP",
             "À qui poser les questions sur les deux ratios",
             "T28-T31 (destinataires des questions)",
             PC, ""),
            ("Ratios CIPRES respectés, l. 1978 à 1989",
             "« Pourcentage des ratios CIPRES respectés » ; « Source : DAI » ; « Données non "
             "disponibles en base »",
             "La part des normes CIPRES tenues",
             "T25-T27 N7 ; T28-T31 N12 (ils fourniront leurs ratios avec leur norme)",
             HBA, ""),
            ("Finance #1 (T66), l. 361 ; repris pour T67 et T68 (l. 372, 378)",
             "« Charges techniques (ct) : 601xxx, 602xxx, 603xxx, 605xxx »",
             "En finance, les prestations se lisent dans ces comptes de la comptabilité",
             "T50-T57 N6 : lire T56 dans la comptabilité, en option",
             QU, "T50-T57 Q5"),
            ("Finance #11 (T74), l. 445 à 449",
             "« Voir la DSI pour les formules de calcul des colonnes du tableau » ; « Norme : "
             "inférieur strictement à 100% »",
             "Le rapport des prestations aux cotisations, côté finance",
             "T50-T57 : même numérateur que N11 ; T74 se fait dans le domaine Finance",
             PC, ""),
            ("Finance #12 (T75), l. 455, 461 et 464",
             "« Réserves en années de prestations » ; « Pour chaque branche : PF, AT/MP, AV "
             "(PVID) »",
             "T75 a besoin des prestations de l'année d'avant, par branche",
             "T50-T57 : T56 les donne ; N6",
             PC, ""),
            ("Termes de référence, l. 4 à 10",
             "« cinq (5) Directions régionales : Ouagadougou, Bobo Dioulasso, Ouahigouya, Fada "
             "N'gourma, Dédougou »",
             "« Par DR » veut dire ces cinq directions régionales",
             "T25-T27 N2 ; T28-T31 N9 ; T32-T41 N3 ; T42-T49 N5 ; T50-T57 N8",
             AI, ""),
            ("Indicateurs, l. 1065",
             "« De manière globale, donner la possibilité de désagréger l'indicateur »",
             "Chaque indicateur doit pouvoir se découper : DR, agence, type de prestation",
             "T25-T27 N2 ; T28-T31 N9 ; T32-T41 N3, N4 ; T42-T49 N5 ; T50-T57 N8",
             AI, ""),
            ("Attentes, l. 29 ; caractéristiques, l. 51 et 68",
             "Données « traquées jusqu'au niveau de granularité le plus élémentaires » ; "
             "analyses « y compris au niveau transactionnel »",
             "Chaque chiffre doit se déplier jusqu'à la liste des dossiers, des personnes ou des "
             "paiements",
             "T25-T27 N6 ; T28-T31 N13 ; T32-T41 N12 ; T42-T49 (liste des nouveaux, à rendre "
             "consultable) ; T50-T57 N8 (l. 29)",
             AI, ""),
            ("Attentes et caractéristiques, l. 31 et 74",
             "Présentation multidimensionnelle ; une dimension ajoutée après coup vaut pour "
             "tous les indicateurs",
             "Les mêmes axes partout : branche, ligne, DR, agence, mois",
             "T25-T27 N2, N6 ; T28-T31 N9, N13 ; T32-T41 N3, N4, N12 ; T42-T49 N1, N5 ; T50-T57 "
             "N8",
             AI, ""),
            ("Attentes, l. 32",
             "« Emission d'alertes sur les indicateurs de performances »",
             "Prévenir quand un délai ou un taux sort de sa norme",
             "T25-T27 N7 ; T28-T31 N12. Sans objet pour T32 à T57 (aucune norme)",
             AI, ""),
            ("Attentes, l. 35 (T25 à T49)",
             "« Production des rapports et des états de restitution à des fréquences "
             "adaptées »",
             "Chaque tableau à sa fréquence",
             "T25-T27 N1 ; T28-T31 N6 ; T32-T41 N1 ; T42-T49 N1",
             AI, ""),
            ("Attentes, l. 35 (T50 à T57)",
             "« Production des rapports et des états de restitution à des fréquences "
             "adaptées »",
             "Toute la famille est annuelle, comme le demande le recueil",
             "T50-T57 (l. 160, 166, 176, 236, 242, 320, 327)",
             PC, ""),
            ("Attentes et caractéristiques, l. 28, 34 et 75 (T25 à T27, T32 à T57)",
             "Peu d'intervention manuelle ; restitutions automatisées ; rafraîchissement "
             "automatique",
             "Les chiffres sortent de requêtes, sans saisie à la main",
             "T25-T27, T32-T41, T42-T49, T50-T57 : requêtes sur la base",
             PC, ""),
            ("Attentes et caractéristiques, l. 28, 34 et 75 (T28 à T31)",
             "Peu d'intervention manuelle ; restitutions automatisées ; rafraîchissement "
             "automatique",
             "Cela se heurte aux fichiers Excel de la DAI",
             "T28-T31 N10 (T31 lu dans la base), N11 (un modèle de fichier fixe, chargé dans la "
             "vue)",
             AI, ""),
            ("Caractéristiques, l. 53 à 55 et 60 (T28 à T31)",
             "Charger des fichiers locaux ou distants ; faire des indicateurs à partir de bases "
             "ou de documents",
             "La BI peut prendre les fichiers de la DAI",
             "T28-T31 N11",
             AI, ""),
            ("Caractéristiques, l. 53 à 55 et 60 (T50 à T57)",
             "Charger des fichiers locaux ou distants ; faire des indicateurs à partir de bases "
             "ou de documents",
             "L'outil sait charger un fichier Excel",
             "T50-T57 : possible pour l'ASS (N5) si aucune source automatique n'est retenue",
             PC, ""),
            ("Caractéristiques, l. 58",
             "Règles de transcodage (passage d'un code à un libellé) écrites et rejouables",
             "La correspondance entre codes de prestation et lignes des tableaux est écrite",
             "Les cinq familles : table de correspondance du README (R2)",
             PC, ""),
            ("Caractéristiques, l. 72 (T28 à T41)",
             "« Permettre le contrôle par profil d'utilisateurs des conditions d'accès aux "
             "données (règles de confidentialité) »",
             "Les contrôles de la DAI et la liste des bénéficiaires sont sensibles : seuls "
             "certains profils les voient",
             "T28-T31 N14 ; T32-T41 N12",
             AI, "T28-T31 Q10"),
            ("Caractéristiques, l. 72 (T50 à T57)",
             "« Permettre le contrôle par profil d'utilisateurs des conditions d'accès aux "
             "données (règles de confidentialité) »",
             "Les données des décédés sont personnelles",
             "T50-T57 : T57 ne lit pas la vue `V_PRESTATAIRES_DECEDES_BI` (nom, adresse, "
             "téléphone)",
             PC, "T50-T57 Q23"),
            ("Attentes et caractéristiques, l. 24 et 78",
             "Profilage de la qualité des données",
             "Mesurer les trous de la base",
             "Les cinq familles : lots de contrôle et points d'attention",
             PC, ""),
            ("Général, l. 15 et 92 à 94",
             "La source : l'« application de gestion des prestations »",
             "Les tableaux se calculent sur la copie de cette application",
             "Les cinq familles : copie de la base au 02/02/2026",
             PC, ""),
        ],
    },
]

# 6.6 : indicateurs hors des tableaux, qu'aucune famille ne classait avant la critique du
# 08/11/2026 ; rangés depuis dans la partie 1 de T50-T57, avec ce statut.
SANS_STATUT = [
    ("Indicateur 1, l. 1075 à 1084",
     "« La proportion des clients satisfaits des services rendus » ; biennal ; par DR et par "
     "type de prestation ; DES, DCP, DR ; « Données non disponibles en base »",
     "Une enquête de satisfaction, pas un calcul sur la base",
     "T50-T57 (partie 1) : le recueil dit lui-même que la donnée n'est pas dans la base",
     HBA, ""),
    ("Indicateur ASS, l. 1297 à 1304",
     "« La proportion des clients pris en charge selon les normes et protocole en vigueur en "
     "matière sanitaire » ; annuel ; par SMI ; DP",
     "SMI : les centres de santé maternelle et infantile de la CNSS",
     "T50-T57 (partie 1), proposition BI : l'application des prestations ne suit pas les soins "
     "des centres (l'annuaire CNSS 2024 publie leur activité à part, tableaux 115 à 117)",
     HBA, ""),
    ("Indicateur ASS, l. 1308 à 1316",
     "« Le taux de réussite au CQP » (certificat de qualification professionnelle) ; « Source : "
     "Excel » ; annuel ; par DR (Ouaga, Bobo)",
     "Les résultats d'une formation suivie par la CNSS, tenus dans un fichier Excel",
     "T50-T57 (partie 1) : le recueil donne lui-même un fichier Excel pour source",
     HBA, ""),
    ("Nouvelles prestations, l. 1340 à 1356",
     "« Proportion de nouvelles prestations promues » ; « A confirmer avec DAI et DPC » ; "
     "annuel ; DCP",
     "La CNSS ne sait pas encore elle-même ce que l'indicateur compte",
     "T50-T57 (partie 1) : rien à calculer tant que la CNSS ne l'a pas défini",
     QU, "T50-T57 Q24"),
    ("Réclamations, l. 1358 à 1368",
     "« La variation du nombre de réclamations (plainte & réclamations) » ; « Source : Excel "
     "(les DR) » ; trimestriel, annuel ; par DR et agence",
     "Le nombre de réclamations d'une période moins celui de la période d'avant, tiré des "
     "fichiers Excel des DR",
     "T50-T57 (partie 1) : le recueil donne lui-même les fichiers Excel des DR pour source",
     HBA, ""),
    ("Document de référence, l. 80 et 81",
     "« Document de référence des tableaux de la CNSS »",
     "Un document annoncé, ni nommé ni joint",
     "T50-T57 (partie 1) : à obtenir de la CNSS ; il vaut pour toutes les familles",
     QU, "T50-T57 Q25"),
]

# 7 : les tableaux sans aucune ligne dans le recueil.
SANS_LIGNE = [
    ("T26 (dossiers AT/MP)",
     "Liste du domaine Prestation, l. 115 à 335 : aucune ligne pour le tableau 26",
     "Rien sur sa maquette. Ses durées et son taux restant à traiter sont pourtant définis par "
     "les indicateurs AT/MP (l. 1146 à 1188)",
     "La requête t26 et la fiche T26 suivent l'Annuaire CIPRES et les indicateurs AT/MP ; "
     "règles de T25 et T27",
     QU, "T25-T27 Q12"),
    ("T39 et T40 (rentes d'orphelins et d'ascendants)",
     "Aucune ligne dans le recueil, ni dans le tableau 1 de Kaya 2017",
     "Seule la maquette de l'Annuaire CIPRES fait foi",
     "T32-T41 (ligne de la partie 1) : la BI leur applique les règles de T38 (« rentiers », "
     "sortie au mois, au trimestre, à l'année)",
     QU, "T32-T41 Q13"),
    ("T56 (prestations servies par branche)",
     "Liste du domaine Prestation, l. 115 à 335 : aucune ligne pour le tableau 56",
     "Rien sur T56 ; l'annuaire CNSS 2024 publie déjà son équivalent (tableau 106)",
     "Requête 2 de t50_t57 faite d'après l'Annuaire CIPRES",
     QU, "T50-T57 Q21"),
]

# 8 : les questions ouvertes, par direction, sans doublon.
# (n°, question, à qui, en attendant, références et « déjà posée »)
QUESTIONS = [
    {
        "titre": "8.1 À la DCP (Direction centrale des prestations)",
        "questions": [
            ("DCP 1", "Le tableau de l'année doit-il aussi compter les dossiers déposés les "
             "années d'avant (liquidés dans l'année, ou encore en attente au 31/12), ou "
             "seulement ceux déposés dans l'année ?",
             "DCP, pour T25 à T27 (DES et DR pour T27)",
             "chaque étape dans son année ; en instance = tout ce qui attend au 31/12 "
             "(`lecture_periode` = STOCK)",
             "T25-T27 Q1 · déjà posée : README ; points_a_valider n° 14"),
            ("DCP 2", "Au bout de combien de mois une demande restée sans suite doit-elle sortir "
             "des dossiers en instance (ou faut-il la garder sans limite) ?",
             "DCP (pensions) ; DP et DR (AT/MP) ; DES et DR (PF)",
             "`debut_stock` = 01/01/2016 partout ; `sans_suite_mois` = 24 en AT/MP et en PF ; "
             "aucune limite pour les pensions",
             "T25-T27 Q2 · déjà posée : fiches T25 Q3, T26 Q1, T27 Q3 ; points_a_valider n° 21 "
             "et 64"),
            ("DCP 3", "Un dossier de coordination, payé par une autre caisse mais reçu et étudié "
             "par la CNSS, compte-t-il dans T25 à T27 ?", "DCP",
             "compté (`coordination` = GARDER)",
             "T25-T27 Q4 · déjà posée : fiche T25 Q1 ; points_a_valider n° 19 ; README"),
            ("DCP 4", "Tous les refus de dossier sont-ils saisis dans le module des rejets de "
             "l'application ?", "DCP, avec la DSI",
             "seuls les refus du module comptent ; un refus non saisi reste en instance",
             "T25-T27 Q5 · déjà posée : fiches T25 Q5, T27 Q4 ; points_a_valider n° 22 ; README "
             "(DSI)"),
            ("DCP 5", "Pour la CIPRES, faut-il aussi garder la maquette d'origine des tableaux "
             "que le recueil change (T25, T27, T28 à T30, T57) ?", "DCP, avec la DPC",
             "la version du recueil (et de Kaya 2017) ; la maquette d'origine se refait en "
             "additionnant les lignes",
             "T25-T27 Q22 ; T28-T31 Q13 ; T50-T57, point d'attention 16 (pour T57)"),
            ("DCP 6", "Pour les indicateurs de délai, mesure-t-on seulement la première demande "
             "(nouvel allocataire, première demande de rente, première demande de pension), "
             "comme votre feuille 03 ?", "DCP, DES, DP",
             "toutes les demandes liquidées (`premiere_demande` = TOUTES)",
             "T25-T27 Q18 · nouvelle"),
            ("DCP 7", "L'« achat de rente » ajouté à T29 par l'atelier de Kaya est-il le rachat "
             "de rente (un capital versé à la place de la rente) ?", "DCP, avec les DR",
             "oui : la ligne compte les dossiers qui ont reçu un rachat de rente",
             "T28-T31 Q11 · nouvelle (voisine de DP 6)"),
            ("DCP 8", "Dans T30, la ligne « Autres prestations de la branche » devient-elle "
             "« Frais médicaux de maternité », comme dans T27 ?", "DCP, avec la DES",
             "oui (T28-T31 N3) ; ligne vide, faute de ces frais dans la base",
             "T28-T31 Q12 · nouvelle pour T30"),
            ("DCP 9", "Un trop-perçu se compte-t-il le mois où il est établi ?", "DCP",
             "oui, le mois où il est établi",
             "T28-T31 Q17 · déjà posée : fiche T31 Q2 ; README"),
            ("DCP 10", "Une pension versée après un décès déclaré en retard devient-elle un "
             "trop-perçu ?", "DCP", "on ne compte que les trop-perçus enregistrés",
             "T28-T31 Q18 · déjà posée : fiche T31 Q3 ; README ; points_a_valider n° 30"),
            ("DCP 11", "Comment voit-on qu'un trop-perçu a été remboursé ?", "DCP, avec la DSI",
             "le remboursement n'entre pas dans T31 (`rem_tp_deduit` = NON)",
             "T28-T31 Q19 · déjà posée : fiche T31 Q5 ; points_a_valider n° 31"),
            ("DCP 12", "Pourquoi les moins-perçus des prestations familiales ont-ils presque "
             "disparu (56 362 en 2023, 1 384 en 2025) ?", "DCP",
             "on les compte tels qu'ils sont",
             "T28-T31 Q20 · déjà posée : README ; points_a_valider n° 32"),
            ("DCP 13", "Pourquoi les trop-perçus sur convention entre caisses passent-ils de 15 "
             "en 2024 à 758 en 2025 ?", "DCP",
             "comptés comme les autres trop-perçus, colonne PVID",
             "T28-T31 Q21 · nouvelle (le constat est au README, lot 9)"),
            ("DCP 14", "Les « prestations payées » de T31 doivent-elles égaler les prestations "
             "servies de votre annuaire, soins des accidents du travail, rachats de rente et "
             "frais médicaux de maternité compris ?", "DCP, avec la DPC",
             "les paiements ordinaires seulement (`payees_types` = PM)",
             "T28-T31 Q22 · déjà posée : points_a_valider n° 52"),
            ("DCP 15", "Les rappels de moins-perçus (et, pour T31, les avances), versés par un "
             "ajustement et non par un paiement ordinaire, sont-ils des prestations payées, à "
             "compter dans T31 et dans T56 ?", "DCP, avec la DAI et la DPC",
             "non, ni dans T31 ni dans T56 (dans T56, seuls les rachats de rente entrent : "
             "T50-T57 N4)",
             "T28-T31 Q23 ; T50-T57 Q19 · déjà posée : fiche T31 Q4 ; fiche T50-T57 Q3 ; "
             "points_a_valider n° 33 et 52"),
            ("DCP 16", "Le recueil demande les taux d'indus par type de prestation, mais un "
             "trop-perçu n'est relié à aucune prestation dans la base : la branche suffit-elle ?",
             "DCP, avec la DSI", "par branche seulement", "T28-T31 Q27 · nouvelle"),
            ("DCP 17", "Le montant d'un tableau des bénéficiaires est-il ce qui a été payé dans "
             "l'année aux personnes comptées au 31/12, ou tout ce qui a été payé dans l'année, "
             "y compris aux personnes sorties ?", "DCP (pensions) ; DES et DR (T41) ; DP et DR "
             "(rentes)", "payé aux personnes comptées (`montant` = COMPTES)",
             "T32-T41 Q1 · déjà posée : points_a_valider n° 35 ; fiches T32 Q3, T41 Q3 ; README"),
            ("DCP 18", "Les décès des veuves pensionnées, ou l'arrêt de leur pension, sont-ils "
             "souvent enregistrés plusieurs mois après ?", "DCP, avec les DR de Bobo-Dioulasso "
             "et de Ouagadougou", "R6 inchangée ; R13 proposée",
             "T32-T41 Q4 · déjà posée : points_a_valider n° 37 ; README ; requêtes A06, A07"),
            ("DCP 19", "Pour un tableau du mois ou du trimestre, le montant est-il celui de la "
             "période seule, ou le cumul depuis le 1er janvier ?", "DCP, DES, DP",
             "le montant de la période ; l'année est la somme des mois (`montant_periode` = "
             "PERIODE)", "T32-T41 Q7 · nouvelle"),
            ("DCP 20", "En attendant le module de conversion, gardez-vous dans T33 les invalides "
             "qui ont passé l'âge de la retraite, ou faut-il les montrer à part ?", "DCP",
             "gardés dans T33 (`invalides_age_retraite` = DANS_T33)",
             "T32-T41 Q15 · nouvelle (le module : recueil l. 270)"),
            ("DCP 21", "Faut-il garder dans la pension moyenne les pensions de coordination que "
             "paie l'autre caisse (la CNSS ne leur verse rien) ?", "DCP",
             "oui : montant ÷ effectif total (`moyenne_coordination` = TOUTES)",
             "T32-T41 Q16 · déjà posée : points_a_valider n° 36 ; fiche T32 Q5"),
            ("DCP 22", "Pour le rapport démographique, une pension de coordination payée par "
             "l'autre caisse compte-t-elle comme un retraité de la CNSS ?", "DCP",
             "oui (`coordination_retraites` = AVEC)", "T32-T41 Q17 · nouvelle"),
            ("DCP 23", "Le taux de remplacement porte-t-il sur toutes les pensions directes en "
             "cours, ou seulement sur les nouvelles pensions de l'année ?", "DCP, avec la DES",
             "toutes les pensions directes en cours (`remplacement_population` = STOCK) ; "
             "l'indicateur n'est pas encore calculé (il attend aussi DSI 8)",
             "T32-T41 Q18 ; T42-T49 Q19 · nouvelle (remarque B37 du recueil)"),
            ("DCP 24", "Un contrôle d'existence est-il prévu pour les pensionnés très âgés et "
             "pour les pensions encore actives dont le titulaire est décédé ?", "DCP",
             "comptés tant que la pension est en cours et que la personne n'est pas déclarée "
             "décédée", "T32-T41 Q22 · déjà posée : points_a_valider n° 43 ; fiche T32 Q4"),
            ("DCP 25", "Une pension dont le dossier a été transféré à une autre caisse "
             "compte-t-elle encore parmi vos bénéficiaires au 31/12 ?", "DCP, avec la DSI",
             "comptée (`dossier_transfere` = GARDER)", "T32-T41 Q23 · nouvelle"),
            ("DCP 26", "Pour un orphelin, une veuve ou un parent, la personne à compter est-elle "
             "bien l'ayant droit lui-même, et non l'assuré décédé titulaire du dossier ?",
             "DCP, avec les auteurs du recueil",
             "on compte l'ayant droit (R5) ; « Idem. Tab 41 » est lu « même périodicité »",
             "T42-T49 Q1 (vaut pour T32 à T49 ; T32-T41, ligne de la partie 1 et point "
             "d'attention 1) · nouvelle (remarque B05 du recueil)"),
            ("DCP 27", "Un nouveau pensionné ou rentier compte-t-il l'année où sa prestation est "
             "saisie, ou l'année où la décision lui est notifiée ?", "DCP",
             "la saisie (`date_entree` = 'SAISIE'), comme l'annuaire CNSS 2024",
             "T42-T49 Q2 · déjà posée : points_a_valider n° 4 ; fiche T42 Q1 ; README R7"),
            ("DCP 28", "Le montant d'un nouveau pensionné comprend-il le rappel des mois d'avant "
             "l'année, payé avec sa première pension ?", "DCP",
             "les mois de l'année seulement (`date_montant` = 'MOIS') ; le rappel dans une "
             "colonne à part (T42-T49 N8)",
             "T42-T49 Q3 · posée au point du 02/11/2026, en attente ; points_a_valider n° 1 et "
             "45 ; fiche T42 Q4 ; README R4 ; voisine de DPC 1"),
            ("DCP 29", "Dans un tableau mensuel ou trimestriel des nouveaux, quel montant "
             "voulez-vous : ce qui leur a été payé dans la période, ou le montant mensuel de "
             "leur pension ou de leur rente ?", "DCP",
             "ce qui est payé pour les mois de la période (T42-T49 N3)",
             "T42-T49 Q4 · nouvelle"),
            ("DCP 30", "Pour une pension de coordination, la colonne `PE_COT_AUTRE` donne-t-elle "
             "le nombre de mois cotisés dans l'autre caisse, déjà compris dans le total des "
             "mois ?", "DCP, à défaut la DSI",
             "services passés = total des mois moins `PE_COT_AUTRE`, pour les pensions de "
             "coordination",
             "T42-T49 Q10 · déjà posée : points_a_valider n° 11 ; fiche T42 Q5 ; README"),
            ("DCP 31", "Pour T42, la « partie CNSS » d'une pension de coordination vaut-elle pour "
             "les mois de travail, pour le montant, ou pour les deux ?", "DCP",
             "pour les deux : mois sans l'autre caisse (R9), montant payé par la CNSS (N6)",
             "T42-T49 Q11 · nouvelle"),
            ("DCP 32", "Quand la CNSS paie elle-même une pension de coordination, son paiement "
             "comprend-il la part de l'autre caisse ?", "DCP",
             "le montant est ce que la CNSS a payé (T42-T49 N6)", "T42-T49 Q12 · nouvelle"),
            ("DCP 33", "La retraite anticipée compte-t-elle dans T42, avec les nouveaux retraités "
             "de vieillesse ?", "DCP", "oui (`retraite_anticipee` = DANS_T42)",
             "T42-T49 Q13 · déjà posée : points_a_valider n° 18 et 34 ; fiche T32 Q2"),
            ("DCP 34", "Un orphelin de 25 ans ou plus peut-il recevoir une nouvelle pension "
             "d'orphelin ?", "DCP",
             "compté dans « 20 ans et plus » de T44 ou de T46 (`orphelin_age_max` : aucune "
             "limite)", "T42-T49 Q14 · liée à points_a_valider n° 10"),
            ("DCP 35", "Pourquoi 220 retraités qui avaient déjà une pension en ont-ils reçu une "
             "nouvelle en 2025 (47 en 2024) ?", "DCP, avec la DSI",
             "ils ne sont pas comptés comme nouveaux (R7)",
             "T42-T49 Q17 · déjà posée : fiche T42 Q2"),
            ("DCP 36", "Quand une pension d'invalidité sera transformée en pension de vieillesse "
             "à l'âge de la retraite, l'assuré comptera-t-il comme nouveau retraité dans T42 ?",
             "DCP, et la DSI qui doit développer le module",
             "sans objet aujourd'hui ; avec R7, il serait compté l'année de la conversion",
             "T42-T49 Q18 · nouvelle (remarque B31 du recueil)"),
            ("DCP 37", "Pour T43, faut-il garder la dernière tranche « 60 ans et plus », ou la "
             "détailler (60-64, 65-69…) ?", "DCP, avec la DPC",
             "la maquette telle quelle (question facultative)", "T42-T49 Q21 · nouvelle"),
            ("DCP 38", "Pour T54 et T55, le « montant liquidé » est-il la somme due pour les "
             "indemnités et allocations liquidées dans l'année, même payée l'année suivante ?",
             "DCP",
             "oui (`montant_t54_t55` = 'LIQUIDE') dès que la requête est revue ; aujourd'hui, "
             "elle prend les paiements de l'année",
             "T50-T57 Q3 · nouvelle (remarque B15 du recueil)"),
            ("DCP 39", "Un assuré mort en activité, dont le décès ouvre une pension ou une "
             "allocation de survivants, compte-t-il dans T57 (branche AVS, qualité "
             "« assuré ») ?", "DCP, avec la DPC",
             "non (`actifs_decedes` = NON) : seuls les titulaires d'une pension directe comptent "
             "en AVS", "T50-T57 Q4 · déjà posée : README"),
            ("DCP 40", "Pour T51, T52 et T53, compte-t-on aussi les personnes dont la prestation "
             "est liquidée dans l'année, comme pour T50 et T54 ?", "DCP",
             "oui : la personne qui a une prestation du tableau notifiée dans l'année",
             "T50-T57 Q8 · déjà posée : README ; points_a_valider n° 49"),
            ("DCP 41", "Au-delà de combien de mois cotisés un assuré a-t-il droit à une pension "
             "plutôt qu'à une allocation de vieillesse ?", "DCP",
             "tous les mois de 1 à 600 entrent dans la moyenne de T50 (`mois_cotises_max` = 600)",
             "T50-T57 Q9 · déjà posée : README ; points_a_valider n° 62"),
            ("DCP 42", "Avec le filtre « ayant droit » de T57, faut-il compter dans les PF les "
             "conjoints et les enfants d'allocataires décédés ?", "DCP, avec la DPC",
             "PF = allocataires seulement (titulaires du dossier)",
             "T50-T57 Q10 · remarque B26 du recueil"),
            ("DCP 43", "Un sinistré décédé est-il seulement la victime morte sur le coup (vos "
             "« décès immédiats »), ou aussi celle qui meurt de son accident des mois après ?",
             "DCP (service AT/MP)",
             "toute victime morte dans l'année qui suit son accident (`delai_sinistre_jours` = "
             "366)", "T50-T57 Q12 · déjà posée : points_a_valider n° 56 ; README"),
            ("DCP 44", "L'épouse non salariée d'un assuré reçoit-elle l'allocation prénatale sous "
             "l'ancien code des PF (FAP) plutôt que sous celui de la maternité (MAP) ?", "DCP",
             "T55 compte les deux codes : la réponse ne change pas le tableau",
             "T50-T57 Q22 · nouvelle"),
            ("DCP 45", "Que doit compter l'indicateur « proportion de nouvelles prestations "
             "promues » : quelles prestations sont « nouvelles », et rapportées à quoi ?",
             "DCP, avec la DAI et la DPC", "l'indicateur n'est pas calculé",
             "T50-T57 Q24 · nouvelle (le recueil, l. 1340 à 1356, la laisse ouverte)"),
        ],
    },
    {
        "titre": "8.2 À la DP et aux directions régionales (accidents du travail et maladies "
                 "professionnelles)",
        "questions": [
            ("DP 1", "Une déclaration d'accident qui n'a donné ni arrêt de travail, ni soins, ni "
             "rente compte-t-elle comme un dossier déposé, en attente de traitement ?",
             "DP et DR", "comptée dans les déposés (Total) ; en instance 24 mois au plus",
             "T25-T27 Q3 · déjà posée : fiche T26 Q1 ; points_a_valider n° 23"),
            ("DP 2", "Les remboursements de soins des accidents du travail entrent-ils dans T26, "
             "sur la ligne « Autres prestations de la branche » ?", "DP et DR",
             "oui : un dossier soigné = une demande, liquidée à la saisie du premier "
             "remboursement",
             "T25-T27 Q7 · déjà posée : fiche T26 Q2 ; points_a_valider n° 24"),
            ("DP 3", "La durée d'une rente viagère part-elle de la réception du certificat de "
             "consolidation, ou de la demande déposée au guichet ?", "DP et DR",
             "la demande au guichet (`depart_rente` = GUICHET, exception provisoire au recueil)",
             "T25-T27 Q8 · déjà posée : fiche T26 Q3 ; points_a_valider n° 25 ; remarque B16 "
             "du recueil"),
            ("DP 4", "Le délai des « rentes de survivants » du recueil part du certificat de "
             "consolidation, qui concerne la victime : vise-t-il en fait la rente de la "
             "victime elle-même (rente viagère) ?", "DP et DR",
             "une rente de survivant part de la demande du survivant au guichet",
             "T25-T27 Q9 · remarque B16 du recueil"),
            ("DP 5", "Entre la fin de l'arrêt de travail et la notification de l'indemnité "
             "journalière d'accident, quelle pièce ou quelle étape attend-on ?", "DP et DR",
             "durée comptée depuis la déclaration, publiée telle quelle",
             "T25-T27 Q10 · déjà posée : fiche T26 Q4 ; points_a_valider n° 26"),
            ("DP 6", "Un rachat de rente d'accident du travail est-il un dossier liquidé à "
             "compter dans T26 ?", "DP et DR", "non (`rachat_rente_t26` = NON)",
             "T25-T27 Q11 · nouvelle (voisine de DCP 7 et de points_a_valider n° 52)"),
            ("DP 7", "T26, T39 et T40 n'ont aucune ligne dans le recueil : les produit-on comme "
             "l'Annuaire CIPRES les décrit, avec les règles des tableaux voisins (T25 et T27 "
             "pour T26 ; T38 pour T39 et T40 : « rentiers », sortie au mois, au trimestre, à "
             "l'année) ?", "DP et DR, avec la DCP", "oui",
             "T25-T27 Q12 ; T32-T41 Q13 · remarque B38 du recueil"),
            ("DP 8", "Jusqu'à quel âge une rente d'orphelin d'accident du travail est-elle "
             "versée ?", "DP et DR",
             "comme les pensions, moins de 25 ans (`age_max_orphelin` = 25)",
             "T32-T41 Q12 · nouvelle"),
            ("DP 9", "Pour T38, « le cumul sans préciser un exercice » veut-il dire tous les "
             "rentiers en cours à la date choisie, quelle que soit l'année où leur rente a "
             "commencé ?", "DP et DR", "oui, un stock à la date d'arrêté (R6)",
             "T32-T41 Q14 · remarque B28 du recueil"),
            ("DP 10", "Comment comptez-vous vos 44 nouvelles rentes viagères de 2024 ?",
             "DP et DR, avec la DPC",
             "la première rente viagère de la personne (39 en 2024) ; à poser après le lot 10",
             "T42-T49 Q6 · déjà posée : points_a_valider n° 48 ; README"),
            ("DP 11", "Votre tableau 77 compte 13 nouveaux ascendants de rente en 2024, la base "
             "en a 15 : comment les comptez-vous ?", "DP et DR, avec la DPC",
             "les 15 sont comptés", "T42-T49 Q7 · nouvelle"),
            ("DP 12", "Pour le degré d'IPP de T48, faut-il prendre le taux de la victime à son "
             "entrée, ou son dernier taux de l'année ?", "DP et DR",
             "le dernier taux de l'année (`taux_ipp` = DERNIER)", "T42-T49 Q15 · nouvelle"),
        ],
    },
    {
        "titre": "8.3 À la DES et aux directions régionales (prestations familiales)",
        "questions": [
            ("DES 1", "Quand un allocataire déclare un nouvel enfant, est-ce un dossier déposé et "
             "liquidé de plus dans T27 ?", "DES et DR", "oui (`enfant_ajoute` = 1)",
             "T25-T27 Q13 · déjà posée : fiche T27 Q1 ; points_a_valider n° 27"),
            ("DES 2", "Une grossesse compte-t-elle sur les deux lignes de T27 (IJ de maternité et "
             "allocations prénatales), ou une seule fois ?", "DES et DR",
             "deux lignes dans T27 ; une seule (l'IJ) dans l'indicateur de délai",
             "T25-T27 Q14 · déjà posée : fiche T27 Q2 ; points_a_valider n° 28"),
            ("DES 3", "Un enfant dont l'allocation court au 31/12, mais qui n'a rien touché dans "
             "l'année, compte-t-il dans T41 ?", "DES et DR",
             "non : seuls les enfants payés comptent (`t41_paiement` = 12_MOIS)",
             "T32-T41 Q5 · déjà posée : points_a_valider n° 38 ; fiche T41 Q1"),
            ("DES 4", "Pour T41 à la fin d'un mois ou d'un trimestre, un enfant compte-t-il s'il "
             "a été payé dans les 12 mois qui précèdent ?", "DES et DR",
             "oui, 12 mois avant la date d'arrêté (T32-T41 N10)", "T32-T41 Q6 · nouvelle"),
            ("DES 5", "Si le titulaire d'un dossier d'allocations familiales est décédé et que "
             "ses enfants touchent encore, qui est l'allocataire dans T41 ?", "DES et DR",
             "le titulaire, compté à l'âge qu'il aurait", "T32-T41 Q8 · nouvelle"),
        ],
    },
    {
        "titre": "8.4 À la DAI (Direction de l'audit interne) et à l'auteur de la grille des "
                 "ratios",
        "questions": [
            ("DAI 1", "Depuis juillet 2025, où sont enregistrés les contrôles a posteriori des "
             "dossiers de prestations ?", "DAI, avec la DSI",
             "T28 à T30 ne sont pas produits",
             "T28-T31 Q1 · déjà posée : README ; points_a_valider n° 29 ; planche 02 « vues "
             "cnss » ; carte C1"),
            ("DAI 2", "La table des contrôles de l'application a-t-elle un jour contenu tous vos "
             "contrôles ?", "DAI", "la table ne sert pas, même pour le passé",
             "T28-T31 Q2 · nouvelle"),
            ("DAI 3", "Pouvez-vous nous donner vos contrôles des cinq derniers exercices, un par "
             "ligne : le dossier, la prestation contrôlée, la date, le résultat et le montant "
             "de l'indu trouvé ?", "DAI, avec la DSI",
             "rien : la vue `V_BI_CONTROLE_POSTERIORI` est seulement proposée",
             "T28-T31 Q3 · planche 02 « vues cnss » (07/11/2026)"),
            ("DAI 4", "Un contrôle porte-t-il sur tout le dossier, ou sur une seule prestation "
             "du dossier ?", "DAI", "la ligne du tableau est celle de la prestation contrôlée",
             "T28-T31 Q4 · planche 02 « vues cnss »"),
            ("DAI 5", "Quels résultats peut avoir un contrôle (conforme, anomalie, en cours…) ?",
             "DAI", "tout contrôle enregistré compte, quel que soit son résultat",
             "T28-T31 Q5 · planche 02 « vues cnss »"),
            ("DAI 6", "Pour le taux de contrôle, divise-t-on par les dossiers payés dans la "
             "période, ou par les dossiers « liquidés ou en portefeuille » de votre grille ?",
             "DAI, avec la DCP et les DR",
             "les dossiers payés (`denominateur_controle` = 'PAYES')", "T28-T31 Q6 · nouvelle"),
            ("DAI 7", "Pour les prestations versées une fois ou sur quelques mois (indemnités "
             "journalières, allocations prénatales, allocations de vieillesse), le dossier payé "
             "de T28 à T30 est-il celui payé dans la période, ou celui liquidé dans la "
             "période ?", "DAI, avec la DCP",
             "payé dans la période (`dossier_paye` = 'PAIEMENT')", "T28-T31 Q7 · nouvelle"),
            ("DAI 8", "Quand on produit T28 à T30 pour un trimestre ou un semestre, compte-t-on "
             "les dossiers payés depuis le 1er janvier ?", "DAI, avec la DCP",
             "oui, depuis le 1er janvier ; les contrôles, eux, par trimestre",
             "T28-T31 Q8 · nouvelle"),
            ("DAI 9", "Dans le taux de contrôle, un dossier contrôlé deux fois dans l'année "
             "compte-t-il deux fois ?", "DAI",
             "une fois dans le taux ; deux fois dans la colonne TOTAL (`controle_compte` = "
             "'DOSSIERS')", "T28-T31 Q9 · nouvelle"),
            ("DAI 10", "Qui peut voir, dans la BI, le résultat des contrôles et le montant d'indu "
             "trouvé pour chaque dossier ?", "DAI, avec la DSI",
             "seuls les taux sont diffusés ; le détail reste réservé (T28-T31 N14)",
             "T28-T31 Q10 · nouvelle"),
            ("DAI 11", "Le tableau des indus doit-il prendre tous les trop-perçus enregistrés "
             "dans l'application, ou seulement ceux trouvés par vos contrôles ?",
             "DAI, avec la DCP",
             "tous les trop-perçus de l'application (`source_indus` = 'BASE')",
             "T28-T31 Q14 · planche 02 « vues cnss », question 4 ; remarque B13 du recueil"),
            ("DAI 12", "Le taux d'indus se calcule-t-il sur toutes les prestations payées de la "
             "période, ou sur le seul montant des dossiers contrôlés ?", "DAI",
             "sur toutes les prestations payées (`taux_denominateur` = 'PAYEES'), formule du "
             "recueil", "T28-T31 Q15 · nouvelle"),
            ("DAI 13", "Les moins-perçus (sommes payées en moins, rattrapées par un rappel) "
             "entrent-ils dans le taux d'indus ?", "DAI, avec la DCP",
             "non : ils sont montrés à part, avec le taux des deux ensemble à côté",
             "T28-T31 Q16 · déjà posée : fiche T31 Q1 ; README ; points_a_valider n° 17"),
            ("DAI 14", "Les taux de contrôle et d'indus s'affichent-ils avec 2 chiffres après la "
             "virgule, comme le tableau 76 ?", "DAI", "3 chiffres (`decimales` = 3)",
             "T28-T31 Q30 · nouvelle (remarque B35 du recueil)"),
            ("DAI 15", "Le ratio des ressources de cotisations allouées aux prestations "
             "compte-t-il aussi les soins (prestations en nature), ou seulement les prestations "
             "en espèces ?", "DAI, avec la DPC",
             "soins compris, comme T56 (`nature_t56` = OUI) ; prestations datées à la "
             "notification", "T50-T57 Q20 · nouvelle"),
            ("DAI 16", "Pour le taux de dossiers restant à traiter, quelle formule retenir : "
             "celle du recueil (en instance à la fin ÷ reçus dans l'année) ou celle de votre "
             "feuille 04 (en instance à la fin ÷ (en instance au début + reçus − rejetés)) ?",
             "l'auteur de la grille des ratios, à identifier (DAI, DPCG ou DPC) ; en attendant, "
             "la DCP",
             "la formule du recueil (`formule_taux` = RECUEIL), norme 15 %",
             "T25-T27 Q19 · déjà posée : fiche T25, règle 10 ; points_a_valider n° 15"),
        ],
    },
    {
        "titre": "8.5 À la DSI (Direction des systèmes d'information)",
        "questions": [
            ("DSI 1", "Une réception de pension au statut « A » (un code dont le sens n'est écrit "
             "nulle part), sans dossier, est-elle en attente ou annulée ?", "DSI",
             "comptée en instance (`reception_statut_a` = EN_INSTANCE)",
             "T25-T27 Q6 · déjà posée : README ; points_a_valider n° 58"),
            ("DSI 2", "Où sont enregistrés les frais médicaux de maternité que la CNSS paie "
             "(25,0 millions FCFA en 2024) ?", "DSI, avec la DPC et la DFC",
             "ligne « Frais médicaux de maternité » de T27 vide ; absents de la ligne PF de T56",
             "T25-T27 Q15 ; T50-T57 Q17 · nouvelle (remarque B27 du recueil)"),
            ("DSI 3", "Quelle date de l'application est la « date de validation » d'un dossier de "
             "prestations familiales ?", "DSI, avec la DES",
             "la notification des droits (`fin_duree_pf` = NOTIFICATION)",
             "T25-T27 Q16 · remarque B18 du recueil"),
            ("DSI 4", "Où sont enregistrées la date de réception du certificat du médecin "
             "traitant et la date de convocation, qui font le délai des remboursements de "
             "soins ?", "DSI, avec la DP",
             "pas d'indicateur ; T26 mesure les soins de la déclaration au premier remboursement",
             "T25-T27 Q17 · remarque B17 du recueil"),
            ("DSI 5", "L'agence d'un dossier est-elle bien son service dans l'application "
             "(`SP_NO`) ?", "DSI, avec la DPC",
             "agence = service du dossier, sinon celui de la réception (`agence` = SERVICE)",
             "T25-T27 Q20 ; T28-T31 Q28 ; T32-T41 Q20 · remarque B34 du recueil"),
            ("DSI 6", "Le statut R d'un ajustement veut-il dire « annulé » ?", "DSI",
             "écarté ; sans effet depuis 2015",
             "T28-T31 Q25 · déjà posée : fiche T31 Q5 ; points_a_valider n° 17 et 58"),
            ("DSI 7", "Parmi les paiements liés à un trop-perçu, lesquels sont des retenues, "
             "lesquels des versements ?", "DSI",
             "T31 lit le montant établi du trop-perçu, pas ces paiements",
             "T28-T31 Q26 · déjà posée : README ; points_a_valider n° 61"),
            ("DSI 8", "Le salaire moyen enregistré avec chaque pension (`PE_SAL_MOYEN`) est-il "
             "bien le salaire moyen des cinq meilleures années ?", "DSI, avec la DCP",
             "pas d'indicateur de taux de remplacement",
             "T32-T41 Q19 · nouvelle (remarque B37 du recueil)"),
            ("DSI 9", "Où sont enregistrés les paiements AT/MP absents de la base des "
             "prestations : une part des rentes (12 à 25 % en 2024) et des indemnités "
             "journalières, et la plupart des soins (139,7 millions FCFA publiés en 2024) ?", "DSI, avec la DFC et la DP",
             "les paiements de la base ; montants des rentes avec une réserve (« incomplet ») ; "
             "T56 AT/MP = paiements et remboursements de la base",
             "T32-T41 Q21 ; T42-T49 Q5 ; T50-T57 Q16 · déjà posée : README ; points_a_valider "
             "n° 51 ; fiche T50-T57 Q3"),
            ("DSI 10", "Quel champ de la base garde le montant calculé à la liquidation d'une "
             "prestation ?", "DSI",
             "montant liquidé = somme des paiements (statut D ou P) de la prestation, quelle "
             "que soit leur date", "T50-T57 Q7 · nouvelle"),
            ("DSI 11", "Où la base garde-t-elle les « décès immédiats » des accidents du travail "
             "pour les années d'avant 2025 ?", "DSI",
             "la règle de l'année qui suit l'accident",
             "T50-T57 Q13 · déjà posée : points_a_valider n° 66 (en partie) ; README"),
            ("DSI 12", "Peut-on retirer le nom, l'adresse et le téléphone de la vue "
             "`V_PRESTATAIRES_DECEDES_BI` ?", "DSI",
             "la BI ne lit pas cette vue", "T50-T57 Q23 · déjà posée : points_a_valider n° 66 ; "
             "README"),
        ],
    },
    {
        "titre": "8.6 À la DPC (Direction de la prospective et de la coopération, qui fait "
                 "l'annuaire)",
        "questions": [
            ("DPC 1", "Dans votre annuaire, un paiement est-il rangé dans le mois et l'année "
             "qu'il paie, dans le mois où il est saisi (rappels compris), ou dans le mois où il "
             "est versé ?", "DPC (Service études et statistique), avec la DCP",
             "le mois payé (`date_montant` = MOIS) ; T50-T57 propose de passer à la saisie",
             "T28-T31 Q24 ; T32-T41 Q2 ; T50-T57 Q1 (même formulation dans les trois familles "
             "depuis le 08/11) · déjà posée au point du 02/11/2026, sans "
             "réponse ; README R4 ; points_a_valider n° 1"),
            ("DPC 2", "Votre annuaire de l'année N est-il fait avec la base arrêtée vers la fin "
             "mars de l'année N+1 ?", "DPC (Service études et statistique)",
             "proposition R13 ; aujourd'hui, le cliché du 02/02/2026 ; prévoir de lancer le "
             "calcul de l'exercice N fin mars N+1",
             "T32-T41 Q3 ; T50-T57 Q2 · déjà posée : points_a_valider n° 2 ; README R13"),
            ("DPC 3", "Votre tableau 27 « départs à la retraite » (3 729 en 2024) compte-t-il "
             "les demandes de pension reçues au guichet ?", "DPC (Service études et "
             "statistique)",
             "pas de comparaison ; la BI compare tout de même les réceptions de pension de 2024 "
             "au tableau 27", "T25-T27 Q21 · nouvelle"),
            ("DPC 4", "Le montant des allocations familiales de votre annuaire (8,24 milliards "
             "FCFA en 2024) comprend-il des allocations validées mais pas encore versées ?",
             "DPC, avec la DES",
             "les allocations payées (statut D ou P) des enfants comptés",
             "T32-T41 Q9 · déjà posée : points_a_valider n° 40 ; fiche T41 Q3"),
            ("DPC 5", "Dans vos tableaux par âge (19, 25, 36), l'âge est-il l'année de "
             "l'exercice moins l'année de naissance, ou est-il calculé à une autre date ?",
             "DPC (Service études et statistique)",
             "année de l'exercice moins année de naissance (R8), comme le glossaire CIPRES",
             "T32-T41 Q10 ; T42-T49 Q9 · déjà posée : points_a_valider n° 44 ; fiche T42 Q3"),
            ("DPC 6", "Votre tableau des enfants d'allocataires (tableau 36) compte-t-il aussi "
             "les enfants sortis dans l'année parce qu'ils ont atteint 21 ans ?",
             "DPC, avec la DES", "enfants en cours au 31/12 et payés dans l'année",
             "T32-T41 Q11 · nouvelle"),
            ("DPC 7", "Comment votre tableau 25 obtient-il le sexe des nouveaux retraités (527 "
             "femmes publiées pour 2024, 641 dans la base) ?",
             "DPC (Service études et statistique)",
             "le sexe de la fiche de la personne",
             "T42-T49 Q8 · déjà posée : points_a_valider n° 44 ; fiche T42 Q3 ; README"),
            ("DPC 8", "Pour une période à cheval sur deux années, l'âge d'un nouveau se "
             "compte-t-il avec l'année de son entrée ?", "DPC, avec la DCP",
             "année de la saisie moins année de naissance (T42-T49 N2)",
             "T42-T49 Q16 · nouvelle"),
            ("DPC 9", "Comment calculez-vous le stock d'orphelins de 2024, qui monte alors que "
             "des orphelins atteignent 25 ans chaque année ?",
             "DPC (Service études et statistique)",
             "sans effet sur T44 et T46 ; à garder en tête pour T35 et T39",
             "T42-T49 Q20 · nouvelle"),
            ("DPC 10", "Pour T56, faut-il prendre les paiements de l'application des prestations, "
             "ou les charges techniques de la comptabilité (comptes 601, 602, 603, 605), comme "
             "votre tableau 106 ?", "DPC et DFC (la DSI pour l'accès)",
             "les paiements de l'application (`source_t56` = 'DEBOURS')",
             "T50-T57 Q5 · nouvelle"),
            ("DPC 11", "Dans un tableau sur cinq ans, les années passées reprennent-elles les "
             "chiffres déjà publiés ?", "DPC",
             "les années N−4 à N−1 sont recalculées sur le cliché lu",
             "T50-T57 Q6 · nouvelle"),
            ("DPC 12", "Dans T57, une personne comptée dans deux branches (un retraité aussi "
             "allocataire) compte-t-elle deux fois dans le TOTAL ?", "DPC",
             "oui : TOTAL = somme des branches", "T50-T57 Q11 · nouvelle"),
            ("DPC 13", "Pour vos 24 rentiers décédés de 2024 (tableau 81), la rente devait-elle "
             "être en cours au décès ?", "DPC, avec la DCP",
             "rente en cours au décès, radiée au plus tôt 31 jours avant "
             "(`marge_radiation_jours` = 31)", "T50-T57 Q14 · nouvelle"),
            ("DPC 14", "D'où viennent les 89 décès de pensionnés ascendants de 2024 (tableau 26), "
             "pour 59 pensions d'ascendant en cours ?", "DPC",
             "sans effet par défaut : avec le filtre « assuré », les ascendants ne comptent pas",
             "T50-T57 Q15 · déjà posée : README ; points_a_valider n° 57"),
            ("DPC 15", "Pour la ligne « Autres » de T56 (l'action sanitaire et sociale, comme "
             "votre tableau 106), quel montant prendre ?", "DPC, avec la DPASS et la DCG",
             "ligne vide (`ligne_autres_t56`)",
             "T50-T57 Q18 · déjà posée : points_a_valider n° 53"),
            ("DPC 16", "T56 n'a aucune ligne dans le recueil : reste-t-il à produire ?",
             "DPC (comité du projet)", "produit selon l'Annuaire CIPRES (requête 2)",
             "T50-T57 Q21 · remarque B38 du recueil"),
            ("DPC 17", "Le « document de référence des tableaux de la CNSS » annoncé par le "
             "recueil (l. 80 et 81) existe-t-il, et pouvez-vous nous le donner ?",
             "DPC (comité du projet)",
             "les sources restent le recueil v7, Kaya 2017, l'Annuaire CIPRES et l'annuaire "
             "CNSS 2024", "T50-T57 Q25 · nouvelle (vaut pour toutes les familles)"),
        ],
    },
    {
        "titre": "8.7 À la DCR (Direction centrale du recouvrement)",
        "questions": [
            ("DCR 1", "Le taux de prestations indues rangé aussi sous le recouvrement (recueil, "
             "l. 1608) est-il suivi par la DCR ou par la DAI ?", "DCR, avec la DAI",
             "un seul calcul, celui de la l. 1369", "T28-T31 Q29 · nouvelle"),
        ],
    },
]

# 9 : ce que la vérification a trouvé (signalé, rien n'est corrigé dans les familles).
SIGNALEMENTS = [
    ("Corrigé : deux statuts contraires",
     "Délai de liquidation des pensions (l. 1216 à 1238) et délai des rentes de survivants "
     "(l. 1170 à 1179) : T42-T49 disait « pris en compte » là où T25-T27, qui les traite, dit "
     "« à intégrer » et « question ». T42-T49 prend maintenant le statut de T25-T27 ; chaque "
     "demande n'a plus ici qu'une ligne."),
    ("Corrigé : six demandes sans statut",
     "Satisfaction, normes sanitaires des SMI, CQP, réclamations (hors base), nouvelles "
     "prestations et document de référence (questions T50-T57 Q24 et Q25, nouvelles) : ils "
     "sont rangés dans la partie 1 de T50-T57 ; les autres familles y renvoient."),
    ("Corrigé : T39 et T40 sans ligne",
     "T32-T41 leur donne une ligne de la partie 1 (question Q13), comme T25-T27 pour T26 et "
     "T50-T57 pour T56."),
    ("Corrigé : une règle qui dépend d'une autre famille",
     "La l. 298 reprise pour T50 (T50-T57 R9, retrait de `PE_COT_AUTRE`) passe de « pris en "
     "compte » à « question (T42-T49 Q10) », le statut de la même phrase pour T42."),
    ("Corrigé : « Idem. Tab 41 » dans T32 à T38",
     "T32-T41 a maintenant une ligne « allocataire = titulaire du dossier » avec le statut "
     "« question (T42-T49 Q1) », comme T42-T49 : la question est posée une seule fois pour "
     "T32 à T49."),
    ("Corrigé : des questions liées pas citées sur la ligne",
     "l. 127 (T25-T27 Q15) ; l. 1104, 1180, 1239 (T25-T27 Q19) ; l. 166 (T50-T57 Q3) ; "
     "l. 152 (T32-T41 Q8) ; l. 72 (T28-T31 Q10) : la ligne du document de famille cite "
     "maintenant sa question, et la colonne « Question liée » la reprend."),
    ("Corrigé : une même question posée de trois façons",
     "La date d'un paiement : T32-T41 Q2 et T50-T57 Q1 prennent la formulation de T28-T31 Q24 "
     "(le mois payé, la saisie ou le versement) et disent qu'elles en sont la reprise (DPC 1). "
     "Les autres reprises (DPC 2, DPC 5, DCP 15, DCP 23, DSI 9) le disent aussi."),
    ("Reste, justifié : une même valeur par défaut, deux propositions",
     "Pour la date d'un paiement, toutes les familles gardent le mois payé (`date_montant` = "
     "MOIS) ; T50-T57 propose en plus de passer à la saisie. Ce réglage est dans quatre "
     "requêtes et se décide une fois pour tout le domaine (DPC 1)."),
    ("Reste, justifié : une question avec deux « En attendant »",
     "Taux de remplacement (DCP 23) : T32-T41 Q18 retient un réglage (toutes les pensions en "
     "cours), T42-T49 Q19 dit « pas calculé ». Les deux sont vrais : le réglage existe, "
     "l'indicateur attend le salaire moyen (DSI 8)."),
    ("Reste, justifié : des statuts différents selon le tableau",
     "Une même ligne vaut pour des tableaux différents : l. 127 (T27, T30, T56), l. 142 à 144, "
     "l. 152, l. 194, l. 248 et 255, l. 283, l. 298, l. 326, l. 333, l. 1102, l. 1129 à 1141. "
     "Les lignes générales l. 28, 34, 75 ; l. 35 ; l. 53 à 60 ; l. 72 changent selon la "
     "famille. Chaque statut est sur sa ligne."),
    ("Écartées : trois demandes du relevé",
     "Le relevé ligne à ligne compte 105 demandes. Trois ne sont pas des prestations : ratio "
     "de perte et réclamations traitées (recouvrement, l. 1599 à 1621), clients informés sur "
     "les produits (communication, l. 1841 à 1852)."),
]

VERIFS = [
    ("Chaque ligne des cinq tableaux « Ce que demande le recueil » des familles (35, 39, 31, "
     "31 et 48 lignes, soit 184) retrouvée ici, avec le même statut et la même question",
     "Claude", "fait"),
    ("Les 105 demandes du relevé ligne à ligne placées : 102 ici, 3 écartées (hors "
     "prestations)", "Claude", "fait"),
    ("Le recueil relu à la source : besoins_v7.txt, l. 1 à 100, 112 à 336, 355 à 380, 440 à "
     "470, 1018 à 1380, 1595 à 1622, 1975 à 1990", "Claude", "fait"),
    ("Les 121 questions des familles regroupées par direction : 108 après 10 fusions (13 "
     "doublons)", "Claude", "fait"),
    ("Critique d'exhaustivité du 08/11 : statuts contraires, demandes sans statut, questions "
     "posées deux fois, verdicts de l'annuaire, corrigés dans les familles", "Claude", "fait"),
    ("Le schéma : zéro avertissement du générateur", "Claude", "fait"),
    ("Poser les 108 questions, direction par direction", "CNSS, via toi", "à poser"),
]

SOURCES = (
    "Sources : recueil des besoins v7 (besoins_v7.txt, version du 14/01/2026 et indicateurs du "
    "20/01/2026) ; relevé ligne à ligne recueil_v7_toutes_les_demandes.json "
    "(style_grh/travail/resultats) ; les cinq documents de famille "
    "(analyses/prestations/regles_t25_t27_dossiers.md, regles_t28_t31_controles_indus.md, "
    "regles_t32_t41_beneficiaires.md, regles_t42_t49_nouveaux.md, regles_t50_t57_evolution.md), "
    "qui font foi pour les statuts, les règles et les questions. Aucun chiffre nouveau : ceux "
    "des questions viennent des documents de famille. Détail : "
    "analyses/prestations/besoins_v7_prestations.md."
)

DATA = {
    "seed": 7,
    "titre": "Le recueil des besoins v7 : toutes les demandes sur les prestations, et où elles "
             "en sont",
    "sous_titre": (
        "État au 08/11/2026. Toutes les demandes du recueil des besoins v7 (14/01 et 20/01/2026) "
        "qui touchent les prestations, ligne par ligne, avec le statut et la question que leur "
        "donnent les cinq documents de famille (T25-T27, T28-T31, T32-T41, T42-T49, T50-T57). "
        "Tout est proposé, rien n'est validé par la CNSS. « T32-T41 Q4 » : la question Q4 du "
        "document de la famille T32 à T41 ; « T32-T41 N3 » : sa règle nouvelle N3. Le détail est "
        "dans analyses/prestations/besoins_v7_prestations.md."
    ),
    "en_bref": [],          # rempli plus bas, une fois les lignes comptées
}


# ---------------------------------------------------------------------- comptes
STATUT_ORDRE = [PC, AI, QU, HBA, HBI, SANS]
COLONNES_RECAP = FAMILLES + ["Indicateurs et transverses (partie 6)",
                             "Tableaux sans ligne (partie 7)"]


def toutes_les_lignes():
    """[(colonne du récapitulatif, ligne)] pour toutes les lignes des parties 1 à 7."""
    out = []
    for partie in PARTIES:
        col = partie["cle"] if partie["cle"] in FAMILLES else COLONNES_RECAP[5]
        out += [(col, l) for l in partie["lignes"]]
    out += [(COLONNES_RECAP[5], l) for l in SANS_STATUT]
    out += [(COLONNES_RECAP[6], l) for l in SANS_LIGNE]
    return out


def comptes():
    """{statut: {colonne: n}}."""
    tab = {s: {c: 0 for c in COLONNES_RECAP} for s in STATUT_ORDRE}
    for col, ligne in toutes_les_lignes():
        tab[ligne[4]][col] += 1
    return tab


def nb_questions():
    return sum(len(g["questions"]) for g in QUESTIONS)


def _total(tab, s):
    return sum(tab[s].values())


_T = comptes()
_N = sum(_total(_T, s) for s in STATUT_ORDRE)

DATA["en_bref"] = [
    f"Le recueil v7, relevé ligne à ligne, contient 105 demandes ; 102 touchent les "
    f"prestations (les 3 autres portent sur le recouvrement et la communication). Elles font "
    f"ici {_N} lignes : quand plusieurs familles reprennent une demande avec des statuts "
    f"différents, elle a une ligne par statut, et chaque renvoi « Idem. Tab n » est détaillé.",
    f"Où elles en sont : {_total(_T, PC)} prises en compte (une règle ou une requête proposée "
    f"le fait déjà), {_total(_T, AI)} à intégrer (proposées, pas encore faites), "
    f"{_total(_T, QU)} en question à la CNSS, {_total(_T, HBA)} hors base (la donnée n'est "
    f"pas dans la base), {_total(_T, HBI)} hors BI (un travail sur l'application), "
    "et aucune sans statut : chaque demande a maintenant un statut dans un document de "
    "famille.",
    "Beaucoup de « à intégrer » viennent de trois demandes répétées : sortir les tableaux au "
    "mois et au trimestre, les découper par direction régionale (DR) et par agence, garder le "
    "détail ligne à ligne. Les règles sont écrites ; les requêtes restent à compléter après "
    "visa de la CNSS.",
    "Trois points pèsent le plus. Les contrôles a posteriori sont dans les fichiers Excel de la "
    "DAI (Direction de l'audit interne), pas dans la base : sans eux, T28 à T30 restent vides. "
    "« Idem. Tab 41 », lu à la lettre, ferait compter l'assuré décédé à la place de sa veuve "
    "ou de son orphelin : on le lit « même périodicité ». T26, T39, T40 et T56 n'ont aucune "
    "ligne dans le recueil.",
    f"{nb_questions()} questions ouvertes, regroupées par direction (121 dans les familles, "
    "dont 13 doublons fusionnés). Aucune ne bloque : chacune dit ce que fait la requête en "
    "attendant, le plus souvent un réglage écrit en tête de la requête, avec sa valeur par "
    "défaut.",
    "Les décisions les plus attendues (numéros de la partie 8) : le mois d'un paiement (DPC 1, "
    "déjà posée le 02/11), le cliché de fin mars (DPC 2), les contrôles de la DAI (DAI 1 à "
    "3), qui compter pour un survivant (DCP 26), le stock des dossiers en instance (DCP 1, "
    "DCP 2), le taux d'indus (DAI 12, DAI 13), le montant liquidé de T54 et T55 (DCP 38).",
    "La critique du 08/11 a corrigé dans les familles ce que ce document signalait : deux "
    "statuts contraires (délai des pensions, délai des rentes de survivants), six demandes "
    "sans statut (rangées dans T50-T57), T39 et T40 sans ligne, une question de la date des "
    "paiements posée de trois façons. Ce qui reste est justifié en partie 9.",
]

# Les mots : les mêmes que le paragraphe « Les mots de ce document » du md (en plus court).
MOTS = (
    "Les mots. Une ligne : une demande du recueil telle qu'une ou plusieurs familles la "
    "traitent, avec un seul statut. l. 125 : la ligne 125 du texte du recueil. Prestation #1 : "
    "la ligne 1 du tableau « Prestation » du recueil. « Idem. Tab 27 » : le recueil renvoie à "
    "ce qu'il a dit pour le tableau 27. Liquidé : les droits sont calculés et notifiés. Payé : "
    "un paiement de l'application (débours) au statut D ou P, c'est-à-dire versé. Ajustement : "
    "une écriture qui corrige un montant (trop-perçu, rappel, avance, rachat de rente). Ayant "
    "droit : la veuve, le veuf, l'orphelin ou le parent d'un assuré, qui reçoit une prestation "
    "de son chef. Rentier (ou crédirentier) : le titulaire d'une rente d'accident du travail. "
    "Branches : AVS (assurance vieillesse et survivants), que la CNSS appelle PVID (pensions) ; "
    "AT/MP (accidents du travail et maladies professionnelles), ou RP ; PF (prestations "
    "familiales, maternité comprise). IJ : indemnité journalière. ASS : action sanitaire et "
    "sociale. Réglages : les choix écrits en tête de chaque requête (la CTE parametres). Pnn : "
    "le résultat d'une requête de contrôle déjà lancée sur la copie de la base."
)


# ---------------------------------------------------------------------- schéma
def sans_backticks(t):
    return t.replace("`", "")


def ou_et_question(ligne):
    ou, q = sans_backticks(ligne[3]), ligne[5]
    return f"{ou} · question : {q}" if q else ou


def cellule_statut(st):
    return ("statut", st) if st else "sans statut"


COLS_RECUEIL = [("Où dans le recueil", 300), ("Ce que dit le recueil", 780), ("En clair", 640),
                ("Où c'est traité, question liée", INNER - 300 - 780 - 640 - 220),
                ("Statut", 220)]


def dessiner():
    p = Page(DATA["seed"])
    d = p.d
    # --- en-tête
    d.text(0, 0, DATA["titre"], 48)
    st = wrap(DATA["sous_titre"], 22, WIDTH - 40)
    d.text(0, 66, st, 22, C["doux"])
    y = 66 + height_of(st, 22) + 24
    y = p.en_bref(y, DATA["en_bref"]) + 30
    d.text(0, y, "Statut :", 17, C["doux"])
    items = [(bg, bord, lab, "dashed" if k == HBI else "solid")
             for k, (bg, bord, lab) in STATUTS.items()]
    items.append((C["blanc"], C["rouge"], "sans statut (aucune depuis le 08/11)", "dashed"))
    p.legende(100, y, items)
    y += 56
    y = p.bloc(0, y, WIDTH, MOTS, 19, (C["blanc"], C["doux"])) + 40
    p.y = y

    # --- récapitulatif
    cadre, top = p.section("Le récapitulatif : nombre de lignes par statut et par partie")
    largeur = (INNER - 420) // 8
    cols = [("Statut", 420)] + [(c, largeur) for c in COLONNES_RECAP] + [("Total", largeur)]
    lignes = []
    for s in STATUT_ORDRE:
        lignes.append([cellule_statut(s)] + [str(_T[s][c]) for c in COLONNES_RECAP]
                      + [str(_total(_T, s))])
    lignes.append(["Total"] + [str(sum(_T[s][c] for s in STATUT_ORDRE)) for c in COLONNES_RECAP]
                  + [str(_N)])
    bas = p.tableau(X0, top, cols, lignes)
    bas = p.bloc(X0, bas + 24, INNER,
                 "Une ligne = une demande du recueil telle qu'une ou plusieurs familles la "
                 "traitent, avec un seul statut. Le statut et la question sont ceux du document "
                 "de famille, sans rien changer.", 19, (C["bleu_clair"], C["bleu"]))
    p.fermer(cadre, bas)

    # --- parties 1 à 6.5
    for partie in PARTIES:
        p.recueil([(l[0], l[1], l[2], ou_et_question(l), l[4]) for l in partie["lignes"]],
                  partie["note"], titre=partie["titre"])

    # --- 6.6 et 7 : lignes sans statut possible
    for titre, note, liste, col0 in [
        ("6.6 Les indicateurs hors des tableaux, rangés dans la famille T50-T57", "Aucune "
         "famille ne les classait avant la critique du 08/11 ; ils sont maintenant dans la "
         "partie 1 de T50-T57, avec ce statut.", SANS_STATUT,
         "Où dans le recueil"),
        ("7. Ce que le recueil ne dit pas : les tableaux sans aucune ligne", "Le tableau "
         "Prestation du recueil reprend les 30 tableaux examinés à Kaya en 2017, dans le même "
         "ordre : T26, T39, T40 et T56 n'y sont pas (remarque B38 du recueil). T29, T46, T47, "
         "T49, T53 et T55 n'ont qu'un renvoi « Idem ».", SANS_LIGNE, "Tableau"),
    ]:
        cadre, top = p.section(titre)
        top = p.bloc(X0, top, INNER, note, 19, (C["bleu_clair"], C["bleu"])) + 24
        cols = [(col0, 300)] + COLS_RECUEIL[1:]
        bas = p.tableau(X0, top, cols, [[l[0], l[1], l[2], ou_et_question(l),
                                         cellule_statut(l[4])] for l in liste])
        p.fermer(cadre, bas)

    # --- 8 : questions
    cadre, top = p.section("8. Les questions ouvertes, par direction, sans doublon")
    bas = p.bloc(X0, top, INNER,
                 "Une question par point, posée une seule fois même si plusieurs familles la "
                 "posent (toutes ses références sont données). Aucune ne bloque : « En "
                 "attendant » dit ce que fait la requête. Sigles : DCP, Direction centrale des "
                 "prestations ; DP, sigle du recueil pour les AT/MP (à faire préciser) ; DR, "
                 "directions régionales ; DES, sigle du recueil pour les PF (ou la direction qui "
                 "l'a remplacée) ; DAI, audit interne ; DSI, systèmes d'information ; DPC, "
                 "prospective et coopération (elle fait l'annuaire) ; DFC, financière et "
                 "comptable ; DCR, recouvrement ; DPASS, prévention et action sanitaire et "
                 "sociale ; DCG, sigle du recueil (contrôle de gestion, à faire préciser).",
                 19, (C["bleu_clair"], C["bleu"]))
    p.fermer(cadre, bas)
    for groupe in QUESTIONS:
        # la pastille ne porte que le numéro (la direction est dans le titre de la section) ;
        # l'identifiant complet (« DCP 26 ») ouvre la ligne des références
        p.questions([(n.split(" ")[1], q, a, sans_backticks(e), f"{n} · {sans_backticks(r)}")
                     for n, q, a, e, r in groupe["questions"]],
                    par_ligne=3, titre=groupe["titre"])

    # --- 9 : vérification
    p.attention([(t, sans_backticks(x)) for t, x in SIGNALEMENTS], par_ligne=4,
                note="Ce que la vérification a trouvé : ce qui a été corrigé dans les "
                     "documents de famille le 08/11, et ce qui reste, justifié.",
                titre="9. Comment c'est vérifié : ce qui a été trouvé")
    p.fin(VERIFS, SOURCES, titre="9. Comment c'est vérifié : les contrôles faits et la suite")
    return d


def build():
    d = dessiner()
    for w in d.warnings:
        print("ATTENTION", w)
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
