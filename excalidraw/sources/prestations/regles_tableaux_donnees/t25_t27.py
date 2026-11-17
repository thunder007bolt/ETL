"""
Règles par tableau : famille T25–T27 (les dossiers : déposés, liquidés par durée, en instance,
rejetés ; T25 pensions, T26 accidents du travail et maladies professionnelles, T27 prestations
familiales et maternité).

Même format que T32 dans regles_par_tableau.py. Sources : fiche des règles
(analyses/regles_cipres/fiche_regles.md, règles 1 à 36), document de famille
(analyses/prestations/regles_t25_t27_dossiers.md), requêtes proposées t25, t26 et t27
(analyses/prestations/t2*_requete_proposee.sql) et points à valider
(analyses/prestations/points_a_valider.md). Chiffres : partie 4 du document de famille
(« Face à l'annuaire CNSS 2024 »).
"""

_CHAPEAU_FIN = (
    "Voici chaque règle appliquée et le champ de la base qui la porte. Une remarque ? "
    "Écrivez-la dans la dernière colonne. En orange : règle encore en question (n° du point à valider)."
)

# Règles communes aux trois tableaux (fiche des règles 2, 13, 14, 15, 19, 21, 24, 26, 27)
_ETAPE = (
    "Chaque étape compte dans sa période : le dépôt au jour de la réception, la liquidation au jour "
    "de la notification, le rejet au jour du rejet. Déposé le 20/11/2024 et notifié le 05/02/2025 : "
    "déposé en 2024, liquidé en 2025.",
    "RECEPTION_DOSSIER.RD_DATE_RECEPTION,\nPRESTATION_ESP.PE_DATE_NOTIFICATION,\n"
    "DOSSIER_REJETE.DOSRE_DATE_REJET",
)
_LIQUIDE = (
    "Liquidé : à la première notification des droits de la demande. Le statut du dossier "
    "(actif ou fermé) ne sert pas.",
    "PRESTATION_ESP.PE_DATE_NOTIFICATION, la première ;\nDOSSIER.DOS_STATUT non utilisé",
    None,
)
_DUREE = (
    "Durée de liquidation : les jours de calendrier entre le dépôt et la première notification. "
    "Une durée négative (erreur de saisie) compte 0.",
    "PE_DATE_NOTIFICATION − RD_DATE_RECEPTION",
)
_COLONNES = (
    "Les colonnes : 0 à 14 jours, 15 à 45 jours, 46 jours et plus. Par ligne : la durée moyenne "
    "et la médiane.",
    "calcul",
    None,
)
_REJETE = (
    "Rejeté : un refus enregistré dans le module des rejets, compté l'année du rejet. Ni le statut "
    "de la réception, ni la fermeture du dossier ne valent refus : un refus non saisi reste en instance.",
    "DOSSIER_REJETE.DOSRE_DATE_REJET",
)
_INSTANCE = (
    "En instance : déposée, ni liquidée ni rejetée à la fin de la période. On écarte les dossiers "
    "fermés jamais notifiés, et les réceptions d'avant 2016 jamais reliées à un dossier.",
    "DOSSIER.DOS_STATUT = F sans notification ;\nRD_DATE_RECEPTION < 01/01/2016 sans DOS_CODE",
    None,
)
_TOTAL = (
    "Le Total compte toutes les demandes, même celles qui n'ont pas encore de ligne. Sa durée moyenne "
    "se recalcule sur toutes les demandes, pas en moyenne des lignes.",
    "calcul",
    None,
)
_TAUX = (
    "Taux de dossiers restant à traiter = en instance à la fin ÷ dossiers reçus dans la période × 100. "
    "Norme : 15 % au plus.",
    "calcul ; norme dans les paramètres",
)
_DELAI = (
    "Délai de liquidation (indicateur) : moyenne, écart type et part liquidée en 45 jours au plus, "
    "sur toutes les demandes liquidées. Norme : une moyenne de moins de 45 jours. Chaque indicateur "
    "qui a une norme porte la mention « dans les normes » ou « hors normes ».",
    "calcul ; norme dans les paramètres",
    None,
)
_PERIODE = (
    "Au mois, au trimestre et à l'année : les dépôts, liquidations et rejets s'additionnent ; "
    "le stock en instance se lit à la fin de la période.",
    "paramètre de la requête",
    None,
)
_PHOTO = (
    "Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi plus tard "
    "n'entre que dans les photos suivantes.",
    "copie de la base de fin janvier N+1",
    None,
)
_DECOUPAGE = (
    "Chaque chiffre se découpe par direction régionale et par agence : celles du dossier, à défaut "
    "celles de la réception.",
    "DOSSIER.DR_NO, DOSSIER.SP_NO ;\nsinon RECEPTION_DOSSIER.DR_NO, SP_NO",
    None,
)
_DEMANDES = (
    "On compte des demandes, pas des dossiers : un dossier porte une demande par ligne du tableau.",
    "une demande = DOSSIER.DOS_CODE × une ligne",
    None,
)


def _avec(regle, point=None):
    """Une règle commune à deux éléments (texte, champ), avec son point à valider."""
    return (regle[0], regle[1], point)


TABLEAUX = {
    "T25": {
        "titre": "T25 — Les dossiers de pension de l'année, par durée de liquidation : les règles de la BI",
        "chapeau": (
            "T25 compte, pour l'année, les demandes de pension déposées, liquidées (en moins de 15 jours, "
            "de 15 à 45 jours, en plus de 45 jours), en instance à la fin de la période et rejetées, "
            "par type de pension, avec la durée de liquidation.\n" + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("La branche des pensions (vieillesse, invalidité, décès).", "DOSSIER.TDOS_CODE = V", None),
                _DEMANDES,
                ("La réversion (la pension du défunt reprise par sa veuve ou ses orphelins) est une "
                 "demande à part, sur la ligne des survivants, qu'elle soit dans un nouveau dossier ou "
                 "dans celui du défunt. Une veuve et ses orphelins notifiés ensemble font une seule demande.",
                 "PRESTATION_ESP.TPE_CODE = RPSV, RPSF, RPSE, RPA", "5"),
                ("La ligne vient du code de la prestation : vieillesse, retraite anticipée (sur sa propre "
                 "ligne), invalidité, survivants, allocation de vieillesse (versée en une fois), "
                 "allocation de survivant.",
                 "PRESTATION_ESP.TPE_CODE : RPV ; RPRA ; RPI ;\nRPSV, RPSF, RPSE, RPA ; RAV ;\n"
                 "RASC, RASO, RASA, RAS", None),
                ("Les accessoires (bonification, majorations pour enfant, charges du ménage) suivent la "
                 "pension : pas de demande de plus. Un dossier qui n'a qu'un accessoire va en "
                 "« Autres prestations de la branche ».",
                 "TPE_CODE = RBEC, RME, RME-COL, RCCM", None),
            ]),
            ("Les dossiers retenus", [
                ("Un dossier de coordination payé par l'autre caisse, mais reçu et étudié par la CNSS, "
                 "est compté.", "DOSSIER.DOS_CATEGORIE = COOR_AC : gardé", "19"),
                ("Un dossier transféré à une autre caisse est retiré.",
                 "DOSSIER.DOS_CATEGORIE = CNSS_AC : écarté", None),
            ]),
            ("Déposé", [
                _avec(_ETAPE, "14"),
                ("Déposé : la demande reçue au guichet. La réception qui ouvre un dossier va à sa première "
                 "demande ; une réception sur un dossier qui existe déjà va à la réversion.",
                 "RECEPTION_DOSSIER.RD_DATE_RECEPTION ;\nouvre le dossier : DOSSIER.RD_ID ;\n"
                 "dossier existant : RECEPTION_DOSSIER.DOS_CODE", None),
                ("Une demande sans réception au guichet prend sa date de demande. Une demande ne compte "
                 "qu'un dépôt, le premier.", "PRESTATION_ESP.PE_DATE_DEMANDE", None),
            ]),
            ("Liquidé et durée", [
                _LIQUIDE,
                _avec(_DUREE),
                _COLONNES,
            ]),
            ("En instance et rejeté", [
                _avec(_REJETE, "22"),
                _INSTANCE,
                ("Pas de limite de durée pour les pensions : une demande sans suite reste en instance, "
                 "même des années après son dépôt.", "RD_DATE_RECEPTION, sans limite d'ancienneté", "21"),
            ]),
            ("Le Total et les indicateurs", [
                _TOTAL,
                _avec(_TAUX, "15"),
                _DELAI,
            ]),
            ("La période et le découpage", [
                _PERIODE,
                _PHOTO,
                _DECOUPAGE,
            ]),
        ],
        "chiffres": (
            "L'annuaire CNSS 2024 ne publie ni dépôts, ni dossiers en instance, ni rejets, ni durées. "
            "Seul contrôle direct : 831 allocations de vieillesse liquidées en 2024 dans la base, à leur "
            "première notification, pour 833 publiées (tableau 24) : −0,24 %."
        ),
    },
    "T26": {
        "titre": "T26 — Les dossiers d'accident du travail de l'année, par durée de liquidation : "
                 "les règles de la BI",
        "chapeau": (
            "T26 compte, pour l'année, les demandes d'accident du travail et de maladie professionnelle "
            "déposées, liquidées (en moins de 15 jours, de 15 à 45 jours, en plus de 45 jours), en instance "
            "à la fin de la période et rejetées, par type de prestation, avec la durée de liquidation.\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("Le recueil des besoins ne prévoit pas T26 : on le produit comme l'Annuaire CIPRES le "
                 "décrit, avec les règles de T25 et T27.", "maquette de l'Annuaire CIPRES", "73"),
                ("La branche des accidents du travail et maladies professionnelles.",
                 "DOSSIER.TDOS_CODE = A", None),
                ("On compte des demandes, pas des dossiers : un dossier porte une demande par ligne, "
                 "par exemple les indemnités journalières puis la rente.",
                 "une demande = DOSSIER.DOS_CODE × une ligne", None),
                ("La ligne vient du code de la prestation : rente viagère de la victime, rentes de "
                 "survivants (veuve, veuf, orphelin, ascendant), indemnités journalières, allocation "
                 "d'incapacité (moins de 15 % d'incapacité).",
                 "PRESTATION_ESP.TPE_CODE : ARI ;\nARSV, ARSF, ARSE, ARSA ; AIJ ; AAI", None),
                ("Les soins remboursés vont en « Autres prestations de la branche » : une demande par "
                 "dossier, liquidée à la saisie du premier remboursement, pas à son approbation.",
                 "DEBOURS avec TPN_CODE rempli (soins) ;\nDEB_DATE_INSERT, le premier", "24"),
                ("Un rachat de rente (capital versé en une fois à la place de la rente) n'est pas compté.",
                 "AJUSTEMENT.TAJ_CODE = RR : écarté", "78"),
                ("Les dossiers de coordination reçus par la CNSS sont gardés ; les dossiers transférés à "
                 "une autre caisse sont retirés.",
                 "DOSSIER.DOS_CATEGORIE :\nCOOR_AC gardé, CNSS_AC écarté", None),
            ]),
            ("Déposé", [
                _avec(_ETAPE),
                ("La déclaration de l'accident ou de la maladie ouvre le dossier et dépose les indemnités "
                 "journalières et les soins.",
                 "RECEPTION_DOSSIER.RD_DATE_RECEPTION\nde la réception qui ouvre le dossier (DOSSIER.RD_ID)",
                 None),
                ("La rente, l'allocation d'incapacité ou la rente de survivant se dépose plus tard sur le "
                 "même dossier, par une demande au guichet. Sans cette demande : le certificat de "
                 "consolidation, sinon la déclaration.",
                 "RECEPTION_DOSSIER.RD_DATE_RECEPTION (dossier existant) ;\n"
                 "sinon DOSSIER.DOS_DATE_REC_CM_RENTE", "25"),
                ("Une déclaration qui n'a donné ni indemnité, ni soins, ni rente compte dans les déposés, "
                 "sur la seule ligne Total.",
                 "DOSSIER.TDOS_CODE = A\nsans PRESTATION_ESP ni DEBOURS", "23"),
            ]),
            ("Liquidé et durée", [
                _LIQUIDE,
                ("Une indemnité journalière ne fait qu'une demande, même si l'arrêt est prolongé. Sa durée "
                 "part de la déclaration de l'accident et se publie telle quelle.",
                 "TPE_CODE = AIJ, la première notification ;\ndépart : la déclaration", None),
                _avec(_DUREE),
                _COLONNES,
            ]),
            ("En instance et rejeté", [
                _avec(_REJETE),
                _INSTANCE,
                ("Une demande toujours sans suite 24 mois après son dépôt sort du stock en instance.",
                 "RD_DATE_RECEPTION + 24 mois\n(paramètre de la requête)", "21"),
            ]),
            ("Le Total et les indicateurs", [
                _TOTAL,
                _avec(_TAUX),
                _DELAI,
                ("Taux de variation des déclarations = (année n − année n−1) ÷ année n−1 × 100, à deux "
                 "décimales, par direction régionale et par agence, sur les dossiers d'accident ouverts.",
                 "DOSSIER.TDOS_CODE = A,\nDOSSIER.DOS_DATE_OUVERTURE", None),
            ]),
            ("La période et le découpage", [
                _PERIODE,
                _PHOTO,
                _DECOUPAGE,
            ]),
        ],
        "chiffres": (
            "L'annuaire CNSS 2024 ne publie ni dépôts, ni dossiers en instance, ni rejets, ni durées. "
            "Accidents déclarés en 2024 : 1 189 dossiers ouverts dans la base, 1 171 publiés (tableau 47) : "
            "+1,54 % ; nouvelles rentes viagères : 39 dans la base, 44 publiées (tableau 75) : −11,4 %, "
            "écart à expliquer."
        ),
    },
    "T27": {
        "titre": "T27 — Les dossiers de prestations familiales et de maternité de l'année, par durée de "
                 "liquidation : les règles de la BI",
        "chapeau": (
            "T27 compte, pour l'année, les demandes de prestations familiales et de maternité déposées, "
            "liquidées (en moins de 15 jours, de 15 à 45 jours, en plus de 45 jours), en instance à la fin "
            "de la période et rejetées, par type de prestation, avec la durée de liquidation.\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("Les prestations familiales et la maternité, comptée avec les prestations familiales.",
                 "DOSSIER.TDOS_CODE = F ou M", None),
                _DEMANDES,
                ("La ligne vient du code de la prestation : allocations familiales, indemnités journalières "
                 "de maternité, allocations prénatales (code de la maternité et ancien code des "
                 "prestations familiales).",
                 "PRESTATION_ESP.TPE_CODE : FAF ; MIJ ;\nMAP, FAP", None),
                ("La dernière ligne s'appelle « Frais médicaux de maternité » (accouchement, consultation, "
                 "examens, hospitalisation…). Elle reste vide : la base n'a aucun de ces paiements.",
                 "aucun DEBOURS de ces frais de 2020 à 2025", "129"),
                ("Chaque enfant ajouté au dossier d'un allocataire est une demande de plus, déposée à la "
                 "date de demande de son allocation et liquidée à sa notification.",
                 "PRESTATION_ESP.TPE_CODE = FAF, notifiée\naprès la première du dossier ; PE_DATE_DEMANDE",
                 "27"),
                ("Une grossesse fait deux demandes, l'indemnité journalière et l'allocation prénatale, "
                 "déposées et liquidées ensemble.",
                 "DOSSIER.TDOS_CODE = M : TPE_CODE = MIJ et MAP", "28"),
                ("Les dossiers de coordination reçus par la CNSS sont gardés ; les dossiers transférés à "
                 "une autre caisse sont retirés.",
                 "DOSSIER.DOS_CATEGORIE :\nCOOR_AC gardé, CNSS_AC écarté", None),
            ]),
            ("Déposé", [
                _avec(_ETAPE, "14"),
                ("Déposé : la réception qui ouvre le dossier (nouvel allocataire, nouvelle grossesse) "
                 "dépose chacune de ses demandes.",
                 "RECEPTION_DOSSIER.RD_DATE_RECEPTION ;\nDOSSIER.RD_ID", None),
                ("Une réception sur un dossier qui existe déjà (certificat de scolarité ou de vie) n'est "
                 "pas un dépôt.", "RECEPTION_DOSSIER.DOS_CODE d'un dossier existant : écartée", None),
            ]),
            ("Liquidé et durée", [
                _LIQUIDE,
                ("Durée de liquidation : les jours de calendrier entre le dépôt et la première notification "
                 "(0 si elle est négative). Le recueil la fait finir à une « date de validation » encore "
                 "inconnue : on prend la notification en attendant.",
                 "PE_DATE_NOTIFICATION − RD_DATE_RECEPTION", "130"),
                _COLONNES,
            ]),
            ("En instance et rejeté", [
                _avec(_REJETE),
                _INSTANCE,
                ("Une demande toujours sans suite 24 mois après son dépôt sort du stock en instance.",
                 "RD_DATE_RECEPTION + 24 mois\n(paramètre de la requête)", "21"),
            ]),
            ("Le Total et les indicateurs", [
                _TOTAL,
                _avec(_TAUX),
                ("Délai des prestations familiales (indicateur) : seulement les allocations familiales et "
                 "les indemnités journalières de maternité ; une grossesse y compte une fois, par "
                 "l'indemnité. Moyenne, écart type, part liquidée en 45 jours au plus. Norme : une "
                 "moyenne de moins de 45 jours, avec la mention « dans les normes » ou « hors normes ».",
                 "TPE_CODE = FAF et MIJ ;\nnorme dans les paramètres", None),
            ]),
            ("La période et le découpage", [
                _PERIODE,
                _PHOTO,
                _DECOUPAGE,
            ]),
        ],
        "chiffres": (
            "L'annuaire CNSS 2024 ne publie ni dépôts, ni dossiers en instance, ni rejets, ni durées. "
            "Indemnités journalières de maternité liquidées en 2024 : 3 890 dans la base, 3 893 publiées (tableau 35) : −0,08 % ; "
            "frais médicaux de maternité : 25,0 millions FCFA payés en 2024 selon l'annuaire (tableaux 40 et 41), "
            "aucun dans la base."
        ),
    },
}
