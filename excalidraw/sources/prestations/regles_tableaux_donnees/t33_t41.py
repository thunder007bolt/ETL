"""
Règles par tableau : famille T33–T41 (les bénéficiaires à une date d'arrêté : T33 invalidité,
T34 veuves et veufs, T35 orphelins, T36 ascendants ; T37 à T40 les rentiers des accidents du
travail et maladies professionnelles ; T41 les allocataires des prestations familiales).

Même format que T32 dans regles_par_tableau.py, dont on garde les groupes et les formulations
quand la règle est la même. Sources : fiche des règles (analyses/regles_cipres/fiche_regles.md,
règles 1 à 16 et 48 à 60), document de famille (analyses/prestations/
regles_t32_t41_beneficiaires.md), requête proposée (analyses/prestations/
t32_t41_requete_proposee.sql), points à valider (analyses/prestations/points_a_valider.md et
_v2), tranches des maquettes (analyses/prestations/reference_cipres_T25_T57.md). Chiffres :
partie 4 du document de famille (« Face à l'annuaire CNSS 2024 ») et comparaison A01
(analyses/comparaison_annuaire_cnss_2024/resultats/A01_tableau18.tsv).

Écart voulu avec la fiche des règles (règle 50) et la requête : T39 suit la maquette CIPRES,
les rentiers orphelins de 25 ans et plus sont comptés sur leur ligne (point 102).
"""

_CHAPEAU_FIN = (
    "Voici chaque règle appliquée et le champ de la base qui la porte. Une remarque ? "
    "Écrivez-la dans la dernière colonne. En orange : règle encore en question (n° du point à valider)."
)

_V = ("La branche des pensions.", "DOSSIER.TDOS_CODE = V", None)
_A = ("La branche des accidents du travail et maladies professionnelles.", "DOSSIER.TDOS_CODE = A", None)
_F = ("La branche des prestations familiales.", "DOSSIER.TDOS_CODE = F", None)

_DATE_ARRETE = ("La date d'arrêté", [
    ("Chaque fin de mois, le 31 décembre compris.", "paramètre de la requête", None),
    ("Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi "
     "plus tard n'entre que dans les photos suivantes.", "copie de la base de fin janvier N+1", None),
])

_SEXE = ("Le sexe : 1 hommes, 2 femmes.", "INDIVIDU.IND_SEXE", None)

_JAMAIS_DEUX = ("Une personne peut aussi être dans un autre tableau : on n'additionne jamais "
                "deux tableaux pour compter des personnes.", "chaque tableau compté à part", None)

_COORDINATION = ("Pension de coordination payée par l'autre caisse : comptée, sans montant. Seule la part "
                 "payée par la CNSS entre dans le montant.", "DOSSIER.DOS_CATEGORIE = COOR_AC")


def _chapeau(debut):
    return debut + "\n" + _CHAPEAU_FIN


def _age(tranches):
    return ("L'âge = l'année du calcul moins l'année de naissance, sans regarder le jour. "
            f"Les tranches sont celles de la maquette CIPRES : {tranches}.",
            "INDIVIDU.IND_DATE_NAISSANCE", None)


def _naissance_1900(derniere):
    return (f"Une date de naissance par défaut (année 1900) va dans « {derniere} ».",
            "IND_DATE_NAISSANCE en 1900", None)


def _en_cours(prestation, sans_paiement):
    """Les cinq règles de T32 « Qui est en cours », pour une pension ou une rente."""
    return [
        (f"La {prestation} a commencé avant ou le jour de la date d'arrêté.",
         "PRESTATION_ESP.PE_DATE_EFFET ≤ date d'arrêté", None),
        ("Elle n'est pas radiée.", "PE_DATE_RADIATION vide ou après la date", None),
        ("Elle n'est pas suspendue, sauf si elle a repris avant la date.",
         "PE_DATE_SUSPENSION vide ou après la date,\nou PE_DATE_REACTIVATION avant la date", None),
        ("La personne est en vie.", "INDIVIDU.IND_DATE_DECES vide ou après la date", None),
        (f"Payée ou non : une {prestation} en cours compte même sans paiement à la fin de la période "
         f"({sans_paiement}).", "aucun paiement exigé", None),
    ]


def _montant(personnes, seule, codes, incomplet=False):
    """Le groupe « Le montant » de T32, pour les pensions et les rentes."""
    regles = [
        (f"Ce qui a été payé dans la période aux {personnes} comptés.",
         "DEBOURS.DEB_MONTANT, relié par PE_ID", "35"),
        ("Un paiement est un débours de paiement payé, par titre de paiement (D) ou sans titre (P). "
         "Jamais le net à payer du titre.", "DEBOURS.DEB_TYPE = PM,\nDEB_STATUT = D ou P", None),
        ("Un paiement compte dans le mois qu'il paie, pas dans celui où il est saisi.",
         "DEBOURS.DEB_DATE_EFFET", None),
        (f"{seule} : seuls ses paiements entrent dans le montant.",
         f"seulement les débours des {codes}", None),
    ]
    if incomplet:
        regles.append(
            ("Une partie des paiements des rentes d'accident manque dans la base : le montant porte "
             "la mention « incomplet ».", "DEBOURS des dossiers TDOS_CODE = A", "51"))
    regles.append(("L'année = la somme des mois. Montant moyen = montant ÷ effectif.", "calcul", None))
    return ("Le montant", regles)


def _decoupage(controle, sources, point=None):
    return ("Le découpage et le contrôle", [
        ("Chaque chiffre se découpe par direction régionale et par agence, celles du dossier.",
         "DOSSIER.DR_NO, DOSSIER.SP_NO", None),
        (f"Contrôle : stock de fin = stock d'avant + {controle} − décès − autres sorties.", sources, point),
    ])


_RENTIERS_MOT = ("On dit « rentiers », et non plus « crédirentiers ».", "libellés du tableau", None)

_RENTE_SANS_PAIEMENT = "certificat de vie attendu, par exemple"

TABLEAUX = {
    "T33": {
        "titre": "T33 — Les pensionnés d'invalidité par âge et par sexe : les règles de la BI",
        "chapeau": _chapeau(
            "T33 compte, à une date d'arrêté, les personnes qui ont une pension d'invalidité en cours, "
            "par tranche d'âge de 5 ans et par sexe, avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _V,
                ("La pension d'invalidité.", "PRESTATION_ESP.TPE_CODE = RPI", None),
                ("La personne qui a droit, une seule fois, même avec deux pensions d'invalidité.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", None),
                ("Les accessoires (bonification, majorations pour enfant, charges du ménage) "
                 "ne font pas un pensionné de plus.",
                 "TPE_CODE = RBEC, RME, RME-COL, RCCM : écartés", None),
            ]),
            ("Qui est en cours à la date d'arrêté",
             _en_cours("pension", "certificat de vie attendu, par exemple")),
            _DATE_ARRETE,
            ("L'âge et le sexe", [
                _age("moins de 25 ans, 25-29 … 55-59, 60 ans et +"),
                _naissance_1900("60 ans et +"),
                _SEXE,
            ]),
            _montant("pensionnés", "La pension d'invalidité seule", "pensions RPI"),
            ("Les cas particuliers", [
                (*_COORDINATION, None),
                ("Un invalide reste dans T33 après l'âge de la retraite, ligne « 60 ans et + », tant que "
                 "l'application ne convertit pas sa pension en pension de vieillesse (module à développer "
                 "par la DSI). La BI ne le déplace pas elle-même.",
                 "PRESTATION_ESP.TPE_CODE = RPI, quel que soit l'âge", "100"),
            ]),
            _decoupage("nouveaux invalides", "T33 de l'année d'avant ;\nentrées calculées à part"),
        ],
        "chiffres": (
            "Avec ces règles, au 31/12/2024 : 125 invalides (91 hommes, 34 femmes), dont 72 de 60 ans et plus, "
            "pour 119 dans l'annuaire CNSS 2024 (86 hommes, 33 femmes ; 70 de 60 ans et plus) : +5,0 %. "
            "Montant payé aux invalides comptés : 112,1 M, pour 113,6 M publiés."
        ),
    },
    "T34": {
        "titre": "T34 — Les pensionnés veuves et veufs par âge et par sexe : les règles de la BI",
        "chapeau": _chapeau(
            "T34 compte, à une date d'arrêté, les veuves et les veufs qui ont une pension de survivant en cours, "
            "par tranche d'âge de 5 ans et par sexe, avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _V,
                ("La pension de veuve ou de veuf.", "PRESTATION_ESP.TPE_CODE = RPSV, RPSF", None),
                ("La veuve ou le veuf lui-même, à son âge et à son sexe, pas l'assuré décédé au nom de qui "
                 "le dossier est ouvert. Une seule fois, même avec deux pensions.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois\n(pas DOSSIER.IND_ID)", "71"),
                ("Une veuve qui a aussi sa propre retraite compte ici et dans T32 : on n'additionne jamais "
                 "deux tableaux.", "T32 et T34 comptés à part", None),
            ]),
            ("Qui est en cours à la date d'arrêté",
             _en_cours("pension", "certificat de vie attendu, par exemple")),
            _DATE_ARRETE,
            ("L'âge et le sexe", [
                _age("moins de 15 ans, 15-19 … 95-99, 100 ans et plus"),
                _naissance_1900("100 ans et plus"),
                _SEXE,
            ]),
            _montant("pensionnés", "La pension de veuve ou de veuf seule", "pensions RPSV et RPSF"),
            ("Les cas particuliers", [
                (*_COORDINATION, "36"),
                ("Dossier transféré à une autre caisse : compté aujourd'hui.",
                 "DOSSIER.DOS_CATEGORIE = CNSS_AC", "101"),
                ("Une veuve dont le décès ou l'arrêt de la pension est saisi après la photo de fin janvier "
                 "reste comptée pour l'année ; elle sort dans les photos suivantes.",
                 "INDIVIDU.IND_DATE_DECES, PE_DATE_RADIATION\n(la base ne garde pas le jour de saisie du décès)",
                 "37"),
            ]),
            _decoupage("nouveaux veufs et veuves", "T34 de l'année d'avant, T43"),
        ],
        "chiffres": (
            "Avec ces règles, au 31/12/2024 : 20 715 veuves et veufs (20 359 femmes, 356 hommes), pour 21 767 "
            "dans l'annuaire CNSS 2024 (21 438 veuves, 329 veufs) : −4,8 %. Il manque 1 079 veuves (−5,0 %), "
            "surtout à Bobo-Dioulasso et à Ouagadougou. Montant des pensions de survivants (veuves, veufs, "
            "orphelins et ascendants ensemble) payé aux comptés : 7,16 Md, pour 7,05 Md publiés."
        ),
    },
    "T35": {
        "titre": "T35 — Les pensionnés orphelins par âge et par sexe : les règles de la BI",
        "chapeau": _chapeau(
            "T35 compte, à une date d'arrêté, les orphelins de moins de 25 ans qui ont une pension d'orphelin "
            "en cours, par tranche d'âge de 5 ans et par sexe, avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _V,
                ("La pension d'orphelin.", "PRESTATION_ESP.TPE_CODE = RPSE", None),
                ("L'orphelin lui-même, à son âge et à son sexe, pas le tuteur qui reçoit l'argent. "
                 "Une seule fois, même avec deux pensions.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", "71"),
            ]),
            ("Qui est en cours à la date d'arrêté",
             _en_cours("pension", "certificat de vie attendu, par exemple") + [
                 ("Seulement les orphelins de moins de 25 ans, même si leur pension n'est pas radiée.",
                  "année du calcul − année de IND_DATE_NAISSANCE < 25", None),
                 ("Une pension d'orphelin de 25 ans et plus encore en cours, faute de radiation, "
                  "n'est pas comptée : elle est seulement signalée.",
                  "RPSE sans PE_DATE_RADIATION, orphelin de 25 ans et plus", "104"),
             ]),
            _DATE_ARRETE,
            ("L'âge et le sexe", [
                _age("moins de 5 ans, 5-9, 10-14, 15-19, 20-24 ans. La ligne « 25 ans et plus » "
                     "de la maquette est supprimée"),
                _SEXE,
            ]),
            _montant("orphelins", "La pension d'orphelin seule", "pensions RPSE"),
            ("Les cas particuliers", [
                (*_COORDINATION, "36"),
            ]),
            _decoupage("nouveaux orphelins", "T35 de l'année d'avant, T44 ;\nsorties : décès et 25 ans atteints",
                       "105"),
        ],
        "chiffres": (
            "Au 31/12/2024, la base a 13 784 pensions d'orphelin en cours, 13 769 d'orphelins en vie, dont "
            "7 995 de moins de 25 ans ; 6 227 ont été payées dans l'année. Comptés en personnes, 7 970 orphelins "
            "de moins de 25 ans, pour 9 478 dans l'annuaire CNSS 2024 (−15,9 %). Ce chiffre publié ne tient "
            "pas avec les entrées de l'annuaire : 8 687 + 769 nouveaux − 2 décès = 9 454, sans aucune sortie "
            "à 25 ans."
        ),
    },
    "T36": {
        "titre": "T36 — Les pensionnés ascendants par âge et par sexe : les règles de la BI",
        "chapeau": _chapeau(
            "T36 compte, à une date d'arrêté, le père ou la mère d'un assuré décédé qui ont une pension "
            "d'ascendant en cours, par tranche d'âge de 5 ans et par sexe, avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _V,
                ("La pension d'ascendant.", "PRESTATION_ESP.TPE_CODE = RPA", None),
                ("Le père ou la mère lui-même, à son âge et à son sexe, pas l'assuré décédé au nom de qui "
                 "le dossier est ouvert. Une seule fois, même avec deux pensions.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois\n(pas DOSSIER.IND_ID)", "71"),
                _JAMAIS_DEUX,
            ]),
            ("Qui est en cours à la date d'arrêté",
             _en_cours("pension", "certificat de vie attendu, par exemple")),
            _DATE_ARRETE,
            ("L'âge et le sexe", [
                _age("moins de 50 ans, 50-54 … 95-99, 100 ans et plus"),
                _naissance_1900("100 ans et plus"),
                _SEXE,
            ]),
            _montant("pensionnés", "La pension d'ascendant seule", "pensions RPA"),
            ("Les cas particuliers", [
                (*_COORDINATION, None),
            ]),
            _decoupage("nouveaux ascendants", "T36 de l'année d'avant ;\nentrées calculées à part"),
        ],
        "chiffres": (
            "Avec ces règles, au 31/12/2024 : 59 ascendants, comme dans l'annuaire CNSS 2024 (59), avec le même "
            "nombre dans chacune des cinq directions régionales. De 2020 à 2023, la base en compte de 9 % "
            "à 21 % de plus que l'annuaire."
        ),
    },
    "T37": {
        "titre": "T37 — Les rentiers viagers, victimes d'accident du travail, par âge et par sexe : "
                 "les règles de la BI",
        "chapeau": _chapeau(
            "T37 compte, à une date d'arrêté, les victimes d'un accident du travail ou d'une maladie "
            "professionnelle qui ont une rente viagère en cours, par tranche d'âge de 5 ans et par sexe, "
            "avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _A,
                ("La rente viagère de la victime.", "PRESTATION_ESP.TPE_CODE = ARI", None),
                ("La victime elle-même, une seule fois, même avec deux rentes viagères.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", None),
            ]),
            ("Qui est en cours à la date d'arrêté", _en_cours("rente", _RENTE_SANS_PAIEMENT)),
            _DATE_ARRETE,
            ("L'âge et le sexe", [
                _age("moins de 20 ans, 20-24 … 95-99, 100 ans et plus"),
                _naissance_1900("100 ans et plus"),
                _SEXE,
            ]),
            _montant("rentiers", "La rente viagère seule", "rentes ARI", incomplet=True),
            ("Les cas particuliers", [
                ("Un accidenté à moins de 15 % d'incapacité touche une allocation d'incapacité, pas une rente : "
                 "il n'est pas dans T37.", "TPE_CODE = AAI : écarté", None),
            ]),
            _decoupage("nouveaux rentiers", "T37 de l'année d'avant, T48"),
        ],
        "chiffres": (
            "Avec ces règles, au 31/12/2024 : 1 236 rentiers viagers (1 046 hommes, 190 femmes), pour 1 255 "
            "dans l'annuaire CNSS 2024 (1 061 hommes, 194 femmes) : −1,5 %. Montant payé aux rentiers comptés : "
            "384,1 M, pour 455,0 M publiés (−15,6 %) : une partie des paiements manque dans la base."
        ),
    },
    "T38": {
        "titre": "T38 — Les rentiers veuves et veufs d'accident du travail par âge et par sexe : "
                 "les règles de la BI",
        "chapeau": _chapeau(
            "T38 compte, à une date d'arrêté, les veuves et les veufs d'une victime d'accident du travail ou de "
            "maladie professionnelle qui ont une rente en cours, par tranche d'âge de 5 ans et par sexe, "
            "avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _A,
                ("La rente de veuve ou de veuf.", "PRESTATION_ESP.TPE_CODE = ARSV, ARSF", None),
                ("La veuve ou le veuf lui-même, à son âge et à son sexe, pas la victime décédée au nom de qui "
                 "le dossier est ouvert. Une seule fois, même avec deux rentes.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois\n(pas DOSSIER.IND_ID)", "71"),
                _RENTIERS_MOT,
            ]),
            ("Qui est en cours à la date d'arrêté", _en_cours("rente", _RENTE_SANS_PAIEMENT)),
            ("La date d'arrêté", _DATE_ARRETE[1] + [
                ("Le « cumul » demandé par le recueil = tous les rentiers en cours à la date d'arrêté, "
                 "quelle que soit l'année où leur rente a commencé.",
                 "PE_DATE_EFFET ≤ date d'arrêté, sans date de début", "103"),
            ]),
            ("L'âge et le sexe", [
                _age("moins de 20 ans, 20-24 … 95-99, 100 ans et plus"),
                _naissance_1900("100 ans et plus"),
                _SEXE,
            ]),
            _montant("rentiers", "La rente de veuve ou de veuf seule", "rentes ARSV et ARSF", incomplet=True),
            ("Les cas particuliers", [_JAMAIS_DEUX]),
            _decoupage("nouveaux rentiers veufs et veuves", "T38 de l'année d'avant, T49"),
        ],
        "chiffres": (
            "Avec ces règles, au 31/12/2024 : 288 rentiers veufs et veuves, pour 285 dans l'annuaire CNSS 2024 "
            "(283 veuves, 2 veufs) : +1,1 %. Montant des rentes de survivants (T38 à T40 ensemble) payé aux "
            "comptés : 274,8 M, pour 368,6 M publiés (−25,5 %)."
        ),
    },
    "T39": {
        "titre": "T39 — Les rentiers orphelins d'accident du travail par âge et par sexe : les règles de la BI",
        "chapeau": _chapeau(
            "T39 compte, à une date d'arrêté, les orphelins d'une victime d'accident du travail ou de maladie "
            "professionnelle qui ont une rente en cours, par tranche d'âge de 5 ans et par sexe, "
            "avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _A,
                ("La rente d'orphelin.", "PRESTATION_ESP.TPE_CODE = ARSE", None),
                ("L'orphelin lui-même, à son âge et à son sexe, pas le tuteur qui reçoit l'argent. "
                 "Une seule fois, même avec deux rentes.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", None),
                ("Le recueil ne dit rien de T39 : il suit la maquette CIPRES et les règles de T38, "
                 "en disant « rentiers ».", "mêmes règles que T38", "73"),
            ]),
            ("Qui est en cours à la date d'arrêté", _en_cours("rente", _RENTE_SANS_PAIEMENT) + [
                ("Les orphelins de 25 ans et plus sont comptés, sur la ligne « 25 ans et plus » de la maquette : "
                 "le recueil ne la supprime que pour T35.",
                 "année du calcul − année de IND_DATE_NAISSANCE ≥ 25", "102"),
            ]),
            _DATE_ARRETE,
            ("L'âge et le sexe", [
                _age("moins de 5 ans, 5-9, 10-14, 15-19, 20-24, 25 ans et plus"),
                _SEXE,
            ]),
            _montant("rentiers", "La rente d'orphelin seule", "rentes ARSE", incomplet=True),
            ("Les cas particuliers", [_JAMAIS_DEUX]),
            _decoupage("nouveaux rentiers orphelins", "T39 de l'année d'avant, T46"),
        ],
        "chiffres": (
            "Au 31/12/2024 : 531 rentiers orphelins en cours, dont 454 de moins de 25 ans et 77 de 25 ans "
            "et plus, pour 485 dans l'annuaire CNSS 2024 : +9,5 % en les comptant tous, −6,4 % sans les "
            "25 ans et plus. De 2021 à 2023, l'annuaire est sous nos chiffres, même sans les 25 ans et plus."
        ),
    },
    "T40": {
        "titre": "T40 — Les rentiers ascendants d'accident du travail par âge et par sexe : les règles de la BI",
        "chapeau": _chapeau(
            "T40 compte, à une date d'arrêté, le père ou la mère d'une victime décédée d'un accident du travail "
            "ou d'une maladie professionnelle qui ont une rente en cours, par tranche d'âge de 5 ans et par sexe, "
            "avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _A,
                ("La rente d'ascendant.", "PRESTATION_ESP.TPE_CODE = ARSA", None),
                ("Le père ou la mère lui-même, à son âge et à son sexe, pas la victime décédée au nom de qui "
                 "le dossier est ouvert. Une seule fois, même avec deux rentes.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois\n(pas DOSSIER.IND_ID)", None),
                ("Le recueil ne dit rien de T40 : il suit la maquette CIPRES et les règles de T38, "
                 "en disant « rentiers ».", "mêmes règles que T38", "73"),
            ]),
            ("Qui est en cours à la date d'arrêté", _en_cours("rente", _RENTE_SANS_PAIEMENT)),
            _DATE_ARRETE,
            ("L'âge et le sexe", [
                _age("moins de 50 ans, 50-54 … 75-79, 80 ans et plus"),
                _naissance_1900("80 ans et plus"),
                _SEXE,
            ]),
            _montant("rentiers", "La rente d'ascendant seule", "rentes ARSA", incomplet=True),
            ("Les cas particuliers", [_JAMAIS_DEUX]),
            _decoupage("nouveaux rentiers ascendants", "T40 de l'année d'avant, T47"),
        ],
        "chiffres": (
            "Avec ces règles, au 31/12/2024 : 214 rentiers ascendants, pour 207 dans l'annuaire CNSS 2024 : "
            "+3,4 %. De 2020 à 2023, la base en compte de 9 % à 22 % de plus que l'annuaire."
        ),
    },
    "T41": {
        "titre": "T41 — Les allocataires des prestations familiales par âge et par sexe : les règles de la BI",
        "chapeau": _chapeau(
            "T41 compte, à une date d'arrêté, les allocataires des prestations familiales, c'est-à-dire les "
            "titulaires d'un dossier qui ont au moins un enfant compté, par tranche d'âge de 5 ans et par sexe, "
            "avec le montant payé et le montant moyen."),
        "groupes": [
            ("Ce qui est compté", [
                _F,
                ("Seules les allocations familiales comptent : une future mère qui ne touche que "
                 "l'allocation prénatale n'est pas une allocataire.",
                 "PRESTATION_ESP.TPE_CODE = FAF ;\nFAP et MAP écartés", None),
                ("L'allocataire est le titulaire du dossier (l'assuré), compté une fois, même avec "
                 "deux dossiers.", "DOSSIER.IND_ID, compté une fois", None),
            ]),
            ("Qui est en cours à la date d'arrêté", [
                ("L'allocation de l'enfant a commencé avant ou le jour de la date d'arrêté.",
                 "PRESTATION_ESP.PE_DATE_EFFET ≤ date d'arrêté", None),
                ("Elle n'est pas radiée.", "PE_DATE_RADIATION vide ou après la date", None),
                ("Elle n'est pas suspendue, sauf si elle a repris avant la date.",
                 "PE_DATE_SUSPENSION vide ou après la date,\nou PE_DATE_REACTIVATION avant la date", None),
                ("L'enfant a 21 ans au plus et il est en vie.",
                 "PRESTATION_ESP.IND_ID_PREST (l'enfant) ;\nINDIVIDU.IND_DATE_NAISSANCE, IND_DATE_DECES", None),
                ("L'enfant a été payé : un enfant qui a droit mais n'a rien touché (certificat attendu, "
                 "par exemple) n'est pas compté.", "DEBOURS.DEB_TYPE = PM,\nDEB_STATUT = D ou P", "38"),
                ("Payé dans les 12 mois avant la date d'arrêté ; au 31 décembre, dans l'année civile.",
                 "DEBOURS.DEB_DATE_EFFET dans les 12 mois", "99"),
                ("L'allocataire est compté s'il a au moins un enfant compté.",
                 "DOSSIER.DOS_CODE de l'allocation de l'enfant", None),
            ]),
            _DATE_ARRETE,
            ("L'âge et le sexe", [
                ("L'âge et le sexe sont ceux du titulaire du dossier. L'âge = l'année du calcul moins l'année "
                 "de naissance, sans regarder le jour. Les tranches sont celles de la maquette CIPRES : "
                 "moins de 20 ans, 20-24 … 75-79, 80 ans et plus.",
                 "INDIVIDU.IND_DATE_NAISSANCE du titulaire\n(DOSSIER.IND_ID)", None),
                _naissance_1900("80 ans et plus"),
                ("Le sexe : 1 hommes, 2 femmes.", "INDIVIDU.IND_SEXE du titulaire", None),
            ]),
            ("Le montant", [
                ("Ce qui a été payé dans la période pour les enfants comptés.",
                 "DEBOURS.DEB_MONTANT, relié par PE_ID", "40"),
                ("Un paiement est un débours de paiement payé, par titre de paiement (D) ou sans titre (P). "
                 "Jamais le net à payer du titre.", "DEBOURS.DEB_TYPE = PM,\nDEB_STATUT = D ou P", None),
                ("Un paiement compte dans le mois qu'il paie, pas dans celui où il est saisi.",
                 "DEBOURS.DEB_DATE_EFFET", None),
                ("L'année = la somme des mois. Montant moyen = montant ÷ effectif.", "calcul", None),
            ]),
            ("Les cas particuliers", [
                ("Titulaire décédé dont les enfants touchent encore par un tuteur : il reste l'allocataire, "
                 "à l'âge qu'il aurait.", "DOSSIER.IND_ID ; IND_DATE_DECES non regardée", "39"),
                ("À côté de T41, un tableau des enfants comptés par tranche d'âge (moins de 5 ans, 5-9, 10-14, "
                 "15-19, 20-21 ans) et par sexe, avec le montant.",
                 "PRESTATION_ESP.IND_ID_PREST (l'enfant), INDIVIDU", "42"),
            ]),
            _decoupage("nouveaux allocataires", "T41 de l'année d'avant ;\nentrées calculées à part"),
        ],
        "chiffres": (
            "Avec ces règles, au 31/12/2024 : 98 048 allocataires, pour 97 378 dans l'annuaire CNSS 2024 (+0,7 %), "
            "et 271 577 enfants comptés pour 268 481 (+1,2 %) ; sans la condition de paiement, +5,4 % "
            "d'allocataires. Montant payé pour les enfants comptés : 7,69 Md, pour 8,24 Md publiés (−6,7 %)."
        ),
    },
}
