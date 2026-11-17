"""Règles par tableau : famille T42–T49, les nouveaux bénéficiaires de l'année.

Même format que T32 (regles_par_tableau.py) : pour chaque tableau, titre, chapeau, groupes
de règles (texte, champs de la base, point à valider ou None) et chiffres.

Sources : analyses/regles_cipres/fiche_regles.md (règles 1 à 16 et 61 à 67),
analyses/prestations/regles_t42_t49_nouveaux.md (règles, cas, « Face à l'annuaire CNSS 2024 »),
t42_t49_requete_proposee.sql, lot7_nouveaux_beneficiaires.sql (P82 à P91),
annuaire_cnss_2024_prestations.md, points_a_valider.md (« Tous les tableaux, un par un »).
Rédigé le 15/11/2026.
"""

_FIN_CHAPEAU = (
    "Voici chaque règle appliquée et le champ de la base qui la porte. Une remarque ? "
    "Écrivez-la dans la dernière colonne. En orange : règle encore en question (n° du point à valider)."
)


def _chapeau(phrase):
    return phrase + "\n" + _FIN_CHAPEAU


# ── Règles communes ─────────────────────────────────────────────────────────

def _nouveau(objet, codes, point="4"):
    """Qui est un nouveau : la première fois, pas les révisions, les sortis comptent."""
    return [
        (f"Nouveau : sa toute première {objet} est saisie dans la période.",
         f"PRESTATION_ESP.PE_DATE_INSERT de la première\n{codes} de la personne", point),
        ("Une révision ou un recalcul n'est pas une entrée : une nouvelle prestation saisie "
         "pour qui en avait déjà une ne compte pas.",
         "première PE_DATE_INSERT avant la période : écartée", None),
        ("Un nouveau mort ou sorti avant la fin de la période compte quand même : "
         "une entrée reste une entrée.",
         "INDIVIDU.IND_DATE_DECES, PE_DATE_RADIATION :\nnon regardées", None),
    ]


_COORDINATION_COMPTEE = (
    "Une pension de coordination payée par l'autre caisse compte aussi.",
    "DOSSIER.DOS_CATEGORIE = COOR_AC : comptée", None)


def _periode(libre):
    if libre:
        texte = ("Chaque mois, chaque trimestre et l'année, ou entre deux dates choisies. Une personne "
                 "n'entre qu'une fois : les effectifs des mois s'additionnent pour faire l'année.")
    else:
        texte = ("Chaque mois, chaque trimestre et l'année. Une personne n'entre qu'une fois : "
                 "les effectifs des mois s'additionnent pour faire l'année.")
    return [
        (texte, "paramètres de la requête : debut, fin", None),
        ("Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi "
         "plus tard n'entre que dans les photos suivantes.", "copie de la base de fin janvier N+1", None),
    ]


def _age_sexe(tranches, grille, point_age=None, point_sexe=None):
    texte_age = ("L'âge = l'année de l'entrée moins l'année de naissance, sans regarder le jour. "
                 "Sur une année civile, c'est l'année du tableau.")
    if point_age:
        texte_age += " Sur une période à cheval sur deux années, chacun prend l'âge de l'année de son entrée."
    return [
        (texte_age, "INDIVIDU.IND_DATE_NAISSANCE ;\nannée de PE_DATE_INSERT", point_age),
        (f"Les tranches sont celles de la maquette CIPRES : {tranches}.", f"grille de la requête : {grille}", None),
        ("Un âge négatif, vide, ou de 120 ans et plus (comme la date par défaut en 1900) "
         "va en « âge inconnu ».", "IND_DATE_NAISSANCE vide, après l'année,\nou âge ≥ 120", None),
        ("Le sexe : 1 hommes, 2 femmes, celui de la fiche de la personne.", "INDIVIDU.IND_SEXE", point_sexe),
    ]


def _montant(objet, point_somme="108"):
    """Le montant : mois payés de la période, débours D ou P, rappel à part."""
    return [
        (f"Ce qui est payé pour les mois de la période, pour les {objet} du tableau saisies dans la période.",
         "DEBOURS.DEB_MONTANT, relié par PE_ID", None),
        ("Un paiement est un débours de paiement payé, par titre de paiement (D) ou sans titre (P). "
         "Jamais le net à payer du titre.", "DEBOURS.DEB_TYPE = PM,\nDEB_STATUT = D ou P", None),
        ("Un paiement compte dans le mois qu'il paie, pas dans celui où il est saisi.",
         "DEBOURS.DEB_DATE_EFFET", None),
        ("Le rappel des mois d'avant la période, payé avec le premier paiement, se montre à part, "
         "hors du montant.", "DEB_DATE_EFFET avant le début de la période", "45"),
        ("Les montants des mois ne s'additionnent pas pour faire l'année : un nouveau entré en mars "
         "pèse un mois dans le tableau de mars, dix dans celui de l'année.", "calcul", point_somme),
    ]


_ACCESSOIRES = (
    "Les accessoires (bonification, majorations pour enfant, charges du ménage) ne font pas "
    "un nouveau pensionné de plus.", "TPE_CODE = RBEC, RME, RME-COL, RCCM : écartés", None)

_ALLOCATION_SURVIVANTS = (
    "L'allocation de survivants, versée une seule fois, n'est pas une pension : elle va dans T51.",
    "TPE_CODE = RASC, RASO, RASA : écartées", None)

_COORDINATION_MONTANT = (
    "Pension de coordination payée par l'autre caisse : comptée, sans montant. Seule la part "
    "payée par la CNSS entre dans le montant.", "DOSSIER.DOS_CATEGORIE = COOR_AC", None)

_RACHAT = (
    "Le rachat de rente (un capital versé à la place de la rente) n'entre pas dans le montant : "
    "il est payé par un ajustement, pas par un paiement.", "AJUSTEMENT.TAJ_CODE = RR : écarté", None)

_RENTES_INCOMPLETES = (
    "Une part des paiements de rentes manque dans la base : on prend ce qu'elle contient, "
    "et le montant porte la mention « incomplet ».", "DEBOURS des dossiers DOSSIER.TDOS_CODE = A", "51")

_DR_AGENCE = (
    "Chaque chiffre se découpe par direction régionale et par agence, celles du dossier.",
    "DOSSIER.DR_NO, DOSSIER.SP_NO", None)


def _ayant_droit(qui, point="71"):
    return (f"On compte l'ayant droit lui-même ({qui}), à son âge, pas l'assuré décédé titulaire du "
            "dossier. « Idem. Tab 41 » du recueil veut seulement dire « même périodicité ».",
            "PRESTATION_ESP.IND_ID_PREST,\npas DOSSIER.IND_ID", point)


def _une_fois(autres):
    return (f"Une fois dans le tableau, même avec deux prestations. La même personne peut être aussi "
            f"dans {autres} : on n'additionne jamais deux tableaux.",
            "PRESTATION_ESP.IND_ID_PREST, comptée une fois", None)


def _controle(stock, nouveaux):
    return (f"Contrôle : stock de {stock} en fin de période = stock d'avant + nouveaux de {nouveaux} "
            "− décès − autres sorties.", f"{stock} de la période d'avant, {nouveaux}", None)


# ── Les tableaux ────────────────────────────────────────────────────────────

TABLEAUX = {
    "T42": {
        "titre": "T42 — Les nouveaux retraités de l'année et leurs services passés : les règles de la BI",
        "chapeau": _chapeau(
            "T42 compte les personnes qui reçoivent pour la première fois, dans la période, une pension de "
            "vieillesse ou de retraite anticipée, par tranche d'âge de 5 ans et par sexe, avec le montant "
            "payé et le total de leurs services passés (mois cotisés)."),
        "groupes": [
            ("Ce qui est compté", [
                ("La pension de vieillesse et la retraite anticipée, comptées ensemble : T42 n'a pas "
                 "de ligne par type de pension.", "PRESTATION_ESP.TPE_CODE = RPV, RPRA", "116"),
                ("La personne qui a droit, une seule fois, même avec deux pensions saisies dans la période.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", None),
                _ACCESSOIRES,
                ("L'allocation de vieillesse, versée une seule fois, va dans T50. La pension d'invalidité "
                 "n'a pas de tableau des nouveaux.", "TPE_CODE = RAV, RPI : écartées", None),
            ]),
            ("Qui est un nouveau", _nouveau("pension de vieillesse ou de retraite anticipée", "RPV ou RPRA")
             + [_COORDINATION_COMPTEE]),
            ("La période", _periode(libre=False)),
            ("L'âge et le sexe", _age_sexe(
                "moins de 50 ans, puis 5 ans par 5 ans, jusqu'à « 100 ans et plus »", "de 50 à 100 ans",
                point_sexe="44")),
            ("Le montant", _montant("pensions", point_somme=None) + [
                ("La pension seule : l'argent des accessoires n'entre pas dans le montant.",
                 "seulement les débours des pensions RPV et RPRA", None),
                ("Pension de coordination payée par l'autre caisse : comptée, sans montant. Payée par la "
                 "CNSS : le montant est ce que la CNSS a payé.",
                 "DOSSIER.DOS_CATEGORIE = COOR_AC, COOR_CNSS", None),
            ]),
            ("Les services passés", [
                ("Services passés = les mois cotisés de la dernière pension saisie dans la période, "
                 "de 1 à 600 mois (au-delà, comme 999, la valeur est écartée), additionnés par tranche "
                 "et par sexe, en mois.", "PRESTATION_ESP.PE_MOIS_COTISATION,\nde 1 à 600", "16"),
                ("Pension de coordination : on retire les mois cotisés dans l'autre caisse. Il reste "
                 "la part CNSS.", "PE_MOIS_COTISATION − PE_COT_AUTRE ;\nDOS_CATEGORIE = COOR_AC, COOR_CNSS",
                 "11"),
            ]),
            ("Le découpage et le contrôle", [
                _DR_AGENCE,
                _controle("T32", "T42"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 2 463 nouveaux retraités, dont 234 payés par l'autre caisse, pour "
            "2 489 dans l'annuaire CNSS 2024 (2 486 en vieillesse et 3 en retraite anticipée) : −1,0 %. "
            "Par tranche de 5 ans, l'âge retombe de 55 à 69 ans (55 – 59 ans : 1 450 pour 1 429, révisions "
            "comprises) ; le sexe s'écarte (641 femmes pour 527). L'annuaire ne publie ni montant ni "
            "services passés des nouveaux."
        ),
    },

    "T43": {
        "titre": "T43 — Les nouveaux veufs et veuves pensionnés de l'année : les règles de la BI",
        "chapeau": _chapeau(
            "T43 compte les personnes qui reçoivent pour la première fois, dans la période, une pension de "
            "veuve ou de veuf, par tranche d'âge et par sexe, avec le montant payé."),
        "groupes": [
            ("Ce qui est compté", [
                ("La pension de veuve ou de veuf.", "PRESTATION_ESP.TPE_CODE = RPSV (veuve),\nRPSF (veuf)",
                 None),
                _ayant_droit("la veuve, le veuf"),
                _une_fois("T49"),
                _ALLOCATION_SURVIVANTS,
                _ACCESSOIRES,
            ]),
            ("Qui est un nouveau", _nouveau("pension de veuve ou de veuf", "RPSV ou RPSF")
             + [_COORDINATION_COMPTEE]),
            ("La période", _periode(libre=True)),
            ("L'âge et le sexe", [
                _age_sexe("", "", point_age="107")[0],
                ("Les tranches sont celles de la maquette CIPRES : moins de 20 ans, puis 5 ans par 5 ans, "
                 "jusqu'à « 60 ans et plus ».", "grille de la requête : de 20 à 60 ans", "113"),
            ] + _age_sexe("", "")[2:]),
            ("Le montant", _montant("pensions") + [_COORDINATION_MONTANT]),
            ("Le découpage et le contrôle", [
                _DR_AGENCE,
                _controle("T34", "T43"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 771 nouveaux veufs et veuves, pour 766 dans l'annuaire CNSS 2024 "
            "(732 veuves et 34 veufs) : +0,7 %. Dans la base, 413 des 772 veufs et veuves saisis en 2024 "
            "(53 %) tombent dans « 60 ans et plus ». L'annuaire ne publie ni leur âge ni leur montant."
        ),
    },

    "T44": {
        "titre": "T44 — Les nouveaux orphelins pensionnés de l'année : les règles de la BI",
        "chapeau": _chapeau(
            "T44 compte les orphelins qui reçoivent pour la première fois, dans la période, une pension "
            "d'orphelin, par tranche d'âge et par sexe, avec le montant payé."),
        "groupes": [
            ("Ce qui est compté", [
                ("La pension d'orphelin.", "PRESTATION_ESP.TPE_CODE = RPSE", None),
                _ayant_droit("l'orphelin, pas le tuteur qui touche l'argent"),
                _une_fois("T46"),
                _ALLOCATION_SURVIVANTS,
                _ACCESSOIRES,
            ]),
            ("Qui est un nouveau", _nouveau("pension d'orphelin", "RPSE") + [_COORDINATION_COMPTEE]),
            ("La période", _periode(libre=True)),
            ("L'âge et le sexe", [
                _age_sexe("", "", point_age="107")[0],
                ("Les tranches sont celles de la maquette CIPRES : moins de 5 ans, 5 – 9, 10 – 14, "
                 "15 – 19 ans, « 20 ans et plus ».", "grille de la requête : de 5 à 20 ans", None),
                ("Pas d'âge limite pour un nouvel orphelin : un orphelin de 25 ans ou plus va dans "
                 "« 20 ans et plus ».", "aucune limite d'âge sur RPSE", "10"),
            ] + _age_sexe("", "")[2:]),
            ("Le montant", _montant("pensions", point_somme=None) + [_COORDINATION_MONTANT]),
            ("Le découpage et le contrôle", [
                _DR_AGENCE,
                _controle("T35", "T44"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 773 nouveaux orphelins, pour 769 dans l'annuaire CNSS 2024 : +0,5 %. "
            "2 d'entre eux ont de 25 à 29 ans. L'annuaire ne publie ni leur âge ni leur montant."
        ),
    },

    "T45": {
        "titre": "T45 — Les nouveaux rentiers d'accident du travail de l'année : les règles de la BI",
        "chapeau": _chapeau(
            "T45 compte les personnes qui reçoivent pour la première fois, dans la période, une rente "
            "d'accident du travail ou de maladie professionnelle, la victime ou un ayant droit, par tranche "
            "d'âge et par sexe, avec le montant payé."),
        "groupes": [
            ("Ce qui est compté", [
                ("Toutes les rentes : celle de la victime (rente viagère) et celles de ses ayants droit "
                 "(veuve, veuf, orphelin, ascendant).",
                 "PRESTATION_ESP.TPE_CODE = ARI, ARSV,\nARSF, ARSE, ARSA", "46"),
                ("L'allocation d'incapacité (taux sous 15 %) n'est pas une rente : elle va dans T53.",
                 "TPE_CODE = AAI : écartée", None),
                _ayant_droit("la veuve, le veuf, l'orphelin, le parent"),
                _une_fois("T46 à T49"),
            ]),
            ("Qui est un nouveau", _nouveau("rente d'accident du travail (tous types confondus)",
                                            "ARI, ARSV, ARSF, ARSE ou ARSA") + [
                ("Une personne qui avait déjà une rente d'un autre type (orpheline en 2015, victime en "
                 "2024) n'est pas nouvelle dans T45.", "première rente de la personne, tous codes", None),
            ]),
            ("La période", _periode(libre=True)),
            ("L'âge et le sexe", _age_sexe(
                "moins de 15 ans, puis 5 ans par 5 ans, jusqu'à « 100 ans et plus »", "de 15 à 100 ans")),
            ("Le montant", _montant("rentes") + [_RACHAT, _RENTES_INCOMPLETES]),
            ("Le découpage et le contrôle", [
                _DR_AGENCE,
                ("Contrôle : T45 = T46 + T47 + T48 + T49, si personne n'entre dans deux de ces tableaux "
                 "la même année et si personne n'avait déjà une autre rente.", "T46, T47, T48, T49", None),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 102 nouveaux rentiers, pour 105 dans l'annuaire CNSS 2024 "
            "(tableaux 75 et 77 additionnés ; l'annuaire ne publie pas ce total) : −2,9 % ; 131 si l'on "
            "comptait les recalculs. Les paiements de rentes de la base sont 12 à 25 % sous l'annuaire : "
            "les montants seront trop bas."
        ),
    },

    "T46": {
        "titre": "T46 — Les nouveaux orphelins rentiers d'accident du travail : les règles de la BI",
        "chapeau": _chapeau(
            "T46 compte les orphelins qui reçoivent pour la première fois, dans la période, une rente "
            "d'orphelin d'accident du travail, par tranche d'âge et par sexe, avec le montant payé."),
        "groupes": [
            ("Ce qui est compté", [
                ("La rente d'orphelin d'accident du travail.", "PRESTATION_ESP.TPE_CODE = ARSE", None),
                _ayant_droit("l'orphelin, pas le tuteur qui touche l'argent"),
                _une_fois("T45"),
            ]),
            ("Qui est un nouveau", _nouveau("rente d'orphelin", "ARSE")),
            ("La période", _periode(libre=True)),
            ("L'âge et le sexe", [
                _age_sexe("", "", point_age="107")[0],
                ("Les tranches sont celles de la maquette CIPRES : moins de 5 ans, 5 – 9, 10 – 14, "
                 "15 – 19 ans, « 20 ans et plus ».", "grille de la requête : de 5 à 20 ans", None),
                ("Pas d'âge limite pour un nouvel orphelin : un orphelin de 25 ans ou plus va dans "
                 "« 20 ans et plus ».", "aucune limite d'âge sur ARSE", "10"),
            ] + _age_sexe("", "")[2:]),
            ("Le montant", _montant("rentes", point_somme=None) + [_RACHAT, _RENTES_INCOMPLETES]),
            ("Le découpage et le contrôle", [
                _DR_AGENCE,
                _controle("T39", "T46"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 44 nouveaux orphelins de rente, pour 44 dans l'annuaire CNSS 2024 "
            "(tableau 77) : exact. L'annuaire ne publie ni leur âge ni leur montant."
        ),
    },

    "T47": {
        "titre": "T47 — Les nouveaux ascendants rentiers d'accident du travail : les règles de la BI",
        "chapeau": _chapeau(
            "T47 compte les pères et mères d'une victime décédée qui reçoivent pour la première fois, dans "
            "la période, une rente d'ascendant d'accident du travail, par tranche d'âge et par sexe, avec "
            "le montant payé."),
        "groupes": [
            ("Ce qui est compté", [
                ("La seule rente d'ascendant d'accident du travail. La pension d'ascendant de la branche "
                 "des pensions n'a pas de tableau des nouveaux.",
                 "PRESTATION_ESP.TPE_CODE = ARSA ;\nRPA écartée", "47"),
                _ayant_droit("le père ou la mère"),
                _une_fois("T45"),
            ]),
            ("Qui est un nouveau", _nouveau("rente d'ascendant", "ARSA") + [
                ("Chaque ascendant dont la première rente est saisie dans la période est compté, sans "
                 "autre filtre (date d'effet de la rente, sortie, lien avec la victime).",
                 "PE_DATE_EFFET, PE_DATE_RADIATION :\nnon regardées", "114"),
            ]),
            ("La période", _periode(libre=True)),
            ("L'âge et le sexe", _age_sexe(
                "moins de 50 ans, puis 5 ans par 5 ans, jusqu'à « 100 ans et plus »", "de 50 à 100 ans")),
            ("Le montant", _montant("rentes", point_somme=None) + [_RACHAT, _RENTES_INCOMPLETES]),
            ("Le découpage et le contrôle", [
                _DR_AGENCE,
                _controle("T40", "T47"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 15 nouveaux ascendants de rente, pour 13 dans l'annuaire CNSS 2024 "
            "(tableau 77) : +15,4 %, à la saisie comme à la notification. L'annuaire ne publie ni leur âge "
            "ni leur montant."
        ),
    },

    "T48": {
        "titre": "T48 — Les nouvelles victimes rentières et leur degré moyen d'IPP : les règles de la BI",
        "chapeau": _chapeau(
            "T48 compte les victimes d'accident du travail qui reçoivent pour la première fois, dans la "
            "période, une rente viagère, par tranche d'âge et par sexe, avec le montant payé et le degré "
            "moyen d'incapacité permanente partielle (IPP)."),
        "groupes": [
            ("Ce qui est compté", [
                ("La rente viagère de la victime elle-même, versée à vie.", "PRESTATION_ESP.TPE_CODE = ARI",
                 None),
                ("L'allocation d'incapacité (taux sous 15 %) n'est pas une rente : elle va dans T53.",
                 "TPE_CODE = AAI : écartée", None),
                ("La victime, une seule fois, même si sa rente est recalculée dans la période. Elle est "
                 "aussi dans T45 : on n'additionne jamais deux tableaux.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", None),
            ]),
            ("Qui est un nouveau", _nouveau("rente viagère", "ARI", point="48")),
            ("La période", _periode(libre=True)),
            ("L'âge et le sexe", _age_sexe(
                "moins de 20 ans, puis 5 ans par 5 ans, jusqu'à « 65 ans et plus »", "de 20 à 65 ans",
                point_age="107") + [
                ("La maquette n'a pas de colonne Total : hommes et femmes seulement.",
                 "colonne Total de la requête masquée", None),
            ]),
            ("Le montant", _montant("rentes", point_somme=None) + [_RACHAT, _RENTES_INCOMPLETES]),
            ("Le degré moyen d'IPP", [
                ("Degré moyen d'IPP = somme des taux d'incapacité des nouveaux de la tranche, divisée par "
                 "leur nombre, pour chaque sexe.", "PRESTATION_ESP.PE_TAUX_INCAPACITE,\nmoyenne", None),
                ("Le taux d'une victime est celui de sa dernière rente viagère saisie dans la période.",
                 "dernière ARI par PE_DATE_INSERT", "115"),
                ("Le taux va de 15 à 100 % et il est toujours saisi.", "PE_TAUX_INCAPACITE de 15 à 100", None),
            ]),
            ("Le découpage et le contrôle", [
                _DR_AGENCE,
                _controle("T37", "T48"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 39 nouvelles victimes rentières, pour 44 dans l'annuaire CNSS 2024 "
            "(tableau 75) : −11,4 % ; on en aurait 57 en comptant les rentes notifiées dans l'année, 68 avec "
            "toutes les rentes saisies. Taux d'incapacité des rentes viagères saisies en 2024, révisions comprises : de 15 à 70 %, "
            "24 en médiane. L'annuaire ne publie ni montant ni degré d'IPP des nouveaux."
        ),
    },

    "T49": {
        "titre": "T49 — Les nouveaux veufs et veuves rentiers d'accident du travail : les règles de la BI",
        "chapeau": _chapeau(
            "T49 compte les veufs et veuves d'une victime décédée qui reçoivent pour la première fois, dans "
            "la période, une rente de survivant d'accident du travail, par tranche d'âge et par sexe, avec "
            "le montant payé."),
        "groupes": [
            ("Ce qui est compté", [
                ("La rente de veuve ou de veuf d'accident du travail.",
                 "PRESTATION_ESP.TPE_CODE = ARSV (veuve),\nARSF (veuf)", None),
                _ayant_droit("la veuve, le veuf"),
                _une_fois("T45 et T43"),
            ]),
            ("Qui est un nouveau", _nouveau("rente de veuve ou de veuf", "ARSV ou ARSF")),
            ("La période", _periode(libre=True)),
            ("L'âge et le sexe", _age_sexe(
                "moins de 20 ans, puis 5 ans par 5 ans, jusqu'à « 65 ans et plus »", "de 20 à 65 ans",
                point_age="107")),
            ("Le montant", _montant("rentes", point_somme=None) + [_RACHAT, _RENTES_INCOMPLETES]),
            ("Le découpage et le contrôle", [
                _DR_AGENCE,
                _controle("T38", "T49"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 4 nouvelles veuves de rente, pour 4 veuves et aucun veuf dans "
            "l'annuaire CNSS 2024 (tableau 77) : exact. L'annuaire ne publie ni leur âge ni leur montant."
        ),
    },
}
