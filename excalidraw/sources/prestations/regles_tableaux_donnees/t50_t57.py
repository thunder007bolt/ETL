"""Règles par tableau : famille T50–T57 (l'évolution sur cinq ans).

T50 allocations de vieillesse (RAV, avec les services passés moyens), T51 allocations de survivants
(RASC, RASO, RASA), T52 IJ AT/MP (AIJ), T53 allocations d'incapacité (AAI), T54 IJ de maternité
(MIJ), T55 allocations prénatales (MAP, FAP), T56 prestations servies par branche, T57 décédés
par branche. Même format que T32 dans regles_par_tableau.py.

Sources : analyses/regles_cipres/fiche_regles.md (règles 1 à 16 et 68 à 77) ;
analyses/prestations/regles_t50_t57_evolution.md (règles, cas, « Face à l'annuaire CNSS 2024 ») ;
analyses/prestations/t50_t57_requete_proposee.sql (champs) ; lot8_evolution_5_ans.sql (P93) ;
analyses/prestations/points_a_valider.md (points en orange).
"""

_CHAPEAU_FIN = (
    "Voici chaque règle appliquée et le champ de la base qui la porte. Une remarque ? "
    "Écrivez-la dans la dernière colonne. En orange : règle encore en question (n° du point à valider)."
)

# Règles communes, reprises telles quelles dans plusieurs tableaux
_PAIEMENT = ("Un paiement est un débours de paiement payé, par titre de paiement (D) ou sans titre (P). "
             "Jamais le net à payer du titre.", "DEBOURS.DEB_TYPE = PM,\nDEB_STATUT = D ou P", None)
_MOIS_PAYE = ("Un paiement compte dans le mois qu'il paie, pas dans celui où il est saisi. "
              "Un rappel versé en février pour octobre compte en octobre.", "DEBOURS.DEB_DATE_EFFET", None)
_PAS_DE_MOYENNE = ("L'effectif (les personnes notifiées) et le montant (les paiements de l'année) ne portent "
                   "pas sur les mêmes personnes : la BI ne calcule pas de montant moyen.",
                   "calcul : aucun montant moyen", None)
_PHOTO = ("Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi plus tard "
          "n'entre que dans les photos suivantes.", "copie de la base de fin janvier N+1", None)
_ANNEES_PASSEES = ("Les quatre années d'avant reprennent les chiffres de leur photo officielle. "
                   "Une correction se signale par une note.", "résultats gardés de chaque exercice", None)
_ANNUEL = ("Le tableau sort une fois par an.", "paramètre de la requête : l'exercice N", None)
_DECOUPAGE = ("Chaque chiffre se découpe par direction régionale et par agence, celles du dossier.",
              "DOSSIER.DR_NO, DOSSIER.SP_NO", None)


def _detail(quoi):
    return (f"La BI garde le détail de chaque chiffre ({quoi}), sans nom ni numéro d'assuré. "
            "Chacun le voit selon son profil.", "une ligne par personne et par paiement", None)


def _cinq_ans(total):
    return (f"Une ligne par année, des cinq dernières années (N−4 à N), {total}",
            "années N−4 à N", None)


def _annee_notifiee(qui, quoi, point):
    return (f"{qui} compte l'année où {quoi} lui est notifiée, c'est-à-dire liquidée. "
            "Ni la saisie ni le paiement ne comptent.", "PRESTATION_ESP.PE_DATE_NOTIFICATION", point)


def _plusieurs_annees(quoi):
    return (f"Elle compte une fois dans l'année, mais peut compter plusieurs années : {quoi}",
            "une fois par année de notification", None)


def _montant_paye(quoi):
    return (f"Le montant : tout ce qui est payé dans l'année pour {quoi}, même notifiés une année d'avant.",
            "DEBOURS.DEB_MONTANT, relié par PE_ID", None)


_SEXE_HF = ("Le sexe : 1 hommes, 2 femmes, en trois blocs HOMMES, FEMMES et TOTAL, comme la maquette CIPRES.",
            "INDIVIDU.IND_SEXE", None)

_PAIEMENTS_AT_MANQUANTS = (
    "On prend les paiements que la base contient. Une partie de l'argent versé pour les accidents "
    "du travail n'y est pas.", "DEBOURS des dossiers DOSSIER.TDOS_CODE = A", "51")

_MONTANT_LIQUIDE_SOMME = (
    "Le montant calculé à la liquidation n'a pas été trouvé dans la base : en attendant, le montant "
    "liquidé est la somme des paiements payés de ces prestations.",
    "DEBOURS, statut D ou P ;\nmontant de la liquidation : non trouvé\n(table ETAPE, TEA_CODE = CIP ?)", "134")


TABLEAUX = {
    "T50": {
        "titre": "T50 — Les allocations de vieillesse sur cinq ans : les règles de la BI",
        "chapeau": (
            "T50 compte, pour chacune des cinq dernières années, les personnes qui ont reçu une allocation "
            "de vieillesse, par sexe, avec le montant payé et le nombre moyen de mois cotisés (les services passés).\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("La branche des pensions.", "DOSSIER.TDOS_CODE = V", None),
                ("L'allocation de vieillesse : une somme versée en une seule fois, à la place d'une pension, "
                 "à l'assuré qui n'a pas assez cotisé pour une pension.", "PRESTATION_ESP.TPE_CODE = RAV", None),
                ("La personne qui a droit, une seule fois dans l'année, même avec deux allocations.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", "50"),
                ("Le sexe : 1 hommes, 2 femmes, en deux blocs HOMMES et FEMMES, comme la maquette CIPRES.",
                 "INDIVIDU.IND_SEXE", None),
            ]),
            ("L'année d'une personne", [
                _annee_notifiee("Une personne", "son allocation", None),
                ("Une allocation demandée une année et notifiée l'année suivante compte l'année de la notification.",
                 "PE_DATE_NOTIFICATION, pas la date de la demande", None),
            ]),
            ("Le montant", [
                _montant_paye("les allocations de vieillesse"),
                _PAIEMENT,
                _MOIS_PAYE,
                _PAS_DE_MOYENNE,
            ]),
            ("Les services passés", [
                ("Les services passés d'un allocataire : les mois cotisés de sa dernière allocation notifiée "
                 "dans l'année.", "PRESTATION_ESP.PE_MOIS_COTISATION", "16"),
                ("Allocation de coordination (deux caisses) : on garde la part CNSS, les mois cotisés moins "
                 "ceux de l'autre caisse.", "PE_MOIS_COTISATION − PE_COT_AUTRE ;\nDOSSIER.DOS_CATEGORIE", "11"),
                ("Seuls les mois de 1 à 600 (50 ans) entrent dans la moyenne.",
                 "PE_MOIS_COTISATION entre 1 et 600", "128"),
                ("La moyenne se fait par sexe. La ligne TOTAL donne la moyenne des cinq ans, chaque année "
                 "pesant selon son nombre d'allocataires.", "calcul", None),
            ]),
            ("Les cinq ans", [
                _cinq_ans("et une ligne TOTAL."),
                _PHOTO,
                _ANNEES_PASSEES,
                _ANNUEL,
            ]),
            ("Le découpage", [
                _DECOUPAGE,
                _detail("une ligne par allocataire et par paiement"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 831 allocataires de vieillesse (690 hommes, 141 femmes), pour 833 dans "
            "l'annuaire CNSS 2024 : −0,24 %. Montant payé au mois payé : 517,33 M, pour 497,73 M publiés (+3,9 %). "
            "Services passés : 95,5 mois en moyenne (part CNSS), non publiés par l'annuaire."
        ),
    },
    "T51": {
        "titre": "T51 — Les allocations de survivants sur cinq ans : les règles de la BI",
        "chapeau": (
            "T51 compte, pour chacune des cinq dernières années, les veuves, veufs, orphelins et ascendants "
            "qui ont reçu une allocation de survivants, par sexe, avec le montant payé.\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("La branche des pensions.", "DOSSIER.TDOS_CODE = V", None),
                ("L'allocation de survivants : une somme versée en une seule fois aux survivants d'un assuré "
                 "décédé, à la place d'une pension. Elle a trois parts : celle du conjoint, de l'orphelin "
                 "et de l'ascendant.", "PRESTATION_ESP.TPE_CODE = RASC, RASO, RASA", None),
                ("Les ascendants (le père ou la mère de l'assuré) comptent, comme le demande le recueil des besoins.",
                 "TPE_CODE = RASA", None),
                ("L'ancien code global de l'allocation ne se liquide plus : le tableau se lit sur les trois parts.",
                 "TPE_CODE = RAS : aucune en 2024", None),
                ("La personne qui a droit, une seule fois dans l'année : l'orphelin, pas son tuteur qui touche l'argent.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", "50"),
                _SEXE_HF,
            ]),
            ("L'année d'une personne", [
                _annee_notifiee("Une personne", "sa part d'allocation", "49"),
                ("Une part demandée une année et notifiée l'année suivante compte l'année de la notification.",
                 "PE_DATE_NOTIFICATION, pas la date de la demande", None),
            ]),
            ("Le montant", [
                _montant_paye("les allocations de survivants"),
                _PAIEMENT,
                _MOIS_PAYE,
                _PAS_DE_MOYENNE,
            ]),
            ("Les cinq ans", [
                _cinq_ans("et une ligne TOTAL des cinq ans, que la BI ajoute à la maquette CIPRES."),
                _PHOTO,
                _ANNEES_PASSEES,
                _ANNUEL,
            ]),
            ("Le découpage", [
                _DECOUPAGE,
                _detail("une ligne par bénéficiaire et par paiement"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 571 bénéficiaires (81 parts de conjoint, 472 d'orphelin, 18 d'ascendant) ; "
            "l'annuaire CNSS 2024 ne publie pas cet effectif. Montant au mois payé : 187,08 M, pour 197,91 M "
            "publiés (−5,5 %)."
        ),
    },
    "T52": {
        "titre": "T52 — Les indemnités journalières d'accident du travail sur cinq ans : les règles de la BI",
        "chapeau": (
            "T52 compte, pour chacune des cinq dernières années, les victimes d'accident du travail ou de maladie "
            "professionnelle qui ont reçu une indemnité journalière pendant leur arrêt, par sexe, avec le montant payé.\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("La branche des accidents du travail et maladies professionnelles.", "DOSSIER.TDOS_CODE = A", None),
                ("L'indemnité journalière : versée à la victime pendant son arrêt de travail.",
                 "PRESTATION_ESP.TPE_CODE = AIJ", None),
                ("La victime, une seule fois dans l'année, même si son arrêt est prolongé et qu'elle a "
                 "plusieurs indemnités.", "PRESTATION_ESP.IND_ID_PREST, comptée une fois", "50"),
                _SEXE_HF,
            ]),
            ("L'année d'une personne", [
                _annee_notifiee("Une victime", "une indemnité", "49"),
                ("Une indemnité notifiée l'année après l'arrêt compte l'année de sa notification "
                 "(une indemnité attend souvent plusieurs mois).", "PE_DATE_NOTIFICATION, pas la date de l'arrêt", None),
                _plusieurs_annees("une indemnité notifiée en 2023 et une autre en 2024 la font compter les deux années."),
            ]),
            ("Le montant", [
                _montant_paye("les indemnités journalières, toutes celles de la victime comprises"),
                _PAIEMENT,
                _MOIS_PAYE,
                _PAIEMENTS_AT_MANQUANTS,
                _PAS_DE_MOYENNE,
            ]),
            ("Les cinq ans", [
                _cinq_ans("et une ligne TOTAL des cinq ans, que la BI ajoute à la maquette CIPRES."),
                _PHOTO,
                _ANNEES_PASSEES,
                _ANNUEL,
            ]),
            ("Le découpage", [
                _DECOUPAGE,
                _detail("une ligne par victime et par paiement"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 195 victimes (371 indemnités) ; l'annuaire CNSS 2024 ne publie pas cet "
            "effectif. Montant au mois payé : 46,02 M, pour 65,42 M publiés (−30 %)."
        ),
    },
    "T53": {
        "titre": "T53 — Les allocations d'incapacité sur cinq ans : les règles de la BI",
        "chapeau": (
            "T53 compte, pour chacune des cinq dernières années, les victimes d'accident du travail qui ont reçu "
            "une allocation d'incapacité, par sexe, avec le montant payé.\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("La branche des accidents du travail et maladies professionnelles.", "DOSSIER.TDOS_CODE = A", None),
                ("L'allocation d'incapacité : versée à la victime dont l'incapacité est trop faible pour une rente.",
                 "PRESTATION_ESP.TPE_CODE = AAI", None),
                ("La victime, une seule fois dans l'année, même avec deux allocations.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", "50"),
                _SEXE_HF,
            ]),
            ("L'année d'une personne", [
                _annee_notifiee("Une victime", "son allocation", "49"),
                _plusieurs_annees("une allocation notifiée chaque année la fait compter chaque année."),
            ]),
            ("Le montant", [
                _montant_paye("les allocations d'incapacité"),
                _PAIEMENT,
                _MOIS_PAYE,
                _PAIEMENTS_AT_MANQUANTS,
                _PAS_DE_MOYENNE,
            ]),
            ("Les cinq ans", [
                _cinq_ans("et une ligne TOTAL des cinq ans, que la BI ajoute à la maquette CIPRES."),
                _PHOTO,
                _ANNEES_PASSEES,
                _ANNUEL,
            ]),
            ("Le découpage", [
                _DECOUPAGE,
                _detail("une ligne par victime et par paiement"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 53 victimes ; l'annuaire CNSS 2024 ne publie pas cet effectif. "
            "Montant au mois payé : 23,24 M, pour 20,03 M publiés (+16 %)."
        ),
    },
    "T54": {
        "titre": "T54 — Les indemnités de maternité sur cinq ans : les règles de la BI",
        "chapeau": (
            "T54 compte, pour chacune des cinq dernières années, les salariées qui ont reçu une indemnité "
            "journalière de maternité pendant leur congé, avec le montant liquidé.\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("La branche maternité, comptée avec les prestations familiales.", "DOSSIER.TDOS_CODE = M", None),
                ("L'indemnité journalière de maternité : versée à la salariée pendant son congé de maternité.",
                 "PRESTATION_ESP.TPE_CODE = MIJ", None),
                ("La mère, une seule fois dans l'année.", "PRESTATION_ESP.IND_ID_PREST, comptée une fois", "50"),
                ("Les femmes seulement : une colonne FEMMES, comme la maquette CIPRES.",
                 "INDIVIDU.IND_SEXE = 2", None),
            ]),
            ("L'année d'une personne", [
                _annee_notifiee("Une mère", "son indemnité", None),
                _plusieurs_annees("une indemnité notifiée chaque année la fait compter chaque année."),
            ]),
            ("Le montant liquidé", [
                ("Le montant est celui des indemnités notifiées dans l'année, quelle que soit la date des "
                 "paiements. Une indemnité notifiée en décembre, payée en décembre et en février : les deux "
                 "paiements vont dans l'année de la notification.",
                 "DEBOURS.DEB_MONTANT des MIJ dont\nPE_DATE_NOTIFICATION est dans l'année", "117"),
                _MONTANT_LIQUIDE_SOMME,
                _PAIEMENT,
            ]),
            ("Les cinq ans", [
                _cinq_ans("et une ligne TOTAL des cinq ans, comme l'a voulu l'atelier de Kaya en 2017."),
                _PHOTO,
                _ANNEES_PASSEES,
                _ANNUEL,
            ]),
            ("Le découpage", [
                _DECOUPAGE,
                _detail("une ligne par mère et par paiement"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 3 890 mères, pour 3 893 dans l'annuaire CNSS 2024 : −0,1 %. "
            "Le montant liquidé reste à mesurer ; les paiements de l'année font 2,75 Md, pour 2,87 Md publiés (−4,2 %)."
        ),
    },
    "T55": {
        "titre": "T55 — Les allocations prénatales sur cinq ans : les règles de la BI",
        "chapeau": (
            "T55 compte, pour chacune des cinq dernières années, les femmes enceintes qui ont reçu une allocation "
            "prénatale, avec le montant liquidé.\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("La maternité et les prestations familiales.", "DOSSIER.TDOS_CODE = M ou F", None),
                ("L'allocation prénatale : versée à la salariée enceinte, ou à l'épouse enceinte d'un salarié.",
                 "PRESTATION_ESP.TPE_CODE = MAP", None),
                ("L'ancien code des prestations familiales pour la même allocation compte aussi.",
                 "TPE_CODE = FAP", None),
                ("La femme, une seule fois dans l'année, même avec les deux codes.",
                 "PRESTATION_ESP.IND_ID_PREST, comptée une fois", "50"),
                ("Les femmes seulement : une colonne FEMMES, comme la maquette CIPRES.",
                 "INDIVIDU.IND_SEXE = 2", None),
            ]),
            ("L'année d'une personne", [
                _annee_notifiee("Une femme", "son allocation", None),
                _plusieurs_annees("une allocation notifiée chaque année la fait compter chaque année."),
            ]),
            ("Le montant liquidé", [
                ("Le montant est celui des allocations notifiées dans l'année, quelle que soit la date des paiements.",
                 "DEBOURS.DEB_MONTANT des MAP et FAP dont\nPE_DATE_NOTIFICATION est dans l'année", "117"),
                _MONTANT_LIQUIDE_SOMME,
                _PAIEMENT,
            ]),
            ("Les cinq ans", [
                _cinq_ans("et une ligne TOTAL des cinq ans, comme l'a voulu l'atelier de Kaya en 2017."),
                _PHOTO,
                _ANNEES_PASSEES,
                _ANNUEL,
            ]),
            ("Le découpage", [
                _DECOUPAGE,
                _detail("une ligne par bénéficiaire et par paiement"),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 3 912 femmes (3 857 avec le code de la maternité, 56 avec l'ancien code) ; "
            "l'annuaire CNSS 2024 ne publie pas cet effectif. Le montant liquidé reste à mesurer ; les paiements "
            "de l'année font 50,68 M, pour 52,79 M publiés (−4,0 %)."
        ),
    },
    "T56": {
        "titre": "T56 — Les prestations servies par branche sur cinq ans : les règles de la BI",
        "chapeau": (
            "T56 additionne, pour chacune des cinq dernières années, l'argent versé par branche : prestations "
            "familiales (PF), accidents du travail (AT/MP), vieillesse et survivants (AVS), et une ligne « Autres ».\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Les branches", [
                ("Une ligne par branche, PF, AT/MP et AVS, puis la ligne « Autres » et une ligne TOTAL.",
                 "lignes PF, AT/MP, AVS, AUTRES, TOTAL", None),
                ("La branche est celle du dossier : V pour l'AVS (la « branche pension » de la CNSS), "
                 "A pour l'AT/MP (les risques professionnels), F pour les PF.",
                 "DOSSIER.TDOS_CODE, porté par le débours", None),
                ("La maternité va dans les PF : indemnités de maternité et allocations prénatales.",
                 "DOSSIER.TDOS_CODE = M → PF", None),
            ]),
            ("Ce qui est compté", [
                ("Les paiements, lus dans l'application des prestations : un débours de paiement payé, par titre "
                 "de paiement (D) ou sans titre (P).",
                 "DEBOURS.DEB_TYPE = PM,\nDEB_STATUT = D ou P", None),
                ("Jamais le net à payer du titre de paiement : le montant de chaque débours.",
                 "DEBOURS.DEB_MONTANT", None),
                ("Les soins remboursés des accidents du travail.", "DEBOURS.DEB_TYPE = RB, statut D ou P", None),
                ("Les rachats de rente : le capital versé à une victime à la place de sa rente, en AT/MP.",
                 "DEBOURS.DEB_TYPE = AJ relié à\nAJUSTEMENT.TAJ_CODE = RR", None),
                ("Les accessoires des pensions (bonification, majorations pour enfant, charges du ménage) "
                 "vont dans l'AVS.", "TPE_CODE = RBEC, RME, RME-COL, RCCM", None),
                ("Les rappels de moins-perçus et les avances versés par une correction de paiement n'y sont "
                 "pas aujourd'hui.", "DEBOURS.DEB_TYPE = AJ,\nTAJ_CODE = MP, RC, AV : écartés", "33"),
                ("Pension de coordination payée par l'autre caisse : aucun montant. Seule la part payée par "
                 "la CNSS entre.", "DOSSIER.DOS_CATEGORIE = COOR_AC", None),
                ("Les montants viennent de l'application des prestations, pas de la comptabilité.",
                 "DEBOURS ; comptes 601, 602, 603, 605\n(V_BI_BALANCE_COMPTABLE) non lus", "123"),
            ]),
            ("La date", [
                _MOIS_PAYE,
                ("L'année = la somme des mois.", "calcul", None),
            ]),
            ("Ce qui manque dans la base", [
                ("La plupart des soins des accidents du travail sont payés hors de la base : on prend ce "
                 "qu'elle contient.", "DEBOURS.DEB_TYPE = RB,\ndossiers TDOS_CODE = A", "51"),
                ("Les frais médicaux de maternité ne sont pas dans la base : ils manquent dans la ligne PF.",
                 "aucun paiement de ces frais (DEBOURS)", "129"),
            ]),
            ("La ligne « Autres »", [
                ("La ligne « Autres » reçoit l'action sanitaire et sociale de la CNSS (ses centres de santé, "
                 "par exemple). Ce montant n'est pas dans la base des prestations.",
                 "montant fourni hors de la base", None),
                ("Elle reste vide tant que la DPC ne fournit pas le montant : sans donnée, la case reste vide, "
                 "jamais 0.", "À faire (DPC) : fournir le montant", "53"),
            ]),
            ("Les cinq ans", [
                ("Une colonne par année, des cinq dernières années (N−4 à N).", "années N−4 à N", None),
                _PHOTO,
                _ANNEES_PASSEES,
                _ANNUEL,
            ]),
            ("Le découpage", [
                _DECOUPAGE,
                _detail("une ligne par paiement"),
            ]),
        ],
        "chiffres": (
            "En 2024, au mois payé, sans les rachats de rente : AVS 47,24 Md (46,63 publiés, +1,3 %), PF 10,75 Md "
            "(11,18 publiés, −3,8 %), AT/MP 0,76 Md (1,07 publié, −28,9 %) ; les trois branches ensemble : −0,2 %. "
            "Rachats de rente : 21,5 M dans la base, 25,4 M publiés."
        ),
    },
    "T57": {
        "titre": "T57 — Les assurés décédés par branche sur cinq ans : les règles de la BI",
        "chapeau": (
            "T57 compte, pour chacune des cinq dernières années, les personnes décédées par branche : prestations "
            "familiales (PF), accidents du travail (AT/MP), vieillesse et survivants (AVS), avec un TOTAL.\n"
            + _CHAPEAU_FIN
        ),
        "groupes": [
            ("Ce qui est compté", [
                ("Les personnes mortes dans l'année, une fois par branche.", "INDIVIDU.IND_DATE_DECES", None),
                ("Chaque décédé porte sa qualité, assuré ou ayant droit. Par défaut, le tableau montre les "
                 "assurés ; un filtre montre les ayants droit, ou les deux.",
                 "colonne QUALITE, filtrée dans le rapport ;\npar défaut : assuré", None),
                ("Chaque branche se détaille par type de personne : retraité, rentier, sinistré, allocataire ; "
                 "veuve, veuf, orphelin, ascendant.", "type, tiré du code de la prestation", None),
            ]),
            ("Les assurés, branche par branche", [
                ("AVS : le retraité dont la pension de vieillesse, de retraite anticipée ou d'invalidité "
                 "est en cours à son décès.", "PRESTATION_ESP.TPE_CODE = RPV, RPRA, RPI", None),
                ("AT/MP : le rentier victime, dont la rente viagère est en cours à son décès.",
                 "PRESTATION_ESP.TPE_CODE = ARI", None),
                ("En cours au décès : commencée avant, et non radiée, ou radiée au plus tôt 31 jours avant.",
                 "PE_DATE_EFFET ≤ date du décès ;\nPE_DATE_RADIATION vide ou ≥ décès − 31 jours", "121"),
                ("AT/MP : le sinistré, une victime d'accident du travail morte dans l'année qui suit son accident.",
                 "DOSSIER.TDOS_CODE = A ;\nDOS_DATE_ACCIDENT ≤ décès < accident + 366 jours", "56"),
                ("PF : l'allocataire, titulaire d'un dossier dont une allocation familiale est en cours à son décès.",
                 "DOSSIER.IND_ID, TDOS_CODE = F ;\nPRESTATION_ESP.TPE_CODE = FAF", None),
                ("L'assuré mort en activité, sans pension ni rente à son nom, n'est pas compté, même si son "
                 "décès ouvre une pension à sa veuve.", "aucune prestation à son nom", "54"),
            ]),
            ("Les ayants droit (avec le filtre)", [
                ("La veuve, le veuf, l'orphelin ou l'ascendant titulaire d'une pension de survivant (AVS) ou "
                 "d'une rente de survivant (AT/MP), en cours à son décès.",
                 "TPE_CODE = RPSV, RPSF, RPSE, RPA ;\nARSV, ARSF, ARSE, ARSA", None),
                ("PF : seuls les allocataires sont comptés. Avec le filtre « ayant droit », la colonne PF reste vide.",
                 "DOSSIER.TDOS_CODE = F : titulaire seul", "119"),
            ]),
            ("Les colonnes et le TOTAL", [
                ("Colonnes PF, AT/MP, AVS et TOTAL : la colonne « Autres » de la maquette est remplacée par le TOTAL.",
                 "colonnes PF, AT_MP, AVS, TOTAL", None),
                ("Le TOTAL d'une année est la somme des branches : une personne dans deux branches "
                 "(un retraité aussi allocataire) y compte deux fois.", "TOTAL = PF + AT/MP + AVS", "120"),
                ("Une ligne TOTAL additionne les cinq années.", "ligne TOTAL", None),
            ]),
            ("L'année du décès et les cinq ans", [
                ("Une ligne par année, des cinq dernières années (N−4 à N).", "années N−4 à N", None),
                ("Un décès compte l'année où il a lieu, s'il est saisi avant la photo de fin janvier N+1. "
                 "Saisi plus tard, il n'entre que dans les photos suivantes.",
                 "INDIVIDU.IND_DATE_DECES ;\ncopie de la base de fin janvier N+1", None),
                _ANNEES_PASSEES,
                _ANNUEL,
            ]),
            ("Le découpage et les données personnelles", [
                _DECOUPAGE,
                ("La BI garde le détail de chaque chiffre (une ligne par décès), sans nom ni numéro d'assuré. "
                 "Chacun le voit selon son profil.", "une ligne par décédé et par branche", None),
                ("Les décès se lisent dans les tables de l'application. La vue des décédés préparée par la DSI "
                 "n'est pas lue : elle contient des données personnelles.",
                 "INDIVIDU, PRESTATION_ESP, DOSSIER ;\nvue V_PRESTATAIRES_DECEDES_BI non lue", None),
            ]),
        ],
        "chiffres": (
            "Dans la copie de la base du 02/02/2026, décès de 2024 : 622 retraités de droit direct, 15 rentiers "
            "victimes, 10 sinistrés, 194 allocataires. L'annuaire CNSS 2024 publie 451 retraités de droit direct "
            "et 21 rentiers victimes décédés, et 14 morts sur le coup ; la copie contient des décès saisis après "
            "la photo de l'annuaire."
        ),
    },
}
