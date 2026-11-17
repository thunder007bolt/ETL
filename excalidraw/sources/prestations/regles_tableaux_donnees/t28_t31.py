"""
Règles par tableau : famille T28–T31 (contrôles a posteriori et indus), au format du pilote T32
de regles_par_tableau.py.

T28 (pensions), T29 (accidents du travail et maladies professionnelles) et T30 (prestations
familiales et maternité) comptent les dossiers payés et les dossiers contrôlés a posteriori ;
T31 les prestations payées, les trop-perçus et les moins-perçus par branche et par mois.

Sources : analyses/regles_cipres/fiche_regles.md (règles 1 à 16 et 37 à 47) ;
analyses/prestations/regles_t28_t31_controles_indus.md ; t31_requete_proposee.sql ;
lot9_indus.sql (P102 à P106) ; lot11_controles_t28_t31.sql (P120, P121) ; lot1 (P13) ;
points_a_valider.md (points en orange : 79, 80, 82, 83, 84, 86, 87 ; T31 : 33, 88, 90 à 93).
"""

CHAPEAU_FIN = (
    "Voici chaque règle appliquée et le champ de la base qui la porte. Une remarque ? "
    "Écrivez-la dans la dernière colonne. En orange : règle encore en question (n° du point à valider)."
)


# ── T28 à T30 : ce qui est commun aux trois tableaux de contrôle ─────────────────────────────

def _dossier_paye(regle_82, base_82, paiement_extra=None):
    """Le groupe « Le dossier payé », avec la règle du point 82 propre au tableau."""
    paiement = (
        "Un dossier est payé s'il a reçu au moins un paiement dans la période. Un paiement est un "
        "débours de paiement payé, par titre de paiement (D) ou sans titre (P), compté dans le mois "
        "qu'il paie."
    )
    base = "DEBOURS.DEB_TYPE = PM, DEB_STATUT = D ou P,\nDEB_DATE_EFFET, relié par DOS_CODE"
    if paiement_extra:
        paiement += " " + paiement_extra[0]
        base += " ;\n" + paiement_extra[1]
    return ("Le dossier payé", [
        (paiement, base, None),
        (regle_82, base_82, "82"),
        ("Un dossier compte une fois sur chaque ligne où il est payé, et une seule fois au TOTAL.",
         "DOSSIER.DOS_CODE, compté une fois par ligne", None),
        ("Un dossier de coordination payé par l'autre caisse n'est pas un dossier payé : "
         "la CNSS ne lui verse rien.", "DOSSIER.DOS_CATEGORIE = COOR_AC : écarté", None),
    ])


CONTROLE = ("Le contrôle a posteriori", [
    ("Un contrôle a posteriori est une vérification du dossier enregistrée par la DAI (Direction "
     "de l'audit interne) après le paiement. Seul compte le contrôle d'un dossier déjà payé.",
     "DOSSIERS_CONTROLES.DOS_CODE ;\nà venir : vue V_BI_CONTROLE_POSTERIORI", None),
    ("La coche « vérifié » d'un paiement n'est pas un contrôle : elle est faite à la saisie.",
     "DEBOURS.DEB_VERIFIE : non lu", None),
    ("Un contrôle va sur la ligne de la prestation contrôlée. La table des contrôles ne donne que "
     "le dossier et le type de prestation, pas la prestation d'une personne précise.",
     "DOSSIERS_CONTROLES.TPE_CODE ;\npas de PE_ID dans la table", "79"),
    ("Tout contrôle enregistré compte, quel que soit son résultat.",
     "aucun tri sur le résultat", "80"),
    ("Un contrôle compte dans le trimestre de sa date. La colonne TOTAL additionne les quatre "
     "trimestres : un dossier contrôlé en février et en octobre y compte deux fois.",
     "DOSSIERS_CONTROLES.DC_DATE_CONTROLE ;\nannées impossibles (202, 203, 2055) écartées", None),
])

TAUX_CONTROLE = ("Le taux et sa norme", [
    ("Taux de contrôle = dossiers contrôlés ÷ dossiers payés × 100. Un contrôle sur un dossier "
     "non payé dans la période ne compte pas.", "calcul", None),
    ("Dans le taux, un dossier contrôlé deux fois dans la période compte une fois.",
     "DOSSIERS_CONTROLES.DOS_CODE, compté une fois", "84"),
    ("Norme : 5 % au moins. Le taux porte la mention « dans les normes » ou « hors normes ». "
     "La norme est un paramètre.", "paramètre de la requête", None),
])

DAI = ("Ce qu'il faut à la DAI", [
    ("La table des contrôles de l'application ne suffit pas : 115 contrôles en 2025, plus rien "
     "depuis juillet 2025. Elle n'a ni la prestation, ni la période contrôlée, ni le montant.",
     "DOSSIERS_CONTROLES : DC_ID, TDOS_CODE, DOS_CODE,\nTPE_CODE, DC_DATE_CONTROLE, AUD_ID, MCI_ID",
     None),
    ("À faire (DAI et DSI) : la DAI enregistre chaque contrôle ; la DSI les met dans une vue, une "
     "ligne par contrôle : dossier, branche, prestation contrôlée, date, résultat, montant de "
     "l'indu trouvé. Ni nom ni numéro bancaire.", "vue V_BI_CONTROLE_POSTERIORI (à faire)", None),
    ("Les textes libres d'un contrôle (remarque, recommandation) ne sont jamais lus. Le résultat "
     "d'un contrôle n'est visible que par les profils autorisés ; les autres voient les taux.",
     "DC_REMARQUE, DC_RECOMMANDE : non lus", None),
])


def _decoupage(code):
    return ("Le découpage et la périodicité", [
        (f"{code} se sort au trimestre, au semestre et à l'année. Un taux se recalcule sur les "
         "sommes de la période, jamais comme une moyenne de taux.", "paramètre de la requête", None),
        ("Les dossiers payés se comptent du 1er janvier à la fin de la période, les contrôles "
         "trimestre par trimestre : le taux du 4e trimestre est celui de l'année.",
         "paramètre de la requête", "83"),
        ("Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi plus "
         "tard n'entre que dans les photos suivantes.", "copie de la base de fin janvier N+1", None),
        ("Chaque chiffre se découpe par direction régionale et par agence, celles du dossier, "
         "et par type de prestation (les lignes).", "DOSSIER.DR_NO, DOSSIER.SP_NO", None),
    ])


def _controle(titre, chapeau, lignes, dossier_paye, code, chiffres):
    return {
        "titre": titre,
        "chapeau": chapeau + "\n" + CHAPEAU_FIN,
        "groupes": [lignes, dossier_paye, CONTROLE, TAUX_CONTROLE, DAI, _decoupage(code)],
        "chiffres": chiffres,
    }


TABLEAUX = {
    # ── T28 ──────────────────────────────────────────────────────────────────────────────────
    "T28": _controle(
        "T28 — Les dossiers de pension payés et contrôlés a posteriori : les règles de la BI",
        "T28 compte, par type de pension, les dossiers payés et les dossiers vérifiés par la DAI "
        "après le paiement (contrôlés a posteriori), trimestre par trimestre, avec le taux de "
        "contrôle : 5 % au moins.",
        ("Les lignes", [
            ("La branche des pensions.", "DOSSIER.TDOS_CODE = V", None),
            ("Pension de vieillesse ; pension de retraite anticipée, sur sa propre ligne ; "
             "pension d'invalidité.", "PRESTATION_ESP.TPE_CODE = RPV ;\nRPRA ; RPI", None),
            ("Pension de survivant : veuve, veuf et orphelin regroupés. Pension d'ascendant (le "
             "père ou la mère d'un assuré décédé).", "TPE_CODE = RPSV, RPSF, RPSE ;\nascendant : RPA",
             None),
            ("Allocation de vieillesse, versée en une fois. Allocation de survivant : conjoint, "
             "orphelin et ascendant.", "TPE_CODE = RAV ;\nRAS, RASC, RASO, RASA", None),
            ("Plus de ligne « pension de solidarité » : la CNSS n'en sert aucune. « Autres "
             "prestations de la branche » : le remboursement de cotisations.",
             "aucun code de solidarité ;\nTPE_CODE = RRC", None),
            ("Les accessoires (bonification, majorations pour enfant, charges du ménage) suivent "
             "la ligne de leur pension : ils ne font pas compter le dossier une fois de plus.",
             "TPE_CODE = RBEC, RME, RME-COL, RCCM :\nsur la ligne de leur pension", None),
        ]),
        _dossier_paye(
            "L'allocation de vieillesse est versée en une fois : on compte le dossier payé dans la "
            "période, pas celui liquidé (l'annuaire CNSS compte à la liquidation).",
            "payé : DEBOURS dans la période ;\nliquidé : PRESTATION_ESP.PE_DATE_NOTIFICATION "
            "(non retenu)"),
        "T28",
        "L'annuaire CNSS 2024 ne publie pas de contrôles a posteriori. Dans la base, en 2025 : 25 "
        "contrôles de dossiers de pension pour 47 732 dossiers de pension ayant reçu un débours ; "
        "toutes branches, 115 contrôles pour 155 206 dossiers payés (0,07 %), loin des 5 %. "
        "Allocations de vieillesse de 2024 : 753 personnes payées et 832 liquidées dans la base, "
        "pour 833 publiées (tableau 24).",
    ),

    # ── T29 ──────────────────────────────────────────────────────────────────────────────────
    "T29": _controle(
        "T29 — Les dossiers d'accident du travail payés et contrôlés a posteriori : les règles de la BI",
        "T29 compte, par type de prestation des accidents du travail et maladies professionnelles, "
        "les dossiers payés et les dossiers vérifiés par la DAI après le paiement (contrôlés a "
        "posteriori), trimestre par trimestre, avec le taux de contrôle : 5 % au moins.",
        ("Les lignes", [
            ("La branche des accidents du travail et maladies professionnelles.",
             "DOSSIER.TDOS_CODE = A", None),
            ("Indemnités journalières ; rentes viagères de la victime.",
             "PRESTATION_ESP.TPE_CODE = AIJ ;\nARI", None),
            ("Rentes de survivants : veuve, veuf, orphelin et ascendant, regroupées.",
             "TPE_CODE = ARSV, ARSF, ARSE, ARSA", None),
            ("Allocations d'incapacité (incapacité de moins de 15 %).", "TPE_CODE = AAI", None),
            ("« Achat de rente » : lu comme le rachat de rente, un capital versé une fois à la "
             "place de la rente. La ligne compte les dossiers qui ont reçu un rachat dans la période.",
             "AJUSTEMENT.TAJ_CODE = RR", "86"),
            ("« Autres prestations de la branche » : les soins remboursés.",
             "DEBOURS.DEB_TYPE = RB", None),
        ]),
        _dossier_paye(
            "Les indemnités journalières sont versées sur quelques mois : on compte le dossier payé "
            "dans la période, pas celui liquidé.",
            "payé : DEBOURS dans la période ;\nliquidé : PRESTATION_ESP.PE_DATE_NOTIFICATION "
            "(non retenu)",
            paiement_extra=(
                "Pour les soins, c'est le remboursement de soins ; pour le rachat de rente, "
                "son débours d'ajustement.",
                "soins : DEB_TYPE = RB ;\nrachat : DEB_TYPE = AJ lié à TAJ_CODE = RR")),
        "T29",
        "L'annuaire CNSS 2024 ne publie pas de contrôles a posteriori. Dans la base, en 2025 : 4 "
        "contrôles de dossiers AT/MP pour 2 134 dossiers AT/MP ayant reçu un débours ; toutes "
        "branches, 115 contrôles pour 155 206 dossiers payés (0,07 %), loin des 5 %. Rachats de "
        "rente de 2024 : 11 dans la base, pour 21,5 M, face à 25,4 M publiés (tableaux 90, 96 et "
        "97) : −15,2 %.",
    ),

    # ── T30 ──────────────────────────────────────────────────────────────────────────────────
    "T30": _controle(
        "T30 — Les dossiers de prestations familiales payés et contrôlés a posteriori : les règles "
        "de la BI",
        "T30 compte, par type de prestation familiale ou de maternité, les dossiers payés et les "
        "dossiers vérifiés par la DAI après le paiement (contrôlés a posteriori), trimestre par "
        "trimestre, avec le taux de contrôle : 5 % au moins.",
        ("Les lignes", [
            ("Les prestations familiales, avec la maternité.", "DOSSIER.TDOS_CODE = F ou M", None),
            ("Allocations prénatales : celle de la maternité et l'ancien code des prestations "
             "familiales.", "PRESTATION_ESP.TPE_CODE = MAP, FAP", None),
            ("Indemnités journalières de maternité.", "TPE_CODE = MIJ", None),
            ("Allocations familiales.", "TPE_CODE = FAF", None),
            ("La dernière ligne s'appelle « Frais médicaux de maternité », comme dans T27. Elle "
             "reste vide tant que la source n'est pas fournie.",
             "aucun soin remboursé (DEB_TYPE = RB)\nsur les dossiers M", "87"),
            ("« Bénéficiaires payés » veut dire dossiers payés : on compte des dossiers, pas des "
             "personnes ni des enfants.", "DOSSIER.DOS_CODE", None),
        ]),
        _dossier_paye(
            "Allocations prénatales et indemnités journalières de maternité, versées une fois ou "
            "sur quelques mois : on compte le dossier payé dans la période, pas celui liquidé "
            "(l'annuaire CNSS compte à la liquidation).",
            "payé : DEBOURS dans la période ;\nliquidé : PRESTATION_ESP.PE_DATE_NOTIFICATION "
            "(non retenu)"),
        "T30",
        "L'annuaire CNSS 2024 ne publie pas de contrôles a posteriori. Dans la base, en 2025 : 40 "
        "contrôles de dossiers de prestations familiales et 46 de maternité, pour 100 113 et 5 227 "
        "dossiers ayant reçu un débours ; toutes branches, 115 contrôles pour 155 206 dossiers "
        "payés (0,07 %). En 2024 : 4 881 personnes payées en indemnités de maternité et 3 890 "
        "liquidées, pour 3 893 publiées ; 104 611 allocataires payés dans l'année, pour 97 378 "
        "au 31/12 dans l'annuaire.",
    ),

    # ── T31 ──────────────────────────────────────────────────────────────────────────────────
    "T31": {
        "titre": "T31 — Les prestations payées et les indus, par branche et par mois : les règles de la BI",
        "chapeau": (
            "T31 donne, mois par mois et pour chaque branche (pensions, accidents du travail, "
            "prestations familiales), les prestations payées, les sommes payées en trop "
            "(trop-perçus) et en moins (moins-perçus), avec le taux de prestations indues : "
            "2 % au plus.\n" + CHAPEAU_FIN
        ),
        "groupes": [
            ("Les prestations payées", [
                ("Trois branches : les pensions, les accidents du travail et maladies "
                 "professionnelles, les prestations familiales avec la maternité.",
                 "DOSSIER.TDOS_CODE = V ; A ; F ou M", None),
                ("Un paiement est un débours de paiement payé, par titre de paiement (D) ou sans "
                 "titre (P). On prend son montant, jamais le net à payer du titre.",
                 "DEBOURS.DEB_TYPE = PM,\nDEB_STATUT = D ou P, DEB_MONTANT", None),
                ("Un paiement compte dans le mois qu'il paie, pas dans celui où il est saisi.",
                 "DEBOURS.DEB_DATE_EFFET", None),
                ("Les paiements ordinaires seulement : ni les soins remboursés des accidents du "
                 "travail, ni les rachats de rente.",
                 "DEB_TYPE = RB : écarté ;\nAJUSTEMENT.TAJ_CODE = RR : écarté", "88"),
                ("Les rappels de moins-perçus et les avances, versés par un débours d'ajustement, "
                 "ne sont pas dans les prestations payées.",
                 "DEBOURS.DEB_TYPE = AJ, relié par AJ_ID\nà TAJ_CODE = MP, RC, AV : écarté", "33"),
            ]),
            ("Le trop-perçu (l'indu)", [
                ("Un trop-perçu, ou indu, est une somme payée en trop que la CNSS doit reprendre : "
                 "un trop-perçu ordinaire ou un trop-perçu sur convention entre caisses.",
                 "AJUSTEMENT.TAJ_CODE = TP, TP-CONV ;\nmontant AJ_MONTANT", None),
                ("Sa branche est celle du dossier : un trop-perçu de maternité va avec les "
                 "prestations familiales.", "AJUSTEMENT.DOS_CODE → DOSSIER.TDOS_CODE", None),
                ("Il compte une seule fois, le mois où il est établi : ni les mois payés en trop, "
                 "ni les mois où il est repris.", "AJUSTEMENT.AJ_DATE_ETABLISSEMENT", None),
                ("Un ajustement rejeté ou annulé ne compte pas. En cours ou soldé, il compte.",
                 "AJUSTEMENT.AJ_STATUT <> R\n(A en cours, C soldé)", None),
                ("Les trop-perçus sur convention comptent comme les autres, avec les pensions. "
                 "Presque tous touchent des dossiers de coordination payés par la CNSS, et ils "
                 "bondissent en 2025.",
                 "TAJ_CODE = TP-CONV ;\nDOSSIER.DOS_CATEGORIE = COOR_CNSS", "92"),
            ]),
            ("Le moins-perçu", [
                ("Un moins-perçu est une somme payée en moins, rattrapée par un rappel : un "
                 "moins-perçu ordinaire ou un rappel sur convention entre caisses. Il compte le "
                 "mois où il est établi, comme le trop-perçu.",
                 "AJUSTEMENT.TAJ_CODE = MP, RC ;\nAJ_DATE_ETABLISSEMENT", None),
                ("Les moins-perçus sont montrés à part et n'entrent pas dans le taux d'indus. "
                 "Le taux des deux ensemble est affiché à côté.", "TAJ_CODE = MP, RC : hors du taux",
                 "91"),
            ]),
            ("Ce qui n'est pas un indu", [
                ("Le remboursement d'un trop-perçu n'est pas un nouvel indu ; il ne diminue pas non "
                 "plus les trop-perçus du tableau.", "TAJ_CODE = REM-TP : écarté", None),
                ("Les avances (dont l'avance COVID) et leur reprise ne sont ni des trop-perçus ni "
                 "des moins-perçus.", "TAJ_CODE = AV, AV-COVID19, REV-AV : écartés", None),
                ("Ni les mois de pension gratuits, ni les dons COVID, ni les rachats de rente, "
                 "ni les oppositions.", "TAJ_CODE = MPG, DON-COVID19, RR, OI, OE :\nécartés", None),
            ]),
            ("Le taux et sa norme", [
                ("Taux d'indus = trop-perçus ÷ toutes les prestations payées de la période × 100, "
                 "par branche et au total. La grille de la CNSS divise par le montant des seuls "
                 "dossiers contrôlés : il se calcule par les paiements de ces dossiers, reste à "
                 "fixer la période de ces paiements.",
                 "calcul ; montant des dossiers contrôlés :\nDEBOURS.DEB_MONTANT par DOS_CODE "
                 "de DOSSIERS_CONTROLES", "90"),
                ("Norme : 2 % au plus, lue sur l'année, pas mois par mois. Le taux porte la mention "
                 "« dans les normes » ou « hors normes ». La norme est un paramètre.",
                 "paramètre de la requête", None),
                ("Le taux s'affiche avec 3 chiffres après la virgule. Sur un trimestre ou l'année, "
                 "il se recalcule sur les sommes, jamais comme une moyenne des mois.", "calcul", None),
            ]),
            ("Le découpage", [
                ("T31 se sort au mois, au trimestre et à l'année.", "paramètre de la requête", None),
                ("Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi "
                 "plus tard n'entre que dans les photos suivantes.",
                 "copie de la base de fin janvier N+1", None),
                ("Les indus viennent de l'application. Le fichier Excel de la DAI (Direction de "
                 "l'audit interne) sert seulement à comparer.",
                 "AJUSTEMENT ; fichier de la DAI : comparaison", None),
                ("Chaque chiffre se découpe par direction régionale et par agence, celles du "
                 "dossier, pour les prestations payées comme pour les indus.",
                 "DOSSIER.DR_NO, DOSSIER.SP_NO", None),
                ("Le taux d'indus se donne par branche, pas par type de prestation : aucun "
                 "trop-perçu n'est relié à une prestation.",
                 "PE_AJ : aucun trop-perçu relié ;\nbranche par DOSSIER.TDOS_CODE", "93"),
                ("La BI garde le détail de chaque paiement et de chaque ajustement, sans nom ni "
                 "numéro d'assuré. La remarque libre d'un ajustement n'est jamais lue.",
                 "AJUSTEMENT.AJ_REMARQUE : non lue", None),
            ]),
        ],
        "chiffres": (
            "Avec ces règles, en 2024 : 684 trop-perçus pour 229,5 M, sur 58,73 Md payés : taux "
            "d'indus 0,39 % (pensions 0,47 %, accidents du travail 0,47 %, prestations familiales "
            "0,045 %), sous la norme de 2 % ; moins-perçus : 414,4 M. L'annuaire CNSS 2024 ne publie "
            "pas d'indus ; nos payées sont à −0,26 % de ses prestations servies (58,88 Md), mais par "
            "compensation : pensions +1,3 %, prestations familiales −3,8 %, accidents du travail −20,3 %."
        ),
    },
}
