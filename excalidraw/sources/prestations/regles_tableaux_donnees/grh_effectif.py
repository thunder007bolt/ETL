"""
Règles par tableau : GRH, l'effectif du personnel (T58 effectif à la clôture par catégorie, âge et
sexe ; T59 par sexe et domaine d'activité ; T61 par sexe et ancienneté ; T65 taux d'encadrement sur
cinq ans).

Même format que T32 dans regles_par_tableau.py. Les quatre tableaux comptent les mêmes agents : les
règles de présence (qui est un agent de la CNSS, qui est présent, les départs, la date d'arrêté)
sont écrites une seule fois plus bas et reprises mot pour mot dans chaque tableau.

Sources : fiche des règles (analyses/regles_cipres/fiche_regles.md, règles 1 à 4, 78 à 87, 94 et
99) ; règles de l'effectif (analyses/grh/regles_effectif.md, G1 à G7, G12, G13) ; requête proposée
(analyses/grh/t58_t65_effectif_requete_proposee.sql) ; points à valider GRH
(analyses/grh/points_a_valider_v2.md, du 17/11/2026) ; maquettes (analyses/grh/
reference_cipres_T58_T65.md). Chiffres : en-têtes « Résultat » des lots 17 (Q98, Q100) et 19
(Q111, Q112), repris dans regles_effectif.md et la requête proposée.

Points en orange : 5 (disponibilité, réglée par l'annuaire CNSS 2024 le 08/11/2026, à confirmer
par la DRH), 14 (détachés, suspendus), 2 (les 84 engagés de 2019) sur les règles communes ; 15 (T58,
catégorie des contractuels) ; 35 (T65, actifs ou tout l'effectif). Les points 1, 6 à 10, 16 et 34
sont tranchés (réunion du 03/11/2026, recueil des besoins v7) ; le point 18 décrit l'ancienne
règle du domaine, remplacée par le fichier des unités.
"""

_CHAPEAU_FIN = (
    "Voici chaque règle appliquée et le champ de la base qui la porte. Une remarque ? "
    "Écrivez-la dans la dernière colonne. En orange : règle encore en question (n° du point à valider)."
)


def _chapeau(debut):
    return debut + "\n" + _CHAPEAU_FIN


# ── Les règles de présence, communes à T58, T59, T61 et T65 ─────────────────────────────────────

_AGENT = ("Qui est un agent de la CNSS", [
    ("Seuls comptent les agents de la CNSS. Les agents de la fonction publique en poste à la CNSS "
     "et les agents affectés ne sont pas comptés.",
     "GRH_PERSONNE.QUALITE = A\n(B fonction publique, C affecté : écartés)", None),
    ("La nature, permanent ou temporaire, est celle de la dernière situation de l'agent à la date "
     "d'arrêté. Un tableau pour les permanents, un pour les temporaires (les contractuels).",
     "GRH_SITUATION.NATURE_EMPLOIE = PERMANENT\nou TEMPORAIRE, dernière DATE_DEBUT ≤ date d'arrêté",
     None),
    ("Un agent compte une seule fois à chaque date d'arrêté.",
     "GRH_PERSONNE.PERS_ID, compté une fois", None),
])

_PRESENT = ("Qui est présent à la date d'arrêté", [
    ("Un permanent est présent s'il est entré au plus tard à la date d'arrêté : la date d'embauche, "
     "à défaut la prise de service.",
     "GRH_PERSONNE.DATE_EMBAUCHE,\nà défaut DATE_PRISE_SERVICE ≤ date d'arrêté", None),
    ("Et s'il n'est pas encore parti : sa fiche n'est pas inactive, ou son premier départ tombe après "
     "la date d'arrêté. Une fiche sans état (embauche récente) compte comme encore là.",
     "GRH_PERSONNE.ETAT_EMPLOYE ≠ INACTIF (vide compris),\nou premier départ après la date", "45"),
    ("Un temporaire est présent si la date d'arrêté tombe dans un de ses contrats. Un contrat commence "
     "à la décision d'engagement temporaire ; sans elle, le temporaire n'est pas compté.",
     "GRH_DECISION.CDEC_CODE = 50, DATE_VIGUEUR", None),
    ("Le contrat finit au premier départ saisi après son début, sinon la veille du contrat suivant. "
     "Sans fin connue, l'agent reste compté : la BI ne fixe pas de durée de contrat.",
     "départ après le début, sinon la 50 suivante ;\nsinon aucune fin", "20"),
    ("Un agent en disponibilité à la date d'arrêté sort de l'effectif : sa dernière mise en "
     "disponibilité n'est pas suivie d'une remise en activité. Il revient à sa remise en activité, "
     "comme dans l'annuaire CNSS 2024.",
     "GRH_DECISION.CDEC_CODE = 07, puis 18 ;\nréglage disponibilite = HORS_EFFECTIF", None),
    ("Un agent détaché, mis à disposition, suspendu ou sous les drapeaux reste compté : la base ne "
     "date pas ces absences.",
     "GRH_SITUATION.STATUT_EMPLOYE = B, D, F, H :\nnon lu (pas daté)", "14"),
])

_DEPARTS = ("Les départs", [
    ("Un départ est une retraite, une liquidation des droits après décès, une fin de contrat, une "
     "démission, une rupture du contrat ou un licenciement, à sa date d'effet. Sans décision, on prend "
     "la date de décès.",
     "GRH_DECISION.CDEC_CODE = 02, 34, 35, 36, 52, 54,\nDATE_VIGUEUR ; sinon GRH_PERSONNE.DATE_DECES",
     "3"),
    ("Une décision annulée par une décision d'annulation ne compte pas.",
     "GRH_DECISION.CDEC_CODE = 40,\nNO_DECI_MODIF = la décision annulée", None),
    ("Une décision qui vise plusieurs agents compte pour chacun d'eux.",
     "GRH_DECISION_PERSONNE.PERS_ID", None),
    ("Une fiche inactive sans aucune date de départ n'est comptée à aucune date. C'est le cas des "
     "84 agents engagés en 2019 : l'annuaire CNSS les compte de 2019 à 2023.",
     "GRH_PERSONNE.ETAT_EMPLOYE = INACTIF,\nni décision de départ ni DATE_DECES", "2"),
])

_DATE_ARRETE = ("La date d'arrêté", [
    ("La date d'arrêté est le 31 décembre. Les fins de mois sont en option.",
     "paramètre de la requête (periodicite)", None),
    ("Pour l'année N, la photo de la base de fin janvier N+1 fait foi. Ce qui est saisi "
     "plus tard n'entre que dans les photos suivantes.", "copie de la base de fin janvier N+1", None),
])

_PRESENCE = [_AGENT, _PRESENT, _DEPARTS, _DATE_ARRETE]

# ── Les autres règles partagées ──────────────────────────────────────────────────────────────────

_SEXE = ("Le sexe se lit sur la fiche de l'agent : M hommes, F femmes. Un autre code ne compte que "
         "dans le total.", "GRH_PERSONNE.SEXE", None)

_COLONNES = ("Les colonnes : hommes et femmes, par catégorie rangée comme dans T58 (employés, agents "
             "de maîtrise, cadres moyens, cadres supérieurs).",
             "GRH_SITUATION.CATEGORIE regroupée\n× GRH_PERSONNE.SEXE", None)

_DECOUPE = ("Chaque chiffre se découpe par direction, agence et direction régionale : celles de l'unité "
            "de l'agent dans sa dernière situation.",
            "GRH_SITUATION.UA_CODE\n→ DIM_GRH_UNITE_ADMINISTRATIVE", "56")

_CONTROLE = ("Contrôle : T58, T59 et T61 comptent les mêmes agents à la même date ; leur total est "
             "l'effectif de fin d'année de T60.",
             "DTM_GRH_EFFECTIF.NB_AGENTS (même requête) ;\nfin de T60", None)


TABLEAUX = {
    "T58": {
        "titre": "T58 — L'effectif à la clôture par catégorie, âge et sexe : les règles de la BI",
        "chapeau": _chapeau(
            "T58 compte les agents de la CNSS présents au 31 décembre, par catégorie professionnelle, "
            "tranche d'âge de 5 ans et sexe : un tableau pour les permanents, puis un pour les "
            "temporaires (les contractuels)."),
        "groupes": [
            *_PRESENCE,
            ("La catégorie", [
                ("La catégorie est celle de la grille de salaire, dans la dernière situation de l'agent "
                 "à la date d'arrêté.", "GRH_SITUATION.CATEGORIE", None),
                ("Employés : catégories 1 et 2. Agents de maîtrise : 3. Cadres moyens : 4 et 5. Cadres "
                 "supérieurs : 6 et hors catégorie. Les cadres de la CIPRES sont les cadres moyens et "
                 "supérieurs.", "CATEGORIE = 01-02 ; 03 ; 04-05 ; 06 et HC", "47"),
                ("Un agent sans catégorie connue est compté dans le total, sans colonne de catégorie.",
                 "CATEGORIE vide ou autre code :\nSANS_CATEGORIE", None),
                ("Les temporaires sont rangés comme les permanents, par la catégorie de leur situation.",
                 "GRH_SITUATION.CATEGORIE,\nNATURE_EMPLOIE = TEMPORAIRE", "15"),
            ]),
            ("L'âge et le sexe", [
                ("L'âge est le nombre d'années pleines à la date d'arrêté, au jour près. Ce n'est pas le "
                 "calcul des prestations, qui ne regarde que l'année.", "GRH_PERSONNE.DATE_NAISS", None),
                ("Les tranches sont celles de la maquette CIPRES : moins de 20 ans, 20-24 … 55-59, "
                 "60 ans et plus.", "DIM_GRH_TRANCHE_AGE", None),
                ("Une date de naissance absente, ou un âge hors de 0 à 119 ans : l'agent est compté dans "
                 "le total, sans tranche d'âge.", "DATE_NAISS vide ou fausse : tranche vide", None),
                _SEXE,
            ]),
            ("Le découpage et le contrôle", [_DECOUPE, _CONTROLE]),
        ],
        "chiffres": (
            "Requête lancée le 17/11/2026 sur la base : 741 permanents au 31/12/2024, pour 758 dans l'annuaire "
            "CNSS 2024. Employés 133 hommes et 44 femmes (133 et 44 publiés), agents de maîtrise 92 et 114 "
            "(94 et 114), cadres moyens 138 et 163 (143 et 168), cadres supérieurs 39 et 18 (42 et 20). Les "
            "tranches d'âge sont retrouvées à quelques agents près, sauf 5 fiches sans état de 69 à 80 ans, à "
            "écarter (point 45). Temporaires : 299, pour 46 contractuels publiés, car les fins de contrat ne "
            "sont pas saisies ; 47 en ne gardant que les fiches non inactives (point 20)."
        ),
    },
    "T59": {
        "titre": "T59 — L'effectif par sexe et domaine d'activité : les règles de la BI",
        "chapeau": _chapeau(
            "T59 compte les agents de la CNSS présents au 31 décembre, par domaine d'activité et par "
            "sexe : les mêmes agents que T58, permanents puis temporaires."),
        "groupes": [
            *_PRESENCE,
            ("Le domaine d'activité", [
                ("Le domaine est celui de l'unité de l'agent dans sa dernière situation, lu dans le "
                 "fichier des unités tenu par la CNSS.",
                 "GRH_SITUATION.UA_CODE → fichier unité → domaine\n(correspondance_unite_domaine.xlsx)",
                 None),
                ("Cinq domaines, ceux du recueil des besoins de la CNSS : recouvrement, prestations, "
                 "prévention des risques professionnels, action sanitaire et sociale, gestion "
                 "administrative et financière. Le recueil passe avant la maquette CIPRES, qui en a sept.",
                 "DOMAINE_ACTIVITE", None),
                ("Une unité absente du fichier, ou sans domaine, donne « Domaine non renseigné » : la BI "
                 "ne choisit pas le domaine à la place de la CNSS. Tant que le fichier n'est pas rempli "
                 "(132 unités à décider), tout T59 est sur cette ligne.",
                 "DOMAINE_ACTIVITE = Domaine non renseigné", "48"),
                ("Le code de domaine noté dans la situation ne sert pas : il change d'un agent à l'autre "
                 "dans une même unité (66 unités sur 139).",
                 "GRH_SITUATION.CODE_DOMAINE_ACT : non lu", None),
                ("Un agent rattaché à une unité fermée est compté, avec le domaine de cette unité "
                 "(10 agents, situation à corriger dans l'application).",
                 "GRH_SITUATION.UA_CODE d'une unité fermée", None),
            ]),
            ("Le sexe et les colonnes", [_SEXE, _COLONNES]),
            ("Le découpage et le contrôle", [_DECOUPE, _CONTROLE]),
        ],
        "chiffres": None,
    },
    "T61": {
        "titre": "T61 — L'effectif par sexe et ancienneté : les règles de la BI",
        "chapeau": _chapeau(
            "T61 compte les agents de la CNSS présents au 31 décembre, par tranche d'ancienneté dans la "
            "CNSS et par sexe : les mêmes agents que T58, permanents puis temporaires."),
        "groupes": [
            *_PRESENCE,
            ("L'ancienneté", [
                ("L'ancienneté est le nombre d'années pleines passées à la CNSS, de la prise de service "
                 "(à défaut l'embauche) jusqu'à la date d'arrêté.",
                 "GRH_PERSONNE.DATE_PRISE_SERVICE,\nà défaut DATE_EMBAUCHE", None),
                ("On retire les périodes de disponibilité, de la mise en disponibilité à la remise en "
                 "activité (ou à la date d'arrêté).",
                 "GRH_DECISION.CDEC_CODE = 07 → 18", None),
                ("On retire aussi le temps entre un départ et un nouvel engagement, temporaire ou "
                 "permanent.", "départ → GRH_DECISION.CDEC_CODE = 50 ou 51", None),
                ("Les tranches sont celles de la maquette CIPRES : moins d'un an, 1-4, 5-9 … 35-39, "
                 "40 ans et plus.", "DIM_GRH_TRANCHE_ANCIENNETE", None),
            ]),
            ("Le sexe et les colonnes", [_SEXE, _COLONNES]),
            ("Le découpage et le contrôle", [_DECOUPE, _CONTROLE]),
        ],
        "chiffres": (
            "Requête lancée le 17/11/2026 sur la base, avec la nouvelle règle : au 31/12/2024, chaque tranche "
            "est à 4 agents près de l'annuaire CNSS 2024 (moins d'un an : 102 pour 103 ; 5 à 9 ans : 120 pour "
            "122 ; 10 à 14 ans : 225 pour 232 ; 20 à 24 ans : 117 pour 120). Seule la tranche « 40 ans et "
            "plus » s'écarte : 6 pour 1, à cause de 5 fiches sans état anciennes, à écarter (point 45)."
        ),
    },
    "T65": {
        "titre": "T65 — Le taux d'encadrement sur cinq ans : les règles de la BI",
        "chapeau": _chapeau(
            "T65 donne, au 31 décembre de chacune des cinq dernières années, les encadrants, les "
            "non-encadrants, l'effectif total et le taux d'encadrement : la part des cadres dans "
            "l'effectif."),
        "groupes": [
            *_PRESENCE,
            ("Les encadrants", [
                ("Un encadrant est un cadre, moyen ou supérieur, d'après la catégorie de la grille de "
                 "salaire de sa dernière situation à la date d'arrêté.",
                 "GRH_SITUATION.CATEGORIE = 04, 05, 06, HC", "47"),
                ("Les non-encadrants sont les employés, les agents de maîtrise et les agents sans "
                 "catégorie.", "CATEGORIE = 01, 02, 03, vide ou autre", None),
                ("Ni la qualification ni le poste (postes de commandement) ne servent : seule compte la "
                 "catégorie.", "GRH_SITUATION.QUAL_CODE, FNCT_CODE : non lus", None),
            ]),
            ("Le taux", [
                ("Taux d'encadrement = encadrants ÷ tout l'effectif compté × 100, pas seulement les "
                 "agents actifs.", "DTM_GRH_EFFECTIF : NB_CADRES ÷ NB_AGENTS", None),
                ("Norme CIPRES : 10 % au plus.", "paramètre", None),
                ("Le taux se donne au 31 décembre de chacune des cinq dernières années closes.",
                 "ID_TEMPS = 31/12 des 5 derniers exercices", "57"),
            ]),
            ("Le découpage et le contrôle", [
                _DECOUPE,
                ("Contrôle : l'effectif total de T65 est celui de T58 à la même date ; les encadrants, "
                 "ses cadres moyens et supérieurs.", "T58 du même 31/12", None),
            ]),
        ],
        "chiffres": (
            "Requête lancée le 17/11/2026 sur la base : 48,3 % en 2024 (358 cadres sur 741 permanents), pour "
            "49,2 % dans l'annuaire CNSS 2024 (373 sur 758). De 2020 à 2023 : 41,9 %, 42,2 %, 42,4 % et "
            "41,9 %, pour environ 41 % publiés. En 2025 : 50,1 % (442 sur 883)."
        ),
    },
}
