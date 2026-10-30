"""
Fiches de maîtrise du domaine des prestations (T25 à T57), dossier « 3 prestations ».

Elles reprennent tout le travail fait du 26 au 30/10/2026 (analyses/prestations, lots 1 à 4, la
comparaison avec l'annuaire de la CNSS 2024), sous forme de blocs à réviser : le domaine, le
vocabulaire, les données, la démarche, les découvertes, les règles, les erreurs des DTM, les
questions ouvertes, l'avancement, et les questions qu'on peut te poser.
"""

from fiches import Bloc, Fiche, Partie

D = "3 prestations/"
ETAT_AU = "état au 30/10/2026"


def regle(ref, titre, etat, regle_txt, pourquoi, preuve, question, reponse, pied):
    """Bloc d'une règle de calcul, toujours dans le même ordre."""
    return Bloc(titre, [("La règle", regle_txt), ("Pourquoi", pourquoi), ("La preuve", preuve),
                        (f"Si on te demande : {question}", reponse)],
                ref=ref, genre="regle", etat=etat, pied=pied)


def constat(ref, titre, fait, chiffre, importance, pied):
    """Bloc d'un fait établi dans les données."""
    return Bloc(titre, [("Ce qu'on a découvert", fait), ("Le chiffre", chiffre),
                        ("Pourquoi ça compte", importance)],
                ref=ref, genre="constat", etat="prouvé", pied=pied)


def erreur(ref, titre, verdict, fait, verifie, consequence, pied):
    """Bloc d'une erreur supposée des requêtes DTM actuelles."""
    return Bloc(titre, [("Ce que fait la requête DTM", fait), ("Ce qu'on a vérifié", verifie),
                        ("Ce que ça change", consequence)],
                ref=ref, genre="erreur", etat=verdict, pied=pied)


def question(ref, titre, q, pourquoi, change, proposition, pied=""):
    """Bloc d'une question encore ouverte."""
    return Bloc(titre, [("La question", q), ("Pourquoi on la pose", pourquoi),
                        ("Ce que change la réponse", change), ("Notre proposition", proposition)],
                ref=ref, genre="question", etat="ouvert", pied=pied)


def mot(titre, definition, exemple, attention=""):
    """Bloc d'un mot du vocabulaire."""
    champs = [("Ce que c'est", definition), ("Exemple", exemple)]
    if attention:
        champs.append(("Attention", attention))
    return Bloc(titre, champs, genre="notion")


# =============================================================================================
# 01 Le domaine en bref
# =============================================================================================

F01 = Fiche(
    fichier=D + "01 le domaine en bref.excalidraw",
    titre="Les prestations en bref : ce que verse la CNSS, ce que la CIPRES demande",
    sous_titre="Fiche 01 du domaine des prestations (T25 à T57) · " + ETAT_AU,
    objectif="les quatre types de dossier de prestations et ce que chacun verse, les 33 "
             "tableaux CIPRES du domaine rangés en six familles, et où en est chaque famille.",
    parties=[
        Partie("Les quatre types de dossier", colonnes=4,
               intro="Dans la base, chaque dossier a un type (TDOS_CODE). Ce type dit sa "
                     "branche.",
               blocs=[
            Bloc("V · Les pensions (PVID)", [
                ("Ce que c'est", "La branche vieillesse, invalidité, décès."),
                ("Ce qu'elle verse", "la pension de vieillesse (chaque mois), la retraite "
                                     "anticipée, la pension d'invalidité, les pensions de "
                                     "survivants (veuve, veuf, orphelin, ascendant), les "
                                     "allocations de vieillesse et de survivants (versées une "
                                     "fois), et des accessoires (bonification pour enfant…)."),
                ("Un ordre de grandeur", "62 231 pensionnés et 46,6 Md versés en 2024 "
                                         "(annuaire de la CNSS)."),
            ], ref="V"),
            Bloc("A · Les accidents du travail (AT/MP)", [
                ("Ce que c'est", "La branche des risques professionnels : accidents du travail "
                                 "et maladies professionnelles."),
                ("Ce qu'elle verse", "des indemnités journalières (IJ) pendant l'arrêt, des "
                                     "soins remboursés, puis, après la consolidation, une rente "
                                     "ou une allocation d'incapacité ; des rentes de survivants "
                                     "en cas de décès."),
                ("Un ordre de grandeur", "environ 1 300 déclarations par an ; une sur deux ne "
                                         "donne jamais rien."),
            ], ref="A"),
            Bloc("F · Les prestations familiales", [
                ("Ce que c'est", "Les allocations versées aux familles des travailleurs."),
                ("Ce qu'elle verse", "les allocations familiales, par enfant, payées chaque "
                                     "trimestre à l'allocataire ; une ancienne allocation "
                                     "prénatale (FAP), presque éteinte."),
                ("Un ordre de grandeur", "en 2025, environ 8 200 nouveaux allocataires et 8 100 "
                                         "dossiers où un enfant est ajouté."),
            ], ref="F"),
            Bloc("M · La maternité", [
                ("Ce que c'est", "Rattachée aux prestations familiales. Un dossier par "
                                 "grossesse."),
                ("Ce qu'elle verse", "les indemnités journalières de maternité et l'allocation "
                                     "prénatale (MAP), notifiées ensemble."),
                ("Un ordre de grandeur", "environ 4 200 grossesses par an."),
                ("Attention", "la requête DTM actuelle appelle M « maladie » : c'est faux."),
            ], ref="M"),
        ]),
        Partie("Ce que demande l'Annuaire CIPRES : six familles de tableaux", colonnes=3,
               intro="Les 33 tableaux du domaine se rangent en six familles. Dans une famille, "
                     "une seule requête et les mêmes règles servent à tous les tableaux.",
               blocs=[
            Bloc("Dossiers et durée de liquidation", [
                ("Tableaux", "T25 pensions, T26 accidents du travail, T27 prestations "
                             "familiales."),
                ("Ce qu'ils mesurent", "les demandes déposées, liquidées (en moins de 15 jours, "
                                       "de 15 à 45, plus de 45), en instance et rejetées. "
                                       "Norme : moins de 45 jours en moyenne."),
                ("Où on en est", "règles, requêtes et fiches de validation faites ; on attend "
                                 "les réponses du métier."),
            ], ref="1", genre="etape", etat="en cours"),
            Bloc("Contrôles a posteriori", [
                ("Tableaux", "T28 à T30."),
                ("Ce qu'ils mesurent", "la part des dossiers payés qui ont été contrôlés après "
                                       "paiement. Norme : au moins 5 %."),
                ("Où on en est", "bloqué : plus aucun contrôle saisi depuis juillet 2025 ; on "
                                 "attend la DSI."),
            ], ref="6", genre="etape", etat="à faire"),
            Bloc("Indus", [
                ("Tableaux", "T31."),
                ("Ce qu'ils mesurent", "les sommes versées en trop (trop perçus) ou en moins "
                                       "(moins perçus), par mois et par branche. Norme : 2 % au "
                                       "plus."),
                ("Où on en est", "règle proposée (I1) ; il faut un lot de requêtes."),
            ], ref="5", genre="etape", etat="à faire"),
            Bloc("Bénéficiaires au 31 décembre", [
                ("Tableaux", "T32 à T41 : pensionnés, crédirentiers, allocataires, par âge et "
                             "par sexe, avec les montants."),
                ("Ce qu'ils mesurent", "le stock de bénéficiaires en fin d'année : la base des "
                                       "études actuarielles."),
                ("Où on en est", "règles prêtes et confirmées par l'annuaire de la CNSS ; requête "
                                 "et fiche à faire. C'est la prochaine famille."),
            ], ref="2", genre="etape", etat="en cours"),
            Bloc("Nouveaux bénéficiaires", [
                ("Tableaux", "T42 à T49."),
                ("Ce qu'ils mesurent", "les entrées de l'année, par âge et par sexe ; T42 "
                                       "ajoute les services passés, T48 le taux d'incapacité."),
                ("Où on en est", "règle R7 revue avec l'annuaire de la CNSS ; à faire après la "
                                 "famille 2."),
            ], ref="3", genre="etape", etat="à faire"),
            Bloc("Évolution sur 5 ans", [
                ("Tableaux", "T50 à T57."),
                ("Ce qu'ils mesurent", "effectifs et montants année par année : allocations, IJ, "
                                       "maternité, prestations par branche, décédés."),
                ("Où on en est", "règles des paiements prêtes ; T57 (décédés) à définir."),
            ], ref="4", genre="etape", etat="à faire"),
        ]),
        Partie("Les trois idées à retenir", colonnes=3, blocs=[
            Bloc("On recalcule depuis la base", [
                ("", "Les tables DTM actuelles se trompent (fiche 08) : tous les chiffres sont "
                     "refaits depuis la base de gestion, avec des règles écrites et prouvées."),
            ]),
            Bloc("On compte des demandes et des stocks", [
                ("", "Pour T25 à T27, une demande = un dossier × une ligne du tableau. Pour T32 "
                     "à T41, un stock au 31 décembre : les pensions en cours ce jour-là."),
            ]),
            Bloc("Un paiement = débours D ou P", [
                ("", "Seuls les débours de paiement au statut D (payé par effet) ou P (payé "
                     "sans effet) sont des paiements, pour le montant du débours."),
            ]),
        ]),
    ],
)

# =============================================================================================
# 02 Le vocabulaire
# =============================================================================================

F02 = Fiche(
    fichier=D + "02 le vocabulaire.excalidraw",
    titre="Le vocabulaire des prestations : les mots à connaître",
    sous_titre="Fiche 02 du domaine des prestations · un bloc par mot, avec un exemple · "
               + ETAT_AU,
    objectif="le sens précis des mots du métier et du projet, pour suivre une discussion avec "
             "la direction des prestations ou la DSI sans te perdre.",
    parties=[
        Partie("Le parcours d'une demande", colonnes=4, blocs=[
            mot("Réception", "Le dépôt d'un dossier au guichet, enregistré avec sa date et ses "
                             "pièces.",
                "Une veuve dépose sa demande de pension le 10/03/2025 : c'est une réception.",
                "NOUVEAU ouvre un dossier ; ANCIEN est déposée sur un dossier existant."),
            mot("Dossier", "Le dossier ouvert par une demande, qui porte ensuite toutes les "
                           "prestations d'une même personne ou d'un même accident.",
                "Le dossier de pension d'un retraité porte sa pension, puis, à son décès, celle "
                "de sa veuve.",
                "Statut A = actif, F = fermé ; ni l'un ni l'autre ne veut dire « liquidé »."),
            mot("Prestation", "Un droit ouvert dans un dossier : une pension, une rente, une "
                              "allocation, des IJ…",
                "Dans un dossier d'accident : des IJ, puis une rente des années après."),
            mot("Notification", "La décision écrite qui fixe les droits du bénéficiaire.",
                "La pension de M. A est notifiée le 13/03/2025.",
                "On prend la date de notification de la prestation, celle du dossier est "
                "toujours vide."),
            mot("Liquidation", "Le calcul et l'attribution des droits, qui se termine par la "
                               "notification (Annuaire p. 64).",
                "Liquidé le 13/03/2025 = notifié le 13/03/2025."),
            mot("Durée de liquidation", "Le temps entre le dépôt et la notification des droits "
                                        "(Annuaire p. 64).",
                "Reçu le 10/01, notifié le 13/03 : 62 jours.",
                "Norme CIPRES : moins de 45 jours en moyenne."),
            mot("En instance", "Une demande déposée, ni liquidée ni rejetée au 31 décembre.",
                "Reçue le 17/11/2025, notifiée le 22/01/2026 : en instance au 31/12/2025."),
            mot("Rejet", "Un refus enregistré dans le module des rejets, avec un motif.",
                "Prescription, pas de bénéficiaire, pièce non conforme.",
                "Très peu sont saisis : une quinzaine par an pour les pensions."),
        ]),
        Partie("Les personnes", colonnes=4, blocs=[
            mot("Assuré", "Le travailleur immatriculé qui cotise et ouvre les droits.",
                "Un salarié qui part à la retraite."),
            mot("Ayant droit", "La personne à qui la prestation est due.",
                "L'orphelin, pour une pension d'orphelin.",
                "C'est elle qu'on compte (règle R5), pas toujours celle qu'on paie."),
            mot("Bénéficiaire payé", "La personne qui reçoit l'argent.",
                "Le tuteur d'un orphelin.",
                "La requête DTM actuelle compte cette personne : c'est une erreur pour les "
                "orphelins."),
            mot("Allocataire", "Le parent qui reçoit les allocations familiales pour ses "
                               "enfants.",
                "Une mère qui touche les allocations de ses trois enfants.",
                "On compte l'allocataire une fois, pas une fois par enfant."),
            mot("Survivants", "Les proches d'un assuré ou d'un pensionné décédé : veuve, veuf, "
                              "orphelins, ascendants (parents).",
                "La veuve et les deux orphelins d'un pensionné."),
            mot("Pensionné", "La personne qui touche une pension.",
                "Au 31/12/2024 : 30 730 retraités de vieillesse (annuaire de la CNSS)."),
            mot("Crédirentier", "La personne qui touche une rente d'accident du travail.",
                "La victime d'un accident, après consolidation, ou sa veuve."),
        ]),
        Partie("Les prestations", colonnes=4, blocs=[
            mot("Pension", "Un versement régulier (chaque mois) de la branche des pensions.",
                "La pension de vieillesse."),
            mot("Allocation de vieillesse", "Un versement unique, quand l'assuré n'a pas assez "
                                            "cotisé pour une pension (Annuaire p. 62).",
                "Code RAV."),
            mot("Rente", "Le versement régulier d'un accident du travail, après consolidation, "
                         "ou aux survivants.",
                "Code ARI (rente viagère), ARSV (rente de veuve)."),
            mot("Indemnités journalières (IJ)", "Un revenu de remplacement pendant un arrêt de "
                                                "travail ou un congé de maternité.",
                "Codes AIJ (accident) et MIJ (maternité)."),
            mot("Soins", "Les frais médicaux remboursés après un accident du travail "
                         "(prestations en nature).",
                "Pharmacie, consultations, radios.",
                "Ils ne passent pas par le guichet et n'ont pas de notification."),
            mot("Accessoire", "Un complément versé avec une pension.",
                "La bonification pour enfant à charge (RBEC).",
                "Il ne fait pas un pensionné de plus (règle A1)."),
            mot("Réversion", "La pension de survivant versée après le décès d'un pensionné, "
                             "dans son dossier.",
                "La veuve d'un retraité décédé dépose sa demande sur son dossier."),
            mot("Consolidation", "Le moment où l'état d'une victime d'accident est stabilisé : "
                                 "on fixe alors son taux d'incapacité.",
                "Elle ouvre la rente ou l'allocation d'incapacité."),
            mot("Retraite anticipée", "Une pension de vieillesse prise avant l'âge normal.",
                "Code RPRA ; une ligne à part en T25 (atelier de Kaya)."),
            mot("Coordination", "Un dossier partagé avec une autre caisse, qui paie tout ou "
                                "partie.",
                "Catégorie COOR_AC : payé par une autre caisse, mais instruit par la CNSS."),
        ]),
        Partie("L'argent", colonnes=4, blocs=[
            mot("Débours", "Une ligne de paiement préparée pour une prestation et un mois.",
                "La pension de mars 2025 de M. A : un débours.",
                "Seuls les débours de type PM au statut D ou P sont des paiements."),
            mot("Effet", "L'ordre de paiement (virement, espèces) qui regroupe des débours.",
                "Un virement bancaire (BVB) pour plusieurs mois.",
                "Son « net à payer » ne doit pas être additionné débours par débours."),
            mot("Statut D, P, V", "D = payé par un effet décaissé ; P = payé sans effet ; V = "
                                  "validé mais pas encore payé.",
                "En 2025 : 53,72 Md au statut D, 7,63 Md au statut P."),
            mot("Mois payé", "Le mois que le paiement couvre (DEB_DATE_EFFET), pas la date du "
                             "versement.",
                "La pension de décembre, préparée fin novembre, compte en décembre."),
            mot("Rappel", "Un paiement d'un mois passé, saisi plus tard.",
                "9 % des débours sont saisis deux mois ou plus après leur mois."),
            mot("Indu", "Une somme versée à tort : trop perçu (versé en trop) ou moins perçu "
                        "(versé en moins).",
                "Une pension payée après le décès : trop perçu.",
                "Norme CIPRES : au plus 2 % des prestations."),
        ]),
        Partie("Les mots du projet", colonnes=4, blocs=[
            mot("Cliché", "La photo mensuelle de la base copiée dans l'entrepôt (MMYYYY).",
                "Le cliché 092026 = septembre 2026."),
            mot("Fait (FAIT_…)", "Une table de l'entrepôt, copie d'une table de la base.",
                "DWH.FAIT_DOSSIER est la copie de DOSSIER."),
            mot("DTM", "Une table calculée qui porte les chiffres d'un tableau.",
                "DTM_DOSSIER alimente T25 à T27."),
            mot("Lot, requête Pnn", "Un fichier de requêtes de profilage ; chaque requête a un "
                                    "numéro P01, P02…",
                "P25 : les pensionnés au 31/12/2025."),
            mot("Règle (R, D, A, I, C)", "Une façon de compter, écrite et justifiée.",
                "R6 : le pensionné au 31 décembre ; D2 : on compte des demandes."),
            mot("Constat (B)", "Un fait établi dans les données.",
                "B3 : le statut F d'un dossier veut dire fermé."),
            mot("Hypothèse (H)", "Une erreur supposée d'une requête DTM, vérifiée dans les "
                                 "données.",
                "H6 : le net à payer est répété sur chaque débours (confirmée)."),
            mot("Proposition BI", "Notre choix, cohérent avec les données mais sans source "
                                  "écrite : à faire valider.",
                "Le stock en instance limité à 24 mois sans suite."),
        ]),
    ],
)

# =============================================================================================
# 03 Les données
# =============================================================================================


def table(nom, garde, colonnes, piege):
    return Bloc(nom, [("Ce qu'elle garde", garde), ("Les colonnes à connaître", colonnes),
                      ("Le piège", piege)], genre="donnee")


F03 = Fiche(
    fichier=D + "03 les donnees.excalidraw",
    titre="Les données : les tables et les codes de la base des prestations",
    sous_titre="Fiche 03 du domaine des prestations · base source, schéma USER_DWH, copie au "
               "02/02/2026 · " + ETAT_AU,
    objectif="les cinq tables du chemin d'une demande et leurs liens, les autres tables utiles, "
             "les codes à connaître par cœur, et les pièges de chacune.",
    parties=[
        Partie("Le chemin d'une demande dans les tables", colonnes=5, flux=True,
               intro="De gauche à droite : le guichet, le dossier, le droit, le paiement, "
                     "l'ordre de paiement.",
               blocs=[
            table("RECEPTION_DOSSIER",
                  "Chaque dépôt au guichet : 55 000 réceptions de pension depuis l'origine.",
                  "RD_ID, RD_DATE_RECEPTION, RD_TYPE_RECEPTION (NOUVEAU ou ANCIEN), DOS_CODE "
                  "(le dossier visé), TDOS_CODE.",
                  "Les anciens dossiers n'ont pas de lien vers leur réception."),
            table("DOSSIER",
                  "Chaque dossier, avec son type et sa direction régionale.",
                  "DOS_CODE, TDOS_CODE (V, A, F, M), RD_ID (la réception qui l'a ouvert), "
                  "DOS_STATUT, DOS_CATEGORIE, DR_NO.",
                  "DOS_STATUT F veut dire fermé, pas liquidé ; DOS_DATE_NOTIFICATION est "
                  "toujours vide."),
            table("PRESTATION_ESP",
                  "Chaque droit en espèces : pension, rente, allocation, IJ.",
                  "PE_ID, DOS_CODE, TPE_CODE (le type), IND_ID_PREST (l'ayant droit), PE_STATUT, "
                  "PE_DATE_NOTIFICATION, PE_DATE_EFFET, PE_DATE_RADIATION, PE_DATE_INSERT.",
                  "Les mois cotisés sont recopiés sur chaque prestation du dossier."),
            table("DEBOURS",
                  "Chaque ligne de paiement, par prestation et par mois.",
                  "PE_ID, DEB_TYPE (PM = paiement), DEB_STATUT (D, P, V…), DEB_MONTANT, "
                  "DEB_DATE_EFFET (le mois payé), TPN_CODE (les soins).",
                  "DEB_VERIFIE vérifie la saisie : ce n'est pas un contrôle a posteriori."),
            table("EFFET",
                  "L'ordre de paiement : virement (BVB) ou espèces (BE), 98 % des effets.",
                  "le numéro de l'effet, son statut, son net à payer.",
                  "Le net à payer est le total de l'effet : l'ajouter à chaque débours gonfle "
                  "les montants 3,8 fois. 62 % des paiements PF de 2025 n'ont pas d'effet."),
        ]),
        Partie("Les autres tables utiles", colonnes=4, blocs=[
            table("NOTIFICATION_PREST",
                  "La notification imprimée et signée, par dossier et par code.",
                  "NOT_DATE_SIGNATURE, TPE_CODE.",
                  "Pas imprimée pour les IJ ; la date de la prestation suffit."),
            table("DOSSIER_REJETE",
                  "Les refus, avec un motif codé.",
                  "DOSRE_DATE_REJET, DOSRE_MOTIF, RD_ID, DOS_CODE.",
                  "Presque vide : 146 rejets de pension depuis 2017."),
            table("INDIVIDU",
                  "Les personnes : assurés, ayants droit, bénéficiaires.",
                  "IND_ID, IND_SEXE (1 ou 2), IND_DATE_NAISSANCE, IND_DATE_DECES.",
                  "Le sexe est un nombre ; les décès sont saisis en retard."),
            table("RECEPTION_DOS_PIECE",
                  "Les pièces fournies à chaque réception.",
                  "TPJ_CODE (acte de naissance, certificat de consolidation…), RDP_DATE_INSERT.",
                  "96 % des pièces sont saisies le jour de la réception."),
            table("AJUSTEMENT et PE_AJ",
                  "Les régularisations : trop perçus, moins perçus, remboursements.",
                  "le type d'ajustement (TP, MP, REM-TP…), son statut.",
                  "Un remboursement de trop perçu (REM-TP) n'est pas un indu de plus."),
            table("DOSSIERS_CONTROLES",
                  "Les contrôles a posteriori des dossiers payés.",
                  "le dossier contrôlé, l'année du contrôle.",
                  "Plus alimentée depuis juillet 2025 ; des années fausses (202, 2055)."),
            table("ETAPE",
                  "Les étapes de calcul d'une prestation.",
                  "le type d'étape (calcul initial, révision, majoration…).",
                  "Ce sont des calculs du montant, pas des événements de la vie."),
            table("TSINISTRE",
                  "Les sinistres des accidents du travail.",
                  "la date de l'accident, la nature de la lésion.",
                  "Plus alimentée depuis décembre 2024."),
        ]),
        Partie("Les codes à connaître", colonnes=4, blocs=[
            Bloc("Type de dossier (TDOS_CODE)", [
                ("", "V pensions · A accidents du travail et maladies professionnelles · F "
                     "prestations familiales · M maternité, rattachée aux prestations "
                     "familiales."),
            ], genre="donnee"),
            Bloc("Statut d'un dossier", [
                ("", "A actif · F fermé. Ni l'un ni l'autre ne dit si le dossier est liquidé."),
            ], genre="donnee"),
            Bloc("Statut d'une prestation", [
                ("", "A active · F fermée, avec une date de radiation · S suspendue, avec une "
                     "date de suspension. Fiable."),
            ], genre="donnee"),
            Bloc("Statut d'un débours", [
                ("", "D payé par un effet décaissé · P payé sans effet · V validé, pas encore "
                     "payé · N, A, S jamais approuvés. Paiement = D ou P."),
            ], genre="donnee"),
            Bloc("Type de débours", [
                ("", "PM paiement · RB remboursement de soins · AJ ajustement · CP autre. Seul "
                     "PM est un paiement de prestation."),
            ], genre="donnee"),
            Bloc("Type de réception", [
                ("", "NOUVEAU : une nouvelle demande, qui ouvre son dossier · ANCIEN : déposée "
                     "sur un dossier existant (réversion, demande de rente)."),
            ], genre="donnee"),
            Bloc("Catégorie de dossier", [
                ("", "ORDINAIRE · COOR_CNSS et COOR_AC : coordination avec une autre caisse · "
                     "CNSS_AC : transféré à une autre caisse. 120 518 dossiers sans catégorie."),
            ], genre="donnee"),
            Bloc("Directions régionales", [
                ("", "DRO Ouagadougou · DRB Bobo-Dioulasso · DRN Nord (Ouahigouya) · DRF Fada "
                     "N'Gourma · DRD Dédougou. 20 services, 350 lieux de paiement."),
            ], genre="donnee"),
        ]),
        Partie("Les 28 codes de prestation, et leur ligne CIPRES", colonnes=4,
               intro="Le type de prestation (TPE_CODE) donne la ligne du tableau (règle R2).",
               blocs=[
            Bloc("Les pensions (V)", [
                ("", "RPV pension de vieillesse · RPRA retraite anticipée · RPI invalidité · "
                     "RPSV veuve · RPSF veuf · RPSE orphelin · RPA ascendant."),
                ("Lignes", "T25, T28, T32 à T36."),
            ], genre="donnee"),
            Bloc("Les allocations (V)", [
                ("", "RAV allocation de vieillesse · RAS allocation de survivants, et ses parts "
                     "RASC conjoint, RASO orphelin, RASA ascendant. Versées une fois."),
                ("Lignes", "T25, T50, T51."),
            ], genre="donnee"),
            Bloc("Les accessoires (V)", [
                ("", "RBEC bonification pour enfant · RME et RME-COL majorations pour enfant · "
                     "RCCM charges du ménage · RRC remboursement de cotisations."),
                ("Lignes", "suivent la pension ; « autres prestations » en T25."),
            ], genre="donnee"),
            Bloc("Les accidents du travail (A)", [
                ("", "ARI rente viagère · ARSV, ARSF rentes de veuve, de veuf · ARSE orphelin · "
                     "ARSA ascendant · AIJ indemnités journalières · AAI allocation "
                     "d'incapacité. Plus les soins (codes en nature)."),
                ("Lignes", "T26, T29, T37 à T40, T48, T52, T53."),
            ], genre="donnee"),
            Bloc("Les prestations familiales (F)", [
                ("", "FAF allocations familiales · FAP ancienne allocation prénatale."),
                ("Lignes", "T27, T30, T41, T55."),
            ], genre="donnee"),
            Bloc("La maternité (M)", [
                ("", "MIJ indemnités journalières de maternité · MAP allocation prénatale."),
                ("Lignes", "T27, T30, T54, T55."),
            ], genre="donnee"),
        ]),
    ],
)

# =============================================================================================
# 04 La démarche
# =============================================================================================


def lot(ref, titre, question_txt, lance, trouve, change, pied):
    return Bloc(titre, [("La question de départ", question_txt), ("Ce qu'on a lancé", lance),
                        ("Ce qu'on a trouvé", trouve), ("Ce que ça a changé", change)],
                ref=ref, genre="etape", etat="fait", pied=pied)


F04 = Fiche(
    fichier=D + "04 la demarche.excalidraw",
    titre="La démarche : six étapes, de l'inventaire à l'annuaire de la CNSS",
    sous_titre="Fiche 04 du domaine des prestations · du 26 au 30/10/2026 · " + ETAT_AU,
    objectif="dans quel ordre on a travaillé, la question que posait chaque lot de requêtes, "
             "ce qu'il a montré et ce qu'il a changé, et ce qu'on a produit.",
    parties=[
        Partie("Le chemin parcouru", colonnes=3, flux=True,
               intro="Chaque étape répond aux questions laissées par la précédente. Les "
                     "flèches donnent l'ordre.",
               blocs=[
            lot("Lot 1", "L'inventaire",
                "Que contient la base, et les requêtes DTM se trompent-elles ?",
                "19 requêtes (P01 à P19) : volumes, codes, statuts, qualité des personnes ; "
                "test des 14 erreurs supposées des DTM.",
                "La base est une copie figée au 02/02/2026 ; M = maternité ; le statut F d'un "
                "dossier veut dire fermé ; le DTM ne garde que les dossiers payés, gonfle les "
                "montants 3,8 fois et compte le tuteur au lieu de l'orphelin.",
                "On recalcule tout depuis la base.",
                "26/10/2026 · lot1_inventaire_prestations.sql"),
            lot("Lot 2", "Les paiements et les pensionnés",
                "Qu'est-ce qu'un paiement ? Qui est pensionné au 31/12, qui est nouveau ? Les "
                "mois cotisés sont-ils fiables ?",
                "11 requêtes (P20 à P30) sur les débours, l'état des prestations, une première "
                "version de T32.",
                "Un paiement = débours D ou P ; 29 721 retraités de vieillesse payés fin 2025 ; "
                "8 447 pensions d'orphelin actives sans paiement ; mois cotisés recopiés.",
                "Les règles R1 à R12.",
                "26/10/2026 · lot2_statuts_stocks_nouveaux.sql"),
            lot("Lot 3 A", "Les listes de codes",
                "Quel est le libellé exact de chaque code ?",
                "17 requêtes (P31 à P47) : toutes les listes de codes et la géographie.",
                "28 codes de prestation sûrs ; RPRA = retraite anticipée ; les accessoires ; "
                "51 prestations en nature ; 6 directions, 20 services, 350 lieux de paiement.",
                "La correspondance code → ligne CIPRES ; le schéma des tables.",
                "29/10/2026 · lot3_listes_de_codes.sql"),
            lot("Lot 3 B", "Les questions de T25",
                "Les cinq questions de la fiche T25 : coordination, date du dépôt, vieilles "
                "demandes, pension de la veuve, refus.",
                "11 requêtes (P48 à P58), une ou plus par question.",
                "La CNSS instruit les dossiers de coordination ; le dossier est complet au "
                "guichet ; le stock de 23 218 demandes est faux ; la réversion est dans le "
                "dossier du défunt ; les refus sont rares.",
                "On compte des demandes (dossier × ligne) : règles D1 à D8, requête T25 revue.",
                "30/10/2026 · lot3b_controles_t25.sql"),
            lot("Lot 4", "T26 et T27",
                "Qu'est-ce qu'une demande en accident du travail, en prestations familiales, "
                "en maternité ?",
                "15 requêtes (P59 à P73).",
                "Les IJ puis la rente des années après ; 46 % de déclarations sans suite ; des "
                "IJ notifiées 7 mois après l'arrêt ; l'enfant ajouté sans guichet ; une "
                "grossesse sur deux lignes.",
                "Les règles D9 à D14 ; les requêtes et les fiches T26 et T27.",
                "30/10/2026 · lot4_controles_t26_t27.sql"),
            lot("Annuaire CNSS", "La comparaison avec l'annuaire de la CNSS 2024",
                "Notre base est-elle celle de la CNSS, et nos règles donnent-elles ses "
                "chiffres ?",
                "8 requêtes (A01 à A08), dont 5 exécutées, sur les tableaux 18 à 23 de "
                "l'annuaire.",
                "Le tableau 18 retrouvé à 2 ou 3 % près ; la CNSS compte un stock au 31/12, "
                "payé ou non ; un nouveau pensionné à la saisie ; les montants payés.",
                "R1 à R5 et A1 confirmées, R6 corrigée, R7 revue.",
                "30/10/2026 · analyses/comparaison_annuaire_cnss_2024"),
        ]),
        Partie("Ce qu'on a produit", colonnes=4, blocs=[
            Bloc("Les requêtes proposées", [
                ("", "t25, t26 et t27_requete_proposee.sql (T25 à T27) ; "
                     "dtm_dossier_requete_proposee.sql (la DTM_DOSSIER, à aligner) ; "
                     "t25_q4_deux_cas.sql (la question de la veuve). Aucune n'est encore "
                     "exécutée."),
            ], genre="suite"),
            Bloc("Les fiches de validation", [
                ("", "T25, T26 et T27, dans schemas/prestations/tableaux cipres : maquette, "
                     "exemples, dix règles avec leur SQL, quatre ou cinq questions. En attente "
                     "des réponses du métier."),
            ], genre="suite"),
            Bloc("Les schémas de travail", [
                ("", "regles_prestations (toutes les règles et leurs preuves), "
                     "tables_prestations, vie_dossier, modele_conceptuel_dossier, "
                     "domaine_prestations, prestation_esp."),
            ], genre="suite"),
            Bloc("Les documents et le suivi", [
                ("", "Pour la CNSS : « Prestations sociales (T25 à T57) », « Règles de calcul », "
                     "le guide du métier. Pour nous : README et TODO de analyses/prestations, "
                     "resultats/ pour les listes longues."),
            ], genre="suite"),
        ]),
    ],
)

# =============================================================================================
# 05 Les découvertes
# =============================================================================================

F05 = Fiche(
    fichier=D + "05 les decouvertes.excalidraw",
    titre="Les découvertes : ce que la base nous a appris",
    sous_titre="Fiche 05 du domaine des prestations · les faits B1 à B25, et ce que confirme "
               "l'annuaire de la CNSS · " + ETAT_AU,
    objectif="les faits établis dans les données, avec le chiffre qui les prouve et la "
             "raison pour laquelle ils comptent. Ce sont eux qui fondent les règles.",
    parties=[
        Partie("La base et ses statuts", colonnes=3, blocs=[
            constat("B1", "Une copie figée de la base",
                    "La base des prestations est une copie arrêtée au 02/02/2026 ; l'entrepôt en "
                    "est la copie complète.",
                    "dernière saisie le 02/02/2026 pour 10 tables.",
                    "Les chiffres de 2025 sont presque complets, ceux de 2026 non. Les décès et "
                    "les rappels saisis plus tard manquent.",
                    "Preuve : P01, P17"),
            constat("B2", "Les quatre branches",
                    "V pensions, A accidents du travail, F prestations familiales, M maternité, "
                    "rattachée aux prestations familiales.",
                    "aucune prestation en nature (soins) en M.",
                    "Le DTM appelle M « maladie » : ses tableaux mettent la maternité au "
                    "mauvais endroit.",
                    "Preuve : P02, P05, P31"),
            constat("B3", "Le statut d'un dossier",
                    "A veut dire actif, F fermé. Aucun ne veut dire « en instance » ou "
                    "« liquidé ».",
                    "en 2025, 310 dossiers au statut F pour 15 479 notifiés ; 61 632 dossiers "
                    "de pension actifs.",
                    "Le DTM lit F comme « liquidé » : il trouve 50 fois trop peu de "
                    "liquidations.",
                    "Preuve : P02, P10"),
            constat("B4", "Le statut d'une prestation",
                    "A active, F fermée (avec une date de radiation), S suspendue (avec une date "
                    "de suspension).",
                    "statut et dates concordent pour tous les codes.",
                    "On peut savoir si une pension était en cours à n'importe quelle date : "
                    "c'est la base de la règle R6.",
                    "Preuve : P24"),
            constat("B6", "Le type de pension vient du code",
                    "Le type vient du code de la prestation (28 codes), pas de la catégorie du "
                    "dossier, qui dit seulement ordinaire, coordination ou transfert.",
                    "28 codes au libellé sûr ; 120 518 dossiers sans catégorie.",
                    "La ligne d'un tableau se lit sur la prestation (règle R2).",
                    "Preuve : P03, P04, P33, P34"),
            constat("B7", "Les étapes sont des calculs",
                    "Les étapes d'une prestation sont des calculs du montant (initial, "
                    "révision, majoration), pas des événements de la vie.",
                    "13 types d'étape, un calcul initial par prestation.",
                    "On ne peut pas lire un décès ou une suspension dans les étapes : il faut "
                    "les dates de la prestation.",
                    "Preuve : P12, P40"),
        ]),
        Partie("Les paiements et les personnes", colonnes=3, blocs=[
            constat("B5", "Le statut d'un paiement",
                    "D = payé par un effet décaissé ; P = payé sans effet ; V = validé mais pas "
                    "payé ; N, A, S = jamais approuvés.",
                    "2025 : 53,72 Md au statut D et 7,63 Md au statut P, soit 61,35 Md.",
                    "C'est la définition d'un paiement (règle R3), confirmée par l'annuaire de "
                    "la CNSS.",
                    "Preuve : P20 à P23"),
            constat("B15", "Les paiements sans effet",
                    "62 % des paiements de prestations familiales de 2025 n'ont pas d'effet de "
                    "paiement.",
                    "tous les paiements sont pourtant reliés à leur prestation.",
                    "La personne doit se lire sur la prestation, pas sur l'effet : sinon les "
                    "allocataires disparaissent.",
                    "Preuve : P07, P20, P23"),
            constat("B14", "Les personnes",
                    "Le sexe et la date de naissance sont fiables, les décès saisis en retard. "
                    "Pour un orphelin, la personne payée est le tuteur.",
                    "deux fois moins de décès saisis en 2025 ; 29 689 pensions d'orphelin, "
                    "toutes payées à un tiers.",
                    "Compter la personne payée ferait compter des tuteurs adultes au lieu "
                    "d'orphelins (règle R5).",
                    "Preuve : P15, P16"),
            constat("B16", "Les mois cotisés",
                    "Ce sont ceux de l'assuré, recopiés sur chaque prestation du dossier ; 999 "
                    "est une valeur par défaut.",
                    "médiane de 295 mois pour une pension de vieillesse.",
                    "Les additionner à chaque paiement les gonfle (règle R9).",
                    "Preuve : P29, P30"),
            constat("B17", "Les tranches d'âge",
                    "La base a 21 tranches de 5 ans ; les 11 grilles de l'Annuaire s'obtiennent "
                    "en les regroupant.",
                    "toutes les bornes CIPRES sont des multiples de 5.",
                    "Pas besoin de nouvelle table : on regroupe (règle R8).",
                    "Preuve : P14, P42"),
            constat("B13", "Les contrôles a posteriori",
                    "Presque absents, et plus saisis depuis juillet 2025. Le drapeau "
                    "DEB_VERIFIE n'est pas un contrôle.",
                    "115 contrôles en 2025 pour 155 206 dossiers payés : 0,07 % (norme : 5 %).",
                    "T28 à T30 ne peuvent pas être justes tant que les contrôles ne sont pas "
                    "saisis.",
                    "Preuve : P06, P13"),
        ]),
        Partie("Le guichet et les dossiers de pension", colonnes=3, blocs=[
            constat("B8", "Deux sortes de demandes au guichet",
                    "NOUVEAU : une nouvelle demande, qui ouvre son dossier. ANCIEN : une demande "
                    "sur un dossier existant, presque toujours une réversion.",
                    "51 417 NOUVEAU, 4 018 ANCIEN ; 94 % des ANCIEN donnent une pension de "
                    "survivant.",
                    "Le type de réception dit à quelle demande rattacher un dépôt (règle D3).",
                    "Preuve : P52, P58"),
            constat("B9", "La réversion",
                    "Quand un pensionné décède, la pension de sa veuve et de ses orphelins est "
                    "ajoutée à son dossier.",
                    "13 104 des 25 541 pensions de veuve sont dans le dossier du défunt.",
                    "Compter des dossiers ferait perdre environ 900 réversions par an : on "
                    "compte des demandes (règle D2).",
                    "Preuve : P51, P54, P58"),
            constat("B10", "Le dossier est complet au guichet",
                    "Les pièces sont saisies le jour de la réception.",
                    "96 % des réceptions de 2025.",
                    "La date de réception est bien celle du dossier complet, comme le demande "
                    "l'atelier de Kaya.",
                    "Preuve : P49"),
            constat("B11", "Les trous des anciennes données",
                    "Les anciens dossiers n'ont pas de lien vers leur réception ; environ 8 000 "
                    "vieilles pensions radiées n'ont pas de date de notification.",
                    "13 149 réceptions d'avant 2016 sans dossier.",
                    "Sans précaution, elles restent « en instance » pour toujours (règle D6).",
                    "Preuve : P48, P50, P57"),
            constat("B12", "Les refus",
                    "Ils ne sont notés que dans le module des rejets, avec un motif, et "
                    "rarement.",
                    "146 rejets de pension depuis 2017, 14 en 2025.",
                    "La colonne « rejetés » restera presque vide tant que les services ne "
                    "saisissent pas les refus.",
                    "Preuve : P09, P55, P56"),
        ]),
        Partie("Les accidents du travail", colonnes=3, blocs=[
            constat("B18", "Un dossier, plusieurs liquidations",
                    "Les IJ sont liquidées d'abord, la rente ou l'allocation des années après, "
                    "dans le même dossier.",
                    "191 dossiers « IJ + allocation » sur 231 ont plusieurs dates de "
                    "notification.",
                    "Comme en T25, on compte une demande par ligne (règle D2).",
                    "Preuve : P59, P61"),
            constat("B19", "Beaucoup de déclarations sans suite",
                    "Un dossier sur deux ne donne jamais rien : ni IJ, ni soins, ni rente. Il "
                    "n'est ni fermé ni rejeté.",
                    "4 158 dossiers sur 9 106 ouverts de 2019 à 2025.",
                    "Sans limite, plus de 4 000 déclarations resteraient en instance (règle "
                    "D12).",
                    "Preuve : P59, P68"),
            constat("B20", "Les soins, un flux hors guichet",
                    "Les soins se remboursent 8 à 10 fois par dossier, sans passer par le "
                    "guichet. Les vieilles dates d'approbation ont été remplies en bloc en 2024.",
                    "650 à 960 dossiers par an.",
                    "On date les soins par la saisie du premier remboursement (règle D11).",
                    "Preuve : P63, P70, P73"),
            constat("B24", "Les IJ attendent 7 mois",
                    "L'arrêt de travail dure un mois, mais l'IJ est notifiée environ 7 mois "
                    "après sa fin, sans que la base dise pourquoi.",
                    "arrêt de 27 jours en médiane ; IJ 211 jours après la fin de l'arrêt.",
                    "Les IJ seront presque toutes « plus de 45 jours » : une question pour le "
                    "métier (Q4 de T26).",
                    "Preuve : P62, P69"),
            constat("B25", "Les demandes sur un dossier existant",
                    "Une réception ANCIEN sur un dossier d'accident est une demande de rente ou "
                    "de rente de survivant.",
                    "70 % suivies d'une rente ou d'une allocation, 9 % d'une rente de "
                    "survivant.",
                    "Elle sert de dépôt pour la rente, comme la réversion en T25 (règle D9).",
                    "Preuve : P60, P70, P71"),
        ]),
        Partie("Les prestations familiales et la maternité", colonnes=3, blocs=[
            constat("B21", "L'enfant ajouté ne passe pas au guichet",
                    "Chaque année, environ 9 000 dossiers d'allocataires reçoivent un enfant de "
                    "plus, sans réception.",
                    "2025 : 8 246 dossiers à leur première notification, 8 124 renotifiés.",
                    "Faut-il compter l'ajout comme une demande ? Il double le chiffre (règle "
                    "D13).",
                    "Preuve : P64, P65"),
            constat("B22", "Un dossier de maternité = une grossesse",
                    "Une mère a un dossier par grossesse, déposé 3 semaines avant "
                    "l'accouchement, avec l'IJ et l'allocation prénatale.",
                    "99,4 % des dossiers ont les deux ; notifiés 16 jours après la réception.",
                    "Une grossesse compte sur deux lignes de T27 (règle D14).",
                    "Preuve : P65, P67"),
            constat("B23", "L'allocation prénatale est notifiée",
                    "Elle passe par le dossier de maternité (code MAP), avec réception et "
                    "notification.",
                    "4 245 notifiées en 2025, 15 jours après la demande.",
                    "L'atelier de Kaya 2017 disait le contraire, pour l'ancien code : ce n'est "
                    "plus vrai.",
                    "Preuve : P66"),
        ]),
        Partie("Ce que confirme l'annuaire de la CNSS 2024", colonnes=3, blocs=[
            constat("A01", "La base est celle de la CNSS",
                    "Notre base retrouve le tableau 18 de l'annuaire (pensionnés de 2020 à "
                    "2024).",
                    "60 171 pensionnés en 2024 pour 62 231 publiés ; écart de 2 à 3 % chaque "
                    "année.",
                    "Nos chiffres sont exploitables, et nos règles se vérifient sur des "
                    "chiffres officiels.",
                    "analyses/comparaison_annuaire_cnss_2024"),
            constat("A01", "La CNSS compte un stock au 31 décembre",
                    "Elle compte les pensions en cours au 31/12, titulaire en vie, payées ou "
                    "non ; les orphelins jusqu'à 24 ans.",
                    "exiger un paiement au 4e trimestre donne 8 % de retraités de moins.",
                    "La règle R6 a été corrigée en conséquence.",
                    "analyses/comparaison_annuaire_cnss_2024"),
            constat("A03, A05", "Nouveaux pensionnés et montants",
                    "Un nouveau pensionné est compté à la saisie de sa pension ; les montants "
                    "publiés sont les montants payés.",
                    "4 090 saisies pour 4 037 publiés ; 47,24 Md payés pour 46,63 Md publiés.",
                    "R7 revue ; R3 confirmée.",
                    "analyses/comparaison_annuaire_cnss_2024"),
        ]),
    ],
)

# =============================================================================================
# 06 Les règles des pensionnés et des paiements
# =============================================================================================

F06 = Fiche(
    fichier=D + "06 les regles des pensionnes et des paiements.excalidraw",
    titre="Les règles des pensionnés et des paiements",
    sous_titre="Fiche 06 du domaine des prestations · R1 à R9 et A1, pour T31 à T57 · "
               + ETAT_AU,
    objectif="comment on range une prestation dans une branche et une ligne, ce qu'est un "
             "paiement, qui est compté comme pensionné ou comme nouveau pensionné, et comment "
             "répondre si on te conteste une de ces règles.",
    parties=[
        Partie("Les branches et les lignes", colonnes=3, blocs=[
            regle("R1", "Les branches", "prouvé",
                  "V → pensions (PVID) ; A → accidents du travail (AT/MP) ; F et M → "
                  "prestations familiales.",
                  "Les tableaux sont par branche ; la maternité doit rejoindre les PF.",
                  "aucun soin payé en M : M n'est pas la maladie. La branche des pensions "
                  "retombe sur l'annuaire de la CNSS à 1,3 % près.",
                  "pourquoi la maternité dans les PF ?",
                  "Parce que le type M est la maternité, rattachée à la branche PF dans la "
                  "base même.",
                  "Requêtes : P02, P05, P31 · Tous les tableaux"),
            regle("R2", "La ligne CIPRES", "proposé",
                  "La ligne d'un tableau se lit sur le code de la prestation, selon la "
                  "correspondance de la fiche 03.",
                  "La catégorie du dossier ne dit pas le type de pension.",
                  "28 codes au libellé sûr ; pour les pensions, une ligne par code, comme "
                  "l'annuaire de la CNSS.",
                  "où va la retraite anticipée ?",
                  "Sur sa propre ligne en T25, comme l'a décidé l'atelier de Kaya ; ailleurs "
                  "avec la vieillesse, à confirmer.",
                  "Requêtes : P04, P34 · Tous les tableaux"),
            regle("A1", "Les accessoires de pension", "prouvé",
                  "Bonification ou majoration pour enfant, charges du ménage : ils s'ajoutent "
                  "à une pension, ne font pas un pensionné de plus et suivent sa ligne.",
                  "Sinon, un retraité avec trois bonifications compterait quatre fois.",
                  "68 836 bonifications (RBEC). L'annuaire de la CNSS ne compte que les 7 types "
                  "de pension.",
                  "les accessoires sont-ils perdus ?",
                  "Non : ils sont dans les montants de la branche, pas dans les effectifs.",
                  "Requête : P34 · T25, T32 à T49"),
        ]),
        Partie("Les paiements", colonnes=2, blocs=[
            regle("R3", "Ce qu'est un paiement", "prouvé",
                  "Un paiement = un débours de type PM au statut D ou P. Son montant est celui "
                  "du débours, jamais le net à payer de l'effet.",
                  "V n'est pas encore payé ; le net à payer est le total de l'effet, répété "
                  "sur chacun de ses débours.",
                  "61,35 Md payés en 2025 ; le DTM, avec le net à payer, trouve 222,9 Md pour "
                  "58,5 Md (3,8 fois trop). Montants à 0,7–2,5 % de l'annuaire de la CNSS.",
                  "pourquoi pas le statut V ?",
                  "Validé ne veut pas dire décaissé ; l'atelier de Kaya dit « payées = sommes "
                  "décaissées ».",
                  "Requêtes : P07, P08, P20, P21, P36 · T31, T32 à T56"),
            regle("R4", "Le mois d'un paiement", "prouvé",
                  "Un paiement compte au mois qu'il couvre (DEB_DATE_EFFET), pas à la date du "
                  "versement.",
                  "Les débours sont préparés environ 3 semaines avant l'échéance ; un rappel "
                  "couvre un mois passé.",
                  "9 % des débours sont saisis 2 mois ou plus après leur mois ; les montants "
                  "annuels ainsi calculés retombent sur l'annuaire de la CNSS.",
                  "un rappel de 2024 payé en 2025 ?",
                  "Il compte en 2024, le mois qu'il couvre.",
                  "Requête : P20 · T31, T50 à T56"),
        ]),
        Partie("Les personnes et les pensionnés", colonnes=3, blocs=[
            regle("R5", "La personne comptée", "proposé",
                  "L'ayant droit de la prestation. Pour les allocations familiales, "
                  "l'allocataire.",
                  "Pour un orphelin, la personne payée est le tuteur, un adulte.",
                  "29 689 pensions d'orphelin, toutes payées à un tiers ; l'annuaire de la CNSS "
                  "compte bien l'orphelin, de 0 à 24 ans.",
                  "et le tuteur ?",
                  "Il reçoit l'argent, mais le droit est celui de l'orphelin : c'est lui qu'on "
                  "compte, par son âge.",
                  "Requête : P16 · T32 à T49"),
            regle("R6", "Le pensionné au 31 décembre", "proposé",
                  "Pension en cours au 31/12 (effet avant, ni radiée ni suspendue), titulaire "
                  "en vie, payée ou non ; orphelins de moins de 25 ans ; une personne comptée "
                  "une fois.",
                  "C'est la définition de l'annuaire de la CNSS (corrigée le 30/10/2026 ; avant, "
                  "on exigeait un paiement au 4e trimestre).",
                  "retraités 2024 : 31 180 pour 30 730 publiés (+1,5 %) ; avec le paiement : "
                  "8 % de moins.",
                  "une pension non payée en décembre compte-t-elle ?",
                  "Oui, si elle est en cours et le titulaire en vie, comme dans l'annuaire de la "
                  "CNSS. La limite de 25 ans des orphelins reste à confirmer.",
                  "Requêtes : P24, P25, P26 ; annuaire A01 · T32 à T41"),
            regle("R7", "Le nouveau pensionné", "proposé",
                  "Pension saisie dans l'année ; une personne comptée une fois.",
                  "C'est ainsi que compte l'annuaire de la CNSS (revue le 30/10/2026 ; avant, "
                  "la notification).",
                  "2024 : 4 090 saisies pour 4 037 publiés ; la notification en donne 3 888.",
                  "pourquoi pas la date d'effet ?",
                  "Un tiers des pensions ont un effet rétroactif : la date d'effet compte trop "
                  "tôt. À confirmer avec la direction des prestations.",
                  "Requêtes : P27, P28 ; annuaire A03 · T42 à T49"),
            regle("R8", "L'âge", "proposé",
                  "Année de l'exercice moins année de naissance ; tranches de 5 ans "
                  "regroupées selon la grille de chaque tableau.",
                  "C'est la convention des tableaux CIPRES, et la base a déjà ses tranches.",
                  "naissance fiable ; 21 tranches de 5 ans.",
                  "et l'âge exact au 31/12 ?",
                  "L'écart est de quelques mois : la convention par année est plus simple et "
                  "stable.",
                  "Requêtes : P14, P15, P42 · T32 à T49"),
            regle("R9", "Les services passés", "proposé",
                  "Mois cotisés de la dernière prestation de la personne ; valeurs au-dessus "
                  "de 600 écartées ; total seulement.",
                  "Les mois sont recopiés sur chaque prestation : les additionner les gonfle.",
                  "médiane de 295 mois ; 999 = valeur par défaut.",
                  "pourquoi écarter au-delà de 600 ?",
                  "600 mois = 50 ans de cotisation : au-delà, c'est une valeur par défaut, pas "
                  "une carrière.",
                  "Requêtes : P29, P30 · T42, T50"),
        ]),
    ],
)

# =============================================================================================
# 07 Les règles des dossiers (T25 à T27)
# =============================================================================================

F07 = Fiche(
    fichier=D + "07 les regles des dossiers.excalidraw",
    titre="Les règles des dossiers : T25, T26 et T27",
    sous_titre="Fiche 07 du domaine des prestations · D1 à D14 · " + ETAT_AU,
    objectif="pourquoi on compte des demandes et pas des dossiers, comment chaque colonne de "
             "T25 à T27 est calculée, ce qui change pour les accidents du travail et les "
             "prestations familiales, et ce que demandent les fiches de validation.",
    parties=[
        Partie("L'idée clé : on compte des demandes", colonnes=3, blocs=[
            Bloc("Ce que demande le tableau", [
                ("", "Pour chaque type de prestation : les dossiers déposés, liquidés (en moins "
                     "de 15 jours, de 15 à 45, plus de 45), en instance au 31/12, rejetés. Plus "
                     "la durée moyenne, comparée à la norme de 45 jours (Annuaire p. 12, 34)."),
            ]),
            Bloc("Pourquoi pas des dossiers", [
                ("", "Un dossier de pension porte la pension du retraité, puis, à son décès, la "
                     "réversion de sa veuve et de ses orphelins, notifiée des années après. "
                     "Compter les dossiers ferait perdre ces réversions : environ 900 en 2025."),
            ]),
            Bloc("Une demande = un dossier × une ligne", [
                ("", "Le dossier d'un pensionné décédé porte deux demandes : sa pension (ligne "
                     "1) et la réversion (ligne 4). Chacune a son dépôt, sa liquidation et sa "
                     "durée. Un dossier qui n'a qu'une ligne est une seule demande."),
            ]),
        ]),
        Partie("Les règles communes (T25 à T27)", colonnes=3, blocs=[
            regle("D1", "Les dossiers concernés", "proposé",
                  "Les dossiers de la branche, y compris la coordination payée par une autre "
                  "caisse ; sans les dossiers transférés à une autre caisse.",
                  "La CNSS reçoit et instruit la coordination : l'exclure cacherait un vrai "
                  "travail.",
                  "coordination : 76 % reçus au guichet de la CNSS, 238 dépôts et 292 "
                  "liquidations en 2025.",
                  "pourquoi garder un dossier payé ailleurs ?",
                  "T25 mesure le travail de liquidation de la CNSS, pas qui paie.",
                  "Requêtes : P03, P48 · question Q1 de la fiche T25"),
            regle("D2", "On compte des demandes", "prouvé",
                  "Une demande = un dossier × une ligne du tableau.",
                  "La réversion est ajoutée au dossier du défunt.",
                  "en 2025, 675 des 800 pensions de veuve notifiées ne sont pas la première "
                  "notification de leur dossier.",
                  "la veuve a-t-elle son propre dossier ?",
                  "Les deux pratiques existent ; compter par demande les traite de la même "
                  "façon.",
                  "Requêtes : P51, P52, P54, P58 · question Q4 de la fiche T25"),
            regle("D3", "Déposé", "proposé",
                  "Une réception au guichet = un dépôt. NOUVEAU va à la demande du dossier "
                  "qu'elle ouvre, ANCIEN à la réversion du dossier visé. Une réversion sans "
                  "réception prend la date de demande de sa pension.",
                  "L'Annuaire fait partir la durée du dépôt ; l'atelier de Kaya, de la "
                  "réception du dossier complet.",
                  "4 671 réceptions de pension en 2025 : 4 055 NOUVEAU, 616 ANCIEN ; 96 % des "
                  "pièces saisies le jour même.",
                  "et si des pièces arrivent plus tard ?",
                  "4 % seulement : la date de réception reste celle du dossier complet.",
                  "Requêtes : P09, P49, P52, P58"),
            regle("D4", "Liquidé", "prouvé",
                  "Une demande est liquidée à la première notification de ses droits, l'année "
                  "de cette notification ; jamais d'après le statut du dossier.",
                  "Le statut F veut dire fermé.",
                  "2025 : 3 901 dossiers + environ 900 réversions ≈ 4 800 liquidations ; le "
                  "statut F n'en trouvait que 310.",
                  "pourquoi la première notification ?",
                  "Les suivantes sont des révisions ou des réversions, comptées à part.",
                  "Requêtes : P10, P27, P48, P54"),
            regle("D5", "La durée de liquidation", "proposé",
                  "Jours entre le dépôt et la première notification (négatif = 0). Colonnes : "
                  "moins de 15 jours, 15 à 45, plus de 45 ; moyenne et médiane.",
                  "C'est la définition de l'Annuaire (p. 64).",
                  "pensions 2025 : médiane 62 jours, moyenne 82 ; 18 %, 22 % et 60 % des "
                  "dossiers.",
                  "pourquoi aussi la médiane ?",
                  "Quelques dossiers très lents font monter la moyenne ; la médiane montre le "
                  "cas courant.",
                  "Requêtes : P10, P49"),
            regle("D6", "En instance au 31/12", "proposé",
                  "Déposée, ni liquidée ni rejetée au 31/12. Sans les dossiers fermés jamais "
                  "notifiés, ni les vieilles réceptions jamais reliées à un dossier.",
                  "Sans ces exclusions, les trous des anciennes données restent en stock pour "
                  "toujours.",
                  "sans exclusion : 23 218 demandes en instance fin 2025, cinq fois les dépôts "
                  "d'une année.",
                  "quel est le vrai stock ?",
                  "Quelques centaines par an. La date de départ (2016) ou une limite (24 mois "
                  "en T26 et T27) reste à valider.",
                  "Requêtes : P50, P57, P58 · question Q3 de la fiche T25"),
            regle("D7", "Rejeté", "prouvé",
                  "Les refus enregistrés dans le module des rejets, comptés l'année du rejet.",
                  "C'est le seul endroit où un refus est noté.",
                  "14 rejets de pension en 2025 ; ni le statut de la réception ni la fermeture "
                  "ne valent refus.",
                  "pourquoi si peu de rejets ?",
                  "Les services ne saisissent pas les refus, comme le notait déjà l'atelier de "
                  "Kaya.",
                  "Requêtes : P09, P55, P56 · question Q5 de la fiche T25"),
            regle("D8", "Le Total et le taux restant à traiter", "proposé",
                  "Le Total recalcule la durée sur toutes les demandes. Taux = en instance au "
                  "31/12 ÷ (en instance au 1er janvier + déposées dans l'année).",
                  "Une moyenne des moyennes des lignes serait fausse ; l'atelier de Kaya cite "
                  "le taux sans formule.",
                  "proposition BI, sans source écrite pour la formule.",
                  "d'où vient la formule ?",
                  "De nous : c'est la part du travail de l'année encore à faire au 31/12.",
                  "Requêtes : aucune · T25 à T27"),
        ]),
        Partie("Ce qui change pour les accidents du travail (T26)", colonnes=2, blocs=[
            regle("D9", "Les dépôts", "proposé",
                  "La déclaration dépose les IJ et les soins. Une rente, une allocation ou une "
                  "rente de survivant est déposée par sa demande au guichet (certificat de "
                  "consolidation ou de décès), sinon à la date du certificat pour la rente.",
                  "La rente arrive des années après la déclaration, par une nouvelle demande.",
                  "70 % des demandes sur un dossier existant sont suivies d'une rente ; durée "
                  "médiane depuis cette demande : 13 à 35 jours.",
                  "et l'atelier de Kaya, qui part du certificat ?",
                  "C'est l'autre choix (64 à 107 jours) : question Q3 de la fiche T26.",
                  "Requêtes : P60, P62, P71"),
            regle("D10", "Les IJ et l'allocation d'incapacité", "proposé",
                  "Une IJ est déposée avec la déclaration et liquidée à sa première "
                  "notification ; sa durée part de la déclaration. L'allocation suit la règle "
                  "D4.",
                  "Aucune date de la base ne marque la fin du dossier d'IJ.",
                  "IJ de 2025 : 183 jours en médiane, pour un mois d'arrêt ; allocation "
                  "notifiée depuis 2019.",
                  "pourquoi les IJ sont-elles si lentes ?",
                  "On ne sait pas : c'est la question Q4 de la fiche T26.",
                  "Requêtes : P61, P62, P69, P72"),
            regle("D11", "Les soins", "proposé",
                  "Les soins vont en « autres prestations de la branche ». Un dossier y compte "
                  "une fois, liquidé à la saisie de son premier remboursement.",
                  "Les soins n'ont pas de notification ; les vieilles dates d'approbation ont "
                  "été remplies en bloc en 2024.",
                  "650 à 960 dossiers par an, 3 à 5 mois après la déclaration.",
                  "les soins entrent-ils dans T26 ?",
                  "Notre proposition est oui ; c'est la question Q2 de la fiche T26.",
                  "Requêtes : P59, P63, P73"),
            regle("D12", "Les déclarations sans suite", "ouvert",
                  "Une déclaration qui n'a rien donné reste en instance au plus 24 mois, puis "
                  "sort du stock (proposition).",
                  "Une sur deux ne donne jamais rien et n'est jamais fermée.",
                  "35 à 53 % des dossiers ouverts chaque année.",
                  "est-ce une demande à liquider ?",
                  "À la direction des prestations de le dire : question Q1 de la fiche T26.",
                  "Requêtes : P59, P68"),
        ]),
        Partie("Ce qui change pour les prestations familiales (T27)", colonnes=2, blocs=[
            regle("D13", "L'enfant ajouté", "ouvert",
                  "Si c'est une demande : déposée à la date de demande de son allocation, "
                  "liquidée à sa notification.",
                  "Il ne passe pas au guichet, et il est aussi fréquent que les nouveaux "
                  "allocataires.",
                  "2025 : 8 246 liquidations sans les ajouts, environ 16 400 avec.",
                  "faut-il le compter ?",
                  "Notre proposition est oui (c'est la moitié du travail) ; question Q1 de la "
                  "fiche T27.",
                  "Requêtes : P64, P65"),
            regle("D14", "Une grossesse, deux lignes", "proposé",
                  "Le dossier d'une grossesse compte une demande en IJ de maternité et une en "
                  "allocations prénatales, déposées et liquidées ensemble.",
                  "C'est la règle D2 ; T54 et T55 comptent aussi les deux.",
                  "99,4 % des dossiers portent les deux ; 4 211 en 2025.",
                  "le Total compte-t-il la grossesse deux fois ?",
                  "Oui, comme deux demandes ; sinon, il faut choisir une ligne : question Q2 "
                  "de la fiche T27.",
                  "Requêtes : P66, P67"),
        ]),
        Partie("Les fiches de validation", colonnes=3,
               intro="Dans schemas/prestations/tableaux cipres. Le métier coche une case par "
                     "règle et répond aux questions.",
               blocs=[
            Bloc("T25 · les dossiers de pension", [
                ("Les questions", "Q1 la coordination · Q2 la date du dépôt · Q3 les vieilles "
                                  "demandes · Q4 la pension de la veuve · Q5 les refus."),
                ("Ce qu'on attend", "les réponses de la direction des prestations."),
            ], genre="suite", etat="en cours"),
            Bloc("T26 · les dossiers d'accident du travail", [
                ("Les questions", "Q1 les déclarations sans suite · Q2 les soins · Q3 le "
                                  "départ de la durée d'une rente · Q4 l'attente des IJ."),
                ("Ce qu'on attend", "les réponses de la direction des prestations."),
            ], genre="suite", etat="en cours"),
            Bloc("T27 · les dossiers de prestations familiales", [
                ("Les questions", "Q1 l'enfant ajouté · Q2 la grossesse sur deux lignes · Q3 "
                                  "les demandes sans suite · Q4 les refus."),
                ("Ce qu'on attend", "les réponses de la direction des prestations."),
            ], genre="suite", etat="en cours"),
        ]),
    ],
)

# =============================================================================================
# 08 Les erreurs des DTM actuels
# =============================================================================================

F08 = Fiche(
    fichier=D + "08 les erreurs des dtm actuels.excalidraw",
    titre="Les erreurs des requêtes DTM actuelles",
    sous_titre="Fiche 08 du domaine des prestations · les hypothèses H1 à H14, vérifiées dans "
               "les données · " + ETAT_AU,
    objectif="quelles tables DTM alimentent les tableaux, les quatorze erreurs qu'on soupçonnait "
             "et ce que les données en ont dit, et ce qu'on propose à la place.",
    parties=[
        Partie("Les tables DTM du domaine", colonnes=3, blocs=[
            Bloc("DTM_DOSSIER", [("", "T25 à T27 : dossiers déposés, liquidés, en instance, "
                                      "rejetés. Requête dossier.sql.")], genre="donnee"),
            Bloc("DTM_CONTROLE_PRESTATION", [("", "T28 à T30 : dossiers payés et contrôlés. "
                                                  "Requête controle_prestation.sql.")],
                 genre="donnee"),
            Bloc("DTM_PRESTATION_INDUE", [("", "T31 : trop perçus et moins perçus. Requête "
                                               "prestation_indue.sql.")], genre="donnee"),
            Bloc("DTM_PRESTATION", [("", "T32 à T57 : bénéficiaires et montants, par mois. "
                                         "Requête prestation.sql.")], genre="donnee"),
            Bloc("DTM_INDICATEURS_PRESTATIONS", [("", "Les taux : durée, contrôles, indus. "
                                                      "Requête indicateurs_prestations.sql.")],
                 genre="donnee"),
        ]),
        Partie("Les quatorze erreurs supposées", colonnes=3, blocs=[
            erreur("H1", "Seuls les dossiers payés sont gardés", "confirmée",
                   "DTM_DOSSIER ne garde que les dossiers qui ont un paiement.",
                   "en 2025, il garde 33 % des dossiers AT/MP ouverts, 61 % en PF, 83 % en "
                   "pensions.",
                   "Les demandes pas encore payées, en instance ou rejetées disparaissent.",
                   "Requête : P09"),
            erreur("H2", "L'année d'ouverture partout", "tranchée",
                   "Un dossier est rangé à l'année de son ouverture, même pour sa liquidation.",
                   "un dossier reçu en 2024 et liquidé en 2025 doit compter en 2025 comme "
                   "liquidé.",
                   "Chaque événement compte à sa propre date (règles D3, D4, D6).",
                   "Conception"),
            erreur("H3", "Les délais nuls ou négatifs", "écartée",
                   "Les tranches de durée perdraient les délais nuls ou négatifs.",
                   "6 délais nuls, aucun négatif en 2025.",
                   "Rien à corriger ; on ramène quand même un négatif à 0.",
                   "Requête : P10"),
            erreur("H4", "Le statut F lu comme « liquidé »", "confirmée",
                   "Un dossier au statut F est compté liquidé, au statut A en instance.",
                   "F = fermé : 310 « liquidés » en 2025 pour 15 479 notifiés.",
                   "Les liquidations sont 50 fois trop faibles, le stock énorme.",
                   "Requêtes : P02, P10"),
            erreur("H5", "M lu comme « maladie »", "tranchée",
                   "La branche M est libellée « Maladie ».",
                   "M = maternité, branche PF : aucun soin payé en M.",
                   "La maternité va dans T27, T54, T55.",
                   "Requêtes : P02, P05"),
            erreur("H6", "Le net à payer répété", "confirmée",
                   "Le montant pris est le net à payer de l'effet, répété sur chacun de ses "
                   "débours.",
                   "222,9 Md au lieu de 58,5 Md en 2025 : 3,8 fois trop.",
                   "Tous les montants de T31 à T56 sont faux ; il faut le montant du débours "
                   "(R3).",
                   "Requête : P08"),
            erreur("H7", "Les débours annulés", "écartée",
                   "Des débours annulés compteraient comme payés.",
                   "aucun paiement annulé dans les données ; mais le statut n'est pas filtré.",
                   "On filtre quand même D et P (règle R3).",
                   "Requête : P07"),
            erreur("H8", "Des bénéficiaires par mois", "tranchée",
                   "Les bénéficiaires sont comptés mois par mois.",
                   "des personnes distinctes par mois ne s'additionnent pas sur l'année.",
                   "Il faut un effectif au 31/12 (règle R6) : une table annuelle à proposer.",
                   "Conception"),
            erreur("H9", "Une seule grille d'âge", "en partie",
                   "Une seule grille de tranches d'âge pour tous les tableaux.",
                   "la base a 21 tranches de 5 ans ; l'Annuaire en demande 11 grilles.",
                   "On regroupe les tranches de 5 ans selon chaque tableau (règle R8).",
                   "Requête : P14"),
            erreur("H10", "Les mois cotisés additionnés", "en partie",
                   "Les mois cotisés sont additionnés à chaque paiement.",
                   "ils sont recopiés sur chaque prestation ; 999 par défaut.",
                   "Services passés gonflés : on prend la dernière prestation (règle R9).",
                   "Requêtes : P29, P30"),
            erreur("H11", "Le contrôle par DEB_VERIFIE", "confirmée",
                   "Un débours « vérifié » est compté comme un contrôle a posteriori.",
                   "DEB_VERIFIE vérifie la saisie ; les vrais contrôles : 0,07 % en 2025.",
                   "Le taux de contrôle est faux ; il faut la table des contrôles (règle C1).",
                   "Requête : P13"),
            erreur("H12", "Tout ajustement non trop perçu = moins perçu", "en partie",
                   "Un ajustement qui n'est pas un trop perçu est rangé en moins perçu.",
                   "vrai, sauf le remboursement de trop perçu (REM-TP) ; et la maternité est "
                   "exclue à tort.",
                   "Les indus sont mal classés (règle I1).",
                   "Requête : P11"),
            erreur("H13", "La personne payée", "confirmée",
                   "La personne comptée est celle qui reçoit l'argent.",
                   "pour les orphelins, c'est le tuteur, jamais l'orphelin.",
                   "Les orphelins sont comptés comme des adultes (règle R5).",
                   "Requête : P16"),
            erreur("H14", "La table des types de prestation", "écartée",
                   "DIM_TYPE_PRESTATION ne serait pas chargée.",
                   "elle existe, remplie à la main.",
                   "Rien à corriger.",
                   "Requête : P18"),
        ]),
        Partie("Ce qu'on propose à la place", colonnes=3, blocs=[
            Bloc("DTM_DOSSIER", [
                ("", "Une requête proposée : mêmes colonnes plus la somme des délais ; chaque "
                     "événement compté à son mois ; le stock en fin de mois. À aligner sur les "
                     "demandes (règle D2), avec l'accord de la personne en charge d'Oracle "
                     "Analytics."),
            ], genre="suite", etat="en cours"),
            Bloc("DTM_PRESTATION", [
                ("", "À corriger : montant du débours, paiements D et P, mois payé, ayant "
                     "droit, maternité dans les PF. Pour les effectifs, une table annuelle des "
                     "bénéficiaires, à proposer avant toute création."),
            ], genre="suite", etat="à faire"),
            Bloc("DTM_ACCIDENT_TRAVAIL", [
                ("", "Elle cherche les codes IJ et RV, qui n'existent pas : les vrais sont AIJ "
                     "et ARI. Ses montants sont sans doute toujours à zéro (à vérifier). "
                     "Correction à proposer."),
            ], genre="suite", etat="à faire"),
        ]),
    ],
)

# =============================================================================================
# 09 Les questions ouvertes
# =============================================================================================

F09 = Fiche(
    fichier=D + "09 les questions ouvertes.excalidraw",
    titre="Les questions encore ouvertes",
    sous_titre="Fiche 09 du domaine des prestations · à la direction des prestations et à la "
               "DSI · " + ETAT_AU,
    objectif="ce qui attend encore une réponse, à qui on le demande, pourquoi, et ce que chaque "
             "réponse changerait dans les tableaux.",
    parties=[
        Partie("À la direction des prestations : les dossiers (T25 à T27)", colonnes=3,
               intro="Posées dans les fiches de validation. Notre proposition est déjà dans les "
                     "requêtes ; la réponse peut la changer.",
               blocs=[
            question("T25 Q1", "La coordination",
                     "Les dossiers payés par une autre caisse comptent-ils dans T25 ?",
                     "La CNSS les reçoit et en notifie la moitié.",
                     "6 à 8 % de T25 en plus ou en moins.",
                     "Les garder ; n'exclure que les dossiers transférés."),
            question("T25 Q3", "Les vieilles demandes",
                     "Une demande en attente depuis des années reste-t-elle en instance ?",
                     "Telle quelle, la règle compte 23 218 demandes en instance fin 2025.",
                     "Le stock et le taux restant à traiter.",
                     "Compter à partir de 2016, ou au plus 24 mois sans suite."),
            question("T25 Q4", "La pension de la veuve",
                     "Est-elle ouverte dans un nouveau dossier ou dans celui du défunt ?",
                     "Les deux pratiques existent dans la base.",
                     "Rien, si on compte par demande ; sinon, 900 réversions perdues.",
                     "Compter chaque demande (règle D2)."),
            question("T25 Q5", "Les refus",
                     "Les refus sont-ils tous saisis dans le module des rejets ?",
                     "On n'en trouve qu'une quinzaine par an.",
                     "La colonne « rejetés » et le stock en instance.",
                     "Demander aux services de saisir les refus."),
            question("T26 Q1", "Les déclarations sans suite",
                     "Une déclaration d'accident sans arrêt ni soin est-elle une demande à "
                     "liquider ?",
                     "Une sur deux ne donne jamais rien.",
                     "Environ 550 dépôts par an, et le stock.",
                     "La compter, au plus 24 mois en instance."),
            question("T26 Q2", "Les soins",
                     "Les remboursements de soins entrent-ils dans T26 ?",
                     "C'est la prestation la plus fréquente de la branche.",
                     "Environ 650 demandes par an en « autres prestations ».",
                     "Oui, liquidés au premier remboursement saisi."),
            question("T26 Q3", "Le départ de la durée d'une rente",
                     "La demande au guichet, ou le certificat de consolidation ?",
                     "L'atelier de Kaya cite le certificat ; la base a les deux.",
                     "14 jours ou 64 jours en médiane.",
                     "La demande au guichet, connue pour toutes."),
            question("T26 Q4", "L'attente des IJ",
                     "Qu'attend une IJ pendant 7 mois après la fin de l'arrêt ?",
                     "La base n'a aucune date pour l'expliquer.",
                     "Si une pièce est saisie, la durée pourrait partir d'elle.",
                     "Compter depuis la déclaration en attendant."),
            question("T27 Q1", "L'enfant ajouté",
                     "Un enfant ajouté au dossier d'un allocataire est-il une demande ?",
                     "Il ne passe pas au guichet, et il est aussi fréquent que les nouveaux.",
                     "8 246 ou environ 16 400 liquidations en 2025.",
                     "Oui : c'est la moitié du travail des PF."),
            question("T27 Q2", "La grossesse sur deux lignes",
                     "Une grossesse compte-t-elle en IJ et en allocations prénatales, ou une "
                     "fois ?",
                     "Le même dossier porte les deux.",
                     "La ligne des allocations prénatales, et le Total.",
                     "Les deux lignes, comme T54 et T55."),
        ]),
        Partie("À la direction des prestations : les pensionnés et les paiements", colonnes=3,
               blocs=[
            question("R6", "Le pensionné au 31 décembre",
                     "Retient-on, comme l'annuaire de la CNSS, la pension en cours, payée ou "
                     "non ?",
                     "C'est ce que publie la CNSS ; avant, on exigeait un paiement.",
                     "8 % de retraités en plus ou en moins.",
                     "Oui, s'aligner sur l'annuaire."),
            question("R6", "Les orphelins de 25 ans et plus",
                     "La limite d'âge est-elle 25 ans ? Faut-il radier les pensions plus "
                     "âgées ?",
                     "8 447 pensions d'orphelin actives n'ont aucun paiement ; l'annuaire ne "
                     "compte que les moins de 25 ans.",
                     "Environ 6 000 orphelins en plus ou en moins.",
                     "Moins de 25 ans, et signaler les pensions à radier."),
            question("R7", "Le nouveau pensionné",
                     "Le compte-t-on à la saisie de sa pension ou à sa notification ?",
                     "L'annuaire de la CNSS compte à la saisie.",
                     "Environ 4 % de nouveaux pensionnés.",
                     "La saisie, comme l'annuaire."),
            question("R2", "La retraite anticipée et les accessoires",
                     "Dans quelle ligne vont-ils ?",
                     "La retraite anticipée a sa ligne en T25 (Kaya), pas ailleurs.",
                     "La ligne vieillesse des autres tableaux.",
                     "Avec la vieillesse hors T25 ; accessoires dans les montants."),
            question("T57", "Les décédés",
                     "Bénéficiaires décédés, ou assurés décédés qui ouvrent des droits ?",
                     "L'atelier de Kaya l'a renommé « assurés décédés déclarés ».",
                     "Le contenu de T57.",
                     "Suivre Kaya, à reconfirmer."),
            question("I1", "Les indus",
                     "Un remboursement de trop perçu est-il un indu ? Les avances et les mois "
                     "gratuits ?",
                     "La requête actuelle les mélange.",
                     "Le taux d'indus, comparé à la norme de 2 %.",
                     "Les écarter ; garder trop perçus et moins perçus."),
            question("P26", "Le contrôle d'existence",
                     "69 pensionnés payés ont 100 ans ou plus, 215 actifs sont décédés : un "
                     "contrôle est-il prévu ?",
                     "Des pensions versées à tort, peut-être.",
                     "Les indus et le nombre de pensionnés.",
                     "Signaler ces cas à la direction."),
        ]),
        Partie("À la DSI", colonnes=3, blocs=[
            question("DSI", "Le sens des statuts",
                     "Que veulent dire les statuts sans libellé : dossier, prestation, débours, "
                     "effet, réception, notification, ajustement ?",
                     "On les a déduits des données.",
                     "La confirmation de presque toutes nos règles.",
                     "Nos lectures : B3 à B5."),
            question("DSI", "Les paiements sans effet",
                     "Pourquoi 62 % des paiements PF sont-ils sans effet ? Quel est le circuit "
                     "P ?",
                     "On ne voit pas comment ils sont versés.",
                     "La personne payée et la date du paiement.",
                     "Lire la personne sur la prestation."),
            question("DSI", "Les contrôles a posteriori",
                     "Où sont saisis les contrôles depuis juillet 2025 ?",
                     "La table n'est plus alimentée.",
                     "T28 à T30.",
                     "Rien sans réponse : tableaux bloqués."),
            question("DSI", "Le lien du déposant",
                     "Que veulent dire les codes L, V, T, F, A, et le statut A des réceptions "
                     "depuis mai 2025 ?",
                     "L semble vouloir dire « le demandeur lui-même ».",
                     "La lecture des réversions.",
                     "Notre lecture, à confirmer."),
            question("DSI", "Les mois cotisés et les catégories",
                     "Que veut dire 999 ? Les 120 518 dossiers sans catégorie sont-ils tous "
                     "ordinaires ?",
                     "Des valeurs par défaut faussent les calculs.",
                     "T42, T50 et la coordination.",
                     "Écarter 999 ; lire « sans catégorie » comme ordinaire."),
            question("DSI", "La fraîcheur de la base",
                     "Quand la copie du 02/02/2026 sera-t-elle rafraîchie ? TSINISTRE est-elle "
                     "abandonnée ?",
                     "Les chiffres de 2026 sont incomplets.",
                     "La date à laquelle on pourra produire 2025 en entier.",
                     "Demander un rafraîchissement."),
        ]),
    ],
)

# =============================================================================================
# 10 Où on en est
# =============================================================================================

F10 = Fiche(
    fichier=D + "10 ou on en est.excalidraw",
    titre="Où on en est, et la suite",
    sous_titre="Fiche 10 du domaine des prestations · " + ETAT_AU,
    objectif="ce qui est fait pour chaque famille de tableaux, ce qui reste, dans quel ordre, "
             "et qui doit agir.",
    parties=[
        Partie("Les six familles de tableaux", colonnes=3, blocs=[
            Bloc("1 · Dossiers (T25 à T27)", [
                ("Fait", "règles D1 à D14, trois requêtes proposées, trois fiches de "
                         "validation."),
                ("Reste", "exécuter les requêtes ; recueillir les réponses ; aligner la "
                          "DTM_DOSSIER."),
            ], genre="suite", etat="en cours"),
            Bloc("2 · Bénéficiaires au 31/12 (T32 à T41)", [
                ("Fait", "règles R5, R6, R8, confirmées par l'annuaire de la CNSS ; une "
                         "première version de T32."),
                ("Reste", "une requête commune, une fiche pilote T32, puis T33 à T41 ; les "
                          "allocataires de T41."),
            ], genre="suite", etat="en cours"),
            Bloc("3 · Nouveaux (T42 à T49)", [
                ("Fait", "règle R7 revue avec l'annuaire."),
                ("Reste", "les fiches ; le taux d'incapacité de T48."),
            ], genre="suite", etat="à faire"),
            Bloc("4 · Évolution sur 5 ans (T50 à T57)", [
                ("Fait", "règles des paiements R3 et R4, confirmées."),
                ("Reste", "T54 et T55 comptent les liquidées (Kaya) ; définir T57."),
            ], genre="suite", etat="à faire"),
            Bloc("5 · Indus (T31)", [
                ("Fait", "règle I1 proposée."),
                ("Reste", "un lot de requêtes sur les ajustements."),
            ], genre="suite", etat="à faire"),
            Bloc("6 · Contrôles (T28 à T30)", [
                ("Fait", "le constat : 0,07 % en 2025, plus rien depuis juillet."),
                ("Reste", "bloqué tant que la DSI n'a pas répondu."),
            ], genre="suite", etat="à faire"),
        ]),
        Partie("Les prochaines étapes, dans l'ordre", colonnes=3, flux=True, blocs=[
            Bloc("1. Exécuter T25, T26, T27", [
                ("Qui", "toi, sur le cliché 092026."),
                ("Pour", "vérifier que les règles donnent des chiffres plausibles."),
            ], genre="suite"),
            Bloc("2. Recueillir les réponses", [
                ("Qui", "la direction des prestations, avec les fiches de validation."),
                ("Pour", "trancher les questions de la fiche 09."),
            ], genre="suite"),
            Bloc("3. Les bénéficiaires au 31/12", [
                ("Qui", "Claude, puis toi pour l'exécution."),
                ("Pour", "la famille 2 : requête commune et fiche pilote T32."),
            ], genre="suite"),
            Bloc("4. Les lots qui restent", [
                ("Qui", "Claude, puis toi."),
                ("Pour", "les codes IJ et RV de DTM_ACCIDENT_TRAVAIL, les indus, les "
                         "allocataires, les tables pas encore mesurées."),
            ], genre="suite"),
            Bloc("5. Corriger les DTM", [
                ("Qui", "Claude, avec l'accord de la personne en charge d'Oracle Analytics."),
                ("Pour", "DTM_DOSSIER, DTM_PRESTATION, DTM_PRESTATION_INDUE, après visa du "
                         "métier."),
            ], genre="suite"),
            Bloc("6. Finir la comparaison", [
                ("Qui", "toi : relancer A04, exécuter A06 à A08."),
                ("Pour", "expliquer les écarts des veuves et des orphelins depuis 2023."),
            ], genre="suite"),
        ]),
        Partie("Qui fait quoi", colonnes=4, blocs=[
            Bloc("Toi", [("", "Exécuter les lots et les requêtes, présenter les fiches au "
                              "métier, décider de ce qui entre dans le suivi.")],
                 genre="notion"),
            Bloc("Claude", [("", "Écrire les requêtes, lire les résultats, proposer les règles, "
                                 "tenir à jour les fiches, les schémas et le suivi.")],
                 genre="notion"),
            Bloc("La direction des prestations", [("", "Valider ou corriger les règles, "
                                                       "répondre aux questions métier.")],
                 genre="notion"),
            Bloc("La DSI et Oracle Analytics", [("", "Confirmer le sens des statuts et des "
                                                     "circuits ; accepter les changements des "
                                                     "tables DTM.")], genre="notion"),
        ]),
    ],
)

# =============================================================================================
# 11 Se préparer aux questions
# =============================================================================================


def chiffre(valeur, sens, source):
    return Bloc(valeur, [("", sens)], genre="constat", pied=source)


def qr(q, r, appui):
    return Bloc(q, [("Ta réponse", r), ("Ton appui", appui)], genre="reponse")


F11 = Fiche(
    fichier=D + "11 se preparer aux questions.excalidraw",
    titre="Se préparer aux questions : les chiffres et les réponses",
    sous_titre="Fiche 11 du domaine des prestations · à relire avant une réunion · " + ETAT_AU,
    objectif="les chiffres à connaître par cœur, et une réponse courte et appuyée aux questions "
             "qu'on peut te poser sur le travail des prestations.",
    parties=[
        Partie("Les chiffres à connaître", colonnes=4, blocs=[
            chiffre("02/02/2026", "La date de la copie de la base sur laquelle on travaille.",
                    "B1 · P01"),
            chiffre("28 codes", "Les types de prestation en espèces, tous au libellé sûr.",
                    "B6 · P34"),
            chiffre("61,35 Md", "Payés en 2025 (débours D et P), toutes branches.", "B5 · P20"),
            chiffre("× 3,8", "Les montants gonflés par les DTM actuels, avec le net à payer.",
                    "H6 · P08"),
            chiffre("4 671", "Demandes de pension reçues au guichet en 2025.", "D3 · P09"),
            chiffre("≈ 4 800", "Demandes de pension liquidées en 2025, dont environ 900 "
                               "réversions.", "D4 · P54"),
            chiffre("62 jours", "La durée médiane de liquidation d'une pension en 2025 ; la "
                                "norme est de 45 jours en moyenne.", "D5 · P10"),
            chiffre("0,07 %", "Les dossiers contrôlés après paiement en 2025 ; la norme est "
                              "d'au moins 5 %.", "B13 · P13"),
            chiffre("Une sur deux", "46 % des déclarations d'accident du travail ne donnent "
                                    "jamais rien.", "B19 · P59"),
            chiffre("7 mois", "L'attente d'une IJ d'accident après la fin de l'arrêt.",
                    "B24 · P69"),
            chiffre("8 447", "Pensions d'orphelin actives sans aucun paiement fin 2025.",
                    "R6 · P25"),
            chiffre("60 171 / 62 231", "Pensionnés de 2024 : notre base / l'annuaire de la "
                                       "CNSS (−3,3 %).", "Annuaire · A01"),
        ]),
        Partie("Les questions qu'on peut te poser", colonnes=3, blocs=[
            qr("Pourquoi vos chiffres ne sont-ils pas ceux des DTM actuels ?",
               "Parce que les requêtes DTM se trompent : elles ne gardent que les dossiers "
               "payés, lisent « fermé » comme « liquidé », gonflent les montants 3,8 fois et "
               "comptent le tuteur au lieu de l'orphelin.",
               "Fiche 08 : H1, H4, H6, H13."),
            qr("Vos chiffres sont-ils fiables ?",
               "Notre base retrouve l'annuaire de la CNSS 2024 à 2 ou 3 % près, et ses montants "
               "à 1 ou 2 %. Les écarts viennent de ce que la base a changé depuis la "
               "publication.",
               "Fiche 05 : A01 ; dossier comparaison_annuaire_cnss_2024."),
            qr("Pourquoi ne pas utiliser le statut du dossier ?",
               "Le statut F veut dire fermé, pas liquidé : il donne 310 liquidations en 2025 "
               "pour 15 479 notifications.",
               "B3, H4 · P02, P10."),
            qr("Comment savez-vous qu'un paiement est réel ?",
               "Seuls les débours D (payé par effet) et P (payé sans effet) sont payés ; les "
               "montants ainsi calculés retombent sur ceux publiés par la CNSS.",
               "B5, R3 · P20 à P23."),
            qr("Pourquoi compter des demandes et pas des dossiers ?",
               "La réversion d'une veuve est ajoutée au dossier du défunt : en comptant des "
               "dossiers, on perdrait environ 900 réversions par an.",
               "D2, B9 · P51, P54."),
            qr("Pourquoi une durée de liquidation si longue ?",
               "C'est ce que disent les données : 62 jours en médiane pour les pensions. Le "
               "dossier est complet au guichet, donc ce n'est pas l'attente des pièces.",
               "D5, B10 · P10, P49."),
            qr("Pourquoi un stock en instance si bas ?",
               "Les 23 218 demandes « en instance » de la règle brute sont surtout de vieilles "
               "réceptions sans lien et des pensions radiées sans date : pas des demandes en "
               "attente.",
               "D6, B11 · P50, P57."),
            qr("Pourquoi compter l'orphelin et pas le tuteur ?",
               "Le droit est celui de l'orphelin ; l'annuaire de la CNSS le compte aussi, par "
               "son âge, de 0 à 24 ans.",
               "R5, B14 · P16."),
            qr("Qui est pensionné au 31 décembre ?",
               "La pension en cours ce jour-là, titulaire en vie, payée ou non : c'est la "
               "définition de l'annuaire de la CNSS.",
               "R6 · P25, annuaire A01."),
            qr("Pourquoi la maternité dans les prestations familiales ?",
               "Le type M de la base est la maternité, rattachée à la branche PF ; aucun soin "
               "n'y est payé, ce n'est pas la maladie.",
               "R1, B2 · P02, P05."),
            qr("Pourquoi les IJ d'accident sont-elles si lentes ?",
               "L'arrêt dure un mois, mais l'IJ est notifiée 7 mois après sa fin. La base ne dit "
               "pas pourquoi : la question est posée au métier.",
               "B24 · P69 · question Q4 de T26."),
            qr("Que faites-vous des déclarations sans suite ?",
               "Elles comptent comme déposées, puis restent en instance au plus 24 mois. C'est "
               "une proposition, à valider.",
               "D12, B19 · question Q1 de T26."),
            qr("Un enfant ajouté compte-t-il ?",
               "C'est la question Q1 de T27. Notre proposition est oui : 16 400 liquidations au "
               "lieu de 8 246 en 2025.",
               "D13, B21 · P64."),
            qr("C'est quoi, une « Proposition BI » ?",
               "Notre choix, cohérent avec les données, quand aucun document ne tranche. On le "
               "dit franchement et on demande l'avis du métier.",
               "Fiche 02 du projet : les cinq sources."),
            qr("Pourquoi ne pas corriger les DTM tout de suite ?",
               "Le modèle ne doit pas être refondu ; chaque changement passe par le visa du "
               "métier et l'accord de la personne en charge d'Oracle Analytics.",
               "Fiche 01 du projet : les règles du jeu."),
            qr("Et les contrôles a posteriori ?",
               "0,07 % en 2025, et plus aucune saisie depuis juillet. Le drapeau DEB_VERIFIE "
               "n'est pas un contrôle. On attend la DSI.",
               "B13, H11 · P13."),
        ]),
        Partie("Pour t'entraîner", colonnes=3, blocs=[
            Bloc("Explique en une minute", [
                ("", "Ce qu'est une demande dans T25, avec l'exemple de la veuve. Puis ce qu'est "
                     "un paiement, et qui est pensionné au 31 décembre."),
            ], genre="reponse"),
            Bloc("Refais le chemin", [
                ("", "Sans regarder, cite les cinq tables du chemin d'une demande, dans l'ordre, "
                     "et le piège de chacune (fiche 03)."),
            ], genre="reponse"),
            Bloc("Défends une règle", [
                ("", "Choisis une règle jaune (proposée) de la fiche 06 ou 07 : dis-la, donne sa "
                     "preuve, puis réponds à « et si on faisait autrement ? »."),
            ], genre="reponse"),
        ]),
    ],
)

FICHES = [F01, F02, F03, F04, F05, F06, F07, F08, F09, F10, F11]
