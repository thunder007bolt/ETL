"""
Données du journal de bord du projet BI de la CNSS : la mémoire du travail, jour par jour.

C'est la source unique du journal : generer.py en tire les vues Excalidraw de schemas/journal
(tableau de bord, frise, décisions, où trouver quoi, une fiche par jour). Pour Claude, c'est aussi
le premier fichier à lire en début de séance : l'état, ce qui attend, la suite.

À tenir à jour à la fin de chaque étape de travail :
    1. ajouter ou compléter le Jour du jour dans JOURS (ses chantiers, ce qui reste le soir) ;
    2. ajouter les nouvelles décisions à DECISIONS (et marquer « remplacée » celles qu'elles
       remplacent) ;
    3. mettre à jour ETAT, EN_ATTENTE, SUITE et, si besoin, LIVRABLES ;
    4. régénérer : python3 excalidraw/sources/journal/generer.py --force

Les jours du 19 au 29/10/2026 sont reconstitués à partir des notes de mémoire de Claude, des
dates des lots de requêtes et de l'historique git (source="reconstitué") ; ils sont moins
détaillés que les suivants.
"""

from dataclasses import dataclass, field


@dataclass
class Chantier:
    """Un morceau de travail d'une journée."""
    titre: str
    domaine: str                 # Projet, Prestations, GRH, Outils…
    demande: str = ""            # ce que l'utilisateur a demandé
    fait: str = ""               # ce qui a été fait
    trouve: str = ""             # ce qu'on a trouvé
    decide: str = ""             # ce qui a été décidé
    fichiers: str = ""           # fichiers et documents produits
    etat: str = "fait"           # fait, en cours, à faire


@dataclass
class Jour:
    date: str                    # AAAA-MM-JJ
    titre: str                   # la journée en quelques mots
    marquants: list              # 2 à 4 faits marquants, pour la frise
    chantiers: list = field(default_factory=list)
    reste: list = field(default_factory=list)    # ce qui restait le soir
    source: str = "séance"       # séance ou reconstitué


@dataclass
class Decision:
    date: str
    titre: str
    decision: str
    pourquoi: str
    qui: str
    effet: str
    statut: str = "en vigueur"   # en vigueur ou remplacée
    remplacee_par: str = ""


# =============================================================================================
# Les jours
# =============================================================================================

AVANT = ("Avant la mission, de mai à juillet 2026 : la chaîne ETL (faits du DWH, clichés "
         "multiples), les premières tables DTM et les indicateurs CIPRES des prestations et de "
         "la GRH, d'après l'historique git.")

JOURS = [
    Jour("2026-10-19", "Arrivée sur le terrain",
         ["Début de la mission à la CNSS, au Burkina.",
          "Règle : pas de refonte du modèle DTM.",
          "Artefact « Roadmap métier » : les questions au métier."],
         chantiers=[
             Chantier("Le cadre du projet", "Projet",
                      fait="Rôle précisé : intégration (Python, Jenkins) et modèle DTM repris de "
                           "l'équipe précédente.",
                      decide="Pas de refonte du modèle DTM : on corrige, de façon compatible, "
                             "après visa du métier.",
                      fichiers="Mémoire : rôle, contrainte du modèle."),
             Chantier("La roadmap métier", "Projet",
                      fait="Un artefact avec les questions au métier (136 à comprendre, 127 à "
                           "confirmer) et les vues V01 à V29.",
                      fichiers="Artefact « Roadmap métier »."),
         ],
         source="reconstitué"),
    Jour("2026-10-20", "Les vues simples de la DSI",
         ["Décision : la DSI fournit des vues simples, l'équipe BI fait les calculs CIPRES.",
          "Doc « Spécification des vues BI CIPRES » pour la DSI, domaines 1 à 4."],
         chantiers=[
             Chantier("Le partage des rôles avec la DSI", "Projet",
                      decide="La DSI livre des vues simples, une par objet ; les règles CIPRES "
                             "restent côté BI. Remplace l'idée de la veille (vues déjà calculées "
                             "par la DSI).",
                      fichiers="Doc « Spécification des vues BI CIPRES »."),
         ],
         source="reconstitué"),
    Jour("2026-10-21", "GRH : le profilage des mouvements",
         ["Lots 1 à 9 de la GRH (Q01 à Q69).",
          "GRH_MOUVEMENT n'a ni recrutements ni départs : piste GRH_DECISION.",
          "Règle : historiser chaque requête dans analyses/<domaine>."],
         chantiers=[
             Chantier("Les mouvements du personnel (T60)", "GRH",
                      fait="Neuf lots de requêtes sur GRH_MOUVEMENT, les décisions, l'effectif, "
                           "la formation et la comptabilité.",
                      trouve="GRH_MOUVEMENT est incomplet ; les décisions (GRH_DECISION) "
                             "portent les entrées et les sorties.",
                      decide="Chaque requête de profilage est gardée dans "
                             "analyses/<domaine>/lotN, avec son objectif et son résultat ; pas de "
                             "FETCH FIRST (base antérieure à 12c).",
                      fichiers="analyses/grh/lot1 à lot9 ; doc « Rapport mouvements du "
                               "personnel »."),
         ],
         source="reconstitué"),
    Jour("2026-10-22", "GRH : les requêtes DTM corrigées",
         ["Effectif, ratio, indicateurs et formation corrigés (commit 1b44a0c).",
          "Le DWH ne garde qu'un cliché (102026).",
          "Validées par le lot 12 : 885 agents en 2025."],
         chantiers=[
             Chantier("Les quatre requêtes DTM de la GRH", "GRH",
                      fait="Réécriture de l'effectif, du ratio, des indicateurs et de la "
                           "formation ; tests (lots 10 à 12).",
                      trouve="Les faits du DWH ne gardent que le dernier cliché ; le budget de "
                             "formation est vide.",
                      decide="Les docs d'exploitation montrent la requête finale et ses règles, "
                             "jamais les corrections.",
                      fichiers="domaines/grh/dtm/sql ; doc d'exploitation des requêtes DTM GRH."),
         ],
         source="reconstitué"),
    Jour("2026-10-23", "GRH : un questionnaire simple pour la CNSS",
         ["Le doc technique jugé trop complexe pour la CNSS.",
          "Questionnaire CIPRES GRH : 33 questions sur T58 à T65."],
         chantiers=[
             Chantier("Le questionnaire GRH", "GRH",
                      decide="Pour le métier, des documents simples, sans SQL ni codes ; le "
                             "technique reste pour la DSI et le BI.",
                      fichiers="Doc « Questionnaire CIPRES GRH »."),
         ],
         source="reconstitué"),
    Jour("2026-10-24", "GRH : les agents temporaires",
         ["Lot 13 : temporaires et agents de la fonction publique.",
          "Questionnaire complété (question 13, glossaire)."],
         source="reconstitué"),
    Jour("2026-10-25", "GRH : l'Annuaire CIPRES devient lisible",
         ["Poppler installé : l'Annuaire se lit avec pdftotext.",
          "Lots 14 et 15 : les types d'agents.",
          "Encadrant = cadre de T58, d'après l'Annuaire."],
         source="reconstitué"),
    Jour("2026-10-26", "Prestations : le lancement",
         ["Lots 1 et 2 des prestations exécutés ; règles R1 à R12.",
          "Atelier de Kaya 2017 déposé.",
          "Décision : le contenu des DTM n'est pas fiable."],
         chantiers=[
             Chantier("L'inventaire et les paiements", "Prestations",
                      fait="Lot 1 (P01 à P19) et lot 2 (P20 à P30), sur la base source.",
                      trouve="Base figée au 02/02/2026 ; paiement = débours D ou P ; montants "
                             "du DTM gonflés 3,8 fois ; M = maternité.",
                      decide="Ne pas analyser le contenu des DTM : on recalcule depuis la source.",
                      fichiers="analyses/prestations/lot1, lot2 ; docs « T60 pour la CNSS » et "
                               "« Prestations sociales (T25 à T57) »."),
             Chantier("Les domaines d'activité", "GRH",
                      fait="Lot 16 GRH ; commit 15715fd des requêtes GRH."),
         ],
         source="reconstitué"),
    Jour("2026-10-27", "Prestations : T25 et le guide du métier",
         ["Première requête T25 proposée.",
          "Doc « Guide du métier » : CNSS, CIPRES, lexique.",
          "Doc « Règles de calcul » : T25 à valider."],
         source="reconstitué"),
    Jour("2026-10-29", "Tout en Excalidraw",
         ["Décision : documents et explications en schémas Excalidraw.",
          "Revue des DTM_PRESTATION et DTM_DOSSIER ; DTM_DOSSIER proposée.",
          "Lot 3 A : toutes les listes de codes."],
         chantiers=[
             Chantier("La visionneuse et les premiers schémas", "Outils",
                      fait="La visionneuse locale, l'historique des versions, les schémas du "
                           "domaine, des tables et de PRESTATION_ESP.",
                      decide="Tout se fait en schémas Excalidraw ; chaque choix cite sa source "
                             "(Annuaire, Kaya, Base, Données, Proposition BI)."),
             Chantier("Les DTM et les codes", "Prestations",
                      fait="Revue du code de DTM_PRESTATION et DTM_DOSSIER ; requête "
                           "DTM_DOSSIER proposée ; lot 3 A (P31 à P47) ; TODO en 12 tâches.",
                      fichiers="dtm_dossier_requete_proposee.sql ; artefact « Tables des "
                               "prestations »."),
         ],
         source="reconstitué"),
    Jour("2026-10-30", "Prestations : T25 à T27, l'annuaire, la maîtrise",
         ["Lot 3 B et lot 4 : on compte des demandes ; T25, T26, T27 prêts.",
          "Comparaison avec l'annuaire de la CNSS 2024 : 2 à 3 % d'écart.",
          "Règles R6 et R7 revues ; fiches de maîtrise par domaine."],
         chantiers=[
             Chantier("La fiche de validation T25", "Prestations",
                      demande="Une fiche par tableau CIPRES, pour que le métier valide nos "
                              "règles.",
                      fait="Fiche T25 (maquette, exemples, dix règles avec leur SQL, cinq "
                           "questions) ; lot 3 B (P48 à P58) pour répondre aux questions.",
                      trouve="La réversion est dans le dossier du défunt ; le stock en instance "
                             "brut est faux ; les refus sont rares.",
                      decide="Compter des demandes (dossier × ligne), règles D1 à D8 ; le "
                             "gabarit T25 sert pour les autres tableaux.",
                      fichiers="lot3b_controles_t25.sql ; t25_requete_proposee.sql ; "
                               "T25_dossiers_de_pension."),
             Chantier("Le schéma de toutes les règles", "Prestations",
                      demande="Une feuille qui liste toutes les règles avec leurs preuves.",
                      fait="regles_prestations ; vie_dossier et le modèle conceptuel mis à "
                           "jour.",
                      decide="Une section « Récap » en tête de chaque réponse."),
             Chantier("T26 et T27", "Prestations",
                      demande="Attaquer les autres tableaux, à partir de T25.",
                      fait="Lot 4 (P59 à P73) ; règles D9 à D14 ; requêtes et fiches T26 et "
                           "T27.",
                      trouve="IJ puis rente des années après ; 46 % de déclarations AT/MP sans "
                             "suite ; IJ notifiées 7 mois après l'arrêt ; enfant ajouté sans "
                             "guichet ; grossesse sur deux lignes.",
                      fichiers="lot4_controles_t26_t27.sql ; t26 et t27_requete_proposee.sql ; "
                               "fiches T26 et T27."),
             Chantier("L'annuaire de la CNSS 2024", "Prestations",
                      demande="Reconstituer le tableau 18 de l'annuaire, pour comparer.",
                      fait="Requêtes A01 à A08, dans un dossier à part ; schéma « annuaire "
                           "cnss/pensions_2024 ».",
                      trouve="Tableau 18 retrouvé à 2 ou 3 % près ; la CNSS compte un stock au "
                             "31/12, payé ou non ; un nouveau pensionné à la saisie.",
                      decide="Ne rien mélanger sans demande ; puis, à ta demande, R1 à R5 "
                             "confirmées, R6 corrigée, R7 revue.",
                      fichiers="analyses/comparaison_annuaire_cnss_2024."),
             Chantier("Les fiches de maîtrise", "Outils",
                      demande="Un dossier pour t'approprier tout le travail, rangé par domaine "
                              "et évolutif.",
                      fait="schemas/maitrise : sommaire, 3 fiches « projet », 11 fiches "
                           "« prestations » ; générateur sources/maitrise.",
                      fichiers="Commits 2ab48b4 et c17b853."),
         ]),
    Jour("2026-10-31", "Visionneuse hors ligne, journal de bord, T32 à T41",
         ["La visionneuse marche sans Internet.",
          "Le journal de bord : la mémoire du travail, jour par jour.",
          "T32 à T41 : lot 5 exécuté, règles à moins de 1 % de l'annuaire de la CNSS pour T32.",
          "Lot 6 rédigé pour les écarts qui restent : T41, dates par défaut, suspensions."],
         chantiers=[
             Chantier("La visionneuse hors ligne", "Outils",
                      demande="Excalidraw ne marche pas sans Internet.",
                      fait="Excalidraw, React et les polices copiés dans viewer/vendor, "
                           "construits par viewer/build ; vérifié : aucune requête vers "
                           "Internet.",
                      fichiers="excalidraw/viewer/vendor, viewer/build."),
             Chantier("Le journal de bord", "Outils",
                      demande="Une mémoire en Excalidraw pour suivre l'avancement, revenir en "
                              "arrière et voir ce qui a été fait.",
                      fait="schemas/journal : tableau de bord, frise, décisions, où trouver "
                           "quoi, une fiche par jour ; données dans sources/journal/journal.py.",
                      decide="Le journal est tenu à jour à chaque étape, et lu en début de "
                             "séance."),
             Chantier("Les bénéficiaires au 31/12 (T32 à T41)", "Prestations",
                      demande="Attaquer la famille 2 : T32 à T41.",
                      fait="Lot 5 (P74 à P78) : stock par tableau de 2020 à 2025, montant, âge "
                           "et sexe, doublons, coordination. Une requête commune aux dix "
                           "tableaux. La fiche pilote T32 (8 règles, 4 questions).",
                      trouve="L'annuaire de la CNSS donne de quoi vérifier T37 à T41 : rentes "
                             "par type, 97 378 allocataires et 268 481 enfants en 2024 ; enfants "
                             "de 21 ans au plus. Kaya 2017 : T41 compte le dossier de "
                             "l'allocataire ; T35 sans « 25 ans et plus ».",
                      fichiers="lot5_beneficiaires_31_12.sql ; t32_t41_requete_proposee.sql ; "
                               "fiche T32_pensionnes_vieillesse.",
                      etat="fait"),
             Chantier("Les résultats du lot 5 (T32 à T41)", "Prestations",
                      demande="Tu as collé les résultats de P74 à P78.",
                      fait="Résultats et constats dans les en-têtes, listes dans resultats/"
                           "lot5_*.tsv, constats « Lot 5 » au README. Requête commune et fiche "
                           "T32 ajustées (chiffres, âge inconnu dès 120 ans, question Q5) ; "
                           "règles R3, R5, R6, R8 revues. Lot 6 (P79 à P81) rédigé.",
                      trouve="T32 31 093 pour 30 808 publiés (+0,9 %), T36 exact, rentes à "
                             "quelques %. Sexe 1 = hommes. La CNSS compte les pensions non "
                             "payées, dont 2 901 de coordination payées par une autre caisse. "
                             "Montant publié entre nos deux montants. T41 +5 %, mais les "
                             "enfants payés retombent à +1,1 %. Veuves et orphelins sous le "
                             "publié depuis 2023. 500 veuves de 120 ans : date par défaut.",
                      decide="Coordination comptée sans montant ; montant des seuls comptés "
                             "(proposition) ; 120 ans et plus en âge inconnu.",
                      fichiers="lot5_beneficiaires_31_12.sql ; lot6_ecarts_t32_t41.sql ; "
                               "resultats/lot5_*.tsv ; t32_t41_requete_proposee.sql ; fiche "
                               "T32 ; regles_prestations.",
                      etat="fait"),
         ],
         reste=["Toi : exécuter le lot 6 (P79 à P81) ; exécuter T25, T26, T27 ; relancer A04, "
                "exécuter A06 à A08.",
                "Commiter la visionneuse hors ligne, le journal et T32 à T41.",
                "Ensuite : fixer la règle de T41, puis T42 à T49."]),
    Jour("2026-11-01", "Le lot 6 : T41 tranché",
         ["T41 : seuls les enfants payés dans l'année comptent (+0,7 % de l'annuaire).",
          "L'année 1900 est une date de naissance par défaut (503 bénéficiaires).",
          "Les suspensions n'expliquent pas l'écart des veuves : reste A06, A07.",
          "La fiche T41, sur le gabarit de T32.",
          "T42 à T49 : lot 7 exécuté, la première saisie retombe sur l'annuaire à 1 %.",
          "La fiche pilote T42.",
          "T50 à T57 : lot 8 exécuté ; liquidées = annuaire, montant à la saisie du paiement.",
          "L'annuaire de la CNSS est arrêté fin mars : règle R13.",
          "La fiche pilote de T50 à T57 ; la date des montants ne bloque plus rien.",
          "T31 (indus) : lot 9 exécuté, taux d'indus 0,39 % en 2024, norme 2 %.",
          "La fiche T31.",
          "GRH : la méthode des prestations ; l'annuaire de la CNSS publie T58 à T65.",
          "GRH, lot 17 : 742 permanents pour 758 ; âge et ancienneté retrouvés ; lot 18.",
          "GRH, Q99 : 104 recrutements en 2024 comme l'annuaire ; les 84 de 2019 retrouvés.",
          "GRH, lot 18 : la catégorie de la CNSS est celle de la situation ; lot 19.",
          "GRH, lot 19 : T58 et T65 retrouvés ; 46 contractuels = engagés de l'année.",
          "GRH : retraites à la date de la décision 02 ; lot 20 pour les autres mouvements.",
          "GRH : disponibilités (07) et remises (18) retrouvées ; règles G1 à G11 écrites.",
          "GRH : le schéma des règles (regles_grh) en Excalidraw.",
          "GRH : trois requêtes proposées (effectif, T60, T62), avec leurs réglages.",
          "GRH : le dossier « tableaux cipres » et les fiches T58, T60, T62.",
          "Réunion du 02/11 : un doc des points à valider par domaine."],
         chantiers=[
             Chantier("Les résultats du lot 6 (T32 à T41)", "Prestations",
                      demande="Tu as collé les résultats de P79 à P81.",
                      fait="Résultats et constats dans les en-têtes, listes dans resultats/"
                           "lot6_*.tsv, constats « Lot 6 » au README. Requête commune : T41 "
                           "ne garde que les enfants payés. Règles R5, R6, R8, fiche T32 et "
                           "schéma des règles mis à jour.",
                      trouve="T41 : 98 048 allocataires et 271 577 enfants en 2024 avec la "
                             "condition de paiement, pour 97 378 et 268 481 publiés ; +5 % "
                             "sans. Les dossiers saisis après coup n'y sont pour rien. 1900 = "
                             "date par défaut (499 veuves) ; les autres grands âges ont des "
                             "dates approchées. 250 veuves suspendues seulement, pour 1 052 de "
                             "moins que l'annuaire.",
                      decide="T41 : un enfant compte s'il a été payé dans l'année ; "
                             "l'allocataire est le dossier qui en a au moins un (proposition). "
                             "R6 reste pour les pensions.",
                      fichiers="lot6_ecarts_t32_t41.sql ; resultats/lot6_*.tsv ; "
                               "t32_t41_requete_proposee.sql ; fiche T32 ; regles_prestations.",
                      etat="fait"),
             Chantier("La fiche T41", "Prestations",
                      demande="Une fiche à part pour T41.",
                      fait="Fiche T41_allocataires : maquette, allocataires de 2020 à 2024 face "
                           "à l'annuaire de la CNSS, six allocataires fictifs, huit règles avec "
                           "leur SQL, cinq questions (enfant payé, qui est l'allocataire, "
                           "montant, prénatales, tableau des enfants).",
                      trouve="L'Annuaire place T41 p. 14 (description) et p. 41 (maquette), le "
                             "glossaire p. 62 ; les pages de T32 étaient décalées d'une page, "
                             "corrigées dans la fiche T32 et la requête. Kaya 2017 demandait un "
                             "tableau des enfants (question Q5).",
                      fichiers="sources/prestations/t41_allocataires.py ; fiche "
                               "T41_allocataires.",
                      etat="fait"),
             Chantier("Les nouveaux bénéficiaires (T42 à T49)", "Prestations",
                      demande="Passer à T42–T49.",
                      fait="Lot 7 (P82 à P87) : entrées de 2020 à 2025, âge et sexe des "
                           "nouveaux retraités, âge de tous les nouveaux, montant avec ou sans "
                           "rappel, services passés (T42), degré d'IPP (T48). Requête commune "
                           "aux huit tableaux.",
                      trouve="L'annuaire de la CNSS publie de quoi tout contrôler en 2024 : "
                             "nouveaux pensionnés par type (tableau 23) et par âge et sexe "
                             "(tableau 25), nouvelles rentes viagères (75), nouveaux ayants "
                             "droit de rentes (77). Kaya 2017 : services passés en mois, part "
                             "CNSS pour la coordination ; T45 à T49 rangés avec les AT/MP.",
                      decide="Nouveau = première prestation du tableau saisie dans l'exercice "
                             "(proposition) ; T45 = toutes les rentes AT/MP, T47 = ARSA ; la "
                             "fiche pilote T42 viendra après le lot.",
                      fichiers="lot7_nouveaux_beneficiaires.sql ; t42_t49_requete_proposee.sql.",
                      etat="fait"),
             Chantier("Les résultats du lot 7 (T42 à T49)", "Prestations",
                      demande="Tu as collé P82 à P86, puis P88 à P91, puis P87 ; les "
                              "valeurs nulles se lisent-elles bien ?",
                      fait="Résultats et constats dans les en-têtes, listes dans resultats/"
                           "lot7_*.tsv, constats « Lot 7 » au README ; R7 et R9 revues ; "
                           "partie B ajoutée et reportée ; part CNSS des services passés dans "
                           "la requête.",
                      trouve="Première saisie : T42 2 463 pour 2 489, veuves +0,7 %, orphelins "
                             "+0,5 % ; 220 reliquidations en 2025 ; T48 39 pour 44. Âge juste "
                             "par tranche, pas le sexe (641 femmes pour 527). Rappel : 8 % "
                             "retraités, deux fois l'année pour les rentes d'orphelins. Mois "
                             "cotisés toujours remplis ; PE_COT_AUTRE = mois dans l'autre "
                             "caisse ; par accident, T48 ne change pas ; taux d'IPP toujours "
                             "rempli, de 15 à 100.",
                      decide="Une révision n'est pas une entrée ; coordination comptée sans "
                             "montant ; montant des mois de l'exercice, rappel à part ; services "
                             "passés = total moins l'autre caisse (propositions).",
                      fichiers="lot7_nouveaux_beneficiaires.sql ; resultats/lot7_*.tsv ; "
                               "t42_t49_requete_proposee.sql ; regles_prestations.",
                      etat="fait"),
             Chantier("La fiche pilote T42", "Prestations",
                      demande="Oui, fiche T42.",
                      fait="Fiche T42_nouveaux_retraites sur le gabarit de T32 : maquette avec "
                           "services passés, nouveaux retraités de 2024 par âge face à "
                           "l'annuaire de la CNSS, six retraités fictifs, huit règles avec leur "
                           "SQL, cinq questions (saisie ou notification, révisions, sexe et âge "
                           "du tableau 25, rappel, part CNSS en coordination).",
                      fichiers="sources/prestations/t42_nouveaux_retraites.py ; fiche "
                               "T42_nouveaux_retraites.",
                      etat="fait"),
             Chantier("L'évolution sur 5 ans (T50 à T57)", "Prestations",
                      demande="Oui, T50–T57.",
                      fait="Lot 8 (P92 à P97) : liquidés, payés ou saisis par année, détail par "
                           "code, services passés des allocations de vieillesse, montants par "
                           "branche, colonnes de décès, décédés par branche. Requête commune en "
                           "trois parties : T50 à T55, T56, T57.",
                      trouve="L'annuaire de la CNSS publie de quoi contrôler presque tout : "
                             "allocations de vieillesse (833 liquidées en 2024), IJ de maternité "
                             "sur 5 ans, prestations servies par branche, décès de pensionnés "
                             "(648) et de rentiers (24). Kaya 2017 : T54 et T55 comptent les "
                             "liquidées ; T57 = assurés décédés déclarés par branche.",
                      decide="Bénéficiaire de l'année = prestation liquidée ; montant = payé "
                             "pour les mois de l'année ; T57 à la date du décès (propositions).",
                      fichiers="lot8_evolution_5_ans.sql ; t50_t57_requete_proposee.sql.",
                      etat="fait"),
             Chantier("Les résultats du lot 8 (T50 à T57)", "Prestations",
                      demande="Tu as collé P92 à P97, puis P98 à P100, puis P101.",
                      fait="Résultats et constats dans les en-têtes, listes dans resultats/"
                           "lot8_*.tsv, constats « Lot 8 » au README ; requête T50 à T57 "
                           "ajustée ; partie B ajoutée et reportée ; règle R13 proposée.",
                      trouve="Liquidées : allocations de vieillesse 831 pour 833, IJ de "
                             "maternité 3 890 pour 3 893. Montant : la saisie du paiement "
                             "retombe à 0,02 % (mois payé +3,9 %). T56 : AVS +1,3 %, PF −3,8 %, "
                             "AT/MP −29 % (nature introuvable). T57 : 747 décès de pensionnés "
                             "pour 648 ; ceux connus fin mars 2025 retombent sur l'annuaire "
                             "mois par mois (444 pour 447). La vue des décédés de la DSI est "
                             "plus large que T57 et expose nom, adresse, téléphone.",
                      decide="Bénéficiaire = liquidé ; T56 sans les ajustements ; calcul sur "
                             "le cliché de fin mars N+1 (R13) ; R4 : à décider (propositions).",
                      fichiers="lot8_evolution_5_ans.sql ; resultats/lot8_*.tsv ; "
                               "t50_t57_requete_proposee.sql ; regles_prestations.",
                      etat="fait"),
             Chantier("La fiche pilote de T50 à T57", "Prestations",
                      demande="Oui, fiche T50–T57 (après avoir noté la question R4 pour le "
                              "point du 02/11, sans bloquer).",
                      fait="Réglage date_montant dans les trois requêtes. Fiche "
                           "T50_T57_evolution_5_ans : trois maquettes (T50, T56, T57), IJ de "
                           "maternité face à l'annuaire, six cas fictifs, huit règles avec "
                           "leur SQL, cinq questions (date des montants, arrêt de l'annuaire, "
                           "T56, T57, personnes ou prestations).",
                      fichiers="sources/prestations/t50_t57_evolution.py ; fiche "
                               "T50_T57_evolution_5_ans.",
                      etat="fait"),
             Chantier("Les indus (T31)", "Prestations",
                      demande="Oui, T31.",
                      fait="Lot 9 (P102 à P106) : ajustements par type, branche et année, sens "
                           "des statuts, débours de type AJ, T31 de 2024 rempli avec son taux, "
                           "trop-perçus après décès. Requête T31 (mois × branche : payées, "
                           "trop-perçus, moins-perçus, taux).",
                      trouve="Kaya 2017 : les indus sont les trop-perçus, manques à gagner ; les "
                             "moins-perçus n'en sont pas ; « payées » = décaissées ; norme 2 %. "
                             "L'annuaire de la CNSS ne publie pas d'indus.",
                      decide="Trop-perçus = TP et TP-CONV au mois de leur établissement ; "
                             "REM-TP, avances, mois gratuits hors du tableau ; maternité "
                             "comprise (propositions).",
                      fichiers="lot9_indus.sql ; t31_requete_proposee.sql.",
                      etat="fait"),
             Chantier("Les résultats du lot 9 (T31)", "Prestations",
                      demande="Tu as collé P102 à P106.",
                      fait="Résultats et constats dans les en-têtes, listes dans resultats/"
                           "lot9_*.tsv, constats « Lot 9 » au README ; requête T31 (statut R "
                           "écarté) et règle I1 revues.",
                      trouve="Taux d'indus 0,39 % en 2024 (PVID 0,47 %). Statuts : C soldé, A "
                             "en cours, R annulé. Les débours d'ajustement versent avances, "
                             "rappels, rachats de rente. 7 trop-perçus seulement après décès.",
                      decide="T31 compte les trop-perçus établis dans le mois, sauf annulés ; "
                             "les rachats de rente sont à mettre dans T56 (question).",
                      fichiers="lot9_indus.sql ; resultats/lot9_*.tsv ; "
                               "t31_requete_proposee.sql ; regles_prestations.",
                      etat="fait"),
             Chantier("La fiche T31", "Prestations",
                      demande="Oui, fiche T31.",
                      fait="Fiche T31_prestations_indues : maquette à 12 colonnes, taux d'indus "
                           "de 2024 face à la norme, six cas fictifs, huit règles avec leur SQL, "
                           "cinq questions (moins-perçus, mois d'un indu, pensions après décès, "
                           "date des payées, statuts).",
                      fichiers="sources/prestations/t31_indus.py ; fiche "
                               "T31_prestations_indues.",
                      etat="fait"),
             Chantier("Le domaine GRH, comme les prestations", "GRH",
                      demande="Faire aussi pour la GRH tout le travail fait pour les "
                              "prestations.",
                      fait="Plan (analyses/grh/TODO.md) : contrôle par l'annuaire, règles, "
                           "requêtes, fiches de validation par famille, schéma des règles, "
                           "fiches de maîtrise. Lot 17 (Q97 à Q102) : les tableaux 119 à 124 "
                           "de l'annuaire reconstitués depuis la base.",
                      trouve="L'annuaire de la CNSS 2024 publie le personnel permanent au "
                             "31/12/2024 sous la forme de T58, T60, T61, T62 et T65 : 758 "
                             "permanents (+46 contractuels), 104 recrutements, 480 formés, 49 % "
                             "de cadres.",
                      decide="Reconstituer d'abord ces tableaux (lot 17), puis écrire les "
                             "règles, les requêtes et les fiches.",
                      fichiers="analyses/grh/TODO.md ; "
                               "lot17_comparaison_annuaire_cnss_2024.sql.",
                      etat="fait"),
             Chantier("Les résultats du lot 17 GRH", "GRH",
                      demande="Tu as collé les résultats de Q97 à Q102 (Q99 en erreur).",
                      fait="Résultats et constats dans les en-têtes ; Q99 (T60) réécrite "
                           "après l'erreur ORA-00904 ; lot 18 (Q103 à Q108) pour expliquer "
                           "les écarts ; README et TODO de la GRH.",
                      trouve="742 permanents pour 758 publiés. Âge et ancienneté en années "
                             "pleines : retrouvés à quelques unités près. La catégorie ne "
                             "retombe pas (209 cadres moyens pour 311, 113 cadres supérieurs "
                             "pour 62). De 2020 à 2023, il manque 92 à 99 permanents par an : "
                             "sans doute les 84 engagés de 2019 radiés sans date. 7 "
                             "temporaires pour 46 contractuels ; 410 formés pour 480.",
                      decide="Garder le périmètre (agent CNSS permanent) et les règles d'âge "
                             "et d'ancienneté ; chercher le champ de catégorie de la CNSS et "
                             "tester la date d'état des fiches (DATE_ETAT_EMPLOYE) pour dater "
                             "les départs.",
                      fichiers="analyses/grh/lot17_comparaison_annuaire_cnss_2024.sql ; "
                               "lot18_ecarts_annuaire_cnss_2024.sql.",
                      etat="fait"),
             Chantier("Q99 relancée : les mouvements (T60)", "GRH",
                      demande="Tu as collé le résultat de Q99.",
                      fait="Résultat et constat dans l'en-tête de Q99 ; Q109 ajoutée au lot "
                           "18 (sorties par code, disponibilités et remises en activité).",
                      trouve="104 recrutements en 2024, comme l'annuaire ; retraites à 4 "
                             "près, démissions exactes. En 2019, 84 recrutés jamais présents "
                             "en fin d'année : ce sont les 84 « autres » de l'annuaire (entrés "
                             "en 2019, sortis en 2024). Sans eux, 8 à 16 agents d'écart par "
                             "an. Aucun décès daté depuis 2010 : lire la décision 34.",
                      decide="Garder pour T60 : recrutement = embauche dans l'année ; "
                             "retraite = 02 ; démission = 36 ; licenciement = 54 ; décès à "
                             "lire dans la 34 (à confirmer par Q109).",
                      fichiers="analyses/grh/lot17_comparaison_annuaire_cnss_2024.sql (Q99) ; "
                               "lot18_ecarts_annuaire_cnss_2024.sql (Q109).",
                      etat="en cours"),
             Chantier("Les résultats du lot 18 GRH", "GRH",
                      demande="Tu as collé les résultats de Q103 à Q108.",
                      fait="Résultats et constats dans les en-têtes ; lot 19 (Q110 à Q115) "
                           "pour refaire T58, T62 et T65 avec la catégorie de la CNSS.",
                      trouve="La CNSS classe par la catégorie de la situation (grille de "
                             "salaire) : 01-02 employés, 03 maîtrise, 04-05 cadres moyens, 06 "
                             "cadres supérieurs ; 173, 207, 304, 58 pour 177, 208, 311, 62 ; "
                             "48,8 % de cadres pour 49 %. Les 84 de 2019 forment un seul lot, "
                             "sans date de départ. La date d'état ne date pas les départs.",
                      decide="Catégorie CIPRES = GRH_SITUATION.CATEGORIE ; la catégorie 05 "
                             "en cadres moyens comme l'annuaire 2024 (Kaya 2017 : cadres "
                             "supérieurs), question à la DRH réglée par un paramètre.",
                      fichiers="analyses/grh/lot18_ecarts_annuaire_cnss_2024.sql ; "
                               "lot19_categorie_cnss.sql.",
                      etat="fait"),
             Chantier("Les résultats du lot 19 GRH", "GRH",
                      demande="Tu as collé les résultats de Q110 (en erreur) à Q115.",
                      fait="Résultats et constats dans les en-têtes ; Q110 réécrite sans la "
                           "vue des catégories ; Q116 ajoutée (retraites mois par mois).",
                      trouve="T58 : chaque case catégorie et sexe à 4 agents près. T65 : à "
                             "0,3 point près de 2020 à 2024, avec les 84 de 2019. Les 46 "
                             "contractuels = 44 engagés (50) en 2024 + 2 fin 2023. T62 ne "
                             "retombe pas (409 agents, 524 inscriptions pour 480). 9 "
                             "retraites en décembre 2024 : peut-être comptées présentes.",
                      decide="Contractuels = temporaires engagés (50) dans les 12 derniers "
                             "mois, sans sortie ; T62 en agents distincts, question à la "
                             "DRH ; écart de 1 % sur l'effectif accepté.",
                      fichiers="analyses/grh/lot19_categorie_cnss.sql.",
                      etat="fait"),
             Chantier("Q109, Q110, Q116 relancées : les mouvements (T60)", "GRH",
                      demande="Tu as collé Q109, Q110 et Q116.",
                      fait="Résultats et constats dans les lots 18 et 19 ; lot 20 (Q117) "
                           "pour les décisions de position et les statuts de fin de carrière.",
                      trouve="Les catégories 01 à 06 n'ont pas de libellé d'employé ou de "
                             "cadre : le regroupement vient de l'annuaire. Une retraite de "
                             "décembre n'est pas comptée l'année suivante. Disponibilités et "
                             "remises en activité absentes des statuts de situation ; décès "
                             "par la liquidation (34), décalés d'environ 8 mois.",
                      decide="Retraite à la date de la décision 02 ; écart de 2 % sur "
                             "l'effectif 2024 accepté ; chercher décès, disponibilités, "
                             "remises et la sortie des 84 dans les décisions 07, 18, 37 et "
                             "les statuts X, Y.",
                      fichiers="analyses/grh/lot18 (Q109) ; lot19 (Q110, Q116) ; "
                               "lot20_mouvements_t60.sql.",
                      etat="fait"),
             Chantier("Q117 et les règles GRH", "GRH",
                      demande="Tu as collé le résultat de Q117.",
                      fait="Résultat et constat dans le lot 20 ; règles G1 à G11 au README "
                           "de la GRH, avec leurs preuves ; 7 questions à la DRH (QG1 à QG7).",
                      trouve="Disponibilités = décision 07 (18 pour 21 sur 2020-2024) ; "
                             "remises en activité = décision 18 (22 pour 19). Statuts R, X, Y "
                             "presque jamais saisis ; 10 des 84 de 2019 radiés dès 2019.",
                      decide="Règles G1 à G11 proposées : périmètre, présence, catégorie, "
                             "âge, ancienneté, encadrement, contractuels, entrées, sorties, "
                             "formés, années passées. Chaque question a un réglage.",
                      fichiers="analyses/grh/lot20_mouvements_t60.sql ; analyses/grh/README.md.",
                      etat="fait"),
             Chantier("Le schéma des règles GRH", "GRH",
                      demande="Tu as tout ? Intégrer les choses dans Excalidraw.",
                      fait="Schéma regles_grh : l'essentiel, les lots 1 à 20, l'annuaire de la "
                           "CNSS 2024 face à la base ligne par ligne, les règles G1 à G11, les "
                           "questions QG1 à QG7 et la suite, l'index Q97 à Q117.",
                      trouve="Tous les résultats des lots 17 à 20 sont reportés : rien ne "
                             "manque pour écrire les requêtes.",
                      decide="Le schéma se régénère à chaque nouvelle règle, comme celui des "
                             "prestations.",
                      fichiers="excalidraw/sources/grh/regles_grh.py ; schemas/grh/"
                               "regles_grh.excalidraw.",
                      etat="fait"),
             Chantier("Les requêtes GRH proposées", "GRH",
                      demande="Oui, les requêtes.",
                      fait="Trois requêtes dans analyses/grh, sur les faits du DWH : "
                           "t58_t65_effectif (T58 permanents et contractuels, T59, T61, T65 "
                           "sur cinq ans), t60 (mouvement sur cinq ans, par différence d'une "
                           "fin d'année à l'autre), t62 (formés par catégorie, avec ou sans "
                           "promotion). Contrôle attendu sur 2024 dans chaque en-tête.",
                      trouve="Les questions de la DRH tiennent dans trois réglages : "
                             "categorie_05, duree_contrat, disponibilite.",
                      decide="Requêtes proposées d'abord, comme pour les prestations ; les "
                             "requêtes DTM ne changent qu'après le visa de la DRH, sans "
                             "changer leurs colonnes.",
                      fichiers="analyses/grh/t58_t65_effectif_requete_proposee.sql ; "
                               "t60_requete_proposee.sql ; t62_requete_proposee.sql.",
                      etat="fait"),
             Chantier("Les fiches de validation de la GRH", "GRH",
                      demande="Dans Excalidraw, pas de dossier « tableaux » pour la GRH comme "
                              "pour les prestations.",
                      fait="Dossier grh/tableaux cipres : fiche pilote T58 (T58, T59, T61, "
                           "T65 ; neuf règles), T60 (huit règles), T62 (huit règles), sur le "
                           "gabarit des prestations : maquette, chiffres de l'annuaire de la "
                           "CNSS face à la base, six agents fictifs, règles avec leur SQL, "
                           "cinq questions à la DRH.",
                      trouve="Le gabarit des prestations sert tel quel ; seule la légende des "
                             "sources change (base GRH, copie du 18/02/2026).",
                      decide="T63 et T64 sans fiche tant que la paie et la comptabilité "
                             "manquent.",
                      fichiers="excalidraw/sources/grh (fiche_grh, t58_effectif, t60_mouvement, "
                               "t62_formation) ; schemas/grh/tableaux cipres.",
                      etat="fait"),
             Chantier("Les points à valider, pour la réunion du 02/11", "Projet",
                      demande="Un doc concis par domaine, qui regroupe les règles et les "
                              "questions à valider, écrit comme par une personne ; puis en "
                              "Word, comme fait par toi, et en Excalidraw.",
                      fait="analyses/prestations/points_a_valider.md (40 points) et "
                           "analyses/grh/points_a_valider.md (27 points) : d'abord les 5 "
                           "points à trancher, puis les règles appliquées, les questions par "
                           "tableau et celles pour la DSI. Versions Word (points_a_valider.docx, "
                           "à côté des .md) et Excalidraw (schemas/prestations et schemas/grh), "
                           "faites à partir des .md. Complétés ensuite pour être exhaustifs : 52 "
                           "points (prestations) et 43 (GRH, avec le questionnaire CIPRES et le "
                           "doc T60), et la liste de tous les tableaux, T25 à T65, avec leurs "
                           "codes et leurs points.",
                      trouve="Les questions étaient réparties entre les README, les fiches, "
                             "le journal et deux anciens docs GRH ; T28 à T30, T63 et T64 "
                             "n'avaient pas encore de questions à eux.",
                      decide="Rien n'est changé dans les règles, README et TODO : les "
                             "réponses de la réunion y seront reportées ensuite.",
                      fichiers="analyses/<domaine>/points_a_valider.md et .docx ; "
                               "schemas/<domaine>/points_a_valider ; "
                               "sources/points_a_valider.py.",
                      etat="fait"),
         ],
         reste=["Toi : poser au point du 02/11 la question de la date des montants (R4) ; "
                "A06 et A07 ; les requêtes proposées.",
                "Commiter le travail du 31/10 et du 01/11.",
                "Ensuite : T28 à T30 dès la réponse de la DSI ; les fiches de maîtrise à "
                "mettre à jour ; la décision R4 n'arrête rien (réglage date_montant)."]),
]

# =============================================================================================
# Les décisions
# =============================================================================================

DECISIONS = [
    Decision("2026-10-19", "Pas de refonte du modèle DTM",
             "On garde les tables DTM ; on corrige leurs requêtes de façon compatible, mises en "
             "production après visa du métier.",
             "Oracle Analytics et ses tableaux de bord reposent sur ces tables.",
             "Le cadre du projet",
             "Toute correction passe par une requête proposée, puis un visa."),
    Decision("2026-10-19", "La DSI calcule les vues",
             "La DSI porte toutes les règles et livre des vues déjà calculées.",
             "Première idée du partage des rôles.",
             "Toi et la DSI",
             "Aucun : remplacée dès le lendemain.", statut="remplacée",
             remplacee_par="les vues simples, le 20/10"),
    Decision("2026-10-20", "Des vues simples, les calculs côté BI",
             "La DSI livre des vues simples, une par objet ; l'équipe BI applique les règles "
             "CIPRES.",
             "Les règles doivent être expliquées, validées et corrigées par nous.",
             "Toi",
             "Spécification des vues BI CIPRES pour les domaines 1 à 4."),
    Decision("2026-10-21", "Historiser chaque requête",
             "Chaque requête de profilage va dans analyses/<domaine>/lotN, avec son objectif, "
             "son résultat et son constat.",
             "Garder la trace de ce qui a été vérifié, et pourquoi.",
             "Toi",
             "Les lots numérotés, P pour les prestations, Q pour la GRH."),
    Decision("2026-10-22", "Les docs d'exploitation sans les corrections",
             "Un document d'exploitation montre la requête finale et ses règles, jamais "
             "l'avant et l'après.",
             "Le lecteur n'a pas besoin de l'historique des corrections.",
             "Toi",
             "Les en-têtes des requêtes ne décrivent que leurs règles."),
    Decision("2026-10-23", "Des documents simples pour le métier",
             "Pour la CNSS : des questions claires par tableau, sans SQL ni codes ; le "
             "technique reste pour la DSI et le BI.",
             "Le doc technique de la GRH était trop complexe à transmettre.",
             "Toi",
             "Le questionnaire GRH, puis les docs des prestations."),
    Decision("2026-10-26", "Le contenu des DTM ne fait pas foi",
             "On n'analyse pas le contenu des tables DTM ; on recalcule depuis la base source, et "
             "on relit le code des DTM seulement pour le corriger.",
             "Leurs requêtes contiennent des erreurs connues.",
             "Toi",
             "Le profilage se fait sur USER_DWH (base source)."),
    Decision("2026-10-29", "Tout en Excalidraw",
             "Les documents et les explications se font en schémas Excalidraw.",
             "« C'est plus clair. »",
             "Toi",
             "La visionneuse, le builder Python, les schémas de chaque domaine."),
    Decision("2026-10-29", "Chaque choix cite sa source",
             "Chaque règle dit d'où elle vient : Annuaire (page), Kaya 2017, Base, Données "
             "(requête), sinon « Proposition BI ».",
             "Tu dois pouvoir expliquer et défendre chaque choix.",
             "Toi",
             "Les étiquettes de source des fiches de validation."),
    Decision("2026-10-30", "Compter des demandes, pas des dossiers",
             "Pour T25 à T27, une demande = un dossier × une ligne du tableau.",
             "La réversion est ajoutée au dossier du défunt : environ 900 par an perdues sinon.",
             "Claude, d'après les données (lot 3 B)",
             "Règles D1 à D14, requêtes T25 à T27."),
    Decision("2026-10-30", "Un récap en tête de chaque réponse",
             "Chaque réponse commence par trois à cinq puces : fait, trouvé, reste à faire et par "
             "qui.",
             "Tu lis peu le chat.",
             "Toi",
             "Toutes les réponses."),
    Decision("2026-10-30", "Le gabarit de fiche T25",
             "La fiche de validation T25 sert de modèle aux autres tableaux.",
             "« Une base assez correcte pour attaquer les autres tableaux. »",
             "Toi",
             "Fiches T26 et T27 sur le même gabarit."),
    Decision("2026-10-30", "Ne rien mélanger",
             "Une demande ponctuelle va dans un dossier à part, sans toucher aux règles, aux "
             "schémas, au README ni au TODO sans demande.",
             "Garder le suivi principal propre ; c'est toi qui décides ce qui y entre.",
             "Toi",
             "La comparaison avec l'annuaire rangée à part."),
    Decision("2026-10-30", "Le pensionné et le nouveau pensionné, comme l'annuaire",
             "R6 : pension en cours au 31/12, titulaire en vie, payée ou non ; orphelins de "
             "moins de 25 ans. R7 : pension saisie dans l'année.",
             "C'est ainsi que compte l'annuaire de la CNSS 2024 ; nos chiffres y retombent.",
             "Toi, après la comparaison",
             "Règles R6 et R7 du README et du schéma des règles ; à confirmer avec la direction "
             "des prestations."),
    Decision("2026-10-30", "Des fiches de maîtrise par domaine",
             "Un dossier schemas/maitrise, un sous-dossier par domaine CIPRES, le même plan en "
             "11 fiches, des blocs séparés.",
             "Le schéma des règles était trop condensé pour t'approprier le travail.",
             "Toi",
             "Le sommaire, 0 projet, 3 prestations ; les autres domaines à venir."),
    Decision("2026-10-31", "Une visionneuse sans Internet",
             "Excalidraw, React et les polices sont servis par le projet (viewer/vendor).",
             "Sur le terrain, la connexion n'est pas toujours là.",
             "Toi",
             "Reconstruire vendor après toute modification de viewer/app.js."),
    Decision("2026-10-31", "Un journal de bord tenu à chaque étape",
             "schemas/journal garde l'état, les jours, les décisions et les livrables ; Claude "
             "le lit en début de séance et le met à jour à la fin de chaque étape.",
             "Suivre l'avancement, revenir en arrière, ne rien refaire deux fois.",
             "Toi",
             "Le fichier sources/journal/journal.py, source unique du journal."),
    Decision("2026-10-31", "La coordination comptée sans montant",
             "Les pensions de coordination payées par une autre caisse (COOR_AC) comptent "
             "dans l'effectif de T32 à T36, sans montant.",
             "L'annuaire de la CNSS les compte (sans elles, T32 2024 serait à −8 %) ; Kaya "
             "2017 : seule la part CNSS des montants compte.",
             "Proposition BI, à confirmer par la direction des prestations",
             "La pension moyenne baisse de près de 9 % : question Q5 de la fiche T32.",
             statut="proposé"),
    Decision("2026-10-31", "120 ans et plus : âge inconnu",
             "Un âge de 120 ans ou plus, négatif ou absent va dans la ligne « âge inconnu ».",
             "500 veuves de 120 à 124 ans en 2024 : une date de naissance par défaut.",
             "Proposition BI",
             "Requête commune T32 à T41 et règle R8 ; dates vérifiées par P80.",
             statut="proposé"),
    Decision("2026-11-01", "Calculer l'exercice sur le cliché de fin mars",
             "L'exercice N se calcule sur le cliché de fin mars N+1 (règle R13).",
             "C'est la date d'arrêt de l'annuaire de la CNSS : les décès de 2024 connus fin mars "
             "2025 retombent mois par mois sur le tableau 26 (444 pour 447, lot 8 P99).",
             "Proposition BI, à confirmer par la CNSS",
             "Les écarts dus aux déclarations tardives (décès, radiations de veuves et "
             "d'orphelins) disparaissent ; le job annuel se lance en avril.",
             statut="proposé"),
    Decision("2026-11-01", "La date des montants : en attente, sans bloquer",
             "La question (mois payé ou saisie du paiement) est posée au point du 02/11 ; en "
             "attendant, un réglage date_montant dans les requêtes T32 à T57, réglé sur le "
             "mois payé.",
             "La saisie retombe mieux sur l'annuaire sur 11 lignes sur 16 (lot 8 P98), mais "
             "le choix revient à la CNSS.",
             "Toi",
             "Le travail continue ; dès la réponse, un réglage à changer et la règle "
             "« Montant » des fiches T32, T41, T42 à revoir.",
             statut="ouvert"),
    Decision("2026-11-01", "T41 : seuls les enfants payés",
             "Un enfant d'allocataire compte au 31/12 s'il a droit (en cours, 21 ans au plus) "
             "et a été payé dans l'année ; l'allocataire est le dossier qui a au moins un "
             "enfant compté.",
             "C'est la seule définition qui retombe sur l'annuaire de la CNSS : +0,7 % "
             "d'allocataires en 2024, contre +5 % sans le paiement (lot 6, P79).",
             "Proposition BI, à confirmer par la direction des prestations",
             "T41 s'écarte de R6 (payée ou non), qui reste vraie pour les pensions ; requête "
             "commune modifiée.",
             statut="proposé"),
    Decision("2026-11-01", "GRH : la catégorie est celle de la grille de salaire",
             "La catégorie CIPRES d'un agent (T58, T62, T65) se lit dans la catégorie de sa "
             "situation (GRH_SITUATION.CATEGORIE) : 01 et 02 employés, 03 agents de "
             "maîtrise, 04 et 05 cadres moyens, 06 cadres supérieurs. Plus le niveau de la "
             "qualification.",
             "C'est le seul champ qui retombe sur l'annuaire de la CNSS 2024 : 173, 207, 304 "
             "et 58 pour 177, 208, 311 et 62 ; 48,8 % de cadres pour 49 % (lot 18, Q103).",
             "Proposition BI, à confirmer par la DRH (Kaya 2017 rangeait 05 en cadres "
             "supérieurs)",
             "Les requêtes grh_effectif, grh_ratio et grh_formation changent de champ ; la "
             "catégorie 05 devient un paramètre.",
             statut="proposé"),
]

# =============================================================================================
# L'état aujourd'hui
# =============================================================================================

MISE_A_JOUR = "2026-11-01"

# (domaine, état, ce qui est fait, ce qui vient)
ETAT = [
    ("Prestations (T25 à T57)", "en cours",
     "Famille 1 (dossiers, T25 à T27) : règles, requêtes et fiches prêtes. Famille 2 "
     "(bénéficiaires au 31/12, T32 à T41) : lots 5 et 6 exécutés, règles à 1 % près de "
     "l'annuaire de la CNSS (T41 tranché) ; requête commune et fiche T32 ajustées.",
     "Famille 3 (T42 à T49) : lot 7 exécuté, fiche pilote T42. Famille 4 (T50 à T57) : "
     "lot 8 exécuté, fiche pilote faite. T31 : lot 9 exécuté (indus 0,39 %), fiche faite. "
     "Restent T28 à T30 (DSI). R4 posée au point du 02/11."),
    ("GRH (T58 à T65)", "en cours",
     "Quatre requêtes DTM corrigées et testées (885 agents en 2025) ; questionnaire CIPRES "
     "pour la DRH. Plan sur le modèle des prestations ; lot 17 exécuté : 742 permanents "
     "pour 758 publiés ; âge, ancienneté, recrutements et retraites retrouvés ; lot 18 : "
     "la catégorie de la CNSS trouvée ; lots 19 et 20 : T58, T65 et T60 retrouvés, "
     "contractuels expliqués.",
     "Règles G1 à G11, schéma regles_grh, fiches T58, T60, T62 ; trois requêtes proposées "
     "à exécuter ; visa de la DRH."),
    ("Immatriculation et recouvrement (T1 à T24)", "à faire",
     "Les vues simples sont spécifiées pour la DSI (20/10).",
     "Pas encore d'analyse."),
    ("Gestion financière (T66 à T76)", "à faire",
     "Rien encore.",
     "Pas encore d'analyse."),
    ("Outils", "fait",
     "Visionneuse hors ligne, fiches de maîtrise, journal de bord, schémas de travail.",
     "Tenir le journal et les fiches à jour."),
]

# (qui, ce qu'on attend)
EN_ATTENTE = [
    ("Toi", "Au point du 02/11 : la date des montants (R4). Exécuter les trois requêtes "
            "GRH proposées (102026, 2024) ; faire viser G1 à G11 et poser QG1 à QG7 ; A06 et A07 ; les requêtes proposées sur le cliché 092026 ; relancer "
            "A04 ; commiter le travail du 31/10 et du 01/11."),
    ("La direction des prestations", "Répondre aux questions des fiches T25 à T27 (13), "
                                     "T31, T32, T41, T42 et T50-T57 (5 chacune) ; confirmer "
                                     "R6, "
                                     "R7, la limite de 25 ans des orphelins, le montant, la "
                                     "coordination et la règle de T41 (enfants payés)."),
    ("La DRH", "Remplir les fiches T58, T60, T62 ; viser les règles G1 à G11 ; répondre à "
               "QG1 à QG7 et au questionnaire CIPRES."),
    ("La DSI", "Le sens des statuts, le circuit des paiements sans effet, où sont saisis les "
               "contrôles a posteriori."),
    ("Oracle Analytics", "Accepter le changement de sens des colonnes de DTM_DOSSIER."),
]

# la suite, dans l'ordre
SUITE = [
    ("Les requêtes GRH", "Reporter l'exécution des trois requêtes proposées ; ajuster les "
                         "fiches T58, T60, T62 si les chiffres bougent ; puis les fiches de "
                         "maîtrise « 4 gestion administrative »."),
    ("Les fiches de maîtrise", "Mettre à jour le dossier 3 prestations avec les lots 5 à 9 et "
                               "les règles R13, T41, T50-T57, T31."),
    ("Les contrôles (T28 à T30)", "Bloqués : la table des contrôles n'est plus alimentée "
                                  "depuis juillet 2025 ; réponse de la DSI attendue."),
    ("La décision R4", "Dès ta réponse : régler date_montant, revoir la règle « Montant » des "
                       "fiches T32, T41, T42."),
    ("Finir T32 à T41", "Reporter A06 et A07 pour les veuves et les orphelins ; faire "
                        "valider les fiches T32 et T41."),
    ("Exécuter T25 à T27", "Comparer aux chiffres du lot 4 et ajuster les requêtes."),
    ("Les fiches de maîtrise de la GRH", "Le dossier 4 gestion administrative, sur le même plan."),
    ("Les nouveaux bénéficiaires", "T42 à T49, sur la même requête que T32 à T41."),
    ("Les lots qui restent", "Codes IJ et RV de DTM_ACCIDENT_TRAVAIL, indus."),
    ("Corriger les DTM", "Après visa du métier et accord d'Oracle Analytics."),
]

# =============================================================================================
# Où trouver quoi
# =============================================================================================

# (rubrique, [(nom, où, ce qu'on y trouve)])
LIVRABLES = [
    ("Les analyses (requêtes et constats)", [
        ("Prestations", "analyses/prestations", "lots 1 à 9 (P01 à P106), README (constats, "
                                                "règles, questions), TODO, resultats/"),
        ("GRH", "analyses/grh", "lots 1 à 20 (Q01 à Q117), règles G1 à G11, requêtes "
                                "proposées (effectif, T60, T62), README, TODO"),
        ("Annuaire de la CNSS 2024", "analyses/comparaison_annuaire_cnss_2024",
         "requêtes A01 à A08, tableau 18 reconstitué, déductions"),
    ]),
    ("Les requêtes proposées ou corrigées", [
        ("T25, T26, T27", "analyses/prestations/t25…t27_requete_proposee.sql",
         "les dossiers de prestations, par demande ; pas encore exécutées"),
        ("T32 à T41", "analyses/prestations/t32_t41_requete_proposee.sql",
         "les bénéficiaires au 31/12 par âge et sexe ; pas encore exécutée"),
        ("T42 à T49", "analyses/prestations/t42_t49_requete_proposee.sql",
         "les nouveaux bénéficiaires par âge et sexe ; pas encore exécutée"),
        ("T50 à T57", "analyses/prestations/t50_t57_requete_proposee.sql",
         "l'évolution sur 5 ans, par branche ; pas encore exécutée"),
        ("T31", "analyses/prestations/t31_requete_proposee.sql",
         "les indus par mois et par branche ; pas encore exécutée"),
        ("DTM_DOSSIER", "analyses/prestations/dtm_dossier_requete_proposee.sql",
         "la table DTM des dossiers, à aligner sur les demandes"),
        ("GRH", "domaines/grh/dtm/sql", "effectif, ratio, indicateurs, formation corrigés"),
    ]),
    ("Les schémas Excalidraw", [
        ("Journal de bord", "schemas/journal", "cette mémoire : état, jours, décisions"),
        ("Maîtrise", "schemas/maitrise", "les fiches de révision, par domaine"),
        ("Prestations", "schemas/prestations", "toutes les règles, les tables, le parcours d'un "
                                               "dossier, les fiches de validation T25 à T27, "
                                               "T31, T32, T41, T42 et T50-T57"),
        ("Annuaire de la CNSS", "schemas/annuaire cnss", "la comparaison des pensions 2024"),
        ("GRH", "schemas/grh", "regles_grh (règles G1 à G11) ; tableaux cipres : fiches T58, "
                               "T60, T62"),
    ]),
    ("Les documents et artefacts (claude.ai)", [
        ("Roadmap métier", "claude.ai/artifact/419Uwt2pL2sbqKjiUBHTbu",
         "questions au métier, vues V01 à V29"),
        ("Spécification des vues BI CIPRES", "claude.ai/code/artifact/3c8c81ad…",
         "les vues simples demandées à la DSI"),
        ("Rapport mouvements du personnel", "claude.ai/artifact/RagXh2KPVP1GrpPd6hRDeC",
         "GRH : constats, règles, questions"),
        ("Exploitation des requêtes DTM GRH", "claude.ai/artifact/MogHiLZrKBTXs5zPJRcCdS",
         "les 4 requêtes et leurs 28 règles"),
        ("Questionnaire CIPRES GRH", "claude.ai/artifact/TyET2bik8ej4DVn4mmejwT",
         "33 questions sur T58 à T65"),
        ("T60 pour la CNSS", "claude.ai/artifact/1XzFW1t6EUAi9eJugZvQxD",
         "mouvements du personnel : constats et questions"),
        ("Prestations sociales (T25 à T57)", "claude.ai/artifact/NsNPyBAqapyfLvoFxoqUkj",
         "constats, règles et questions pour la CNSS"),
        ("Guide du métier", "claude.ai/artifact/MyA586UQY6XsSpskDw1aHX",
         "CNSS, CIPRES, lexique T25 à T57"),
        ("Règles de calcul des prestations", "claude.ai/artifact/UBXiUqe7T7ChuXSPvDwQFR",
         "validation tableau par tableau"),
        ("Tables des prestations", "claude.ai/artifact/UEtDg8Wa5xiEhGu999krDR",
         "les 17 tables, leurs liens et leurs codes"),
    ]),
    ("Les références", [
        ("Annuaire CIPRES", "cnss_files/Annuaire_statistique_CIPRES_finalise V110920.pdf",
         "les 76 tableaux, leurs maquettes, le glossaire"),
        ("Atelier de Kaya 2017", "cnss_files/Atelier de kaya.docx",
         "les décisions de la CNSS sur les tableaux"),
        ("Annuaire de la CNSS 2024", "cnss_files/Annuaire_2024_CNSS_BURKINA-FASO.pdf",
         "les chiffres publiés pour 2024"),
    ]),
]
