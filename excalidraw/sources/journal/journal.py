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
          "Réunion du 02/11 : un doc des points à valider par domaine.",
          "Fiches CIPRES : des requêtes courtes sur les vraies tables, à la place des extraits."],
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
                           "codes et leurs points. Puis réécrits pour qu'un lecteur qui n'a "
                           "jamais ouvert la base comprenne et refasse chaque point : une "
                           "rubrique « Pour lire ce document » (tables et codes), et pour "
                           "chaque point « C'est quoi », « Dans la base », le chiffre, une "
                           "seule question et la réponse (68 points prestations, 44 GRH).",
                      trouve="Les questions étaient réparties entre les README, les fiches, "
                             "le journal et deux anciens docs GRH ; T28 à T30, T63 et T64 "
                             "n'avaient pas encore de questions à eux.",
                      decide="Rien n'est changé dans les règles, README et TODO : les "
                             "réponses de la réunion y seront reportées ensuite.",
                      fichiers="analyses/<domaine>/points_a_valider.md et .docx ; "
                               "schemas/<domaine>/points_a_valider ; "
                               "sources/points_a_valider.py.",
                      etat="fait"),
             Chantier("Les requêtes des fiches CIPRES, en clair", "Projet",
                      demande="Dans les fiches « tableaux cipres », des requêtes pas claires "
                              "(« si.rn ») : mettre une requête condensée, avec les vraies "
                              "tables et les vrais champs ; un test sur T58, puis « applique "
                              "partout ».",
                      fait="À droite de chaque règle, une requête courte sur les tables de "
                           "l'application (USER_GRH ou USER_DWH), pour 2024 (2025 pour T25 à "
                           "T27), à coller dans SQL Developer, avec le chiffre à retrouver en "
                           "commentaire. Une règle construit la liste réutilisée par les "
                           "suivantes (permanents_2024, demandes, pensions_2024…). Fait sur "
                           "les 13 fiches : T58, T60, T62 ; T25, T26, T27, T31, T32, T41, T42, "
                           "T50 à T57. Contrôles : lignes de 52 caractères au plus, aucun texte "
                           "qui déborde, polices vérifiées dans la visionneuse.",
                      trouve="Le test de T58 a été validé tel quel ; les requêtes complètes, sur "
                             "les copies de l'entrepôt, restent les références (citées sous "
                             "chaque fiche).",
                      decide="Les requêtes des fiches servent à vérifier un chiffre à la main ; "
                             "elles ne remplacent pas les requêtes proposées.",
                      fichiers="excalidraw/sources/grh (t58, t60, t62) ; "
                               "excalidraw/sources/prestations (t25 à t50) ; "
                               "schemas/<domaine>/tableaux cipres.",
                      etat="fait"),
         ],
         reste=["Toi : poser au point du 02/11 la question de la date des montants (R4) ; "
                "A06 et A07 ; les requêtes proposées.",
                "Commiter le travail du 31/10 et du 01/11.",
                "Ensuite : T28 à T30 dès la réponse de la DSI ; les fiches de maîtrise à "
                "mettre à jour ; la décision R4 n'arrête rien (réglage date_montant)."]),
    Jour("2026-11-02", "La fiche de lecture, terme par terme",
         ["Fiche de lecture des prestations, à lire à côté de l'Annuaire CIPRES.",
          "Exemple sur T25 : onze termes, chacun compris, situé dans la base, vérifiable.",
          "Forme validée : tous les tableaux, T25 à T57, et la fiche de la GRH, T58 à T65."],
         chantiers=[
             Chantier("La fiche de lecture des tableaux CIPRES", "Projet",
                      demande="Un fichier par domaine, à lire à côté de l'Annuaire CIPRES : pour "
                              "chaque tableau et chaque terme (liquidé, déposé…), ma "
                              "compréhension et la question « tu valides ? », et à côté d'où je "
                              "prends les données et la requête. Un exemple d'abord, sur une "
                              "seule page.",
                      fait="Schéma prestations/fiche_de_lecture : comment lire, les tables de la "
                           "base, puis T25 en onze termes (dossier PVID, lignes, déposés, "
                           "liquidés, durée, trois colonnes, en instance, rejetés, durée moyenne "
                           "et norme, Total, taux de dossiers restant à traiter). Chaque terme : "
                           "ce que dit l'Annuaire avec la page, ce que j'en ai compris, un "
                           "exemple, la question, les cases de réponse ; où je le prends dans la "
                           "base ; la requête pour vérifier (celle de la fiche T25, renumérotée "
                           "par terme).",
                      trouve="Forme validée (« oui ça me convient ») : faite ensuite pour tous "
                             "les tableaux. Prestations : T25, T26, T27, T28 à T30, T31, T32 à "
                             "T40, T41, T42 à T49, T50 à T57 (79 termes). GRH : T58, T59, T61 "
                             "et T65 ensemble, T60, T62, T63 et T64 (31 termes). T28 à T30, T63 "
                             "et T64 n'ont pas encore toutes leurs données : leurs termes "
                             "posent la question et disent où chercher. Ta question sur "
                             "« l'annuaire de la CNSS » : c'était l'annuaire statistique CNSS "
                             "2024, pas l'Annuaire CIPRES ; les deux fiches le disent "
                             "maintenant partout. Le terme 7 de T62 (avec ou sans promotion) "
                             "reprend le lot 21 : 2 promotions liées à une formation en 2024, "
                             "6 publiées.",
                      decide="Une section par famille de tableaux calculés de la même façon ; "
                             "la requête est donnée pour le premier, on change les codes pour "
                             "les autres. Deux noms fixes : « Annuaire CIPRES » (les pages "
                             "citées, l'encadré rouge) et « annuaire CNSS 2024 » (la "
                             "comparaison des chiffres).",
                      fichiers="sources/fiche_de_lecture.py ; "
                               "sources/prestations/lecture_prestations.py ; "
                               "sources/grh/lecture_grh.py ; schemas/prestations/"
                               "fiche_de_lecture et schemas/grh/fiche_de_lecture.",
                      etat="fait"),
         ],
         reste=["Toi : présenter les deux fiches de lecture à côté de l'Annuaire CIPRES, et "
                "noter les réponses dans les cases.",
                "DRH : d'où viennent les 6 formés avec promotion de 2024 (T62) ? La base "
                "n'en lie que 2 à une formation (lot 21, exécuté).",
                "Toi : exécuter le lot 22 de la GRH (Q121 à Q123, structure du grand livre "
                "STG_GL pour T63 et T64) et me coller le résultat ; ensuite, les totaux 2024 "
                "par compte (651 et 652, 63812, 6386 et 6577, 61 à 68, gestion G ou 7).",
                "Ensuite : reporter les réponses dans les règles (README, schémas des règles)."]),
    Jour("2026-11-03", "Le cheminement d'une rente dans la base",
         ["Le terme « dossiers déposés » de T26, les IJ et les rentes, expliqués simplement.",
          "Le chemin d'une rente AT/MP dans la base, de la déclaration au paiement.",
          "Ce chemin mis en schéma, sur une feuille à part."],
         chantiers=[
             Chantier("Le cheminement d'une rente AT/MP", "Prestations",
                      demande="Expliquer en français facile le terme « dossiers déposés » de "
                              "T26, puis ce que sont les IJ et une rente ; montrer comment une "
                              "rente est enregistrée dans la base, étape par étape ; enfin le "
                              "mettre en Excalidraw, sur une feuille à part.",
                      fait="Schéma prestations/vie_dossier/cheminement_rente : le parcours en "
                           "7 étapes (accident, demande au guichet, calcul, notification, "
                           "paiement, vie de la rente, rentes de survivants), ce que chaque "
                           "étape écrit (tables, colonnes, chiffres du lot 4, pièges), les clés "
                           "d'une table à l'autre, un exemple fictif de bout en bout, les "
                           "tableaux CIPRES où la rente compte.",
                      trouve="Rien de nouveau dans les données : tout vient des lots 4 et 7. "
                             "Une rente n'ouvre pas de dossier : elle arrive par une réception "
                             "ANCIEN sur le dossier de l'accident et devient une ligne ARI de "
                             "PRESTATION_ESP (ARSV, ARSF, ARSE, ARSA pour les survivants) ; "
                             "chaque mois payé est un débours.",
                      decide="Une feuille à part : les fiches, les règles et les README des "
                             "prestations ne changent pas.",
                      fichiers="excalidraw/sources/prestations/cheminement_rente.py ; "
                               "schemas/prestations/vie_dossier/cheminement_rente.excalidraw.",
                      etat="fait"),
             Chantier("T26 : la question des dossiers déposés, en clair", "Prestations",
                      demande="Dire ce que la question du terme « dossiers déposés » doit "
                              "éclaircir, puis la reformuler pour qu'elle soit limpide.",
                      fait="Fiche de lecture des prestations, T26, terme 3 : compris, exemple "
                           "et question réécrits. La question devient : « quand la victime "
                           "revient au guichet demander sa rente, des années après "
                           "l'accident, cette demande compte comme un nouveau dossier déposé, "
                           "l'année où elle est faite ? ».",
                      trouve="Le point à trancher : un dépôt par accident, à la déclaration, "
                             "ou un dépôt par demande. Avec un seul dépôt par accident, la "
                             "ligne des rentes reste vide et la durée de liquidation d'une "
                             "rente passe de 14 à 986 jours en médiane (lot 4, P62).",
                      decide="La règle de la requête T26 ne change pas : un dépôt par demande, "
                             "en attendant la réponse.",
                      fichiers="sources/prestations/lecture_prestations.py ; "
                               "schemas/prestations/fiche_de_lecture.",
                      etat="fait"),
             Chantier("Les termes flous des deux fiches de lecture, réécrits", "Projet",
                      demande="Faire pareil pour les autres termes que je trouve flous.",
                      fait="Relu les 110 termes. Réécrits : 35 entrées de la fiche des "
                           "prestations (T25 à T57, dont les termes communs à T26 et T27) et 11 "
                           "de la GRH (T59, T60, T62, T63, T64). Chaque fois une seule question, "
                           "concrète ; plus de questions doubles, de codes non expliqués "
                           "(PE_COT_AUTRE, PIL, CPT…) ni de « tu valides ce classement ? » ; un "
                           "exemple chiffré quand il aide ; les normes CIPRES rappelées (45 "
                           "jours, 5 % de contrôles, 85 %).",
                      trouve="T63 et T64 ne demandent plus « quels comptes ? » : ils proposent "
                             "les comptes de la grille de la CNSS (61 à 68 ; 651 et 652 ; "
                             "63812, 6386 et 6577) et, pour les frais de personnel, les "
                             "comptes 65 (proposition BI).",
                      decide="Seuls les textes changent : les règles et les requêtes restent "
                             "les mêmes.",
                      fichiers="sources/prestations/lecture_prestations.py ; "
                               "sources/grh/lecture_grh.py ; schemas/<domaine>/fiche_de_lecture.",
                      etat="fait"),
         ],
         reste=["Toi : présenter les deux fiches de lecture à côté de l'Annuaire CIPRES, et "
                "noter les réponses dans les cases.",
                "DRH : d'où viennent les 6 formés avec promotion de 2024 (T62) ?",
                "Toi : exécuter le lot 22 de la GRH (Q121 à Q123) et me coller le résultat ; "
                "ensuite, les totaux 2024 par compte pour T63 et T64.",
                "Ensuite : reporter les réponses dans les règles (README, schémas des règles)."]),
    Jour("2026-11-04", "Le recueil des besoins de la CNSS",
         ["Un nouveau document de la CNSS : le recueil des besoins (v7), ateliers de janvier 2026.",
          "Ses remarques, B01 à B41, sur une fiche à part, face à nos règles."],
         chantiers=[
             Chantier("Le recueil des besoins de la CNSS (v7)", "Projet",
                      demande="Lire en entier le nouveau document de cnss_files, puis faire une "
                              "fiche Excalidraw qui énumère les remarques ; on verra après ce "
                              "qu'on en reporte.",
                      fait="Lu : 2566BF_CNSS_Analytics_Description_Besoins_v7.docx (termes de "
                           "référence, tableaux CIPRES par domaine avec les commentaires de la "
                           "CNSS, indicateurs du plan stratégique, 16 notes). Fiche "
                           "besoins cnss/remarques_besoins_v7 : 41 remarques numérotées, en 7 "
                           "parties.",
                      trouve="12 confirmations (ligne de retraite anticipée, orphelins de "
                             "moins de 25 ans, 5e catégorie en cadres moyens, comptes 65…), 8 "
                             "contradictions (T31 et T28 tirés des fichiers Excel de la DAI, "
                             "T54-T55 au montant liquidé, durée d'une rente depuis le certificat "
                             "de consolidation, T59 en 5 domaines, ancienneté de T61), des "
                             "comptes qui ne concordent pas avec la grille des ratios, et des "
                             "exigences nouvelles (périodicité, période en paramètre, "
                             "déclinaison par direction et agence).",
                      decide="Rien n'est reporté dans les règles, fiches ou requêtes : la fiche "
                             "sert à choisir ce qu'on reprend.",
                      fichiers="excalidraw/sources/besoins_cnss/remarques_besoins_v7.py ; "
                               "schemas/besoins cnss/remarques_besoins_v7.excalidraw.",
                      etat="fait"),
             Chantier("Les deux enregistrements de la réunion GRH du 03/11", "GRH",
                      demande="Extraire les informations utiles de deux audios de notre réunion "
                              "(cnss_files : opt.m4a, 59 min ; Voix 261103_144325.m4a, 13 min).",
                      fait="Transcription sur le Mac, sans envoi en ligne : whisper.cpp et le "
                           "modèle large-v3-turbo (installés le 04/11, ~/.cache/whisper.cpp). Une "
                           "première passe bouclait sur des phrases répétées ; refaite sans "
                           "contexte (option -mc 0). Lu en entier, informations triées par "
                           "tableau (T59, T60, T61, T62, T63, T64).",
                      trouve="T59 : construire les domaines à partir des services (unités "
                             "administratives) avec une table de correspondance demandée à la "
                             "CNSS. T60 : GRH_MOUVEMENT surtout interne, entrées et sorties dans "
                             "GRH_SITUATION ; recrutement = toute entrée ? ; prise de service ou "
                             "engagement ; date de décès jamais saisie. T62 : la finalité, pas la "
                             "promotion obtenue ; diplômante = avec promotion ; toutes les "
                             "formations ; catégorie à la date demandée. T63-T64 : la "
                             "comptabilité, par des vues alimentées par la CNSS.",
                      decide="Rien n'est reporté dans les règles : à trier avec toi.",
                      fichiers="Fiche reunions/reunion_grh_2026-11-03 (M01 à M12, avec la "
                               "minute de chaque point) ; script "
                               "sources/reunions/reunion_grh_2026_11_03.py ; transcriptions dans "
                               "le scratchpad de la session (temporaires).",
                      etat="fait"),
             Chantier("GRH : le compte rendu de la réunion, et l'effectif finalisé", "GRH",
                      demande="Reprendre la réunion du 03/11 terme par terme (fiche de lecture "
                              "GRH, termes 1 à 9 : l'effectif) ; faire le fichier Excel de "
                              "correspondance unité → domaine ; mettre à jour la requête de "
                              "l'effectif, avec la liste de ce qui reste et les points qui "
                              "peuvent changer. Ne pas toucher à T60 ni T62, pas encore vus.",
                      fait="Décisions notées terme par terme (filtre global de nature, G2 "
                           "d'accord, catégories CNSS et CIPRES du recueil, temporaire sans fin "
                           "connue = compté, ancienneté depuis la prise de service sans les "
                           "interruptions, domaine par l'unité). Lot 23 (Q124 à Q131) exécuté : "
                           "le code d'unité est UA_CODE, 432 unités CNSS. Fichier "
                           "correspondance_unite_domaine.xlsx : les 432 unités, proposition par "
                           "règles, liste déroulante, reprise des réponses à chaque mise à jour. "
                           "Requête t58_t65_effectif réécrite, puis remise sur la structure de "
                           "DTM_GRH_EFFECTIF à ta remarque (mêmes colonnes, même grain, même "
                           "paramètre ; seules les règles changent ; 5 colonnes ajoutées : "
                           "CATEGORIE, GROUPE_CATEGORIE, UA_CODE, DOMAINE_ACTIVITE, NB_CADRES) ; "
                           "règles, réglages, points susceptibles de changer et 7 tâches "
                           "restantes dans analyses/grh/regles_effectif.md.",
                      trouve="Le code de domaine de la situation n'est pas fiable (66 unités sur "
                             "139 mélangent les codes). Le recueil des besoins passe avant Kaya "
                             "et l'Annuaire CIPRES (consigne de l'utilisateur). La mise en page "
                             "des tableaux CIPRES se fait dans OBIEE, pas dans le DTM.",
                      decide="Requête de l'effectif prête à exécuter ; T59 en « Domaine non "
                             "renseigné » tant que le fichier n'est pas rendu et branché.",
                      fichiers="analyses/grh : lot23_unites_administratives_t59.sql, "
                               "unites_cnss.tsv, correspondance_unite_domaine.py et .xlsx, "
                               "t58_t65_effectif_requete_proposee.sql, regles_effectif.md.",
                      etat="en cours"),
         ],
         reste=["Toi : exécuter les deux requêtes de contrôle de l'effectif (en-tête du "
                "fichier, cliché 102026) et me coller le résultat.",
                "CNSS : remplir le fichier unité → domaine (132 lignes « À décider »).",
                "Ensuite : les termes de T60 et T62 de la fiche de lecture.",
                "Toi : relire la fiche des remarques et dire lesquelles reporter dans nos "
                "règles et nos fiches.",
                "DFC : trancher les comptes (charges de fonctionnement 61 à 66 ou 61 à 68 ; "
                "dépenses de formation).",
                "Toi : exécuter le lot 22 de la GRH (Q121 à Q123) et me coller le résultat.",
                "Rien n'est commité depuis le 01/11."]),
    Jour("2026-11-05", "Les décisions face aux situations",
         ["D'où vient chaque information de l'effectif : la fiche de l'agent, les décisions, "
          "la situation.",
          "Lot 24 rédigé pour vérifier que les trois tables racontent la même histoire."],
         chantiers=[
             Chantier("GRH : sorties, retours et entrées, décision ou situation ?", "GRH",
                      demande="Comprendre ce que veulent dire « sorties et retours » dans "
                              "l'ancienneté (recueil des besoins, T61), où on les lit, si la "
                              "nature de la situation peut servir pour les sorties ou pour "
                              "les entrées ; puis préparer la requête de vérification.",
                      fait="Réponses données à partir des lots 2, 13, 17, 18 et 20. Lot 24 "
                           "rédigé (Q134 à Q139) : CLA par agent et face à la date "
                           "d'embauche ; nature et statut de la situation autour des "
                           "décisions 07, 14, 18, 37, 50, 51 ; statut E face aux décisions 07 "
                           "et 18 ; permanents en disponibilité au 31/12/2024 selon la "
                           "décision et selon le statut.",
                      trouve="La fiche de l'agent donne l'entrée (date d'embauche : les 104 "
                             "recrutements de 2024 retrouvés) ; les décisions donnent les "
                             "départs, disponibilités, remises en activité et engagements ; la "
                             "situation donne le classement. La nature de la situation n'a aucun "
                             "code de sortie ni de retour (REC = reclassement, RET = "
                             "rétrogradation, DIS = mise à disposition). Le statut E n'est "
                             "presque jamais saisi (lot 18) : les agents inactifs comptés par le "
                             "statut risquent de manquer les disponibilités.",
                      decide="Rien n'est changé dans les règles ni dans la requête de "
                             "l'effectif avant le résultat du lot 24.",
                      fichiers="analyses/grh/lot24_decisions_et_situations.sql.",
                      etat="fait"),
             Chantier("GRH : le résultat du lot 24", "GRH",
                      demande="Lire le résultat du lot 24 (Q134 à Q139), collé par "
                              "l'utilisateur.",
                      fait="Résultats et constats reportés dans le lot, bilan en tête du "
                           "fichier, README à jour.",
                      trouve="L'entrée reste la date d'embauche : le CLA la suit 9 fois sur 10 "
                             "depuis 2019, mais avant 2000 c'est une reprise des données. Les "
                             "décisions 07, 14, 18 et les renouvellements 50 ne créent presque "
                             "jamais de situation. Le statut de la situation n'est pas daté : il "
                             "est modifié sur la situation en cours au départ ou à l'absence "
                             "(145 CLA de temporaires au statut X, 24 permanents présents au "
                             "31/12/2024 déjà en R). Au 31/12/2024, les 6 permanents en "
                             "disponibilité selon les décisions sont au statut A, donc comptés "
                             "actifs par NB_AGENTS_INACTIFS.",
                      decide="Proposition BI : compter les agents en disponibilité par les "
                             "décisions 07 et 18, comme QG6 et G5. Acceptée par toi le 05/11.",
                      fichiers="analyses/grh/lot24_decisions_et_situations.sql ; "
                               "analyses/grh/README.md.",
                      etat="fait"),
             Chantier("GRH : deux corrections de la requête de l'effectif", "GRH",
                      demande="Vérifier si la requête proposée T58 à T65 est correcte après le "
                              "lot 24, puis corriger les deux points trouvés.",
                      fait="Relue : entrée, départs, contrats, ancienneté et classement "
                           "concordent avec le lot 24. Corrigé : les actifs et inactifs se "
                           "lisent dans les décisions 07 et 18 (règle G13, réglage inactifs, "
                           "STATUT pour revenir à l'ancien calcul) ; une fiche sans état compte "
                           "comme encore là (G2). Contrôle T65 étendu au 31/12/2025. "
                           "regles_effectif.md et README à jour.",
                      trouve="La requête demandait une fiche ACTIF : les 67 fiches sans état "
                             "(embauches récentes jusqu'au 28/01/2026) n'étaient comptées nulle "
                             "part, ce qui risquait de sous-compter le 31/12/2025.",
                      decide="G13 et la fiche sans état sont des propositions BI, à faire "
                             "viser par la DRH avec les autres règles.",
                      fichiers="analyses/grh : t58_t65_effectif_requete_proposee.sql, "
                               "regles_effectif.md, README.md.",
                      etat="fait"),
         ],
         reste=["Toi : exécuter les deux requêtes de contrôle de l'effectif, jusqu'au "
                "31/12/2025, et me coller le résultat.",
                "Le schéma des règles GRH (regles_grh) n'a ni G12 ni G13.",
                "DSI : confirmer que le statut est modifié sur la situation en cours, et le "
                "sens des natures REM, DET, ANN, ABA.",
                "DRH : viser G13 et la fiche sans état, avec les autres règles.",
                "CNSS : remplir le fichier unité → domaine.",
                "Question à poser : au retour d'un agent, retirer le temps d'absence de "
                "l'ancienneté, ou repartir de zéro ?",
                "Rien n'est commité depuis le 01/11."]),
    Jour("2026-11-07", "La requête de l'effectif expliquée, et le lot 25",
         ["La requête de l'effectif en schéma : une seule photo, mais des dates, et trois "
          "agents fictifs rejoués au 31/12 de 2018 à 2025.",
          "Lot 25 : les agents partis puis revenus sont tous des temporaires. G2 tient, la "
          "requête ne change pas.",
          "Tous les cas d'agents de l'effectif en un schéma : compté, inactif ou pas compté ; "
          "puis la version technique, reliée au SQL.",
          "T63 et T64 : la structure simple à envoyer à la DSI et à la DFC, sans SQL.",
          "Prestations et Finance : trois vues à demander à la CNSS, dont une balance "
          "comptable commune."],
         chantiers=[
             Chantier("GRH : la requête de l'effectif expliquée en schéma", "GRH",
                      demande="Un petit schéma Excalidraw pour voir concrètement comment la "
                              "requête de l'effectif retrouve ce qui est demandé pour les "
                              "années passées.",
                      fait="Schéma en quatre parties : l'idée (le DWH ne garde qu'une photo, "
                           "mais la fiche, les situations et les décisions sont datées) ; la "
                           "requête partie par partie (0 à 8, puis OBIEE) ; trois agents "
                           "fictifs sur une frise de 2018 à 2025 (un permanent promu, mis en "
                           "disponibilité puis retraité ; un temporaire à trois contrats ; un "
                           "agent parti sans date, comme les 84 engagés de 2019) ; ce qui en "
                           "sort et ce que la base ne permet pas de rejouer.",
                      trouve="En dessinant : G2 ne lit que le premier départ et la fiche "
                             "d'aujourd'hui. Un agent parti puis revenu pourrait être mal "
                             "compté ; vérifié par le lot 25.",
                      fichiers="schemas/grh/requete_effectif_pas_a_pas.excalidraw ; "
                               "sources/grh/requete_effectif_pas_a_pas.py.",
                      etat="fait"),
             Chantier("GRH : lot 25, les agents partis puis revenus", "GRH",
                      demande="Préparer un lot pour compter les agents partis puis revenus, "
                              "et mesurer ce que G2 en fait à chaque 31/12.",
                      fait="Lot 25 rédigé (Q140 à Q142), testé dans SQLite sur six agents "
                           "fictifs, exécuté par toi ; résultats reportés dans le lot et le "
                           "README des analyses GRH.",
                      trouve="41 allers-retours pour 30 agents, tous des fins de contrat (35) "
                             "suivies plus tard d'un nouvel engagement temporaire (50). Aucun "
                             "permanent parti puis revenu, aucune titularisation juste après "
                             "une 35. G2 ne compte personne pendant une absence et n'oublie "
                             "qu'un agent sans situation, de 2015 à 2019. 747 permanents au "
                             "31/12/2024 (742 au lot 17, qui ne prenait que les fiches ACTIF) ; "
                             "886 au 31/12/2025.",
                      decide="G2 reste telle quelle : la requête de l'effectif ne change pas.",
                      fichiers="analyses/grh/lot25_partis_puis_revenus.sql ; "
                               "analyses/grh/README.md.",
                      etat="fait"),
             Chantier("GRH : la structure comptable de T63 et T64", "GRH",
                      demande="Mettre en place la structure (table ou vue matérialisée) que "
                              "la CNSS alimentera, pour construire les tableaux GRH qui "
                              "demandent des montants financiers. Toujours lire d'abord le "
                              "recueil des besoins.",
                      fait="Recueil v7 relu (RH lignes 6 et 7, indicateur de formation, "
                           "Finance T66 et T69), puis l'Annuaire, la grille des ratios et "
                           "Kaya. Table STG_GL.BI_GRH_SOLDE_COMPTABLE proposée : montants "
                           "par exercice, mois, gestion, compte et section analytique, "
                           "réalisés et budgétés. Elle est remplie par la DSI depuis Oracle "
                           "GL, par son traitement ou par une vue matérialisée posée "
                           "dessus. Avec elle : cinq questions à la DFC et quatre contrôles, "
                           "testés dans SQLite.",
                      trouve="Le recueil et la grille ne donnent pas les mêmes comptes : "
                             "formation « 63, 86 et 87 » ou 63812, 6386 et 6577 ; charges "
                             "61 à 66 ou 61 à 68 ; les frais de personnel prennent G et S, "
                             "les charges G seule.",
                      decide="Proposition BI : des montants par compte, pas des rubriques "
                             "déjà calculées ; la BI choisit les comptes par réglage.",
                      fichiers="analyses/grh/t63_t64_structure_comptable.sql ; "
                               "analyses/grh/README.md.",
                      etat="fait"),
             Chantier("GRH : tous les cas d'agents de l'effectif", "GRH",
                      demande="Lister clairement tous les cas d'agents que traite la requête "
                              "de l'effectif (permanent, temporaire en fin de contrat…), puis "
                              "les mettre dans un schéma Excalidraw simple.",
                      fait="Recueil v7 relu (RH lignes 1 et 4 : permanent ou temporaire ; "
                           "sorties et retours pour l'ancienneté). Liste donnée dans la "
                           "conversation, puis schéma : le parcours d'un agent au 31/12, "
                           "12 cas de permanents, 7 cas de temporaires, 3 cas entre les deux, "
                           "le rangement (catégorie, âge, sexe, ancienneté, domaine), les "
                           "réglages et les sources.",
                      trouve="Un agent sans situation au 31/12 est compté, mais sa nature est "
                             "vide : les filtres Permanent et Temporaire d'OBIEE l'écartent "
                             "des tableaux. Nombre à compter.",
                      fichiers="schemas/grh/cas_agents_effectif.excalidraw ; "
                               "sources/grh/cas_agents_effectif.py.",
                      etat="fait"),
             Chantier("GRH : les cas d'agents reliés à la requête SQL", "GRH",
                      demande="Une seconde version, plus technique, qui relie le schéma des "
                              "cas aux requêtes, pour mieux comprendre la requête SQL.",
                      fait="Schéma en sept parties : la requête en un coup d'œil (parties 0 "
                           "à 8, CTE, lignes du fichier, grain, alias) ; le WHERE de base "
                           "annoté ligne par ligne ; pour chaque cas (permanents, temporaires, "
                           "entre les deux), le verdict, la partie, le bout de SQL qui décide "
                           "et la raison ; les axes et les mesures ; le comptage final.",
                      trouve="Trois cas tiennent à un NULL ramené par un LEFT JOIN : fiche "
                             "INACTIF sans date (NULL > date jamais vrai), temporaire sans 50 "
                             "(tc.PERS_ID NULL), agent sans situation (NATURE_EMPLOI NULL).",
                      fichiers="schemas/grh/cas_agents_effectif_sql.excalidraw ; "
                               "sources/grh/cas_agents_effectif_sql.py.",
                      etat="fait"),
             Chantier("GRH : la requête de T63 et T64", "GRH",
                      demande="Les tableaux GRH en rapport avec la finance : deux ou trois "
                              "tableaux à faire, en reprenant ce qui a été dit.",
                      fait="Recueil v7 relu (RH lignes 6 et 7, Finance ligne 1, indicateur "
                           "FOR / MSAL), puis l'Annuaire, Kaya et la grille : ce sont T63 "
                           "(FP / CF) et T64 (FOR / MSAL et FOR / BUD). Requête proposée pour "
                           "DTM_INDICATEURS_FINANCES_GRH, mêmes colonnes et même grain, sept "
                           "colonnes ajoutées ; fichier de déploiement (fait DWH et ODS, SQL du "
                           "fait, colonnes du DTM). Testée : syntaxe Oracle, rejeu SQLite sans "
                           "écart.",
                      trouve="« 63, 86 et 87 » pourrait vouloir dire 6386 et 6387. FP sur G et "
                             "S face à CF sur G seule peut donner un ratio T63 au-dessus de 1.",
                      decide="Proposition BI : comptes par réglage, valeurs par défaut du "
                             "recueil ; taux seulement pour un exercice clos ; la déclinaison "
                             "par unité viendra dans une autre table (QF5).",
                      fichiers="analyses/grh/t63_t64_requete_proposee.sql ; "
                               "analyses/grh/t63_t64_deploiement.sql ; analyses/grh/README.md.",
                      etat="fait"),
             Chantier("GRH : la structure de T63-T64 à envoyer à la CNSS", "GRH",
                      demande="Pas de SQL : c'est à la CNSS de fournir les données. Définir "
                              "seulement une structure simple (colonnes, types, granularité) "
                              "que la DSI créera, en table ou en vue, et alimentera.",
                      fait="Schéma à envoyer à la DSI et à la DFC : pourquoi (T63, T64, "
                           "recueil v7), la structure (nom proposé, une ligne, dix colonnes "
                           "avec type et contenu), des lignes fictives, les règles "
                           "d'alimentation, ce que la BI en fera et les cinq questions à la DFC.",
                      trouve="Le doc « Spécification des vues BI CIPRES » n'est pas accessible "
                             "depuis cette conversation.",
                      decide="Structure simplifiée : pas de mois (T63 et T64 sont annuels), un "
                             "montant net, un indicateur d'exercice clos. Les fichiers SQL "
                             "d'avant restent des brouillons BI, à aligner.",
                      fichiers="schemas/grh/t63_t64_donnees_a_fournir.excalidraw ; "
                               "sources/grh/t63_t64_donnees_a_fournir.py.",
                      etat="fait"),
             Chantier("Prestations et Finance : les vues à demander à la CNSS", "Tous",
                      demande="D'après le recueil v7 et les tableaux CIPRES, trouver ce qui "
                              "manque en prestations et en gestion financière, et proposer des "
                              "vues simples, combinées si possible, que la CNSS créera dans la "
                              "base source. Ne pas lire la comptabilité de la copie : la CNSS "
                              "la dit non valide.",
                      fait="Recueil v7 relu (Prestation, Finance), maquettes de l'Annuaire "
                           "(T28 à T30, T63 à T76), Kaya, nos lots. Quatre schémas dans "
                           "schemas/vues cnss : le sommaire, et trois vues. "
                           "V_BI_BALANCE_COMPTABLE est la balance d'Oracle GL, mois par mois, "
                           "avec le budget : T63, T64, T66 à T76, SP1, SP2. "
                           "V_BI_CONTROLE_POSTERIORI couvre T28 à T30, V_BI_RESERVE couvre "
                           "T75. Le schéma T63-T64 reprend la vue commune.",
                      trouve="Prestations : seuls les contrôles a posteriori manquent. T31 "
                             "est dans la base (AJUSTEMENT) et T71 semble dans TXDEPOT. "
                             "Finance : une seule balance mensuelle suffit, avec le mois 0 "
                             "pour T72 et la branche pour T66 à T68 ; les réserves sont "
                             "calculées à la main par la DFC.",
                      decide="Trois vues au lieu d'une par tableau ; noms en V_BI_ ; la BI "
                             "regroupe les comptes par réglage. Lot 22 abandonné.",
                      fichiers="schemas/vues cnss (00 à 03) ; sources/vues_cnss "
                               "(vues_cnss.py, commun.py) ; schéma t63_t64_donnees_a_fournir "
                               "aligné.",
                      etat="fait"),
         ],
         reste=["Toi : exécuter les deux requêtes de contrôle de l'effectif (DWH, cliché "
                "102026), jusqu'au 31/12/2025, et me coller le résultat.",
                "Le schéma des règles GRH (regles_grh) n'a ni G12 ni G13.",
                "DSI : confirmer que le statut est modifié sur la situation en cours, et le "
                "sens des natures REM, DET, ANN, ABA.",
                "DRH : viser G13 et la fiche sans état, avec les autres règles.",
                "CNSS : remplir le fichier unité → domaine.",
                "Question à poser : au retour d'un agent, retirer le temps d'absence de "
                "l'ancienneté, ou repartir de zéro ?",
                "Toi : soumettre les schémas « vues cnss » à la CNSS (DSI, DFC, DAI).",
                "DSI, DFC, DAI : créer et alimenter les trois vues, répondre aux questions des "
                "schémas ; ensuite, la BI écrit les requêtes (T28 à T30, T63 à T76).",
                "À commiter : le lot 25, la structure de T63-T64, le schéma "
                "requete_effectif_pas_a_pas, les schémas cas_agents_effectif (simple et SQL), "
                "la requête et le déploiement de T63-T64, le schéma t63_t64_donnees_a_fournir, "
                "les schémas vues cnss et leurs sources, "
                "le README des "
                "analyses GRH, et les retouches de la requête de l'effectif.",
                "Compter les agents sans situation à chaque 31/12 (comptés, mais dans aucun "
                "tableau)."]),
    Jour("2026-11-08", "T60, le mouvement du personnel",
         ["T60 réécrite sur la structure de DTM_GRH_MOUVEMENT : deux photos de l'effectif "
          "de T58, d'un 31/12 à l'autre.",
          "Le détail des « autres » demandé par le recueil v7 : les codes de mouvement de "
          "la CNSS, déjà dans l'application GRH.",
          "Les règles G14 à G17, et un schéma de T60 cas par cas."],
         chantiers=[
             Chantier("GRH : la requête de T60", "GRH",
                      demande="Attaquer le tableau 60, le mouvement du personnel.",
                      fait="Recueil v7 relu (RH ligne 3 : détailler les « autres », "
                           "déclinaison direction, agence, bureau, périodicité annuelle ; RH "
                           "ligne 1 : permanent ou temporaire), puis Kaya, l'Annuaire CIPRES, "
                           "la réunion du 03/11 (M06 à M12) et les lots 1, 17, 18, 20, 24, 25. "
                           "Requête réécrite avec la structure de DTM_GRH_MOUVEMENT (mêmes "
                           "colonnes, même grain, même paramètre ; quatre colonnes ajoutées : "
                           "QUALITE_EMPLOYE, NATURE_EMPLOI, UA_CODE, MOTIF_MVT). Début et fin = "
                           "les présents de T58 au 31/12 ; entrées et sorties d'une photo à "
                           "l'autre, par nature, avec les codes de la CNSS et un motif ; "
                           "réglages passages_annee et mouvements_internes. Testée hors ligne : "
                           "syntaxe Oracle, rejeu SQLite sur 22 agents fictifs et 4 "
                           "combinaisons de réglages, sans écart, même début que T58.",
                      trouve="La table des types de mouvement de la CNSS (GRH_TYPE_MOUVEMENT) "
                             "a déjà le détail des « autres » : remise en activité, détaché ; "
                             "licenciement, démission, disponibilité, mise à disposition, fin "
                             "de détachement, fin de contrat. Le tableau 121 de l'annuaire CNSS "
                             "2024 détaille de la même façon. Par unité, début + entrées − "
                             "sorties ne donne pas la fin à cause des mutations : la fin d'une "
                             "unité se lit dans T58.",
                      decide="Proposition BI : la requête reprend ces codes ; la disponibilité "
                             "suit le réglage de l'effectif (PRESENT) ; un agent entré et sorti "
                             "dans l'année n'est pas compté (Q4 de la fiche T60) ; les mouvements "
                             "internes restent lus comme avant.",
                      fichiers="analyses/grh : t60_requete_proposee.sql (réécrite), "
                               "regles_t60.md, README, TODO.",
                      etat="fait"),
             Chantier("GRH : T60 cas par cas, en schéma", "GRH",
                      demande="Expliquer la nouvelle requête de T60 en schéma, comme les cas "
                              "d'agents de l'effectif (tout en Excalidraw).",
                      fait="Schéma en six parties : les deux photos ; la maquette CIPRES avec "
                           "les codes de la CNSS sous chaque colonne ; 6 cas d'entrée ; 8 cas "
                           "de sortie ; 8 cas sans mouvement ; 2024 face à l'annuaire CNSS. "
                           "Puis T60 dans OBIEE, les réglages et les sources.",
                      trouve="2024 : 677 au début, 104 recrutements, 25 retraites, 5 décès, 4 "
                             "autres sorties, 747 à la fin, pour 767, 104, 23, 3, 94 et 758 "
                             "publiés ; l'écart vient des 84 engagés de 2019, des "
                             "disponibilités et du décalage des décès.",
                      fichiers="schemas/grh/cas_mouvements_t60.excalidraw ; "
                               "sources/grh/cas_mouvements_t60.py.",
                      etat="fait"),
             Chantier("GRH : la disponibilité, réglage de la CTE parametres de T60", "GRH",
                      demande="Rendre paramétrable, dans T60, le traitement des disponibilités "
                              "et des remises en activité, comme dans la CTE parametres de la "
                              "requête de l'effectif.",
                      fait="J'avais d'abord compris une variable saisie à l'exécution (:2) et "
                           "modifié les deux requêtes : annulé à ta remarque. L'effectif est "
                           "revenu à sa version commitée (requête, règles, deux schémas). Dans "
                           "T60, la disponibilité reste un réglage de la CTE parametres (PRESENT "
                           "ou HORS_EFFECTIF), avec un commentaire qui dit ce que fait chaque "
                           "valeur ; le cliché n'y figure plus. L'effectif avait déjà ce réglage "
                           "(ligne 170, appliqué dans le WHERE) : seul son commentaire est rendu "
                           "explicite. Tests refaits : 0 écart pour les deux valeurs.",
                      trouve="Le schéma cas_agents_effectif porte trois annotations "
                             "« A MIEUX ECLAIRCIR » (fin de contrat puis réengagé, temporaire "
                             "devenu permanent, aucune fin de contrat saisie) : il n'a pas été "
                             "régénéré.",
                      decide="« Paramètres » veut dire la CTE parametres de la requête, pas une "
                             "variable :2. Même valeur de disponibilite dans T58 et T60 : tu "
                             "gardes PRESENT en attendant la réponse de la DRH (QG6).",
                      fichiers="analyses/grh : t60_requete_proposee.sql, regles_t60.md, "
                               "README, t58_t65_effectif_requete_proposee.sql et "
                               "regles_effectif.md (commentaire) ; schéma cas_mouvements_t60.",
                      etat="fait"),
             Chantier("GRH : la requête du mouvement pas à pas, et les schémas rangés", "GRH",
                      demande="Un schéma « requête du mouvement pas à pas », comme celui de "
                              "l'effectif ; et regrouper les schémas de la GRH, en désordre.",
                      fait="Schéma en quatre parties : l'idée des deux photos ; la requête partie "
                           "par partie, avec ses numéros de ligne lus dans le fichier SQL ; six "
                           "agents fictifs d'un 31/12 à l'autre (retraite, recrutement, "
                           "titularisation, disponibilité, décès lu à la liquidation, entré et "
                           "sorti la même année) ; les lignes de 2024 qu'ils donnent, rejouées "
                           "dans la requête, le contrôle attendu et les limites. Schémas GRH "
                           "rangés par famille : T58 effectif, T60 mouvements, T63 T64 finances, "
                           "déplacés avec leur historique comme le fait la visionneuse.",
                      trouve="Les six agents donnent bien les lignes du schéma, avec PRESENT comme "
                             "avec HORS_EFFECTIF (DISP en 2023, REM en 2025).",
                      fichiers="schemas/grh/T60 mouvements/requete_mouvement_pas_a_pas ; "
                               "sources/grh/requete_mouvement_pas_a_pas.py ; dossiers schemas/grh/"
                               "T58 effectif, T60 mouvements, T63 T64 finances ; README.",
                      etat="fait"),
         ],
         reste=["Toi : exécuter les deux requêtes de contrôle de T60 (en-tête du fichier, "
                "cliché 102026 ; disponibilite à PRESENT, puis à HORS_EFFECTIF) et me coller le "
                "résultat.",
                "Claude : éclaircir les trois cas annotés « A MIEUX ECLAIRCIR » du schéma "
                "cas_agents_effectif, si tu le veux.",
                "Toi : vérifier que DTM.DIM_GRH_TYPE_MOUVEMENT a les 18 codes de la CNSS, plus "
                "NOM.",
                "Claude : reporter les règles de T60 dans la fiche de lecture GRH et dans la "
                "fiche T60, avant de les voir en réunion.",
                "DRH : trancher la disponibilité (QG6), qui change T58 et T60 ensemble ; "
                "répondre aux 8 questions de regles_t60.md.",
                "Toi : exécuter les deux requêtes de contrôle de l'effectif (DWH, cliché "
                "102026), jusqu'au 31/12/2025.",
                "Toi : soumettre les schémas « vues cnss » à la CNSS (DSI, DFC, DAI).",
                "À commiter : la requête T60 et regles_t60.md, les schémas de T60 (cas, pas "
                "à pas) et le rangement des schémas GRH en dossiers, "
                "les schémas vues cnss et leurs sources, les README, le TODO GRH et le "
                "journal."]),
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
    Decision("2026-11-08", "T60 : deux photos de T58 et les codes de mouvement de la CNSS",
             "Le mouvement d'un exercice se lit entre les présents de T58 au 31/12 d'avant et "
             "au 31/12 (début + entrées − sorties = fin) ; chaque entrée et chaque sortie "
             "reçoit un code de la table GRH_TYPE_MOUVEMENT de la CNSS (RECRU, REM, DETAC, "
             "AUTRE_ENT ; RET, DEC, LIC, DEM, DISP, FIN_CTRAT, AUTRE_SORT…) et un motif.",
             "Le recueil des besoins v7 demande de détailler les « autres » ; la CNSS a déjà "
             "cette liste, et l'annuaire CNSS 2024 (tableau 121) l'utilise. La table des "
             "mouvements de l'application n'a ni recrutement ni départ (lot 1, M06).",
             "Proposition BI, à faire viser par la DRH",
             "grh_mouvement.sql sera remplacée par t60_requete_proposee.sql, avec quatre "
             "colonnes de plus dans DTM_GRH_MOUVEMENT ; T60 se met en page dans OBIEE.",
             statut="proposé"),
]

# =============================================================================================
# L'état aujourd'hui
# =============================================================================================

MISE_A_JOUR = "2026-11-08"

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
     "contractuels expliqués ; lot 24 : G13 et la fiche sans état ; lot 25 : G2 tient "
     "(les agents partis puis revenus sont tous des temporaires). T63 et T64 : structure "
     "comptable et requête proposées (07/11). T60 réécrite le 08/11 sur la structure du "
     "DTM, avec le détail des « autres » par les codes de la CNSS.",
     "Règles G1 à G17, schéma regles_grh, fiches T58, T60, T62, schémas de la requête de "
     "l'effectif, des cas d'agents et de T60 ; requêtes de l'effectif et de T60 à exécuter ; "
     "fiche de lecture T60 à mettre à jour, puis réunion ; visa de la DRH ; "
     "T63-T64 attendent la DFC et le premier chargement de la DSI."),
    ("Immatriculation et recouvrement (T1 à T24)", "à faire",
     "Les vues simples sont spécifiées pour la DSI (20/10).",
     "Pas encore d'analyse."),
    ("Gestion financière (T66 à T76)", "en cours",
     "Vues proposées le 07/11 (schemas/vues cnss) : la balance comptable commune "
     "(T66 à T76, SP1, SP2) et les réserves (T75). La comptabilité de la copie n'est pas "
     "lue : la CNSS la dit non valide.",
     "Soumettre les vues à la CNSS ; requêtes à écrire quand elles seront créées."),
    ("Outils", "fait",
     "Visionneuse hors ligne, fiches de maîtrise, journal de bord, schémas de travail.",
     "Tenir le journal et les fiches à jour."),
]

# (qui, ce qu'on attend)
EN_ATTENTE = [
    ("Toi", "Exécuter les contrôles de T60 et de l'effectif (cliché 102026 ; "
            "disponibilite à PRESENT, puis à HORS_EFFECTIF) ; vérifier les "
            "codes de DIM_GRH_TYPE_MOUVEMENT. "
            "Au point du 02/11 : la date des montants (R4). Exécuter les trois requêtes "
            "GRH proposées (102026, 2024) ; faire viser G1 à G11 et poser QG1 à QG7 ; A06 et A07 ; les requêtes proposées sur le cliché 092026 ; relancer "
            "A04 ; commiter le travail du 31/10 et du 01/11."),
    ("La direction des prestations", "Répondre aux questions des fiches T25 à T27 (13), "
                                     "T31, T32, T41, T42 et T50-T57 (5 chacune) ; confirmer "
                                     "R6, "
                                     "R7, la limite de 25 ans des orphelins, le montant, la "
                                     "coordination et la règle de T41 (enfants payés)."),
    ("La DRH", "Remplir les fiches T58, T60, T62 ; viser les règles G1 à G17 ; répondre à "
               "QG1 à QG7 et au questionnaire CIPRES ; trancher la disponibilité (QG6) et "
               "les 8 questions de T60 (regles_t60.md)."),
    ("La DSI", "Le sens des statuts, le circuit des paiements sans effet ; créer et "
               "alimenter les vues des schémas vues cnss : V_BI_BALANCE_COMPTABLE (depuis "
               "Oracle GL), V_BI_CONTROLE_POSTERIORI (avec la DAI), V_BI_RESERVE (avec la "
               "DFC)."),
    ("La DFC", "Répondre aux questions des schémas vues cnss 01 et 03 et T63-T64 : branche "
               "des écritures, types de placement, réalisé du budget, comptes de formation et "
               "de masse salariale, réserves."),
    ("La DAI", "Dire où sont les contrôles a posteriori depuis juillet 2025 ; alimenter "
               "V_BI_CONTROLE_POSTERIORI ; comparer son fichier des indus à la base (T31)."),
    ("Oracle Analytics", "Accepter le changement de sens des colonnes de DTM_DOSSIER."),
]

# la suite, dans l'ordre
SUITE = [
    ("T60", "Reporter le résultat des contrôles ; mettre à jour les termes de T60 de la fiche "
            "de lecture et la fiche T60 ; voir T60 en réunion avec la CNSS ; puis T62."),
    ("Les requêtes GRH", "Reporter l'exécution des trois requêtes proposées ; ajuster les "
                         "fiches T58, T60, T62 si les chiffres bougent ; puis les fiches de "
                         "maîtrise « 4 gestion administrative »."),
    ("Les fiches de maîtrise", "Mettre à jour le dossier 3 prestations avec les lots 5 à 9 et "
                               "les règles R13, T41, T50-T57, T31."),
    ("Les contrôles (T28 à T30)", "La table des contrôles n'est plus alimentée depuis "
                                  "juillet 2025 : vue V_BI_CONTROLE_POSTERIORI proposée le "
                                  "07/11, à créer par la CNSS."),
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
        ("GRH", "analyses/grh", "lots 1 à 25 (Q01 à Q142), règles G1 à G17, requêtes "
                                "proposées (effectif, T60, T62), regles_effectif.md, "
                                "regles_t60.md, README, TODO"),
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
        ("Vues CNSS", "schemas/vues cnss", "les vues à demander à la CNSS : sommaire, balance "
                                          "comptable, contrôles a posteriori, réserves"),
        ("GRH", "schemas/grh", "rangé par famille : T58 effectif, T60 mouvements, T63 T64 "
                               "finances ; regles_grh (règles G1 à G11) ; requete_effectif_pas_a_pas "
                               "(la requête de l'effectif expliquée) ; cas_agents_effectif "
                               "(tous les cas d'agents) et cas_agents_effectif_sql (les mêmes, "
                               "reliés à la requête) ; cas_mouvements_t60 (T60 cas par "
                               "cas) et requete_mouvement_pas_a_pas ; t63_t64_donnees_a_fournir (la "
                               "structure à envoyer à la CNSS) ; tableaux cipres : "
                               "fiches T58, T60, T62"),
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
