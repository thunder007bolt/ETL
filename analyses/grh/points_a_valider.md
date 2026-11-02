# GRH : règles et questions à valider

Mis à jour le 01/11/2026. Base consultée : copie de l'application de gestion du personnel (schéma USER_GRH) du 18/02/2026, comparée à l'annuaire statistique CNSS 2024 (partie IX, tableaux 119 à 124).

Chaque point dit de quoi on parle, où on le voit dans la base, ce qu'on fait aujourd'hui, puis pose une seule question. Les noms en majuscules sont ceux des tables et des champs de la base : avec eux, on peut refaire chaque chiffre. Tous les tableaux T58 à T65 sont repris à la fin. S'il n'y a pas le temps de tout voir : les points 1 à 5 d'abord.

## Pour lire ce document

- La fiche de l'agent, table GRH_PERSONNE : une ligne par personne. On y trouve le sexe (SEXE : M ou F), la date de naissance (DATE_NAISS), la date d'embauche (DATE_EMBAUCHE), la date de prise de service (DATE_PRISE_SERVICE), l'état (ETAT_EMPLOYE : ACTIF ou INACTIF) et la qualité (QUALITE : A agent de la CNSS, B agent de la fonction publique en poste à la CNSS, C agent affecté).
- La situation de l'agent, table GRH_SITUATION : une ligne à chaque changement dans sa carrière (catégorie, poste, position), avec sa date de début (DATE_DEBUT). Pour connaître un agent au 31/12, on prend sa dernière situation commencée ce jour-là ou avant. Elle donne la nature de l'emploi (NATURE_EMPLOIE : PERMANENT ou TEMPORAIRE), la catégorie (CATEGORIE : 01 à 06), la position (STATUT_EMPLOYE) et le domaine d'activité (CODE_DOMAINE_ACT).
- La position de l'agent (STATUT_EMPLOYE) : A activité normale, B détaché, C stagiaire, D mis à disposition, E en disponibilité, F suspendu, G en congé payé, H sous les drapeaux, R retraité, X radié, Y décédé. Les mots sont expliqués aux points 5 et 14.
- Les décisions, table GRH_DECISION : chaque décision a un code (CDEC_CODE) et une date d'effet (DATE_VIGUEUR). Une décision qui vise plusieurs agents les liste dans GRH_DECISION_PERSONNE. Codes utiles : 02 retraite ; 07 mise en disponibilité ; 14 mise à disposition ; 18 remise en activité ; 34 liquidation des droits après un décès ; 35 indemnité de fin de contrat ; 36 démission ; 37 accueil d'un agent détaché ; 40 annulation d'une décision ; 41 modification d'une décision ; 50 engagement d'un temporaire ; 51 engagement d'un permanent ; 52 rupture de contrat ; 54 licenciement.
- Les formations, table GRH_FORMATION (date de début, date de fin, et catégorie : PERF perfectionnement, DIPLO diplômante, CONF conférence) ; les inscriptions, table GRH_INSCRIPTION : un agent inscrit à une formation, avec ses frais et son classement avant et après la formation.

## À trancher en priorité

1. Dans quel groupe mettre la 5e catégorie ?
   C'est quoi : chaque agent a une catégorie dans la grille de salaire de la CNSS, de la 1re à la 6e. Les tableaux T58, T62 et T65 regroupent les agents en employés, agents de maîtrise, cadres moyens et cadres supérieurs.
   Dans la base : la catégorie est dans la dernière situation de l'agent au 31/12 (GRH_SITUATION, champ CATEGORIE, codes 01 à 06).
   Aujourd'hui : 01 et 02 employés ; 03 agents de maîtrise ; 04 et 05 cadres moyens ; 06 cadres supérieurs. Avec ce rangement, on retrouve l'annuaire 2024 : 173, 207, 304 et 58 agents pour 177, 208, 311 et 62 publiés.
   Question : la 5e catégorie, ce sont des cadres moyens, comme dans l'annuaire 2024, ou des cadres supérieurs, comme à l'atelier de Kaya en 2017 ?
   Réponse :

2. Les 84 agents engagés en 2019
   C'est quoi : en août et septembre 2019, 84 personnes ont été engagées comme agents permanents (décision 51).
   Dans la base : leurs fiches sont inactives (GRH_PERSONNE, ETAT_EMPLOYE = INACTIF) et n'ont aucune décision de départ. 10 sont marquées radiées dès 2019 (STATUT_EMPLOYE = X).
   Chiffre : l'annuaire les compte dans l'effectif de 2019 à 2023, puis les fait sortir en 2024 (84 « autres sorties »). La base ne sait pas quand ils sont partis : elle ne les compte dans aucune année.
   Question : que sont devenus ces 84 agents, et à quelle date sont-ils partis ?
   Réponse :

3. La date d'un décès
   C'est quoi : pour sortir de l'effectif un agent décédé, il faut la date de son décès.
   Dans la base : la date de décès (GRH_PERSONNE, champ DATE_DECES) n'est plus remplie depuis 2010 : 59 dates pour 435 agents décédés. On prend à la place la décision de liquidation des droits (code 34), qui arrive environ 8 mois après le décès.
   Question : les décès sont-ils notés ailleurs ? Sinon, peut-on saisir la date du décès, ou au moins celle du communiqué nécrologique, comme prévu à Kaya en 2017 ?
   Réponse :

4. Les agents formés en 2024
   C'est quoi : le tableau T62 compte les agents qui ont suivi une formation dans l'année.
   Dans la base : les formations sont dans GRH_FORMATION et les agents inscrits dans GRH_INSCRIPTION. Chaque agent compte une fois.
   Chiffre : l'annuaire publie 480 agents permanents formés en 2024 (132 employés, 142 agents de maîtrise, 118 cadres moyens, 88 cadres supérieurs). La base en donne 409, pour 524 inscriptions, avec deux fois plus de cadres moyens.
   Question : d'où viennent les chiffres de l'annuaire ? D'un autre fichier que l'application du personnel ?
   Réponse :

5. L'agent en disponibilité
   C'est quoi : un agent en disponibilité arrête de travailler pour un temps, à sa demande et sans salaire, puis peut revenir. Un agent mis à disposition travaille pour un autre organisme tout en restant agent de la CNSS.
   Dans la base : la mise en disponibilité est la décision 07 et le retour la décision 18 (remise en activité) ; la mise à disposition est la décision 14.
   Aujourd'hui : ces agents restent dans l'effectif. L'annuaire, lui, fait sortir l'agent en disponibilité, et le fait rentrer à sa remise en activité.
   Question : un agent en disponibilité, ou mis à disposition, au 31/12 compte-t-il dans l'effectif ?
   Réponse :

## Ce qu'on applique, à confirmer

6. Qui compte dans l'effectif
   C'est quoi : un agent permanent est engagé sans limite de durée ; un temporaire, pour une durée limitée. La CNSS accueille aussi des agents de la fonction publique.
   Dans la base : on garde les agents de la CNSS (GRH_PERSONNE, QUALITE = A) dont la dernière situation au 31/12 est « permanent » (GRH_SITUATION, NATURE_EMPLOIE = PERMANENT). Les agents de la fonction publique (QUALITE = B) et les affectés (C) sont à part, les temporaires aussi (point 10).
   Chiffre : 742 permanents au 31/12/2024 ; l'annuaire en publie 758.
   Question : on garde cette règle ?
   Réponse :

7. Présent au 31 décembre
   Dans la base : un agent est présent au 31/12 s'il est entré au plus tard ce jour-là (DATE_EMBAUCHE, à défaut DATE_PRISE_SERVICE) et s'il n'est pas encore parti : sa fiche est active (ETAT_EMPLOYE = ACTIF), ou sa date de départ tombe après le 31/12.
   La date de départ : la première décision de départ (02 retraite, 34 liquidation après décès, 35 fin de contrat, 36 démission, 52 rupture de contrat, 54 licenciement), si elle n'a pas été annulée par une décision 40 ; à défaut, la date de décès.
   Limite : un agent inactif sans aucune décision de départ n'a pas de date de départ ; il n'est compté nulle part (point 25).
   Question : on garde cette règle ?
   Réponse :

8. L'âge et l'ancienneté
   Dans la base : l'âge est le nombre d'années pleines entre la date de naissance (DATE_NAISS) et le 31/12 ; l'ancienneté, entre la date d'embauche (à défaut la prise de service) et le 31/12. Les tranches sont celles de l'Annuaire CIPRES : moins de 20 ans, 20-24 ans… ; moins d'un an, 1-4 ans…
   Chiffre : on retrouve l'annuaire 2024 à quelques agents près dans chaque tranche.
   Attention : pour les prestations, l'âge se calcule autrement (année moins année de naissance).
   Question : on garde ce calcul ?
   Réponse :

9. Le taux d'encadrement (T65)
   C'est quoi : la part des cadres dans le personnel.
   Dans la base : le nombre de permanents des catégories 04, 05 et 06 (point 1), divisé par le nombre de permanents au 31/12.
   Chiffre : 48,8 % en 2024 ; l'annuaire publie 49,2 %.
   Question : on garde ce calcul ? Voir aussi les points 34 et 35.
   Réponse :

10. Les contractuels
   C'est quoi : un contractuel est un agent temporaire, engagé pour une durée limitée.
   Dans la base : un agent dont la dernière situation est « temporaire » (NATURE_EMPLOIE = TEMPORAIRE), dont le dernier engagement (décision 50) date de moins de 18 mois au 31/12, et qui n'a pas de décision de départ depuis. Un contrat renouvelé après une indemnité de fin de contrat (35) n'est pas un départ.
   Chiffre : 46 contractuels fin 2024, comme l'annuaire.
   Question : on garde cette règle ?
   Réponse :

11. Les entrées et les sorties (T60)
   Dans la base : un recrutement est un agent embauché dans l'année (DATE_EMBAUCHE). Une retraite est une décision 02, une démission une 36, un licenciement une 54, une mise en disponibilité une 07, une remise en activité une 18, chacune à sa date d'effet (DATE_VIGUEUR).
   Chiffre : 104 recrutements en 2024, comme l'annuaire.
   Hors du tableau : les changements de poste (affectation, mutation, nomination, promotion), qui ne changent pas l'effectif.
   Question : on garde ces règles ?
   Réponse :

12. Le décès d'un retraité
   Aujourd'hui : un agent mort après son départ à la retraite n'est pas compté dans les décès de T60 : il avait déjà quitté l'effectif.
   Question : d'accord ?
   Réponse :

13. Les agents formés (T62)
   Dans la base : un agent inscrit (GRH_INSCRIPTION) à une formation qui commence dans l'année (GRH_FORMATION, date de début), comme décidé à Kaya en 2017. Il compte une fois, même s'il a suivi plusieurs formations. Toutes les formations comptent, quel que soit leur statut. Sa catégorie est celle de sa situation au 31/12, comme dans T58.
   Question : on garde ces règles ?
   Réponse :

## Questions par tableau

T58 et T61, l'effectif

14. Les agents absents au 31/12
   C'est quoi : un agent détaché travaille pour un temps dans un autre organisme (un ministère, une autre institution) et reste agent de la CNSS. Un agent suspendu est écarté de son poste pour un temps, en général après une faute, en attendant une décision. Un agent sous les drapeaux fait son service militaire.
   Dans la base : dans la dernière situation de l'agent au 31/12 (GRH_SITUATION), le champ STATUT_EMPLOYE vaut B pour un détaché, F pour un suspendu, H pour un agent sous les drapeaux.
   Chiffre : au 31/12/2024, 2 des 742 permanents sont dans une de ces positions, ou en disponibilité, ou mis à disposition.
   Question : ces agents comptent-ils dans l'effectif ? Nous proposons oui : ils font toujours partie du personnel.
   Réponse :

15. Les contractuels par catégorie
   C'est quoi : l'annuaire range ses 46 contractuels de 2024 en 18 employés, 13 agents de maîtrise, 10 cadres moyens et 5 cadres supérieurs.
   Dans la base : avec la catégorie de la situation, comme pour les permanents, on trouve bien 18 employés, mais 8 agents de maîtrise, 18 cadres moyens et aucun cadre supérieur.
   Question : comment classez-vous un contractuel dans une catégorie ?
   Réponse :

16. La durée d'un contrat temporaire
   C'est quoi : pour savoir si un contractuel est encore là au 31/12, il faut connaître la fin de son contrat.
   Dans la base : la date de fin du contrat n'est pas saisie. On suppose donc qu'un contrat dure 18 mois à partir de la décision d'engagement (50).
   Question : combien de temps dure un contrat temporaire ?
   Réponse :

17. L'effectif officiel
   Chiffre : l'annuaire donne l'effectif permanent de 2020 à 2024.
   Question : pouvez-vous nous donner l'effectif officiel au 31/12/2019 et au 31/12/2025, pour comparer ?
   Réponse :

T59, les domaines

18. Les domaines d'activité
   C'est quoi : T59 répartit le personnel en sept domaines : immatriculation, recouvrement, contentieux, prestations, prévention des risques professionnels, action sanitaire et sociale, autres domaines.
   Dans la base : le domaine est dans la situation de l'agent (GRH_SITUATION, champ CODE_DOMAINE_ACT) : IMM, REC, CONT, PRS, PREV, ACT, PIL (pilotage), CPT (comptabilité), AUT.
   Aujourd'hui : chaque code va dans son domaine, ACT en action sanitaire et sociale, et PIL, CPT et AUT en autres domaines.
   Chiffre : fin 2025, le pilotage compte 431 agents actifs sur 879, surtout dans les directions ; le contentieux, un seul.
   Question : ce rangement est-il juste, et le pilotage sert-il de domaine par défaut ?
   Réponse :

T60, les mouvements

19. Les temporaires dans T60
   C'est quoi : T60 suit les entrées et les sorties de personnel sur cinq ans. L'annuaire (tableau 121) n'y met que les permanents.
   Dans la base : un temporaire est souvent réengagé plusieurs fois. En 2025, 84 engagements temporaires (décision 50) ne concernent que 45 nouvelles personnes.
   Question : T60 doit-il compter aussi les temporaires ? Si oui, seulement les nouveaux, pas les renouvellements ?
   Réponse :

20. La fin d'un contrat temporaire
   Dans la base : aucune date de fin de contrat n'est saisie. La seule trace est la décision d'indemnité de fin de contrat (35), prise environ 9 mois après la fin réelle.
   Question : peut-on avoir la vraie date de fin de contrat ?
   Réponse :

21. Les radiations
   C'est quoi : un agent radié est retiré de la liste du personnel.
   Dans la base : 387 personnes ont la position « radié » (STATUT_EMPLOYE = X) : 233 temporaires, 94 permanents et quelques autres. 272 n'ont ni décision ni date.
   Question : que recouvre une radiation, et dans quelle ligne de T60 la mettre ?
   Réponse :

22. Les retraites
   Dans la base : une retraite est une décision 02, à sa date d'effet (DATE_VIGUEUR).
   Chiffre : 39, 31, 29, 30 et 25 départs à la retraite de 2020 à 2024 dans la base ; 43, 31, 26, 32 et 23 dans l'annuaire.
   Question : la date d'effet de la décision 02 est-elle bien le jour du départ ? Et quand une retraite a été modifiée après coup (218 décisions de modification, avec des dates sans rapport), quelle date garder ?
   Réponse :

23. Les détachements
   C'est quoi : un détachement est l'arrivée à la CNSS d'un agent d'une autre administration, pour un temps, ou le départ pour un temps d'un agent de la CNSS dans un autre organisme.
   Chiffre : l'annuaire compte 2 détachements et 2 fins de détachement en 2023, puis 1 détachement en 2024.
   Dans la base : aucune décision 37 (accueil d'un agent détaché) pour un permanent.
   Question : où sont saisis les détachements ?
   Réponse :

24. Entré et parti la même année
   Aujourd'hui : on trouve les entrées et les sorties en comparant l'effectif d'un 31/12 au suivant. Un agent recruté puis parti dans la même année n'y apparaît pas.
   Question : faut-il le compter quand même, une fois en entrée et une fois en sortie ?
   Réponse :

25. Les départs sans date
   Dans la base : 817 personnes sont parties sans aucune date, ni décision de départ, ni date de décès. Elles ne sont comptées dans aucune année.
   Question : pouvez-vous nous donner le registre des sorties des cinq dernières années (date et motif), ou compléter ces dates dans l'application ?
   Réponse :

T62, les formés

26. La formation « avec promotion »
   C'est quoi : T62 sépare les formations « avec promotion », après lesquelles l'agent change de classement, et « sans promotion ».
   Dans la base : l'inscription garde le classement avant et après la formation (GRH_INSCRIPTION, champs CLA_AVANT et CLA_APRES), mais ils ne sont remplis que pour les formations diplômantes. 2 changements en 2024, pour 6 promotions dans l'annuaire.
   Question : où est noté le reclassement après une formation ? Sinon, compte-t-on « avec promotion » toute formation diplômante ?
   Réponse :

27. Les contractuels formés
   Question : T62 compte-t-il aussi les contractuels formés ? Il y en a un en 2024.
   Réponse :

28. Une formation sur deux années
   Aujourd'hui : une formation compte pour l'année où elle commence (Kaya 2017).
   Question : une formation commencée en décembre et finie en janvier compte bien pour l'année de son début ?
   Réponse :

T63, les frais de personnel (pour la direction financière)

29. Les frais de personnel et les charges de fonctionnement
   C'est quoi : T63 compare les frais de personnel (salaires, charges sociales…) à l'ensemble des charges de fonctionnement de la CNSS ; la norme CIPRES est de 85 % au plus.
   Dans la base : la paie n'est pas dans notre copie. Kaya 2017 dit que ces montants viennent de l'application comptable.
   Question : quels comptes de la comptabilité donnent les frais de personnel et les charges de fonctionnement ?
   Réponse :

30. Des montants douteux
   Chiffre : dans les données comptables consultées, les charges de fonctionnement de 2025 valent celles de 2024 plus un montant rond, et celles de la branche des pensions passent de 16,64 Md en 2023 à 1,88 Md en 2024.
   Question : données de test, erreur de saisie ou changement de périmètre ?
   Réponse :

T64, la formation et la masse salariale

31. La dépense de formation
   Dans la base : chaque inscription (GRH_INSCRIPTION) porte sept frais : inscription, scolarité, transport, séjour, dossier, assurance et frais accessoires. On les additionne, un frais vide comptant pour zéro, et on rattache la dépense à l'année où la formation commence.
   Chiffre : 215 M FCFA pour 691 inscriptions en 2025.
   Question : est-ce bien la dépense de formation ?
   Réponse :

32. Le budget de formation
   Dans la base : une formation garde l'année de son budget, pas son montant.
   Question : où trouver le montant du budget de formation ?
   Réponse :

33. La masse salariale
   C'est quoi : la masse salariale est le total des salaires versés ; T64 y rapporte la dépense de formation (norme : entre 1 et 5 %).
   Question : prend-on le compte 651 seul (Kaya 2017) ou les comptes 651 et 652 (Annuaire CIPRES) ?
   Réponse :

T65, l'encadrement

34. Les cadres moyens sont-ils des encadrants ?
   Chiffre : avec les cadres moyens, le taux d'encadrement est de 48,8 % en 2024, comme l'annuaire ; avec les seuls cadres supérieurs, de 7,8 %. L'Annuaire CIPRES donne une norme de 10 % au plus, et Kaya 2017 laissait la notion de cadre à préciser.
   Question : les cadres moyens comptent-ils parmi les encadrants ?
   Réponse :

35. Les actifs ou tout l'effectif ?
   C'est quoi : un agent actif travaille à la CNSS ; un détaché, un suspendu ou un agent en disponibilité est inactif (point 14).
   Chiffre : en 2024, 740 actifs sur 742 permanents : l'écart est faible.
   Question : le taux se calcule-t-il sur les seuls actifs, ou sur tout l'effectif ?
   Réponse :

## Plutôt pour la DSI

36. Les départs mal enregistrés
   Dans la base : quand un agent part, sa situation n'est jamais close ; 691 personnes parties gardent la position « activité normale » (STATUT_EMPLOYE = A). Les positions R, X et Y ne sont presque jamais saisies, et la date d'état de la fiche (DATE_ETAT_EMPLOYE) n'est plus remplie depuis 2020.
   Question : est-ce voulu ? La date de départ est-elle notée ailleurs ?
   Réponse :

37. La mise à jour de 2020
   Dans la base : en 2020, 1 247 états de sortie ont été changés en une seule fois.
   Question : les vraies dates de sortie ont-elles été gardées quelque part ? Elles dateraient une partie des 817 départs sans date.
   Réponse :

38. Une vue des décisions
   Question : pouvez-vous ajouter une vue des décisions avec les agents concernés (V_BI_DECISION_AGENT, à partir de GRH_DECISION et GRH_DECISION_PERSONNE) à la spécification des vues du domaine 4 ? Les entrées et les sorties de T60 en viennent.
   Réponse :

39. Les formations
   Dans la base : toutes les formations sont au statut N, et la table GRH_CAT_FORMATION, lue par le chargement de l'entrepôt, n'existe pas.
   Question : que veut dire le statut N, et où est passée cette table ?
   Réponse :

40. La paie
   Dans la base : la paie n'est pas dans la copie ; les 22 vues de paie sont invalides. La vue V_STAT_RMSCAT donne un montant par année et par catégorie.
   Question : peut-on avoir la paie ? V_STAT_RMSCAT est-elle la masse salariale ?
   Réponse :

41. Les autres codes de catégorie
   Dans la base : à côté des catégories 01 à 06, on trouve HC et les codes I à V et 001 à 005, pour aucun permanent en 2024.
   Question : I à V et 001 à 005 sont-ils les catégories de la fonction publique ? HC veut-il bien dire hors catégorie ?
   Réponse :

42. Le registre des mouvements
   Dans la base : GRH_MOUVEMENT ne contient que des changements de poste. On y lit DIS = mise à disposition, REC = reclassement, RET = rétrogradation ; 1 621 mouvements sont à E dans le champ de validation, et 91 % ne sont jamais validés ; le code NOM (nomination) manque dans la liste de référence.
   Question : cette lecture est-elle juste, et que veulent dire E et A ?
   Réponse :

43. Des dates impossibles
   Dans la base : des situations sont datées de l'an 212, 2201 ou 3015, et des décisions de l'an 5 ou des années 200 à 209.
   Question : peut-on les corriger ?
   Réponse :

44. La copie de la base
   Question : la copie date du 18/02/2026. Quand sera-t-elle rafraîchie ?
   Réponse :

## Tous les tableaux, un par un

- T58, effectif par catégorie, âge et sexe, permanents puis contractuels : points 1, 2, 5 à 8, 10, 14 à 17.
- T59, effectif par sexe et domaine d'activité : points 6, 7, 14, 18.
- T60, mouvement du personnel sur cinq ans : points 2, 3, 5, 7, 11, 12, 19 à 25.
- T61, effectif par ancienneté et sexe : points 6 à 8, 14.
- T62, personnel formé par catégorie, sexe et finalité : points 1, 4, 13, 26 à 28.
- T63, ratio des frais de personnel sur les charges de fonctionnement : points 29, 30, 40.
- T64, ratios des dépenses de formation sur la masse salariale et sur le budget : points 31 à 33, 40.
- T65, taux d'encadrement sur cinq ans : points 1, 2, 6, 7, 9, 34, 35.
