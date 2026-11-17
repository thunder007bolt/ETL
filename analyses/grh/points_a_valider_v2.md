# GRH : règles et questions à valider (version 2)

Mis à jour le 17/11/2026. Base consultée : copie de l'application de gestion du personnel (schéma USER_GRH) du 18/02/2026, comparée à l'annuaire statistique CNSS 2024 (partie IX, tableaux 119 à 126) et au recueil des besoins v7 de la CNSS (partie RH, lignes 1 à 8).

Cette version remplace celle du 01/11, qui avait été écrite avant la lecture du recueil des besoins v7 et avant la réunion du 03/11 avec la CNSS. Chaque point garde son numéro : les autres documents y renvoient. Les points déjà tranchés sont regroupés à la fin, avec la réponse et sa source ; les points 29 et 31 sont reformulés, car le recueil a donné une partie de la réponse ; les points 45 à 63 sont nouveaux. Chaque point dit de quoi on parle, ce qu'il permet de faire dans le tableau (Objectif), où on le voit dans la base, ce qu'on fait aujourd'hui et à qui poser la question, puis pose une seule question. Aucune question ne bloque : en attendant la réponse, on applique ce que dit la ligne « Aujourd'hui ». S'il n'y a pas le temps de tout voir : les points à trancher en priorité d'abord.

## Pour lire ce document

- La fiche de l'agent, table GRH_PERSONNE : une ligne par personne. On y trouve le sexe (SEXE : M ou F), la date de naissance (DATE_NAISS), la date d'embauche (DATE_EMBAUCHE), la date de prise de service (DATE_PRISE_SERVICE), la date de décès (DATE_DECES), l'état (ETAT_EMPLOYE : ACTIF, INACTIF, ou vide pour une embauche récente) et la qualité (QUALITE : A agent de la CNSS, B agent de la fonction publique en poste à la CNSS, C agent affecté).
- La situation de l'agent, table GRH_SITUATION : une ligne à chaque changement dans sa carrière, avec sa date de début (DATE_DEBUT). Pour connaître un agent à une date, on prend sa dernière situation commencée ce jour-là ou avant. Elle donne la nature de l'emploi (NATURE_EMPLOIE : PERMANENT ou TEMPORAIRE), la catégorie de la grille de salaire (CATEGORIE : 01 à 06, ou HC), l'unité où il travaille (UA_CODE) et sa position (STATUT_EMPLOYE : A activité normale, B détaché, D mis à disposition, E en disponibilité, F suspendu, H sous les drapeaux, R retraité, X radié, Y décédé). La position n'est pas datée : l'application la modifie sur la situation en cours au lieu d'ouvrir une nouvelle situation.
- Les décisions, table GRH_DECISION : chaque décision a un code (CDEC_CODE) et une date d'effet (DATE_VIGUEUR) ; une décision qui vise plusieurs agents les liste dans GRH_DECISION_PERSONNE. Codes utiles : 02 retraite ; 07 mise en disponibilité ; 14 mise à disposition ; 18 remise en activité ; 34 liquidation des droits après un décès ; 35 indemnité de fin de contrat ; 36 démission ; 37 accueil d'un agent détaché ; 40 annulation d'une décision ; 50 engagement d'un temporaire ; 51 engagement d'un permanent ; 52 rupture de contrat ; 54 licenciement.
- Les unités et leurs domaines : chaque unité (direction, service, agence, bureau, guichet) a un code (UA_CODE). Le domaine d'activité d'une unité se lit dans le fichier « unité → domaine » préparé par la BI (correspondance_unite_domaine.xlsx, 432 unités, dont 157 en vigueur), que la CNSS remplit avec les 5 domaines du recueil des besoins.
- Les mouvements : la table des types de mouvement de l'application (GRH_TYPE_MOUVEMENT) donne les colonnes de T60 et leur détail : RECRU recrutement, REM remise en activité, DETAC détaché, AUTRE_ENT autres entrées ; RET retraite, DEC décès, LIC licenciement, DEM démission, DISP disponibilité, MISE_DIS mise à disposition, FIN_DETACH fin de détachement, FIN_CTRAT fin de contrat, AUTRE_SORT autres sorties.
- Les formations, table GRH_FORMATION (date de début, date de fin, et catégorie : PERF perfectionnement, DIPLO diplômante, CONF conférence) ; les inscriptions, table GRH_INSCRIPTION : un agent inscrit à une formation, avec ses frais et son classement avant et après la formation (CLA_AVANT, CLA_APRES).
- La comptabilité : les montants de T63 et T64 viennent d'Oracle GL, la comptabilité de la CNSS, par une vue que la DSI doit alimenter (V_BI_BALANCE_COMPTABLE) : un montant par exercice, compte, gestion (G générale, S sociale) et section analytique, réalisé et budgété. La comptabilité de la copie de la base n'est pas lue : la CNSS l'a dite non valide le 07/11.
- Les mots du document : un agent permanent est engagé sans limite de durée ; un temporaire, ou contractuel, pour une durée limitée. La date d'arrêté est le jour où l'on compte l'effectif, le 31 décembre pour l'annuaire. Un agent en disponibilité arrête de travailler pour un temps, à sa demande et sans salaire ; un agent mis à disposition travaille pour un autre organisme en restant agent de la CNSS ; un agent détaché travaille pour un temps dans une autre administration ; un agent suspendu est écarté de son poste en attendant une décision ; un agent sous les drapeaux fait son service militaire ; un agent radié est retiré de la liste du personnel.
- Les directions à qui l'on pose les questions : DRH, la Direction des ressources humaines ; DFC, la Direction financière et comptable ; DSI, la Direction des systèmes d'information ; DPC, la Direction de la prospective et de la coopération, qui fait l'annuaire.
- Les documents cités : le recueil des besoins v7 de janvier 2026, partie RH, dont on cite les lignes (« RH ligne 1 » pour T58, jusqu'à « RH ligne 8 » pour T65) ; la réunion du 03/11/2026 avec la CNSS ; l'atelier de Kaya en 2017, où la CNSS a validé les tableaux CIPRES ; l'Annuaire CIPRES, le modèle des tableaux T58 à T65 (leur maquette, p. 17 et 50 à 52) ; l'annuaire statistique CNSS 2024, qui sert seulement à comparer les chiffres.

## À trancher en priorité

2. Les 84 agents engagés en 2019
   C'est quoi : en août et septembre 2019, 84 personnes ont été engagées comme agents permanents (décision 51).
   Objectif : dans T58, T60 et T65, l'effectif de chaque année doit compter les agents vraiment présents. Il permet de suivre l'effectif sur cinq ans et le taux d'encadrement (Annuaire CIPRES, p. 17). La réponse permet de dater le départ de ces 84 agents et de retomber sur l'annuaire de 2019 à 2024.
   Dans la base : leurs fiches sont inactives (GRH_PERSONNE, ETAT_EMPLOYE = INACTIF) et n'ont aucune décision de départ. 10 sont marquées radiées dès 2019 (STATUT_EMPLOYE = X).
   Aujourd'hui : faute de date de départ, ils ne sont comptés dans aucune année. Seul ce qui est saisi dans l'application compte (fiche des règles, règle 3) : une liste à côté ne serait pas lue.
   Chiffre : l'annuaire les compte dans l'effectif de 2019 à 2023, puis les fait sortir en 2024 (84 « autres sorties »).
   À qui : DRH
   Question : que sont devenus ces 84 agents, et pouvez-vous saisir leur départ (décision, date et motif) dans l'application ?
   Réponse :

3. La date d'un décès
   C'est quoi : pour sortir de l'effectif un agent décédé, et le compter dans les décès de T60, il faut la date de son décès.
   Objectif : dans T60, la colonne des décès doit montrer chaque année les agents morts en activité, et T58 doit les retirer de l'effectif à la bonne date. Elle permet de suivre les sorties du personnel sur cinq ans (Annuaire CIPRES, p. 51). La réponse permet de dater les décès au jour près au lieu de 8 mois plus tard.
   Dans la base : la date de décès (GRH_PERSONNE, DATE_DECES) n'est plus remplie depuis 2010 : 59 dates pour 435 agents décédés.
   Aujourd'hui : on prend la décision de liquidation des droits (code 34), qui arrive environ 8 mois après le décès. Kaya 2017 prévoyait de prendre le communiqué nécrologique en attendant l'acte de décès.
   À qui : DRH
   Question : peut-on saisir la date du décès dans l'application, ou au moins celle du communiqué nécrologique ?
   Réponse :

4. Les agents formés en 2024
   C'est quoi : T62 compte les agents qui ont suivi une formation dans l'année.
   Objectif : dans T62, chaque case doit montrer les agents formés de l'année, par catégorie, par sexe et par finalité. Elle permet de suivre l'effort de formation du personnel (Annuaire CIPRES, p. 17). La réponse permet de savoir pourquoi la base ne retombe pas sur l'annuaire, et de prendre la même source.
   Dans la base : les formations sont dans GRH_FORMATION et les agents inscrits dans GRH_INSCRIPTION. Chaque agent compte une fois.
   Aujourd'hui : seule l'application du personnel est lue (fiche des règles, règle 3).
   Chiffre : l'annuaire publie 480 agents permanents formés en 2024 (132 employés, 142 agents de maîtrise, 118 cadres moyens, 88 cadres supérieurs). La base en donne 409, pour 524 inscriptions, avec deux fois plus de cadres moyens.
   À qui : DRH
   Question : d'où viennent les chiffres de l'annuaire ? Si c'est un autre fichier que l'application, pouvez-vous nous le donner, avec la liste de ses colonnes ?
   Réponse :

20. La fin d'un contrat temporaire
   C'est quoi : pour savoir si un contractuel est encore là à une date, il faut connaître la fin de son contrat.
   Objectif : dans T58 (tableau des temporaires) et T60, chaque contractuel doit sortir de l'effectif à la fin de son contrat. Cela permet de compter les temporaires présents à chaque date (recueil v7, RH ligne 1 : par type d'employé). La réponse permet de ne pas garder dans l'effectif des contractuels déjà partis.
   Dans la base : aucune date de fin de contrat n'est saisie. La seule trace est la décision d'indemnité de fin de contrat (35), prise environ 9 mois après la fin réelle.
   Aujourd'hui : un contrat finit au premier départ saisi, sinon la veille du contrat suivant ; sans l'un ni l'autre, l'agent reste compté (réunion du 03/11 : la BI ne fixe pas de durée de contrat).
   À qui : DRH
   Question : peut-on saisir la vraie date de fin de chaque contrat temporaire ?
   Réponse :

25. Les départs sans date
   C'est quoi : un agent parti sans date de départ ne peut être retiré de l'effectif à aucune année.
   Objectif : dans T58 et T60, chaque départ doit tomber dans son année. Cela permet de suivre l'effectif et les sorties sur cinq ans (Annuaire CIPRES, p. 51). La réponse permet de dater les départs des années passées.
   Dans la base : 817 personnes sont parties sans aucune date : ni décision de départ, ni date de décès (GRH_PERSONNE, ETAT_EMPLOYE = INACTIF).
   Aujourd'hui : elles ne sont comptées dans aucune année. Seul ce qui est saisi dans l'application compte (fiche des règles, règle 3) : un registre tenu à côté ne serait pas lu.
   À qui : DRH
   Question : pouvez-vous saisir dans l'application la date et le motif des départs des cinq dernières années (décision de départ, ou date de décès) ?
   Réponse :

48. Le domaine de chaque unité
   C'est quoi : T59 répartit le personnel entre les 5 domaines du recueil des besoins : recouvrement, prestations, prévention des risques professionnels, action sanitaire et sociale, gestion administrative et financière.
   Objectif : dans T59, chaque agent doit tomber dans le domaine de son unité. Cela permet de voir combien d'agents travaillent sur chaque mission de la CNSS (recueil v7, RH ligne 2). La réponse permet de produire T59 : sans elle, tout T59 est en « Domaine non renseigné ».
   Dans la base : l'unité de l'agent (GRH_SITUATION, UA_CODE). Le code de domaine de la situation (CODE_DOMAINE_ACT) n'est pas fiable : les agents d'une même unité ont des codes différents dans 66 unités sur 139.
   Aujourd'hui : la BI a préparé le fichier « unité → domaine » (432 unités) avec une proposition par unité ; 132 unités restent « À décider » (agences, bureaux, guichets, directions régionales).
   À qui : DRH
   Question : pouvez-vous remplir le domaine de chaque unité dans ce fichier, d'abord les 132 lignes « À décider » ?
   Réponse :

57. Les années passées
   C'est quoi : T60, T63, T64 et T65 montrent cinq années. Pour l'année N, la photo de la base de fin janvier N+1 fait foi (fiche des règles, règle 2), mais aucune photo n'existe pour 2021 à 2024.
   Objectif : dans T60, T63, T64 et T65, les années d'avant doivent montrer des chiffres stables et admis par la CNSS. Elles permettent de suivre l'évolution sur cinq ans (Annuaire CIPRES, p. 17). La réponse permet de choisir entre reprendre les chiffres publiés et les recalculer.
   Dans la base : une seule copie, du 18/02/2026. Recalculées sur elle, les années passées s'écartent de l'annuaire, par exemple de 84 agents de 2019 à 2023 (point 2).
   Aujourd'hui : on recalcule toutes les années sur la copie du moment. Pour les prestations, les années d'avant reprennent les chiffres de leur photo officielle (règle 77).
   À qui : DRH, avec la DPC
   Question : pour 2021 à 2024, faut-il reprendre les chiffres publiés dans l'annuaire, ou les recalculer sur la base d'aujourd'hui ?
   Réponse :

## Ce qu'on applique, à confirmer

12. Le décès d'un retraité
   C'est quoi : un agent peut mourir après son départ à la retraite.
   Objectif : dans T60, la colonne des décès doit montrer les agents qui quittent l'effectif par décès. Elle permet de distinguer les départs à la retraite des décès en activité (Annuaire CIPRES, p. 51). La réponse permet de ne compter chaque départ qu'une fois.
   Aujourd'hui : un agent mort après son départ à la retraite n'est pas compté dans les décès de T60 : il avait déjà quitté l'effectif.
   À qui : DRH
   Question : d'accord ?
   Réponse :

13. Les agents formés (T62)
   C'est quoi : un agent formé est un agent inscrit à une formation de l'année.
   Objectif : dans T62, chaque agent formé doit compter une fois dans l'année, dans sa catégorie et son sexe. Cela permet de suivre la part du personnel formé (Annuaire CIPRES, p. 17 ; Kaya 2017). La réponse permet de fixer ce qu'on compte, avant de chercher les écarts avec l'annuaire (point 4).
   Dans la base : un agent inscrit (GRH_INSCRIPTION) à une formation qui commence dans l'année (GRH_FORMATION, date de début), comme décidé à Kaya en 2017.
   Aujourd'hui : il compte une fois, même s'il a suivi plusieurs formations. Toutes les formations comptent, quel que soit leur statut. Sa catégorie est celle de sa situation au 31/12, comme dans T58.
   À qui : DRH
   Question : on garde ces règles ?
   Réponse :

14. Les agents absents à la date d'arrêté
   C'est quoi : un agent détaché, mis à disposition, suspendu ou sous les drapeaux ne travaille pas à la CNSS pour un temps, mais reste agent de la CNSS.
   Objectif : dans T58, T59, T61 et T65, l'effectif doit compter les agents qui font partie du personnel à la date d'arrêté. Il permet de suivre l'effectif et le taux d'encadrement (Annuaire CIPRES, p. 17). La réponse permet de savoir si ces absences font sortir l'agent de l'effectif, comme la disponibilité (point 5).
   Dans la base : la position de la situation (GRH_SITUATION, STATUT_EMPLOYE : B, D, F, H) n'est pas datée ; seule la mise à disposition a une décision (14), sans décision de retour.
   Aujourd'hui : ces agents restent dans l'effectif. Au 31/12/2024, 1 seul permanent est dans une de ces positions.
   À qui : DRH
   Question : un agent détaché, mis à disposition, suspendu ou sous les drapeaux à la date d'arrêté compte-t-il dans l'effectif ?
   Réponse :

45. La fiche sans état
   C'est quoi : quand un agent vient d'être embauché, sa fiche n'a pas encore d'état : elle n'est ni active ni inactive.
   Objectif : dans T58 et T60, un agent embauché avant la date d'arrêté doit être compté, même si sa fiche n'est pas encore passée à « actif ». Cela permet de ne pas perdre les recrutements de fin d'année (recueil v7, RH ligne 1). La réponse permet de fixer la règle pour ces fiches.
   Dans la base : GRH_PERSONNE, ETAT_EMPLOYE vide : 67 fiches, embauchées jusqu'au 28/01/2026.
   Aujourd'hui : une fiche sans état compte comme encore là (depuis le 05/11).
   À qui : DRH
   Question : une fiche sans état est-elle bien celle d'un agent présent ?
   Réponse :

46. Les agents inactifs
   C'est quoi : un agent compté dans l'effectif peut être inactif, c'est-à-dire ne pas travailler à la date d'arrêté.
   Objectif : dans T58, la colonne des agents inactifs doit montrer les agents comptés qui ne travaillent pas. Elle permet de distinguer le personnel en poste du personnel compté (Proposition BI). La réponse permet de savoir où lire ces absences.
   Dans la base : la position de la situation (STATUT_EMPLOYE) n'est pas datée ; les décisions de mise en disponibilité (07) et de remise en activité (18) le sont.
   Aujourd'hui : un agent est inactif s'il est en disponibilité selon les décisions 07 et 18. Comme la disponibilité fait sortir de l'effectif (point 5), la colonne vaut 0.
   À qui : DRH
   Question : peut-on lire les absences dans les décisions, plutôt que dans la position de la situation ?
   Réponse :

47. La catégorie HC
   C'est quoi : à côté des catégories 01 à 06 de la grille de salaire, quelques agents ont la catégorie HC, sans doute « hors catégorie » (point 41).
   Objectif : dans T58, T62 et T65, chaque agent doit tomber dans un groupe de catégories. Cela permet de calculer le taux d'encadrement (recueil v7, RH ligne 1). La réponse permet de ranger ces agents sans les perdre.
   Dans la base : GRH_SITUATION, CATEGORIE = HC.
   Aujourd'hui : HC est rangé en cadres supérieurs. Avec ce rangement, le tableau 126 de l'annuaire CNSS 2024 retombe sur le tableau 119.
   À qui : DRH
   Question : un agent hors catégorie est-il un cadre supérieur ?
   Réponse :

## Questions par tableau

Tous les tableaux

56. L'agence, le bureau et la direction régionale d'une unité
   C'est quoi : le recueil des besoins demande chaque tableau par direction, agence et bureau (RH lignes 1 à 7).
   Objectif : dans T58 à T65, chaque chiffre doit pouvoir se lire par direction, par direction régionale, par agence et par bureau. Cela permet à chaque responsable de suivre son personnel (recueil v7, RH lignes 1 à 7). La réponse permet de rattacher chaque unité à la bonne agence ou au bon bureau.
   Dans la base : les 157 unités en vigueur n'ont que deux natures, « Direction » (21) et « Service » (136) ; chacune a son unité de rattachement (UA_CODE_SUP) et un code de région (REGION : 000, puis 100 à 500). Les 16 agences, 17 bureaux et 5 directions régionales ne se reconnaissent qu'à leur libellé.
   Aujourd'hui : une unité est une agence ou un bureau si son libellé commence par « Agence » ou « Bureau » ; ses services suivent leur rattachement ; la direction régionale vient du code de région.
   À qui : DRH, avec la DSI
   Question : cette lecture est-elle juste, ou l'application garde-t-elle ailleurs le type de chaque unité ?
   Réponse :

T58 et T61, l'effectif

15. Les contractuels par catégorie
   C'est quoi : l'annuaire range ses 46 contractuels de 2024 en 18 employés, 13 agents de maîtrise, 10 cadres moyens et 5 cadres supérieurs.
   Objectif : dans T58, le tableau des temporaires doit ranger chaque contractuel dans sa catégorie. Il permet de suivre les contractuels comme les permanents (recueil v7, RH ligne 1). La réponse permet de retomber sur l'annuaire.
   Dans la base : avec la catégorie de la situation (GRH_SITUATION, CATEGORIE), comme pour les permanents, on trouve bien 18 employés, mais 8 agents de maîtrise, 18 cadres moyens et aucun cadre supérieur.
   À qui : DRH
   Question : comment classez-vous un contractuel dans une catégorie ?
   Réponse :

17. L'effectif officiel
   C'est quoi : l'annuaire donne l'effectif permanent de 2020 à 2024.
   Objectif : dans T58, T60 et T65, l'effectif de la BI doit pouvoir se comparer aux chiffres officiels. Cela permet de vérifier les règles sur plus d'années (Proposition BI). La réponse permet de contrôler le début de T60 en 2020 et la fin en 2025.
   À qui : DRH
   Question : pouvez-vous nous donner l'effectif officiel au 31/12/2019 et au 31/12/2025, pour comparer ?
   Réponse :

61. Les temporaires sans engagement
   C'est quoi : un temporaire est compté pendant ses contrats, qui commencent à la décision d'engagement (50).
   Objectif : dans T58 (tableau des temporaires), chaque contractuel présent doit être compté. Cela permet de suivre les temporaires comme les permanents (recueil v7, RH ligne 1). La réponse permet de savoir si ces agents doivent être comptés.
   Dans la base : 234 temporaires n'ont aucune décision 50 (GRH_DECISION) ; presque tous sont anciens.
   Aujourd'hui : sans date de début de contrat, ils ne sont jamais comptés.
   À qui : DRH
   Question : ces temporaires sont-ils encore sous contrat ? Si oui, pouvez-vous saisir leur décision d'engagement ?
   Réponse :

62. Les agents d'une unité fermée
   C'est quoi : quand une unité est fermée, ses agents devraient être rattachés à une unité en vigueur.
   Objectif : dans T58 à T61, chaque agent doit tomber dans son unité actuelle. Cela permet de le ranger dans la bonne direction, la bonne agence et le bon domaine (recueil v7, RH lignes 1 et 2). La réponse permet de corriger ces situations à la source.
   Dans la base : 10 agents actifs ont une dernière situation (GRH_SITUATION, UA_CODE) dans une unité fermée.
   Aujourd'hui : ils sont comptés, avec l'unité et le domaine de l'ancienne unité.
   À qui : DRH
   Question : pouvez-vous mettre à jour l'unité de ces 10 agents dans l'application ?
   Réponse :

T59, les domaines

58. T59 au jour près
   C'est quoi : le recueil demande T59 à la journée, au mois, au trimestre et à l'année (RH ligne 2).
   Objectif : dans T59, l'effectif par domaine doit se lire aux dates utiles à la CNSS. Cela permet de suivre la répartition du personnel en cours d'année (recueil v7, RH ligne 2). La réponse permet de savoir si les fins de mois suffisent.
   Aujourd'hui : l'effectif se calcule au 31 décembre, et à chaque fin de mois en option.
   À qui : DRH
   Question : l'effectif par domaine à chaque fin de mois suffit-il, ou en faut-il un chaque jour ?
   Réponse :

T60, les mouvements

21. Les radiations
   C'est quoi : un agent radié est retiré de la liste du personnel.
   Objectif : dans T60, chaque sortie doit tomber dans sa colonne : retraite, décès ou autre. Cela permet de suivre les motifs de départ (recueil v7, RH ligne 3 : détailler les autres). La réponse permet de ranger les radiations.
   Dans la base : 387 personnes ont la position « radié » (STATUT_EMPLOYE = X) : 233 temporaires, 94 permanents et quelques autres. 272 n'ont ni décision ni date.
   À qui : DRH
   Question : que recouvre une radiation, et dans quelle sortie de T60 la mettre ?
   Réponse :

22. Les retraites
   C'est quoi : la colonne des retraites compte les agents admis à la retraite dans l'année.
   Objectif : dans T60, chaque retraite doit tomber dans l'année du départ. Elle permet de suivre les départs à la retraite sur cinq ans (Annuaire CIPRES, p. 51). La réponse permet de prendre la bonne date quand une retraite a été modifiée.
   Dans la base : une retraite est une décision 02, à sa date d'effet (DATE_VIGUEUR).
   Chiffre : 39, 31, 29, 30 et 25 départs à la retraite de 2020 à 2024 dans la base ; 43, 31, 26, 32 et 23 dans l'annuaire.
   À qui : DRH
   Question : la date d'effet de la décision 02 est-elle le jour du départ, et quelle date garder quand la retraite a été modifiée après coup (218 décisions de modification) ?
   Réponse :

23. Les détachements
   C'est quoi : un détachement est l'arrivée pour un temps d'un agent d'une autre administration, ou le départ pour un temps d'un agent de la CNSS.
   Objectif : dans T60, les détachements et leurs fins doivent apparaître dans les autres entrées et sorties. Ils permettent de détailler les « autres », comme le demande le recueil (RH ligne 3). La réponse permet de les compter comme l'annuaire.
   Dans la base : aucune décision 37 (accueil d'un agent détaché) pour un permanent.
   Chiffre : l'annuaire compte 2 détachements et 2 fins de détachement en 2023, puis 1 détachement en 2024.
   À qui : DRH
   Question : où sont saisis les détachements et leurs fins ?
   Réponse :

24. Entré et parti la même année
   C'est quoi : un agent peut être recruté puis partir avant le 31 décembre.
   Objectif : dans T60, les entrées et les sorties doivent compter tous les mouvements de l'année. Elles permettent de suivre les recrutements et les départs (Annuaire CIPRES, p. 51). La réponse permet de savoir si ces passages courts doivent apparaître.
   Aujourd'hui : on trouve les entrées et les sorties en comparant l'effectif d'un 31/12 au suivant : un agent entré et parti dans la même année n'apparaît pas.
   À qui : DRH
   Question : faut-il le compter quand même, une fois en entrée et une fois en sortie ?
   Réponse :

49. Le détail des « autres »
   C'est quoi : la maquette de T60 n'a que deux colonnes « autres », une pour les entrées, une pour les sorties. Le recueil des besoins demande de les détailler pour la CNSS.
   Objectif : dans T60, les autres entrées et sorties doivent se détailler par motif. Cela permet de savoir pourquoi le personnel bouge (recueil v7, RH ligne 3). La réponse permet de valider la liste des motifs.
   Dans la base : la table des types de mouvement de l'application (GRH_TYPE_MOUVEMENT) : autres entrées REM, DETAC, AUTRE_ENT ; autres sorties LIC, DEM, DISP, MISE_DIS, FIN_DETACH, FIN_CTRAT, AUTRE_SORT. C'est la liste du tableau 121 de l'annuaire CNSS 2024.
   Aujourd'hui : on reprend cette liste telle quelle.
   À qui : DRH
   Question : cette liste des autres entrées et sorties vous convient-elle ?
   Réponse :

50. La date d'entrée
   C'est quoi : un agent a une date d'embauche et une date de prise de service ; elles peuvent différer.
   Objectif : dans T58 et T60, chaque recrutement doit tomber dans l'année où l'agent entre. Cela permet de suivre les recrutements (Annuaire CIPRES, p. 51). La réponse permet d'utiliser la même date partout, ancienneté comprise.
   Dans la base : GRH_PERSONNE, DATE_EMBAUCHE et DATE_PRISE_SERVICE.
   Aujourd'hui : l'entrée est la date d'embauche, sinon la prise de service. L'ancienneté (T61) part de la prise de service, sinon de l'embauche (réunion du 03/11).
   À qui : DRH
   Question : la date d'entrée d'un agent est-elle sa prise de service, comme pour l'ancienneté ?
   Réponse :

51. La mise à disposition
   C'est quoi : un agent mis à disposition travaille pour un autre organisme en restant agent de la CNSS.
   Objectif : dans T58 et T60, la mise à disposition doit être traitée comme la CNSS la voit : une sortie ou non. Cela permet de compter l'effectif comme l'annuaire (recueil v7, RH ligne 3). La réponse permet de remplir, ou non, la sortie MISE_DIS.
   Dans la base : décision 14, sans décision de retour.
   Aujourd'hui : ce n'est pas une sortie ; le tableau 121 de l'annuaire CNSS 2024 n'a pas de colonne pour elle.
   À qui : DRH
   Question : une mise à disposition fait-elle sortir l'agent de l'effectif ?
   Réponse :

52. Les mutations, pour une direction ou une agence
   C'est quoi : le recueil demande T60 par direction, agence et bureau. Pour une agence, un agent muté d'une autre agence est une arrivée.
   Objectif : dans T60 découpé par unité, les mouvements doivent expliquer l'effectif de chaque unité. Cela permet de suivre le personnel par direction et agence (recueil v7, RH ligne 3). La réponse permet de savoir si on montre les arrivées et départs par mutation.
   Aujourd'hui : non : les mutations, affectations et nominations restent hors de T60 ; l'effectif de chaque unité vient de T58.
   À qui : DRH
   Question : pour une direction ou une agence, faut-il montrer les arrivées et les départs par mutation ?
   Réponse :

T62, les formés

26. La formation « avec promotion »
   C'est quoi : T62 sépare les formations « avec promotion », après lesquelles l'agent change de classement, et « sans promotion ».
   Objectif : dans T62, la finalité de chaque formation doit être juste. Elle permet de suivre les formations qui font progresser les agents (Annuaire CIPRES, p. 51). La réponse permet de savoir où lire le reclassement.
   Dans la base : l'inscription garde le classement avant et après la formation (GRH_INSCRIPTION, CLA_AVANT et CLA_APRES), rempli seulement pour les formations diplômantes.
   Chiffre : 2 changements de classement en 2024, pour 6 promotions dans l'annuaire.
   À qui : DRH
   Question : où est noté le reclassement après une formation ? Sinon, compte-t-on « avec promotion » toute formation diplômante ?
   Réponse :

27. Les contractuels formés
   C'est quoi : un contractuel peut aussi suivre une formation.
   Objectif : dans T62, le périmètre des formés doit être le même que celui de l'annuaire. Cela permet de comparer l'effort de formation d'une année à l'autre (Annuaire CIPRES, p. 17). La réponse permet de les compter, ou non.
   Aujourd'hui : seuls les permanents sont comptés. Il y a un contractuel formé en 2024.
   À qui : DRH
   Question : T62 compte-t-il aussi les contractuels formés ?
   Réponse :

28. Une formation sur deux années
   C'est quoi : une formation peut commencer en décembre et finir en janvier.
   Objectif : dans T62, chaque formation doit compter dans une seule année. Cela permet de ne pas compter deux fois un agent (Kaya 2017). La réponse permet de confirmer l'année retenue.
   Aujourd'hui : une formation compte pour l'année où elle commence (Kaya 2017).
   À qui : DRH
   Question : une formation commencée en décembre et finie en janvier compte bien pour l'année de son début ?
   Réponse :

60. La formation longue
   C'est quoi : une formation diplômante peut durer plus d'un an. La maquette de T62 compte les travailleurs « en formation ou formés ». Ce point est resté ouvert à la réunion du 03/11.
   Objectif : dans T62, un agent en formation longue doit être compté comme la CNSS le veut. Cela permet de suivre les agents en formation chaque année (Annuaire CIPRES, p. 51). La réponse permet de le compter une seule fois, ou chaque année.
   Dans la base : GRH_FORMATION, date de début et date de fin.
   Aujourd'hui : il compte seulement l'année où sa formation commence.
   À qui : DRH
   Question : un agent en formation sur deux ans compte-t-il aussi la deuxième année ?
   Réponse :

53. La formation des personnes externes
   C'est quoi : la CNSS forme aussi des personnes qui ne sont pas ses agents (stagiaires, partenaires).
   Objectif : à côté de T62, un tableau doit montrer les personnes externes formées. Il permet de suivre toute l'action de formation de la CNSS (recueil v7, RH ligne 5 : « faire un autre tableau pour la formation des personnes externes », source fichiers Excel, voir le bilan social). La réponse permet de charger ce fichier.
   Dans la base : rien : ces personnes ne sont pas dans l'application du personnel.
   Aujourd'hui : ce tableau n'est pas produit.
   À qui : DRH
   Question : qui tient le fichier Excel des personnes externes formées, et quelles colonnes a-t-il ?
   Réponse :

T63 et T64, les ratios (pour la direction financière)

29. La gestion des charges de fonctionnement
   C'est quoi : T63 compare les frais de personnel aux charges de fonctionnement ; la norme CIPRES est de 85 % au plus. Le recueil donne les comptes : frais de personnel, comptes 65 des gestions G et S (RH ligne 6) ; charges de fonctionnement, comptes 61 à 66 (Finance ligne 1).
   Objectif : dans T63, le numérateur et le dénominateur doivent porter sur le même périmètre. Cela permet de comparer le ratio à la norme de 85 % (Annuaire CIPRES, p. 17). La réponse permet de prendre les mêmes gestions des deux côtés.
   Dans la base : vue V_BI_BALANCE_COMPTABLE, à alimenter (point 55) : comptes, gestions G et S.
   Aujourd'hui : frais de personnel sur les gestions G et S ; charges de fonctionnement sur la gestion G seule.
   À qui : DFC
   Question : les charges de fonctionnement se prennent-elles sur la gestion G seule, ou sur G et S comme les frais de personnel ?
   Réponse :

31. Les comptes de la formation
   C'est quoi : T64 rapporte la dépense de formation à la masse salariale (norme : de 1 à 5 %). Le recueil dit « dépense de formation : 63, 86 et 87 de la gestion G et S » (RH ligne 6).
   Objectif : dans T64, la dépense de formation doit compter tout ce que la CNSS paie pour former ses agents, et rien d'autre. Elle permet de comparer le ratio à la norme (Annuaire CIPRES, p. 17). La réponse permet de prendre les bons comptes.
   Dans la base : vue V_BI_BALANCE_COMPTABLE (point 55). Les frais saisis sur les inscriptions (GRH_INSCRIPTION) ne servent plus.
   Aujourd'hui : comptes commençant par 63, 86 et 87. Pris à la lettre, le compte 63 compte tous les services consommés, pas seulement la formation.
   À qui : DFC
   Question : « 63, 86 et 87 » veut-il dire tous ces comptes, ou des sous-comptes comme 6386 et 6387 ?
   Réponse :

32. Le budget de formation
   C'est quoi : T64 donne aussi la dépense de formation rapportée au budget.
   Objectif : dans T64, le second ratio doit rapporter la formation au bon budget. Il permet de voir quelle part du budget va à la formation (Annuaire CIPRES, p. 52). La réponse permet de choisir les comptes et la version du budget.
   Dans la base : une formation (GRH_FORMATION) garde l'année de son budget, pas son montant. Le budget est dans Oracle GL (vue V_BI_BALANCE_COMPTABLE, montants budgétés).
   Aujourd'hui : budget définitif des comptes 6 et 2, toutes gestions.
   À qui : DFC
   Question : quel budget prendre : fonctionnement seul ou avec l'investissement, initial ou définitif, et quelles gestions ?
   Réponse :

63. La gestion de la masse salariale
   C'est quoi : le recueil donne la gestion pour les frais de personnel et la formation (G et S), pas pour la masse salariale (comptes 651 et 652).
   Objectif : dans T64, la dépense de formation et la masse salariale doivent porter sur le même périmètre. Cela permet de comparer le ratio à la norme de 1 à 5 % (Annuaire CIPRES, p. 17). La réponse permet de fixer les gestions de la masse salariale.
   Dans la base : vue V_BI_BALANCE_COMPTABLE (point 55), gestion G générale et S sociale.
   Aujourd'hui : gestions G et S, comme la formation.
   À qui : DFC
   Question : la masse salariale se prend-elle sur les gestions G et S ?
   Réponse :

54. Les montants par direction, agence et bureau
   C'est quoi : le recueil demande T63 et T64 par direction, agence et bureau (RH lignes 6 et 7).
   Objectif : dans T63 et T64, chaque montant doit pouvoir se lire par unité. Cela permet à chaque direction de suivre ses frais de personnel et sa formation (recueil v7). La réponse permet de savoir si la comptabilité le permet.
   Dans la base : la section analytique d'une écriture, si elle est remplie dans Oracle GL.
   Aujourd'hui : T63 et T64 se donnent pour toute la CNSS.
   À qui : DFC
   Question : les charges portent-elles une section analytique qui dit la direction ou l'agence ?
   Réponse :

T65, l'encadrement

59. Le taux d'encadrement des temporaires
   C'est quoi : le taux d'encadrement est la part des cadres dans l'effectif.
   Objectif : dans T65, le taux doit porter sur le personnel que la CIPRES vise. Il permet de comparer la CNSS à la norme de 10 % au plus (recueil v7, RH ligne 8). La réponse permet de savoir s'il faut un taux pour les seuls permanents, ou aussi pour les temporaires.
   Dans la base : la nature de la dernière situation (GRH_SITUATION, NATURE_EMPLOIE).
   Aujourd'hui : le taux CIPRES se calcule sur les permanents (362 cadres sur 742 en 2024) ; celui des temporaires se lit à part, avec le même filtre que T58.
   À qui : DRH
   Question : le taux d'encadrement CIPRES porte-t-il sur les seuls permanents ?
   Réponse :

## Plutôt pour la DSI

36. Les départs mal enregistrés
   C'est quoi : quand un agent part, sa fiche et sa situation devraient le dire.
   Objectif : dans T58 et T60, chaque départ doit avoir une date. Cela permet de retirer l'agent de l'effectif au bon moment (Proposition BI). La réponse permet de savoir si une autre date existe.
   Dans la base : la situation n'est jamais close ; 691 personnes parties gardent la position « activité normale » (STATUT_EMPLOYE = A). Les positions R, X et Y ne sont presque jamais saisies, et la date d'état de la fiche (DATE_ETAT_EMPLOYE) n'est plus remplie depuis 2020.
   À qui : DSI
   Question : est-ce voulu, et la date de départ est-elle notée ailleurs ?
   Réponse :

37. La mise à jour de 2020
   C'est quoi : en 2020, les états de sortie de nombreuses fiches ont été changés d'un coup.
   Objectif : dans T58 et T60, les départs anciens doivent retrouver leur date. Cela permet de compter l'effectif des années passées (Proposition BI). La réponse permet de dater une partie des 817 départs sans date (point 25).
   Dans la base : en 2020, 1 247 états de sortie ont été changés en une seule fois.
   Aujourd'hui : seul ce qui est dans l'application compte (fiche des règles, règle 3).
   À qui : DSI
   Question : les vraies dates de sortie ont-elles été gardées quelque part, et peut-on les remettre dans l'application ?
   Réponse :

38. Une vue des décisions
   C'est quoi : les entrées et les sorties de T60 se lisent dans les décisions.
   Objectif : dans T60, la BI doit lire les décisions par une vue simple et stable. Cela permet de ne pas dépendre des tables internes de l'application (spécification des vues, domaine 4). La réponse permet d'ajouter la vue à la spécification.
   À qui : DSI
   Question : pouvez-vous ajouter une vue des décisions avec les agents concernés (V_BI_DECISION_AGENT, à partir de GRH_DECISION et GRH_DECISION_PERSONNE) ?
   Réponse :

39. Les formations
   C'est quoi : le chargement de l'entrepôt lit une table des catégories de formation.
   Objectif : dans T62, la finalité doit se lire dans une liste à jour. Cela permet de séparer perfectionnement et diplôme (Annuaire CIPRES, p. 51). La réponse permet de réparer le chargement.
   Dans la base : toutes les formations sont au statut N, et la table GRH_CAT_FORMATION, lue par le chargement, n'existe pas.
   À qui : DSI
   Question : que veut dire le statut N, et où est passée cette table ?
   Réponse :

41. Les autres codes de catégorie
   C'est quoi : la catégorie d'un agent de la CNSS va de 01 à 06 ; d'autres codes existent.
   Objectif : dans T58, T62 et T65, chaque code de catégorie doit être compris. Cela permet de ranger chaque agent (recueil v7, RH ligne 1). La réponse permet de confirmer qu'ils ne concernent pas les agents de la CNSS.
   Dans la base : à côté des catégories 01 à 06, on trouve HC et les codes I à V et 001 à 005, pour aucun permanent en 2024.
   À qui : DSI
   Question : I à V et 001 à 005 sont-ils les catégories de la fonction publique, et HC veut-il bien dire hors catégorie ?
   Réponse :

42. Le registre des mouvements
   C'est quoi : la table des mouvements de l'application garde les changements de poste.
   Objectif : pour les mouvements internes (hors de T60), la BI doit lire les codes comme la CNSS. Cela permet un suivi des carrières, si la CNSS le demande (Proposition BI). La réponse permet de confirmer la lecture.
   Dans la base : GRH_MOUVEMENT ne contient que des changements de poste. On y lit DIS mise à disposition, REC reclassement, RET rétrogradation ; 1 621 mouvements sont à E dans le champ de validation, et 91 % ne sont jamais validés ; le code NOM (nomination) manque dans la liste de référence.
   À qui : DSI
   Question : cette lecture est-elle juste, et que veulent dire E et A ?
   Réponse :

43. Des dates impossibles
   C'est quoi : certaines dates sont des fautes de frappe.
   Objectif : dans tous les tableaux, chaque situation et chaque décision doit tomber à sa vraie date. Cela permet de ne pas perdre d'agents (Proposition BI). La réponse permet de corriger ces lignes à la source.
   Dans la base : des situations sont datées de l'an 212, 2201 ou 3015, et des décisions de l'an 5 ou des années 200 à 209.
   À qui : DSI
   Question : peut-on les corriger ?
   Réponse :

44. La copie de la base
   C'est quoi : la BI calcule sur une copie de la base du personnel.
   Objectif : dans tous les tableaux, les chiffres de l'année N doivent venir de la photo de la base de fin janvier N+1 (fiche des règles, règle 2). La réponse permet de prévoir cette photo chaque année.
   Dans la base : la copie date du 18/02/2026.
   À qui : DSI
   Question : quand sera-t-elle rafraîchie, et peut-on avoir une copie à chaque fin janvier ?
   Réponse :

55. La vue de la comptabilité
   C'est quoi : T63 et T64 lisent la comptabilité d'Oracle GL par une vue préparée par la DSI.
   Objectif : dans T63 et T64, les montants doivent venir de la comptabilité validée. Ils permettent de calculer les deux ratios et de les comparer aux normes (recueil v7, RH lignes 6 et 7 : source Oracle GL). La réponse permet de produire T63 et T64.
   Dans la base : la vue V_BI_BALANCE_COMPTABLE est spécifiée (exercice, compte, gestion, section, réalisé et budget), pas encore créée.
   Aujourd'hui : T63 et T64 ne sont pas produits. Il leur faut cinq exercices (N−4 à N).
   À qui : DSI, avec la DFC
   Question : quand pourrez-vous créer et alimenter cette vue depuis Oracle GL, et depuis quel exercice ?
   Réponse :

## Déjà répondu

1. Dans quel groupe mettre la 5e catégorie ?
   C'est quoi : les catégories 01 à 06 de la grille de salaire sont regroupées pour T58, T62 et T65.
   Réponse donnée : pour la CNSS, 01 et 02 agents d'exécution, 03 agents de maîtrise, 04 et 05 cadres moyens, 06 cadres supérieurs ; pour la CIPRES, les cadres sont 04, 05 et 06. La 5e est donc un cadre moyen.
   Source : recueil v7, RH ligne 1 ; confirmé par l'annuaire CNSS 2024 (173, 207, 304 et 58 agents pour 177, 208, 311 et 62 publiés).

5. L'agent en disponibilité
   C'est quoi : un agent en disponibilité arrête de travailler pour un temps, à sa demande et sans salaire.
   Réponse donnée : il sort de l'effectif à sa mise en disponibilité (décision 07) et y revient à sa remise en activité (18). T60 compte une sortie et une entrée.
   Source : décision du 08/11, d'après le tableau 121 de l'annuaire CNSS 2024 et la réunion du 03/11. La DRH peut encore la revoir.

6. Qui compte dans l'effectif
   C'est quoi : la CNSS a des agents permanents, des temporaires, et accueille des agents de la fonction publique.
   Réponse donnée : seuls les agents de la CNSS (QUALITE = A) ; un tableau des permanents et un des temporaires, selon la nature de la dernière situation.
   Source : réunion du 03/11 ; recueil v7, RH ligne 1 (« par type employé : permanent, temporaire »).

7. Présent au 31 décembre
   C'est quoi : un agent est compté s'il est entré et pas encore parti à la date d'arrêté.
   Réponse donnée : d'accord. Depuis le 05/11, une fiche sans état compte comme encore là (point 45).
   Source : réunion du 03/11.

8. L'âge et l'ancienneté
   C'est quoi : T58 range les agents par âge, T61 par ancienneté.
   Réponse donnée : l'âge en années pleines, d'accord. L'ancienneté part de la prise de service, sinon de l'embauche, et on retire les périodes où l'agent était parti (disponibilité, départ puis nouvel engagement).
   Source : réunion du 03/11 ; recueil v7, RH ligne 4 (« prendre en compte les sorties et retours »).

9. Le taux d'encadrement (T65)
   C'est quoi : la part des cadres dans le personnel.
   Réponse donnée : cadres (catégories 04, 05, 06 et HC) divisés par l'effectif.
   Source : réunion du 03/11 (« on garde comme ça »). Le recueil v7 (RH ligne 8) rappelle seulement la norme de 10 % au plus.

10. Les contractuels
   C'est quoi : un contractuel est un agent temporaire, engagé pour une durée limitée.
   Réponse donnée : un temporaire est présent si la date d'arrêté tombe dans un de ses contrats ; un contrat commence à l'engagement (50) et finit au départ ou au contrat suivant. Plus de durée de 18 mois.
   Source : réunion du 03/11.

11. Les entrées et les sorties (T60)
   C'est quoi : T60 suit les entrées et les sorties du personnel sur cinq ans.
   Réponse donnée : T60 se lit entre deux photos de l'effectif (début + entrées − sorties = fin), avec les codes de mouvement de la CNSS. Les changements de poste restent hors du tableau.
   Source : réunion du 03/11 (la table des mouvements ne garde que les changements de poste) ; règles de T60 du 08/11. Le détail des « autres » est au point 49.

16. La durée d'un contrat temporaire
   C'est quoi : la date de fin d'un contrat n'est pas saisie.
   Réponse donnée : la BI ne fixe pas de durée à la place de la CNSS (voir les points 10 et 20).
   Source : réunion du 03/11.

18. Les domaines d'activité
   C'est quoi : T59 répartit le personnel par domaine d'activité.
   Réponse donnée : 5 domaines (recouvrement, prestations, prévention des risques professionnels, action sanitaire et sociale, gestion administrative et financière), lus par l'unité de l'agent dans le fichier tenu par la CNSS. Le code de domaine de la situation n'est plus utilisé.
   Source : réunion du 03/11 ; recueil v7, RH ligne 2. Reste à remplir le fichier (point 48).

19. Les temporaires dans T60
   C'est quoi : un temporaire est souvent réengagé plusieurs fois.
   Réponse donnée : T60 se lit pour les permanents ou pour les temporaires, comme T58. Un temporaire devenu permanent est une autre entrée des permanents.
   Source : recueil v7, RH ligne 1 (par type d'employé).

30. Des montants douteux
   C'est quoi : les montants comptables lus dans la copie de la base semblaient faux.
   Réponse donnée : la comptabilité de la copie n'est pas valide ; T63 et T64 passent par la vue V_BI_BALANCE_COMPTABLE (point 55).
   Source : la CNSS, le 07/11.

33. La masse salariale
   C'est quoi : T64 rapporte la dépense de formation à la masse salariale.
   Réponse donnée : comptes 651 et 652.
   Source : recueil v7, RH ligne 6, comme l'Annuaire CIPRES.

34. Les cadres moyens sont-ils des encadrants ?
   C'est quoi : avec les cadres moyens, le taux est de 48,8 % en 2024 ; sans eux, de 7,8 %.
   Réponse donnée : oui, un encadrant est un cadre, moyen ou supérieur.
   Source : réunion du 03/11 ; c'est aussi ce que publie l'annuaire CNSS 2024 (49,2 %).

35. Les actifs ou tout l'effectif ?
   C'est quoi : le taux d'encadrement pouvait se calculer sur les seuls actifs.
   Réponse donnée : sans objet. Les agents en disponibilité sortent de l'effectif (point 5), il ne reste presque plus d'inactifs : le taux porte sur tout l'effectif.
   Source : décision du 08/11.

40. La paie
   C'est quoi : la paie n'est pas dans la copie de la base.
   Réponse donnée : sans objet pour les tableaux CIPRES. La masse salariale vient de la comptabilité (comptes 651 et 652), pas de la paie.
   Source : recueil v7, RH ligne 6.

## Tous les tableaux, un par un

- T58, effectif par catégorie, âge et sexe, permanents puis contractuels : points 2, 3, 14, 15, 17, 20, 25, 45, 46, 47, 50, 56, 61, 62 ; déjà répondus : 1, 5 à 8, 10, 16.
- T59, effectif par sexe et domaine d'activité : points 14, 48, 56, 58, 62 ; déjà répondus : 6, 7, 18.
- T60, mouvement du personnel sur cinq ans : points 2, 3, 12, 20 à 25, 49 à 52, 56, 57 ; déjà répondus : 5, 11, 19.
- T61, effectif par ancienneté et sexe : points 14, 50, 56, 62 ; déjà répondus : 6 à 8.
- T62, personnel formé par catégorie, sexe et finalité : points 4, 13, 26 à 28, 47, 53, 56, 60.
- T63, ratio des frais de personnel sur les charges de fonctionnement : points 29, 54, 55, 57 ; déjà répondu : 30.
- T64, ratios des dépenses de formation sur la masse salariale et sur le budget : points 31, 32, 54, 55, 57, 63 ; déjà répondus : 33, 40.
- T65, taux d'encadrement sur cinq ans : points 2, 14, 47, 56, 57, 59 ; déjà répondus : 1, 9, 34, 35.

## Les points de chaque direction

- DRH, la Direction des ressources humaines : points 2 à 4, 12 à 15, 17, 20 à 28, 45 à 53, 56 à 62 (57 avec la DPC).
- DFC, la Direction financière et comptable : points 29, 31, 32, 54, 63, et 55 avec la DSI.
- DSI, la Direction des systèmes d'information : points 36 à 39, 41 à 44, 55, et 56 avec la DRH.
- DPC, la Direction de la prospective et de la coopération, qui fait l'annuaire : point 57.
