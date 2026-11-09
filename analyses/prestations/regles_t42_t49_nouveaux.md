# T42 à T49 : les nouveaux bénéficiaires de l'année (pensionnés et rentiers) — les règles en clair

## En bref

- **Ce que comptent ces tableaux.** Les personnes qui touchent pour la première fois, dans l'année, une pension (T42 les retraités, T43 les veufs et veuves, T44 les orphelins) ou une rente d'accident du travail ou de maladie professionnelle (T45 toutes, T46 les orphelins, T47 les parents, T48 les victimes, T49 les veufs et veuves). Pour chacune : son âge, son sexe et ce qui lui est payé ; en plus, les mois de travail des retraités (T42) et le taux d'incapacité des victimes (T48).
- **Ce que le recueil v7 demande de nouveau.** Chaque tableau par mois, par trimestre et par année, et pour T43 à T49 une période libre (une date de début, une date de fin) ; les mois de travail de T42 comptés en mois, sans ceux cotisés dans une autre caisse ; le taux d'incapacité de T48 en moyenne simple. Sa phrase « Idem. Tab 41 » (« allocataire = titulaire du dossier ») ferait compter le défunt à la place de sa famille : nous la lisons « même périodicité » (N4, Q1).
- **Ce que l'annuaire CNSS 2024 tranche ou confirme.** Il tranche, faute de source plus forte : une personne compte l'année où sa prestation est saisie dans l'application, y compris quand une autre caisse paie la pension (coordination) : nouveaux retraités 2 463 pour 2 489 publiés (−1,0 %), veufs et veuves 771 pour 766 (+0,7 %), orphelins 773 pour 769 (+0,5 %). Il confirme qu'elle ne compte que la première fois (rentes : 102 pour 105), et les orphelins et veuves de rente tombent juste (44 et 4). Par tranche de 5 ans, l'âge des nouveaux retraités colle à l'annuaire de 55 à 69 ans (à 1,6 % près).
- **Ce qui reste en écart.** 39 nouvelles rentes de victimes pour 44 publiées (−11,4 %) ; 15 parents de rente pour 13 (+15,4 %) ; 641 femmes parmi les retraités saisis en 2024 pour 527 publiées (+21,6 %). Les rentes payées dans la base (tout le stock) sont 12 à 25 % sous l'annuaire : les montants de T45 à T49 seront trop bas.
- **Les décisions attendues de la CNSS.** Qui compte pour une rente ou une pension de survivant (Q1) ; saisie ou notification (Q2) ; le rappel des mois passés dans le montant (Q3) ; où sont payées les rentes qui manquent (Q5) ; comment la CNSS compte ses 44 rentes de victimes (Q6, après un dernier test).
- **Ce qui reste à faire.** Claude : rendre la période libre dans la requête (N1 à N3), y ajouter la colonne du rappel (N8) et la direction régionale (N5), rédiger le lot 10 (une requête de contrôle sur la base). Toi : exécuter la requête et le lot 10, puis poser les questions à la CNSS, en commençant par Q1, Q2, Q3 et Q5. Rien n'est validé par la CNSS.

---

**Ce que couvre la famille.** Huit tableaux de l'Annuaire CIPRES, qui comptent les **entrées** de l'année : combien de personnes reçoivent pour la première fois une prestation durable, à quel âge, de quel sexe, et ce qui leur est payé. Trois tableaux pour la branche des pensions : T42 (nouveaux retraités, avec le total de leurs mois de travail), T43 (nouveaux veufs et veuves), T44 (nouveaux orphelins). Cinq pour les accidents du travail et maladies professionnelles (AT/MP) : T45 (tous les nouveaux rentiers), T46 (orphelins), T47 (parents), T48 (victimes, avec leur taux d'incapacité), T49 (veufs et veuves). S'y ajoutent deux indicateurs du recueil v7 qui portent sur les mêmes personnes : le taux de remplacement et les délais de liquidation des premières demandes.

**État au 08/11/2026.** Tout est **proposé**, rien n'est encore validé par la CNSS. Aucune réunion avec la CNSS n'a porté sur ces tableaux ; la question du montant (rappel compris ou non) a été posée au point du 02/11 et attend sa réponse. La requête n'a pas été exécutée : les chiffres cités viennent des lots 7 et 8 et de la comparaison A03, **vus au 02/02/2026** (date de la copie de la base).

**D'où viennent les règles.** Dans l'ordre de force : une décision de réunion (aucune ici), le recueil des besoins v7, l'atelier de Kaya (2017), l'Annuaire CIPRES. L'annuaire statistique de la CNSS 2024 sert à comparer les chiffres, et tranche la pratique quand les sources plus fortes se taisent : c'est le cas de la date d'entrée et de la « première fois ». Les modèles de ratios de la CNSS (classeur « Evaluation des Ratios CIPRES 2025 », feuille 03) ne disent rien de propre à ces tableaux, sauf un contre-indice sur la date (partie 4).

**Ce qui existe déjà, laissé tel quel.** La requête [t42_t49_requete_proposee.sql](t42_t49_requete_proposee.sql) (01/11, une requête pour les huit tableaux) ; le lot 7 ([lot7_nouveaux_beneficiaires.sql](lot7_nouveaux_beneficiaires.sql), P82 à P91) ; les règles R2 à R9 et R13 du [README](README.md) ; la carte A1 du schéma `regles_prestations` ; la fiche de validation T42 (`excalidraw/schemas/prestations/tableaux cipres/T42_nouveaux_retraites.excalidraw`, règles 1 à 8, questions Q1 à Q5) ; [points_a_valider](points_a_valider.md), points n° 1, 2, 4, 10, 11, 18, 34 et 44 à 48. Ce document les reprend avec leurs identifiants, et **propose** les règles nouvelles N1 à N8 sans rien modifier.

**Le schéma associé :** `excalidraw/schemas/prestations/T42 T49 nouveaux/regles_t42_t49_nouveaux.excalidraw`, fait par `excalidraw/sources/prestations/famille_t42_t49_nouveaux.py`. Il dit la même chose en plus court, avec les mêmes identifiants.

**Les mots de ce document.** *Nouveau bénéficiaire*, *entrée* : une personne qui reçoit pour la première fois une prestation de ce tableau. *Loi des entrées* : combien de personnes entrent chaque année, et à quel âge ; les actuaires s'en servent pour prévoir les pensions futures. *Saisie* : le jour où la prestation est enregistrée dans l'application. *Notification* : le jour où la décision est envoyée à l'assuré. *Pensionné* : il touche une pension (branche des pensions). *Rentier*, ou *crédirentier* (le mot de l'Annuaire CIPRES) : il touche une rente d'accident du travail ou de maladie professionnelle. *Victime* : l'assuré blessé ou malade. *Rente viagère* : la rente de la victime elle-même, à vie. *Ayant droit*, *survivant* : la veuve, le veuf, l'orphelin ou le parent qui a droit à une prestation après le décès d'un assuré. *IPP* (incapacité permanente partielle) : le taux d'incapacité de la victime, en %. *Services passés* : le total des mois de travail salarié de l'assuré. *Coordination* : l'assuré a aussi cotisé dans une autre caisse, souvent d'un autre pays ; `COOR_AC` : l'autre caisse paie ; `COOR_CNSS` : la CNSS paie (catégorie `DOS_CATEGORIE` du dossier). *Rappel* : les mois d'avant la période, payés en retard avec le premier paiement. *Md* : milliard de FCFA ; *M* : million. Le *cliché* : la copie de la base sur laquelle on calcule. Les *réglages* : les choix écrits dans le bloc `parametres` en tête de la requête (une « CTE » en langage SQL) ; on les change sans réécrire la requête. *Pnn*, *lot n*, *Ann* : le résultat numéroté d'une requête de contrôle déjà lancée sur la base. Les sigles des directions sont expliqués en tête de la partie 6.

---

## Ce que disent les sources

| Source | Ce qu'elle dit pour ces tableaux |
|---|---|
| Recueil des besoins v7 (14/01/2026 et 20/01/2026) | T42 (l. 296 à 299) : services passés en mois ; coordination, « uniquement la partie CNSS » ; par mois, trimestre et année. T43, T44, T45 (l. 305, 312, 203) : « Indiquer la période (début, fin) en paramètre » et « Idem. Tab 41 », c'est-à-dire la définition de l'allocataire de T41 (« titulaire du dossier ») et sa périodicité. T45 (l. 202) : « Nouveaux crédirentiers : rentiers ». T46 à T49 (l. 210, 216, 224, 230) : « Idem. Tab 45 ». T48 (l. 222 et 223) : le degré d'IPP est la moyenne des taux ; chaque rentier a un taux. Autour : allocation de survivants (l. 326, T51), retraite anticipée à part dans T25 et T28 (l. 248, 255), taux de remplacement et délais de liquidation (indicateurs, l. 1170 à 1265) |
| Réunions avec la CNSS | Aucune sur T42 à T49 à ce jour. La question du rappel (points_a_valider n° 1 et n° 45) a été posée au point du 02/11, sans réponse |
| Atelier de Kaya (2017) | Tableau 1 : T42, services passés « à estimer en nombre de mois » et coordination « uniquement la partie CNSS » (l. 348 et 349) ; T45 et T46, « que les services techniques s'appliquent pour une saisie correcte des données » (l. 203 et 213) ; T48, « réévaluer périodiquement le taux d'IPP » (l. 233) ; T45 à T49 rangés avec les AT/MP (l. 194 à 242) ; T41, « c'est le dossier de l'allocataire qui est considéré » (l. 120) |
| Annuaire CIPRES | Descriptions p. 15 (T42 à T48) et p. 16 (T49) : les nouveaux bénéficiaires de l'exercice et les montants servis, par tranche d'âge et par sexe, pour « définir la loi des entrées » ; à la fin de l'exercice. Maquettes p. 42 à 45. Glossaire : âge = année de constatation moins année de naissance (p. 62) ; allocataire, « en matière de prestations familiales » (p. 62) ; pensionné veuf, orphelin, ascendant (p. 65) ; services passés (p. 66) |
| Annuaire CNSS 2024 | Nouveaux pensionnés par type et par direction régionale (tableau 23), nouveaux retraités par âge et sexe (tableau 25), nouvelles rentes viagères par âge et sexe (tableau 75), nouveaux ayants droit de rentes par nature (tableau 77). Aucun montant, aucun service passé, aucun taux d'IPP pour les nouveaux. Autour : montants des pensions (tableaux 19, 28), des rentes (90, 96, 97), stocks et décès (18, 20, 26, 76, 80, 81) |
| Modèles de ratios de la CNSS (2025) | Feuille 03 : délai de liquidation sur la « 1ère demande » de pension et la « 1ère demande de rente », de la réception du dossier à la notification des droits. Les feuilles 04 à 06 servent à T25 à T31. Modèles vides |
| Données de la base | Lot 7 (P82 à P91), lot 8 (P92, P93, P98, P99), lot 9 (P104, P105), lot 3 B (P51) ; comparaisons A02 et A03 (dossier `comparaison_annuaire_cnss_2024`) |

---

## 0. Les tableaux de la famille

| Tableau | Ce qu'il compte (Annuaire CIPRES) | Page | Ce qu'on a déjà |
|---|---|---|---|
| T42 | Les nouveaux pensionnés de vieillesse de l'exercice, leurs montants et le total de leurs services passés, par tranche d'âge et par sexe. Lignes : moins de 50 ans, 50 – 54 … 95 – 99 ans, 100 ans et plus, TOTAL ; blocs hommes et femmes (effectif, montant, services passés), sans colonne Total | p. 15 ; maquette p. 42 ; glossaire p. 62 et 66 | `t42_t49_requete_proposee.sql` (pas exécutée) ; fiche T42 (8 règles, 5 questions) ; lot 7 |
| T43 | Les nouveaux pensionnés veufs et veuves et leurs montants. Lignes : moins de 20 ans, 20 – 24 … 55 – 59 ans, 60 ans et plus, TOTAL ; hommes, femmes, total (effectif, montant) | p. 15 ; maquette p. 42 ; glossaire p. 65 | même requête ; lot 7 |
| T44 | Les nouveaux pensionnés orphelins et leurs montants. Lignes : moins de 5 ans, 5 – 9, 10 – 14, 15 – 19 ans, 20 ans et plus, TOTAL | p. 15 ; maquette p. 43 ; glossaire p. 65 | même requête ; lot 7 |
| T45 | Les nouveaux crédirentiers de la branche AT/MP et leurs montants, sans type de rente : toute la branche. Lignes : moins de 15 ans, 15 – 19 … 95 – 99 ans, 100 ans et plus, TOTAL (la tranche « moins de 15 ans » suppose des orphelins) | p. 15 ; maquette p. 43 ; glossaire p. 65 | même requête ; lot 7 |
| T46 | Les nouveaux crédirentiers orphelins (rentes AT/MP) et leurs montants. Même grille que T44 | p. 15 ; maquette p. 44 | même requête ; lot 7 |
| T47 | Les nouveaux crédirentiers ascendants (père ou mère d'une victime décédée) et leurs montants. Lignes : moins de 50 ans … 100 ans et plus. Le titre ne nomme pas la branche ; « crédirentier » veut dire rente, donc AT/MP. Le glossaire (p. 65) ne définit que le pensionné ascendant de la branche des pensions : pour T47, on transpose | p. 15 ; maquette p. 44 | même requête ; lot 7 |
| T48 | Les nouveaux bénéficiaires de rente viagère (la victime elle-même), leurs montants et leur degré moyen d'IPP. Lignes : moins de 20 ans, 20 – 24 … 60 – 64 ans, 65 ans et plus ; blocs hommes et femmes, sans colonne Total. L'IPP n'est pas défini dans le glossaire | p. 15 ; maquette p. 45 | même requête ; lot 7 |
| T49 | Les nouveaux crédirentiers veufs et veuves (rentes AT/MP) et leurs montants. Même grille que T48 | p. 16 ; maquette p. 45 | même requête ; lot 7 |

La requête `t42_t49_requete_proposee.sql` sort les huit tableaux d'un coup : une ligne par tableau et par tranche d'âge, avec l'effectif et le montant des hommes, des femmes et le total, plus les services passés (T42) et le degré d'IPP (T48). Elle ne calcule aujourd'hui que l'année civile.

---

## 1. Ce que demande le recueil des besoins v7

Toutes les lignes du recueil qui touchent T42 à T49. Les numéros de ligne sont ceux de `besoins_v7.txt`. Les renvois (« Idem. Tab 45 », « Idem. Tab 41 ») sont rattachés à la ligne qu'ils reprennent.

| Réf. | Ce que dit le recueil | En clair | Où c'est pris en compte | Statut |
|---|---|---|---|---|
| Prestation #25 (T42), l. 296 | « Service passé : total des périodes d'activités salarié en nombre de mois » | La colonne « Total services passés » additionne les mois de travail salarié des nouveaux retraités de chaque tranche | R9 : mois cotisés (`PE_MOIS_COTISATION`), toujours remplis (P86) ; fiche T42, règle 8 | pris en compte |
| Prestation #25 (T42), l. 297 | « La modalité « total services passés » est à estimer en nombre de mois » | En mois, pas en années. « Estimer » laisse penser qu'une valeur approchée suffit. Même phrase que Kaya 2017 (l. 348) | R9 | pris en compte |
| Prestation #25 (T42), l. 298 | « Pour les pensions de coordination, prendre uniquement la partie CNSS » | Pour un assuré qui a aussi cotisé dans une autre caisse, on ne garde que la part de la CNSS. La phrase ne dit pas si c'est pour les mois, pour le montant, ou pour les deux | R9 (mois : total moins `PE_COT_AUTRE`) ; N6 (montant : ce que la CNSS a payé) | question (Q10, Q11) |
| Prestation #25 (T42), l. 299 | « Périodicité : Mensuelle, Trimestrielle, Annuelle » | T42 sort par mois, par trimestre et par année. L'Annuaire CIPRES et Kaya 2017 disaient « fin d'exercice » : le recueil, plus fort, l'emporte | N1 ; la requête n'est qu'annuelle | à intégrer |
| Prestation #11 (T45), l. 203 ; #26 (T43), l. 305 ; #27 (T44), l. 312 ; repris pour T46 à T49 par « Idem. Tab 45 » (l. 210, 216, 224, 230) | « Indiquer la période (début, fin) en paramètre » | L'utilisateur choisit une date de début et une date de fin ; le tableau compte les personnes entrées entre les deux. T42 ne le demande pas, mais a la même périodicité | N1, N2, N3 ; réglages `debut` et `fin` à rendre libres ; remarque B33 du recueil | à intégrer |
| Prestation #11 (T45), l. 202 ; repris pour T46 à T49 (l. 210, 216, 224, 230) | « Nouveaux crédirentiers : rentiers » | Un crédirentier est un rentier : la victime ou un membre de sa famille qui touche une rente AT/MP. T45 compte donc toutes les nouvelles rentes de la branche | R2 (T45 = toutes les rentes AT/MP) | pris en compte |
| #11 (T45), l. 204 ; #26 (T43), l. 306 ; #27 (T44), l. 313 ; repris pour T46 à T49 ; renvoi à #4 (T41), l. 153 | « Idem. Tab 41 » : « Périodicité : Mensuelle, Trimestrielle, Annuelle » | T43 à T49 sortent aussi par mois, par trimestre et par année | N1 ; remarque B32 du recueil | à intégrer |
| Mêmes lignes ; renvoi à #4 (T41), l. 152 | « Idem. Tab 41 » : « Allocataire : Titulaire du dossier (assuré) » | Lu à la lettre pour un survivant, on compterait l'assuré décédé à la place de sa veuve, de son orphelin ou de son parent, avec l'âge et le sexe du défunt. Pour T48 (la victime), les deux lectures donnent la même personne | N4 : on garde l'ayant droit (R5). La remarque B05 du recueil étend la définition à T45 à T49 sans en tirer la conséquence, et oublie T43 et T44 | question (Q1) |
| Prestation #14 (T48), l. 222 | « Dégré IPP : moyenne des taux IPP (somme des taux diviser par le total de la tranche) » | Pour chaque tranche, la somme des taux d'incapacité des nouveaux rentiers, divisée par leur nombre | N7 (degré moyen par tranche et par sexe dans la requête). Quel taux si la rente change dans l'année : Q15 | pris en compte |
| Prestation #14 (T48), l. 223 | « Chaque rentier possède un taux dans l'application » | Le taux est toujours saisi | Vérifié : jamais vide ni nul, de 15 à 100, de 2020 à 2025 (P87) ; remarque B08 du recueil | pris en compte |
| Prestation #20 (T32), l. 263 | « Pour les pensions de coordination, les montants concernent uniquement la partie CNSS » | Écrit pour le stock des retraités. Vaut par analogie pour le montant des nouveaux veufs, veuves et orphelins (T43, T44) | N6 ; remarque B07 du recueil | pris en compte |
| Prestation #18 (T25), l. 248 ; #19 (T28), l. 255 | « Ajouter une ligne « Pension de retraite anticipée » » | Le recueil veut voir la retraite anticipée à part dans T25 et T28. Pour T42, il ne dit rien | R2 : dans T42 avec la vieillesse, par proposition BI (comme T32) ; remarque B01 du recueil | question (Q13) |
| Indicateurs pensions, l. 1247 à 1252 et l. 1256 à 1262 | « Le nombre de retraités est le nombre de bénéficiaires de pensions directes (Pension vieillesse, Pension anticipé, pension d'invalidité) » | Vaut pour le rapport démographique et le taux de remplacement, pas pour T42 : sinon, les nouvelles pensions d'invalidité iraient aussi dans T42 | Pas repris pour T42 (R2) | pris en compte |
| Prestation #23 (T35), l. 283 | « Supprimer la tranche d'âge « 25 ans et plus » » | Écrit pour le stock des orphelins (T35). Les grilles de T44 et T46 s'arrêtent à « 20 ans et plus », où tombent aujourd'hui 2 nouveaux orphelins de 25 à 29 ans (2024, P84) | Réglage `orphelin_age_max` (à créer) ; remarque B04 du recueil | question (Q14) |
| Prestation #21 (T33), l. 270 | « Les informaticiens doivent développer un nouveau module pour la gestion des pensions d'invalidité qui sont convertis en pension normale dès que l'assuré atteint l'âge de départ à la retraite » | Un développement de l'application. Quand il existera, un pensionné d'invalidité recevra une pension de vieillesse | Hors BI pour le module. Effet sur T42 : Q18 ; remarque B31 du recueil | hors BI |
| Prestation #28 (T50), l. 319 et 320 | « Service passé : total des périodes d'activités salarié en nombre de mois » ; « Périodicité : Annuelle » | T50 compte les allocations de vieillesse, versées une seule fois : c'est la frontière de T42 | R2 : une allocation de vieillesse ne compte pas dans T42 | pris en compte |
| Prestation #29 (T51), l. 326 | « Allocation de survivants : veufs/veuves, orphelins et ascendants » | Une allocation versée une seule fois à la famille : c'est la frontière de T43 et T44 | R2 : elle ne compte ni dans T43 ni dans T44 (elle va dans T51) | pris en compte |
| Indicateurs pensions, l. 1217 à 1238 (note l. 1224) | Délai de liquidation : du dossier complet reçu au guichet à « la date à laquelle la notification des droits a été signifiée à l'assuré » ; trimestriel et annuel ; par type, DR et agence | Porte sur les mêmes premières pensions que T42 à T44, mais datées à la notification | Traité dans la famille T25-T27 (D4, N5), avec le même statut : la requête ne le calcule pas encore. Ici : contre-indice pour R7, qui compte à la saisie (Q2) | à intégrer |
| Indicateurs AT/MP, l. 1170 à 1179 | Délai des rentes de survivants : du certificat de consolidation à la notification ; annuel ; par DR et agence | Sa population est celle des nouvelles rentes de T45 à T49 : il lui faut la même règle de la « première rente » (R7) | Traité dans la famille T25-T27, avec le même statut (T25-T27 Q8, Q9 ; remarque B16 du recueil) | question (T25-T27 Q8, Q9) |
| Indicateurs, l. 1133 | « Le montant total des prestations ordonnancées au titre de la période est l'ensemble des prestations ayant fait l'objet d'une notification » | Écrit pour un ratio de ressources, pas pour définir un nouveau pensionné | R7 garde la saisie | pris en compte |
| Indicateur « taux de remplacement », l. 1256 à 1265 | R = montant de la pension ÷ dernier revenu d'activité × 100 ; dernier revenu = salaire moyen des cinq meilleures années ; pensions directes ; annuel ; par DR et agence | La part du salaire que la pension remplace. Se calcule naturellement sur les nouveaux retraités (la population de T42), mais le recueil ne le dit pas | Pas fait. La base garde un salaire moyen sur chaque pension (`PE_SAL_MOYEN`), pas encore étudié ; remarque B37 du recueil | question (Q19) |
| Termes de référence, l. 4 à 10 | « cinq (5) Directions régionales : Ouagadougou, Bobo Dioulasso, Ouahigouya, Fada N'gourma, Dédougou » | « Par DR » veut dire ces cinq directions | N5 | à intégrer |
| Indicateurs, l. 1065 ; indicateurs « par DR et par agences » (l. 1080 à 1265) | « De manière globale, donner la possibilité de désagréger l'indicateur » | Chaque chiffre doit pouvoir se découper, au moins par DR et par agence | N5 ; remarque B34 du recueil | à intégrer |
| Attentes, l. 29 | « Toutes les données des rapports doivent pouvoir être décomposées, traquées jusqu'au niveau de granularité le plus élémentaires » | Chaque chiffre doit se déplier jusqu'à la liste des personnes. Passage repris d'un cahier des charges bancaire (remarque B41 du recueil) : on s'appuie surtout sur les l. 51 et 68 | R7 construit la liste des nouveaux, une ligne par personne et par tableau : à rendre consultable (partie 9) | à intégrer |
| Caractéristiques techniques, l. 51 et 68 | Analyses « y compris au niveau transactionnel » ; sur des axes « non préalablement définis » | La BI garde le détail, une ligne par personne | Comme ci-dessus | à intégrer |
| Attentes et caractéristiques, l. 31 et 74 | Présentation multidimensionnelle ; une dimension ajoutée après coup vaut pour tous les indicateurs | Les mêmes axes pour les huit tableaux : tableau, tranche d'âge, sexe, mois, DR, agence | N1, N5 | à intégrer |
| Attentes, l. 35 | « Production des rapports et des états de restitution à des fréquences adaptées » | Chaque tableau à sa fréquence | N1 | à intégrer |
| Attentes et caractéristiques, l. 28, 34, 75 | Peu d'intervention manuelle ; restitutions automatisées ; rafraîchissement automatique | Les chiffres sortent d'une requête, sans saisie à la main | `t42_t49_requete_proposee.sql` | pris en compte |
| Caractéristiques techniques, l. 58 | Règles de transcodage écrites et rejouables | La correspondance entre codes de prestation et tableaux, et la grille des âges, sont écrites | R2, R8 (table de correspondance du README) | pris en compte |
| Attentes et caractéristiques, l. 24 et 78 | Profilage de la qualité des données | Mesurer les trous de la base : par exemple l'année 1900, date de naissance par défaut | R8 (âge inconnu) ; lot 7 | pris en compte |
| Général, l. 15 et 92 à 94 | La source : l'application de gestion des prestations | Les tableaux se calculent sur la copie de cette application | Toutes les règles ; copie de la base (schéma `USER_DWH`) au 02/02/2026 | pris en compte |

**Quels tableaux n'ont aucune ligne.** Aucun : les huit tableaux ont leur ligne dans le recueil (T42 #25, T43 #26, T44 #27, T45 #11, T46 #12, T47 #13, T48 #14, T49 #15). Mais T46, T47 et T49 n'ont qu'un renvoi (« Idem. Tab 45 »), et T42 n'a ni période libre ni « Idem. Tab 41 ». Le tableau des prestations du recueil ne demande aucun découpage par DR ou par agence pour T42 à T49 : ce découpage vient des lignes générales (l. 29, 1065).

**Les lignes générales qui ne changent rien ici :** l. 72 (accès par profil : ces tableaux ne montrent pas de personne, mais leur liste en montre, voir point d'attention 14), l. 80 et 81 (document de référence, ni nommé ni joint : rangé dans la famille T50-T57, question T50-T57 Q25), l. 53 à 55 et 60 (fichiers Excel : T28 et T31), l. 32 (alertes : aucun de ces tableaux n'a de norme), indicateur de satisfaction (l. 1075 à 1084 : rangé dans la famille T50-T57, hors base), indicateurs de paiement par virement (l. 1085 à 1091) et des ratios CIPRES respectés (l. 1978 à 1989).

---

## 2. Les règles, en clair

### R2 — Le périmètre de chaque tableau

**Règle.** La personne entre dans un tableau selon le code de sa prestation dans l'application (les codes ci-dessous sont ceux de la base). T42 : pension de vieillesse (RPV) et, par proposition, retraite anticipée (RPRA). T43 : pension de veuve ou de veuf (RPSV, RPSF). T44 : pension d'orphelin (RPSE). T45 : toutes les rentes AT/MP (ARI, ARSV, ARSF, ARSE, ARSA). T46 : rente d'orphelin (ARSE). T47 : rente d'ascendant (ARSA). T48 : rente viagère de la victime (ARI). T49 : rente de veuve ou de veuf (ARSV, ARSF). Ne comptent dans aucun de ces tableaux : l'allocation de vieillesse (RAV, versée une seule fois, qui va dans T50), l'allocation de survivants (RAS, RASC, RASO, RASA, versée une seule fois, qui va dans T51), l'allocation d'incapacité (AAI, pour un taux sous 15 %, qui va dans T53), et les nouvelles pensions d'invalidité (RPI) et d'ascendant de la branche des pensions (RPA), qui n'ont pas de tableau des nouveaux.

**Exemple.** La rente d'orphelin d'un enfant dont le père est mort d'un accident du travail compte dans T46 et dans T45. Une veuve qui reçoit une allocation de survivants, versée une seule fois, ne compte pas dans T43 : elle va dans T51.

**Sources.** Annuaire CIPRES p. 15 et 16 (descriptions), p. 42 à 45 (maquettes ; la grille de T45 commence à « moins de 15 ans »), p. 62 (allocation de vieillesse versée en une fois) et p. 65 (rente) ; Recueil v7, l. 202 (« Nouveaux crédirentiers : rentiers »), repris pour T46 à T49 (l. 210, 216, 224, 230), et l. 326 (allocation de survivants) ; Kaya 2017, tableau 1, l. 194 à 242 (T45 à T49 rangés avec les AT/MP) ; Annuaire CNSS 2024, tableau 24 (allocations de vieillesse publiées à part), tableau 28 (allocations de survivants publiées à part : 197 906 991 FCFA en 2024), tableau 77 (ascendants des rentes AT/MP) ; README, table de correspondance ; fiche T42, règle 1 ; Données P82, P87, P93 (81 parts de conjoint, 472 d'orphelin, 18 d'ascendant d'allocation de survivants en 2024).

**État.** Proposée. T45 = total de la branche : appuyé par le recueil (l. 202) et la grille de l'Annuaire CIPRES ; l'annuaire CNSS ne publie pas ce total, il ne peut donc pas le confirmer. T47 = rentes d'ascendant : appuyé par Kaya 2017 et par le tableau 77, mais avec un écart (15 pour 13, Q7). Retraite anticipée dans T42 : proposition BI, à confirmer (Q13 ; points_a_valider n° 18 et n° 34).

### R5 — La personne comptée

**Règle.** On compte la personne qui a droit à la prestation (dans la base, `IND_ID_PREST` de la table `PRESTATION_ESP`), une fois par tableau : la veuve, l'orphelin lui-même (pas le tuteur qui touche l'argent), le parent, la victime. Une personne peut être dans deux tableaux (T43 et T49, par exemple) : on n'additionne jamais deux tableaux pour compter des personnes.

**Exemple.** Un orphelin de 8 ans dont la pension est versée à sa mère compte dans T44, tranche « 5 – 9 ans », colonne des hommes. La mère n'est pas comptée.

**Sources.** Annuaire CIPRES p. 65 (« pensionné orphelin : enfant bénéficiant d'un droit à pension ») et grilles de T44 et T46 (« moins de 5 ans ») ; Annuaire CNSS 2024, tableau 20 (les moins de 25 ans font exactement les 9 478 orphelins du tableau 18 : l'orphelin compte à son âge) et tableau 77 (nouveaux classés « selon la nature du bénéficiaire ») ; README R5 ; Données P16 (la personne payée d'une pension d'orphelin est toujours le tuteur), P82 (T46 : 44 orphelins pour 44 publiés), P77 (doublons rares dans un même tableau).

**État.** Proposée ; confirmée par l'annuaire CNSS 2024. Le recueil dit autre chose pour T41 et le reprend par renvoi : voir N4 et Q1.

### N4 — Les survivants, malgré « Idem. Tab 41 »

**Règle.** Pour T43 à T49, le renvoi « Idem. Tab 41 » se lit « même périodicité » (mois, trimestre, année). La définition de T41 (« Allocataire : titulaire du dossier (assuré) ») ne change pas la personne comptée : on garde l'ayant droit (R5).

**Exemple.** Les trois orphelins d'un ouvrier mort d'un accident du travail sont trois nouveaux rentiers de T46, chacun à son âge. L'ouvrier n'est pas compté. Lue à la lettre, la phrase de T41 ferait compter l'ouvrier, avec son âge et son sexe à lui.

**Sources.** Dans l'ordre des sources : la définition de l'allocataire vaut pour les allocations familiales (Annuaire CIPRES p. 62 : « en matière de prestations familiales » ; T41 est le tableau des allocataires de la branche PF, p. 14 ; Kaya 2017, tableau 1, l. 120 : « c'est le dossier de l'allocataire qui est considéré et non le nombre de prestataires ») ; Recueil v7, l. 202 (« Nouveaux crédirentiers : rentiers ») ; Annuaire CNSS 2024, tableau 77 (4 veuves, 44 orphelins, 13 ascendants : la CNSS compte le survivant). Dans la base, pour une rente de survivant, la personne de la prestation est le survivant, et le titulaire du dossier (`DOSSIER.IND_ID`) reste la victime (schéma `cheminement_rente`). Pour T43 et T44, le titulaire n'est le défunt que si la pension du survivant est rangée dans le dossier du défunt : environ la moitié des pensions de veuve (13 104 sur 25 541, lot 3 B, P51) ; les autres veuves seraient comptées à leur nom.

**État.** Nouvelle, à confirmer par la CNSS (Q1). La même phrase « Idem. Tab 41 » est dans T32 à T38 (l. 188, 196, 264, 271, 277, 284, 290) : la question se pose une seule fois pour T32 à T49.

### R7 — Le nouveau bénéficiaire (l'entrée)

**Règle.** Est nouvelle dans un tableau la personne dont la toute première prestation de ce tableau est saisie dans l'application pendant la période (date de saisie `PE_DATE_INSERT`). Une révision ou un recalcul, c'est-à-dire une nouvelle prestation saisie pour une personne qui en avait déjà une, n'est pas une entrée. Un nouveau qui meurt ou sort avant le 31/12 compte quand même : une entrée de l'année reste une entrée. Une pension de coordination payée par l'autre caisse compte aussi.

**Exemple.** Une pension de veuve saisie le 18/12/2024 et notifiée le 10/01/2025 fait une nouvelle veuve de 2024. Une pension recalculée en 2025 pour un retraité de 2018 ne fait pas un nouveau retraité.

**Sources.** Annuaire CIPRES p. 15 (« nouveaux bénéficiaires de l'exercice », pour « définir la loi des entrées ») ; Annuaire CNSS 2024, tableau 23 (4 037 nouveaux pensionnés publiés ; la base a 4 090 pensions saisies en 2024 et 3 888 notifiées, A03) et tableaux 75 et 77 (rentes : 102 nouveaux en « première fois » pour 105 publiés, 131 avec les recalculs) ; README R7 (revue le 30/10/2026) ; fiche T42, règles 2 et 3 ; Données P82.

**État.** Proposée. La saisie et la « première fois » suivent la pratique de l'annuaire CNSS 2024 ; le recueil, Kaya 2017 et l'Annuaire CIPRES ne disent pas quelle date fait l'entrée. Contre-indice : la feuille 03 des modèles CNSS et les délais du recueil (l. 1174, 1224) datent la liquidation d'une première demande à la notification. À confirmer (Q2 ; points_a_valider n° 4).

### N1 — La période choisie et la périodicité

**Règle.** Le tableau se calcule pour une période choisie : un mois, un trimestre, une année, ou entre deux dates libres. Est nouvelle la personne dont la toute première prestation du tableau, dans tout l'historique, est saisie entre la date de début et la date de fin. Comme une personne n'entre qu'une fois, les effectifs des mois s'additionnent en trimestre et en année ; les montants, non (N3).

**Exemple.** Une pension saisie le 15/03/2025 compte dans le tableau de mars 2025, dans celui du 1er trimestre 2025 et dans celui de l'année 2025, jamais dans un autre mois.

**Sources.** Recueil v7, l. 299 (T42 : mensuelle, trimestrielle, annuelle), l. 203, 305 et 312 (« Indiquer la période (début, fin) en paramètre » : T45, T43, T44), l. 210, 216, 224, 230 (« Idem. Tab 45 » : T46 à T49), l. 153 (périodicité de T41, reprise par « Idem. Tab 41 », l. 204, 306, 313), l. 35 ; remarques B32 et B33 du recueil. Kaya 2017 et l'Annuaire CIPRES disaient « fin d'exercice » (p. 15) : le recueil, plus fort, l'emporte.

**État.** Nouvelle, demandée par le recueil ; à intégrer. Les réglages `debut` et `fin` existent dans la CTE `parametres`, mais ils sont tirés de l'exercice donné au lancement. Pas encore testée sur la base.

### R8 — L'âge et le sexe

**Règle.** Âge = année de l'exercice moins année de naissance, rangé dans la grille du tableau. T42 et T47 : moins de 50 ans, puis des tranches de 5 ans jusqu'à « 100 ans et plus ». T43 : moins de 20 ans … « 60 ans et plus ». T44 et T46 : moins de 5 ans … « 20 ans et plus ». T45 : moins de 15 ans … « 100 ans et plus ». T48 et T49 : moins de 20 ans … « 65 ans et plus ». Un âge négatif, vide, ou de 120 ans et plus (l'année 1900 sert de date de naissance par défaut) va en « âge inconnu ». Sexe : celui de la fiche de la personne (1 homme, 2 femme).

**Exemple.** Né en 1965, retraité en 2025 : 60 ans, tranche « 60 – 64 ans » de T42.

**Sources.** Annuaire CIPRES p. 62 (âge = année de constatation moins année de naissance) et maquettes p. 42 à 45 ; Kaya 2017, tableau 1, l. 203 et 213 (T45 et T46 : « que les services techniques s'appliquent pour une saisie correcte des données » ; pour T29, l. 177, il précise « sexe et âge ») ; Annuaire CNSS 2024, tableau 25 ; README R8 ; décision du 31/10/2026 (120 ans et plus) ; fiche T42, règles 4 à 6 ; Données P83, P84 (aucun âge inconnu, aucun sexe inconnu parmi les nouveaux de 2024).

**État.** Proposée. Par tranche de 5 ans, confirmée de 55 à 69 ans (écarts de 1,6 % au plus), sur une population qui garde les révisions (2 506 personnes, P83) : à refaire sur les seules premières fois (lot 10). Âge par âge et sexe : écarts avec le tableau 25 (Q8, Q9).

### N2 — L'âge sur une période choisie

**Règle.** Âge = année de l'entrée (la saisie) moins année de naissance. Sur une année civile, c'est exactement R8.

**Exemple.** Né en 1968, pension saisie le 10/02/2025 : 57 ans, dans le tableau de février 2025 comme dans celui de 2025. Sur la période de juillet 2024 à juin 2025, une personne entrée en octobre 2024 prend son âge de 2024.

**Sources.** Annuaire CIPRES p. 62 (« année de constatation » d'un fait : ici, l'entrée) ; R8 ; Recueil v7, l. 203, 305, 312 (période libre).

**État.** Nouvelle, proposition BI ; à faire valider (Q16).

### R3 — Le montant

**Règle.** Le montant est ce que la CNSS a payé : les paiements (table `DEBOURS`, type PM, c'est-à-dire paiement) au statut D (payé et rattaché à un titre de paiement, appelé « effet » dans la base) ou P (payé sans ce titre), pour les prestations du tableau saisies dans la période. Une pension payée par l'autre caisse (`COOR_AC`) compte sans montant (N6). Les accessoires (A1) n'y sont pas. Le rachat de rente (un capital versé à la place de la rente, enregistré dans la base comme un ajustement et non comme un paiement PM) n'y est pas non plus.

**Exemple.** En 2024, les personnes saisies dans T42 ont reçu 2,86 Md pour les mois de 2024 ; 321 d'entre elles n'ont rien reçu, dont 234 payées par l'autre caisse. Ces chiffres portent sur 2 510 personnes saisies, révisions comprises : celui de R7 sera un peu plus bas.

**Sources.** Annuaire CIPRES p. 15 (« montants servis ») ; Recueil v7, l. 263 et Kaya 2017, l. 292 (coordination : la part CNSS) ; Annuaire CNSS 2024, tableaux 19 et 28 (pensions) et tableau 90 (rentes) ; README R3 ; fiche T42, règle 7 ; Données P85, P98, P104 (rachats de 2024 payés par ajustement).

**État.** Pensions : les montants de la base retombent sur l'annuaire à 0,7 à 2,5 % près pour le stock (P98). Rentes : 12 à 25 % sous l'annuaire (tableau 90), cause inconnue ; les montants de T45 à T49 ne sont pas fiables tant que la DSI n'a pas répondu (Q5). L'annuaire ne publie aucun montant pour les nouveaux.

### A1 — Les accessoires de pension

**Règle.** Une bonification ou majoration pour enfant (RBEC, RME, RME-COL) ou une contribution aux charges du ménage (RCCM) s'ajoute à une pension. Elle ne fait pas un nouveau pensionné, et elle n'entre pas dans le montant de T42.

**Exemple.** Un nouveau retraité qui touche aussi la bonification pour enfant compte une fois ; le montant de T42 ne garde que sa pension.

**Sources.** Carte A1 de `regles_prestations` et README de la comparaison à l'annuaire ; Annuaire CNSS 2024, tableaux 19 et 28 (le montant des pensionnés de vieillesse est la seule pension, 38 235 880 033 FCFA ; la majoration pour enfant, 537 343 000 FCFA, est publiée à part) ; Données P34, P98 (bonification 2024 : 554 254 000 FCFA dans la base).

**État.** Prouvée pour les personnes. Pour le montant, c'est une précision nouvelle tirée de l'annuaire CNSS 2024 : elle répond à la question du README (« montant à rattacher à la pension ? ») ; à faire valider.

### R4 — La date d'un paiement (réglage `date_montant`)

**Règle.** 'MOIS' (valeur retenue aujourd'hui) : on garde les paiements des mois de la période ; le rappel des mois d'avant n'est pas dans le montant (N8 propose de le montrer à part). 'SAISIE' : on garde les paiements saisis dans la période (un paiement est préparé environ trois semaines avant d'être versé), rappel et pension de janvier suivant compris.

**Exemple.** Personnes saisies dans T42 en 2024 : 2,86 Md avec 'MOIS', 3,22 Md avec 'SAISIE', dont 0,23 Md de rappel et environ 0,28 Md de pensions de janvier 2025 préparées en décembre.

**Sources.** README R4 ; points_a_valider n° 1 et n° 45 ; Annuaire CNSS 2024 : le profil mois par mois des montants publiés (tableaux 28, 41, 90) suit la date de saisie des paiements (corrélation 0,75 contre −0,12 pour les pensions, 0,84 contre −0,08 pour les AT/MP, lot 9 P105), et pour les rentes de survivants, la saisie colle mieux au tableau 90 (−12,5 % contre −25,1 % au mois payé) ; Données P85, P92, P98.

**État.** En attente de la CNSS (question posée au point du 02/11 ; Q3). L'annuaire penche vers 'SAISIE' sans trancher : il ne publie aucun montant de nouveaux.

### N8 — Le rappel, montré à part

**Règle.** À côté du montant, le tableau garde une colonne « rappel » : ce qui est payé aux nouveaux de la période pour des mois d'avant le début de la période. La requête ne la sort pas aujourd'hui, alors que la fiche T42 (règle 7) dit le rappel « montré à part ».

**Exemple.** Un retraité saisi en février 2024, dont la pension est due depuis octobre 2023 : les mois d'octobre à décembre 2023 vont dans la colonne « rappel » de T42 de 2024, pas dans son montant (avec 'MOIS').

**Sources.** README R4 ; fiche T42, règle 7 ; Données P85 (le rappel vaut 8 % du montant de l'année pour T42, 25 % pour T43, 39 % pour T44, 198 % pour T46).

**État.** Nouvelle, proposition BI ; à ajouter dans la requête. Utile quelle que soit la réponse à Q3.

### N3 — Le montant sur une période choisie

**Règle.** Le montant d'un nouveau est ce qui lui est payé pour les mois de la période (ou saisi dans la période si `date_montant` = 'SAISIE'). Les montants des mois ne s'additionnent pas pour faire l'année : un nouveau de mars pèse un mois dans le tableau de mars, dix dans celui de l'année.

**Exemple.** Un retraité entré le 15/03/2025 avec 100 000 FCFA par mois : 100 000 dans le tableau de mars et dans celui du 1er trimestre, 1 000 000 dans celui de 2025 (de mars à décembre).

**Sources.** Annuaire CIPRES p. 15 (« montants servis ») ; R3, R4 ; Recueil v7, l. 299 (T42 par mois) et l. 159 (T54 : « Montant : Montant liquidé », une autre lecture possible : le montant accordé).

**État.** Nouvelle, proposition BI ; à faire valider (Q4).

### N6 — Le montant des pensions de coordination

**Règle.** Pour T42 à T44, le montant d'une pension de coordination est la part CNSS, lue comme ce que la CNSS a payé : zéro quand l'autre caisse paie (`COOR_AC`).

**Exemple.** Un retraité dont la pension est payée par la caisse d'un pays voisin compte dans T42, avec un montant nul.

**Sources.** Recueil v7, l. 298 (T42 : « prendre uniquement la partie CNSS », sans dire si c'est pour les mois ou pour le montant) et, par analogie pour T43 et T44, l. 263 (T32 : « les montants concernent uniquement la partie CNSS ») ; Kaya 2017, l. 292 et 349 ; décision proposée le 31/10/2026 pour T32 à T36 (journal), étendue ici par proposition BI ; remarque B07 du recueil ; Données P82 (234 nouveaux retraités `COOR_AC` en 2024), P85.

**État.** Proposée. Deux points restent ouverts : la « partie CNSS » de T42 vaut-elle pour les mois, pour le montant, ou pour les deux (Q11) ; quand la CNSS paie une pension de coordination (`COOR_CNSS`), son paiement comprend-il la part de l'autre caisse (Q12) ?

### R9 — Les services passés (T42)

**Règle.** Total, par tranche et par sexe, des mois cotisés (`PE_MOIS_COTISATION`) de la dernière pension saisie dans la période, de 1 à 600 mois. Pour une pension de coordination, on retire les mois cotisés dans l'autre caisse (`PE_COT_AUTRE`) : il reste la part CNSS.

**Exemple.** 300 mois au total, dont 120 dans l'autre caisse : 180 mois comptés.

**Sources.** Recueil v7, l. 296 à 298 ; Kaya 2017, l. 348 et 349 ; Annuaire CIPRES p. 66 (services passés = total des périodes d'activité salariée) ; README R9 ; fiche T42, règle 8 ; Données P86 (jamais vide de 2020 à 2025, médiane 270 mois pour les dossiers ordinaires de 2024), P89, P90.

**État.** Proposée ; conforme au recueil pour le comptage en mois. `PE_COT_AUTRE` est rempli pour les 234 pensions `COOR_AC`, pour 118 des 123 `COOR_CNSS` (P86) et pour 3 autres dossiers (P89) ; il est plus petit que le total en moyenne pour les valeurs fréquentes (P90), pas vérifié ligne par ligne. La requête le retire partout où il est rempli : à limiter aux pensions de coordination (partie 9). Son sens reste à confirmer (Q10). Le total de 2024 (738 743 mois, sur les 2 510 personnes saisies, révisions comprises) est compté avant ce retrait.

### N7 — Le degré moyen d'IPP (T48)

**Règle.** Pour chaque tranche et chaque sexe : somme des taux d'incapacité permanente partielle (`PE_TAUX_INCAPACITE`) des nouveaux rentiers, divisée par leur nombre. Le taux d'une personne est celui de sa dernière rente viagère saisie dans la période.

**Exemple.** Trois hommes de 40 à 44 ans, à 20 %, 30 % et 40 % : degré moyen de 30 %.

**Sources.** Recueil v7, l. 222 et 223 ; Annuaire CIPRES p. 15 et 45 ; Données P87 (taux jamais vide, de 15 à 100, médiane 24 en 2024 ; sous 15 %, c'est une allocation d'incapacité, P72).

**État.** La moyenne suit le recueil. Prendre « la dernière rente de l'année » est un choix BI : pour une loi des entrées, le taux à l'entrée se défend aussi (Q15). L'effet est minime : parmi les 39 nouveaux de 2024, un recalcul dans l'année touche au plus 1 personne (69 rentes pour 68 personnes, P87).

### N5 — La direction régionale et l'agence

**Règle.** Chaque nouveau est rattaché à la direction régionale (DR) de son dossier, et à son agence, qui est le service du dossier (`SP_NO`), comme dans la famille T25-T27 (N2). T42 à T49 peuvent alors se découper par DR (DRO Ouagadougou, DRB Bobo-Dioulasso, DRN Nord, c'est-à-dire Ouahigouya, DRF Fada N'Gourma, DRD Dédougou) et par agence.

**Exemple.** Tableau 23 de l'annuaire CNSS : 1 760 des 2 486 nouveaux retraités de 2024 sont à la DRO (70,8 %).

**Sources.** Recueil v7, l. 4 à 10, l. 29, l. 1065 (et les indicateurs « par DR et par agences », l. 1080 à 1265) ; remarque B34 du recueil ; Annuaire CNSS 2024, tableaux 23, 75 et 77 (nouveaux par DR), 22 et 34 (stock et montants des pensions par DR), 96 (montants des rentes par DR) ; Données A02 (stock 2024 avec la DR du dossier : ascendants justes dans les 5 DR, vieillesse à 0 à 4 %), P46 (les 20 services de la base).

**État.** Nouvelle, proposition BI ; à tester sur les nouveaux (lot 10). L'agence suit la réponse de la DSI à T25-T27 Q20.

### R13 — Le cliché du calcul

**Règle.** L'exercice N se calcule sur la copie de la base de fin mars N+1, comme l'annuaire de la CNSS. Pour T42 à T49, cela touche le montant (des paiements sont saisis en retard), presque pas l'effectif : la date de saisie d'une pension ne change pas.

**Exemple.** 5 % du montant de 2024 de T42 (149 M sur 2,86 Md) a été saisi en 2025 ou après.

**Sources.** README R13 ; Données P99 (444 décès de retraités de 2024 connus fin mars 2025, pour 447 publiés), P85.

**État.** Proposée le 01/11/2026 (points_a_valider n° 2). Le seul cliché disponible, 092026, porte la copie du 02/02/2026.

---

## 3. Les cas

| Cas | Ce que fait la règle | Pourquoi |
|---|---|---|
| 1. Une veuve dont la pension (RPSV) est saisie en mars 2024 dans le dossier de son mari retraité décédé, sans nouveau dossier | Comptée dans T43 de 2024, à son âge à elle, avec ce qui lui est payé pour les mois de 2024. Peu importe le dossier : on compte la personne | Compté (R7, R5) ; la moitié des pensions de veuve sont dans le dossier du défunt (P51) |
| 2. Un orphelin de 8 ans dont la pension (RPSE), saisie en 2024, est versée à sa mère, sa tutrice | Compté dans T44 de 2024, tranche « 5 – 9 ans », colonne des hommes ; la mère n'est pas comptée | Compté (R5 ; P16 ; annuaire CNSS, tableau 20) |
| 3. Les trois orphelins d'un ouvrier mort d'un accident du travail, rentes saisies en 2024 | Trois nouveaux dans T46 et dans T45, chacun à son âge. L'ouvrier n'est pas compté | Compté (N4, R5) ; lu à la lettre, « Idem. Tab 41 » ferait compter l'ouvrier (Q1) |
| 4. Une pension saisie le 18/12/2024 et notifiée le 10/01/2025 | Comptée dans T42 de 2024. Montant de 2024 : avec 'MOIS', la pension de décembre 2024, payée en janvier 2025 avec le rappel, compte si le cliché est pris après ce paiement ; avec 'SAISIE', rien. Si l'entrée était la notification, elle serait de 2025 | Question (R7 ; Q2 ; points_a_valider n° 4 ; fiche T42 Q1) |
| 5. Un retraité dont la pension démarre le 1er octobre 2023, saisie en février 2024, premier paiement avec un rappel d'octobre 2023 à janvier 2024 | Compté dans T42 de 2024. Montant : avec 'MOIS', les mois de 2024 seulement, le rappel de 2023 dans sa colonne à part (N8) ; avec 'SAISIE', tout ce qui est versé en 2024 | Selon un réglage (R4 ; N8 ; Q3) ; le rappel vaut 8 % du montant des retraités, 25 % des veuves, 39 % des orphelins (P85) |
| 6. Un retraité de 2018 dont la pension est recalculée en 2025 (une nouvelle pension est saisie) | Pas compté dans T42 de 2025 : ce n'est pas une entrée. Il reste dans T32, les pensionnés au 31/12 | Pas compté (R7) ; 220 cas en 2025, 47 en 2024 (P82 ; Q17) |
| 7. Un nouveau retraité, pension saisie en avril 2025, décédé en septembre 2025 | Compté dans T42 de 2025, avec ce qui lui a été payé ; absent de T32 au 31/12/2025 | Compté (R7) ; 35 cas en 2024 ; les chiffres de l'annuaire ne départagent pas (−1,0 % avec eux, −2,5 % sans) |
| 8. Un retraité qui a cotisé au Burkina et dans un pays voisin ; la CNSS instruit le dossier, l'autre caisse paie (`COOR_AC`) | Compté dans T42, sans montant ; ses services passés ne gardent que ses mois cotisés à la CNSS | Compté (R7, N6, R9) ; 234 cas en 2024 ; sans eux, T42 serait à −10,4 % de l'annuaire |
| 9. Un assuré qui n'a pas assez cotisé pour une pension : il reçoit une allocation de vieillesse, versée une seule fois (RAV) | Pas compté dans T42 : ce n'est pas une pension. Il va dans T50 | Pas compté (R2 ; annuaire CNSS, tableau 24 : 833 allocations publiées à part) |
| 10. Une veuve qui reçoit une allocation de survivants, versée une seule fois (RASC) | Pas comptée dans T43 : elle va dans T51 | Pas compté (R2 ; recueil l. 326 ; annuaire CNSS, tableau 28) |
| 11. Un assuré qui prend une retraite anticipée (RPRA), saisie en 2024, à 53 ans | Compté dans T42 de 2024, tranche « 50 – 54 ans », avec la vieillesse. L'annuaire CNSS les montre à part (3 en 2024) | Question (R2 ; Q13 ; points_a_valider n° 18 et n° 34) |
| 12. Une victime d'accident avec 10 % d'incapacité : elle reçoit une allocation d'incapacité (AAI), pas une rente | Pas comptée dans T48 ni dans T45 : elle va dans T53 | Pas compté (R2 ; P87 : aucune rente viagère sous 15 % ; P72) |
| 13. Une victime dont la rente viagère est saisie en mars 2024 à 20 %, puis recalculée en octobre 2024 à 30 % | Comptée une fois dans T48 de 2024, avec 30 % (la dernière rente de l'année). Recalculée en 2025, elle ne serait pas comptée en 2025 | Selon un réglage (N7, `taux_ipp` ; Q15) ; au plus 1 cas parmi les nouveaux de 2024 (P87) ; les 29 recalculs de 2024 portent sur des rentes plus anciennes |
| 14. La veuve d'un ouvrier mort d'un accident du travail, qui touche aussi une pension de veuve : rente ARSV et pension RPSV saisies en 2024 | Comptée dans T49, dans T45 et dans T43 : une fois par tableau | Compté (R5, R2 ; P77) : on n'additionne jamais deux tableaux pour compter des personnes |
| 15. La mère d'un retraité décédé, qui reçoit une pension d'ascendant de la branche des pensions (RPA), saisie en 2024 | Pas comptée : il n'y a pas de tableau CIPRES des nouveaux ascendants de la branche des pensions (7 en 2024, tableau 23). Le père d'une victime d'accident du travail (rente ARSA), lui, compte dans T47 et dans T45 | Pas compté (R2) |
| 16. Un orphelin de 26 ans dont la pension est saisie en 2024 | Compté aujourd'hui dans « 20 ans et plus » de T44. Au 31/12, un orphelin de 25 ans ou plus n'est plus compté (R6) : la règle des nouveaux n'a pas cette limite | À surveiller (R7 face à R6 ; réglage `orphelin_age_max` ; Q14) ; 2 cas en 2024 (P84) |
| 17. Une personne orpheline de rente AT/MP en 2015, puis victime d'un accident du travail en 2024 | Nouvelle dans T48 de 2024, mais pas dans T45 : sa première rente AT/MP date de 2015 | À surveiller (R7) : T45 peut alors être plus petit que T46 + T47 + T48 + T49 |
| 18. Un pensionné d'invalidité dont la pension deviendra une pension de vieillesse à l'âge de la retraite (le module n'existe pas encore) | Avec R7, il deviendrait un nouveau retraité de T42 l'année de la conversion, alors qu'il était déjà pensionné | Question (R7 ; recueil l. 270 ; Q18) |
| 19. Une pension de 100 000 FCFA par mois saisie le 15/03/2025, payée à partir de mars, vue par mois, trimestre et année | Comptée dans le tableau de mars 2025, du 1er trimestre et de l'année, jamais dans un autre mois. Montant : 100 000 en mars et au 1er trimestre, 1 000 000 sur l'année | Selon un réglage (N1, N3 ; réglages `debut` et `fin` ; Q4) |

---

## 4. Face à l'annuaire CNSS 2024

| Tableau de l'annuaire | Ce qu'on compare | Nous | Publié | Écart | Verdict | Ce que ça change |
|---|---|---|---|---|---|---|
| Tableau 23 (p. 19) | Nouveaux pensionnés de 2024 par type : la date d'entrée | Première fois, à la saisie : T42 2 463, T43 771, T44 773 (P82). Toutes pensions saisies : 4 090 ; notifiées : 3 888, dont vieillesse et anticipée 2 411, veuves et veufs 732, orphelins 734 (A03) | T42 2 489 (2 486 + 3 anticipées) ; T43 766 (732 + 34) ; T44 769 ; total 4 037 | Saisie : T42 −26 (−1,0 %), T43 +5 (+0,7 %), T44 +4 (+0,5 %), toutes pensions +53 (+1,3 %). Notification : −149 (−3,7 %), dont −78 (−3,1 %), −34 (−4,4 %), −35 (−4,6 %) | tranche | Les sources plus fortes ne disent pas quelle date fait l'entrée : la pratique de la CNSS est la saisie. R7 la garde ; Q2 demande une confirmation, à cause du contre-indice de la feuille 03. Les nouvelles pensions d'invalidité (6) et d'ascendant (7) n'ont pas de tableau CIPRES |
| Tableau 23, ligne vieillesse et anticipée | Les pensions de coordination payées par l'autre caisse | Avec : 2 463 ; sans les 234 `COOR_AC` : 2 229 | 2 489 | Avec −26 (−1,0 %) ; sans −260 (−10,4 %) | tranche | La CNSS compte la coordination payée ailleurs : on la garde (R7, N6) |
| Tableau 23, ligne vieillesse et anticipée | Les nouveaux sortis avant le 31/12, les révisions | Sans les 35 sortis : 2 428 ; avec les révisions : 2 510 | 2 489 | −61 (−2,5 %) ; +21 (+0,8 %) | précise | Les chiffres ne départagent ni les sortis ni les révisions. Les sortis restent comptés parce qu'une entrée de l'année reste une entrée (Annuaire CIPRES p. 15) ; les révisions, ce sont les rentes qui les départagent (tableaux 75 et 77) |
| Tableau 25 (p. 20), âges | Âge des nouveaux retraités de 2024, par tranche de T42 | Pensions de vieillesse saisies en 2024, révisions comprises (2 506 personnes, P83) : 55 – 59 ans 1 450 ; 60 – 64 ans 941 ; 65 – 69 ans 89 ; 70 ans et plus 25 | 1 429 ; 956 ; 89 ; 11 (2 486 personnes) | +21 (+1,5 %) ; −15 (−1,6 %) ; 0 ; +14 | confirme | R8 suffit par tranche, de 55 à 69 ans. Âge par âge, nos âges glissent souvent d'un an (56 ans : 266 pour 350 ; 60 ans : 377 pour 519 ; 61 ans : 353 pour 256). La CNSS calcule peut-être l'âge à la date de départ, mais elle compte 13 retraités de 55 ans ou moins quand nous en avons 53, ce qui va contre cette piste (Q9). À refaire sur les seules premières fois (lot 10) |
| Tableau 25 (p. 20), sexe | Sexe des nouveaux retraités de 2024 | 1 865 hommes, 641 femmes (25,6 %) ; nouvelles veuves et veufs : 737 femmes, 35 hommes (P83, P84) | 1 959 hommes, 527 femmes (21,2 %) ; veuves 732, veufs 34 (tableau 23) | Hommes −94 (−4,8 %) ; femmes +114 (+21,6 %) ; veuves +5, veufs +1 | écart à expliquer | Le sexe de la base retombe sur le stock (tableau 19, à moins de 3 %, P75) et sur les nouvelles veuves : l'écart est propre au tableau 25. On garde le sexe de la fiche (Q8) |
| Tableau 75 (p. 60) | Nouvelles rentes viagères de 2024 (T48), par âge et sexe | Première fois : 39 (par accident, c'est-à-dire par dossier : 39 aussi) ; notifiées : 57 ; saisies : 68 personnes (53 hommes, 15 femmes, de 30 à 84 ans, dont 9 de 65 ans et plus) (P82, P84, P91) | 44 (32 hommes, 12 femmes), tous de 25 à 64 ans | Première fois −5 (−11,4 %) ; notifiées +13 (+29,5 %) ; saisies +24 (+54,5 %) | écart à expliquer | Aucune règle testée ne donne 44 ; la première fois reste la plus proche, et l'absence de plus de 65 ans montre que la CNSS laisse de côté les recalculs de vieilles rentes. Deux variantes restent à tester avant de poser Q6 (lot 10). Sa grille est proche de celle de T48, sauf le haut (65 – 69 ans et 70 ans et plus) ; ses libellés « 20 – 25 », « 25 – 30 »… se lisent 20 – 24, 25 – 29 |
| Tableau 77 (p. 61) | Nouveaux ayants droit de rentes de 2024 : orphelins (T46), veuves et veufs (T49) | T46 44 ; T49 4 (4 femmes) | Orphelins 44 ; veuves 4, veufs 0 | 0 ; 0 | confirme | T46 et T49 tombent juste (R2, R5, R7) |
| Tableau 77 (p. 61) | Nouveaux ascendants de rentes de 2024 (T47) | 15 (15 saisis, 15 premières fois, 15 notifiés, P82) | 13 | +2 (+15,4 %) | écart à expliquer | La date n'y change rien. La population est bien celle des rentes AT/MP (R2) ; reste à regarder ces 15 personnes (lot 10) et à demander comment la CNSS compte (Q7) |
| Tableau 77 (p. 61), lecture du recueil | La personne comptée face à « Idem. Tab 41 » (« allocataire = titulaire du dossier ») | En comptant l'ayant droit (R5) : 44 orphelins, 15 ascendants, 4 veuves ; en comptant le titulaire du dossier, on compterait des victimes décédées | Nouveaux classés « selon la nature du bénéficiaire » : 44 orphelins, 13 ascendants, 4 veuves | pas chiffrable : les deux lectures ne comptent pas les mêmes personnes | précise | L'annuaire montre la pratique de la CNSS : elle compte le survivant. Il ne peut pas contredire le recueil, qui est plus fort : la décision reste à la CNSS (N4, Q1) |
| Tableaux 75 et 77 ensemble | Nouveaux rentiers AT/MP de 2024 (T45) et les recalculs | Première fois 102 (44 + 15 + 39 + 4) ; avec les recalculs 131 ; notifiées 123 | 105 (somme des deux tableaux, que la CNSS ne publie pas) | −3 (−2,9 %) ; +26 (+24,8 %) ; +18 (+17,1 %) | confirme | Confirme qu'une rente recalculée n'est pas une entrée (R7). Ne confirme pas le périmètre de T45, qui repose sur le recueil (l. 202) et la grille de l'Annuaire CIPRES ; et l'écart d'ensemble cache deux écarts de sens contraire (T48 −5, T47 +2) |
| Tableaux 18, 23 et 26 (p. 16, 19, 21) | Stock fin 2023 + nouveaux 2024 − décès 2024, face au stock fin 2024 | non calculé avec nos données | Veufs 298 + 34 − 3 = 329 (329) ; invalidité 116 + 6 − 3 = 119 (119) ; anticipée 75 + 3 − 1 = 77 (78) ; veuves 21 461 (21 438) ; vieillesse 31 081 (30 730) ; orphelins 9 454 (9 478) ; ascendants 54 + 7 − 89 = −28 (59) | Veufs 0 ; invalidité 0 ; vieillesse −351 ; orphelins +24 ; ascendants +87 | non comparable | Ce contrôle n'utilise que des chiffres publiés : il ne dit rien de la saisie ni du cliché. Juste pour les veufs et l'invalidité ; −351 pour la vieillesse (sorties autres que les décès) ; impossible pour les orphelins (environ 760 passent 25 ans chaque année) et pour les ascendants (89 décès pour 61 pensions au plus) (Q20) |
| Tableau 20 (p. 17) | Orphelins de pension au 31/12/2024, par âge | — | Les moins de 25 ans font exactement les 9 478 orphelins du tableau 18, dont 3 812 de 20 à 24 ans | — | confirme | Confirme R5 (l'orphelin compte à son âge) et la limite de 25 ans au 31/12 (R6). Montre aussi que des orphelins sortent à 25 ans chaque année : le stock d'orphelins ne peut pas monter de 791 en 2024 avec 769 entrées (Q20). Nos chiffres vont dans l'autre sens : 8 123 orphelins de moins de 25 ans fin 2023, 7 995 fin 2024 (A01) |
| Tableaux 80, 81, 75 et 77 (p. 60 à 62) | Stock des rentes fin 2023 + nouveaux 2024 − décès 2024, face au stock fin 2024 | T46 2024 : 44 | Viagères 1 246 + 44 − 21 = 1 269 (1 255) ; veuves et veufs 287 (285) ; ascendants 208 (207) ; orphelins 379 + 44 − 0 = 423 (485) | Orphelins +62 ; viagères −14 ; veuves et veufs −2 ; ascendants −1 | écart à expliquer | Sans effet sur T46 de 2024 (44 pour 44). Mais des orphelins de rente sont entrés dans le stock 2024 sans être des « nouveaux » publiés : à garder en tête pour T39 (Q20) |
| Tableaux 19 et 28 (p. 16 et 22) | Ce que la CNSS met dans le montant d'un pensionné | Pension de vieillesse 2024 (mois payé) 38 496 066 498 ; bonification pour enfant 554 254 000 (P98) | PRV (pension de retraite vieillesse) 38 235 880 033 ; majoration 537 343 000, publiée à part | Pension +260 186 465 (+0,7 %) ; bonification +16 911 000 (+3,1 %) | précise | Le montant de T42 (pension seule, A1) suit la pratique de la CNSS. L'annuaire ne publie aucun montant de nouveaux : il ne tranche ni R4 ni le rappel |
| Tableau 90 (p. 71) | Montants 2024 des rentes AT/MP (tout le stock) | Mois payé : rentes viagères 399 428 069 ; rentes de survivants 275 923 240. Saisie : 397 834 986 ; 322 631 157 (P98) | Rentes viagères 455 021 317 ; rentes de survivants 368 602 710 ; rachats 25 366 909 | Mois payé : −55 593 248 (−12,2 %) ; −92 679 470 (−25,1 %). Saisie : −12,6 % ; −12,5 % | écart à expliquer | Des paiements de rentes manquent dans la base (question déjà posée à la DSI dans le README) : les montants de T45 à T49 seront trop bas (R3, Q5). Pour les survivants, la saisie colle bien mieux, là où le rappel pèse le plus : indice pour R4. Le rachat de rente, payé par ajustement (P104), reste hors du montant |
| Tableaux 96 et 97 (p. 74) | Montants des rentes AT/MP par DR (2024) et de 2020 à 2024 | pas calculé | Rentes viagères à la DRO 285 513 230 ; rentes de survivants 224,7 M en 2020, 333,0 M en 2023, 368,6 M en 2024 | — | précise | Le tableau 96 servira à contrôler N5 sur les montants des rentes ; le tableau 97 à suivre la règle du montant sur cinq ans |
| Montants mensuels des tableaux 28, 41 et 90 | La date des paiements que la CNSS publie | Corrélation avec la base à la saisie 0,75 (pensions), 0,84 (AT/MP) ; au mois payé −0,12, −0,08 (lot 9, P105) | Pics en février, mai, août, novembre | — | précise | Le profil publié suit la saisie des paiements : appuie 'SAISIE' pour R4, sans trancher pour les nouveaux (Q3) |
| Tableau 24 (p. 19) | Allocations de vieillesse liquidées en 2024 | Hors T42 ; 831 liquidées (première notification), 873 saisies (P92) | 833 (DRO 507, DRB 201, DRN 53, DRF 46, DRD 26) | −2 (−0,2 %) ; +40 (+4,8 %) | confirme | Les allocations restent hors de T42 (R2 ; elles vont dans T50). Attention : la CNSS compte les allocations à la notification, les pensions à la saisie (tableau 23) |
| Tableau 28 (p. 22) | Allocations de survivants de 2024 | Hors T43 et T44 ; 81 parts de conjoint, 472 d'orphelin, 18 d'ascendant (P93) | 197 906 991 FCFA, ligne « Survivants » des allocations, à part des pensions | — | confirme | L'allocation de survivants reste hors de T43 et T44 (R2 ; elle va dans T51) |
| Tableau 27 (p. 21) | « Départs à la retraite » de 2024 | pas de chiffre comparable | 3 729 (janvier 557) ; 2 489 nouveaux retraités + 833 allocations = 3 322 dans les tableaux 23 et 24 | +407 (+12,3 %) face aux tableaux 23 et 24 | non comparable | Ce n'est pas T42. Ne pas s'en servir pour contrôler un T42 mensuel (T25-T27 Q21) |
| Tableau 76 (p. 60) | Stock des rentes viagères fin 2024, par DR, âge et sexe | pas calculé | 1 255 (1 061 hommes, 194 femmes) ; libellés « 20 – 24 ans » | — | précise | Éclaire les libellés du tableau 75. Avec les tableaux 75 et 81, il permettra un contrôle stock + entrées − sorties par tranche d'âge |
| Tableau 49 (p. 39) | Accidents mortels (« décès immédiats ») de 2020 à 2024 | — | 13, 11, 13, 11, 14 | — | non comparable | Un ordre de grandeur pour les nouvelles rentes de survivants : 14 décès en 2024, 61 nouveaux ayants droit (tableau 77) |
| Tableaux 22 et 34 (p. 18 et 28) | Pensionnés et montants des pensions par DR, fin 2024 | A02 (DR du dossier, stock) | 62 231 pensionnés ; montants par DR | Vieillesse à 0 à 4 % selon la DR ; ascendants exacts | précise | La DR du dossier donne des chiffres plausibles pour le stock : à tester sur les nouveaux, face aux tableaux 23, 75 et 77 (N5, lot 10) |
| Tableaux 23, 25, 75, 77 | Montants, services passés, degré d'IPP ; âge et sexe de T43 à T47 et de T49 | — | rien | — | pas publié | Ces colonnes ne se valident qu'avec la CNSS |

**En clair, ce que l'annuaire confirme ou tranche :**
- un nouveau se compte à la saisie (tableau 23 : 4 090 pensions saisies pour 4 037 publiées ; 3 888 à la notification) ;
- seulement la première fois : une rente recalculée n'est pas une entrée (tableaux 75 et 77 : 102 pour 105, 131 avec les recalculs) ;
- la coordination payée ailleurs est comptée (−1,0 % avec, −10,4 % sans) ;
- l'orphelin compte à son âge, et les orphelins et veuves de rente tombent juste (T46 44 pour 44, T49 4 pour 4) ;
- les allocations versées une seule fois (vieillesse, survivants) restent hors de ces tableaux ;
- l'âge par tranche de 5 ans retombe pour T42, de 55 à 69 ans.

**Ce qu'il précise sans trancher :** la personne comptée face à « Idem. Tab 41 » (la CNSS compte le survivant) ; le montant d'un pensionné, sans l'accessoire ; la date des paiements, qui penche vers la saisie ; les sortis et les révisions des pensions, que les chiffres ne départagent pas.

**Ce qu'il contredit :** rien de nos règles. Le contrôle « stock + entrées − décès » qui semblait tout confirmer ne prouve rien : il est faux pour les orphelins et les ascendants.

**Ce qu'il faut expliquer :** les rentes viagères (39 pour 44) ; les ascendants de rente (15 pour 13) ; le sexe des nouveaux retraités (641 femmes pour 527) ; les montants des rentes (12 à 25 % sous l'annuaire) ; la hausse des orphelins dans les stocks (+791 pour les pensions, +106 pour les rentes).

**Ce qu'il ne publie pas :** aucun montant de nouveaux, ni services passés, ni degré d'IPP, ni l'âge et le sexe des nouveaux veufs, veuves, orphelins et ascendants.

---

## 5. Les réglages

Ils s'écrivent dans la CTE `parametres`, le bloc de réglages en tête de `t42_t49_requete_proposee.sql`. Changer une valeur ne demande pas de réécrire la requête. Une décision attendue de la CNSS ne bloque rien : la valeur retenue s'applique en attendant. « À créer » : le choix est aujourd'hui écrit en dur dans la requête, ou n'existe pas.

| Réglage | Requête | Valeur retenue | Autre valeur | Ce que ça change | Qui décide |
|---|---|---|---|---|---|
| `date_montant` | t42_t49 | 'MOIS' : les paiements des mois de la période | 'SAISIE' : les paiements saisis dans la période, rappel et pension de janvier suivant compris | Personnes saisies dans T42 en 2024 : 2,86 Md avec 'MOIS', 3,22 Md avec 'SAISIE' (dont 0,23 Md de rappel et environ 0,28 Md de janvier 2025). Les effectifs ne changent pas | DCP (Q3 ; points_a_valider n° 1 et n° 45 ; posée au point du 02/11) |
| `debut` et `fin` | t42_t49 (existent, tirés de l'exercice) | 1er janvier et 31 décembre de l'exercice | deux dates libres : un mois, un trimestre, ou toute période (par exemple du 01/07/2024 au 30/06/2025) | Fixent qui est nouveau (première prestation du tableau saisie entre les deux dates) et quels paiements comptent (N1, N3) | tranché par le recueil (l. 203, 299, 305, 312) ; l'utilisateur du rapport choisit à chaque calcul |
| `annee` | t42_t49 (existe, l'exercice) | l'exercice, pour l'âge | avec une période libre : l'année de l'entrée de chaque personne (N2) | Sur une année civile, les deux donnent le même âge | DPC, avec la DCP (Q16) |
| `date_entree` (à créer) | t42_t49 (en dur : la saisie) | 'SAISIE' | 'NOTIFICATION' | T42 : 2 397 au lieu de 2 463, soit −66 (−2,7 %) ; face aux 2 489 publiés, −92 (−3,7 %). La variante notifiée de P82 garde les révisions ; une vraie variante « première pension notifiée dans l'année » reste à calculer (lot 10) | DCP (Q2 ; points_a_valider n° 4) ; l'annuaire CNSS compte à la saisie |
| `retraite_anticipee` (à créer) | t42_t49 (en dur : dans T42) | DANS_T42 : la retraite anticipée avec la vieillesse | HORS_T42 | Environ 4 nouveaux retraités de moins par an (4 retraites anticipées saisies en 2024, A03) | DCP (Q13 ; points_a_valider n° 18 et n° 34) |
| `orphelin_age_max` (à créer) | t42_t49 | aucune limite (comme aujourd'hui) | 25 : moins de 25 ans, comme au 31/12 (R6) | T44 : 2 nouveaux orphelins de moins en 2024 (25 à 29 ans, P84) | DCP (Q14 ; lié à points_a_valider n° 10) |
| `taux_ipp` (à créer) | t42_t49 (en dur : dernière rente de l'année) | DERNIER : le taux de la dernière rente viagère saisie dans la période | ENTREE : le taux de la première rente | Au plus 1 personne en 2024 (P87) | DP et DR (Q15) |
| `agence` (à créer) | t42_t49 (N5) | SERVICE : service du dossier (`SP_NO`) | LIEU_PAIEMENT | Le découpage par agence des huit tableaux ; la DR suit le dossier | DSI (T25-T27 Q20) |
| Le cliché (valeur donnée au lancement, pas un réglage de `parametres`) | t42_t49 | 092026 (copie du 02/02/2026) | fin mars N+1 pour l'exercice N (R13) | Touche le montant (5 % du montant 2024 de T42 saisi en 2025 ou après), presque pas l'effectif | BI, avec la DSI (points_a_valider n° 2) |

---

## 6. Les questions ouvertes

Chaque question a une valeur retenue par défaut (« En attendant ») : la réponse change un réglage ou une règle, pas le travail déjà fait. Les destinataires suivent le recueil v7 et l'annuaire CNSS 2024 : la DCP (Direction centrale des prestations) pour les pensions ; la DP et les DR (directions régionales) pour les AT/MP (DP : sigle du recueil, à faire préciser) ; la DES (sigle du recueil, ou la direction qui l'a remplacée) pour les indicateurs qu'elle suit ; la DSI (Direction des systèmes d'information) pour la base ; la DPC (Direction de la prospective et de la coopération, qui fait l'annuaire, avec son Service études et statistique) pour les chiffres publiés.

### Q1 — Pour un orphelin, une veuve ou un parent, la personne à compter est-elle bien l'ayant droit lui-même, et non l'assuré décédé titulaire du dossier ?
**À qui :** DCP, avec les auteurs du recueil.
**En attendant :** on compte l'ayant droit (R5, N4) ; « Idem. Tab 41 » est lu « même périodicité ».
**Pourquoi on la pose :** le recueil reprend pour T32 à T49 la définition de T41, « Allocataire : titulaire du dossier (assuré) ». Lue à la lettre, elle ferait compter le défunt, avec son âge, pour toutes les rentes de survivants et pour environ la moitié des veuves (celles dont la pension est dans le dossier du mari). Or l'allocataire est défini pour les allocations familiales (Annuaire CIPRES p. 62 ; Kaya 2017, T41), et l'annuaire CNSS 2024 compte les survivants (44 orphelins publiés, 44 chez nous). À poser une seule fois pour T32 à T49.
**Déjà posée :** nouvelle ; la remarque B05 du recueil étend la définition à T45 à T49 sans en tirer la conséquence.

### Q2 — Un nouveau pensionné ou rentier compte-t-il l'année où sa prestation est saisie, ou l'année où la décision lui est notifiée ?
**À qui :** DCP.
**En attendant :** la saisie (`date_entree` = 'SAISIE'), comme l'annuaire CNSS 2024.
**Pourquoi on la pose :** ni le recueil, ni Kaya 2017, ni l'Annuaire CIPRES ne le disent. L'annuaire compte à la saisie (4 090 pensions saisies pour 4 037 publiées ; 3 888 à la notification). Mais la feuille 03 de la CNSS et les délais du recueil datent la liquidation d'une première demande à la notification : une même pension peut entrer dans T42 de 2024 (saisie en décembre) et dans le délai de liquidation de 2025 (notifiée en janvier).
**Déjà posée :** points_a_valider n° 4 ; fiche T42 Q1 (où « 4 % de moins » est à lire −2,7 %) ; README R7.

### Q3 — Le montant d'un nouveau pensionné comprend-il le rappel des mois d'avant l'année, payé avec sa première pension ?
**À qui :** DCP.
**En attendant :** les mois de l'année seulement (`date_montant` = 'MOIS') ; le rappel dans une colonne à part (N8).
**Pourquoi on la pose :** le rappel pèse 8 % pour les nouveaux retraités, 25 % pour les veuves, 39 % pour les orphelins, et plus que l'année entière pour les orphelins de rente (198 %, P85). L'annuaire ne publie pas ce montant ; son profil mois par mois suit la saisie des paiements, ce qui penche pour 'SAISIE'.
**Déjà posée :** points_a_valider n° 1 et n° 45 ; fiche T42 Q4 ; README R4 ; posée au point du 02/11, en attente.

### Q4 — Dans un tableau mensuel ou trimestriel des nouveaux, quel montant voulez-vous : ce qui leur a été payé dans la période, ou le montant mensuel de leur pension ou de leur rente ?
**À qui :** DCP.
**En attendant :** ce qui est payé pour les mois de la période (N3).
**Pourquoi on la pose :** le recueil demande ces tableaux par mois, trimestre et année. Les effectifs des mois s'additionnent, pas les montants : un retraité entré en mars pèse un mois dans le tableau de mars, dix dans celui de l'année. Pour T54, le recueil parle de « montant liquidé » (l. 159).
**Déjà posée :** nouvelle (remarques B32 et B33 du recueil).

### Q5 — Où sont enregistrés les paiements de rentes AT/MP qui manquent dans la base (12 % des rentes viagères, 12 à 25 % des rentes de survivants en 2024) ?
**À qui :** DSI, avec la DP.
**En attendant :** les montants de T45 à T49 sont calculés sur ce que la base contient, avec la mention « incomplet ».
**Pourquoi on la pose :** l'annuaire publie 455,0 M de rentes viagères et 368,6 M de rentes de survivants en 2024 (tableau 90) ; la base en a 399,4 M et 275,9 M au mois payé (P98).
**Déjà posée :** README (question à la DSI : des paiements AT/MP manquent) ; même question que T32-T41 Q21 (une seule réponse vaut pour les deux familles et pour T50-T57 Q16).

### Q6 — Comment comptez-vous vos 44 nouvelles rentes viagères de 2024 ?
**À qui :** DP et DR, avec la DPC.
**En attendant :** la première rente viagère de la personne (39 en 2024).
**Pourquoi on la pose :** aucune règle testée ne donne 44 : première fois 39, par accident 39, notifiées 57, toutes les rentes saisies 68 (P82, P91). À poser après le lot 10, qui testera encore la première rente notifiée dans l'année et la première rente avec une date d'effet dans l'année.
**Déjà posée :** points_a_valider n° 48 ; README.

### Q7 — Votre tableau 77 compte 13 nouveaux ascendants de rente en 2024, la base en a 15 : comment les comptez-vous ?
**À qui :** DP et DR, avec la DPC.
**En attendant :** les 15 sont comptés.
**Pourquoi on la pose :** la date ne change rien (15 saisis, 15 premières fois, 15 notifiés, P82). Le lot 10 regardera ces 15 personnes (date d'effet, sortie, lien avec la victime).
**Déjà posée :** nouvelle (points_a_valider n° 47 ne pose que le périmètre).

### Q8 — Comment votre tableau 25 obtient-il le sexe des nouveaux retraités (527 femmes publiées pour 2024, 641 dans la base) ?
**À qui :** DPC (Service études et statistique).
**En attendant :** le sexe de la fiche de la personne (1 homme, 2 femme).
**Pourquoi on la pose :** le sexe de la base retombe sur le stock des retraités et sur les nouvelles veuves : l'écart est propre au tableau 25. Kaya 2017 demandait déjà, pour T45 et T46, « une saisie correcte des données » (pour T29, il précise « sexe et âge »).
**Déjà posée :** points_a_valider n° 44 ; fiche T42 Q3 ; README.

### Q9 — À quelle date calculez-vous l'âge des nouveaux retraités dans votre tableau 25 ?
**À qui :** DPC (Service études et statistique).
**En attendant :** année de l'exercice moins année de naissance (R8).
**Pourquoi on la pose :** par tranche de 5 ans, l'âge retombe (55 – 59 ans : 1 450 pour 1 429) ; âge par âge, non (60 ans : 377 pour 519). La date de départ expliquerait un glissement d'un an, mais vous comptez 13 retraités de 55 ans ou moins quand la base en a 53, ce qui va contre cette piste.
**Déjà posée :** points_a_valider n° 44 ; fiche T42 Q3 ; même question que T32-T41 Q10, pour les tableaux 19, 25 et 36 (une seule réponse vaut pour les deux familles).

### Q10 — Pour une pension de coordination, la colonne `PE_COT_AUTRE` donne-t-elle le nombre de mois cotisés dans l'autre caisse, déjà compris dans le total des mois ?
**À qui :** DCP, à défaut la DSI.
**En attendant :** services passés = total des mois moins `PE_COT_AUTRE`, pour les pensions de coordination.
**Pourquoi on la pose :** le recueil (l. 298) et Kaya 2017 demandent la seule part CNSS. `PE_COT_AUTRE` est rempli pour presque toutes les pensions de coordination, plus petit que le total en moyenne (P86, P89, P90) ; aucun libellé ne le confirme.
**Déjà posée :** points_a_valider n° 11 ; fiche T42 Q5 ; README.

### Q11 — Pour T42, la « partie CNSS » d'une pension de coordination vaut-elle pour les mois de travail, pour le montant, ou pour les deux ?
**À qui :** DCP.
**En attendant :** pour les deux : mois sans l'autre caisse (R9), montant payé par la CNSS (N6).
**Pourquoi on la pose :** la phrase du recueil pour T42 (l. 298) ne dit pas lequel ; celle de T32 (l. 263) parle des montants.
**Déjà posée :** nouvelle.

### Q12 — Quand la CNSS paie elle-même une pension de coordination, son paiement comprend-il la part de l'autre caisse ?
**À qui :** DCP.
**En attendant :** le montant est ce que la CNSS a payé (N6).
**Pourquoi on la pose :** si la CNSS paie toute la pension puis se fait rembourser, son paiement dépasse sa part, et le montant de T42 serait trop haut pour ces pensions (`COOR_CNSS` : 123 retraités saisis en 2024, révisions comprises, P86).
**Déjà posée :** nouvelle.

### Q13 — La retraite anticipée compte-t-elle dans T42, avec les nouveaux retraités de vieillesse ?
**À qui :** DCP.
**En attendant :** oui (`retraite_anticipee` = DANS_T42), comme dans T32.
**Pourquoi on la pose :** le recueil veut voir la retraite anticipée à part dans T25 et T28 ; l'annuaire CNSS la publie à part (3 nouvelles en 2024). T42 n'a pas de ligne par type : la mettre avec la vieillesse est une proposition BI. Environ 4 personnes par an.
**Déjà posée :** points_a_valider n° 18 et n° 34 ; fiche T32 Q2.

### Q14 — Un orphelin de 25 ans ou plus peut-il recevoir une nouvelle pension d'orphelin ?
**À qui :** DCP.
**En attendant :** il est compté, dans « 20 ans et plus » de T44 ou de T46 (`orphelin_age_max` : aucune limite).
**Pourquoi on la pose :** 2 nouveaux orphelins de 25 à 29 ans en 2024 (P84). Au 31/12, la CNSS ne compte que les orphelins de moins de 25 ans (R6, tableau 20), et le recueil supprime la tranche « 25 ans et plus » de T35.
**Déjà posée :** liée à points_a_valider n° 10 (âge limite des orphelins).

### Q15 — Pour le degré d'IPP de T48, faut-il prendre le taux de la victime à son entrée, ou son dernier taux de l'année ?
**À qui :** DP et DR.
**En attendant :** le dernier taux de l'année (`taux_ipp` = DERNIER).
**Pourquoi on la pose :** le recueil fixe la moyenne des taux, pas le taux à prendre quand une rente change dans l'année. Pour une loi des entrées, le taux à l'entrée se défend. Au plus 1 cas en 2024.
**Déjà posée :** nouvelle.

### Q16 — Pour une période à cheval sur deux années (par exemple de juillet 2024 à juin 2025), l'âge d'un nouveau se compte-t-il avec l'année de son entrée ?
**À qui :** DPC (Service études et statistique), avec la DCP.
**En attendant :** année de la saisie moins année de naissance (N2).
**Pourquoi on la pose :** le recueil demande une période libre ; le glossaire de l'Annuaire CIPRES (p. 62) prend « l'année de constatation », qui n'est plus unique sur une telle période.
**Déjà posée :** nouvelle.

### Q17 — Pourquoi 220 retraités qui avaient déjà une pension en ont-ils reçu une nouvelle en 2025 (47 en 2024) ?
**À qui :** DCP, avec la DSI.
**En attendant :** ils ne sont pas comptés comme nouveaux (R7).
**Pourquoi on la pose :** la règle « première fois » les écarte, et l'annuaire ne les compte pas pour les rentes. Mais leur nombre a été multiplié par près de cinq en un an : revalorisation, reprise de dossiers ? La réponse dira si R7 écarte bien des révisions.
**Déjà posée :** fiche T42 Q2 (sous la forme « une révision fait-elle un nouveau retraité ? »).

### Q18 — Quand une pension d'invalidité sera transformée en pension de vieillesse à l'âge de la retraite, l'assuré comptera-t-il comme nouveau retraité dans T42 ?
**À qui :** DCP, et la DSI qui doit développer le module.
**En attendant :** sans objet aujourd'hui : la conversion n'existe pas dans la base. Avec R7, il serait compté l'année de la conversion.
**Pourquoi on la pose :** le recueil (l. 270) et Kaya 2017 demandent cette conversion ; elle ferait entrer dans T42 des personnes déjà pensionnées (119 pensionnés d'invalidité fin 2024, tableau 18).
**Déjà posée :** nouvelle (remarque B31 du recueil).

### Q19 — Le taux de remplacement se calcule-t-il sur les nouveaux retraités de l'année : leur première pension divisée par leur salaire moyen des cinq meilleures années ?
**À qui :** DCP, avec la DES (sigle du recueil).
**En attendant :** pas calculé.
**Pourquoi on la pose :** le recueil donne la formule et le dernier revenu, mais ni la population (toutes les pensions en cours, ou les nouvelles) ni la source du salaire. La base garde un salaire moyen par pension (`PE_SAL_MOYEN`), pas encore étudié.
**Déjà posée :** même question que T32-T41 Q18, qui retient en attendant toutes les pensions directes en cours (une seule réponse vaut pour les deux familles) ; remarque B37 du recueil.

### Q20 — Comment calculez-vous le stock d'orphelins de 2024 ? Il monte de 791 pour les pensions et de 106 pour les rentes, alors que des orphelins atteignent 25 ans chaque année.
**À qui :** DPC (Service études et statistique).
**En attendant :** sans effet sur T44 et T46, qui comptent les entrées ; à garder en tête pour T35 et T39.
**Pourquoi on la pose :** orphelins de pension : 8 687 + 769 nouveaux − 2 décès = 9 454, pour 9 478 publiés, alors qu'environ 760 orphelins passent 25 ans chaque année (3 812 ont de 20 à 24 ans, tableau 20). Orphelins de rente : 379 + 44 − 0 = 423, pour 485. Nos stocks baissent : 8 123 puis 7 995 orphelins de moins de 25 ans (A01).
**Déjà posée :** nouvelle.

### Q21 — Pour T43, faut-il garder la dernière tranche « 60 ans et plus », ou la détailler (60 – 64, 65 – 69…) ?
**À qui :** DCP, avec la DPC.
**En attendant :** la maquette telle quelle.
**Pourquoi on la pose :** 413 des 772 nouveaux veufs et veuves saisis en 2024 (53 %) tombent dans « 60 ans et plus » (P84) : le tableau dira peu de leur âge. Le recueil a déjà fait modifier une grille (T35). Question facultative.
**Déjà posée :** nouvelle.

---

## 7. Les points d'attention

1. **Qui tranche quoi.** Le recueil v7 impose la période et la périodicité, la part CNSS des services passés, la moyenne de l'IPP et « crédirentier = rentier ». Kaya 2017 range T45 à T49 avec les AT/MP. L'Annuaire CIPRES donne les grilles et la définition de l'âge. L'annuaire CNSS 2024 tranche ce que les autres ne disent pas : la saisie comme date d'entrée, la première fois, la coordination comptée.
2. **Ne pas confondre l'Annuaire CIPRES** (le modèle des tableaux) et l'annuaire statistique de la CNSS 2024 (les chiffres publiés). Les pages de l'Annuaire CIPRES sont celles du fichier PDF, comme dans le reste du projet.
3. **Par mois, effectif et montant ne se comportent pas pareil.** Une personne n'entre qu'une fois : les douze mois s'additionnent pour l'effectif, pas pour le montant (N1, N3). Un découpage par mois, chaque personne rangée au mois de son entrée, convient donc aux effectifs de T42 à T49. La « limite du grain » du TODO (tâche 8, écrite pour T32 à T55) vaut pour les tableaux au 31/12, pas pour les entrées.
4. **'SAISIE' ne fait pas qu'ajouter le rappel :** il ajoute aussi la pension de janvier suivant, préparée en décembre. Personnes saisies dans T42 en 2024 : 3,22 Md = 2,71 Md de mois de 2024 saisis en 2024 + 0,23 Md de rappel + environ 0,28 Md de mois de 2025 (calcul sur P85 : 3,216 − 0,227 − (2,861 − 0,149)).
5. **Le montant d'une période close bouge encore,** pas l'effectif : 5 % du montant 2024 de T42 a été saisi en 2025 ou après. Le cliché de fin mars (R13) compte surtout pour les montants.
6. **Deux dates d'entrée coexistent à la CNSS :** la saisie pour les nouveaux pensionnés (tableau 23), la notification pour les allocations de vieillesse (tableau 24 : 831 pour 833). Une pension et une allocation saisies en décembre et notifiées en janvier tombent dans des années différentes.
7. **Les montants des rentes sont incomplets** (tableau 90 : 12 à 25 % sous l'annuaire). Ceux de T45 à T49 seront trop bas tant que la DSI n'a pas répondu (Q5). Le rachat de rente (25 366 909 FCFA publiés en 2024), payé par ajustement, n'entre pas dans le montant (R3) : voisin de T25-T27 Q11.
8. **T45 = T46 + T47 + T48 + T49 sous deux conditions :** que personne n'entre dans deux de ces tableaux la même année, et que personne n'ait déjà eu une autre rente AT/MP (cas 17). C'est vrai en 2024 : 102 = 44 + 15 + 39 + 4.
9. **Les comparaisons d'âge, de sexe, de montant et de mois** (P83 à P86) portent sur toutes les personnes saisies, révisions comprises (2 510 pour T42), et non sur les 2 463 nouveaux de R7. L'âge et le sexe des seules premières fois, et le total des mois après retrait de l'autre caisse, ne sont pas encore calculés.
10. **Coordination :** 5 nouveaux retraités `COOR_CNSS` sur 123 en 2024 n'ont pas de `PE_COT_AUTRE` : leurs services passés restent le total (P86). La requête retire `PE_COT_AUTRE` partout où il est rempli, y compris dans 3 dossiers qui ne sont pas de coordination (P89).
11. **Les maquettes de T42 et T48 n'ont pas de colonne Total** (hommes et femmes seulement) : la requête en sort une, à masquer. Une personne sans sexe connu n'aurait pas de colonne (aucune en 2024).
12. **T43 a une grille trop courte :** 53 % des nouveaux veufs et veuves de 2024 sont dans « 60 ans et plus » (Q21).
13. **Les nouvelles pensions d'invalidité (RPI, 6 en 2024) et d'ascendant de la branche des pensions (RPA, 7)** n'ont aucun tableau CIPRES des nouveaux : elles ne sont pas perdues, la maquette ne les prévoit pas. La phrase « pensions directes » du recueil (l. 1251) vaut pour le rapport démographique, pas pour T42.
14. **Le détail par personne** demandé par le recueil (l. 29, 51, 68) contient des données personnelles : son accès doit dépendre du profil (l. 72).
15. **Le recueil appelle « prestations ordonnancées » les prestations notifiées** (l. 1133) : c'est pour un ratio de ressources, pas la définition d'un nouveau pensionné ; R7 garde la saisie.
16. **Le tableau 27 de l'annuaire** (3 729 « départs à la retraite » en 2024) n'est pas T42 : il dépasse de 407 les nouvelles pensions et allocations de vieillesse. Ne pas s'en servir pour contrôler un T42 mensuel.
17. **Les documents existants, laissés tels quels :** le TODO (point 8) dit encore « nouveau = notifié dans l'année (R7) » ; `prestation_esp` (partie 5) dit « Nouveau pensionné : prestation notifiée » ; `cheminement_rente` redit « rente active et payée au 4e trimestre (R6) » ; l'index de `regles_prestations` résume P27 par « la notification marque l'entrée » ; la fiche T42 dit « 4 % de moins » à la notification (c'est −2,7 %) et le rappel « montré à part » (la requête ne le sort pas) ; la requête dit `PE_COT_AUTRE` « toujours plus petit que le total ». En vigueur : la saisie (R7) et, au 31/12, la pension en cours payée ou non (R6).
18. **Deux familles de remarques B.** Les remarques B01 à B41 du recueil (`remarques_besoins_v7`) n'ont rien à voir avec les cartes B1 à B25 de `regles_prestations` : ce document écrit toujours « remarque Bnn du recueil ».

---

## 8. Comment c'est vérifié

- **Sources relues pour ce document :** `besoins_v7.txt`, l. 1 à 35, 145 à 160, 186 à 232, 245 à 330, 1065, 1133, 1165 à 1265 ; `kaya.txt`, l. 113 à 120, 160 à 242, 292, 335 à 370 ; `annuaire_cipres.txt` (glossaire : allocataire, bénéficiaire, pensionné veuf, orphelin, ascendant, services passés ; titres des maquettes de T41 à T49) ; `annuaire_cnss_2024.txt`, tableaux 28 (ligne « Survivants » des allocations), 49 et 90 ; l'inventaire de l'annuaire CNSS 2024 et ses recoupements (`style_grh/travail/resultats`).
- **Recalculs dans les fichiers des lots :** P82 (entrées de 2020 à 2025 : première fois, saisies, notifiées, sortis, coordination) ; P83 (âges des nouveaux retraités : 55 – 59 ans 1 450, 60 – 64 ans 941, 2 506 personnes ; 53 retraités de 55 ans ou moins) ; P84 (413 des 772 nouveaux veufs et veuves dans « 60 ans et plus » ; 2 orphelins de 25 à 29 ans) ; P85 (rappel : 8 %, 25 %, 39 %, 198 % ; 321 retraités sans paiement) ; P98 (rentes de survivants : 275 923 240 au mois payé, 322 631 157 à la saisie, sommes des quatre codes) ; les écarts du tableau 23 (A03) et des tableaux 75, 77 et 90. Les autres chiffres viennent du dossier de la famille, recalculés par sa contre-vérification.
- **Lots utilisés :** 3 B (P51), 7 (P82 à P91), 8 (P92, P93, P98, P99), 9 (P104, P105) ; comparaisons A01, A02, A03. Aucune requête nouvelle n'a été lancée ; aucune table DTM n'a été lue.
- **Corrections reprises de la contre-vérification :**
  - *erreur* : le contrôle « stock + entrées − décès » (tableaux 18, 23, 26) n'est plus une preuve : verdict « non comparable », juste pour les veufs et l'invalidité, impossible pour les orphelins et les ascendants ; question Q20 ajoutée. Le tableau 77 ne « confirme » plus T47 : « confirme » pour T46 et T49, « écart » pour T47, question Q7. R3 : pensions à 0,7 à 2,5 %, rentes 12 à 25 % sous l'annuaire (tableau 90), question Q5, rachat de rente hors du montant. L'effet de la notification : −66 (−2,7 %), et non 4 %.
  - *manque* : deux variantes à tester avant Q6 (première rente notifiée dans l'année, première rente avec une date d'effet dans l'année : lot 10) ; l'allocation de survivants, frontière de T43 et T44 (R2, cas 10) ; la remarque de Kaya 2017 sur la saisie correcte (R8, Q8) ; la grille de T43 (point 12, Q21).
  - *imprécision* : tableaux 75 et 77 ensemble, la première fois confirmée, pas le périmètre de T45 (R2 ne cite plus l'annuaire CNSS pour T45) ; tableau 25, « confirmé de 55 à 69 ans » sur une population avec révisions, « peut-être » à la date de départ, avec le contre-exemple des 55 ans (Q9 séparée de Q8) ; tableau 77, lecture du recueil, verdict « précise » au lieu de « contredit », arguments dans l'ordre des sources (N4) ; une seule question « Idem. Tab 41 » pour T32 à T49, « environ la moitié des veuves » (Q1) ; `PE_COT_AUTRE` rempli pour 234 `COOR_AC`, 118 `COOR_CNSS` sur 123 et 3 autres dossiers, plus petit en moyenne seulement (R9, Q10) ; N6 cite le recueil l. 298 pour T42 et l. 263 par analogie, décision du 31/10 « proposée pour T32 à T36, étendue par proposition BI », questions Q11 et Q12 ; retraite anticipée dans T42 = proposition BI, la phrase « pensions directes » vaut pour le rapport démographique (R2, point 13) ; R5 ne cite plus le glossaire « bénéficiaire » (p. 62) ; N7, « dernière rente » = choix BI, question Q15, « au plus 1 cas en 2024 » ; décomposition du tableau 23 à la notification tirée de A03 seul ; R4, la colonne de rappel n'existe pas encore (règle N8) ; cas 4, montant selon 'MOIS' ou 'SAISIE' ; Q2 gardée comme vraie question, avec le contre-indice de la feuille 03 ; A1, précision nouvelle tirée des tableaux 19 et 28 ; T45 = somme des quatre tableaux sous deux conditions (point 8, cas 17) ; chiffres de P85 et P86 donnés avec « 2 510 personnes saisies, révisions comprises » ; tableau 75, grille « proche » de T48 ; tableau 23, coordination tranchée, sortis non départagés ; T47, le glossaire ne définit que le pensionné ascendant ; R4 sépare saisie et versement ; « loi des entrées » expliquée.
  - *demandes du recueil oubliées*, toutes reprises en partie 1 : délais de liquidation des pensions (l. 1217 à 1238) et des rentes de survivants (l. 1170 à 1179) ; allocation de survivants (l. 326) ; « Idem. Tab 41 » aussi dans T32 à T38 (N4, Q1) ; l'agence (N5, réglage `agence`).
  - *tableaux de l'annuaire oubliés*, repris en partie 4 : tableaux 90, 96, 97, 76, 20, 49, 22 et 34.
- **Deux points où ce document garde une autre version que la contre-vérification :**
  - **les identifiants N1 à N8.** La contre-vérification proposait de les renommer (R14 à R20), parce que N1 à N9 existent dans `reference_cipres_T25_T57.md` avec un autre sens. La règle commune à toutes les familles est de numéroter N1, N2… les règles nouvelles de chaque famille et de les citer « T42-T49 N3 » hors de ce document : on la suit, comme la famille pilote T25-T27.
  - **la question des révisions.** Le dossier la posait comme une confirmation ; l'annuaire la tranche pour les rentes (tableaux 75 et 77) et la règle s'applique aux pensions par cohérence. On garde une question (Q17) sur la cause des 220 révisions de 2025, qui est une vraie inconnue.
- **Un ajout du rédacteur :** le profil mois par mois des montants de l'annuaire, qui suit la saisie des paiements (recoupement de l'inventaire, lot 9 P105), est cité pour R4 ; il n'était pas dans le dossier.
- **Relecture (exactitude et clarté), 08/11/2026.** Toutes les lignes du recueil qui touchent T42 à T49 ont été relevées à nouveau (grep dans `besoins_v7.txt`, les 17 demandes F4 et les 32 transverses de `recueil_v7_toutes_les_demandes.json`, parties 5 et 6 de REPRISE.md, « recueil_oublie ») : les 31 lignes de la partie 1 sont toutes là, dans le md et dans le schéma, avec le même statut. Chaque correction de la contre-vérification est appliquée. Les chiffres ont été recalculés dans leur source : tableaux 18, 20, 23 à 28, 75 à 81 et 90 de `annuaire_cnss_2024.txt`, P82 à P87, P89, P93, P98, P99 des lots, A01 et A03 de la comparaison ; tous les écarts tombent juste. Corrections faites : la ligne du « montant liquidé » de T54 est la l. 159, pas la l. 158 (N3, Q4, schéma) ; Q8 attribuait à T45 et T46 le « sexe et âge » que Kaya 2017 écrit pour T29 ; la dernière tâche de la partie 9 marquait encore le point n° 47 « appuyé », alors que T47 est en écart (Q7) ; le point d'attention 3 prêtait au TODO une limite réservée aux tableaux au 31/12 (elle est écrite pour T32 à T55) ; Q12 donne les 123 `COOR_CNSS` comme « révisions comprises » ; le point n° 2 est ajouté à la liste des points_a_valider repris. Clarté : « effet » de paiement expliqué (R3) ; montant du cas 4 réécrit ; DES présentée en tête de la partie 6, et la phrase « une valeur par défaut » nuancée (certaines réponses changent une règle, pas un réglage) ; l'« En bref » dit qui fait quoi et ce que veut dire « coordination ». Aucun statut, aucun verdict, aucune source n'a changé.
- **Vérification de forme :** une seule question par point ; les questions déjà posées sont signalées avec leur référence ; aucun nom de personne, aucun numéro d'assuré ou de compte ; le schéma reprend les mêmes identifiants, statuts et verdicts, et le générateur ne donne aucun avertissement.
- **Critique d'exhaustivité et de cohérence entre les cinq familles, 08/11/2026.** Le recueil v7 (l. 115 à 335 et indicateurs l. 1018 à 1378) a été relu à nouveau face aux cinq documents de famille, à la liste du recueil (`besoins_v7_prestations.md`) et au document sur l'annuaire (`annuaire_cnss_2024_prestations.md`). Changements ici : les délais de liquidation des pensions (l. 1217 à 1238) et des rentes de survivants (l. 1170 à 1179) prennent le statut que leur donne la famille T25-T27, qui les traite (« à intégrer » ; « question », T25-T27 Q8 et Q9), au lieu de « pris en compte » : une même demande a désormais le même statut partout. L'« En bref » sépare ce que l'annuaire tranche (la saisie, la coordination : cartes « tranche » du tableau 23) de ce qu'il confirme. Q5, Q9 et Q19 disent qu'elles reprennent T32-T41 Q21, Q10 et Q18. Les verdicts de la partie 4 s'écrivent « écart à expliquer » et « pas publié », comme dans les autres familles. La satisfaction et le document de référence sont rangés dans la famille T50-T57. Les 31 lignes de la partie 1 restent.
- **Numéros des contrôles à écrire, 08/11/2026.** Trois familles réservaient P107 pour des contrôles différents. Les numéros sont maintenant distincts : P107 à P114 pour le lot 10 de T42-T49, contrôles (a) à (h) ; P115 à P119 pour T50-T57 (avant : P107 à P111) ; P120 et suivantes pour T28-T31 (avant : P107 et suivantes). Le document de l'annuaire suit (P115 à P117).

---

## 9. Ce qui reste à faire

| Tâche | Qui | État |
|---|---|---|
| Rendre `debut` et `fin` libres dans la CTE `parametres` (mois, trimestre, période quelconque) et calculer l'âge sur l'année de l'entrée (N1, N2, N3) | Claude, après accord | à faire |
| Ajouter la colonne du rappel (N8) | Claude | à faire |
| Ne retirer `PE_COT_AUTRE` que pour les pensions de coordination (R9) | Claude | à faire |
| Ajouter les réglages « à créer » : `date_entree`, `retraite_anticipee`, `orphelin_age_max`, `taux_ipp`, `agence` | Claude | proposé, à faire après visa |
| Ajouter la DR du dossier et l'agence comme découpage (N5), après le contrôle (b) du lot 10 | Claude | à faire |
| Rendre consultable la liste des nouveaux, une ligne par personne et par tableau (recueil l. 29, 51, 68), avec un accès par profil | Claude | à faire |
| Exécuter `t42_t49_requete_proposee.sql` et retrouver les chiffres du lot 7 pour 2024 (T42 2 463, T43 771, T44 773, T45 102, T46 44, T47 15, T48 39, T49 4) | toi (exécution), puis Claude | à faire |
| Lot 10, P107 à P114 : (a) âge et sexe des 39 nouvelles rentes viagères face au tableau 75 ; (b) nouveaux de 2024 par DR face aux tableaux 23, 75 et 77 ; (c) âge des nouveaux retraités à la date d'effet face au tableau 25 ; (d) âge et sexe des seules premières fois de T42 ; (e) total des services passés après retrait de `PE_COT_AUTRE`, et pensions où il est supérieur ou égal au total ; (f) remplissage de `PE_SAL_MOYEN` ; (g) première prestation notifiée dans l'exercice, et première prestation avec une date d'effet dans l'exercice, face aux tableaux 23, 75 et 77 ; (h) les 15 ascendants de rente de 2024 | Claude rédige, toi exécutes | à faire |
| Poser Q1 à Q21, en commençant par Q1, Q2, Q3, Q5 ; Q6 après le lot 10 | toi, avec la CNSS | à poser |
| Dès la réponse à Q3 : passer `date_montant` à 'SAISIE' si besoin | Claude | en attente |
| Si l'utilisateur le décide : reporter ces règles dans le README, la fiche T42, la fiche de lecture, `regles_prestations` et points_a_valider (n° 46 : T45 appuyé par le recueil ; n° 47 : T47 appuyé par Kaya 2017, avec l'écart de Q7 ; fiche T42 Q1 corrigée à −2,7 %) ; corriger les textes périmés (TODO point 8, `prestation_esp`, `cheminement_rente`, index de `regles_prestations`) et la remarque B05 | Claude, sur demande | non commencé (rien n'a été modifié) |
