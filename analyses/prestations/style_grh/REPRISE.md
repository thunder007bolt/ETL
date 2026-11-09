# Prestations à l'image du GRH : note de reprise

**Rédigée le 08/11/2026 vers 21 h**, quand l'utilisateur a signalé que la limite d'usage approchait (80 %). Avec cette note, on peut reprendre le travail sans la conversation.

## 1. La demande (08/11/2026)

1. **Le principe.** Faire pour les prestations (T25 à T57) ce qui a été fait pour la GRH : des règles claires, des cas, la comparaison avec l'annuaire, les réglages et les questions ouvertes.
2. **Le travail existant reste tel quel.** « Ce travail, tu le laisses tel quel. » On ne touche ni au README, ni à R1–R13, ni à D1–D14, ni aux requêtes proposées, aux lots, aux fiches, à la fiche de lecture, à `regles_prestations`, à `points_a_valider` ou à `pensions_2024`. Tout ce qui est nouveau va dans de **nouveaux fichiers**. Leur intégration aux règles existantes se **propose**, elle ne s'applique pas.
3. **Valider ou contredire avec l'annuaire CNSS 2024.** « On essaie donc d'infirmer ou bien de valider certains éléments sur la base donc du tableau de CNSSS. » Il ne s'agit pas de le recopier trait pour trait : il faut mettre en évidence ce que ses chiffres confirment ou contredisent.
4. **Le recueil des besoins v7 passe en premier.** « Il faut pas oublier le besoin V7 […] l'élément le plus important. » Toutes les demandes du recueil v7 doivent être prises en compte.
5. **Les points ouverts** sont identifiés et rendus clairement.
6. **La forme.** Les termes sont simples et les explications claires : « il faut que tout soit nickel ».

## 2. Où on en est

| Étape | État |
|---|---|
| Repérer le travail existant et les sources | fait |
| Gabarit commun des schémas par famille | fait : `excalidraw/sources/prestations/gabarit_famille.py`, testé : 381 éléments, 0 avertissement |
| Relever toutes les demandes du recueil v7 | fait et audité (voir partie 5) |
| Lire et contrôler l'annuaire CNSS 2024 | fait et audité (voir partie 7) |
| Un dossier par famille, contre-vérifié | fait : corrections des vérifications appliquées |
| Rédiger les règles, les schémas et la liste du recueil | **fait le 08/11** : sept documents et sept schémas (partie 10) |
| Vérifier l'affichage, journal, mémoire | **fait le 08/11** ; commité le 09/11 (branche fix/presta) |

### L'analyse des sources

- Workflow `wf_2c39174a-8f2` : 12 analyses, 0 erreur, terminé le 08/11 vers 21 h 10.
- Ses résultats sont copiés dans `travail/resultats/`. Le dossier `travail/` est ignoré par git : il a son propre `.gitignore`.

| Fichier | Contenu |
|---|---|
| `recueil_v7_toutes_les_demandes.json` | Chaque demande du recueil v7 : référence, ligne, tableaux, demande, en clair, famille F1–F5 ou transverse, catégorie, B-id. S'y ajoutent les éléments non relevés par point B et des remarques. |
| `annuaire_cnss_2024_inventaire_et_coherence.json` | Les tableaux de l'annuaire CNSS 2024 : n°, titre, page, années, ce qu'il compte, tableau CIPRES, contrôle interne, chiffres 2024. S'y ajoutent les recoupements, ce qui n'est pas publié et les modèles de ratios. |
| `dossier_f1.json` … `dossier_f5.json` | Un dossier par famille : tableaux, recueil (avec statut), règles, réglages, annuaire (avec verdict), modèles CNSS, cas, points ouverts, points d'attention, reste à faire. |
| `verification_f1.json` … `verification_f5.json` | La contre-vérification de chaque dossier : problèmes, avec la correction et la gravité ; demandes du recueil oubliées ; tableaux de l'annuaire oubliés ; jugement. **À appliquer avant d'écrire.** |
| `../journal_workflow.jsonl`, `../resultat_final_workflow.txt` | Copies brutes. `../extraire_resultats.py` refait `resultats/` depuis le journal du workflow. |

Les familles :
- **F1** : T25 à T27, les dossiers ;
- **F2** : T28 à T31, les contrôles et les indus (annuaire, tableaux 100 à 113 ; modèles de ratios, feuilles 04 à 06) ;
- **F3** : T32 à T41, les bénéficiaires au 31/12 ;
- **F4** : T42 à T49, les nouveaux bénéficiaires ;
- **F5** : T50 à T57, l'évolution, les branches et les décès.

Les autres fichiers de `travail/` :
- `sources_texte/` : les textes des sources (voir partie 9) ;
- `scratchpad/` : les calculs et scripts des analyses, dont le test du gabarit `scratchpad/gabarit/test_gabarit.py` ;
- `workflow_analyse_sources.js` : le script de l'analyse.

## 3. Ce qui reste à faire, dans l'ordre

Tout le travail demandé le 08/11 est fait (partie 10). Ce qui reste dépend de l'utilisateur ou de la CNSS :

1. **L'utilisateur relit** les cinq documents de famille, en commençant par leurs « En bref ». Il dit ce qu'on reporte dans l'existant (README, TODO, `regles_prestations`, fiches, requêtes proposées). Rien n'est reporté sans son accord.
2. **Trois choix de forme à valider** :
   - la retraite anticipée à part dans T32 (T32-T41 N4) et le montant de T32 sans les accessoires (T32-T41 N13), dits « tranchés » par la présentation de l'annuaire, sans chiffre (`annuaire_cnss_2024_prestations.md`, partie 3.6) ;
   - le « hors base » des normes sanitaires des SMI, qui est une proposition BI ;
   - le réglage `date_montant`, à décider une fois pour les quatre requêtes : MOIS en attendant, T50-T57 propose SAISIE (DPC 1).
3. **Poser les 108 questions à la CNSS**, par direction (`besoins_v7_prestations.md`, partie 8 : DCP 45, DP et DR 12, DES 5, DAI 16, DSI 12, DPC 17, DCR 1). Les plus attendues d'abord : DPC 1 et 2, DAI 1 à 3, DCP 26, DCP 1 et 2.
4. **Écrire les contrôles proposés** quand l'utilisateur le demande. Les numéros sont réservés le 08/11 pour ne pas se chevaucher :
   - A06 à A08 : déjà écrits, à lancer ;
   - P107 à P114 : le lot 10 de T42-T49, contrôles (a) à (h) ;
   - P115 à P119 : T50-T57 (date des paiements, montant liquidé, T57, décès immédiats, rachats dans T56) ;
   - P120 et suivantes : T28-T31 (table des contrôles).
5. **Commiter** seulement si l'utilisateur le demande.

## 4. Livrables prévus (nouveaux fichiers seulement)

**Les règles par famille**, dans `analyses/prestations/` :
- `regles_t25_t27_dossiers.md`
- `regles_t28_t31_controles_indus.md`
- `regles_t32_t41_beneficiaires.md`
- `regles_t42_t49_nouveaux.md`
- `regles_t50_t57_evolution.md`

Elles suivent le format de `analyses/grh/regles_t60.md` :
- intro : état, requête, structure ;
- ce que disent les sources ;
- 1. règles, avec un Exemple et une Source ;
- 2. dans OBIEE ;
- 3. réglages (CTE `parametres`) ;
- 4. points susceptibles de changer ;
- 5. chiffres attendus face à l'annuaire CNSS 2024 ;
- 6. points d'attention ;
- 7. questions à la CNSS, avec « En attendant » ;
- 8. comment c'est vérifié ;
- 9. ce qui reste.

**La liste du recueil :** `analyses/prestations/besoins_v7_prestations.md`. Toutes les demandes du recueil v7, chacune avec son statut, et les points ouverts regroupés.

**Les schémas :**
- un dossier par famille dans `excalidraw/schemas/prestations/` : « T25 T27 dossiers », « T28 T31 controles indus », « T32 T41 beneficiaires », « T42 T49 nouveaux », « T50 T57 evolution ». Chaque schéma est fait par `sources/prestations/famille_*.py`, qui appelle `gabarit_famille.construire(DATA)` ;
- `schemas/prestations/besoins_v7_prestations.excalidraw` ;
- `schemas/annuaire cnss/prestations_2024.excalidraw`, fait par `sources/annuaire_cnss/prestations_2024.py`, sur le modèle de `gestion_administrative_2024.py`. Il contient :
  - l'inventaire des tableaux, avec leur tableau CIPRES ;
  - les contrôles internes ;
  - des cartes de verdict ;
  - ce qui n'est pas publié ;
  - les modèles de ratios (feuilles 03 à 06) ;
  - les propositions et les points ouverts.

### Le gabarit `gabarit_famille.py`

```python
DATA = {seed, titre, sous_titre, tableaux, recueil, recueil_note, regles, cas, cas_note,
        annuaire, annuaire_note, annuaire_apres, reglages, reglages_note,
        questions, questions_note, reste, sources}
```

Ce que contient chaque champ :
- `tableaux` : (tableau, ce qu'il compte selon l'Annuaire CIPRES, page, ce qu'on a déjà).
- `recueil` : (réf, ce que dit le recueil, en clair, où c'est traité, statut). Le statut vaut « pris en compte », « à intégrer », « question », « hors base » ou « hors BI ».
- `regles` : {id, titre, regle, exemple, sources : [(tag, détail)], etat}. Le tag vaut « Recueil v7 », « Réunion », « Kaya 2017 », « Annuaire CIPRES », « Annuaire CNSS 2024 », « Modèles CNSS », « Données » ou « Proposition BI ».
- `cas` : (titre, détail, verdict, genre). Le genre vaut « compte », « pas compte », « reglage », « attention » ou « question ».
- `annuaire` : (tableau, ce qu'on compare, nous, publié, écart, verdict, ce que ça change). Le verdict vaut « confirme », « tranche », « precise », « ecart », « infirme » (affiché « contredit ») ou « non publie ».
- `reglages` : (réglage, requête, valeur retenue, autre valeur, ce que ça change, qui décide).
- `questions` : (n°, question, à qui, en attendant, déjà posée où).

Les valeurs du workflow ne portent pas les mêmes noms que les clés du gabarit. Les traduire ainsi :

| Valeur du workflow | Clé du gabarit |
|---|---|
| « ecart a expliquer » | « ecart » |
| « non comparable » | à ajouter au gabarit, ou « non publie » |
| « selon un reglage » | « reglage » |
| « a surveiller » | « attention » |
| « a integrer » | « à intégrer » |
| « question a la CNSS » | « question » |

## 5. Contrôle du recueil v7 : les 30 demandes Prestations

Claude a relu lui-même les lignes 115 à 335 de `besoins_v7.txt` (version du 14/01/2026). Ce relevé sert à contrôler l'analyse.

1. **T27** (l. 122) :
   - dossiers de l'exercice, déposés et liquidés ;
   - « dossier en instance : dossier déposé, non liquidé et non rejeté au cours de la période » ;
   - la ligne « Autres prestations de la branche » devient « Frais médicaux de maternité » ;
   - périodicité : mensuelle, trimestrielle, annuelle.
2. **T30** (l. 131) :
   - TOTAL = total des trimestres ;
   - « Nbre de bénéf. payés » = nombre de dossiers payés ;
   - périodicité : trimestrielle, semestrielle, annuelle.
3. **T31** (l. 139) :
   - PVID = branche pension ;
   - AT/MP = risques professionnels (RP) ;
   - PF = prestations familiales ;
   - **source : fichiers Excel de la DAI** ;
   - périodicité : mensuelle, trimestrielle, annuelle.
4. **T41** (l. 149) : allocataire = titulaire du dossier (l'assuré) ; périodicité mensuelle, trimestrielle, annuelle.
5. **T54** (l. 156) : montant = montant liquidé ; annuelle.
6. **T55** : idem T54.
7. **T57** (l. 169) :
   - assurés décédés ;
   - « Autres » devient « TOTAL » ;
   - désagréger les branches par type de bénéficiaires ;
   - un filtre assuré ou ayant droit ;
   - annuelle.
8. **T29** : idem T30.
9. **T37** : idem T41.
10. **T38** : crédirentiers = rentiers ; « le cumul sans préciser un exercice » ; idem T41.
11. **T45** : nouveaux crédirentiers = rentiers ; la période (début, fin) en paramètre ; idem T41.
12. **T46** : idem T45.
13. **T47** : idem T45.
14. **T48** : degré d'IPP = moyenne des taux d'IPP (la somme des taux divisée par le total de la tranche) ; chaque rentier a un taux dans l'application ; idem T45.
15. **T49** : idem T45.
16. **T52** : annuelle.
17. **T53** : idem T52.
18. **T25** : ajouter une ligne « Pension de retraite anticipée » ; idem T27.
19. **T28** : ajouter une ligne « Pension de retraite anticipée » ; source : fichiers Excel de la DAI ; idem T30.
20. **T32** : pour les pensions de coordination, seulement la partie CNSS ; idem T41.
21. **T33** : un nouveau module, pour les pensions d'invalidité converties en pensions normales à l'âge de la retraite ; idem T41.
22. **T34** : idem T41.
23. **T35** : supprimer la tranche « 25 ans et plus » ; idem T41.
24. **T36** : idem T41.
25. **T42** :
    - service passé = total des périodes d'activité salariée, en mois ;
    - le « total services passés » s'estime en mois ;
    - coordination : seulement la partie CNSS ;
    - périodicité : mensuelle, trimestrielle, annuelle.
26. **T43** : la période (début, fin) en paramètre ; idem T41.
27. **T44** : idem T43.
28. **T50** : service passé en mois ; annuelle.
29. **T51** : allocation de survivants = veufs et veuves, orphelins et ascendants ; idem T52.
30. **T76** : ratio en pourcentage, à 2 décimales ; annuelle.

Le recueil n'a aucune ligne pour **T26, T39, T40 et T56**.

## 6. Les indicateurs du recueil v7 (l. 1018 à 1378, 20/01/2026)

Beaucoup de formules manquent dans le texte. Règle générale (l. 1065) : « de manière globale, donner la possibilité de désagréger l'indicateur ».

- **Satisfaction** : tous les deux ans, par DR et par type. « Données non disponibles en base. »
- **Payés par virement** : annuel, trimestriel, mensuel. Par type, DR, agence et mode de paiement.
- **PF** (l. 1093) :
  - délai de liquidation : de la réception du dossier complet au guichet à sa validation. Trimestriel et annuel. Par AF et IJ de maternité, DR, agence. DES, DR ;
  - taux restant à traiter (CIPRES) ;
  - ratio de contrôle a posteriori : mensuel, trimestriel, annuel. DAI, DR ;
  - ratio d'indus ;
  - ratio des ressources de cotisations allouées : les prestations ordonnancées sont les prestations notifiées.
- **AT/MP** (l. 1143) :
  - taux de variation des déclarations : annuel. DP, DR ;
  - délai de remboursement des soins : du certificat du médecin traitant à la convocation. Trimestriel et annuel ;
  - délai des rentes de survivants : du certificat de consolidation à la notification. Annuel ;
  - restant à traiter, contrôle, indus, ressources : annuel. DR, DCP.
- **Pensions** (l. 1216) :
  - délai de liquidation : de la réception du dossier complet à la notification à l'assuré. Trimestriel et annuel. Par type, DR, agence. DCP, DR ;
  - restant à traiter ;
  - rapport démographique : les retraités sont les pensions directes (vieillesse, anticipée, invalidité) ;
  - taux de remplacement : le dernier revenu est le salaire moyen des 5 meilleures années ;
  - contrôle, indus, ressources.
- **Action sanitaire et sociale (ASS)** (l. 1296) :
  - normes SMI ;
  - CQP (fichier Excel) ;
  - charges administratives de l'ASS rapportées à celles de la CNSS ;
  - couverture ASS (DCG).
- **Nouvelles prestations** (l. 1338) : « à confirmer avec DAI et DPC ».
- **Réclamations** (l. 1357) :
  - fichiers Excel des DR ;
  - différence n+1 − n ;
  - trimestriel et annuel ;
  - taux de prestations indues (CIPRES).

## 7. L'annuaire CNSS 2024 : où sont les tableaux

Les numéros sont ceux des lignes de `sources_texte/annuaire_cnss_2024.txt`.

| Tableaux | Ligne | Contenu |
|---|---|---|
| 18 à 25 | 1459, 1483, 1518, 1576, 1607, 1636, 1661, 1687 | Pensions : bénéficiaires, nouveaux, montants |
| 26 | 1738 | Décès de pensionnés |
| 27 | 1761 | Départs à la retraite |
| 28 à 33 | 1784 à 2054 | Prestations vieillesse mensuelles |
| 34 | 2102 | — |
| 35 | 2177 | IJ |
| 36 | 2192 | Enfants d'allocataires |
| 37 à 46 | 2214 à 2500 | Prestations familiales et maternité |
| 47 à 74 | 2533 à 3426 | AT/MP déclarés |
| 75 à 80 | 3464 à 3573 | Rentes AT/MP |
| 81 | 3600 | Décès des rentiers |
| 82 à 89 | 3629 à 4006 | Prestations en nature (mensuel) |
| 90 à 99 | 4078 à 4311 | Prestations en espèces |
| 100 à 102 | 4340 à 4390 | Entreprises contrôlées, CSST |
| 103 à 105 | 4437 à 4474 | Ratio des charges de fonctionnement |
| 106 | 4496 | Récapitulatif par branche |
| 107 à 113 | 4521 à 4660 | Délai de remise en banque des titres |
| 114 | 4691 | ASS |
| 115 à 118 | 4726 à 4829 | SMI |
| 119 et suivants | 4858 | GRH (déjà traités) |

## 8. Comparaisons déjà faites

Elles sont dans `analyses/comparaison_annuaire_cnss_2024/README.md`, qu'on ne modifie pas.

- **Tableau 18, nous contre la CNSS :** écarts de +2,2 %, +2,3 %, +1,7 %, −2,1 % et −3,3 % selon les totaux. Le tableau 22 donne le détail par direction.
- **Tableau 23 :** 4 090 contre 4 037 (+1,3 %) en comptant à la saisie ; 3 888 (−3,7 %) en comptant à la notification.
- **Montants :** vieillesse 38,50 contre 38,24 Md ; branche 47,24 contre 46,63 Md.
- **R6 corrigée :** le stock au 31/12, titulaire en vie, payé ou non ; orphelins de moins de 25 ans. **R7 revue :** on compte à la saisie.
- **Veuves et orphelins :** en baisse depuis 2023. A06 à A08 restent à exécuter.
- **P25 :** 8 447 pensions d'orphelins sont actives sans paiement.
- **Autres chiffres à tester :**
  - rentes AT/MP : 2 232 ;
  - victimes AT/MP : 1 171 ;
  - enfants d'allocataires : 268 481 ;
  - bénéficiaires d'IJ : 3 893.

## 9. Règles à respecter

- **Hiérarchie des sources :** réunion CNSS, puis recueil v7, puis Kaya 2017, puis Annuaire CIPRES. L'annuaire CNSS 2024 sert à comparer les chiffres ; il tranche la pratique quand les sources plus fortes se taisent.
- **Une source pour chaque règle :** Annuaire p., Kaya 2017, Base ou Données Pnn. Sinon, la marquer « Proposition BI ». Les termes restent simples, avec une seule question par point.
- **Données sensibles.** Ne jamais copier de numéro de compte bancaire ni de nom. Dans « Evaluation des Ratios CIPRES 2025.xlsx » :
  - ne lire que les feuilles 03 à 06 ;
  - jamais la feuille 'REM CHK BK (2)' ;
  - dans la feuille 03, jamais les colonnes NUMERO DE L'ASSURE ni NOMS ET PRENOMS.
- **Base de données.** Ne pas lire STG_GL. Oracle est antérieur à 12c : utiliser ROWNUM, des alias de 30 caractères au plus, et pas d'alias DEC.
- **Vocabulaire :** « paramètres » désigne la CTE `parametres`, jamais une variable `:2`.
- **Schémas :**
  - jamais `--force` sans avoir regardé `excalidraw/historique/<chemin>/` ;
  - ne jamais lancer un générateur deux fois de suite ;
  - ne jamais lancer `fiche_de_lecture.py` avec `--force`.
- **Commit :** seulement à la demande de l'utilisateur.
- **Réponses :** commencer par un « Récap » de 3 à 5 puces. Les documents et les explications vont en Excalidraw.

## 10. Avancement (tenu au fil de l'eau)

- **08/11, 21 h 30 : audit rapide des résultats.**
  - Les 5 contre-vérifications ont recalculé les chiffres de l'annuaire : ils sont justes.
  - Restent 26 à 31 corrections par famille, surtout des imprécisions, avec de 1 à 6 erreurs selon la famille.
  - Le recueil compte 105 demandes ligne à ligne : F1 12, F2 14, F3 16, F4 17, F5 14, transverses 32.
  - Les règles nouvelles sont numérotées N1… dans chaque famille, et les questions Q1… dans chaque famille. Hors du document, on cite « T25-T27 N3 ».
- **Gabarit complété :** un verdict « non comparable » et une section facultative « 7. Les points d'attention » (`attention` : [(titre, texte)]). La partie « reste » devient alors la 8.
- **Famille pilote T25–T27 lancée** : workflow `wf_a8cc0a4f-199`, en trois passes (rédaction, exactitude, clarté). Ses livrables :
  - `analyses/prestations/regles_t25_t27_dossiers.md` ;
  - `excalidraw/sources/prestations/famille_t25_t27_dossiers.py` ;
  - `excalidraw/schemas/prestations/T25 T27 dossiers/regles_t25_t27_dossiers.excalidraw`.

  Script : `~/.claude/projects/-Users-APPLE-Documents-optiwares-ETL/629a91a6-2e84-4ac4-8fc6-93ac41fadaad/workflows/scripts/prestations-famille-pilote-wf_a8cc0a4f-199.js`. Il sert de base pour les autres familles : changer l'objet `F`.
- **Pilote terminé (08/11, vers 22 h).**
  - 27 règles : 19 existantes et N1 à N8 nouvelles. 22 questions, 35 lignes du recueil.
  - Le schéma : 1 784 éléments, 0 avertissement. Dans la visionneuse (port 8767), `measureText` en Excalifont ne trouve aucun débordement.
  - Le document fait 661 lignes. Il est exact mais long : on ajoute un « En bref » en tête.
- **Gabarit :** `en_bref` (encadré en tête, `DATA["en_bref"]`) et `titre=` sur recueil, questions, attention et fin. Le pilote se reconstruit à l'identique.
- **Suite lancée : workflow `wf_648f022f-92b`.** Trois étapes :
  1. F2 à F5 (rédaction puis vérification), avec en parallèle l'« En bref » du pilote ;
  2. la liste du recueil v7 (`besoins_v7_prestations.md`, `.py` et schéma) et l'annuaire (`annuaire_cnss_2024_prestations.md` ; `sources/annuaire_cnss/prestations_2024.py` vers `schemas/annuaire cnss/prestations_2024.excalidraw`) ;
  3. une critique d'exhaustivité.

  Script : `~/.claude/projects/-Users-APPLE-Documents-optiwares-ETL/629a91a6-2e84-4ac4-8fc6-93ac41fadaad/workflows/scripts/prestations-familles-et-syntheses-wf_648f022f-92b.js`. En cas de coupure, dans la même session : `Workflow({scriptPath, resumeFromRunId: "wf_648f022f-92b"})`. Sinon, regarder quels fichiers existent déjà et relancer seulement ce qui manque.
- **Suite terminée (08/11, vers 23 h) : 14 agents, 0 erreur.** Résultat copié dans `travail/resultat_familles_et_syntheses.json`, avec le journal des agents dans `travail/journal_familles_et_syntheses.jsonl`.

  | Document | Lignes du recueil | Règles | Questions |
  |---|---|---|---|
  | `regles_t25_t27_dossiers` (pilote) | 35 | 27 | 22 |
  | `regles_t28_t31_controles_indus` | 39 | 27 | 30 |
  | `regles_t32_t41_beneficiaires` | 31 | 22 | 23 |
  | `regles_t42_t49_nouveaux` | 31 | 17 | 21 |
  | `regles_t50_t57_evolution` | 48 | 22 | 25 |
  | `besoins_v7_prestations` | 130 sans doublon : 34 prises en compte, 57 à intégrer, 28 questions, 10 hors base, 1 hors BI | — | 108 sans doublon (121 dans les familles) |
  | `annuaire_cnss_2024_prestations` | 101 tableaux de l'annuaire, 120 comparaisons | — | renvoie aux familles |

- **Critique d'exhaustivité.** Les 30 demandes du tableau « Prestation » et tous les indicateurs sont placés ; seules 3 demandes hors prestations sont écartées. La critique a corrigé, dans les familles elles-mêmes :
  - 2 statuts contraires et 6 demandes sans statut ;
  - T39 et T40 sans ligne ;
  - 1 règle qui dépendait d'une autre famille ;
  - la question de la date d'un paiement, posée de trois façons ;
  - 3 verdicts différents pour la même comparaison (tableaux 22, 26, 35), ramenés à « précise ».

  Les autres différences entre familles sont expliquées dans `annuaire_cnss_2024_prestations.md`, partie 3.6.
- **Affichage contrôlé** dans la visionneuse (port 8767, `measureText` en Excalifont), sur les sept schémas (7 849 textes mesurés dans leur cadre) et les trois pages du journal : aucun débordement, aucun chevauchement.
- **Fait aussi :**
  - trois lignes au tableau « Schémas » de `excalidraw/README.md` ;
  - le journal (chantier « fait », reste, état, attentes par direction, suite, livrables), régénéré ;
  - la mémoire.

  Le README, le TODO et les règles des prestations ne sont pas touchés.

- **09/11 : la fiche `points_a_valider` mise à jour.** L'utilisateur a demandé : « mets à jour la fiche point à valider ».
  - La fiche a 137 points. Les 68 du 02/11 gardent leur numéro ; les points 69 à 137 sont nouveaux.
  - Les 108 questions de `besoins_v7_prestations` (partie 8) sont toutes couvertes.
  - Les 8 points prioritaires sont 1, 2, 29, 69, 70, 71, 14 et 21.
  - Chaque point a une ligne « À qui ». La fiche finit par la liste des points de chaque direction.

  Comment elle a été faite :
  - Workflow `wf_68d98419-3fb` : sept rédacteurs, chacun relu, puis une critique d'ensemble.
  - Workflow `wf_feb07d5e-c08` : le resserrage à la densité du 01/11 (environ 620 caractères par point, contre 1 029), contrôlé par `controle_concision.py`, puis une lecture finale.
  - Tout est dans `travail/pav/` :
    - `CONSIGNES.md`, `CONCISION.md` ;
    - `repartition.json` ;
    - les JSON par responsable ;
    - `assembler.py`, qui écrit la fiche ; sans `--ecrire`, il écrit un aperçu ;
    - `numeros.json` (la clé et son numéro) ;
    - `points_a_valider_01-11.md`, la copie de la version du 01/11.
  - Schéma : `points_a_valider.build()` sur les prestations seulement, sans aucun débordement.
  - Word : `excalidraw/sources/points_a_valider_word.js`. Le fichier passe le validateur, et ses numéros sont identiques à ceux de la fiche.
  - Pour une nouvelle mise à jour : modifier les JSON, relancer `assembler.py --ecrire`, puis régénérer le schéma et la version Word.

## 11. Les sources (textes extraits)

Dans `travail/sources_texte/` :
- `besoins_v7.txt` (2 313 lignes) ;
- `annuaire_cipres.txt` (3 175) ;
- `kaya.txt` (1 677) ;
- `annuaire_cnss_2024.txt` (5 126).

Ce sont des copies faites le 08/11 depuis les documents de la CNSS du projet. Les originaux étaient dans le dossier temporaire de la session.
