# T25 à T27 : les dossiers de prestations (déposés, liquidés, en instance) — les règles en clair

## En bref

- **Ce que comptent ces tableaux.** Les dossiers de l'année : déposés, liquidés (les droits sont notifiés à l'assuré) en moins de 15 jours, de 15 à 45 jours ou en plus de 45 jours, en instance au 31/12 (ni liquidés ni rejetés), rejetés. T25 pour les pensions, T26 pour les accidents du travail et maladies professionnelles (AT/MP), T27 pour les prestations familiales (PF) et la maternité. On compte des demandes : un même dossier peut en porter plusieurs, par exemple la pension d'un retraité puis celle de sa veuve (D2).
- **Ce que le recueil v7 demande en plus.** Une ligne « Pension de retraite anticipée » dans T25 (R2) ; la dernière ligne de T27 renommée « Frais médicaux de maternité » (N3) ; les tableaux au mois, au trimestre et à l'année (N1) ; le délai de liquidation et le taux de dossiers restant à traiter, par direction régionale (DR) et par agence, avec leur norme (N2, N4, N5, N7) ; le taux de variation des déclarations d'AT/MP (N8). Il diffère des autres sources sur deux points : pour les PF, le délai finit à une « date de validation » et non à la notification (Q16) ; le taux restant à traiter n'a pas la même formule que la grille de la CNSS (Q19). T26 n'a aucune ligne dans le recueil (Q12).
- **Ce que l'annuaire CNSS 2024 confirme.** Une liquidation compte au jour où les droits sont notifiés : 831 allocations de vieillesse liquidées en 2024 dans la base, 833 publiées (tableau 24, −0,24 %). Il confirme aussi que la maternité va avec les PF (R1), que la majoration pour enfant suit la pension (A1) et que le taux de variation se calcule comme le dit le recueil (N8) ; il publie la retraite anticipée à part, comme le demande le recueil (R2). Les indemnités journalières (IJ) de maternité et les accidents déclarés tombent à 3,5 % près au plus chaque année (tableaux 35 et 47), sans trancher à quelle date compter les IJ, ni ce qu'est une déclaration pour T26.
- **Ce qu'il contredit, ou qui reste en écart.** Les frais médicaux de maternité : l'annuaire en publie 25,0 millions FCFA payés en 2024, la base n'en a aucun (tableaux 40 et 41 ; la fiche T27 les disait « jamais payés »). Les soins des accidents du travail : la base n'a que 13,5 % du montant publié (18,8 millions contre 139,7 millions FCFA). Les nouvelles rentes viagères : 39 dans la base, 44 publiées (−11,4 %). Enfin, l'annuaire ne publie ni dépôts, ni dossiers en instance, ni rejets, ni durées : ces colonnes ne se valident qu'avec la CNSS.
- **Les décisions attendues de la CNSS.** 22 questions ; aucune ne bloque, chacune a une valeur par défaut dans les réglages. Les plus lourdes : **Q2**, au bout de combien de mois une demande sans suite sort des dossiers en instance (avec les réglages actuels, 3 300 à 3 500 demandes de pension resteraient en instance fin 2025, estimation : un taux restant à traiter d'environ 70 % pour une norme de 15 %) ; **Q1**, compter aussi les dossiers déposés les années d'avant, ou seulement ceux de l'année ; **Q19**, quelle formule pour le taux restant à traiter ; **Q13**, un enfant ajouté au dossier d'un allocataire compte-t-il comme un dossier de plus (8 246 ou environ 16 400 allocations familiales liquidées en 2025) ; **Q15**, où sont enregistrés les frais médicaux de maternité.
- **Ce qui reste à faire.** Tout est proposé, rien n'est validé ni exécuté : les chiffres de 2025 viennent des contrôles déjà lancés, sur la copie de la base du 02/02/2026. Reste à exécuter t25 à t27 (toi), à y ajouter le mois, la DR, l'agence et les réglages à créer (Claude), à comparer 2024 à l'annuaire mois par mois et par DR, et à poser Q1 à Q22 à la CNSS (toi).

**Ce que couvre la famille.** Trois tableaux de l'Annuaire CIPRES, sur la même grille : combien de dossiers sont déposés dans l'année, combien sont liquidés (en moins de 15 jours, de 15 à 45 jours, en plus de 45 jours), combien restent en instance au 31/12, combien sont rejetés. T25 pour les pensions (vieillesse, invalidité, décès), T26 pour les accidents du travail et maladies professionnelles (AT/MP), T27 pour les prestations familiales, maternité comprise. S'y ajoutent les indicateurs du recueil v7 qui en sortent : délai de liquidation, taux de dossiers restant à traiter, variation des déclarations d'AT/MP.

**État au 08/11/2026.** Tout est **proposé**, rien n'est encore validé par la CNSS. Aucune réunion avec la CNSS n'a encore porté sur ces tableaux. Les requêtes n'ont pas été exécutées : les chiffres de 2025 cités viennent des lots et sont **vus au 02/02/2026** (date de la copie de la base).

**D'où viennent les règles.** Dans l'ordre de force : une décision de réunion (aucune ici), le recueil des besoins v7, l'atelier de Kaya (2017), l'Annuaire CIPRES. L'annuaire statistique de la CNSS 2024 sert à comparer les chiffres, et tranche la pratique quand les sources plus fortes se taisent. Les modèles de ratios de la CNSS (classeur « Evaluation des Ratios CIPRES 2025 », feuilles 03 et 04) montrent comment la CNSS calcule elle-même.

**Ce qui existe déjà, laissé tel quel.** Requêtes [t25_requete_proposee.sql](t25_requete_proposee.sql), [t26_requete_proposee.sql](t26_requete_proposee.sql), [t27_requete_proposee.sql](t27_requete_proposee.sql) et [dtm_dossier_requete_proposee.sql](dtm_dossier_requete_proposee.sql) ; règles R1, R2, R10 à R13 du [README](README.md) ; cartes D1 à D14, A1 et H2 du schéma `regles_prestations` ; fiches de validation T25, T26 et T27 (`excalidraw/schemas/prestations/tableaux cipres/`) ; [points_a_valider](points_a_valider.md). Ce document les reprend avec leurs identifiants, et **propose** les règles nouvelles N1 à N8 sans rien modifier.

**Le schéma associé :** `excalidraw/schemas/prestations/T25 T27 dossiers/regles_t25_t27_dossiers.excalidraw`, fait par `excalidraw/sources/prestations/famille_t25_t27_dossiers.py`. Il dit la même chose en plus court, avec les mêmes identifiants.

**Les mots de ce document.** *Déposé* : reçu au guichet. *Liquidé* : les droits sont calculés et notifiés à l'assuré (la *notification des droits*). *En instance* : déposé, ni liquidé ni rejeté. *Rejeté* : refusé. Le *stock* : tout ce qui est en instance à une date. Une *demande* : une prestation demandée dans un dossier (un dossier peut en porter plusieurs, règle D2). *PF* : prestations familiales. *IJ* : indemnités journalières. *DR* : direction régionale. *Réversion* : la pension du défunt reprise par sa veuve ou ses orphelins. *Consolidation* : la blessure de la victime d'accident est stabilisée, ce qui ouvre le droit à une rente. Le *cliché* : la copie de la base sur laquelle on calcule. Les *réglages* : les choix écrits dans le bloc `parametres` en tête de chaque requête (une « CTE » en langage SQL) ; on les change sans réécrire la requête. *Pnn*, *lot n* : le résultat numéroté d'une requête de contrôle déjà lancée sur la base. Les sigles des directions sont expliqués en partie 6.

---

## Ce que disent les sources

| Source | Ce qu'elle dit pour ces tableaux |
|---|---|
| Recueil des besoins v7 (14/01/2026 et 20/01/2026) | T27 (l. 125 à 128) : les dossiers de l'exercice, déposés et liquidés ; « dossier en instance : dossier déposé, non liquidé et non rejeté au cours de la période » ; la dernière ligne devient « Frais médicaux de maternité » ; par mois, trimestre et année. T25 (l. 248 et 249) : une ligne « Pension de retraite anticipée », et « Idem. Tab 27 ». T26 : aucune ligne dans la liste des tableaux. Les indicateurs (l. 1093 à 1245) donnent le délai de liquidation et le taux de dossiers restant à traiter pour les trois branches, avec leurs formules, par DR et par agence |
| Réunions avec la CNSS | Aucune sur T25 à T27 à ce jour. Les questions des fiches T25 à T27 et de points_a_valider attendent leur réponse |
| Atelier de Kaya (2017) | Tableau 1 : T25, « ajouter le type de prestation pension de retraite anticipée » (l. 271) ; T27, l'allocation prénatale « ne fait pas l'objet de réception ni de notification », « ne plus recevoir au guichet des dossiers incomplets », le module des rejets existe mais « n'est pas exploité » (l. 80 à 87). Tableau 2 : les mêmes indicateurs que le recueil, avec la même formule du taux restant à traiter ; le délai des PF y finit à la notification des droits (l. 862). Rien sur T26 dans le tableau 1 |
| Annuaire CIPRES | Description p. 11 et 12, maquettes p. 34 : déposés, liquidés par durée (moins de 15 jours, 15 à 45, plus de 45), en instance, rejetés, par type de prestation. But : la durée moyenne, comparée à la norme CIPRES « moins de 45 jours », et le nombre de dossiers restant à traiter en fin d'exercice. Glossaire p. 64 : la durée va du dépôt à la notification des droits |
| Annuaire CNSS 2024 | Ne publie ni dépôts, ni dossiers en instance, ni rejets, ni durées. Trois contrôles directs : allocations de vieillesse liquidées (tableau 24), IJ de maternité (tableaux 35 et 39), accidents déclarés (tableaux 47, 48, 72). Des contrôles indirects, en personnes : retraite anticipée (tableau 23), rentes viagères (75), rentes de survivants (77). Un modèle de découpage par agence (tableaux 73 et 74, et la liste des 16 agences p. xvii à xix) |
| Modèles de ratios de la CNSS (2025) | Feuille 03 : délai = date de notification des droits − date de réception du dossier, en jours ; parts liquidées en 45 jours au plus et au-delà ; moyenne et écart type ; sur la « 1ère demande » de chaque branche. Feuille 04 : taux de restes à traiter = instances de fin ÷ (instances du début + reçus − rejetés), norme 15 % au plus. Les deux feuilles sont des modèles vides |
| Données de la base | Lot 3 B (P48 à P58, pensions), lot 4 (P59 à P73, AT/MP et PF), lots 7 et 8 (P82, P91 à P100), lot 9 (P104) ; comparaisons A01 et A02 (dossier `comparaison_annuaire_cnss_2024`) |

---

## 0. Les tableaux de la famille

| Tableau | Ce qu'il compte (Annuaire CIPRES) | Page | Ce qu'on a déjà |
|---|---|---|---|
| T25 | Les dossiers de pension (vieillesse, invalidité, décès) de l'exercice : déposés, liquidés par durée, en instance, rejetés. Lignes : pension de vieillesse, pension d'invalidité, pension de survivant, allocations de vieillesse, allocation de survivant, autres prestations de la branche, Total | p. 11 ; maquette p. 34 ; glossaire p. 62 et 64 | `t25_requete_proposee.sql` (revue le 30/10, pas exécutée) ; fiche T25 (10 règles, 5 questions) ; lot 3 B |
| T26 | La même grille pour les AT/MP. Lignes : rentes viagères, rentes de survivants, indemnités journalières, allocation AT/MP, autres prestations de la branche, Total | p. 12 ; maquette p. 34 ; glossaire p. 64 et 65 | `t26_requete_proposee.sql` (30/10, pas exécutée) ; fiche T26 ; lot 4 |
| T27 | La même grille pour les prestations familiales et la maternité. Lignes : allocations familiales, indemnités journalières de maternité, allocations prénatales, autres prestations de la branche (à renommer « Frais médicaux de maternité »), Total | p. 12 ; maquette p. 34 ; glossaire p. 62 et 64 | `t27_requete_proposee.sql` (30/10, pas exécutée) ; fiche T27 ; lot 4 |

La `dtm_dossier_requete_proposee.sql` (29/10) est la requête proposée pour la table `DTM_DOSSIER`, qui alimente l'outil de restitution (OBIEE). Elle couvre les trois tableaux au mois et par DR, mais avec les règles d'avant le 30/10 (voir point d'attention 3). Plus bas, on l'appelle « la `dtm_dossier` ».

---

## 1. Ce que demande le recueil des besoins v7

Toutes les lignes du recueil qui touchent T25 à T27. Les numéros de ligne sont ceux de `besoins_v7.txt`. Les formules des indicateurs sont des équations Word, absentes du texte : elles ont été relues dans le document Word (extraction `style_grh/travail/scratchpad/recueil_out/document_dump.txt`).

| Réf. | Ce que dit le recueil | En clair | Où c'est pris en compte | Statut |
|---|---|---|---|---|
| Prestation #1 (T27), l. 125 | « Ce tableau concerne les dossiers de l'exercice (déposés et liquidés) » | Le tableau compte les dépôts et les liquidations de l'année. Deux lectures : chaque étape dans son année (un dépôt de 2024 liquidé en 2025 compte en liquidé en 2025), ou seulement les dossiers déposés dans l'année | H2, D3, D4 : chaque étape dans son année (lecture BI). Réglage `lecture_periode` | question (Q1) |
| Prestation #1 (T27), l. 126 | « Dossier en instance : dossier déposé, non liquidé et non rejeté au cours de la période » | Un dossier déposé, ni liquidé ni rejeté. « Au cours de la période » se lit comme un stock (tout ce qui attend au 31/12) ou comme une cohorte (les dépôts de l'année encore en attente) | D6 : stock au 31/12, limité par `debut_stock` et `sans_suite_mois` (proposition BI, le recueil ne parle pas de limite) | question (Q1, Q2) |
| Prestation #1 (T27), l. 127 | « Autres prestations de la branche : Renommer par “Frais médicaux de maternité” » | La dernière ligne de T27 compte les dossiers de maternité avec des frais pris en charge (accouchement, pharmacie…) | N3. La base n'a aucun paiement de ces frais de 2020 à 2025 : où sont-ils enregistrés (Q15) | à intégrer |
| Prestation #1 (T27), l. 128 | « Périodicité : Mensuelle, Trimestrielle, Annuelle » | T27 sort pour un mois, un trimestre ou une année | N1 (t27 est annuel aujourd'hui) | à intégrer |
| Prestation #18 (T25), l. 248 | « Ajouter une ligne “Pension de retraite anticipée” » | Une ligne à part pour la retraite anticipée (code RPRA) | R2 ; t25, ligne 2 ; fiche T25, ligne 2 (qui ne cite que Kaya 2017) | pris en compte |
| Prestation #18 (T25), l. 249 (« Idem. Tab 27 », renvoie à l. 125) | Les dossiers de l'exercice, déposés et liquidés | Même lecture pour les pensions | H2, D3, D4 ; réglage `lecture_periode` | question (Q1) |
| Prestation #18 (T25), l. 249 (renvoie à l. 126) | La définition du dossier en instance | Même stock pour les pensions | D6 ; pour les pensions, pas de limite de 24 mois aujourd'hui (réglage `sans_suite_mois` à créer dans t25) | question (Q1, Q2) |
| Prestation #18 (T25), l. 249 (renvoie à l. 128) | Périodicité mensuelle, trimestrielle, annuelle | T25 sort aussi au mois et au trimestre | N1 | à intégrer |
| Prestation #18 (T25), l. 249 (renvoie à l. 127) | Le renommage en « Frais médicaux de maternité » | Ne peut viser que T27 : la maternité n'est pas dans la branche des pensions | Lecture BI : T25 garde « Autres prestations de la branche » (un dossier qui n'a qu'un accessoire, A1). À signaler à la CNSS | pris en compte |
| Liste du domaine Prestation, l. 115 à 335 | Aucune ligne pour le tableau 26 | La CNSS n'a rien dit sur la maquette de T26. Mais ses durées et son taux restant à traiter sont définis par les indicateurs AT/MP (l. 1146 à 1188) | t26 et la fiche T26 suivent l'Annuaire CIPRES et les indicateurs ; remarque B38 du recueil (`remarques_besoins_v7`) | question (Q12) |
| Indicateurs PF, l. 1093 à 1103 | « Le délai de liquidation d'un dossier de prestations familiales » : moyenne des durées ; trimestriel et annuel ; par DR et agence ; DES, DR | Le nombre moyen de jours pour liquider un dossier de PF, par trimestre et par an, par direction régionale et par agence | D5 (moyenne et médiane par ligne, à l'année) ; N1, N2, N5 | à intégrer |
| Indicateurs PF, l. 1097 | La durée va de la réception au guichet d'un dossier complet de PF à « la date de validation » | Le recueil a changé la phrase de Kaya 2017 (qui disait « notification ») : pour les PF, la durée finit à la validation | D4, réglage `fin_duree_pf` : la notification en repli provisoire, faute de date de validation connue | question (Q16) |
| Indicateurs PF, l. 1102 | « Par type de prestations (Allocations familiales et Indemnités Journalière de Maternité) » | L'indicateur de délai des PF ne prend que deux types : l'allocation prénatale n'y est pas | D14, N5 : une grossesse compte une fois dans l'indicateur, par l'IJ | question (Q14) |
| Indicateurs PF, l. 1104 à 1110 | « Le taux des dossiers restant à traiter en fin de période (CIPRES) » ; formule : dossiers en instance à la fin de la période ÷ dossiers complets reçus au cours de l'exercice × 100 ; trimestriel et annuel ; par type, DR et agence | La part des dossiers de PF encore en attente à la fin de la période | N4 (formule du recueil par défaut, réglage `formule_taux` ; quelle formule : Q19) ; N1, N2 | à intégrer |
| Indicateurs PF, l. 1129 à 1141 (note l. 1133) ; mêmes ratios AT/MP l. 1210 à 1214 et pensions l. 1287 à 1294 | « Le montant total des prestations ordonnancées au titre de la période est l'ensemble des prestations ayant fait l'objet d'une notification » | Pour la CNSS, une prestation est ordonnancée quand elle est notifiée | D4 s'appuie sur cette note. Le ratio des ressources lui-même (des montants, pour les trois branches) est traité hors de cette famille | pris en compte |
| Indicateurs AT/MP, l. 1146 à 1156 | « Taux de variation du nombre de déclarations d'AT/MP » ; formule : (déclarations de l'année n − déclarations de l'année n−1) ÷ déclarations de l'année n−1 × 100 ; annuel ; par DR et agence ; DP, DR | De combien les déclarations d'accident ou de maladie professionnelle montent ou baissent d'une année sur l'autre | N8 | à intégrer |
| Indicateurs AT/MP, l. 1157 à 1169 | « Le délai de liquidation des remboursements de soins » : de la réception du certificat médical du médecin traitant à la date de convocation ; moyenne ; trimestriel et annuel ; par DR et agence | Du jour où la CNSS reçoit le certificat du médecin traitant au jour de la convocation (convocation de qui : à faire préciser) | Ces deux dates n'ont pas été trouvées (P63, constat BI). Trois champs proches restent à tester avec la DSI. t26 mesure autre chose : de la déclaration au premier remboursement saisi (D11) | question (Q17) |
| Indicateurs AT/MP, l. 1170 à 1179 | « Le délai de liquidation des rentes de survivants » : de la réception du certificat médical de consolidation à la notification ; annuel ; par DR et agence | Le certificat de consolidation concerne la victime, pas ses survivants : la phrase vise sans doute la rente viagère | Réglage `depart_rente` (demande au guichet en attendant) ; rente de survivant depuis la demande du survivant | question (Q8, Q9) |
| Indicateurs AT/MP, l. 1180 à 1188 | Taux des dossiers restant à traiter, même formule ; annuel ; par type, DR et agence ; DR, DCP | La part des dossiers AT/MP en attente au 31/12 | N4 (quelle formule : Q19) ; dépend beaucoup des déclarations sans suite (D12, `sans_suite_mois`) | à intégrer |
| Indicateurs pensions, l. 1216 à 1238 | « Délai de liquidation des dossiers de pension » : de la réception au guichet d'un dossier complet à la notification des droits ; moyenne ; trimestriel et annuel ; par type, DR et agence ; DCP, DR | Le nombre moyen de jours entre le dépôt d'un dossier de pension complet et la notification | D3, D4, D5 (même définition) ; manquent le trimestre, la DR et l'agence (N1, N2) et l'écart type (N5) | à intégrer |
| Indicateurs pensions, l. 1239 à 1245 | Taux des dossiers restant à traiter, même formule ; annuel ; par type, DR et agence | La part des dossiers de pension en attente au 31/12 | N4 (quelle formule : Q19) ; le stock des pensions reste gonflé (D6, Q2) | à intégrer |
| Indicateurs pensions, l. 1251 | « Le nombre de retraités est le nombre de bénéficiaires de pensions directes (Pension vieillesse, Pension anticipé, pension d'invalidité) » | Pour la CNSS, un retraité touche une pension directe : l'allocation de vieillesse n'en est pas | Repris pour comparer les « départs à la retraite » du tableau 27 de l'annuaire aux dépôts : droits propres sans l'allocation de vieillesse (Q21) | pris en compte |
| Prestation #3 (T31), l. 142 à 144 | « PVID : … (Branche pension) ; AT/MP : … (RP) ; PF : Prestation familiale » | Les trois branches de la CNSS. La maternité n'y est pas citée | R1 (la maternité va avec les PF, comme l'annuaire CNSS 2024, tableau 40) | pris en compte |
| Indicateur « ratios CIPRES respectés », l. 1978 à 1989 | Nombre de ratios respectés ÷ ratios CIPRES à respecter × 100 ; « Source : DAI. Données non disponibles en base » | La part des ratios CIPRES qui tiennent leur norme | Pour la CNSS, hors base. T25 à T27 lui fourniront deux ratios avec leur norme (N7) | hors base |
| Termes de référence, l. 4 à 10 | « cinq (5) Directions régionales : Ouagadougou, Bobo Dioulasso, Ouahigouya, Fada N'gourma, Dédougou » | « Par DR » veut dire ces cinq directions | N2 | à intégrer |
| Indicateurs, l. 1065 | « De manière globale, donner la possibilité de désagréger l'indicateur » | Chaque indicateur doit pouvoir se découper | N2 | à intégrer |
| Attentes, l. 29 | « Toutes les données des rapports doivent pouvoir être décomposées, traquées jusqu'au niveau de granularité le plus élémentaires » | Chaque chiffre doit se déplier jusqu'à la liste des dossiers. Ce passage est repris d'un cahier des charges bancaire (remarque B41 du recueil) : on s'appuie surtout sur les l. 51 et 68 | N6 | à intégrer |
| Caractéristiques techniques, l. 51 et 68 | « Réaliser des analyses portant sur l'intégralité des données y compris au niveau transactionnel » ; analyses « sur des axes non préalablement définis » | La BI garde le détail, une ligne par demande, pour tout recouper | N6, N2 | à intégrer |
| Attentes et caractéristiques, l. 31 et 74 | Présentation multidimensionnelle ; une dimension ajoutée après coup vaut pour tous les indicateurs | Les mêmes axes (branche, ligne, DR, agence, mois) pour T25 à T27 et leurs indicateurs | N2, N6 | à intégrer |
| Attentes, l. 32 | « Emission d'alertes sur les indicateurs de performances » | Prévenir quand un délai ou un taux sort de sa norme | N7 : mention « dans les normes » ou « hors normes », normes dans la CTE `parametres` | à intégrer |
| Attentes, l. 35 | « Production des rapports et des états de restitution à des fréquences adaptées » | Chaque tableau à sa fréquence | N1 | à intégrer |
| Attentes et caractéristiques, l. 28, 34, 75 | Peu d'intervention manuelle ; restitutions automatisées ; rafraîchissement automatique | Les chiffres sortent de requêtes, sans saisie à la main | Requêtes sur la base ; la BI peut remplacer la liste remplie à la main de la feuille 03 (N5, N6) | pris en compte |
| Caractéristiques techniques, l. 58 | Règles de transcodage (passage d'un code à un libellé) écrites et rejouables | La correspondance entre codes de prestation et lignes est écrite | R2 (table de correspondance du README) | pris en compte |
| Attentes et caractéristiques, l. 24 et 78 | Profilage de la qualité des données | Mesurer les trous de la base | Lots 3 B et 4 ; points d'attention de la partie 7 | pris en compte |
| Général, l. 15 et 92 à 94 | La source : l'application de gestion des prestations | Les tableaux se calculent sur la copie de cette application | Toutes les règles ; copie de la base (schéma `USER_DWH`) au 02/02/2026 | pris en compte |

**Quels tableaux n'ont aucune ligne.** T26 n'a aucune ligne dans la liste des tableaux du recueil (ni dans le tableau 1 de Kaya 2017) ; ses durées et son stock sont pourtant définis par les indicateurs AT/MP. T25 et T27 ont chacun leurs lignes.

**Les lignes générales qui ne changent rien ici :** l. 72 (accès par profil), l. 53 à 55 et 60 (fichiers Excel : T28 et T31), indicateur de paiement par virement (l. 1085 à 1091), indicateur de satisfaction (l. 1075 à 1084 : rangé dans la famille T50-T57, hors base), l. 80 et 81 (document de référence, ni nommé ni joint : rangé dans la famille T50-T57, question T50-T57 Q25), colonnes « Priorité » vides (l. 120 et 121).

---

## 2. Les règles, en clair

### R1 — Les branches

**Règle.** Un dossier de pension (branche V) va dans T25, un dossier d'accident du travail ou de maladie professionnelle (A) dans T26, un dossier de prestations familiales (F) ou de maternité (M) dans T27.

**Exemple.** Le dossier d'une grossesse (branche M) compte dans T27, et non dans un tableau « maladie ».

**Sources.** Recueil v7, l. 142 à 144 (les trois branches ; la maternité n'y est pas citée) ; Annuaire CNSS 2024, tableau 40 (IJ de maternité, allocations prénatales et frais médicaux dans les PF) ; README, R1 ; Données P02 et P05.

**État.** Confirmée par l'annuaire CNSS 2024.

### D1 — Les dossiers concernés

**Règle.** On prend tous les dossiers de la branche. On garde les dossiers de coordination payés par une autre caisse (catégorie `COOR_AC` de `DOSSIER.DOS_CATEGORIE`), que la CNSS reçoit et étudie. On retire seulement les dossiers transférés à une autre caisse (`CNSS_AC`).

**Exemple.** Un assuré a travaillé au Burkina puis au Mali. Il dépose sa demande à la CNSS, qui l'étudie ; la caisse du Mali paie. Le dossier compte dans T25.

**Sources.** Données P48 (3 326 dossiers `COOR_AC`, 76 % reçus au guichet de la CNSS ; 238 dépôts et 292 liquidations en 2025, 6 à 8 % de T25) ; Annuaire CNSS 2024 (les pensions `COOR_AC` sont dans l'effectif publié, lot 5, P78) ; fiche T25, règle 1.

**État.** Proposée (BI). Question Q4. La `dtm_dossier_requete_proposee.sql` fait l'inverse (elle écarte `COOR_AC`) : réglage `coordination`, pour que les deux requêtes disent la même chose.

### D2 — Ce qu'on compte : des demandes

**Règle.** Une demande = un dossier × une ligne du tableau. Un même dossier peut porter plusieurs demandes, chacune avec son dépôt et sa liquidation : la pension puis la réversion, les IJ puis la rente, l'IJ et l'allocation prénatale d'une grossesse.

**Exemple.** Le dossier d'un retraité décédé porte sa pension (ligne « Pension de vieillesse », liquidée en 2012) et la réversion de sa veuve (ligne « Pension de survivant », déposée et liquidée en 2025).

**Sources.** Annuaire CIPRES p. 11 et 12 (dossiers répartis selon le type de prestation) ; Données P51 (la moitié des pensions de veuve sont dans le dossier du défunt), P54 (902 dossiers renotifiés en 2025), P59 (191 dossiers « IJ + allocation » sur 231 ont plusieurs dates de notification).

**État.** Prouvée par les données. La question Q4 de la fiche T25 (pension de la veuve dans un nouveau dossier ou dans celui du défunt) ne change plus les chiffres : on compte par demande dans les deux cas. Elle n'est donc plus reposée ici.

### R2 — Les lignes de T25 à T27

**Règle.** La ligne vient du code de prestation de l'application (table de correspondance du README ; les codes ci-dessous sont ceux de la base). T25 : vieillesse (RPV), retraite anticipée à part (RPRA), invalidité (RPI), survivants (RPSV, RPSF, RPSE, RPA), allocation de vieillesse (RAV), allocation de survivant (RASC, RASO, RASA, RAS). T26 : rente viagère (ARI), rentes de survivants (ARSV, ARSF, ARSE, ARSA), IJ (AIJ), allocation d'incapacité (AAI), soins. T27 : allocations familiales (FAF), IJ de maternité (MIJ), allocations prénatales (MAP, FAP), frais médicaux de maternité (N3).

**Exemple.** Un assuré qui part en retraite anticipée (code RPRA) : sa demande va sur la ligne « Pension de retraite anticipée », pas sur « Pension de vieillesse ». La ligne sera presque vide : l'annuaire CNSS ne compte que 3 nouvelles retraites anticipées en 2024 (tableau 23).

**Sources.** Recueil v7, l. 248 (ligne anticipée) ; Kaya 2017, tableau 1, l. 271 ; Annuaire CNSS 2024, tableaux 18, 22 et 23 (pension anticipée publiée à part) ; Données P34, P59, P66.

**État.** Ligne anticipée confirmée par le recueil v7. La liste des codes reste à faire viser (points_a_valider n° 18).

### A1 — Les accessoires de pension

**Règle.** La bonification ou majoration pour enfant et la contribution aux charges du ménage s'ajoutent à une pension : elles ne font pas une demande de plus. Seul un dossier qui n'a qu'un accessoire va en « Autres prestations de la branche ».

**Exemple.** Pension de vieillesse + majoration pour enfant : une seule demande, ligne « Pension de vieillesse ».

**Sources.** Annuaire CNSS 2024, tableaux 28 et 34 (la majoration pour enfant est rangée avec la pension de retraite vieillesse) ; Données P34, P98.

**État.** Confirmée par l'annuaire CNSS 2024.

### H2 — Chaque étape dans sa période

**Règle.** Un dépôt compte à la date de la réception, une liquidation à la date de la notification, un rejet à la date du rejet. Le stock en instance se lit à la fin de la période.

**Exemple.** Déposé le 20/11/2024, notifié le 05/02/2025 : déposé en 2024, en instance au 31/12/2024, liquidé en 2025.

**Sources.** Annuaire CIPRES p. 11 (déposés et liquidés « au cours de l'exercice ») ; Recueil v7, l. 125 ; Modèles CNSS feuille 04 (instances au 31/12/N−1 + reçus en N − rejetés en N − liquidés en N : ce calcul suppose qu'un dossier liquidé en N a pu être reçu avant N).

**État.** Proposée, appuyée par la feuille 04. La lecture « cohorte » du recueil reste possible : question Q1, réglage `lecture_periode`. Dans le schéma `regles_prestations`, H2 est l'hypothèse « un dossier compte l'année de son ouverture, pas de sa liquidation », tranchée par la BI dans ce sens : chaque événement compte à sa date. Ce document garde ce sens et en fait une règle, à faire valider (points_a_valider n° 14).

### D3 — Déposé (remplace R10)

**Règle.** Une demande reçue au guichet dans la période est déposée. La réception « NOUVEAU » (nouvelle demande) va à la première demande du dossier qu'elle ouvre. La réception « ANCIEN » (demande sur un dossier qui existe déjà) va à la réversion en T25 ; à la rente, à l'allocation d'incapacité ou à la rente de survivant en T26. Une demande sans réception prend sa date de demande.

**Exemple.** La veuve dépose sa demande le 03/03/2025 sur le dossier de son mari : dépôt de mars 2025, ligne « Pension de survivant ».

**Sources.** Annuaire CIPRES p. 11, 12 et 64 ; Kaya 2017, tableau 1, l. 86 (ne plus recevoir de dossiers incomplets) ; Recueil v7, l. 1097 et 1224 (dossier complet reçu au guichet) ; Modèles CNSS feuille 04 (« dossiers reçus ») ; Données P09 (4 671 réceptions de pension en 2025), P49 (96 % des pièces saisies le jour de la réception), P52, P60, P71.

**État.** Proposée (BI). À noter : t25 et t26 ne testent pas le type de réception (`RD_TYPE_RECEPTION`) ; toute réception qui porte un numéro de dossier sans l'avoir ouvert est traitée comme « ANCIEN ». Quelques dizaines de cas au plus en pensions (P52) ; pas mesuré en AT/MP : à compter à l'exécution (partie 9). La question « la date de réception est-elle celle du dossier complet ? » (points_a_valider n° 20) reste posée ; les données répondent déjà en partie : 96 % des pièces sont saisies le jour de la réception (P49).

### D4 — Liquidé (remplace R11)

**Règle.** Une demande est liquidée à la première notification de ses droits, au mois et à l'année de cette notification. Le statut du dossier ne sert pas (A veut dire actif, F fermé). Exception : pour les PF, le recueil v7 fait finir la durée à une « date de validation » (D5, réglage `fin_duree_pf`).

**Exemple.** Déposée le 20/11/2024, notifiée le 15/01/2025 : liquidée en janvier 2025.

**Sources.** Annuaire CIPRES, glossaire p. 64 ; Recueil v7, l. 1224 (pensions) et l. 1133 (ordonnancé = notifié) ; Kaya 2017, tableau 2 ; Modèles CNSS feuille 03 ; Annuaire CNSS 2024, tableau 24 (831 allocations de vieillesse liquidées en 2024 à leur première notification, pour 833 publiées ; la date de saisie en donnerait 873, soit +4,8 %, P92) ; Données P10 (parmi les dossiers ouverts en 2025, le statut F ne donne que 310 « liquidés », alors que 15 479 sont notifiés), P48 (3 901 dossiers de pension liquidés en 2025).

**État.** Prouvée ; confirmée par l'annuaire CNSS 2024 (tableau 24). Le tableau 35 ne départage pas les dates (partie 4).

### D5 — La durée de liquidation

**Règle.** Nombre de jours de calendrier entre le dépôt et la première notification ; une durée négative compte 0. Trois choses à ne pas mêler : les colonnes de la maquette, 0 à 14 jours, 15 à 45, 46 et plus (Annuaire CIPRES p. 34) ; la part « dans la norme », liquidée en 45 jours au plus (Modèles CNSS feuille 03) ; la durée moyenne, dans la norme si elle est de **moins** de 45 jours (Annuaire CIPRES p. 11). Par ligne : la moyenne et la médiane.

**Exemple.** Reçu le 17/11/2025, notifié le 02/12/2025 : 15 jours, colonne « 15 à 45 jours ». Un dossier liquidé en 45 jours tout juste est dans la norme ; une moyenne de 45,0 jours ne l'est pas.

**Sources.** Annuaire CIPRES p. 11, 34 et 64 ; Kaya 2017, tableau 2 ; Recueil v7, l. 1095 et 1221 ; Modèles CNSS feuille 03 ; Données P10 (dossiers de vieillesse ouverts en 2025 : 17 % en moins de 15 jours, 22 % de 15 à 45, 61 % au-delà ; médiane 62 jours ; PF 13,5 jours, maternité 17 jours, AT/MP 156 jours).

**État.** Proposée ; calcul confirmé par la feuille 03. La « moyenne de 82 jours » de la fiche T25 n'est qu'un chiffre à retrouver : à vérifier à l'exécution de t25.

### D6 — En instance au 31/12 (remplace R12)

**Règle.** Une demande déposée, ni liquidée ni rejetée à la fin de la période. On écarte les dossiers fermés jamais notifiés et les réceptions d'avant le 01/01/2016 jamais reliées à un dossier (réglage `debut_stock`). En AT/MP et en PF, on écarte aussi les demandes toujours sans suite 24 mois après leur dépôt (réglage `sans_suite_mois`). Les pensions n'ont pas cette limite aujourd'hui.

**Exemple.** Une demande reçue le 17/11/2025 et notifiée le 22/01/2026 est en instance au 31/12/2025. Une demande de pension reçue en 2012 et jamais reliée à un dossier n'y est pas : elle est d'avant 2016 (réglage `debut_stock`).

**Sources.** Recueil v7, l. 126 et l. 249 ; Annuaire CIPRES p. 11 ; Modèles CNSS feuille 04 (« dossiers complets en instance ») ; Données P50 (sans aucune exclusion, 23 218 demandes de pension en instance fin 2025, dont 79 % reçues avant 2021), P57 (environ 8 000 dossiers fermés sans notification), P68.

**État.** Définition confirmée par le recueil v7 ; les exclusions sont des propositions BI. **Pour les pensions, le stock reste gonflé** : avec les réglages actuels, il resterait fin 2025 environ 1 337 réceptions de 2016 à 2020 sans dossier ni lien, 1 132 de 2021 à 2025, et environ 900 demandes pas encore notifiées dans des dossiers ouverts, soit **3 300 à 3 500 demandes** (estimation tirée de P50, à vérifier à l'exécution). C'est plus de 70 % des 4 671 dépôts de 2025 (P09) : avec la formule du recueil (N4), le taux restant à traiter serait d'environ 70 % ; avec celle de la feuille 04, de l'ordre de 40 % si le stock du début d'année est du même ordre. La norme est de 15 %. Réglage `sans_suite_mois` à créer dans t25 ; question Q2.

### D7 — Rejeté

**Règle.** Un refus enregistré dans le module des rejets de l'application (`DOSSIER_REJETE`), compté l'année du rejet. Ni le statut de la réception, ni la fermeture du dossier ne valent refus.

**Exemple.** Reçu le 02/06/2025, rejeté le 30/06/2025 : déposé et rejeté en 2025, jamais en instance, sans durée de liquidation.

**Sources.** Annuaire CIPRES p. 34 ; Recueil v7, l. 126 (« non rejeté ») ; Kaya 2017, tableau 1, l. 87 (le module existe mais n'est pas exploité) ; Données P56 (146 rejets de pension depuis 2017), P09 (14 en 2025), P55, P68 (0 à 7 rejets par an en AT/MP, 2 à 62 en PF).

**État.** Prouvée ; saisie sans doute incomplète (question Q5).

### D8 — Le Total

**Règle.** Le Total additionne toutes les demandes, y compris celles qui n'ont pas encore de ligne (une déclaration d'accident sans suite, par exemple). Sa durée moyenne est recalculée sur toutes les demandes, pas faite en moyenne des lignes.

**Exemple.** Une déclaration d'accident du 15/09/2025 qui n'a rien donné compte dans les déposés du Total de T26, sur aucune ligne.

**Sources.** Annuaire CIPRES p. 11 et 34.

**État.** Proposée. D8 donnait aussi une formule du taux restant à traiter : en instance ÷ (en instance au 1er janvier + déposés). N4 propose de la remplacer par celle du recueil ; elle reste disponible par réglage (`formule_taux` = ANCIENNE).

### D9 — AT/MP : les dépôts

**Règle.** La déclaration d'accident ou de maladie ouvre le dossier et dépose les IJ et les soins. Une demande déposée plus tard sur ce dossier dépose la rente ou l'allocation d'incapacité (avec un certificat de consolidation), ou les rentes de survivants (avec un certificat de décès).

**Exemple.** Accident déclaré le 06/01/2024 : dépôt de l'IJ en janvier 2024. Demande de rente le 10/04/2025, avec le certificat de consolidation : dépôt de la rente en avril 2025, sur le même dossier.

**Sources.** Annuaire CNSS 2024, tableaux 47 et 48 ; Données P60, P71 (91 % des consolidations suivies d'une rente ou d'une allocation, 81 % des décès d'une rente de survivant).

**État.** Proposée. L'annuaire **précise** sans confirmer : il compte des accidents déclarés, que la base retrouve à 1,4 à 3,0 % près en dossiers ouverts ; mais t26 comptera les réceptions, plus nombreuses que les dossiers (1 313 déclarations reçues en 2025 pour 1 237 dossiers ouverts, +6 % ; de 2019 à 2025, 361 déclarations sur 9 334 n'ouvrent pas de dossier, P59 et P60).

### D10 — AT/MP : les IJ et l'allocation d'incapacité

**Règle.** Une IJ est déposée avec la déclaration et liquidée à sa première notification : un arrêt prolongé, qui donne plusieurs IJ, ne fait qu'une demande. Sa durée part de la déclaration et se publie telle quelle. L'allocation d'incapacité suit D4.

**Exemple.** Déclaration le 06/01/2025, un mois d'arrêt, IJ notifiée le 15/07/2025 : 190 jours, colonne « plus de 45 jours ».

**Sources.** Annuaire CIPRES, glossaire p. 64 ; Données P61 (97 % des IJ notifiées), P62 (251 jours de médiane depuis la déclaration en 2025), P69, P72.

**État.** Proposée ; question Q10. Les « 183 jours » du README et des cartes D10 et B24 partent de la dernière réception avant la liquidation, pas de la déclaration.

### D11 — AT/MP : les soins

**Règle.** Les soins vont sur la ligne « Autres prestations de la branche » de T26 : une demande par dossier, liquidée à la saisie du premier remboursement, pas à son approbation.

**Exemple.** Déclaration le 03/02/2025, premier remboursement saisi le 12/05/2025 : 98 jours.

**Sources.** Recueil v7, l. 1157 à 1169 (la CNSS suit un délai des remboursements de soins) ; Annuaire CNSS 2024, tableaux 82 et 89 ; Données P63 (650 à 960 dossiers par an, environ 800 en moyenne hors 2024), P73 (973 vieux dossiers approuvés en bloc en 2024).

**État.** Proposée ; question Q7, réglage `soins_t26`. La base n'a que 13,5 % du montant des soins publié en 2024 (partie 4).

### D12 — AT/MP : les déclarations sans suite

**Règle.** Une déclaration qui n'a donné ni IJ, ni soins, ni rente compte dans les déposés (dans le Total, sans ligne). Elle reste en instance jusqu'à 24 mois après son dépôt, puis sort du stock.

**Exemple.** Déclaration du 15/09/2025 sans suite : en instance au 31/12/2025 et au 31/12/2026, hors du stock en septembre 2027.

**Sources.** Annuaire CNSS 2024, tableau 47 (l'annuaire compte toutes les déclarations, qu'elles aient donné quelque chose ou non) ; Données P59 (4 158 dossiers sur 9 106 ouverts de 2019 à 2025 sans aucune prestation), P68 (629 sur 1 189 en 2024, soit 52,9 %).

**État.** Question Q3 ; réglage `sans_suite_mois`.

### D13 — PF : l'enfant ajouté

**Règle.** Selon le réglage `enfant_ajoute` : à 1, chaque enfant ajouté au dossier d'un allocataire est une demande de plus, déposée à la date de demande de son allocation et liquidée à sa notification ; à 0, seuls les nouveaux allocataires comptent.

**Exemple.** Allocataire depuis 2019 ; nouveau-né demandé le 03/03/2025, notifié le 14/03/2025 : 11 jours, si `enfant_ajoute` = 1.

**Sources.** Modèles CNSS feuille 03 (délai PF mesuré sur la « 1ère demande ») ; Données P64 (environ 9 000 dossiers reçoivent un enfant de plus chaque année), P65.

**État.** Question Q13.

### D14 — Maternité : une grossesse, deux lignes

**Règle.** Le dossier d'une grossesse porte deux demandes, l'IJ de maternité et l'allocation prénatale, déposées et liquidées ensemble. Dans l'indicateur de délai des PF, la grossesse ne compte qu'une fois, par l'IJ : le recueil ne prend que les allocations familiales et les IJ de maternité.

**Exemple.** Reçu le 05/05/2025, notifié le 21/05/2025 : 16 jours, une fois sur chaque ligne de T27 ; une seule fois dans l'indicateur de délai.

**Sources.** Recueil v7, l. 1102 ; Kaya 2017, tableau 1, l. 80 à 82 (l'allocation prénatale n'aurait « ni réception ni notification ») ; Données P66 (c'est vrai pour l'ancien code FAP, 33 en 2025, plus pour l'allocation prénatale de maternité MAP : 4 245 notifiées en 2025, 15 jours après la demande), P67 (99,4 % des dossiers portent les deux) ; README, lot 4.

**État.** Proposée ; question Q14, réglage `grossesse_deux_lignes`.

### R13 — Le cliché du calcul

**Règle.** L'exercice N se calcule sur la copie de la base de fin mars N+1, date à laquelle la CNSS arrête son annuaire.

**Exemple.** T25 à T27 de 2025 se calculent sur le cliché de fin mars 2026.

**Sources.** README, R13 ; Données P99 (444 décès de retraités de 2024 connus fin mars 2025, pour 447 publiés au tableau 26 de l'annuaire CNSS 2024).

**État.** Proposée le 01/11/2026 (points_a_valider n° 2). Aujourd'hui, le seul cliché disponible s'appelle 092026 ; il porte la copie arrêtée au 02/02/2026 : pour 2025, deux mois trop tôt (les notifications et rejets saisis en février et mars 2026 manquent) ; pour 2024, dix mois trop tard.

### N1 — Mois, trimestre, année

**Règle.** Chaque dépôt, liquidation ou rejet compte au mois où il se produit ; un trimestre ou une année additionne ses mois. Le stock en instance ne s'additionne pas : il se lit à la fin du dernier mois de la période.

**Exemple.** T27 du 2e trimestre 2025 : dépôts et liquidations d'avril, mai et juin ; en instance au 30/06/2025.

**Sources.** Recueil v7, l. 128 et 249 ; indicateurs l. 1100 et 1228 (trimestriel) ; l. 35 ; Annuaire CNSS 2024, tableau 24 (liquidations par mois) et tableau 39 (bénéficiaires d'IJ par mois, date à vérifier).

**État.** Nouvelle, à intégrer dans t25 à t27 (la `dtm_dossier` proposée le fait déjà, avec les anciennes règles).

### N2 — Par type, direction régionale et agence

**Règle.** Chaque demande porte la direction régionale de son dossier (sinon celle de sa réception) et son agence, qui est le service du dossier (`SP_NO`), sinon celui de la réception. Les délais et les taux restant à traiter se découpent par type de prestation, DR et agence.

**Exemple.** Une demande de pension déposée à l'agence de Koudougou (service 3310 dans la base, rattaché à la direction régionale de Ouagadougou, DRO) compte à la DRO et à l'agence de Koudougou.

**Sources.** Recueil v7, l. 4 à 10, 1065, 1102, 1109, 1155, 1168, 1178, 1187, 1233, 1245, et l. 31, 68, 74 ; Annuaire CNSS 2024, p. xvii à xix (16 agences : DRO 5, DRB 6, DRN 2, DRF 2, DRD 1) et tableaux 73 et 74 (accidents déclarés par « lieu CNSS ») ; Données P46 (les 20 services de la base sont 15 agences et les 5 sièges de DR), comparaison A02.

**État.** Nouvelle, à intégrer. À confirmer par la DSI (question Q20) : où ranger les sièges de DR, et pourquoi 4 agences sont inactives dans la base (Tanghin, Sakaby, Diapaga, Tougan) alors que l'annuaire 2024 les cite.

### N3 — T27 : frais médicaux de maternité

**Règle.** La dernière ligne de T27 s'appelle « Frais médicaux de maternité ». Elle compte les dossiers de maternité qui ont des frais pris en charge : accouchement, consultation, examens, hospitalisation, kit opératoire, pharmacie, radio.

**Exemple.** La CNSS prend en charge la césarienne d'une assurée : son dossier compte sur la ligne « Frais médicaux de maternité ». Aujourd'hui la ligne resterait vide, car la base n'a aucun de ces paiements.

**Sources.** Recueil v7, l. 127 ; Annuaire CNSS 2024, tableaux 40 et 41 (25 030 869 FCFA de frais médicaux en 2024) ; Données P35 (7 types de frais de maternité prévus), P05 et P95 (aucun paiement dans la base de 2020 à 2025).

**État.** Nouvelle ; la ligne reste vide tant que la source des données n'est pas trouvée (question Q15).

### N4 — Le taux de dossiers restant à traiter

**Règle.** Par défaut, la formule du recueil v7 : taux = dossiers en instance à la fin de la période ÷ dossiers complets reçus dans la période × 100. La formule de la feuille 04 de la CNSS reste disponible par réglage : en instance à la fin ÷ (en instance au début + reçus − rejetés). La norme de 15 % au plus vient de la feuille 04.

**Exemple.** 100 en instance au 1er janvier, 400 reçus, 10 rejetés, 340 liquidés, 150 en instance au 31/12. Formule du recueil : 150 ÷ 400 = 37,5 %. Feuille 04 : 150 ÷ 490 = 30,6 %. Ancienne formule de D8 : 150 ÷ 500 = 30,0 %. Hors norme dans les trois cas.

**Sources.** Recueil v7, l. 1104, 1180 et 1239 (formule lue dans le document Word) ; Kaya 2017, tableau 2 (la même formule) ; Modèles CNSS feuille 04 ; Annuaire CIPRES p. 11 (le reste à traiter, sans formule).

**État.** Nouvelle, à valider. Réglage `formule_taux` ; question Q19. Le recueil (source plus forte) et la feuille 04 (pratique de la CNSS) ne disent pas la même chose.

### N5 — Les indicateurs de délai

**Règle.** Pour les indicateurs du recueil, le délai est la moyenne des durées (D5), avec l'écart type et la part liquidée en 45 jours au plus. Par défaut, il porte sur toutes les demandes liquidées. La feuille 03 de la CNSS ne mesure que la « 1ère demande » (nouvel allocataire en PF, première demande de rente en AT/MP, première demande en pension) : c'est une lecture BI, à confirmer, et un réglage. L'indicateur des PF ne prend que les allocations familiales et les IJ de maternité.

**Exemple.** Trois dossiers PF liquidés en 11, 16 et 40 jours : délai moyen 22,3 jours, écart type 15,5 jours (formule ECARTYPE de la feuille 03), 100 % en 45 jours au plus.

**Sources.** Recueil v7, l. 1094 à 1103 et 1217 à 1238 ; Modèles CNSS feuille 03 ; Annuaire CIPRES p. 11.

**État.** Nouvelle, à valider. Réglage `premiere_demande` ; question Q18.

### N6 — Retrouver chaque chiffre

**Règle.** La BI garde la liste des demandes : une ligne par demande, avec sa branche, sa ligne, ses dates de dépôt, de liquidation et de rejet, sa durée, sa DR et son agence. Chaque case de T25 à T27 se retrouve dans cette liste.

**Exemple.** Les 833 allocations de vieillesse liquidées en 2024 se listent une à une, mois par mois.

**Sources.** Recueil v7, l. 51 et 68 (et l. 29) ; Modèles CNSS feuille 03 (liste des dossiers évalués).

**État.** Nouvelle ; aucune requête ne la produit aujourd'hui.

### N7 — Les normes et les alertes

**Règle.** Chaque indicateur à norme porte la mention « dans les normes » ou « hors normes ». Délai moyen : dans la norme s'il est de moins de 45 jours. Part liquidée en 45 jours au plus : affichée à côté. Taux restant à traiter : dans la norme s'il est de 15 % au plus. Les normes sont des réglages de la CTE `parametres`, pas des chiffres écrits dans la requête.

**Exemple.** Dossiers de vieillesse ouverts en 2025 : 39 % liquidés en 45 jours au plus, médiane 62 jours (P10). Le délai moyen sera hors norme.

**Sources.** Recueil v7, l. 32 (alertes) ; Annuaire CIPRES p. 11 (moins de 45 jours) ; Modèles CNSS feuilles 03 et 04 (colonne « Observation »).

**État.** Nouvelle, à intégrer.

### N8 — Le taux de variation des déclarations d'AT/MP

**Règle.** Taux = (déclarations de l'année n − déclarations de l'année n−1) ÷ déclarations de l'année n−1 × 100, en %, à deux décimales, par DR et par agence. Une déclaration est la réception qui ouvre un dossier AT/MP (D9). En attendant de savoir ce que sont les déclarations qui n'ouvrent pas de dossier, on compte les dossiers AT/MP ouverts.

**Exemple.** 2024 dans la base : (1 189 − 1 226) ÷ 1 226 = −3,02 %. Publié : (1 171 − 1 202) ÷ 1 202 = −2,58 %.

**Sources.** Recueil v7, l. 1146 à 1156 (formule lue dans le document Word) ; Annuaire CNSS 2024, tableau 38 (« glissement » calculé de la même façon) et tableau 47 ; Données P68.

**État.** Nouvelle, à intégrer.

---

## 3. Les cas

| Cas | Ce que fait la règle | Pourquoi |
|---|---|---|
| 1. Une veuve dont la pension est ajoutée au dossier de son mari retraité, décédé en 2024 ; demande le 10/03/2025 (réception « ANCIEN »), droits notifiés le 09/05/2025 | T25 de 2025 : une demande de plus, déposée et liquidée sur la ligne « Pension de survivant », en 60 jours (plus de 45 jours), même si le dossier date de 2012 | Compté (D2, D3, D4) |
| 2. Une veuve et trois orphelins du même défunt, notifiés le même jour dans le même dossier | T25 : une seule demande sur la ligne « Pension de survivant ». L'annuaire CNSS compte quatre nouveaux pensionnés (tableau 23) | Compté une fois (D2) ; la ligne de T25 sera plus basse que l'annuaire, c'est normal |
| 3. Un dossier de coordination payé par la caisse du Mali, reçu et étudié par la CNSS (`COOR_AC`) | Compté dans T25 : déposé, liquidé. La `dtm_dossier` proposée l'écarte | Selon un réglage (`coordination` = GARDER ; D1 ; Q4) |
| 4. Une demande de pension reçue en 2012, jamais reliée à un dossier | Comptée dans les déposés de 2012, pas en instance fin 2025, car reçue avant le 01/01/2016 | Selon un réglage (`debut_stock` ; D6) |
| 5. Une demande de pension reçue en 2019, dossier ouvert, jamais notifiée ni rejetée | En instance fin 2025, six ans après : les pensions n'ont pas de limite de 24 mois. 3 300 à 3 500 demandes seraient dans ce cas ou voisins fin 2025 (estimation) | À surveiller (D6 ; réglage `sans_suite_mois` à créer dans t25 ; Q2) |
| 6. Une pension radiée avant 2018 dont la notification n'a jamais été saisie, dossier fermé | Ni liquidée, ni en instance, ni rejetée. Environ 8 000 dossiers dans ce cas | Pas compté (D6, D7 ; P57) |
| 7. Un dossier reçu le 17/11/2025, notifié le 22/01/2026 | T25 de 2025 : déposé en novembre, en instance au 31/12/2025. T25 de 2026 : liquidé en janvier, en 66 jours | Compté (H2, D3 à D6). Avec la lecture « cohorte », il ne compterait en liquidé dans aucun tableau (Q1) |
| 8. Une allocation de vieillesse (versée en une fois, code RAV) notifiée le 14/05/2024 | T25 de 2024 : liquidée sur la ligne « Allocations de vieillesse », au mois de mai. Le tableau 24 de l'annuaire en publie 833 ; la base en trouve 831 | Compté (D4, N1) ; confirmé par l'annuaire |
| 9. Une pension de retraite anticipée (code RPRA) | T25 : déposée et liquidée sur sa ligne à part. L'annuaire compte 3 nouvelles en 2024 : la ligne sera presque vide | Compté (R2 ; recueil l. 248) |
| 10. Une pension de vieillesse avec une majoration pour enfant (code RBEC) dans le même dossier | Une seule demande, ligne « Pension de vieillesse » | Pas compté en plus (A1) |
| 11. Une victime d'accident : IJ notifiée en 2022 ; demande de rente le 10/04/2025 avec le certificat de consolidation ; rente notifiée le 24/04/2025 | T26 : deux demandes. La rente est déposée et liquidée en 2025, en 14 jours depuis la demande au guichet ; depuis le certificat, on aurait 64 jours de médiane | Selon un réglage (`depart_rente` ; D2, D9 ; Q8) |
| 12. Une déclaration d'accident du 15/09/2025 qui ne donne rien (ni arrêt, ni soins, ni rente) | T26 : déposée en 2025 (Total, sans ligne) ; en instance au 31/12/2025 et 2026 ; hors du stock en septembre 2027. 53 % des déclarations de 2024 sont dans ce cas | Selon un réglage (`sans_suite_mois` ; D12 ; Q3) |
| 13. Une déclaration d'accident reçue sans qu'aucun dossier ne s'ouvre | t26 la compte dans les déposés ; les dossiers ouverts ne la voient pas. 361 sur 9 334 de 2019 à 2025 | À surveiller (D9, N8) : doublons possibles, à regarder à l'exécution |
| 14. Un dossier d'accident avec seulement des soins ; premier remboursement saisi 98 jours après la déclaration | Une demande sur la ligne « Autres prestations de la branche » de T26, liquidée en 98 jours | Question (D11 ; Q7) ; la base n'a que 13,5 % du montant publié des soins |
| 15. Un rachat de rente d'accident (capital versé à la place de la rente ; dans la base, un ajustement de type RR) | Pas compté dans T26 aujourd'hui. 11 rachats en 2024 dans la base, 25,4 millions FCFA publiés | Question (Q11 ; réglage `rachat_rente_t26`) |
| 16. Un enfant ajouté au dossier d'un allocataire de 2019 ; demande le 03/03/2025, notifiée le 14/03/2025, sans passer au guichet | Avec `enfant_ajoute` = 1 : une demande de plus, liquidée en 11 jours. Avec 0 : rien dans T27 | Selon un réglage (D13 ; Q13) |
| 17. Une grossesse : dossier reçu le 05/05/2025, IJ et allocation prénatale notifiées le 21/05/2025 | T27 : deux demandes, en 16 jours. Indicateur de délai des PF : une seule (l'IJ) | Question (D14 ; Q14) ; le Total de T27 baisse d'environ 4 200 par an si une seule ligne |
| 18. Les frais médicaux d'une césarienne prise en charge par la CNSS | Le recueil veut une ligne « Frais médicaux de maternité ». La base n'a aucun paiement de ces frais de 2020 à 2025 ; l'annuaire en publie 25,0 millions en 2024 | À surveiller (N3 ; Q15) : ce dossier n'apparaît nulle part aujourd'hui |

---

## 4. Face à l'annuaire CNSS 2024

| Tableau de l'annuaire | Ce qu'on compare | Nous | Publié | Écart | Verdict | Ce que ça change |
|---|---|---|---|---|---|---|
| Tableaux 18 à 113 | Dossiers déposés, en instance, rejetés ; durées de liquidation | — | rien, pour aucune branche | — | pas publié | Ces colonnes de T25 à T27 ne se valident qu'avec la CNSS (fiches T25 à T27) |
| Tableau 24 (p. 19) | Allocations de vieillesse liquidées en 2024 (T25, ligne « Allocations de vieillesse ») | 831 personnes, 832 allocations (première notification, P92, P93) | 833 | −2 (−0,24 %) ; −1 (−0,12 %) | confirme | D4 : la CNSS compte à la notification (la saisie donnerait 873). Elle publie au mois et par DR : modèle de N1 et N2 |
| Tableau 35 (p. 30) | IJ de maternité de 2020 à 2024 (T27, ligne « IJ de maternité ») | Liquidées : 3 438, 3 810, 3 834, 4 069, 3 890 | 3 428, 3 810, 3 973, 4 050, 3 893 | +0,29 %, 0,00 %, −3,50 %, +0,47 %, −0,08 % | précise | Une grossesse = un bénéficiaire par an. Mais la date ne se départage pas : la saisie et l'ouverture du dossier tombent mieux en 2022 et 2023 (dossiers ouverts : +3,85 %, +0,34 %, +0,08 %, +0,25 %, +1,16 %) |
| Tableau 39 (p. 32) | IJ de maternité de 2024 par mois et par DR | 3 890 sur l'année ; pas encore au mois | 3 893 (DRO 2 822, DRB 575, DRN 246, DRF 165, DRD 85) | −3 (−0,08 %) sur l'année | précise | La CNSS suit au mois et par DR, comme le demande le recueil. Une comparaison mois par mois dira si le mois publié est celui de la notification |
| Tableau 47 (p. 38) | Accidents déclarés de 2019 à 2024 (T26, déposés) | Dossiers ouverts : 1 328, 1 403, 1 353, 1 370, 1 226, 1 189 | 1 303, 1 383, 1 334, 1 330, 1 202, 1 171 | +1,42 % à +3,01 % chaque année (+1,54 % en 2024) | précise | Une déclaration ≈ un dossier. Mais t26 comptera les réceptions, plus nombreuses (+6 % en 2025) : écart de T26 à 1 171 pas encore calculé |
| Tableau 47 (p. 38) | Taux de variation des déclarations, 2024 (N8) | −3,02 % (dossiers ouverts) | −2,58 % | 0,44 point | précise | Même sens, même ordre de grandeur. La formule de N8 est celle du recueil et du tableau 38 |
| Tableau 38 (p. 31) | La façon de calculer un taux de variation | (n − n−1) ÷ (n−1), recueil l. 1146 | « glissement » en %, deux décimales | aucun | confirme | La formule de N8 est la pratique de la CNSS |
| Tableaux 48 et 72 (p. 38 et 58) | Accidents déclarés en 2024 par DR et par type | pas encore par DR | DRO 736, DRB 347, DRN 31, DRF 26, DRD 31 ; 1 150 accidents, 21 maladies | — | précise | Modèle du découpage par DR de N2 et N8. T26 ne sépare pas accident et maladie : rien à changer |
| Tableaux 73 et 74 (p. 58 et 59) | Accidents déclarés en 2024 par « lieu CNSS » | pas calculé | 22 lieux (Ouagadougou 649, Bobo-Dioulasso 207, Banfora 85…), total 1 171 | — | précise | Modèle publié du découpage « par agence » (N2) |
| Pages xv à xix | Les directions et les agences de la CNSS | 15 agences et 5 sièges de DR dans la base, 4 agences inactives (P46) | 16 agences (DRO 5, DRB 6, DRN 2, DRF 2, DRD 1) | à rapprocher | précise | Appuie « agence = service du dossier » (N2) et donne les destinataires des questions (DCP, DPASS, DAI, DPCG, DPC) |
| Tableau 22 (p. 18) | Pensionnés au 31/12/2024 par DR, avec la DR du dossier | Comparaison A02 | 62 231 | Vieillesse +0,1 % à +4,0 % selon la DR ; ascendants exacts ; veuves −8,1 % (DRB) et −11,7 % (DRD). Orphelins hors comparaison : A02 ne retire pas les 25 ans et plus | précise | La DR du dossier donne des chiffres plausibles, rien de plus (N2). Les veuves tiennent sans doute aux radiations (A06, A07) |
| Tableau 23 (p. 19) | Nouveaux pensionnés de 2024 (vieillesse et anticipée) | 2 397 notifiés ; 2 463 à la saisie (P82) | 2 489 (vieillesse 2 486 + anticipée 3) | −92 (−3,7 %) ; −26 (−1,0 %) | précise | La CNSS publie l'anticipée à part : appuie la ligne du recueil (R2). Le tableau compte des personnes à la saisie, T25 des demandes à la notification : la ligne des survivants de T25 sera bien plus basse que 1 542 |
| Tableau 27 (p. 21) | « Départs à la retraite » de 2024 | pas calculé pour 2024 | 3 729 | — | non comparable | Si c'est une demande de pension reçue, ce serait le seul chiffre publié de dépôts (Q21). 3 729 dépasse de 12,3 % les entrées publiées de 2024 : 2 489 nouveaux retraités (tableau 23) + 833 allocations de vieillesse (tableau 24) = 3 322 |
| Tableau 75 (p. 60) | Nouvelles rentes viagères de 2024 (T26, ligne « Rentes viagères ») | 39 premières rentes ; 57 personnes notifiées, révisions comprises ; 69 rentes saisies (68 personnes) (P82, P91) | 44 | −5 (−11,4 %) | écart à expliquer | La ligne tournera autour de 40 par an. La CNSS semble compter quelques révisions comme nouvelles. Écart déjà noté au lot 7, sans réponse |
| Tableau 77 (p. 61) | Nouveaux ayants droit de rentes AT/MP en 2024 | 63 personnes (P82) | 61 | +2 (+3,3 %) | précise | Des personnes, pas des demandes : la ligne « Rentes de survivants » de T26 sera bien plus basse (D2) |
| Tableaux 28 et 34 (p. 22 et 28) | Majoration pour enfant 2024 | 539 376 000 FCFA (P98) | 537 343 000 FCFA, rangés avec la pension | +2 033 000 (+0,4 %) | confirme | A1 : un accessoire suit la pension. Remboursements nuls en 2024 : la ligne « Autres prestations » de T25 sera presque vide |
| Tableaux 40 et 41 (p. 32 et 33) | Frais médicaux de maternité 2024 (T27, N3) | 0 FCFA (P95) | 25 030 869 FCFA, payés tous les mois, dans 4 DR sur 5 | tout le montant | contredit | Contredit la fiche T27 (« frais jamais payés ») : ils sont payés, mais hors de la base lue. La ligne du recueil a un vrai contenu (Q15). Confirme R1 (maternité dans les PF) |
| Tableaux 82, 89, 98 et 99 (p. 63 à 75) | Soins AT/MP 2024 (T26, ligne « Autres prestations ») | 18 787 945 FCFA, 692 débours (P95) | 139 668 695 FCFA | −120 880 750 (−86,5 %) | écart à expliquer | Des dossiers de soins peuvent manquer à T26 (D11). Question déjà posée à la DSI (README) |
| Tableaux 90, 96 et 97 (p. 71 et 74) | Rachats de rente AT/MP 2024 | 11 rachats, 21 519 361 FCFA (P104) | 25 366 909 FCFA | −3 847 548 (−15,2 %) | précise | La CNSS traite le rachat comme une prestation à part. Faut-il le compter dans T26 ? (Q11) |
| Tableau 118 (p. 88) | « Dossiers médicaux PF » des centres de santé maternelle et infantile | — | 4 555 dossiers médicaux, 368 dossiers IJ | — | non comparable | Des dossiers vus par les médecins, pas des dossiers de prestations : ne pas les confondre avec les déposés de T27 |

**En clair, ce que l'annuaire confirme :**
- une liquidation compte à la notification des droits (D4) : 831 allocations de vieillesse pour 833 publiées ;
- un accessoire suit la pension (A1) ;
- la maternité est dans la branche des prestations familiales (R1) ;
- la retraite anticipée se publie à part (R2) ;
- le taux de variation se calcule comme le dit le recueil (N8).

**Ce qu'il précise sans trancher :** la date de comptage des IJ de maternité (tableau 35) ; la DR du dossier (tableau 22) ; ce qu'est une déclaration pour T26 (tableau 47) ; l'agence (tableaux 73 et 74, liste p. xvii à xix).

**Ce qu'il contredit :** la fiche T27, qui dit les frais médicaux de maternité « jamais payés » : la CNSS en paie 25,0 millions en 2024, hors de notre base.

**Ce qu'il faut expliquer :** les soins AT/MP (la base n'a que 13,5 % du montant) ; les nouvelles rentes viagères (39 pour 44).

**Ce qu'il ne publie pas :** les dépôts, les dossiers en instance, les rejets, les durées. Le tableau 27 (« départs à la retraite ») pourrait être un chiffre de dépôts : à demander (Q21), et à comparer dès maintenant aux réceptions de pension de 2024.

---

## 5. Les réglages

Ils s'écrivent dans la CTE `parametres`, le bloc de réglages en tête de chaque requête. Changer une valeur ne demande pas de réécrire la requête. Une décision attendue de la CNSS ne bloque rien : la valeur retenue s'applique en attendant. « À créer » : le choix est aujourd'hui écrit en dur dans la requête, ou n'existe pas.

| Réglage | Requête | Valeur retenue | Autre valeur | Ce que ça change | Qui décide |
|---|---|---|---|---|---|
| `debut_stock` | t25 | DATE '2016-01-01' | une autre date ; DATE '1900-01-01' pour ne rien écarter | Une réception de pension d'avant cette date, jamais reliée à un dossier, ne compte pas en instance. Sans ce réglage, 13 149 réceptions d'avant 2016 resteraient en instance pour toujours (P50) | DCP (Q2 ; fiche T25 Q3) |
| `debut_stock` | t26 | DATE '2016-01-01' | idem | Même rôle pour les réceptions AT/MP | DP et DR (Q2) |
| `debut_stock` | t27 | DATE '2016-01-01' | idem | Même rôle pour les réceptions PF et maternité | DES et DR (Q2) |
| `sans_suite_mois` | t26 | 24 | 12, 36 ; 999 pour tout garder | Une demande sans suite 24 mois après son dépôt sort du stock : surtout les déclarations qui n'ont rien donné (629 sur 1 189 en 2024) | DP et DR (Q2, Q3) |
| `sans_suite_mois` | t27 | 24 | 12, 36 ; 999 | Sort du stock les réceptions PF sans dossier (483 en 2025) et les vieilles allocations jamais notifiées (32 % des allocations familiales sans date de notification, P66) | DES et DR (Q2) |
| `sans_suite_mois` (à créer) | t25 | aucune limite (comme aujourd'hui) | 24, comme t26 et t27 | Sans limite, environ 3 300 à 3 500 demandes en instance fin 2025 (estimation, à vérifier), un taux d'environ 70 % (formule du recueil) ; avec 24 mois, il ne resterait que les demandes de 2024 et 2025 | DCP (Q2) |
| `lecture_periode` (à créer) | t25, t26, t27 | STOCK : chaque étape dans son année, en instance = tout ce qui attend au 31/12 | COHORTE : seulement les dossiers déposés dans l'année ; déposés = liquidés + en instance + rejetés | La cohorte sort d'elle-même les vieilles demandes, mais un dossier déposé en décembre et liquidé en janvier n'est liquidé dans aucun tableau | DCP (Q1) |
| `coordination` (à créer) | t25 (en dur : gardée) ; dtm_dossier (en dur : écartée) | GARDER | EXCLURE | 238 dépôts et 292 liquidations de pension en 2025 (6 à 8 % de T25) ; un seul réglage évite que les deux requêtes se contredisent | DCP (Q4) |
| `reception_statut_a` (à créer) | t25 | EN_INSTANCE | ANNULEE | 111 réceptions de pension au statut A sans dossier depuis mai 2025 (P55) : en instance, ou hors du stock | DSI (Q6) |
| `soins_t26` (à créer) | t26 (en dur : soins comptés) | OUI | NON | OUI : 650 à 960 dossiers de soins par an, environ 800 en moyenne, en « Autres prestations ». NON : T26 ne compte que les prestations en espèces (343 liquidées en 2025) | DP et DR (Q7) |
| `depart_rente` (à créer) | t26 (en dur : guichet, sinon certificat, sinon déclaration) | GUICHET (exception provisoire au recueil) | CERTIFICAT_SINON_GUICHET : le certificat de consolidation quand il existe, comme le recueil | Médiane 2025 : 14 jours depuis la demande, 64 depuis le certificat. Le certificat manque pour 15 rentes viagères sur 43 liquidées en 2025, et pour 12 rentes de survivants sur 13 (P62) | DP et DR (Q8, Q9) |
| `rachat_rente_t26` (à créer) | t26 | NON | OUI : un rachat = un dossier liquidé, ligne « Autres prestations » | 11 rachats en 2024 dans la base | DP et DR (Q11) |
| `enfant_ajoute` | t27 | 1 | 0 | 1 : environ 16 400 liquidations d'allocations familiales en 2025. 0 : 8 246 (nouveaux allocataires seulement) | DES et DR (Q13) |
| `grossesse_deux_lignes` (à créer) | t27 (en dur : deux demandes) | 1 | 0 (une seule demande, l'IJ) | 2025 : 4 273 IJ de maternité et 4 245 allocations prénatales ; avec 0, le Total de T27 baisse d'environ 4 200 | DES et DR (Q14) |
| `fin_duree_pf` (à créer) | t27 (en dur : première notification) | NOTIFICATION (repli provisoire : la date de validation n'est pas connue) | VALIDATION, comme le recueil, dès que la DSI a trouvé la date | Change la fin de la durée des PF et de la maternité | DES ; DSI pour le champ (Q16) |
| `formule_taux` (à créer ; proposé à la place de la formule de D8) | t25, t26, t27 | RECUEIL : en instance à la fin ÷ reçus dans la période | GRILLE : en instance à la fin ÷ (en instance au début + reçus − rejetés) ; ANCIENNE : ÷ (en instance au début + déposés) | Le recueil donne un taux plus haut (37,5 % contre 30,6 % dans l'exemple de N4) | Auteur de la grille, à identifier (DAI, DPCG ou DPC) ; en attendant la DCP (Q19) |
| `premiere_demande` (à créer) | indicateurs de délai (N5) | TOUTES : toutes les demandes liquidées | PREMIERE : la première demande seulement, comme la feuille 03 | En PF, l'indicateur sans les enfants ajoutés ; en AT/MP, la rente seule (ni IJ ni soins) | DCP, DES, DP (Q18) |
| `norme_delai_jours`, `norme_taux` (à créer) | t25, t26, t27 (N7) | 45 jours (moyenne de moins de 45) ; 15 % | d'autres normes | Mention « dans les normes » ou « hors normes » | DCP, avec la DAI |
| `periode` (à créer) | t25, t26, t27 (N1) | MOIS : chaque événement à son mois, stock en fin de mois ; l'outil de restitution (OBIEE) additionne les mois en trimestres et en années | ANNEE (comme aujourd'hui) | Permet T25 et T27 au mois, au trimestre, à l'année | tranché par le recueil (l. 128) |
| `agence` (à créer) | t25, t26, t27 (N2) | SERVICE : service du dossier, sinon de la réception | LIEU_PAIEMENT | Le découpage par agence des délais et des taux | DSI (Q20) |
| Le cliché (valeur donnée au lancement, `:1`, pas un réglage de `parametres`) | t25, t26, t27, dtm_dossier | 092026 (copie du 02/02/2026) | fin mars N+1 pour l'exercice N (R13) | Fixe ce que la base sait des notifications, rejets et déclarations saisis tard | BI, avec la DSI (points_a_valider n° 2 et n° 65) |

---

## 6. Les questions ouvertes

Chaque question a une valeur par défaut dans la requête : la réponse change un réglage, pas le travail. Les destinataires suivent le recueil v7 : la DCP (Direction centrale des prestations) pour les pensions ; la DP et les DR (directions régionales) pour les AT/MP (DP : sigle du recueil, à faire préciser, DCP ou DPASS) ; la DES (sigle du recueil, ou la direction qui l'a remplacée) et les DR pour les PF. Les autres sigles viennent de l'annuaire CNSS 2024 : DSI (Direction des systèmes d'information), DPC (Direction de la prospective et de la coopération, qui fait l'annuaire), DAI (Direction de l'audit interne), DPCG (Direction de la planification et du contrôle de gestion), DPASS (Direction de la prévention et de l'action sanitaire et sociale).

### Q1 — Le tableau de 2025 doit-il aussi compter les dossiers déposés avant 2025 (liquidés en 2025, ou encore en attente au 31/12/2025), ou seulement les dossiers déposés en 2025 ?
**À qui :** DCP, pour les trois tableaux (la phrase du recueil est écrite pour T27 : DES et DR ; reprise pour T25 par « Idem. Tab 27 »).
**En attendant :** chaque étape dans son année ; en instance = tout ce qui attend au 31/12 (`lecture_periode` = STOCK).
**Pourquoi on la pose :** « au cours de la période » (l. 126) se lit de deux façons. La feuille 04 de la CNSS appuie le stock ; la lecture « cohorte » rendrait le tableau additif et sortirait d'elle-même les vieilles demandes.
**Déjà posée :** README (« Un dossier compte-t-il l'année de son dépôt ou l'année de sa liquidation ? ») ; points_a_valider n° 14.

### Q2 — Au bout de combien de mois une demande restée sans suite doit-elle sortir des dossiers en instance (ou faut-il la garder sans limite) ?
**À qui :** DCP (pensions), DP et DR (AT/MP), DES et DR (PF).
**En attendant :** `debut_stock` = 01/01/2016 partout ; `sans_suite_mois` = 24 en AT/MP et en PF ; aucune limite pour les pensions.
**Pourquoi on la pose :** le recueil ne prévoit aucune limite. Sans limite, 23 218 demandes de pension seraient en instance fin 2025 ; avec les réglages actuels, encore 3 300 à 3 500 (estimation), soit un taux restant à traiter d'environ 70 % avec la formule du recueil (de l'ordre de 40 % avec celle de la feuille 04), pour une norme de 15 %.
**Déjà posée :** fiche T25 Q3, fiche T26 Q1, fiche T27 Q3 ; points_a_valider n° 21 (à étendre aux pensions, avec ce chiffre) et n° 64 (les réceptions sans dossier).

### Q3 — Une déclaration d'accident qui n'a donné ni arrêt de travail, ni soins, ni rente compte-t-elle comme un dossier déposé, en attente de traitement ?
**À qui :** DP et DR.
**En attendant :** comptée dans les déposés (Total) ; en instance 24 mois au plus.
**Pourquoi on la pose :** 629 des 1 189 dossiers AT/MP ouverts en 2024 n'ont rien donné. L'annuaire les compte dans ses 1 171 accidents déclarés.
**Déjà posée :** fiche T26 Q1 ; points_a_valider n° 23.

### Q4 — Un dossier de coordination, payé par une autre caisse mais reçu et étudié par la CNSS, compte-t-il dans T25 à T27 ?
**À qui :** DCP.
**En attendant :** compté (`coordination` = GARDER).
**Pourquoi on la pose :** 6 à 8 % de T25 en 2025 ; l'annuaire compte ces pensions dans ses effectifs ; la `dtm_dossier` proposée les écarte.
**Déjà posée :** fiche T25 Q1 ; points_a_valider n° 19 ; README.

### Q5 — Tous les refus de dossier sont-ils saisis dans le module des rejets de l'application Prestations ?
**À qui :** DCP, avec la DSI.
**En attendant :** seuls les refus du module comptent ; un refus non saisi reste en instance.
**Pourquoi on la pose :** 14 rejets de pension en 2025, 146 depuis 2017 ; Kaya 2017 notait déjà que le module n'était pas exploité.
**Déjà posée :** fiche T25 Q5, fiche T27 Q4 ; points_a_valider n° 22 ; README (DSI).

### Q6 — Une réception de pension au statut « A » (un code de l'application dont le sens n'est écrit nulle part), sans dossier, est-elle en attente ou annulée ?
**À qui :** DSI.
**En attendant :** comptée en instance (`reception_statut_a` = EN_INSTANCE).
**Pourquoi on la pose :** 111 réceptions dans ce cas depuis mai 2025 (P55) ; une réception annulée doit sortir du stock.
**Déjà posée :** README (sens des statuts, à la DSI) ; points_a_valider n° 58.

### Q7 — Les remboursements de soins des accidents du travail entrent-ils dans T26, sur la ligne « Autres prestations de la branche » ?
**À qui :** DP et DR.
**En attendant :** oui : un dossier soigné = une demande, liquidée à la saisie du premier remboursement.
**Pourquoi on la pose :** environ 800 dossiers par an ; le recueil suit un délai des remboursements de soins. Mais la base n'a que 13,5 % du montant des soins publié.
**Déjà posée :** fiche T26 Q2 ; points_a_valider n° 24.

### Q8 — La durée d'une rente viagère part-elle de la réception du certificat de consolidation, ou de la demande déposée au guichet ?
**À qui :** DP et DR.
**En attendant :** la demande au guichet (`depart_rente` = GUICHET, exception provisoire au recueil).
**Pourquoi on la pose :** le recueil et Kaya 2017 citent le certificat, mais il manque pour 15 rentes viagères sur 43 liquidées en 2025 ; médiane 14 jours depuis la demande, 64 depuis le certificat.
**Déjà posée :** fiche T26 Q3 ; points_a_valider n° 25 ; remarque B16 du recueil.

### Q9 — Dans le recueil, le délai des « rentes de survivants » part du certificat de consolidation, qui concerne la victime : ce délai vise-t-il en fait la rente de la victime elle-même (rente viagère) ?
**À qui :** DP et DR.
**En attendant :** une rente de survivant part de la demande du survivant au guichet (avec le certificat de décès).
**Pourquoi on la pose :** le certificat de consolidation concerne la victime ; il n'est rempli que pour 1 rente de survivant sur 13 liquidées en 2025.
**Déjà posée :** remarque B16 du recueil.

### Q10 — Entre la fin de l'arrêt de travail et la notification de l'indemnité journalière d'accident (211 jours en médiane), quelle pièce ou quelle étape attend-on ?
**À qui :** DP et DR.
**En attendant :** durée comptée depuis la déclaration, publiée telle quelle.
**Pourquoi on la pose :** arrêt de 27 jours en médiane, IJ notifiée 211 jours après sa fin ; ni la reprise du travail ni le certificat de guérison ne sont saisis (P69).
**Déjà posée :** fiche T26 Q4 ; points_a_valider n° 26.

### Q11 — Un rachat de rente d'accident du travail est-il un dossier liquidé à compter dans T26 ?
**À qui :** DP et DR.
**En attendant :** non compté (`rachat_rente_t26` = NON) : dans la base, c'est un ajustement, pas une prestation.
**Pourquoi on la pose :** 11 rachats en 2024 dans la base ; l'annuaire publie 25,4 millions de rachats (tableau 90) ; Kaya 2017 ajoute « achat de rente » dans T29.
**Déjà posée :** nouvelle (voisine de points_a_valider n° 52, pour T56).

### Q12 — T26 n'a aucune ligne dans la liste du recueil : le produit-on comme l'Annuaire CIPRES le décrit, avec les mêmes règles que T25 et T27 ?
**À qui :** DP et DR (structures des indicateurs AT/MP du recueil), avec la DCP.
**En attendant :** oui ; ses durées et son taux suivent les indicateurs AT/MP du recueil.
**Pourquoi on la pose :** T26, T39, T40 et T56 n'ont aucun commentaire dans le recueil, et Kaya 2017 ne les examinait pas. Restent sans appui : les lignes, les déposés, les rejetés.
**Déjà posée :** remarque B38 du recueil.

### Q13 — Quand un allocataire déclare un nouvel enfant, est-ce un dossier déposé et liquidé de plus dans T27 ?
**À qui :** DES et DR.
**En attendant :** oui (`enfant_ajoute` = 1).
**Pourquoi on la pose :** 8 246 liquidations d'allocations familiales en 2025 sans les enfants ajoutés, environ 16 400 avec ; la feuille 03 ne mesure que la « 1ère demande ».
**Déjà posée :** fiche T27 Q1 ; points_a_valider n° 27.

### Q14 — Une grossesse compte-t-elle sur les deux lignes de T27 (IJ de maternité et allocations prénatales), ou une seule fois ?
**À qui :** DES et DR.
**En attendant :** deux lignes dans T27 ; une seule (l'IJ) dans l'indicateur de délai, comme le recueil (l. 1102).
**Pourquoi on la pose :** 99,4 % des dossiers de maternité portent les deux prestations, notifiées ensemble.
**Déjà posée :** fiche T27 Q2 ; points_a_valider n° 28.

### Q15 — Où sont enregistrés les frais médicaux de maternité que la CNSS paie (25,0 millions FCFA en 2024) ?
**À qui :** DSI, avec la DPC (Service études et statistique).
**En attendant :** ligne « Frais médicaux de maternité » vide.
**Pourquoi on la pose :** le recueil demande cette ligne ; la base prévoit 7 types de frais de maternité mais n'a aucun paiement de 2020 à 2025.
**Déjà posée :** nouvelle (remarque B27 du recueil). La famille T50-T57 la reprend pour T56 (T50-T57 Q17) : une seule réponse vaut pour les deux.

### Q16 — Quelle date de l'application est la « date de validation » d'un dossier de prestations familiales ?
**À qui :** DSI, avec la DES.
**En attendant :** la notification des droits (`fin_duree_pf` = NOTIFICATION).
**Pourquoi on la pose :** le recueil a changé la phrase de Kaya 2017 pour faire finir le délai des PF à la validation. Pistes à tester : la date du calcul initial (étape CIP, P40), la date de signature de la notification imprimée (`NOTIFICATION_PREST`, P72), l'approbation du premier paiement.
**Déjà posée :** remarque B18 du recueil.

### Q17 — Où sont enregistrées la date de réception du certificat du médecin traitant et la date de convocation, qui font le délai des remboursements de soins ?
**À qui :** DSI, avec la DP.
**En attendant :** pas d'indicateur ; T26 mesure les soins de la déclaration au premier remboursement.
**Pourquoi on la pose :** le recueil définit ce délai avec ces deux dates. Champs proches à tester : pièces « CME, certificat médical » reçues au guichet (339 réceptions, P70), date de réception de l'avis du médecin conseil (`DOS_DATE_REC_AVIS_MC`, P62), date de rendez-vous de chaque réception (P49).
**Déjà posée :** remarque B17 du recueil.

### Q18 — Pour les indicateurs de délai, mesure-t-on seulement la première demande (nouvel allocataire, première demande de rente, première demande de pension), comme votre feuille 03 ?
**À qui :** DCP, DES, DP.
**En attendant :** toutes les demandes liquidées (`premiere_demande` = TOUTES).
**Pourquoi on la pose :** la feuille 03 évalue le délai sur la « 1ère demande » ; en AT/MP, sur la rente seule.
**Déjà posée :** nouvelle.

### Q19 — Pour le taux de dossiers restant à traiter, quelle formule retenir : celle du recueil (en instance à la fin ÷ reçus dans l'année) ou celle de votre feuille 04 (en instance à la fin ÷ (en instance au début + reçus − rejetés)) ?
**À qui :** l'auteur de la grille des ratios, à identifier (DAI, DPCG ou DPC) ; en attendant, la DCP.
**En attendant :** la formule du recueil (`formule_taux` = RECUEIL), norme 15 %.
**Pourquoi on la pose :** le recueil et Kaya 2017 donnent la même formule ; la feuille 04 en donne une autre, avec la norme.
**Déjà posée :** fiche T25, règle 10 ; points_a_valider n° 15 (qui ne voyait que la formule de la feuille 04).

### Q20 — L'agence d'un dossier est-elle bien son service dans l'application (`SP_NO`) ?
**À qui :** DSI.
**En attendant :** agence = service du dossier, sinon de la réception (`agence` = SERVICE).
**Pourquoi on la pose :** les 20 services de la base sont 15 agences et 5 sièges de DR. Reste à savoir si le siège de la DRO est l'« Agence centrale » de l'annuaire, et pourquoi 4 agences citées par l'annuaire 2024 sont inactives dans la base.
**Déjà posée :** remarque B34 du recueil (la déclinaison).

### Q21 — Votre tableau 27 « départs à la retraite » (3 729 en 2024) compte-t-il les demandes de pension reçues au guichet ?
**À qui :** DPC (Service études et statistique).
**En attendant :** pas de comparaison ; la BI compare tout de même les réceptions de pension de 2024 (droits propres, sans l'allocation de vieillesse, l. 1251) au tableau 27, mois par mois et par DR.
**Pourquoi on la pose :** si oui, ce serait le seul chiffre publié de dossiers déposés.
**Déjà posée :** nouvelle.

### Q22 — Pour la CIPRES, faut-il aussi garder la maquette d'origine de T25 et T27, sans la ligne « retraite anticipée » et avec « Autres prestations de la branche » ?
**À qui :** DCP, avec la DPC.
**En attendant :** la version du recueil ; la maquette d'origine se refait en additionnant les lignes (anticipée avec vieillesse).
**Pourquoi on la pose :** le recueil change les tableaux CIPRES eux-mêmes ; rien ne dit si la CIPRES accepte ces changements.
**Déjà posée :** nouvelle (relevé du recueil, remarques).

---

## 7. Les points d'attention

1. **L'annuaire CNSS 2024 ne publie ni dépôts, ni dossiers en instance, ni rejets, ni durées.** Ces colonnes ne se valident qu'avec la CNSS. L'annuaire sert à trois contrôles directs (tableaux 24 ; 35 et 39 ; 47, 48 et 72), à des contrôles indirects en personnes (tableaux 23, 75, 77) et comme modèle de découpage (tableaux 73 et 74).
2. **Personnes ou demandes.** L'annuaire compte des personnes, T25 à T27 des demandes. Une veuve et ses orphelins font une seule demande : les lignes de survivants de T25 et T26 seront bien plus basses que les 1 542 nouveaux survivants (tableau 23) et les 61 nouveaux ayants droit (tableau 77). Ce n'est pas une erreur.
3. **La `dtm_dossier` proposée (29/10) suit encore les anciennes règles :** elle compte des dossiers et non des demandes, écarte la coordination, ne rattache pas les demandes déposées sur un dossier existant (réversions, rentes, qui restent alors en instance pour toujours), date les soins à l'approbation et n'a pas de limite de stock. Elle seule a le mois et la DR : il faut fusionner, puis obtenir l'accord de Jules, qui tient l'outil de restitution (OBIEE), car le sens de certaines colonnes de `DTM_DOSSIER` change.
4. **Rien n'est exécuté, et le cliché est mal placé.** Les chiffres de 2025 viennent des lots, vus au 02/02/2026 : il manque les saisies de février et mars 2026 (R13).
5. **Le stock des pensions reste gonflé** (D6) : 3 300 à 3 500 demandes en instance fin 2025 avec les réglages actuels, estimation à vérifier. Le taux restant à traiter serait d'environ 70 % avec la formule du recueil (N4), de l'ordre de 40 % avec celle de la feuille 04 : loin de la norme de 15 % dans les deux cas.
6. **« Idem. Tab 27 » pour T25** reprend les définitions et la périodicité, pas le renommage en « Frais médicaux de maternité » (lecture BI, à signaler).
7. **T26 n'a pas de ligne dans la liste du recueil,** mais ses durées et son taux sont définis par les indicateurs AT/MP du recueil (l. 1146 à 1188) et de Kaya 2017 (tableau 2). Seules ses lignes, ses déposés et ses rejetés reposent sur l'Annuaire CIPRES et sur nos données.
8. **Trois délais du recueil ne s'appliquent pas à la lettre.** Le délai des « rentes de survivants » part d'un certificat qui concerne la victime ; le délai des soins part de dates pas encore trouvées ; le délai des PF finit à une date de validation inconnue. Pour les deux premiers, les réglages gardent la pratique de la base ; pour le dernier, la notification en repli.
9. **Trois normes à ne pas mêler** (D5, N7) : colonnes 0 à 14, 15 à 45, 46 et plus ; part en 45 jours au plus ; moyenne de moins de 45 jours.
10. **La feuille 04 de la grille** oublie le stock du début d'année dans la ligne des risques professionnels (à signaler à la CNSS), et déduit les instances de fin par différence (à traiter − liquidés), alors que nos requêtes les comptent : les deux chiffres différeront des demandes sorties du stock sans suite.
11. **La feuille 03 est un modèle vide,** qui prévoit une liste de 100 dossiers au plus par branche (sans doute un échantillon, à confirmer). Les délais de la BI, sur toutes les demandes, ne seront pas comparables à d'éventuelles évaluations faites à la main.
12. **La couverture de la base :** les soins AT/MP de la base font 13,5 % du montant publié, et les frais médicaux de maternité n'y sont pas. Les lignes « Autres prestations » de T26 et « Frais médicaux de maternité » de T27 risquent d'être incomplètes.
13. **IJ de maternité 2022 :** 3 834 liquidées pour 3 973 publiées (−3,5 %), alors que les autres années tombent à moins de 0,5 % ; cette année-là, le chiffre publié colle aux dossiers ouverts (3 976). À éclaircir avant de comparer l'historique.
14. **Les déclarations AT/MP de la base dépassent l'annuaire de 1,4 à 3,0 % chaque année** (dossiers ouverts), et t26 comptera des réceptions, plus nombreuses encore. À comparer sur un cliché de fin mars.
15. **Le stock ne s'additionne pas :** pour un trimestre, on prend le stock de fin de trimestre, et celui de la fin du trimestre d'avant comme stock de début.
16. **Les documents existants, laissés tels quels :** la fiche T25 et la fiche de lecture ne citent que Kaya 2017 pour la ligne « retraite anticipée », alors que le recueil v7 la demande aussi ; la fiche T27 dit les frais de maternité « jamais payés » ; le README et les cartes D10 et B24 donnent 183 jours pour les IJ AT/MP, compté depuis la dernière réception (251 jours depuis la déclaration, P62) ; la carte D5 de `regles_prestations` donne 18 %, 22 % et 60 % et une « moyenne 82 j », alors que P10 mesure 17 %, 22 % et 61 % (dossiers de vieillesse ouverts en 2025) et qu'aucune moyenne n'a été mesurée.
17. **Deux familles de remarques B.** Les remarques B01 à B41 du recueil (`remarques_besoins_v7`) n'ont rien à voir avec les cartes B1 à B25 de `regles_prestations` : ce document écrit toujours « remarque Bnn du recueil ».

---

## 8. Comment c'est vérifié

- **Sources relues pour ce document :** `besoins_v7.txt`, l. 4 à 75, 115 à 128, 142 à 144, 246 à 250, 1065, 1093 à 1252, 1978 à 1989 ; les formules des indicateurs dans le document Word (extraction `recueil_out/document_dump.txt`, l. 1998, 2068, 2118, 2209) et celles de Kaya 2017 (`kaya_out/document_dump.txt`, l. 1442, 1551, 1641) ; `kaya.txt`, l. 75 à 87, 262 à 271, 856 à 942 ; l'annuaire CNSS 2024, liste des agences (l. 789 à 867 du texte) ; `resultats/lot3_P46_services_provinciaux.tsv` ; les en-têtes des CTE `parametres` de t25, t26 et t27.
- **Recalculs :** taux de variation 2024 (−3,02 % et −2,58 %) ; part liquidée en 45 jours au plus en vieillesse 2025 ((560 + 708) ÷ 3 233 = 39 %, P10) ; les exemples de N4 et N5. Les chiffres de l'annuaire et des lots viennent du dossier de la famille, recalculés par sa contre-vérification (sommes, totaux et écarts justes).
- **Lots utilisés :** 3 B (P48 à P58), 4 (P59 à P73), 7 (P82, P91), 8 (P92 à P100), 9 (P104) ; comparaisons A01 et A02. Aucune requête nouvelle n'a été lancée ; aucune table DTM n'a été lue.
- **Corrections reprises de la contre-vérification :**
  - *erreur* : T26 n'est pas « sans appui » : ses durées et son taux viennent des indicateurs AT/MP du recueil et de Kaya 2017 (partie 1, Q12, point d'attention 7) ; titre du tableau 118 sans « (2/2) ».
  - *manque* : l'agence est proposée (service du dossier, P46, annuaire p. xvii à xix, tableaux 73 et 74), la question à la DSI devient une confirmation (N2, Q20) ; estimation du stock des pensions et réglage `sans_suite_mois` pour t25 (D6, Q2) ; remarque de Kaya 2017 sur l'allocation prénatale et limite de l'indicateur PF (D14, Q14) ; cliché 092026 = copie du 02/02/2026 (R13, point 4) ; 183 contre 251 jours (point 16).
  - *imprécision* : lecture stock ou cohorte (statut « question », H2 « appuyée par la feuille 04 », Q1) ; tableau 35, verdict « précise » et D4 confirmée par le seul tableau 24 ; tableaux 47 et 22, verdict « précise » ; fin du délai PF : le recueil l'emporte, la notification n'est qu'un repli (D4, `fin_duree_pf`, Q16) ; délai des soins en « question » et non « hors base », source P63 et non Kaya 2017 (Q17) ; D5 avec les chiffres de P10 et la moyenne de 82 jours « à vérifier » ; D3 avec l'allocation d'incapacité ; trois normes séparées (D5, N7) ; feuille 03 « modèle vide, sans doute un échantillon » ; auteur de la grille à identifier ; questions adressées aux structures du recueil ; `depart_rente` présenté comme exception provisoire ; P56 et P09 pour les rejets ; 650 à 960 dossiers de soins, environ 800 en moyenne ; N1 avec le tableau 39 « date à vérifier » ; renvois « remarque Bnn du recueil » ; l. 51 et 68 pour N6 ; 69 rentes saisies (68 personnes).
  - *demandes du recueil oubliées*, toutes reprises en partie 1 : l. 32 (N7), l. 1102 (D14), structures responsables (partie 6), l. 51 et 68 (N6), l. 142 à 144 (R1), l. 1251 (Q21).
  - *tableaux de l'annuaire oubliés*, repris en partie 4 : tableaux 73 et 74, pages xv à xix, tableau 38.
- **Deux points où ce document va plus loin que le dossier et sa contre-vérification :**
  - **la formule du taux restant à traiter.** Le dossier proposait d'aligner la BI sur la feuille 04 (N4) et disait que Kaya 2017 ne donnait pas de formule. Le document Word du recueil v7 et celui de Kaya 2017 donnent tous deux : en instance à la fin ÷ dossiers complets reçus dans l'exercice × 100. Le recueil étant la source la plus forte, c'est la valeur par défaut (`formule_taux` = RECUEIL) ; la feuille 04 reste une autre valeur (Q19) ;
  - **le taux de variation des déclarations.** Le dossier le donnait comme « proposition BI » ; la contre-vérification proposait le tableau 38. La formule est en fait dans le recueil (document Word, l. 2068 de l'extraction) : N8 cite le recueil, et le tableau 38 comme pratique de la CNSS.
- **Un choix de forme :** la ligne d'ensemble de l'annuaire (« tableaux 18 à 113 ») porte le verdict « pas publié » plutôt que « non comparable » : l'annuaire ne publie pas ces colonnes.
- **Une question retirée :** la pension de la veuve (fiche T25 Q4) ne change plus les chiffres ; elle est dite tranchée par les données dans D2.
- **Relecture finale (08/11/2026) :** toutes les lignes du recueil relues une à une (l. 1 à 335, 1060 à 1300, 1975 à 2210) ; numéros de ligne de Kaya 2017 corrigés (dossiers incomplets l. 86, module des rejets l. 87) ; taux du stock des pensions donné avec la formule du recueil retenue par défaut (environ 70 %, et non 40 %, qui est l'ordre de grandeur de la feuille 04) ; origine des 3 322 entrées publiées (tableaux 23 et 24) ; orphelins du tableau 22 hors comparaison ; sens de H2 dans `regles_prestations` rappelé ; destinataires de Q1 et Q12 alignés sur le recueil ; points_a_valider n° 20, 64 et 65 cités ; variantes AT/MP et pensions du ratio des ressources citées. Chiffres recontrôlés dans les sources : tableaux 23, 24, 27, 35, 39, 47, 48, 72 à 74 et pages xv à xix de l'annuaire CNSS 2024 ; P09, P10, P46, P48 à P58 (lot 3 B), P59 à P73 (lot 4), A02 ; écarts recalculés.
- **Relecture de clarté (08/11/2026), lue comme par un cadre de la CNSS qui ne connaît pas la base :** ajout d'un paragraphe « Les mots de ce document » (déposé, liquidé, en instance, stock, demande, réversion, consolidation, cliché, réglages, Pnn) ; sigles des directions expliqués en tête de la partie 6, d'après l'annuaire CNSS 2024 (DSI, DPC, DAI, DPCG, DPASS) ; la `dtm_dossier`, OBIEE et le schéma `USER_DWH` expliqués ; questions Q1, Q2, Q3, Q6, Q9 et Q10 réécrites pour ne poser qu'une question chacune, lisible seule (Q2 : une seule question au lieu de deux ; Q10 : on demande quelle pièce ou étape est attendue) ; exemples de R2, D6, D9, N2 et N3 rendus plus concrets (D9 : des dates passées au lieu de 2027). Aucun chiffre, aucune source, aucun statut, aucun verdict n'a changé.
- **Critique d'exhaustivité et de cohérence entre les cinq familles, 08/11/2026.** Le recueil v7 (l. 115 à 335 et indicateurs l. 1018 à 1378) a été relu à nouveau face aux cinq documents de famille, à la liste du recueil (`besoins_v7_prestations.md`) et au document sur l'annuaire (`annuaire_cnss_2024_prestations.md`). Changements ici : l'« En bref » ne range plus la retraite anticipée parmi ce que l'annuaire « confirme » (sa carte, tableau 23, dit « précise » ; c'est le recueil qui demande la ligne, R2) et explique « IJ » ; les lignes l. 127 et l. 1104, 1180, 1239 citent la question de leur règle (Q15, Q19) ; la satisfaction et le document de référence (l. 80 et 81) sont rangés dans la famille T50-T57 (hors base ; question T50-T57 Q25) ; Q15 dit que T50-T57 Q17 la reprend. Aucun statut, aucun verdict, aucune source n'a changé.

---

## 9. Ce qui reste à faire

| Tâche | Qui | État |
|---|---|---|
| Exécuter t25, t26 et t27 sur le cliché 092026, puis sur un cliché de fin mars N+1 quand il existera (R13) | toi (exécution), puis Claude | à faire |
| Porter dans t25 à t27 le mois de chaque événement, la DR et l'agence (N1, N2), avec le stock en fin de mois ; ou reprendre la `dtm_dossier` avec les règles du 30/10 | Claude, puis accord de Jules | à faire |
| Ajouter les réglages « à créer » dans les CTE `parametres` (partie 5) | Claude | proposé, à faire après visa |
| Ajouter l'écart type, la part en 45 jours au plus, le taux selon `formule_taux` et la mention de norme (N4, N5, N7) | Claude | à faire |
| Renommer la ligne 4 de T27 « Frais médicaux de maternité » (N3), et la nourrir quand la source sera connue | Claude, après la DSI | à faire |
| Produire la liste des demandes (N6) | Claude | à faire |
| Comparer 2024 aux chiffres publiés, mois par mois et par DR : allocations de vieillesse (tableau 24), IJ de maternité (tableau 39), déclarations (tableaux 47, 48, 72, 73, 74), avec les déclarations reçues et non les seuls dossiers ouverts | Claude, après exécution | à faire |
| Requête de contrôle des IJ de maternité de 2022 : notification, ouverture ou saisie ? | Claude, puis toi | à faire |
| Comparer les réceptions de pension de 2024 (droits propres) au tableau 27, mois par mois et par DR, sans attendre la réponse de la DPC | Claude, puis toi | à faire |
| Calculer le taux de variation des déclarations par DR et par agence (N8) | Claude | à faire |
| Mesurer le stock de T25 avec les réglages proposés, et avec `sans_suite_mois` = 24 | toi (exécution), puis Claude | à faire |
| Compter les réceptions que t25 et t26 traitent comme « ANCIEN » sans l'être (D3), ou ajouter un test sur `RD_TYPE_RECEPTION` | Claude, puis toi | à faire |
| Répondre aux questions Q1 à Q22 (DCP, DP et DR, DES et DR, DSI, DPC, auteur de la grille) | CNSS, via toi | à poser |
| Signaler à l'auteur de la grille l'oubli du stock de début d'année (feuille 04, risques professionnels) | toi | à faire |
| Si l'utilisateur le décide : reporter ces règles dans le README, les fiches T25 à T27, la fiche de lecture, `regles_prestations` et points_a_valider | Claude, sur demande | non commencé (rien n'a été modifié) |
