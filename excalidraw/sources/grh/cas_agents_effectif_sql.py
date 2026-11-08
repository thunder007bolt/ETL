"""
Schéma « Les cas d'agents dans la requête SQL » (état au 07/11/2026).

Version technique de cas_agents_effectif : chaque cas d'agent est relié à la
partie de analyses/grh/t58_t65_effectif_requete_proposee.sql qui le traite, au
bout de SQL qui décide, et à la raison du verdict.
1. la requête en un coup d'œil (parties 0 à 8, CTE, lignes du fichier, grain) ;
2. le cœur : le WHERE de base, ligne par ligne ;
3. les permanents ; 4. les temporaires ; 5. les cas entre les deux ;
6. le rangement (axes et mesures) ; 7. le comptage final.

Les numéros de ligne sont ceux du fichier au 07/11/2026 : à revoir si la
requête change. Les bouts de SQL sont recopiés du fichier ; « … » marque une
coupure, et les commentaires « -- » ajoutés disent ce que vaut la condition.

Usage :
    python3 excalidraw/sources/grh/cas_agents_effectif_sql.py [--force]
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from excalidraw_builder import C, Diagram, main, text_size  # noqa: E402

OUTPUT = ROOT / "schemas" / "grh" / "T58 effectif" / "cas_agents_effectif_sql.excalidraw"

WIDTH = 3040
SQL_SIZE = 17

# genre : (fond du verdict, bord, épaisseur, style, fond de la ligne)
GENRES = {
    "oui": (C["vert_moyen"], C["vert"], 2, "solid", C["blanc"]),
    "inactif": (C["jaune"], C["jaune_bord"], 2, "solid", C["blanc"]),
    "non": (C["gris"], C["gris_bord"], 2, "solid", C["blanc"]),
    "attention": (C["rouge_moyen"], C["rouge"], 3, "solid", C["rouge_clair"]),
    "absent": (C["blanc"], C["gris_bord"], 2, "dashed", C["blanc"]),
    "colonne": (C["bleu_clair"], C["bleu"], 2, "solid", C["blanc"]),
}

# colonnes des tableaux : (titre, largeur)
COLONNES = [("Le cas", 440), ("Verdict", 290), ("Où dans la requête", 400),
            ("Le SQL qui décide", 1180), ("Pourquoi", 590)]
COLONNES_RANGEMENT = [("L'axe", 440), ("Colonnes de la table", 290),
                      ("Où dans la requête", 400), ("Le SQL", 1180), ("À savoir", 590)]
GAP_COL = 15

# ---------------------------------------------------------------- 3. permanents
PERMANENTS = [
    ("En poste", "COMPTÉ", "oui",
     "7 · base, WHERE, branche G2\n(l. 445 à 448)",
     "NVL(si.nature, '-') <> 'TEMPORAIRE'          -- vrai\n"
     "AND p.date_entree <= da.date_arrete          -- vrai\n"
     "AND (p.etat <> 'INACTIF' OR …)               -- vrai",
     "Les trois conditions de la branche des permanents sont vraies."),
    ("Tout juste embauché (fiche sans état)", "COMPTÉ", "oui",
     "2 · personnes (l. 236)\npuis 7 · WHERE (l. 448)",
     "NVL(p.ETAT_EMPLOYE, '-') AS etat\n"
     "…\n"
     "AND (p.etat <> 'INACTIF' OR …)     -- '-' <> 'INACTIF' : vrai",
     "La fiche vide devient '-', qui n'est pas 'INACTIF' : l'agent compte comme encore là."),
    ("Embauché après le 31/12", "PAS COMPTÉ", "non",
     "2 · personnes (l. 240)\npuis 7 · WHERE (l. 446)",
     "COALESCE(p.DATE_EMBAUCHE, p.DATE_PRISE_SERVICE) AS date_entree\n"
     "…\n"
     "AND p.date_entree <= da.date_arrete          -- faux",
     "Sa date d'entrée (embauche, sinon prise de service) tombe après la date d'arrêté."),
    ("Parti avant le 31/12, ou ce jour-là", "PAS COMPTÉ", "non",
     "0 · decisions_depart (l. 176)\n3 · departs, premier_depart\n(l. 274 à 290)\n"
     "7 · WHERE (l. 448)",
     "WHERE code IN (SELECT code FROM decisions_depart)   -- 02 34 35 36 52 54\n"
     "SELECT PERS_ID, MIN(date_depart) AS date_depart     -- le premier départ\n"
     "…\n"
     "AND (p.etat <> 'INACTIF'                 -- faux : fiche INACTIF\n"
     "     OR pd.date_depart > da.date_arrete) -- faux : parti avant ou le 31/12",
     "Les deux côtés du OR sont faux. Un départ le 31/12 même ne passe pas : la comparaison "
     "est stricte (>)."),
    ("Parti après le 31/12", "COMPTÉ CETTE ANNÉE-LÀ", "oui",
     "3 · premier_depart (l. 286)\n7 · WHERE (l. 448)",
     "AND (p.etat <> 'INACTIF'                 -- faux : INACTIF aujourd'hui\n"
     "     OR pd.date_depart > da.date_arrete) -- vrai : 15/01/2025 > 31/12/2024",
     "La fiche INACTIF d'aujourd'hui ne l'empêche pas : son départ tombe après la date "
     "d'arrêté."),
    ("Décédé, sans décision de décès", "SORT À SA DATE DE DÉCÈS", "non",
     "3 · departs (l. 279 à 283)",
     "UNION ALL\n"
     "SELECT PERS_ID, DATE_DECES\n"
     "FROM DWH.FAIT_GRH_PERSONNE\n"
     "WHERE CLICHE = :1\n"
     "  AND DATE_DECES IS NOT NULL",
     "La date de décès entre dans departs comme un départ ; premier_depart garde la plus "
     "ancienne."),
    ("Départ annulé (décision 40)", "COMPTÉ (DÉPART IGNORÉ)", "oui",
     "3 · decisions (l. 267 à 271)",
     "AND NOT EXISTS (SELECT 1\n"
     "                FROM DWH.FAIT_GRH_DECISION a\n"
     "                WHERE a.CLICHE = :1\n"
     "                  AND TO_CHAR(a.CDEC_CODE) = cd.annulation          -- '40'\n"
     "                  AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))",
     "Une décision visée par une 40 n'entre pas dans decisions : ni dans departs, ni dans "
     "engagements, ni dans positions."),
    ("En disponibilité (07 sans retour 18)", "COMPTÉ, MAIS INACTIF", "inactif",
     "5 · positions (l. 330 à 340)\n7 · base, colonne inactif\n(l. 421 à 427)",
     "MAX(CASE WHEN d.code = cd.mise_en_disponibilite THEN d.date_decision END)  -- 07\n"
     "MAX(CASE WHEN d.code = cd.remise_en_activite    THEN d.date_decision END)  -- 18\n"
     "…\n"
     "ELSE CASE WHEN NVL(po.date_dispo,  DATE '1900-01-01')\n"
     "             > NVL(po.date_remise, DATE '1900-01-01')\n"
     "          THEN 1 ELSE 0 END                   -- 1 = inactif",
     "Au 31/12, sa dernière 07 est plus récente que sa dernière 18 : inactif = 1. Il reste "
     "compté, car disponibilite = 'PRESENT' (ligne [4] du WHERE)."),
    ("Revenu de disponibilité (18)", "COMPTÉ, ACTIF", "oui",
     "5 · positions, interruptions,\ninterruptions_a_date\n(l. 330 à 375)\n"
     "7 · base, anciennete (l. 413)",
     "ELSE CASE WHEN NVL(po.date_dispo,  DATE '1900-01-01')\n"
     "             > NVL(po.date_remise, DATE '1900-01-01')\n"
     "          THEN 1 ELSE 0 END          -- 18 plus récente : 0, actif\n"
     "…\n"
     "GREATEST(FLOOR((da.date_arrete - p.debut_anciennete\n"
     "                - CASE pr.retirer_interruptions\n"
     "                      WHEN 'OUI' THEN NVL(ia.jours, 0) ELSE 0 END)\n"
     "               / 365.25), 0)                AS anciennete",
     "Actif au 31/12. Les jours de disponibilité (ia.jours) sont retirés de son ancienneté, "
     "car retirer_interruptions = 'OUI'."),
    ("Détaché, mis à disposition, suspendu, sous les drapeaux", "COMPTÉ, ACTIF", "oui",
     "7 · base, colonne inactif\n(l. 421 à 423)",
     "CASE WHEN pr.inactifs = 'STATUT'            -- faux : réglage 'DECISIONS'\n"
     "     THEN CASE WHEN si.STATUT_EMPLOYE IN ('B','D','E','F','H')\n"
     "               THEN 1 ELSE 0 END\n"
     "     ELSE …                                 -- seules 07 et 18 comptent",
     "Le statut de la situation n'est lu qu'avec le réglage STATUT. Il n'est pas daté "
     "(lot 24)."),
    ("Fiche INACTIF, sans aucune date de départ", "JAMAIS COMPTÉ", "attention",
     "3 · premier_depart (aucune ligne)\n7 · base, LEFT JOIN (l. 434)\net WHERE (l. 448)",
     "LEFT JOIN premier_depart pd ON pd.PERS_ID = p.PERS_ID   -- rien : pd.* = NULL\n"
     "…\n"
     "AND (p.etat <> 'INACTIF'                 -- faux\n"
     "     OR pd.date_depart > da.date_arrete) -- NULL > date : jamais vrai",
     "En SQL, une comparaison avec NULL n'est jamais vraie : la condition échoue à chaque "
     "31/12. Ce sont surtout les 84 engagés de 2019 (G11)."),
    ("Cas absents de la base (lot 25)", "AUCUN CAS AUJOURD'HUI", "absent",
     "3 · premier_depart (l. 287)\n7 · WHERE (l. 448)",
     "MIN(date_depart) AS date_depart   -- parti puis revenu : seul le 1er départ\n"
     "…\n"
     "AND (p.etat <> 'INACTIF'          -- fiche active malgré un départ :\n"
     "     OR …)                        -- vrai, le OR s'arrête là",
     "Parti puis revenu : G2 ne lit que le premier départ. Fiche active malgré un départ "
     "saisi : la fiche l'emporterait. Aucun des deux n'existe."),
]

# ---------------------------------------------------------------- 4. temporaires
TEMPORAIRES = [
    ("Sous contrat le 31/12", "COMPTÉ", "oui",
     "4 · temporaires_sous_contrat\n(l. 317 à 324)\n7 · WHERE (l. 455)",
     "JOIN contrats c ON c.debut       <= da.date_arrete\n"
     "               AND da.date_arrete <  c.fin\n"
     "…\n"
     "AND tc.PERS_ID IS NOT NULL                    -- vrai",
     "Une ligne dans temporaires_sous_contrat pour ce 31/12 et cet agent : la date tombe "
     "dans un contrat."),
    ("Contrat fini avant le 31/12, ou ce jour-là", "PAS COMPTÉ", "non",
     "4 · contrats (l. 306 à 314)\n7 · WHERE (l. 455)",
     "LEAST(NVL(MIN(dep.date_depart), DATE '9999-12-31'),\n"
     "      NVL(g.debut_suivant,      DATE '9999-12-31'))  AS fin\n"
     "…\n"
     "LEFT JOIN departs dep ON dep.PERS_ID     = g.PERS_ID\n"
     "                     AND dep.date_depart > g.debut   -- 1er départ après le début",
     "Fin = premier départ après le début (35 ou autre). Si le 31/12 >= fin, aucune ligne "
     "dans temporaires_sous_contrat : tc.PERS_ID est NULL."),
    ("Contrat renouvelé (nouvelle 50)", "COMPTÉ, SANS COUPURE", "oui",
     "4 · engagements (l. 295 à 303)\npuis contrats",
     "LEAD(e.date_decision) OVER (PARTITION BY e.PERS_ID\n"
     "                            ORDER BY e.date_decision) AS debut_suivant\n"
     "…\n"
     "NVL(g.debut_suivant, DATE '9999-12-31')      -- fin du contrat 1",
     "Le contrat 1 finit le jour où le 2 commence (jour exclu : la veille). Aucun 31/12 ne "
     "tombe dans un trou."),
    ("Fin de contrat, puis réengagé", "COMPTÉ PENDANT SES CONTRATS", "oui",
     "4 · contrats (une ligne par 50)\n5 · interruptions (l. 355 à 363)\n"
     "7 · base, anciennete",
     "SELECT dep.PERS_ID, dep.date_depart AS debut,       -- la 35\n"
     "       (SELECT MIN(e.date_decision)\n"
     "        FROM decisions e, codes_decision c3\n"
     "        WHERE e.PERS_ID = dep.PERS_ID\n"
     "          AND e.code IN (c3.engagement_temporaire, c3.engagement_permanent)\n"
     "          AND e.date_decision > dep.date_depart) AS fin   -- la 50 suivante",
     "Chaque 50 ouvre un contrat ; entre la 35 et la 50 suivante, aucun contrat, donc pas "
     "compté. Ce trou est une interruption, retirée de l'ancienneté."),
    ("Aucune fin de contrat saisie", "COMPTÉ (RÉGLAGE)", "oui",
     "4 · contrats,\ntemporaires_sous_contrat\n7 · WHERE (l. 456 à 458)",
     "fin = DATE '9999-12-31'                         -- ni départ, ni contrat suivant\n"
     "MAX(CASE WHEN c.fin = DATE '9999-12-31' THEN 1 ELSE 0 END) AS fin_inconnue\n"
     "…\n"
     "AND NOT (pr.fin_contrat_inconnue = 'FICHE'    -- faux : réglage 'PRESENT'\n"
     "         AND tc.fin_inconnue = 1\n"
     "         AND p.etat = 'INACTIF')",
     "Le contrat reste ouvert jusqu'en 9999. Avec le réglage FICHE, une fiche INACTIF le "
     "ferait sortir."),
    ("Aucun engagement 50", "PAS COMPTÉ", "attention",
     "4 · engagements (aucune ligne)\n7 · base, LEFT JOIN (l. 437)\net WHERE (l. 455)",
     "WHERE e.PERS_ID IS NOT NULL\n"
     "  AND e.code = cd.engagement_temporaire      -- '50' : aucune ligne\n"
     "…\n"
     "LEFT JOIN temporaires_sous_contrat tc …       -- rien : tc.* = NULL\n"
     "AND tc.PERS_ID IS NOT NULL                    -- faux",
     "Sans 50, pas de contrat : on ne sait pas quand il commence. 234 agents au lot 13, "
     "presque tous anciens."),
    ("Engagement annulé (40)", "CE CONTRAT EST IGNORÉ", "non",
     "3 · decisions (l. 267 à 271)",
     "AND NOT EXISTS (SELECT 1 FROM DWH.FAIT_GRH_DECISION a\n"
     "                WHERE …\n"
     "                  AND TO_CHAR(a.CDEC_CODE) = cd.annulation          -- '40'\n"
     "                  AND TO_CHAR(a.NO_DECI_MODIF) = TO_CHAR(d.DECI_NO))",
     "La 50 annulée n'entre pas dans decisions, donc pas dans engagements : ce contrat "
     "n'existe pas pour la requête."),
]

# ---------------------------------------------------------------- 5. entre les deux
ENTRE_LES_DEUX = [
    ("Temporaire devenu permanent", "SUIT SA SITUATION DU 31/12", "oui",
     "6 · situations (l. 380 à 391)\n7 · base, LEFT JOIN (l. 431)\net WHERE (l. 445, 454)",
     "ROW_NUMBER() OVER (PARTITION BY da.date_arrete, s.PERS_ID\n"
     "                   ORDER BY s.DATE_DEBUT DESC, s.SITU_ID DESC) AS rn\n"
     "…   AND si.rn = 1                            -- la dernière situation\n"
     "NVL(si.nature, '-') <> 'TEMPORAIRE'          -- branche G2, après\n"
     "si.nature = 'TEMPORAIRE'                     -- branche G7, avant",
     "À chaque 31/12, la dernière situation choisit la branche : celle des temporaires "
     "avant, celle des permanents ensuite."),
    ("Aucune situation au 31/12", "COMPTÉ, MAIS DANS AUCUN TABLEAU", "attention",
     "6 · situations (aucune ligne)\n7 · base, LEFT JOIN (l. 431)\n8 · NATURE_EMPLOI",
     "LEFT JOIN situations si … AND si.rn = 1       -- rien : si.* = NULL\n"
     "NVL(si.nature, '-') <> 'TEMPORAIRE'           -- '-' : branche G2\n"
     "si.nature AS NATURE_EMPLOI                    -- NULL dans la table",
     "Compté avec la règle des permanents, mais NATURE_EMPLOI est NULL. Le filtre OBIEE "
     "(= PERMANENT ou = TEMPORAIRE) l'écarte des deux tableaux."),
    ("Agents B et C", "HORS DES TABLEAUX CIPRES", "non",
     "2 · personnes (l. 238)\n8 · QUALITE_EMPLOYE",
     "TO_CHAR(p.QUALITE) AS qualite                 -- A, B ou C\n"
     "p.qualite AS QUALITE_EMPLOYE                  -- aucun filtre dans la requête",
     "La requête garde les trois qualités. Les tableaux CIPRES filtrent QUALITE_EMPLOYE = 'A' "
     "dans OBIEE."),
]

# ---------------------------------------------------------------- 6. rangement
RANGEMENT = [
    ("Catégorie (T58, T59, T61)", "GROUPE_CATEGORIE\nCATEGORIE\nNB_CADRES", "colonne",
     "0 · regroupement_categorie\n(l. 195)\n7 · base (l. 403 à 405)\n8 · SELECT (l. 499)",
     "LEFT JOIN regroupement_categorie rc ON rc.code = si.categorie\n"
     "NVL(rc.groupe, 'SANS_CATEGORIE')  AS GROUPE_CATEGORIE\n"
     "NVL(rc.cadre, 'NON')              AS cadre\n"
     "COUNT(DISTINCT CASE WHEN b.cadre = 'OUI' THEN b.PERS_ID END) AS NB_CADRES",
     "Catégorie de la situation du 31/12. 01-02 employés, 03 maîtrise, 04-05 cadres moyens, "
     "06 et HC cadres supérieurs ; 04, 05, 06, HC sont des cadres (T65)."),
    ("Âge (T58)", "TRANCHE_AGE_CODE\nAGE_MOYEN", "colonne",
     "7 · base (l. 408 à 411)\n8 · LEFT JOIN\nDIM_GRH_TRANCHE_AGE",
     "CASE WHEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12)\n"
     "          BETWEEN 0 AND 119\n"
     "     THEN FLOOR(MONTHS_BETWEEN(da.date_arrete, p.DATE_NAISS) / 12) END AS age\n"
     "LEFT JOIN DTM.DIM_GRH_TRANCHE_AGE ta ON b.age >= ta.AGE_MIN\n"
     "     AND (b.age <= ta.AGE_MAX OR ta.AGE_MAX IS NULL)",
     "Années pleines au 31/12. Date absente ou fausse : age NULL, tranche vide, mais l'agent "
     "reste compté (LEFT JOIN)."),
    ("Sexe (T58, T59, T61)", "SEXE\nLIBELLE_SEXE", "colonne",
     "8 · SELECT (l. 471 à 476)",
     "CASE b.SEXE\n"
     "    WHEN 'M' THEN 'Masculin'\n"
     "    WHEN 'F' THEN 'Féminin'\n"
     "    ELSE 'NON RENSEIGNE'\n"
     "END AS LIBELLE_SEXE",
     "Lu sur la fiche, comme avant."),
    ("Ancienneté (T61)", "TRANCHE_ANC_CODE\nANCIENNETE_MOYENNE", "colonne",
     "2 · personnes (l. 242 à 245)\n5 · interruptions_a_date\n7 · base (l. 413 à 416)",
     "COALESCE(p.DATE_PRISE_SERVICE, p.DATE_EMBAUCHE) AS debut_anciennete\n"
     "…\n"
     "GREATEST(FLOOR((da.date_arrete - p.debut_anciennete\n"
     "                - NVL(ia.jours, 0)) / 365.25), 0) AS anciennete",
     "Années pleines depuis la prise de service, moins ia.jours : les disponibilités et les "
     "trous entre deux contrats avant le 31/12 (recueil, RH ligne 4)."),
    ("Domaine d'activité (T59)", "DOMAINE_ACTIVITE", "colonne",
     "0 · correspondance_domaine\n(l. 208)\n7 · base (l. 406, 442)",
     "LEFT JOIN correspondance_domaine cdo ON cdo.UA_CODE = TO_CHAR(si.UA_CODE)\n"
     "NVL(cdo.DOMAINE, 'Domaine non renseigné') AS DOMAINE_ACTIVITE",
     "La correspondance est vide pour l'instant (WHERE 1 = 0) : tout tombe en « Domaine non "
     "renseigné » jusqu'au fichier de la CNSS."),
    ("Actifs et inactifs", "NB_AGENTS_ACTIFS\nNB_AGENTS_INACTIFS", "colonne",
     "7 · base, inactif (l. 421)\n8 · SELECT (l. 486 à 488)",
     "COUNT(DISTINCT CASE WHEN b.inactif = 0 THEN b.PERS_ID END) AS NB_AGENTS_ACTIFS\n"
     "COUNT(DISTINCT CASE WHEN b.inactif = 1 THEN b.PERS_ID END) AS NB_AGENTS_INACTIFS",
     "inactif = 1 : en disponibilité au 31/12 (voir la partie 3). Les tableaux CIPRES ne "
     "lisent pas ces colonnes."),
]


def couper(txt, size, largeur, mono=False):
    """Coupe un texte en lignes qui tiennent dans la largeur (mot par mot)."""
    lignes = []
    for para in txt.split("\n"):
        courante = ""
        for mot in para.split(" "):
            essai = f"{courante} {mot}".strip()
            if courante and text_size(essai, size, mono)[0] > largeur:
                lignes.append(courante)
                courante = mot
            else:
                courante = essai
        lignes.append(courante)
    return "\n".join(lignes)


def build():
    d = Diagram(seed=60)

    def bloc(x, y, w, h, txt, size=20, couleurs=(C["blanc"], C["encre"]), sw=2, mono=False):
        g = d.group()
        b = d.box(x, y, w, h, bg=couleurs[0], stroke=couleurs[1], stroke_width=sw, groups=g)
        d.text(x + 22, y + 16, txt, size, groups=g, inside=b, mono=mono)
        return b

    def tableau(lignes, top, colonnes=COLONNES):
        """Un tableau : une ligne d'en-tête, puis une ligne par cas. Renvoie le bas."""
        xs, x = [], 40
        for _, w in colonnes:
            xs.append(x)
            x += w + GAP_COL
        for (titre, w), xc in zip(colonnes, xs):
            d.text(xc + 4, top, titre, 20, C["doux"])
        y = top + 40
        for cas, verdict, genre, ou, sql, pourquoi in lignes:
            fond_verdict, bord, sw, style, fond = GENRES[genre]
            w_cas, w_ver, w_ou, w_sql, w_pq = (w for _, w in colonnes)
            t_cas = couper(cas, 21, w_cas - 40)
            t_pq = couper(pourquoi, 18, w_pq - 40)
            verdict = couper(verdict, 18, w_ver - 40)
            hauteurs = [text_size(t_cas, 21)[1], text_size(verdict, 18)[1] + 20,
                        text_size(ou, 18)[1], text_size(sql, SQL_SIZE, mono=True)[1],
                        text_size(t_pq, 18)[1]]
            h = max(max(hauteurs) + 36, 96)
            g = d.group()
            ligne = d.box(40, y, sum(w for _, w in colonnes) + GAP_COL * (len(colonnes) - 1), h,
                          bg=fond, stroke=bord, stroke_width=sw, style=style, groups=g)
            d.text(xs[0] + 18, y + 18, t_cas, 21, groups=g, inside=ligne)
            d.box(xs[1], y + 14, w_ver, h - 28, verdict, 18, bg=fond_verdict, stroke=bord,
                  style=style, groups=g)
            d.text(xs[2] + 14, y + 18, ou, 18, C["bleu"], groups=g, inside=ligne)
            code = d.box(xs[3], y + 10, w_sql, h - 20, bg=C["gris"], stroke=C["gris_bord"],
                         stroke_width=1, groups=g)
            d.text(xs[3] + 14, y + 18, sql, SQL_SIZE, groups=g, inside=code, mono=True)
            d.text(xs[4] + 14, y + 18, t_pq, 18, C["doux"], groups=g, inside=ligne)
            y += h + 14
        return y - 14

    # ------------------------------------------------------------------ titre et légende
    d.text(0, 0, "Les cas d'agents dans la requête SQL", 48)
    d.text(0, 66, "Version technique de « cas_agents_effectif » : chaque cas relié à la partie "
                  "de la requête qui le traite, au bout de SQL qui décide, et à la raison\n"
                  "analyses/grh/t58_t65_effectif_requete_proposee.sql · numéros de ligne du "
                  "fichier au 07/11/2026 · « … » = coupure · les commentaires « -- » disent ce "
                  "que vaut la condition", 22, C["doux"])
    legende = [("compté", "oui"), ("compté, mais inactif", "inactif"), ("pas compté", "non"),
               ("à surveiller", "attention")]
    for i, (txt, genre) in enumerate(legende):
        fond_verdict, bord, sw, _, _ = GENRES[genre]
        d.box(1900 + i * 280, 8, 260, 44, txt, 18, bg=fond_verdict, stroke=bord, stroke_width=sw)

    # ------------------------------------------------------------------ 1. vue d'ensemble
    s1 = 170
    sec1 = d.section(0, s1, WIDTH, 1200, "1. La requête en un coup d'œil : les parties, "
                                         "leurs CTE (blocs du WITH) et leur grain")
    bloc(40, s1 + 80, 2960, 100,
         "0 · LES RÉGLAGES (l. 160 à 211) : parametres, decisions_depart, codes_decision, "
         "regroupement_categorie, correspondance_domaine.\nDe petites tables écrites dans la "
         "requête (SELECT … FROM DUAL), lues par les autres parties avec CROSS JOIN, IN ou "
         "LEFT JOIN. Changer une valeur ici change le résultat sans réécrire le reste.", 20,
         couleurs=(C["jaune_clair"], C["jaune_bord"]))
    parties = [
        ("1 · LES DATES", "annees  l. 217\ndates   l. 224",
         "1 ligne = 1 date d'arrêté\n(le 31/12 des années closes)\nlit FAIT_GRH_SITUATION"),
        ("2 · LES AGENTS", "personnes  l. 235",
         "1 ligne = 1 agent : etat,\nqualite, date_entree,\ndebut_anciennete\n"
         "lit FAIT_GRH_PERSONNE"),
        ("3 · LES DÉPARTS", "decisions       l. 253\ndeparts         l. 274\n"
                            "premier_depart  l. 286",
         "decisions : les décisions utiles,\nsans celles annulées par une 40\n"
         "premier_depart : 1 ligne = 1 agent\nlit FAIT_GRH_DECISION"),
        ("4 · LES CONTRATS", "engagements               l. 295\n"
                             "contrats                  l. 306\n"
                             "temporaires_sous_contrat  l. 317",
         "contrats : 1 ligne = 1 contrat\n(début, fin)\ntemporaires_sous_contrat :\n"
         "1 ligne = 1 date × 1 agent"),
        ("5 · DISPO, ANCIENNETÉ", "positions             l. 330\n"
                                  "interruptions         l. 343\n"
                                  "interruptions_a_date  l. 367",
         "positions : dernière 07 et\ndernière 18 à chaque date\ninterruptions_a_date :\n"
         "jours à retirer, par date"),
        ("6 · LA SITUATION", "situations  l. 380",
         "1 ligne = 1 date × 1 situation\nrn = 1 : la dernière commencée\n"
         "au plus tard à la date\nlit FAIT_GRH_SITUATION"),
    ]
    w_p, h_p = (2960 - 5 * 28) / 6, 330
    boites = []
    for i, (titre, ctes, grain) in enumerate(parties):
        x = 40 + i * (w_p + 28)
        y = s1 + 215
        g = d.group()
        b = d.box(x, y, w_p, h_p, bg=C["blanc"], stroke=C["encre"], groups=g)
        d.text(x + 18, y + 14, titre, 21, groups=g, inside=b)
        d.text(x + 18, y + 54, ctes, 16, groups=g, inside=b, mono=True)
        h_ctes = text_size(ctes, 16, mono=True)[1]
        d.text(x + 18, y + 54 + h_ctes + 14, grain, 17, C["doux"], groups=g, inside=b)
        boites.append(b)

    y7 = s1 + 215 + h_p + 70
    base_sql = ("FROM dates da\n"
                "CROSS JOIN personnes p\n"
                "CROSS JOIN parametres pr\n"
                "LEFT JOIN situations si               -- partie 6, rn = 1\n"
                "LEFT JOIN premier_depart pd           -- partie 3\n"
                "LEFT JOIN positions po                -- partie 5\n"
                "LEFT JOIN temporaires_sous_contrat tc -- partie 4\n"
                "LEFT JOIN interruptions_a_date ia     -- partie 5\n"
                "LEFT JOIN regroupement_categorie rc   -- partie 0\n"
                "LEFT JOIN correspondance_domaine cdo  -- partie 0\n"
                "WHERE (branche G2) OR (branche G7)    -- partie 2 du schéma")
    g7 = d.group()
    p7 = d.box(40, y7, 2960, 330, bg=C["vert_clair"], stroke=C["vert"], stroke_width=3,
               groups=g7)
    d.text(70, y7 + 18, "7 · base (l. 396 à 459) : toutes les dates × tous les agents, "
                        "puis chaque partie s'y accroche", 22, groups=g7, inside=p7)
    code7 = d.box(70, y7 + 62, 1060, 250, bg=C["blanc"], stroke=C["gris_bord"], stroke_width=1,
                  groups=g7)
    d.text(88, y7 + 72, base_sql, 16, groups=g7, inside=code7, mono=True)
    d.text(1180, y7 + 70,
           "LES ALIAS, À GARDER SOUS LES YEUX\n"
           "da = la date d'arrêté · p = l'agent · pr = les réglages\n"
           "si = sa situation à cette date · pd = son premier départ\n"
           "po = ses 07 et 18 à cette date · tc = son contrat à cette date\n"
           "ia = ses jours d'interruption · rc = sa catégorie · cdo = son domaine\n\n"
           "Chaque LEFT JOIN rapporte au plus une ligne : base garde donc une seule\n"
           "ligne par agent et par date. Sans rien à rapporter, les colonnes valent NULL,\n"
           "et c'est ce NULL qui décide de plusieurs cas (fiche sans date, sans 50, sans\n"
           "situation).", 20, groups=g7, inside=p7)
    for b in boites:
        cx = b["x"] + b["width"] / 2
        d.arrow(cx, b["y"] + b["height"] + 6, cx, y7 - 6, b, p7, stroke_width=2)
    y8 = y7 + 330 + 60
    p8 = bloc(40, y8, 2960, 110,
              "8 · SELECT final (l. 465 à 527) : GROUP BY des axes (date, catégorie, sexe, tranches, "
              "unité, domaine, nature, qualité…), COUNT(DISTINCT PERS_ID) et les autres "
              "mesures\n→ une ligne par date d'arrêté et par combinaison d'axes : la table "
              "DTM_GRH_EFFECTIF, que lit OBIEE (filtres qualité A et nature d'emploi)", 20,
              couleurs=(C["violet"], C["encre"]))
    d.varrow(p7, p8, stroke_width=3)
    sec1["height"] = y8 + 110 + 40 - s1

    # ------------------------------------------------------------------ 2. le WHERE
    s2 = y8 + 110 + 40 + 50
    sec2 = d.section(0, s2, WIDTH, 700, "2. Le cœur : le WHERE de base (l. 443 à 458) décide "
                                        "qui est compté à chaque date")
    where = ("WHERE\n"
             "    -- G2 : permanent présent (ou agent sans situation)\n"
             "    (NVL(si.nature, '-') <> 'TEMPORAIRE'                          -- [1]\n"
             "     AND p.date_entree <= da.date_arrete                          -- [2]\n"
             "     AND (p.etat <> 'INACTIF' OR pd.date_depart > da.date_arrete) -- [3]\n"
             "     AND NOT (pr.disponibilite = 'HORS_EFFECTIF'                  -- [4]\n"
             "              AND NVL(po.date_dispo, DATE '1900-01-01')\n"
             "                  > NVL(po.date_remise, DATE '1900-01-01')))\n"
             " OR\n"
             "    -- G7 : temporaire sous contrat\n"
             "    (si.nature = 'TEMPORAIRE'                                     -- [5]\n"
             "     AND tc.PERS_ID IS NOT NULL                                   -- [6]\n"
             "     AND NOT (pr.fin_contrat_inconnue = 'FICHE'                   -- [7]\n"
             "              AND tc.fin_inconnue = 1\n"
             "              AND p.etat = 'INACTIF'))")
    h_where = text_size(where, 19, mono=True)[1] + 40
    bloc(40, s2 + 80, 1080, h_where, where, 19, couleurs=(C["gris"], C["gris_bord"]), mono=True)
    notes = [
        ("[1] Aiguillage vers les permanents : situation pas TEMPORAIRE, ou pas de situation "
         "(NULL devient '-').", "oui"),
        ("[2] Entré au plus tard à la date. Faux → « embauché après le 31/12 ».", "non"),
        ("[3] Pas encore parti : fiche pas INACTIF, OU premier départ après la date. Fiche "
         "INACTIF sans départ : NULL > date n'est jamais vrai → jamais compté.", "attention"),
        ("[4] Réglage disponibilite. 'PRESENT' (valeur actuelle) : toujours vrai, l'agent en "
         "disponibilité reste. 'HORS_EFFECTIF' : il sort.", "inactif"),
        ("[5] Aiguillage vers les temporaires : situation TEMPORAIRE à cette date.", "oui"),
        ("[6] Une ligne dans temporaires_sous_contrat : la date tombe dans un contrat. Pas de "
         "50, ou contrat fini → NULL → pas compté.", "attention"),
        ("[7] Réglage fin_contrat_inconnue. 'PRESENT' (valeur actuelle) : toujours vrai. "
         "'FICHE' : contrat sans fin + fiche INACTIF → sort.", "inactif"),
    ]
    y_n = s2 + 80
    for txt, genre in notes:
        fond_verdict, bord, sw, style, fond = GENRES[genre]
        t = couper(txt, 20, 1840 - 44)
        h = text_size(t, 20)[1] + 26
        bloc(1160, y_n, 1840, h, t, 20, couleurs=(C["blanc"], bord), sw=sw)
        y_n += h + 12
    bas2 = max(s2 + 80 + h_where, y_n - 12)
    sec2["height"] = bas2 + 40 - s2

    # ------------------------------------------------------------------ 3 à 6 : tableaux
    s = bas2 + 40 + 50
    sections = [
        ("3. Les permanents (règle G2)", PERMANENTS, COLONNES),
        ("4. Les temporaires (règle G7)", TEMPORAIRES, COLONNES),
        ("5. Les cas entre les deux", ENTRE_LES_DEUX, COLONNES),
        ("6. Puis chaque agent compté est rangé : les axes et les mesures", RANGEMENT,
         COLONNES_RANGEMENT),
    ]
    for titre, lignes, colonnes in sections:
        sec = d.section(0, s, WIDTH, 500, titre)
        bas = tableau(lignes, s + 80, colonnes)
        sec["height"] = bas + 40 - s
        s = bas + 40 + 50

    # ------------------------------------------------------------------ 7. comptage
    sec7 = d.section(0, s, WIDTH, 420, "7. Le comptage final (partie 8)")
    final = ("SELECT TO_NUMBER(TO_CHAR(b.date_arrete, 'YYYYMMDD')) AS ID_TEMPS,\n"
             "       b.QUAL_CODE, ta.TRANCHE_AGE_CODE, tan.TRANCHE_ANC_CODE, b.SEXE, …,\n"
             "       COUNT(DISTINCT b.PERS_ID)                          AS NB_AGENTS,\n"
             "       …,\n"
             "       COUNT(DISTINCT CASE WHEN b.cadre = 'OUI'\n"
             "                           THEN b.PERS_ID END)            AS NB_CADRES\n"
             "FROM base b\n"
             "LEFT JOIN DTM.DIM_GRH_UNITE_ADMINISTRATIVE u ON u.UA_CODE = b.UA_CODE\n"
             "LEFT JOIN DTM.DIM_GRH_TRANCHE_AGE ta …\n"
             "LEFT JOIN DTM.DIM_GRH_TRANCHE_ANCIENNETE tan …\n"
             "GROUP BY …les mêmes axes…")
    h_final = text_size(final, 18, mono=True)[1] + 40
    bloc(40, s + 80, 1480, h_final, final, 18, couleurs=(C["gris"], C["gris_bord"]), mono=True)
    bloc(1560, s + 80, 1440, h_final,
         couper("À RETENIR\nbase a une seule ligne par agent présent et par date : chaque agent "
                "compte donc une fois par date d'arrêté, sur la ligne de sa combinaison d'axes. "
                "\nOn peut additionner NB_AGENTS sur tous les axes (catégorie, sexe, unité…), "
                "mais jamais d'une date à l'autre : 2023 + 2024 compterait deux fois les mêmes "
                "agents.\nLes tranches et l'unité sont des LEFT JOIN : un agent sans tranche ou "
                "sans unité reste compté, avec une case vide.\nLire avec : cas_agents_effectif "
                "(la version simple) et requete_effectif_pas_a_pas (trois agents rejoués).",
                20, 1440 - 50), 20, couleurs=(C["jaune_clair"], C["jaune_bord"]))
    sec7["height"] = 80 + h_final + 40
    return d


if __name__ == "__main__":
    main(build, OUTPUT)
