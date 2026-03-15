-- DTM_PRESTATION V1
-- Sources : DWH.FAIT_DEBOURS          (principal — grain DEB_ID)
--           LEFT JOIN DWH.FAIT_DOSSIER       (LP_NO, IND_ID)
--           LEFT JOIN DTM.DIM_LIEU_PAIEMENT  (SP_NO, DR_NO — hiérarchie)
--           LEFT JOIN DWH.FAIT_EFFET         (IND_ID_BENEF, NET_A_PAYER)
--           LEFT JOIN DWH.FAIT_LIEN          (GROUPE bénéficiaire)
--           LEFT JOIN DWH.FAIT_INDIVIDU      (SEXE, DATE_NAISSANCE, DATE_DECES)
--           LEFT JOIN DWH.FAIT_PRESTATION_ESP (NB_MOIS_COTISES — dédupliqué par MAX)
-- Grain   : ANNEE × MOIS × CODE_BRANCHE × CODE_PRESTATION × TYPE_PREST × GROUPE
--           × DR_NO × SP_NO × LP_NO × SEXE × TRANCHE_AGE
-- GROUPE  : TIT=Titulaire | VEU=Veuf/Veuve | ORP=Orphelin | ASC=Ascendant | NAT=Prestations nature
-- TYPE_PREST : ESP (PM/AJ) | NAT (RB)
-- TRANCHE_AGE : au 31/12/ANNEE (règle CIPRES — tranches 5 ans) — NULL pour NAT
-- NB_DECES : IND_DATE_DECES IS NOT NULL AND EXTRACT(YEAR FROM IND_DATE_DECES) = ANNEE
--            ⚠ source partielle (MAX=2011 au 11/03/2026) — activé pour usage futur
-- Exclus  : DATE_CHARGEMENT (DEFAULT SYSDATE cible)
SELECT
    -- ── TEMPOREL ────────────────────────────────────────────────────
    EXTRACT(YEAR  FROM b.DEB_DATE_EFFET)                            AS ANNEE,
    EXTRACT(MONTH FROM b.DEB_DATE_EFFET)                            AS MOIS,
    CEIL(EXTRACT(MONTH FROM b.DEB_DATE_EFFET) / 3)                  AS TRIMESTRE,
    -- ── BRANCHE ─────────────────────────────────────────────────────
    b.CODE_BRANCHE,
    CASE b.CODE_BRANCHE
        WHEN 'V' THEN 'PVID'
        WHEN 'A' THEN 'AT/MP'
        WHEN 'F' THEN 'Prestations Familiales'
        WHEN 'M' THEN 'Maternite'
        ELSE          'Inconnu'
    END                                                             AS LIBELLE_BRANCHE,
    -- ── CODE / LIBELLÉ PRESTATION ────────────────────────────────────
    NVL(b.TPE_CODE, b.TPN_CODE)                                     AS CODE_PRESTATION,
    CASE NVL(b.TPE_CODE, b.TPN_CODE)
        -- PVID ESP
        WHEN 'PV'    THEN 'Pension Vieillesse'
        WHEN 'PI'    THEN 'Pension Invalidite'
        WHEN 'AV'    THEN 'Allocation Vieillesse'
        WHEN 'PSV'   THEN 'Pension Survivant Veuf/Veuve'
        WHEN 'PSO'   THEN 'Pension Survivant Orphelin'
        WHEN 'PSA'   THEN 'Pension Survivant Ascendant'
        -- AT/MP ESP
        WHEN 'IJ'    THEN 'Indemnite Journaliere AT'
        WHEN 'AI'    THEN 'Allocation Incapacite'
        WHEN 'RV'    THEN 'Rente Viagere'
        WHEN 'RSV'   THEN 'Rente Survivant Veuf/Veuve'
        WHEN 'RSO'   THEN 'Rente Survivant Orphelin'
        WHEN 'RSA'   THEN 'Rente Survivant Ascendant'
        -- PF / Maternité ESP
        WHEN 'FAF'   THEN 'Allocation Familiale'
        WHEN 'FAP'   THEN 'Allocation Prenatale'
        WHEN 'IJM'   THEN 'Indemnite Journaliere Maternite'
        -- PVID NAT
        WHEN 'RMG-V' THEN 'Remb. Frais Medicaux Generaux PVID'
        WHEN 'RHO-V' THEN 'Remb. Frais Hospitalisation PVID'
        WHEN 'RPH-V' THEN 'Remb. Frais Pharmacie PVID'
        WHEN 'RAP-V' THEN 'Remb. Frais Appareillage PVID'
        -- AT/MP NAT
        WHEN 'RMG-A' THEN 'Remb. Frais Medicaux Generaux AT/MP'
        WHEN 'RHO-A' THEN 'Remb. Frais Hospitalisation AT/MP'
        WHEN 'RPH-A' THEN 'Remb. Frais Pharmacie AT/MP'
        WHEN 'RAP-A' THEN 'Remb. Frais Appareillage AT/MP'
        WHEN 'RRE-A' THEN 'Remb. Frais Readaptation AT/MP'
        -- PF NAT
        WHEN 'RMA-F' THEN 'Remb. Frais Soins Maternite'
        WHEN 'RHO-F' THEN 'Remb. Hospitalisation Maternite'
        -- Maternité NAT
        WHEN 'RMA-M' THEN 'Remb. Frais Accouchement'
        WHEN 'RPH-M' THEN 'Remb. Frais Pharmacie Maternite'
        ELSE               NVL(b.TPE_CODE, b.TPN_CODE)
    END                                                             AS LIBELLE_PRESTATION,
    -- ── TYPE PRESTATION ─────────────────────────────────────────────
    CASE b.DEB_TYPE WHEN 'RB' THEN 'NAT' ELSE 'ESP' END            AS TYPE_PREST,
    -- ── GROUPE BÉNÉFICIAIRE ──────────────────────────────────────────
    b.GROUPE,
    b.LIBELLE_GROUPE,
    -- ── GÉOGRAPHIE ──────────────────────────────────────────────────
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    -- ── DÉMOGRAPHIE ─────────────────────────────────────────────────
    CASE WHEN b.DEB_TYPE = 'RB' THEN NULL ELSE b.IND_SEXE END      AS SEXE,
    CASE
        WHEN b.DEB_TYPE = 'RB' THEN NULL
        WHEN b.IND_SEXE = 1    THEN 'Masculin'
        WHEN b.IND_SEXE = 2    THEN 'Feminin'
        ELSE                        NULL
    END                                                             AS LIBELLE_SEXE,
    -- Tranche âge au 31/12/ANNEE (règle CIPRES — tranches 5 ans) — NULL pour NAT
    CASE
        WHEN b.DEB_TYPE = 'RB'            THEN NULL
        WHEN b.IND_DATE_NAISSANCE IS NULL THEN NULL
        ELSE CASE
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 20 THEN '<20'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 25 THEN '20-24'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 30 THEN '25-29'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 35 THEN '30-34'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 40 THEN '35-39'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 45 THEN '40-44'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 50 THEN '45-49'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 55 THEN '50-54'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 60 THEN '55-59'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 65 THEN '60-64'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 70 THEN '65-69'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 75 THEN '70-74'
            WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 80 THEN '75-79'
            ELSE '80+'
        END
    END                                                             AS TRANCHE_AGE,
    -- ── MESURES ─────────────────────────────────────────────────────
    COUNT(b.DEB_ID)                                                 AS NB_PRESTATIONS,
    -- NVL2(x, x, NULL) ≡ CASE WHEN x IS NOT NULL THEN x END — filtre les NULL sans COUNT(DISTINCT CASE)
    COUNT(DISTINCT NVL2(b.IND_ID_BENEF, b.IND_ID_BENEF, NULL))     AS NB_BENEFICIAIRES,
    SUM(b.DEB_MONTANT)                                              AS MONTANT_TOTAL,
    SUM(b.NET_A_PAYER)                                              AS MONTANT_NET,
    SUM(CASE WHEN b.DEB_VERIFIE = 'O' THEN 1 ELSE 0 END)           AS NB_CONTROLES_POST,
    SUM(b.PE_MOIS_COTISATION)                                       AS NB_MOIS_COTISES,
    SUM(b.FLAG_DECES)                                               AS NB_DECES,
    -- ── CLICHE ──────────────────────────────────────────────────────
    b.CLICHE                                                        AS CLICHE
FROM (
    -- ── Vue inline base ──────────────────────────────────────────────
    -- FAIT_DOSSIER unique par DOS_CODE (pas de produit cartésien)
    -- FAIT_PRESTATION_ESP dédupliqué par MAX (plusieurs lignes par DOS+TPE)
    -- FAIT_LIEN filtré LN_ACTIF='O' et MIN(LN_TYPE) pour dédupliqué
    SELECT
        deb.DEB_ID,
        deb.DEB_MONTANT,
        deb.DEB_VERIFIE,
        deb.DEB_DATE_EFFET,
        deb.DEB_TYPE,
        deb.CODE_BRANCHE,
        deb.TPE_CODE,
        deb.TPN_CODE,
        deb.CLICHE,
        dos.IND_ID,
        dos.LP_NO,
        NVL(lp.DR_NO, deb.DR_NO)                                   AS DR_NO,
        lp.SP_NO,
        efp.IND_ID_BENEF,
        efp.NET_A_PAYER,
        ind.IND_SEXE,
        ind.IND_DATE_NAISSANCE,
        pe.PE_MOIS_COTISATION,
        CASE
            WHEN ind.IND_DATE_DECES IS NOT NULL
             AND EXTRACT(YEAR FROM ind.IND_DATE_DECES)
                 = EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)
            THEN 1 ELSE 0
        END                                                         AS FLAG_DECES,
        -- GROUPE bénéficiaire
        CASE
            WHEN deb.DEB_TYPE = 'RB'                                THEN 'NAT'
            WHEN dos.IND_ID IS NOT NULL
             AND efp.IND_ID_BENEF = dos.IND_ID                      THEN 'TIT'
            ELSE NVL(CASE lg.LN_TYPE
                         WHEN 'C' THEN 'VEU'
                         WHEN 'E' THEN 'ORP'
                         WHEN 'A' THEN 'ASC'
                         ELSE          'TIT'
                     END, 'TIT')
        END                                                         AS GROUPE,
        CASE
            WHEN deb.DEB_TYPE = 'RB'                                THEN 'Prestation en nature'
            WHEN dos.IND_ID IS NOT NULL
             AND efp.IND_ID_BENEF = dos.IND_ID                      THEN 'Titulaire'
            ELSE NVL(CASE lg.LN_TYPE
                         WHEN 'C' THEN 'Veuf / Veuve'
                         WHEN 'E' THEN 'Orphelin'
                         WHEN 'A' THEN 'Ascendant'
                         ELSE          'Titulaire'
                     END, 'Titulaire')
        END                                                         AS LIBELLE_GROUPE
    FROM      DWH.FAIT_DEBOURS              deb
    LEFT JOIN DWH.FAIT_DOSSIER              dos ON dos.DOS_CODE = deb.DOS_CODE
    LEFT JOIN DTM.DIM_LIEU_PAIEMENT         lp  ON lp.LP_NO    = dos.LP_NO
    LEFT JOIN DWH.FAIT_EFFET                efp ON efp.EFP_ID   = deb.EFP_ID
    LEFT JOIN DWH.FAIT_INDIVIDU             ind ON ind.IND_ID   = efp.IND_ID_BENEF
    LEFT JOIN (
        SELECT DOS_CODE, TPE_CODE,
               MAX(PE_MOIS_COTISATION) AS PE_MOIS_COTISATION
        FROM   DWH.FAIT_PRESTATION_ESP
        GROUP BY DOS_CODE, TPE_CODE
    )                                       pe  ON pe.DOS_CODE  = deb.DOS_CODE
                                               AND pe.TPE_CODE  = deb.TPE_CODE
    LEFT JOIN (
        SELECT IND_ID_1, IND_ID_2, MIN(LN_TYPE) AS LN_TYPE
        FROM   DWH.FAIT_LIEN
        WHERE  LN_ACTIF = 'O'
        GROUP BY IND_ID_1, IND_ID_2
    )                                       lg  ON lg.IND_ID_1  = dos.IND_ID
                                               AND lg.IND_ID_2  = efp.IND_ID_BENEF
    WHERE deb.CLICHE = :1
) b
GROUP BY
    EXTRACT(YEAR  FROM b.DEB_DATE_EFFET),
    EXTRACT(MONTH FROM b.DEB_DATE_EFFET),
    CEIL(EXTRACT(MONTH FROM b.DEB_DATE_EFFET) / 3),
    b.CODE_BRANCHE,
    CASE b.CODE_BRANCHE
        WHEN 'V' THEN 'PVID' WHEN 'A' THEN 'AT/MP'
        WHEN 'F' THEN 'Prestations Familiales'
        WHEN 'M' THEN 'Maternite' ELSE 'Inconnu'
    END,
    NVL(b.TPE_CODE, b.TPN_CODE),
    CASE NVL(b.TPE_CODE, b.TPN_CODE)
        WHEN 'PV'    THEN 'Pension Vieillesse'
        WHEN 'PI'    THEN 'Pension Invalidite'
        WHEN 'AV'    THEN 'Allocation Vieillesse'
        WHEN 'PSV'   THEN 'Pension Survivant Veuf/Veuve'
        WHEN 'PSO'   THEN 'Pension Survivant Orphelin'
        WHEN 'PSA'   THEN 'Pension Survivant Ascendant'
        WHEN 'IJ'    THEN 'Indemnite Journaliere AT'
        WHEN 'AI'    THEN 'Allocation Incapacite'
        WHEN 'RV'    THEN 'Rente Viagere'
        WHEN 'RSV'   THEN 'Rente Survivant Veuf/Veuve'
        WHEN 'RSO'   THEN 'Rente Survivant Orphelin'
        WHEN 'RSA'   THEN 'Rente Survivant Ascendant'
        WHEN 'FAF'   THEN 'Allocation Familiale'
        WHEN 'FAP'   THEN 'Allocation Prenatale'
        WHEN 'IJM'   THEN 'Indemnite Journaliere Maternite'
        WHEN 'RMG-V' THEN 'Remb. Frais Medicaux Generaux PVID'
        WHEN 'RHO-V' THEN 'Remb. Frais Hospitalisation PVID'
        WHEN 'RPH-V' THEN 'Remb. Frais Pharmacie PVID'
        WHEN 'RAP-V' THEN 'Remb. Frais Appareillage PVID'
        WHEN 'RMG-A' THEN 'Remb. Frais Medicaux Generaux AT/MP'
        WHEN 'RHO-A' THEN 'Remb. Frais Hospitalisation AT/MP'
        WHEN 'RPH-A' THEN 'Remb. Frais Pharmacie AT/MP'
        WHEN 'RAP-A' THEN 'Remb. Frais Appareillage AT/MP'
        WHEN 'RRE-A' THEN 'Remb. Frais Readaptation AT/MP'
        WHEN 'RMA-F' THEN 'Remb. Frais Soins Maternite'
        WHEN 'RHO-F' THEN 'Remb. Hospitalisation Maternite'
        WHEN 'RMA-M' THEN 'Remb. Frais Accouchement'
        WHEN 'RPH-M' THEN 'Remb. Frais Pharmacie Maternite'
        ELSE               NVL(b.TPE_CODE, b.TPN_CODE)
    END,
    CASE b.DEB_TYPE WHEN 'RB' THEN 'NAT' ELSE 'ESP' END,
    b.GROUPE,
    b.LIBELLE_GROUPE,
    b.DR_NO,
    b.SP_NO,
    b.LP_NO,
    CASE WHEN b.DEB_TYPE = 'RB' THEN NULL ELSE b.IND_SEXE END,
    CASE WHEN b.DEB_TYPE = 'RB' THEN NULL
         WHEN b.IND_SEXE = 1    THEN 'Masculin'
         WHEN b.IND_SEXE = 2    THEN 'Feminin'
         ELSE                        NULL END,
    CASE WHEN b.DEB_TYPE = 'RB' THEN NULL
         WHEN b.IND_DATE_NAISSANCE IS NULL THEN NULL
         ELSE CASE
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 20 THEN '<20'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 25 THEN '20-24'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 30 THEN '25-29'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 35 THEN '30-34'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 40 THEN '35-39'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 45 THEN '40-44'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 50 THEN '45-49'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 55 THEN '50-54'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 60 THEN '55-59'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 65 THEN '60-64'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 70 THEN '65-69'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 75 THEN '70-74'
             WHEN TRUNC(MONTHS_BETWEEN(ADD_MONTHS(TRUNC(b.DEB_DATE_EFFET,'YYYY'),12)-1,b.IND_DATE_NAISSANCE)/12) < 80 THEN '75-79'
             ELSE '80+'
         END
    END,
    b.CLICHE
