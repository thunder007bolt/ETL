"""
Configuration centralisée des 52 dimensions.

Chaque entrée déclare :
    sql_file    : fichier SQL dans shared/dimensions/sql/
    target      : table cible dans le DWH
    key_cols    : colonnes clés pour le MERGE
"""
from domaines.prestation_recouvrement.dims.transformers import (
    transform_assure,
    transform_sinistre,
)
DIM_CONFIG = [
    {
        "sql_file":  "type_accident.sql",
        "target":    "DTM.DIM_TYPE_ACCIDENT",
        "key_cols":  ["TAT_CODE"],
    },
    # {
    #     "sql_file":     "assure.sql",
    #     "target":       "DTM.DIM_ASSURE",
    #     "key_cols":     ["IND_ID"],
    #     "extra_cols":   {"SOURCE_SYSTEME": "INDIVIDU"},
    #     "transform_fn": transform_assure,
    # },
    # {
    #     "sql_file":  "dossier.sql",
    #     "target":    "DTM.DIM_DOSSIER",
    #     "key_cols":  ["DOS_CODE"],
    #     "seq_cols":  {"ID_DOSSIER": "SEQ_DTM.DIM_DOSSIER"},
    #     # "watermark": "d.DOS_DATE_UPDATE",
    # },
    # {
    #     # ID_DOSSIER  : résolu depuis DWH.DTM.DIM_DOSSIER via fk_lookups
    #     # Prérequis   : DTM.DIM_DOSSIER doit être chargé avant DTM.DIM_SINISTRE.
    #     "sql_file":   "sinistre.sql",
    #     "target":     "DTM.DIM_SINISTRE",
    #     "key_cols":   ["DOS_CODE"],
    #     "seq_cols":  {"ID_SINISTRE": "SEQ_DTM.DIM_SINISTRE"},
    #     "fk_lookups": [
    #         {
    #             "join_col":  "DOS_CODE",
    #             "fk_col":    "ID_DOSSIER",
    #             "ref_table": "DTM.DIM_DOSSIER",
    #         }
    #     ],
    #   "transform_fn": transform_sinistre,
    # },
    ##### Demba doit rendre id_type auto increment
    {
        "sql_file":  "type_ajustement.sql",
        
        "target":    "DTM.DIM_TYPE_AJUSTEMENT",
        "seq_cols":  {"ID_TYPE": "DTM.SEQ_DIM_TYPE_AJUSTEMENT"},
        "key_cols":  ["TAJ_CODE"],
    },
    {
        "sql_file":  "type_anomalie_bnts.sql",
        "target":    "DTM.DIM_TYPE_ANOMALIE_BNTS",
        "key_cols":  ["TAB_CODE"],
    },
    # {
    #     "sql_file":  "type_attestation.sql",
    #     "target":    "DTM.DIM_TYPE_ATTESTATION",
    #     "key_cols":  ["TAB_CODE"],
    # },
    {
        "sql_file":  "type_bordereau.sql",
        "target":    "DTM.DIM_TYPE_BORDEREAU",
        "key_cols":  ["TBO_CODE"],
    },
    {
        "sql_file":  "type_debours.sql",
        "target":    "DTM.DIM_TYPE_DEBOURS",
        "key_cols":  ["CODE_TYPE_DEB"],
    },
    {
        "sql_file":  "type_doigt.sql",
        "target":    "DTM.DIM_TYPE_DOIGT",
        "key_cols":  ["CODE_DOIGT"],
    },
    {
        "sql_file":  "type_dossier.sql",
        "target":    "DTM.DIM_TYPE_DOSSIER",
        "key_cols":  ["TDOS_CODE"],
    },
    {
        "sql_file":  "type_effet.sql",
        "target":    "DTM.DIM_TYPE_EFFET",
        "key_cols":  ["TEP_CODE"],
    },
    {
        "sql_file":  "type_etape.sql",
        "target":    "DTM.DIM_TYPE_ETAPE",
        "key_cols":  ["TEA_CODE"],
    },
    {
        "sql_file":  "type_frequence_paiement.sql",
        "target":    "DTM.DIM_TYPE_FREQUENCE_PAIEMENT",
        "key_cols":  ["TFP_CODE"],
    },
    {
        "sql_file":  "type_mode_paiement.sql",
        "target":    "DTM.DIM_TYPE_MODE_PAIEMENT",
        "key_cols":  ["TMP_CODE"],
    },
    {
        "sql_file":  "type_motif.sql",
        "target":    "DTM.DIM_TYPE_MOTIF",
        "key_cols":  ["CODE_TYPE_MOTIF"],
    },
    {
        "sql_file":  "type_oper_caisse.sql",
        "target":    "DTM.DIM_TYPE_OPER_CAISSE",
        "key_cols":  ["TOP_CODE"],
    },
    {
        "sql_file":  "type_piece.sql",
        "target":    "DTM.DIM_TYPE_PIECE",
        "key_cols":  ["TPJ_CODE"],
    },
    {
        "sql_file":  "type_piece_init_ind_assure.sql",
        "target":    "DTM.DIM_TYPE_PIECE_INIT_IND_ASSURE",
        "key_cols":  ["TPJ_CODE"],
    },
    {
        "sql_file":  "type_piece_init_reception.sql",
        "target":    "DTM.DIM_TYPE_PIECE_INIT_RECEPTION",
        "key_cols":  ["TDOS_CODE", "TPJ_CODE"],
    },
    {
        "sql_file":  "type_pj_correspondance.sql",
        "target":    "DTM.DIM_TYPE_PJ_CORRESPONDANCE",
        "key_cols":  ["TPJ_CODE"],
    },
    {
        "sql_file":  "type_prestation_esp.sql",
        "target":    "DTM.DIM_TYPE_PRESTATION_ESP",
        "key_cols":  ["TPE_CODE"],
    },
    {
        "sql_file":  "type_prestation_nat.sql",
        "target":    "DTM.DIM_TYPE_PRESTATION_NAT",
        "key_cols":  ["TPN_CODE"],
    },
    {
        "sql_file":  "type_prestation_regroupe.sql",
        "target":    "DTM.DIM_TYPE_PRESTATION_REGROUPE",
        "key_cols":  ["TPR_CODE"],
    },
    {
        "sql_file":  "type_prs_correspondance.sql",
        "target":    "DTM.DIM_TYPE_PRS_CORRESPONDANCE",
        "key_cols":  ["CODE_TYPE_PRS_CORRRESP"],
    },
    # {
    #     "sql_file":  "type_travailleur.sql",
    #     "target":    "DTM.DIM_TYPE_TRAVAILLEUR",
    #     "key_cols":  ["TTR_CODE"],
    # },
    # {
    #     "sql_file":  "bordereau_pret.sql",
    #     "target":    "DTM.DIM_BORDEREAU_PREST",
    #     "key_cols":  ["BO_ID"],
    #     "strategy":  "gtt",
    # },
    {
        "sql_file":  "branche.sql",
        "target":    "DTM.DIM_BRANCHE",
        "key_cols":  ["BR_CODE"],
    },
    {
        "sql_file":  "caisse_paiement.sql",
        "target":    "DTM.DIM_CAISSE_PAIEMENT",
        "key_cols":  ["DR_NO", "CAP_ID", "LP_NO"],
        "strategy":  "gtt",
        
    },
    {
        "sql_file":  "caissier.sql",
        "target":    "DTM.DIM_CAISSIER",
        "key_cols":  ["CAI_USERNAME"],
        "strategy":  "gtt",
        
    },
    # {
    #     "sql_file":  "categorie_employeur.sql",
    #     "target":    "DTM.DIM_CATEGORIE_EMPLOYEUR",
    #     "key_cols":  ["EMP_ID","CAT_CODE", "SCAT_CODE", "ACT_CODE"],
    #     "strategy":  "gtt",
        
    # },
    {
        "sql_file":  "categorie_pension.sql",
        "target":    "DTM.DIM_CATEGORIE_DOSSIER_PENSION",
        "key_cols":  ["DOS_CATEGORIE"],
        # "strategy":  "gtt",
        
    },
    {
        "sql_file":  "categorie_prestation_nat.sql",
        "target":    "DTM.DIM_CATEGORIE_PRESTATION_NAT",
        "key_cols":  ["CPN_CODE"],
        # "strategy":  "gtt",
        
    },
    {
        "sql_file":  "compte_bancaire.sql",
        "target":    "DTM.DIM_COMPTE_BANCAIRE",
        "key_cols":  ["IF_NO", "AG_CODE", "COM_NO"],
        "strategy":  "gtt",
        
    },
    {
        "sql_file":  "comptes.sql",
        "target":    "DTM.DIM_COMPTES",
        "key_cols":  ["CP_GES_NUMERO", "CP_DIR_NUMERO", "CP_NUMERO"],
        "strategy":  "gtt",
        
    },
    {
        "sql_file":  "controlleur.sql",
        "target":    "DTM.DIM_CONTROLEUR",
        "key_cols":  ["CON_ID"],
        "strategy":  "gtt",
        
    },
    {
        "sql_file":  "departement.sql",
        "target":    "DTM.DIM_DEPARTEMENT",
        "key_cols":  ["PA_NO", "PR_NO", "DPT_NO"],
        "strategy":  "gtt",
        
    },
    {
        "sql_file":  "direction_regionale.sql",
        "target":    "DTM.DIM_DIRECTION_REGIONALE",
        "key_cols":  ["DR_NO"],
        "strategy":  "gtt",
        
    },
    {
        "sql_file":  "forme_juridique.sql",
        "target":    "DTM.DIM_FORME_JURIDIQUE",
        "key_cols":  ["FJ_CODE"],
        "strategy":  "gtt",
        
    },
    # {
    #     "sql_file":  "institution_financiere.sql",
    #     "target":    "DTM.DIM_INSTITUTION_FINANCIERE",
    #     "key_cols":  ["IF_NO"],
    #     "strategy":  "gtt",
        
    # },
    {
        "sql_file":  "lieu_paiement.sql",
        "target":    "DTM.DIM_LIEU_PAIEMENT",
        "key_cols":  ["LP_NO"],
    },
    {
        "sql_file":  "param_caisse_rep_dec.sql",
        "target":    "DTM.DIM_PARAM_CAISSE_REC_DEP",
        "key_cols":  ["CODE_CAISSE"],
    },
    {
        "sql_file":  "param_cpt_bancaire.sql",
        "target":    "DTM.DIM_PARAM_CPT_BANCAIRE",
        "key_cols":  ["NUM_CPT_BANCAIRE"],
        "strategy":  "gtt",
        
    },
    {
        "sql_file":  "parametre_cotisation.sql",
        "target":    "DTM.DIM_PARAMETRES_COTISATION",
        "key_cols":  ["PARC_JOURNAL"],
    },
    {
        "sql_file":  "pays.sql",
        "target":    "DTM.DIM_PAYS",
        "key_cols":  ["PA_NO"],
    },
    {
        "sql_file":  "periode.sql",
        "target":    "DTM.DIM_PERIODE",
        "key_cols":  ["PER_ID"],
    },
    {
        "sql_file": "agence.sql",
        "target":   "DTM.DIM_AGENCE",
        "key_cols": ["IF_NO","AG_CODE"],
        # facultatif : utiliser "strategy": "gtt" si la table est volumineuse
    },
    {
        "sql_file":  "annee.sql",
        "target":    "DTM.DIM_ANNEE",
        "key_cols":  ["AN_ID"],
    },
    {
        # tableau DIM_PERIODICITE_VERSEMENT basé sur la source PERIODICITE
        # colonnes : CODE_PERIODICITE, LIBELLE_PERIODICITE, NB_MOIS
        "sql_file":  "periodicite_versement.sql",
        "seq_cols":  {"ID_PERIODICITE": "DTM.SEQ_DIM_PERIODICITE_VERSEMENT"},
        "target":    "DTM.DIM_PERIODICITE_VERSEMENT",
        "key_cols":  ["CODE_PERIODICITE"],
    },
    {
        "sql_file":  "province.sql",
        "target":    "DTM.DIM_PROVINCE",
        "key_cols":  ["PR_NO", "PA_NO"],
    },
    {
        "sql_file":  "regime_employeur.sql",
        "target":    "DTM.DIM_REGIME_EMPLOYEUR",
        "key_cols":  ["EMP_REGIME"],
        "strategy":  "gtt",
        
    },
    {
        "sql_file":  "secteur_activite.sql",
        "target":    "DTM.DIM_SECTEUR_ACTIVITE",
        "key_cols":  ["SA_NO"],
    },
    {
        "sql_file":  "secteur_operation.sql",
        "target":    "DTM.DIM_SECTEUR_OPERATION",
        "key_cols":  ["EMP_ID", "SA_NO", "SSA_NO", "SO_DATE_DEBUT"],
        "strategy": "gtt"
    },
    {
        "sql_file":  "semestre.sql",
        "target":    "DTM.DIM_SEMESTRE",
        "key_cols":  ["SEM_ID"],
    },
    {
        "sql_file":  "service_provincial.sql",
        "target":    "DTM.DIM_SERVICE_PROVINCIAL",
        "key_cols":  ["SP_NO"],
    },
    {
        "sql_file":  "region.sql",
        "target":    "DTM.DIM_REGION",
        "key_cols":  ["DR_NO", "SP_NO", "LP_NO"],
        "seq_cols":  {"ID_REGION": "DTM.SEQ_DIM_REGION"},
    },
    {
        "sql_file":  "tranche_effectif.sql",
        "target":    "DTM.DIM_TRANCHE_EFFECTIF",
        "key_cols":  ["TEF_CODE"],
    },
    {
        "sql_file":  "trimestre.sql",
        "target":    "DTM.DIM_TRIMESTRE",
        "key_cols":  ["TRIM_ID"],
    },
]
