"""
Configuration centralisée des dimensions — domaine GRH.

Chaque entrée déclare :
    sql_file : fichier SQL dans domaines/grh/dims/sql/
    target   : table cible dans le DWH (schéma DTM)
    key_cols : colonnes clés pour le MERGE

Champs optionnels :
    seq_cols     : dict {col_cible: nom_sequence} — colonnes alimentées par séquence
    strategy     : "gtt" pour les grandes dimensions (défaut : "merge")
    col_map      : dict {col_source: col_cible} — renommage de colonnes
    extra_cols   : dict {col: valeur_ou_callable} — colonnes non présentes en source
    transform_fn : callable (df: DataFrame) -> DataFrame
"""

DIM_CONFIG = [

    {
        "sql_file": "grh_cat_absence.sql",
        "target":   "DTM.DIM_GRH_CAT_ABSENCE",
        "key_cols": ["CABS_CODE"],
    },
    {
        "sql_file": "grh_cat_article.sql",
        "target":   "DTM.DIM_GRH_CAT_ARTICLE",
        "key_cols": ["CDEC_CODE", "ORDRE"],
    },
    {
        "sql_file": "grh_cat_decision.sql",
        "target":   "DTM.DIM_GRH_CAT_DECISION",
        "key_cols": ["CDEC_CODE"],
    },
    {
        "sql_file": "grh_cat_diplome.sql",
        "target":   "DTM.DIM_GRH_CAT_DIPLOME",
        "key_cols": ["CDIP_NO"],
    },
#    {
#        "sql_file": "grh_cat_formation.sql",
#        "target":   "DTM.DIM_GRH_CAT_FORMATION",
#        "key_cols": ["CAT_FORM_CODE"],
#    },
    {
        "sql_file": "grh_cat_frais.sql",
        "target":   "DTM.DIM_GRH_CAT_FRAIS",
        "key_cols": ["CODE_CFR"],
    },
    {
        "sql_file": "grh_cat_sanction.sql",
        "target":   "DTM.DIM_GRH_CAT_SANCTION",
        "key_cols": ["CSAN_NO"],
    },
    {
        "sql_file": "grh_centre_medical.sql",
        "target":   "DTM.DIM_GRH_CENTRE_MEDICAL",
        "key_cols": ["CMED_CODE"],
    },
    {
        "sql_file": "grh_domaine_activite.sql",
        "target":   "DTM.DIM_GRH_DOMAINE_ACTIVITE",
        "key_cols": ["CODE_DOMAINE"],
    },
    {
        "sql_file": "grh_fonction.sql",
        "target":   "DTM.DIM_GRH_FONCTION",
        "key_cols": ["FNCT_CODE"],
    },
    {
        "sql_file": "grh_formation.sql",
        "target":   "DTM.DIM_GRH_FORMATION",
        "key_cols": ["FORM_NO"],
    },
    {
        "sql_file": "grh_fusion_administrative.sql",
        "target":   "DTM.DIM_GRH_FUSION_ADMINISTRATIVE",
        "key_cols": ["CODE_ADM"],
    },
    {
        "sql_file": "grh_lieu.sql",
        "target":   "DTM.DIM_GRH_LIEU",
        "key_cols": ["LIEU_ID"],
    },
    {
        "sql_file": "grh_nationalite.sql",
        "target":   "DTM.DIM_GRH_NATIONALITE",
        "key_cols": ["NATION_CODE"],
    },
    {
        "sql_file": "grh_nature_mouvement.sql",
        "target":   "DTM.DIM_GRH_NATURE_MOUVEMENT",
        "key_cols": ["CODE_NAT_MVT"],
    },
    {
        "sql_file": "grh_profession.sql",
        "target":   "DTM.DIM_GRH_PROFESSION",
        "key_cols": ["PROF_CODE"],
    },
    {
        "sql_file": "grh_qualification.sql",
        "target":   "DTM.DIM_GRH_QUALIFICATION",
        "key_cols": ["QUAL_CODE"],
    },
    {
        "sql_file": "grh_section.sql",
        "target":   "DTM.DIM_GRH_SECTION",
        "key_cols": ["SECT_CODE"],
    },
    {
        "sql_file": "grh_section_analytique.sql",
        "target":   "DTM.DIM_GRH_SECTION_ANALYTIQUE",
        "key_cols": ["CODE_ANA"],
    },
    {
        "sql_file": "grh_sens_mouvement.sql",
        "target":   "DTM.DIM_GRH_SENS_MOUVEMENT",
        "key_cols": ["CODE_SENS_MVT"],
    },
    {
        "sql_file": "grh_type_encadrement.sql",
        "target":   "DTM.DIM_GRH_TYPE_ENCADREMENT",
        "key_cols": ["CODE_TYPE_ENCADRE"],
    },
    {
        "sql_file": "grh_type_mouvement.sql",
        "target":   "DTM.DIM_GRH_TYPE_MOUVEMENT",
        "key_cols": ["CODE_TYPE_MVT"],
    },
    {
        "sql_file": "grh_unite_admin_nature.sql",
        "target":   "DTM.DIM_GRH_UNITE_ADMIN_NATURE",
        "key_cols": ["UA_NATURE"],
    },
    {
        "sql_file": "grh_unite_administrative.sql",
        "target":   "DTM.DIM_GRH_UNITE_ADMINISTRATIVE",
        "key_cols": ["UA_CODE"],
    },

]
