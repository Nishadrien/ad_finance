import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy import text

# DB connection info
host = "35.177.7.188"
port = 5432
user = "postgres"
password = "CLecaM@%Ejo213"
password_encoded = quote_plus(password)  # Encode special chars in password
mfi_db = "mfis"  # database name for MFI schemas

# List of MFI schemas
mfis_schemas = [
    "mfi_402", "mfi_404", "mfi_406", "mfi_413", "mfi_416",
    "mfi_419", "mfi_421", "mfi_924", "mfi_925", "mfi_926",
    "mfi_934", "mfi_945", "mfi_947", "mfi_956", "mfi_958",
    "mfi_959", "mfi_962", "mfi_963", "mfi_977"
]

# Mapping of mfi_xxx to their original databases
mfi_mapping = {
    "mfi_402": ["402_Ville", "402_Siege", "402_Rusizi", "402_Bugesera", "402_Gicumbi", "402_Kicukiro", "402_Nyabugogo", "402_Rubavu"],
    "mfi_404": ["404_nyamata", "404_nyakabuye", "404_nyabugogo", "404_mwezi", "404_masoro", "404_kiyanza"],
    "mfi_406": ["406_siege", "406_rusizi", "406_rukomo", "406_ruhango", "406_rubavu", "406_nyanza", "406_nyagatare",
                "406_nyabugogo", "406_musanze", "406_muhanga", "406_matimba", "406_kimironko", "406_kayonza",
                "406_kanogo", "406_huye", "406_gisozi", "406_gatsibo", "406_gahini"],
    "mfi_413": ["413_rubavu", "413_nyabugogo", "413_kicukiro"],
    "mfi_416": ["416_siege", "416_ruyumba", "416_ruhango", "416_nyabugogo", "416_ntenyo", "416_ndiza", "416_mushishiro",
                "416_muhanga", "416_kayenzi", "416_kamonyi", "416_kabagali", "416_cyabakamyi"],
    "mfi_419": ["419_siege", "419_rutsiro", "419_rusizi", "419_rubengera", "419_nyamasheke", "419_nyabugogo",
                "419_mahoko", "419_bwishyura"],
    "mfi_421": ["421_jali"],
    "mfi_924": ["924_Ubaka"],
    "mfi_925": ["925_zamuka"],
    "mfi_926": ["926_CIC"],
    "mfi_934": ["934_ikirenga"],
    "mfi_945": ["945_comicoka"],
    "mfi_947": ["947_shagasha"],
    "mfi_956": ["956_impamba"],
    "mfi_958": ["958_twizigamire"],
    "mfi_959": ["959_GTF"],
    "mfi_962": ["962_INGASHYA"],
    "mfi_963": ["963_Gisakura"],
    "mfi_977": ["977_IKAPAGI"]
}

# Tables and columns for completeness checking
table_columns = {
    "ad_cli": [
        "id_client", "date_adh", "pp_sexe", "pp_date_naissance",
        "education", "id_cpte_base", "pp_etat_civil", "num_tel", "email",
        "pp_nationalite", "id_loc1", "province", "district", "secteur",
        "cellule", "village", "pp_revenu", "pp_pm_activite_prof",
        "langue_correspondance", "etat"
    ],
    "ad_cpt": [
        "id_cpte", "id_titulaire", "date_ouvert", "etat_cpte", "solde",
        "mode_calcul_int_cpte", "interet_annuel", "devise", "mnt_bloq"
    ],
    "ad_his": [
        "id_his", "type_fonction", "id_client", "login", "date", "id_his_ext"
    ],
    "ad_ecriture": [
        "id_ecriture", "id_his", "date_comptable", "type_operation", "ref_ecriture"
    ],
    "ad_mouvement": [
        "id_mouvement", "id_ecriture", "compte", "sens", "montant", "devise", "date_valeur"
    ],
    "ad_dcr": [
        "id_doss", "id_client", "id_prod", "date_dem", "etat",
        "id_agent_gest", "cre_etat", "cre_date_debloc", "cre_mnt_deb",
        "obj_dem", "gar_tot"
    ],
    "ad_etr": [
        "id_doss", "id_ech", "date_ech", "mnt_cap", "mnt_int"
    ],
    "ad_sre": [
        "id_doss", "id_ech", "date_remb", "mnt_remb_cap", "mnt_remb_int"
    ],
    "ad_calc_int_recevoir_his": [
         "id", "id_doss", "id_ech", "date_traitement", "nb_jours",
         "montant", "etat_int", "cre_etat", "devise"
    ],
    "ad_provision": [
        "id_provision", "id_doss", "montant", "taux", "date_prov",
        "id_his", "is_repris"
    ],
    "ml_demande_credit": [
        "id_client", "id_doss", "id_transaction", "statut_demande", "date_creation"
    ],
    "ad_abonnement": [
        "id_abonnement", "id_client", "identifiant", "num_sms",
        "langue", "is_active"
    ],
    "ad_calc_int_paye_his": [
        "id", "id_cpte", "id_titulaire", "id_prod", "montant_int",
        "devise", "date_calc", "etat_calc_int"
    ],
    "ad_cpt_comptable": [
        "num_cpte_comptable", "libel_cpte_comptable", "sens_cpte",
        "classe_compta", "etat_cpte", "solde", "devise"
    ],
    "adsys_produit_credit": [
        "id", "libel", "tx_interet", "mnt_min", "mnt_max",
        "duree_min_mois", "duree_max_mois", "devise", "is_produit_actif", "date_creation"
    ],
    "ad_gar": [
        "id_gar", "id_doss", "type_gar", "etat_gar", "montant_vente", "devise_vente"
    ],
    "ad_gui": [
        "id_gui", "libel_gui", "date_crea", "ouvert", "cpte_cpta_gui"
    ],
    "adsys_detail_objet": [
        "id", "libel", "code", "id_obj"
    ],
    "adsys_objets_credits": [
        "id", "libel", "code"
    ],
    "ad_dcr_hist": [
        "id", "id_doss", "date_action", "etat", "cre_mnt_deb", "id_client"
    ]
}

# Columns to check for uniqueness (using original column names)
unique_columns = {
    "ad_cli": ["id_client"],
    "ad_cpt": ["id_cpte"],
    "ad_his": ["id_his"],
    "ad_ecriture": ["id_ecriture"],
    "ad_mouvement": ["id_mouvement"],
    "ad_dcr": ["id_doss"],
    "ad_etr": ["id_ech"],
    "ad_sre": ["id_ech"],
    "ad_calc_int_recevoir_his": ["id_doss"],
    "ad_provision": ["id_provision"],
    "ml_demande_credit": ["id_transaction"],
    "ad_abonnement": ["id_abonnement"],
    "ad_calc_int_paye_his": ["id"],
    "ad_cpt_comptable": ["num_cpte_comptable"],
    "adsys_produit_credit": ["id"],
    "ad_gar": ["id_gar"],
    "ad_gui": ["id_gui"],
    "adsys_detail_objet": ["id"],
    "adsys_objets_credits": ["id"],
    "ad_dcr_hist": ["id"],
}

def calculate_metrics(schema, table, columns, source_db=None):
    # Determine database: use source_db if provided, else use mfi_db with schema
    db_name = source_db if source_db else mfi_db
    connect_args = {"options": f"-csearch_path={schema}"} if not source_db else {}
    
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{db_name}",
        connect_args=connect_args
    )
    with engine.connect() as conn:
        # Add condition for ad_cli to filter by statut_juridique = 1
        if table == "ad_cli":
            total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE statut_juridique = 1")).scalar()
        else:
            total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        
        results = []
        # Get unique columns for the current table (if any)
        table_unique_columns = unique_columns.get(table, [])
        
        for col in columns:
            # Completeness calculation
            if table == "ad_cli":
                non_null_count = conn.execute(
                    text(f"SELECT COUNT({col}) FROM {table} WHERE {col} IS NOT NULL AND statut_juridique = 1")
                ).scalar()
            else:
                non_null_count = conn.execute(
                    text(f"SELECT COUNT({col}) FROM {table} WHERE {col} IS NOT NULL")
                ).scalar()
            completeness_pct = (non_null_count / total_rows * 100) if total_rows > 0 else 0
            
            # Integrity calculation
            integrity_pct = "N/A"
            if col in table_unique_columns:
                # Use _unique suffix for MFI schema, original column for source
                integrity_col = f"{col}_unique" if source_db is None else col
                if table == "ad_cli":
                    distinct_count = conn.execute(
                        text(f"SELECT COUNT(DISTINCT {integrity_col}) FROM {table} WHERE {integrity_col} IS NOT NULL AND statut_juridique = 1")
                    ).scalar()
                else:
                    distinct_count = conn.execute(
                        text(f"SELECT COUNT(DISTINCT {integrity_col}) FROM {table} WHERE {integrity_col} IS NOT NULL")
                    ).scalar()
                integrity_pct = (distinct_count / non_null_count * 100) if non_null_count > 0 else 0
                integrity_pct = round(integrity_pct, 2)
            
            results.append({
                "table": table,
                "column": col,  # Use original column name
                "completeness_%": round(completeness_pct, 2),
                "integrity_%": integrity_pct
            })
        return pd.DataFrame(results)

# Styling function for conditional cell background coloring based on completeness_%
def style_completeness_cell(row):
    # Initialize empty styles for all columns
    styles = [''] * len(row)
    # Index of completeness_% column
    completeness_idx = row.index.get_loc('completeness_%')
    # Apply background color only to completeness_% cell
    if 80 <= row['completeness_%'] < 90:
        styles[completeness_idx] = 'background-color: orange'
    elif row['completeness_%'] < 80:
        styles[completeness_idx] = 'background-color: red'
    return styles

st.title("MFIS Data Quality Assessment")

# MFI schema selection
selected_mfi = st.selectbox("Select MFI Schema", mfis_schemas)

# Source selection (branches for the selected MFI)
source_options = ["None"] + mfi_mapping.get(selected_mfi, [])
selected_source = st.selectbox("Select Source (Branch)", source_options)

# Table selection
selected_table = st.selectbox("Select Table", list(table_columns.keys()))

if st.button("Submit"):
    with st.spinner("Calculating completeness and integrity..."):
        # Pass source_db if a source is selected, else None for MFI schema
        source_db = selected_source if selected_source != "None" else None
        df = calculate_metrics(selected_mfi, selected_table, table_columns[selected_table], source_db)
    
    # Update success message to reflect MFI or source
    level = f"{selected_source} (Source)" if source_db else f"{selected_mfi} (MFI Schema)"
    st.success(f"Metrics calculated for {selected_table} in {level}")
    
    # Apply styling to the completeness_% column and format completeness_% and integrity_% to 2 decimal places
    styled_df = df.style.apply(style_completeness_cell, axis=1).format(
        {'completeness_%': '{:.2f}', 'integrity_%': lambda x: '{:.2f}'.format(x) if isinstance(x, (int, float)) else x}
    )
    st.dataframe(styled_df)

    # Update CSV filename to include source if selected
    filename = f"{selected_source}_{selected_table}_metrics.csv" if source_db else f"{selected_mfi}_{selected_table}_metrics.csv"
    csv = df.to_csv(index=False)
    st.download_button(label="Download CSV", data=csv, file_name=filename, mime='text/csv')
