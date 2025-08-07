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
mfi_db = "mfis"  # database name

# List of MFI schemas
mfis_schemas = [
    "mfi_402", "mfi_404", "mfi_406", "mfi_413", "mfi_416",
    "mfi_419", "mfi_421", "mfi_924", "mfi_925", "mfi_926",
    "mfi_934", "mfi_945", "mfi_947", "mfi_956", "mfi_958",
    "mfi_959", "mfi_962", "mfi_963", "mfi_977"
]

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
        "id", "id_doss", "date_action", "etat", "cre_mnt_deb","id_client"
    ]
}

# Columns to check for uniqueness (mapped to original column names)
unique_columns = {
    "ad_cli": ["id_client_unique"],
    "ad_cpt": ["id_cpte_unique"],
    "ad_his": ["id_his_unique"],
    "ad_ecriture": ["id_ecriture_unique"],
    "ad_mouvement": ["id_mouvement_unique"],
    "ad_dcr": ["id_doss_unique"],
    "ad_etr": ["id_ech_unique"],
    "ad_sre": ["id_ech_unique"],
    "ad_calc_int_recevoir_his": ["id_doss_unique"],
    "ad_provision": ["id_provision_unique"],
    "ml_demande_credit": ["id_transaction_unique"],
    "ad_abonnement": ["id_abonnement_unique"],
    "ad_calc_int_paye_his": ["id_unique"],
    "ad_cpt_comptable": ["num_cpte_comptable_unique"],
    "adsys_produit_credit": ["id_unique"],
    "ad_gar": ["id_gar_unique"],
    "ad_gui": ["id_gui_unique"],
    "adsys_detail_objet": ["id_unique"],
    "adsys_objets_credits": ["id_unique"],
    "ad_dcr_hist": ["id_unique"],
}

def calculate_metrics(schema, table, columns):
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{mfi_db}",
        connect_args={"options": f"-csearch_path={schema}"}
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
        # Create a mapping from unique column names to original column names
        unique_to_original = {col + "_unique": col for col in columns if col + "_unique" in table_unique_columns}
        
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
            unique_col = col + "_unique"
            if unique_col in table_unique_columns:
                if table == "ad_cli":
                    distinct_count = conn.execute(
                        text(f"SELECT COUNT(DISTINCT {unique_col}) FROM {table} WHERE {unique_col} IS NOT NULL AND statut_juridique = 1")
                    ).scalar()
                else:
                    distinct_count = conn.execute(
                        text(f"SELECT COUNT(DISTINCT {unique_col}) FROM {table} WHERE {unique_col} IS NOT NULL")
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

# Styling function for conditional row background coloring based on completeness_%
def style_completeness_row(row):
    if 80 <= row['completeness_%'] < 90:
        return [f'background-color: orange' for _ in row]
    elif row['completeness_%'] < 80:
        return [f'background-color: red' for _ in row]
    else:
        return ['' for _ in row]  # No background color for values >= 90

st.title("MFIS Data Data Quality Assessment")

selected_mfi = st.selectbox("Select MFI Schema", mfis_schemas)
selected_table = st.selectbox("Select Table", list(table_columns.keys()))

if st.button("Submit"):
    with st.spinner("Calculating completeness and integrity..."):
        df = calculate_metrics(selected_mfi, selected_table, table_columns[selected_table])
    st.success(f"Metrics calculated for {selected_table} in {selected_mfi}")
    
    # Apply styling to the entire row and format completeness_% and integrity_% to 2 decimal places
    styled_df = df.style.apply(style_completeness_row, axis=1).format(
        {'completeness_%': '{:.2f}', 'integrity_%': lambda x: '{:.2f}'.format(x) if isinstance(x, (int, float)) else x}
    )
    st.dataframe(styled_df)

    csv = df.to_csv(index=False)
    st.download_button(label="Download CSV", data=csv, file_name=f"{selected_mfi}_{selected_table}_metrics.csv", mime='text/csv')
