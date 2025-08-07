import streamlit as st
import pandas as pd
from sqlalchemy import create_engine,text
from urllib.parse import quote_plus


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
        "id_his", "type_fonction", "id_client", "login", "date", "id_his_extr"
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
        "id", "id_doss", "date_action", "etat", "cre_mnt_deb"
    ]
}

unique_columns = {
    "ad_cli": ["id_client", "id_cpte_base"],  # client ID and base account unique
    "ad_cpt": ["id_cpte"],                    # account ID unique
    "ad_his": ["id_his"],                     # transaction history ID unique
    "ad_ecriture": ["id_ecriture"],           # accounting entry ID unique
    "ad_mouvement": ["id_mouvement"],         # movement ID unique
    "ad_dcr": ["id_doss"],                     # loan dossier ID unique
    "ad_etr": ["id_ech"],                      # installment ID unique
    "ad_sre": ["id_ech"],                      # installment ID unique (payment)
    "ad_calc_int_recevoir_his": ["id"],       # interest received history ID unique
    "ad_provision": ["id_provision"],          # provision ID unique
    "ml_demande_credit": ["id_transaction"],  # transaction ID unique
    "ad_abonnement": ["id_abonnement"],        # subscription ID unique
    "ad_calc_int_paye_his": ["id"],            # interest payment history ID unique
    "ad_cpt_comptable": ["num_cpte_comptable"],# accounting account number unique
    "adsys_produit_credit": ["id"],             # credit product ID unique
    "ad_gar": ["id_gar"],                        # guarantee ID unique
    "ad_gui": ["id_gui"],                        # guichet ID unique
    "adsys_detail_objet": ["id"],                # detail objet ID unique
    "adsys_objets_credits": ["id"],              # objet credits ID unique
    "ad_dcr_hist": ["id"],                        # loan credit history ID unique
}


def calculate_completeness(schema, table, columns):
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password_encoded}@{host}:{port}/{mfi_db}",
        connect_args={"options": f"-csearch_path={schema}"}
    )
    with engine.connect() as conn:
        total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        results = []
        for col in columns:
            non_null_count = conn.execute(text(f"SELECT COUNT({col}) FROM {table} WHERE {col} IS NOT NULL")).scalar()
            completeness_pct = (non_null_count / total_rows * 100) if total_rows > 0 else 0
            results.append({
                "table": table,
                "column": col,
                "completeness_%": round(completeness_pct, 2)
            })
        return pd.DataFrame(results)

st.title("AD Finance Data Completeness Dashboard")

selected_mfi = st.selectbox("Select MFI Schema", mfis_schemas)
selected_table = st.selectbox("Select Table", list(table_columns.keys()))

if st.button("Calculate Completeness"):
    with st.spinner("Calculating completeness..."):
        df = calculate_completeness(selected_mfi, selected_table, table_columns[selected_table])
    st.success(f"Completeness calculated for {selected_table} in {selected_mfi}")
    st.dataframe(df)

    csv = df.to_csv(index=False)
    st.download_button(label="Download CSV", data=csv, file_name=f"{selected_mfi}_{selected_table}_completeness.csv", mime='text/csv')
