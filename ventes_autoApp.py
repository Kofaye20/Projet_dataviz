from typing import List, Tuple
import webbrowser
import pymongo
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import streamlit as st


def set_page_config():
    st.set_page_config(
        page_title="Sales Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown("<style> footer {visibility: hidden;} </style>", unsafe_allow_html=True)

@st.cache_resource
def connexion_db(db_name, collection_name, cluster_uri):
    try:
        client = pymongo.MongoClient(cluster_uri)
        db = client[db_name]
        collection = db[collection_name]
        message = "Connexion réussie à la base de données."
        return collection, message
    except Exception as e:
        message = f"Erreur lors de la connexion à la base de données : {str(e)}"
        return None, message

def cursor_to_dataframe(cursor):
    df = pd.DataFrame(list(cursor))  
    return df

def afficher_apercu_df(df):
    widget_key = f"rows_to_show_{id(df)}"
    num_rows = st.slider("Nombre de lignes à afficher :", 1, df.shape[0], 5, key=widget_key)
    st.write(df.head(num_rows))

if 'clicked' not in st.session_state:
    st.session_state.clicked = False

def inspecter_dataframe_button(df):
    if st.sidebar.button("Inspecter le DataFrame", key="inspect_button"):
        st.session_state.clicked = True
        st.success("Inspection effectuée !")
    if st.session_state.clicked:
        with st.sidebar.expander("Détails de l'inspection"):
            st.info(f"Nombre de lignes du Dataframe : {df.shape[0]}")
            st.info(f"Nombre de colonnes du Dataframe : {df.shape[1]}")
            st.info("Type de données par colonne :")
            st.write(df.dtypes)
            st.info("Nombre de valeurs manquantes par colonne :")
            st.write(df.isnull().sum())
            st.info("Valeurs nulles ou champs vides :")
            st.write(df[df.isnull().any(axis=1)])
            st.info("Statistiques descriptives du DataFrame :")
            st.write(df.describe())
            
            # Bouton pour rediriger vers Google Colab
            if st.button("Nettoyer avec PySpark", key="spark_button", help="Cliquez ici pour nettoyer avec PySpark"):
                notebook_url = "https://colab.research.google.com/drive/1pXo6FZEd-B_Q_jR3NfRzsqEgARNlSnCg"
                webbrowser.open(notebook_url)  
            st.image("data/Pyspark.png", caption="pyspark_image", width=120, use_column_width=False)

def select_collection_from_mongodb(db_name, cluster_uri):
    try:
        client = pymongo.MongoClient(cluster_uri)
        db = client[db_name]
        collections_list = db.list_collection_names()

        selected_collection = st.selectbox("Sélectionnez une collection", collections_list)

        return selected_collection  
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données MongoDB : {e}")

def display_top_profitable_customers_bar_chart(collection):
    st.header("Top 10 des clients les plus rentables")
    top_customers_df = collection.sort_values(by="Total des ventes", ascending=False).head(10)
    plt.figure(figsize=(10, 6))
    plt.barh(top_customers_df["CUSTOMERNAME"], top_customers_df["Total des ventes"], color='skyblue')
    plt.xlabel('Total des ventes')
    plt.ylabel('Clients')
    plt.title('Top 10 des clients les plus rentables')
    plt.gca().invert_yaxis()
    st.pyplot()

def display_bottom_profitable_customers_bar_chart(collection):
    st.header("Top 10 des clients les moins rentables")
    bottom_customers_df = collection.sort_values(by="Total des ventes").head(10)
    plt.figure(figsize=(10, 6))
    plt.barh(bottom_customers_df["CUSTOMERNAME"], bottom_customers_df["Total des ventes"], color='salmon')
    plt.xlabel('Total des ventes')
    plt.ylabel('Clients')
    plt.title('Top 10 des clients les moins rentables')
    plt.gca().invert_yaxis()
    st.pyplot()

def display_most_sold_products_pie_chart(collection):
    st.header("Produits les plus vendus (répartition circulaire)")
    pipeline = [
        {"$sortByCount": "$PRODUCTLINE"}  
    ]
    most_sold_products = list(collection.aggregate(pipeline))
    df_most_sold_products = pd.DataFrame(most_sold_products)
    df_most_sold_products.rename(columns={"_id": "PRODUCTLINE", "count": "Total des ventes"}, inplace=True)
    fig = px.pie(df_most_sold_products, values='Total des ventes', names='PRODUCTLINE',
                 title='Répartition des produits les plus vendus')
    st.plotly_chart(fig, use_container_width=True, width=1000, height=1000)

def display_sales_trends_over_time_line_chart(collection):
    st.header("Evolution des ventes au fil du temps")
    pipeline = [
        {"$project": {"MONTH": {"$month": "$ORDERDATE"}, "SALES": 1}},  # Extraire le mois et le montant de la vente
        {"$group": {"_id": "$MONTH", "Total des ventes": {"$sum": "$SALES"}}},  # Regrouper par mois et calculer le total des ventes
        {"$sort": {"_id": 1}}  # Trier par mois
    ]
    sales_trends_monthly = list(collection.aggregate(pipeline))
    df_sales_trends_monthly = pd.DataFrame(sales_trends_monthly)
    df_sales_trends_monthly.rename(columns={"_id": "MONTH"}, inplace=True)
    fig = px.line(df_sales_trends_monthly, x='MONTH', y='Total des ventes', markers=True,
                  labels={'Total des ventes': 'Total des ventes', 'MONTH': 'Mois'},
                  title='Évolution des ventes au fil du temps')
    st.plotly_chart(fig, use_container_width=True, width=900, height=600)

def display_sales_by_product_and_country_scatter_plot(collection):
    st.header("Ventes par lignes de produits et par pays")
    product_country_sales = collection.find({}, {"PRODUCTLINE_GROUPED": 1, "COUNTRY": 1, "Total des ventes": 1})
    df_product_country_sales = pd.DataFrame(product_country_sales)
    plt.figure(figsize=(12, 6))
    for country in df_product_country_sales['COUNTRY'].unique():
        data_country = df_product_country_sales[df_product_country_sales['COUNTRY'] == country]
        plt.scatter(data_country['PRODUCTLINE_GROUPED'], data_country['Total des ventes'], label=country)
    plt.xlabel('Ligne de produit')
    plt.ylabel('Ventes')
    plt.title('Ventes par lignes de produits et par pays')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    st.pyplot(plt)

def display_sales_stats_by_status_bar_chart(collection):
    st.header("Statistiques des ventes par statut de commande")
    sales_stats_by_status = collection.find({}, {"STATUS": 1, "Mean_Sales": 1, "Stddev_Sales": 1})
    df_sales_stats_by_status = pd.DataFrame(sales_stats_by_status)
    if "Stddev_Sales" in df_sales_stats_by_status.columns:
        df_sales_stats_by_status = df_sales_stats_by_status.dropna(subset=['Stddev_Sales'])       
        plt.figure(figsize=(10, 6))
        plt.bar(df_sales_stats_by_status['STATUS'], df_sales_stats_by_status['Mean_Sales'],
                yerr=df_sales_stats_by_status['Stddev_Sales'], color='b', ecolor='r', capsize=5)
        plt.xlabel('Statut de commande')
        plt.ylabel('Ventes')
        plt.title('Statistiques des ventes par statut de commande')
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        st.pyplot(plt)
    else:
        st.error("La colonne 'Stddev_Sales' n'existe pas dans les données.")


def exploration(collection):
    st.header("Exploration des données")

    graph_options = ["TOP 10 des clients les plus renables(barres)", "Top 10 de clients les moins rentables(barres)","Produits les plus vendus(répartition circulaire)",
                     "Evolution des ventes au fil du temps", "Ventes par lignes de produits et par pays", "statistiques des ventes par statut de commande"]
    selected_graph = st.sidebar.selectbox("Sélectionnez un graphique", graph_options)

    # Affichage du graphique sélectionné
    if selected_graph == "TOP 10 des clients les plus rentables(barres)":
        display_top_profitable_customers_bar_chart(collection)
    elif selected_graph == "TOP 10 des clients les moins rentables(barres)":
        display_bottom_profitable_customers_bar_chart(collection)
    elif selected_graph == "Produits les plus vendus(répartition circulaire)":
        display_most_sold_products_pie_chart(collection)
    elif selected_graph == "Evolution des ventes au fil du temps":
        display_sales_trends_over_time_line_chart(collection)
    elif selected_graph == "Ventes par lignes de produits et par pays":
        display_sales_by_product_and_country_scatter_plot(collection)
    elif selected_graph == "statistiques des ventes par statut de commande":
         display_sales_stats_by_status_bar_chart(collection)

   



#dans le main :

def main():
    set_page_config()

    st.title("📊 Sales Dashboard")

    cluster_uri = "mongodb+srv://Emma:mongo_emma@clusterautosales.ppc9aov.mongodb.net/?retryWrites=true&w=majority"
    database_name = "dataviz_project_db"
    collection_name = "auto_sales"
    collection, message = connexion_db(database_name, collection_name, cluster_uri)

    if collection is not None:
        st.success(message)
        cursor = collection.find()
        df = cursor_to_dataframe(cursor)
        afficher_apercu_df(df)
        inspecter_dataframe_button(df)
    else:
        st.error("La collection n'a pas été trouvée.")
    
    if "explorer_clicked" not in st.session_state:
        st.session_state.explorer_clicked = False
    if "select_clicked" not in st.session_state:
        st.session_state.select_clicked = False

    if st.sidebar.button("Explorer les données"):
        st.session_state.explorer_clicked = True
        st.session_state.select_clicked = False
    if st.session_state.explorer_clicked:
        exploration(collection)

    if st.sidebar.button("Sélectionner une autre collection"):
        st.session_state.select_clicked = True
        st.session_state.explorer_clicked = False
        selected_collection = select_collection_from_mongodb(database_name, cluster_uri)

        if selected_collection:
            st.success(f"Collection sélectionnée : {selected_collection}")
            collection_data = collection.find()
            df = cursor_to_dataframe(collection_data)
            afficher_apercu_df(df)
        else:
            st.error("Aucune collection n'a été sélectionnée.")

if __name__ == "__main__":
    main()

    

