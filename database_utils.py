import psycopg2, database_auth

connection = None
cursor = None


def open_db_connection():
    """Opens the connection to the SQL database"""

    global connection, cursor
    try:
        connection = database_auth.getPostgreSQLConnection(psycopg2)
        cursor = connection.cursor()
        # print("PostgreSQL connection is open")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


def close_db_connection():
    """Closes the connection to the SQL database and commits the queries"""

    # closing database connection.
    if connection:
        connection.commit()
        cursor.close()
        connection.close()

# Maak nieuw schema en de bijbehorende primary en foreign keys
def create_schema_standard_recs():
    try:
        cursor.execute(
            "CREATE TABLE if not exists public.product_standard_recs (product_id varchar not null, recommendations varchar[] null, constraint product_standard_recommendation_pkey primary key (product_id), constraint product_standard_recommendation_product_id_fkey foreign key (product_id) references products(product_id));")
    except:
        connection.rollback()


def retrieve_recommendations(product_id: str, list_limit=4):
    # haal alle producten die o.b.v 'sub_sub_categorie' bij elkaar passen
    try:
        cursor.execute(f"""select pio.product_id , count(pio.product_id) as p_count from product_in_order pio inner join product_categories pc on pc.product_id = pio.product_id where session_id in (select session_id from product_in_order pio2 where product_id like '{product_id}') and pio.product_id not like '{product_id}' and pc.sub_sub_category not in ( select sub_sub_category from product_categories pc2 where product_id like '{product_id}') and sub_sub_category is not null group by pio.product_id, pc.sub_sub_category order by p_count desc limit {list_limit}; """)
    except:
        connection.rollback()

    # Haal alle ids uit de resultaten
    # Als er geen resultaten zijn (dus er zijn geen producten (die eerder bij elkaar gekocht zijn) die op elkaar lijken)
    # Dan een lijst met None's returnen
    try:
        results = [i[0] for i in cursor.fetchall()]
        return results
    except psycopg2.ProgrammingError:
        return [None for i in range(list_limit)]

def fill_recommendations(product_id, recs):
    # Insert een lijst met de product_id met de recommendations in de database
    try:
        cursor.execute(f"insert into public.product_standard_recs (product_id, recommendations) values (%s, %s)", (product_id, recs))
    except:
        connection.rollback()
        pass

def fill_table_with_all_recs(limit_recs=4):
    # open de connectie
    open_db_connection()
    # Haal alle producten op die een sub_sub_categorie hebben. (anders kunnen we niet matchen want er is geen categorie die duidelijk genoeg is)
    try:
        cursor.execute("select p.product_id, pc.sub_sub_category from products p inner join product_categories pc on pc.product_id = p.product_id where sub_sub_category is not null;")
    except:
        connection.rollback

    # uit deze lijst, filter alle ids
    all_product_ids = [e[0] for e in cursor.fetchall()]
    # counter voor progress init
    counter = 1

    # loop door alle ids
    for product_id in all_product_ids:
        # print progress op de eerste, laatste en elke duizendste keer
        if counter == 1 or counter == len(all_product_ids) or counter % 1000 == 0:
            print(f"Progress: {counter}/{len(all_product_ids)}")

        # Maak schema aan
        create_schema_standard_recs()
        # Haal recommendations op (o.b.v sub_sub_categorie)
        recs = retrieve_recommendations(product_id, list_limit=limit_recs)
        # Stop ze in de database
        fill_recommendations(product_id, recs)
        # progress bijwerken
        counter += 1
    # connectie sluiten. Hierbij wordt gecommit naar de database
    close_db_connection()


