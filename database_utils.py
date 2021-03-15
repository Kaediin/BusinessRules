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


def create_schema_standard_recs():
    try:
        cursor.execute(
            "CREATE TABLE if not exists public.product_standard_recs (product_id varchar not null, recommendations varchar[] null, constraint product_standard_recommendation_pkey primary key (product_id), constraint product_standard_recommendation_product_id_fkey foreign key (product_id) references products(product_id));")
    except:
        connection.rollback()


def retrieve_recommendations(product_id: str, list_limit=4):
    try:
        cursor.execute(f"""select pio.product_id , count(pio.product_id) as p_count from product_in_order pio inner join product_categories pc on pc.product_id = pio.product_id where session_id in (select session_id from product_in_order pio2 where product_id like '{product_id}') and pio.product_id not like '{product_id}' and pc.sub_sub_category not in ( select sub_sub_category from product_categories pc2 where product_id like '{product_id}') and sub_sub_category is not null group by pio.product_id, pc.sub_sub_category order by p_count desc limit {list_limit}; """)
    except:
        connection.rollback()

    try:
        results = [i[0] for i in cursor.fetchall()]
        return results
    except psycopg2.ProgrammingError:
        return [None for i in range(list_limit)]

def fill_recommendations(product_id, recs):
    try:
        cursor.execute(f"insert into public.product_standard_recs (product_id, recommendations) values (%s, %s)", (product_id, recs))
    except:
        connection.rollback()
        pass

def fill_table_with_all_recs(limit_recs=4):
    open_db_connection()
    try:
        cursor.execute("select p.product_id, pc.sub_sub_category from products p inner join product_categories pc on pc.product_id = p.product_id where sub_sub_category is not null;")
    except:
        connection.rollback


    all_product_ids = [e[0] for e in cursor.fetchall()]
    counter = 1
    for product_id in all_product_ids:
        if counter == 1 or counter == len(all_product_ids) or counter % 1000 == 0:
            print(f"Progress: {counter}/{len(all_product_ids)}")
        create_schema_standard_recs()
        recs = retrieve_recommendations(product_id, list_limit=limit_recs)
        fill_recommendations(product_id, recs)
        counter += 1
    close_db_connection()


