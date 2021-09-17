import commons
def db_con():
    connection = commons.db
    db = connection.booksearch
    return db