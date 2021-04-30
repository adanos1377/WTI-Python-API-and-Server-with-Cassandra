from cassandra.cluster import Cluster
from cassandra.query import dict_factory

class cassandra:
    def __init__(self):
        self.keyspace="ratings"
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        self.ratings = "movie_ratings"
        self.profiles = "user_profiles"
        self.create_keyspace()
        self.session.set_keyspace(self.keyspace)
        self.session.row_factory = dict_factory
        self.create_table(self.ratings)
        self.create_table(self.profiles)
    def create_keyspace(self):
        self.session.execute("""CREATE KEYSPACE IF NOT EXISTS """ + self.keyspace + """
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }""")


    def create_table(self,table):
        if table==self.ratings:
            self.session.execute(
            """CREATE TABLE IF NOT EXISTS """ + self.keyspace + """.""" + table + """ 
            (userID float ,movieID float,rating float,action float, adventure float, animation float, children float, comedy float,
            crime float, documentary float, drama float, fantasy float, noir float, horror float, IMAX float, musical float,
            mystery float, romance float, Sci_Fi float, short float, thriller float, war float, western float, PRIMARY KEY(userID,movieID,rating))""")
        if table==self.profiles:
            self.session.execute(
            """CREATE TABLE IF NOT EXISTS """ + self.keyspace + """.""" + table + """ 
            (userID float ,action float, adventure float, animation float, children float, comedy float,
            crime float, documentary float, drama float, fantasy float, noir float, horror float, IMAX float, musical float,
            mystery float, romance float, Sci_Fi float, short float, thriller float, war float, western float, PRIMARY KEY(userID))""")

    def push_data_table(self,table, data,id=None):
        if table==self.ratings:
            self.session.execute(
            """INSERT INTO """ + self.keyspace + """.""" + table + """(userID, movieID, rating, action, adventure, animation,children, comedy,
            crime, documentary, drama, fantasy, noir, horror, IMAX, musical, mystery, romance, Sci_Fi, short, thriller, war, western)
             VALUES (%(userID)s, %(movieID)s,%(rating)s,%(action)s,%(adventure)s,%(animation)s,
             %(children)s,%(comedy)s,%(crime)s,%(documentary)s,%(drama)s,%(fantasy)s,
             %(noir)s,%(horror)s,%(IMAX)s,%(musical)s,%(mystery)s,%(romance)s,
             %(Sci_Fi)s,%(short)s,%(thriller)s,%(war)s,%(western)s)         """,
            {'userID': data["userID"], 'movieID': data["movieID"], 'rating': data["rating"],'action':data["genre-Action"],'adventure':data["genre-Adventure"],
             'animation':data["genre-Animation"], 'children':data["genre-Children"], 'comedy':data["genre-Comedy"], 'crime':data["genre-Crime"],'documentary':data["genre-Documentary"],
             'drama': data["genre-Drama"],'fantasy':data["genre-Fantasy"], 'noir':data["genre-Film-Noir"], 'horror':data["genre-Horror"], 'IMAX':data["genre-IMAX"],
             'musical':data["genre-Musical"], 'mystery':data["genre-Mystery"],'romance':data["genre-Romance"], 'Sci_Fi':data["genre-Sci-Fi"], 'short':data["genre-Short"],
             'thriller':data["genre-Thriller"], 'war':data["genre-War"], 'western':data["genre-Western"]})
        if table==self.profiles:
            self.session.execute(
            """INSERT INTO """ + self.keyspace + """.""" + table + """(userID, action, adventure, animation,children, comedy,
            crime, documentary, drama, fantasy, noir, horror, IMAX, musical, mystery, romance, Sci_Fi, short, thriller, war, western)
             VALUES (%(userID)s, %(action)s,%(adventure)s,%(animation)s,
             %(children)s,%(comedy)s,%(crime)s,%(documentary)s,%(drama)s,%(fantasy)s,
             %(noir)s,%(horror)s,%(IMAX)s,%(musical)s,%(mystery)s,%(romance)s,
             %(Sci_Fi)s,%(short)s,%(thriller)s,%(war)s,%(western)s)         """,
            {'userID': id, 'action':data[0],'adventure':data[1],
             'animation':data[2], 'children':data[3], 'comedy':data[4], 'crime':data[5],'documentary':data[6],
             'drama': data[7],'fantasy':data[8], 'noir':data[9], 'horror':data[10], 'IMAX':data[11],
             'musical':data[12], 'mystery':data[13],'romance':data[14], 'Sci_Fi':data[15], 'short':data[16],
             'thriller':data[17], 'war':data[18], 'western':data[19]})

    def get_data_table(self, table, id=None):
        if id==None:
            rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + table + ";")
            return rows
        else:
            rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + table + " WHERE userid="+id+";")
            return rows

    def clear_table(self,table):
        self.session.execute("TRUNCATE " + self.keyspace + "." + table + ";")


    def delete_table(self,table):
        self.session.execute("DROP TABLE " + self.keyspace + "." + table + ";")


if __name__ == "__main__":
    cass=cassandra()
    table = "user_avg_rating"
    # utworzenia połączenia z klastrem

    # utworzenie nowego keyspace

    # ustawienie używanego keyspace w sesji

    # użycie dict_factory pozwala na zwracanie słowników
    # znanych z języka Python przy zapytaniach do bazy danych

    # tworzenie tabeli

    # umieszczanie danych w tabeli

    # pobieranie zawartości tabeli i wyświetlanie danych

    # czyszczenie zawartości tabeli

    # usuwanie tabeli
