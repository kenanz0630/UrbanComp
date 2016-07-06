import psycopg2

try:
    conn = psycopg2.connect("dbname='pittsburgh' user='postgres' host='localhost' password='qwertyui'")
except:
    print "I am unable to connect to the database"


cur = conn.cursor()
try:
    cur.execute("""SELECT * from tweet_pgh limit 10;""")
except:
    print "I can't SELECT from bar"

rows = cur.fetchall()
print "\nRows: \n"
for row in rows:
    print "   ", row[1]