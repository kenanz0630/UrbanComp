import psycopg2

conn=psycopg2.connect('''
	dbname='tweet' 
	user='kenanz'
	password='ZKNnaiad_0630' 
	host='ec2-54-221-193-1.compute-1.amazonaws.com'
	''')
cur=conn.cursor()

cur.execute('''
	select user_screen_name, 
	date_part('year',created_at),
	date_part('doy',created_at),
	date_part('dow',created_at),
	date_part('hour', created_at),
	st_astext(coordinates),text,source  
	from tweet_chicago limit 10;''')

data=cur.fetchall()
for line in data:
	print line