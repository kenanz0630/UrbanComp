import dbProcess, os, math, shapefile
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.colors as colors

'''--- MAIN FUNCTION ---'''
def foursq_tweet(tbname,dbname='tweet_pgh',user='postgres'): 
	#process 4sq auto_tweet to extract venue 
	#export tweets of each venue
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export auto-tweet txt'
	pg.cur.execute('''
		select txt from %s where auto_tweet='4sq' group by txt;
		'''%pg.tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	txt=[data[i][0] for i in xrange(n)]
	
	print 'Process venue name'
	venues=process_venue_name(txt)
	venues.remove('')
	print 'Find %d venue names'%(len(venues))

	print 'Export auto-tweet of each venue'
	data=init_clusters(len(venues),venues)
	for venue in venues:
		venue_p=venue.replace("'","''")
		pg.cur.execute('''
			select username,year,doy,dow,hour,lon,lat,senti_val from %s 
			where auto_tweet='4sq' and txt like '%%I''m at %s%%';
			'''%(pg.tbname,venue_p))
		data[venue]=pg.cur.fetchall()
		print '-- %s %d'%(venue,pg.cur.rowcount)

	'''
	print 'Clean auto-tweet txt'
	for venue in venues:
		data[venue]=clean_txt(data[venue])
	'''

	print 'Write results'
	f=os.open('txt\\'+tbname+'_4sq_tweet.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,user,year,doy,dow,hour,lon,lat,senti_val\n')

	for venue in venues:
		n=len(data[venue])
		print 'Venue: %s %d'%(venue,n)
		for i in xrange(n):
			(user,year,doy,dow,hour,lon,lat,senti_val)=data[venue][i]
			os.write(f,'%s,%s,%d,%d,%d,%d,%0.4f,%0.4f,%0.3f\n'%(venue,user,year,doy,dow,hour,lon,lat,senti_val))

	os.close(f) 
