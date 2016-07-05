import dbProcess, os
import tweet_temp
import datetime as dt
import matplotlib.pyplot as plt
import tweet_auto_sub as sub

'''--- MAIN FUNCTION ---'''
	
def venue_tweet_cls(fn_venue,fn_tweet): #rewrite venue tweet by cluster
	print 'Extract cluster venues'
	venues=extract_venue_cls(fn_venue)

	print 'Extract venue tweets'
	tweet=extract_venue_tweet(fn_tweet)

	print 'Process and write cluster tweets'
	write_venue_tweet_cls(fn_venue,venues,tweet)


def auto_tweet_venue_both(tbname,dbname='tweet_pgh',user='postgres'):
	#merge 4sq and inst venues
	print 'Export and process auto-tweet, group by venues and write venue tweet'
	print '-- Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	auto=['4sq','inst']
	sub.auto_tweet_both(pg,auto)

	print 'Process venue name and rewrite venue tweet'
	fname=tbname+'_tweet'
	sub.auto_venue(fname)


def auto_tweet_venue(tbname,auto,dbname='tweet_pgh',user='postgres'): 
	#extract 4sq/inst venues, draw venue map and seach tweets with error sentiment values
	print 'Export and process auto-tweet, group by venues and write venue tweet'
	print '-- Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	if auto=='4sq':
		auto_txt="I'm at "
	elif auto=='inst':
		auto_txt="Just posted a photo "
	sub.auto_tweet(pg,auto,auto_txt)

	print 'Process venue name and rewrite venue tweet'
	fname=tbname+'_%s_tweet'%auto
	sub.auto_venue(fname)

	print 'Draw venue map'
	fname=tbname+'_%s_venue'%auto
	#sub.auto_venue_map(fname)
	
	print 'Extract auto-tweet with err senti' 
	fn_tweet=tbname+'_%s_tweet_new'%auto
	#sub.auto_err_tweet(fname,fn_tweet)
	

def auto_tweet_map(tbname,auto,dbname='tweet_pgh',user='postgres',sep='\t'): #export auto_tweet and draw map
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export auto-tweet coordis'
	pg.cur.execute('''select lon,lat,username,year,doy,dow,hour,txt,senti_val 
		from %s where auto_tweet='%s';'''%(pg.tbname,auto))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	
	print 'Draw auto-tweet map'
	plt.figure()
	x=[float(data[i][0]) for i in xrange(n)]
	y=[float(data[i][1]) for i in xrange(n)]
	plt.scatter(x,y,marker='o')
	plt.xlabel('lon')
	plt.ylabel('lat')
	plt.xlim(-80.10,-79.86)
	plt.ylim(40.36,40.51)
	plt.title('%s-tweet Map'%auto)
	plt.savefig('png\\'+tbname+'_%s.png'%auto)
	
	print 'Write results'
	f=os.open('txt\\auto-tweet\\'+tbname+'_tweet_%s_full.txt'%auto,os.O_RDWR|os.O_CREAT)
	os.write(f,'lon,lat,user,year,doy,dow,hour,txt,senti_val\n'.replace(',',sep))

	for i in xrange(n):
		(lon,lat,username,year,doy,dow,hour,txt,senti)=data[i]
		if 'amp' in txt:
			txt=txt.replace(' amp ',' & ')
		os.write(f,('%0.4f,%0.4f,%s,%d,%d,%d,%d,'%(lon,lat,username,year,doy,dow,hour)).replace(',',sep)\
			+'%s%s%0.3f\n'%(txt,sep,senti))

	os.close(f) 
	

def auto_tweet_senti_hist(tbname,names,dbname='tweet_pgh',user='postgres'): #senti_val hist of each category
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export and process data'
	bins=[-1+0.1*i for i in xrange(20)]
	data=[]
	for name in names:
		if name=='non':
			auto_tweet='is Null'
		else:
			auto_tweet="='%s'"%name
		data+=[tweet_senti_hist_part(pg,auto_tweet,bins)]
	
	print 'Write results'
	f=os.open('txt\\auto-tweet\\'+tbname+'_senti_hist_auto.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'bin,'+','.join(names)+'\n')
	for i in xrange(20):
		os.write(f,'%0.1f,'%bins[i]+','.join(['%d'%data[j][i] for j in xrange(len(names))])+'\n')

	os.close(f)

def auto_tweet_stats(tbname,names,dbname='tweet_pgh',user='postgres'): #positive/neutral/negative tweet_n of each category
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print "Process data"
	data=[]
	for name in names:
		if name=='non':
			auto_tweet='is Null'
		else:
			auto_tweet="='%s'"%name
		pg.cur.execute('''
			select count(*),avg(senti_val) from %s where auto_tweet %s;
			'''%(tbname,auto_tweet))
		(count,senti_avg)=pg.cur.fetchone()
		pg.cur.execute('''

			select count(*) from %s where auto_tweet %s 
			group by senti_bool order by senti_bool;
			'''%(tbname,auto_tweet))
		senti_bool=pg.cur.fetchall()
		data+=[[count,senti_avg,senti_bool]]

	print 'Write results'
	f=os.open('txt\\auto-tweet\\'+tbname+'_auto_tweet_stats.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'name,count,senti_avg,neu_n,pos_n,neg_n\n')
	for i in xrange(len(data)):
		[count,senti_avg,senti_bool]=data[i]
		os.write(f,'%s,%d,%0.4f,%d,%d,%d\n'%(names[i],count,senti_avg,senti_bool[1][0],senti_bool[2][0],senti_bool[0][0]))

	os.close(f)



'''--- HELPER FUNCTION ---'''
def init_clusters(k,names,vals=False):
	d=dict()
	for i in xrange(k):
		key=names[i]
		if vals:
			d[key]=vals[i]
		else:	
			d[key]=[]
	return d

'''------------------------------------------------------------------'''
def tweet_senti_hist_part(pg,auto_tweet,bins):
	data=[0 for bin in bins]
	for i in xrange(len(bins)):
		bin=bins[i]
		if bin==0.9:
			pg.cur.execute('''
			select count(*) from %s where auto_tweet %s and 
			senti_val>= %f and senti_val<=1;'''%(pg.tbname,auto_tweet,bin))
		else:
			pg.cur.execute('''
			select count(*) from %s where auto_tweet %s and 
			senti_val>= %f and senti_val< %f;'''%(pg.tbname,auto_tweet,bin,bin+0.1))		
		data[i]=pg.cur.fetchone()[0]
	
	return data	
	
'''------------------------------------------------------------------'''
def extract_venue_cls(fname,sep='\t'):
	data=file('txt\\auto-tweet\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]	
	venue=[data[i][1] for i in xrange(n)]
	coordi=[[float(data[i][2]),float(data[i][3])] for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]

	venue_dict=init_clusters(len(venue),venue,[[clid[i],coordi[i]] for i in xrange(n)])

	return venue_dict

def extract_venue_tweet(fname,sep='\t'):
	data=file('txt\\auto-tweet\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	venues=list(set([data[i][0] for i in xrange(n)]))
	tweet=init_clusters(len(venues),venues)
	for venue in venues:
		tweet[venue]=[data[i][1:-1] for i in xrange(n) if data[i][0]==venue]

	return tweet	
	
def write_venue_tweet_cls(fname,venues,tweet,sep='\t'):
	f=os.open('txt\\auto-tweet\\'+fname+'_tweet.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid,venue,lon,lat,user,year,doy,dow,hour'.replace(',',sep)+'\n')
	
	for venue in venues:
		data=tweet[venue]
		[clid,coordi]=venues[venue]
		if coordi!=[float(data[0][0]),float(data[0][1])]:
			print 'Error: venue coordis not match'
		else:
			n=len(data)
			for i in xrange(n):
				os.write(f,sep.join(['%d'%clid,'%s'%venue]+data[i])+'\n')

	os.close(f)

