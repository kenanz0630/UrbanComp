import dbProcess, os
import tweet_temp
import datetime as dt

'''--- MAIN FUNCTION ---'''
def tweet_n(tbname,names,dbname='tweet_pgh',user='postgres'): #tweet_n of each category by date 
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export data'
	tweet_dict=[]
	for name in names:
		if name=='non':
			auto_tweet='is Null'
		else:
			auto_tweet="='%s'"%name
		tweet_dict+=[tweet_n_export_data(pg,auto_tweet)]

	print "Process data"
	start=dt.datetime.strptime("2014-01-22", "%Y-%m-%d")
	end=dt.datetime.strptime("2015-11-07", "%Y-%m-%d")
	day_n=(end-start).days

	ts=[start+dt.timedelta(days=i) for i in xrange(day_n)]
	ts=[ts[i].strftime("%Y-%m-%d") for i in xrange(day_n)]

	tweet_ts=[]
	for i in xrange(len(names)):
		tweet_ts+=[tweet_n_process_data(tweet_dict[i],ts,day_n)]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_n_auto.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,'+','.join(names)+'\n')
	for i in xrange(day_n):
		os.write(f,'%s,'%ts[i]+','.join(['%d'%tweet_ts[j][i] for j in xrange(len(names))])+'\n')

	os.close(f) 

def tweet_n_doy(tbname,names,dbname='tweet_pgh',user='postgres'): ##tweet_n of each category by doy
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export data'
	tweet_dict=[]
	for name in names:
		if name=='non':
			auto_tweet='is Null'
		else:
			auto_tweet="='%s'"%name
		tweet_dict+=[tweet_n_doy_export_data(pg,auto_tweet)]

	print "Process data"
	ts=range(1,366)
	
	tweet_ts=[]
	for i in xrange(len(names)):
		tweet_ts+=[tweet_n_process_data(tweet_dict[i],ts,365)]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_n_doy_auto.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'doy,'+','.join(names)+'\n')
	for i in xrange(365):
		os.write(f,'%d,'%ts[i]+','.join(['%d'%tweet_ts[j][i] for j in xrange(len(names))])+'\n')

	os.close(f)

def tweet_senti_hist(tbname,names,dbname='tweet_pgh',user='postgres'): #senti_val hist of each category
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
	f=os.open('txt\\'+tbname+'_senti_hist_auto.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'bin,'+','.join(names)+'\n')
	for i in xrange(20):
		os.write(f,'%0.1f,'%bins[i]+','.join(['%d'%data[j][i] for j in xrange(len(names))])+'\n')

	os.close(f)


def tweet_stats(tbname,names,dbname='tweet_pgh',user='postgres'): #positive/neutral/negative tweet_n of each category
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
			select count(*),avg(senti_val) from tweet_all_new where auto_tweet %s;
			'''%auto_tweet)
		(count,senti_avg)=pg.cur.fetchone()
		pg.cur.execute('''

			select count(*) from tweet_all_new where auto_tweet %s 
			group by senti_bool order by senti_bool;
			'''%auto_tweet)
		senti_bool=pg.cur.fetchall()
		data+=[[count,senti_avg,senti_bool]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_auto_tweet_stats.txt',os.O_RDWR|os.O_CREAT)
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
def tweet_n_export_data(pg,auto_tweet):
	pg.cur.execute('''
		select (date '2014-01-01'+ doy-1) as temp,count(*) 
		from %s where year=2014 and auto_tweet %s group by temp order by temp;
		'''%(pg.tbname,auto_tweet))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n=[data[i][-1] for i in xrange(n)]
	temp=[str(data[i][0]) for i in xrange(n)]
	pg.cur.execute('''
		select (date '2015-01-01'+ doy-1) as temp,count(*) 
		from %s where year=2015 and auto_tweet %s group by temp order by temp;
		'''%(pg.tbname,auto_tweet))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n+=[data[i][-1] for i in xrange(n)]
	temp+=[str(data[i][0]) for i in xrange(n)]
	tweet_dict=init_clusters(len(temp),temp,tweet_n)

	return tweet_dict

def tweet_n_doy_export_data(pg,auto_tweet):
	pg.cur.execute('''
		select doy,count(*) from %s where auto_tweet %s group by doy order by doy;
		'''%(pg.tbname,auto_tweet))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	tweet_n=[data[i][1] for i in xrange(n)]
	temp=[data[i][0] for i in xrange(n)] 
	tweet_dict=init_clusters(n,temp,tweet_n)

	return tweet_dict


def tweet_n_process_data(tweet_dict,ts,day_n):	
	tweet_ts=[0 for i in xrange(day_n)]
	for i in xrange(day_n):
		if ts[i] in tweet_dict:
			tweet_ts[i]=tweet_dict[ts[i]]

	return tweet_ts


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
	
	
