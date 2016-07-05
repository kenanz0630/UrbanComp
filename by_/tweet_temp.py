import dbProcess, tweet_venue 
import os, copy
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from scipy import stats


'''--- MAIN FUNCTION ---'''
'''--- AUTO/GENU TWEET_N AT CITY LEVEL ---'''
def tweet_n_city(tbname,autos=['non','4sq','inst','job'],dbname='tweet_pgh',user='postgres'): 
	# auto/genu weet_n_city by date
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export diff auto and genu tweet data'
	names=[]
	for auto in autos:
		if auto=='non':
			names+=['is Null']
		else:
			names+=["='%s'"%auto]

	tweet_n=[export_tweet_all(pg,name) for name in names]

	print 'Process data'	
	k=len(autos)
	temp=[np.sort(tweet_n[i].keys()) for i in xrange(k)]
	start=min([dt.datetime.strptime(temp[i][0], "%Y-%m-%d") for i in xrange(k)])
	end=max([dt.datetime.strptime(temp[i][-1], "%Y-%m-%d") for i in xrange(k)])
	day_n=(end-start).days
	
	ts=[start+dt.timedelta(days=i) for i in xrange(day_n)]
	ts=[ts[i].strftime("%Y-%m-%d") for i in xrange(day_n)]
	tweet_ts=[[0 for j in xrange(k)] for i in xrange(day_n)]
	for i in xrange(day_n):
		for j in xrange(k):
			if ts[i] in tweet_n[j]:
				tweet_ts[i][j]=tweet_n[j][ts[i]]

	print 'Write results'
	f=os.open('txt\\temp\\'+tbname+'_n_city.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,'+','.join(['%s_n'%auto for auto in autos])+'\n')
	for i in xrange(day_n):
		os.write(f,'%s,'%ts[i]+','.join(['%d'%tweet_ts[i][j] for j in xrange(k)])+'\n')

	os.close(f)

def tweet_n_city_scale(tbname,scale,autos=['non','4sq','inst','job'],dbname='tweet_pgh',user='postgres'):
	# auto/genu tweet_n_city by scale
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export diff auto and genu tweet data of time-scale %s'%scale
	names=[]
	for auto in autos:
		if auto=='non':
			names+=['is Null']
		else:
			names+=["='%s'"%auto]
	
	if scale=='how':
		temp='dow*24+hour'
		ts=range(24*7)
	else:
		temp=scale
		ts=range(1,366)	

	tweet_n=[export_tweet_all_scale(pg,name,temp) for name in names]

	k=len(autos)
	tweet_ts=[[0 for j in xrange(k)] for i in xrange(len(ts))]
	for i in xrange(len(ts)):
		for j in xrange(k):
			if ts[i] in tweet_n[j]:
				tweet_ts[i][j]=tweet_n[j][ts[i]]

	print 'Write results'
	f=os.open('txt\\temp\\'+tbname+'_n_city_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,'%scale+','.join(['%s_n'%auto for auto in autos])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%s,'%ts[i]+','.join(['%d'%tweet_ts[i][j] for j in xrange(k)])+'\n')

	os.close(f)


'''--- GENU_TWEET TEMP SENTI AT CITY-LEVEL---'''
def tweet_senti_city(tbname,dbname='tweet_pgh',user='postgres'):
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Genu-tweet senti_val '
	pg.cur.execute('''
		select avg(senti_val),stddev(senti_val) 
		from %s where auto_tweet is Null;'''%pg.tbname)
	(avg,std)=pg.cur.fetchall()[0]
	print '-- avg %0.4f std %0.4f'%(avg,std)
	(pos_b,neg_b)=(avg+std,avg-std)	
	#0.041,0.280

	print 'Export genue-tweet senti'	
	print '-- happy tweets'	
	pg.cur.execute('''
		select (date '2014-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2014 and auto_tweet is Null
		and senti_val>%0.3f group by temp order by temp;'''%(tbname,pos_b))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	pos_n=[data[i][-1] for i in xrange(n)]
	pos_t=[str(data[i][0]) for i in xrange(n)]
	pg.cur.execute('''
		select (date '2015-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2015 and auto_tweet is Null
		and senti_val>%0.3f group by temp order by temp;'''%(tbname,pos_b))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	pos_n+=[data[i][-1] for i in xrange(n)]
	pos_t+=[str(data[i][0]) for i in xrange(n)]

	pos_dict=init_clusters(len(pos_t),pos_t,pos_n)

	
	print '-- unhappy tweets'	
	pg.cur.execute('''
		select (date '2014-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2014 and auto_tweet is Null
		and senti_val<%0.3f group by temp order by temp;'''%(tbname,neg_b))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	neg_n=[data[i][-1] for i in xrange(n)]
	neg_t=[str(data[i][0]) for i in xrange(n)]
	pg.cur.execute('''
		select (date '2015-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2015 and auto_tweet is Null
		and senti_val<%0.3f group by temp order by temp;'''%(tbname,neg_b))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	neg_n+=[data[i][-1] for i in xrange(n)]
	neg_t+=[str(data[i][0]) for i in xrange(n)]

	neg_dict=init_clusters(len(neg_t),neg_t,neg_n)

	print '-- avg tweet senti'	
	pg.cur.execute('''
		select (date '2014-01-01'+ doy -1) as temp,sum((senti_val-%0.4f)/%0.4f),count(*)
		from %s where year=2014 and auto_tweet is Null
		group by temp order by temp;'''%(avg,std,tbname))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	senti=[data[i][1:] for i in xrange(n)]
	senti_t=[str(data[i][0]) for i in xrange(n)]
	pg.cur.execute('''
		select (date '2015-01-01'+ doy -1) as temp,sum(senti_val),count(*)
		from %s where year=2015 and auto_tweet is Null
		group by temp order by temp;'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	senti+=[data[i][1:] for i in xrange(n)]
	senti_t+=[str(data[i][0]) for i in xrange(n)]

	senti_dict=init_clusters(len(senti_t),senti_t,senti)

	print 'Process temp tweet senti'
	start=dt.datetime.strptime(min(pos_t[0],neg_t[0],senti_t[0]), "%Y-%m-%d")
	end=dt.datetime.strptime(max(pos_t[-1],neg_t[-1],senti_t[-1]), "%Y-%m-%d")
	day_n=(end-start).days

	ts=[start+dt.timedelta(days=i) for i in xrange(day_n)]
	ts=[ts[i].strftime("%Y-%m-%d") for i in xrange(day_n)]
	tweet_ts=[[0,0,0.0,0] for i in xrange(day_n)]
	for i in xrange(day_n):
		if ts[i] in pos_dict:
			tweet_ts[i][0]=pos_dict[ts[i]]
		if ts[i] in neg_dict:
			tweet_ts[i][1]=neg_dict[ts[i]]
		if ts[i] in senti_dict:
			tweet_ts[i][2]=senti_dict[ts[i]][0]
			tweet_ts[i][3]=int(senti_dict[ts[i]][1])

	print 'Write results'
	f=os.open('txt\\temp\\'+tbname+'_senti_city.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,pos_n,neg_n,sum_senti,n\n')
	for i in xrange(day_n):
		os.write(f,'%s,%d,%d,%0.3f,%d\n'%(ts[i],tweet_ts[i][0],tweet_ts[i][1],\
			tweet_ts[i][2],tweet_ts[i][3]))

	os.close(f)

def tweet_senti_city_scale(tbname,scale,dbname='tweet_pgh',user='postgres'):
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)
	
	print 'Process temp tweet senti'	
	if scale=='how':
		name='dow*24+hour'
		ts=range(24*7)
	else:
		name=scale
		ts=range(1,366)

	print 'Genu-tweet senti_val '
	pg.cur.execute('''
		select avg(senti_val),stddev(senti_val) 
		from %s where auto_tweet is Null;'''%pg.tbname)
	(avg,std)=pg.cur.fetchall()[0]
	print '-- avg %0.4f std %0.4f'%(avg,std)
	(pos_b,neg_b)=(avg+std,avg-std)	

	print 'Export data senti of time-scale %s'%scale
	print '-- happy tweets'
	pg.cur.execute('''
		select %s,count(*) from %s where auto_tweet is Null
		and senti_val>%0.3f group by %s order by %s;'''%(name,tbname,pos_b,name,name))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	pos_n=[data[i][1] for i in xrange(n)]
	pos_t=[data[i][0] for i in xrange(n)] 
	pos_dict=init_clusters(n,pos_t,pos_n)

	print '-- unhappy tweets'
	pg.cur.execute('''
		select %s,count(*) from %s where auto_tweet is Null
		and senti_val<%0.3f group by %s order by %s;'''%(name,tbname,neg_b,name,name))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	neg_n=[data[i][1] for i in xrange(n)]
	neg_t=[data[i][0] for i in xrange(n)] 
	neg_dict=init_clusters(n,neg_t,neg_n)	

	print '-- avg tweet senti'
	pg.cur.execute('''
		select %s,sum((senti_val-%0.4f)/%0.4f),count(*) from %s where auto_tweet is Null
		group by %s order by %s;'''%(name,avg,std,tbname,name,name))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	senti=[data[i][1:] for i in xrange(n)]
	senti_t=[data[i][0] for i in xrange(n)]
	senti_dict=init_clusters(len(senti_t),senti_t,senti)

	
	tweet_ts=[[0,0,0.0,0] for i in xrange(len(ts))]

	for i in xrange(len(ts)):
		if ts[i] in pos_dict:
			tweet_ts[i][0]=pos_dict[ts[i]]
		if ts[i] in neg_dict:
			tweet_ts[i][1]=neg_dict[ts[i]]
		if ts[i] in senti_dict:
			tweet_ts[i][2]=senti_dict[ts[i]][0]
			tweet_ts[i][3]=senti_dict[ts[i]][1]

	print 'Write results'
	f=os.open('txt\\temp\\'+tbname+'_senti_city_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,pos_n,neg_n,sum_senti,n\n'%scale)
	for i in xrange(len(ts)):
		os.write(f,'%d,%d,%d,%0.3f,%d\n'%(ts[i],tweet_ts[i][0],tweet_ts[i][1],\
			tweet_ts[i][2],tweet_ts[i][3]))

	os.close(f)

def tweet_senti_city_hist(tbname,dbname='tweet_pgh',user='postgres'):
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)
	
	print 'Export and process data'
	pg.cur.execute('''
		select avg(senti_val),stddev(senti_val) from %s where auto_tweet is Null;'''
		%(pg.tbname))
	(avg,std)=pg.cur.fetchone()
	[senti_max,senti_min]=[(1-avg)/std,(-1-avg)/std]

	bins=[0.5*i for i in xrange(int(senti_min/0.5)-1,int(senti_max/0.5)+1)]
	k=len(bins)
	data=[0 for bin in bins]
	for i in xrange(k):
		if i==k-1:
			pg.cur.execute('''
			select count(*) from %s where auto_tweet is Null and 
			(senti_val-%f)/%f>= %f ;'''%(pg.tbname,avg,std,bins[i]))
		else:
			pg.cur.execute('''
			select count(*) from %s where auto_tweet is Null and 
			(senti_val-%f)/%f>= %f and (senti_val-%f)/%f< %f;
			'''%(pg.tbname,avg,std,bins[i],avg,std,bins[i+1]))		
		data[i]=pg.cur.fetchone()[0]

	

	print 'Write results'
	f=os.open('txt\\temp\\'+tbname+'_senti_hist.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'bin,N\n')
	for i in xrange(k):
		os.write(f,'%0.3f,%d\n'%(bins[i],data[i]))

	os.close(f)

def tweet_senti_city_abs(tbname,dbname='tweet_pgh',user='postgres'):
	#export pos_n,neg_n,abs_senti,n by hour of week
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)
	
	print 'Process temp tweet senti'	
	name='dow*24+hour'
	ts=range(24*7)

	print 'Genu-tweet senti_val '
	pg.cur.execute('''
		select avg(senti_val),stddev(senti_val) 
		from %s where auto_tweet is Null;'''%pg.tbname)
	(avg,std)=pg.cur.fetchall()[0]
	print '-- avg %0.4f std %0.4f'%(avg,std)
	(pos_b,neg_b)=(avg+std,avg-std)	

	print 'Export data senti'
	print '-- happy tweets'
	pg.cur.execute('''
		select %s,count(*),sum((senti_val-%0.4f)/%0.4f) from %s where auto_tweet is Null
		and senti_val>%0.3f group by %s order by %s;'''%(name,avg,std,tbname,pos_b,name,name))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	pos_senti=[data[i][1:] for i in xrange(n)]
	pos_t=[data[i][0] for i in xrange(n)] 
	pos_dict=init_clusters(n,pos_t,pos_senti)

	print '-- unhappy tweets'
	pg.cur.execute('''
		select %s,count(*),sum((senti_val-%0.4f)/%0.4f) from %s where auto_tweet is Null
		and senti_val<%0.3f group by %s order by %s;'''%(name,avg,std,tbname,neg_b,name,name))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	neg_senti=[data[i][1:] for i in xrange(n)]
	neg_t=[data[i][0] for i in xrange(n)] 
	neg_dict=init_clusters(n,neg_t,neg_senti)

	pg.cur.execute('''
		select %s,count(*) from %s where auto_tweet is Null
		group by %s order by %s;'''%(name,tbname,name,name))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	n_n=[data[i][1] for i in xrange(n)]
	n_t=[data[i][0] for i in xrange(n)]	
	n_dict=init_clusters(n,n_t,n_n)
	
	tweet_ts=[[0,0,0.0,0] for i in xrange(len(ts))]

	for i in xrange(len(ts)):
		if ts[i] in pos_dict:
			tweet_ts[i][0]=pos_dict[ts[i]][0]
			tweet_ts[i][2]+=pos_dict[ts[i]][1]
		if ts[i] in neg_dict:
			tweet_ts[i][1]=neg_dict[ts[i]][0]
			tweet_ts[i][2]-=neg_dict[ts[i]][1] #neg_senti is negative
		if ts[i] in n_dict:
			tweet_ts[i][3]=n_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\temp\\'+tbname+'_senti_city_abs_how.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'how,pos_n,neg_n,abs_senti,n\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,%d,%d,%0.3f,%d\n'%(ts[i],tweet_ts[i][0],tweet_ts[i][1],\
			tweet_ts[i][2],tweet_ts[i][3]))

	os.close(f)	



'''--- GENU_TWEET CLUSTER TEMP SENTI---'''
def tweet_senti_cls(fname,cls): 
	#n,sum_senti,pos_n,neg_n of happy tweet clusters by date
	print 'Extract tweet clusters data'
	[tweet_dict,start,end]=extract_tweet_senti_cls(fname,cls)

	print 'Process tweet temp senti'
	day_n=(max(end)-min(start)).days
	ts=[min(start)+dt.timedelta(days=i) for i in xrange(day_n)]
	tweet_ts=init_clusters(len(cls),cls)
	for cl in cls:
		tweet_ts[cl]=process_tweet_senti_cls(ts,tweet_dict[cl])

	print 'Write results'
	f=os.open('txt\\temp\\'+fname+'_senti.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,,,'%cl for cl in cls])+'\n')
	os.write(f,'date,'+','.join(['n,sum_senti,pos_n,neg_n' for cl in cls])+'\n')
	for i in xrange(day_n):
		os.write(f,'%s,'%ts[i].strftime("%Y-%m-%d")+','.join(['%d,%0.3f,%d,%d'%(
			tweet_ts[cl][i][0],tweet_ts[cl][i][1],tweet_ts[cl][i][2],tweet_ts[cl][i][3]
			) for cl in cls])+'\n')

	os.close(f)

def tweet_senti_cls_scale(fname,cls,scale):
	#n,sum_senti,pos_n,neg_n of happy tweet clusters by doy/how
	print 'Extract tweet clusters data'
	[tweet_dict,start,end]=extract_tweet_senti_cls(fname,cls,scale)

	print 'Process tweet temp senti'
	if scale=='how':
		name='dow*24+hour'
		ts=range(24*7)
	else:
		name=scale
		ts=range(1,366)

	tweet_ts=init_clusters(len(cls),cls)
	for cl in cls:
		tweet_ts[cl]=process_tweet_senti_cls(ts,tweet_dict[cl])

	print 'Write results'
	f=os.open('txt\\temp\\'+fname+'_senti_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,,,'%cl for cl in cls])+'\n')
	os.write(f,'%s,'%scale+','.join(['n,sum_senti,pos_n,neg_n' for cl in cls])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,'%ts[i]+','.join(['%d,%0.3f,%d,%d'%(
			tweet_ts[cl][i][0],tweet_ts[cl][i][1],tweet_ts[cl][i][2],tweet_ts[cl][i][3]
			) for cl in cls])+'\n')

	os.close(f)

def tweet_senti_cls_norm(fname,cls):
	#user-normalized n,sum_senti,pos_n,neg_n of happy tweet clusters by date
	print 'Extract tweet clusters data'
	[tweet_dict,start,end]=extract_tweet_senti_norm_cls(fname,cls)

	print 'Process tweet temp senti'
	day_n=(max(end)-min(start)).days
	ts=[min(start)+dt.timedelta(days=i) for i in xrange(day_n)]
	tweet_ts=init_clusters(len(cls),cls)
	for cl in cls:
		tweet_ts[cl]=process_tweet_senti_cls(ts,tweet_dict[cl])

	print 'Write results'
	f=os.open('txt\\temp\\'+fname+'_senti_norm.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,,,'%cl for cl in cls])+'\n')
	os.write(f,'date,'+','.join(['n,sum_senti,pos_n,neg_n' for cl in cls])+'\n')
	for i in xrange(day_n):
		os.write(f,'%s,'%ts[i].strftime("%Y-%m-%d")+','.join(['%d,%0.3f,%0.3f,%0.3f'%(
			tweet_ts[cl][i][0],tweet_ts[cl][i][1],tweet_ts[cl][i][2],tweet_ts[cl][i][3]
			) for cl in cls])+'\n')

	os.close(f)

def tweet_senti_cls_norm_scale(fname,cls,scale):
	#user-normalized n,sum_senti,pos_n,neg_n of happy tweet clusters by doy/how
	print 'Extract tweet clusters data'
	[tweet_dict,start,end]=extract_tweet_senti_norm_cls(fname,cls,scale)

	print 'Process tweet temp senti'
	if scale=='how':
		name='dow*24+hour'
		ts=range(24*7)
	else:
		name=scale
		ts=range(1,366)

	tweet_ts=init_clusters(len(cls),cls)
	for cl in cls:
		tweet_ts[cl]=process_tweet_senti_cls(ts,tweet_dict[cl])

	print 'Write results'
	f=os.open('txt\\temp\\'+fname+'_senti_norm_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,,,'%cl for cl in cls])+'\n')
	os.write(f,'%s,'%scale+','.join(['n,sum_senti,pos_n,neg_n' for cl in cls])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,'%ts[i]+','.join(['%d,%0.3f,%0.3f,%0.3f'%(
			tweet_ts[cl][i][0],tweet_ts[cl][i][1],tweet_ts[cl][i][2],tweet_ts[cl][i][3]
			) for cl in cls])+'\n')

	os.close(f)

def tweet_senti_cls_norm_abs(fname,cls):
	#user-normalized n,abs_senti,pos_n,neg_n of happy/unhappy tweet clusters by date	
	print 'Extract tweet clusters data'
	[tweet_dict,start,end]=extract_tweet_senti_norm_cls(fname,cls,'how',abs_senti=True)

	print 'Process tweet temp senti'
	name='dow*24+hour'
	ts=range(24*7)

	tweet_ts=init_clusters(len(cls),cls)
	for cl in cls:
		tweet_ts[cl]=process_tweet_senti_cls(ts,tweet_dict[cl])

	print 'Write results'
	f=os.open('txt\\temp\\'+fname+'_senti_norm_abs_how.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,,,'%cl for cl in cls])+'\n')
	os.write(f,'how,'+','.join(['n,abs_senti,pos_n,neg_n' for cl in cls])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,'%ts[i]+','.join(['%d,%0.3f,%0.3f,%0.3f'%(
			tweet_ts[cl][i][0],tweet_ts[cl][i][1],tweet_ts[cl][i][2],tweet_ts[cl][i][3]
			) for cl in cls])+'\n')

	os.close(f)



'''--- GENU_TWEET CLUSTER SENTI ANALYSIS---'''
def tweet_senti_cls_stats(fname,sep='\t'):
	print 'Extract senti_val by clusters'
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	senti=[float(data[i][-2]) for i in xrange(n)]
	happy=[int(data[i][-1]) for i in xrange(n)]

	cls=range(max(clid)+1)
	senti_cls=init_clusters(len(cls),cls)
	for cl in cls:
		senti_cls[cl]=[[senti[i] for i in xrange(n) if clid[i]==cl],
						[happy[i] for i in xrange(n) if clid[i]==cl and happ[i]==1],
						[happy[i] for i in xrange(n) if clid[i]==cl and happ[i]==-1]]

	print 'Write results'
	write_tweet_senti_cls_stats(fname+'_senti_sum',senti_cls)
	
	print 'Plot senti-hist by clusters'
	for cl in cls:
		plt.figure()
		bins=np.linspace(-4,3,15)
		plt.hist(senti_cls[cl][0],bins)		
		plt.title('Tweet Sentiment Histogram of Cluster%d'%cl)
		plt.savefig('png\\senti_cls\\'+fname+'_senti_cl%d.png'%cl)
		plt.close()

def tweet_senti_cls_user(tbname,senti,cls,m,dbname='tweet_pgh',user='postgres'):
	#export username with most happy/unhappy tweets of clusters
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)
	
	print 'Export most-tweet users'
	user_cl=init_clusters(len(cls),cls)
	for cl in cls:
		pg.cur.execute('''
			select username,count(*) from %s where clid=%d and senti_bool=%d
			group by username order by count(*) desc;'''%(pg.tbname,cl,senti))
		data=pg.cur.fetchall()
		n=pg.cur.rowcount
		if n<m:
			user_cl[cl]=data+[('',0) for i in xrange(n,m)]
		else:
			user_cl[cl]=data[:m]

	print 'Write results'
	ext='pos' if senti==1 else 'neg'
	f=os.open('txt\\temp\\'+tbname+'_user_%s.txt'%ext,os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,'%cl for cl in cls])+'\n')
	for i in xrange(m):
		os.write(f,'%d,'%(i+1)+','.join(['%s,%d'\
			%(user_cl[cl][i][0],user_cl[cl][i][1]) for cl in cls])+'\n')

	os.close(f)

def tweet_senti_norm_cls_stats(fname,sep='\t'): #normalize senti_val by #_tweet of each user
	print 'Extract senti_val and user by clusters'
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	idx=var.index('user')

	n-=1
	data=data[1:]
	clid=[int(data[i][0]) for i in xrange(n)]
	senti=[float(data[i][-2]) for i in xrange(n)]
	happy=[int(data[i][-1]) for i in xrange(n)]
	user=[data[i][idx] for i in xrange(n)]

	cls=range(max(clid)+1)
	senti_cls=init_clusters(len(cls),cls)
	for cl in cls:
		senti_cls[cl]=[[senti[i] for i in xrange(n) if clid[i]==cl],
						[happy[i] for i in xrange(n) if clid[i]==cl],
						[user[i] for i in xrange(n) if clid[i]==cl]]

	print 'Process cluster senti'
	for cl in cls:
		print '-- cluster%d'%cl
		senti_cls[cl]=tweet_senti_norm(senti_cls[cl])

	print 'Write results'
	write_tweet_senti_cls_stats(fname+'_senti_norm_sum',senti_cls)

def tweet_senti_cls_ttest(fname,sample,cls=False,iters=10,pthresh=0.1,sep='\t'): 
	#test if senti means are significantly diff
	#'N' means Hypothesis mean_diff=0 is rejected
	#'Y' means Hypothesis mean_diff=0 is not rejected
	print 'Extract senti_val and user by clusters'
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	idx=var.index('user')

	n-=1
	data=data[1:]
	clid=[int(data[i][0]) for i in xrange(n)]
	senti=[float(data[i][-2]) for i in xrange(n)]
	happy=[int(data[i][-1]) for i in xrange(n)]
	user=[data[i][idx] for i in xrange(n)]

	if not cls:
		cls=range(max(clid)+1)
	senti_cls=init_clusters(len(cls),cls)
	for cl in cls:
		senti_cls[cl]=[[senti[i] for i in xrange(n) if clid[i]==cl],
						[happy[i] for i in xrange(n) if clid[i]==cl],
						[user[i] for i in xrange(n) if clid[i]==cl]]

	print 'Process cluster senti'
	for cl in cls:
		print '-- cluster%d'%cl
		senti_cls[cl]=tweet_senti_norm(senti_cls[cl])[0]
	
	senti_avg=[np.mean(senti_cls[cl]) for cl in cls]
	order=list(np.argsort(senti_avg)[::-1])

	print 'Iteratively sampling tweets and calculate t stats'
	k=len(cls)
	cls=[cls[order[i]] for i in xrange(k)]
	samp_cls=init_clusters(k,cls,[np.zeros((sample,)) for j in xrange(k)])
	for i in xrange(iters):
		idx=np.random.permutation(sample)
		for cl in cls:
			samp_cls[cl]=np.add(samp_cls[cl],[senti_cls[cl][i] for i in idx])

	for cl in cls:
		samp_cls[cl]=list(1.0*samp_cls[cl]/iters)

	mtx_t=np.zeros((k,k))
	mtx_p=np.zeros((k,k))
	for i in xrange(k):
		for j in xrange(i+1,k):
			(mtx_t[i][j],mtx_p[i][j])=stats.ttest_ind(samp_cls[cls[i]],samp_cls[cls[j]])

	print 'Write results'
	f=os.open('txt\\temp\\'+fname+'_senti_ttest.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['%d,'%cl for cl in cls[1:]])+'\n')
	for i in xrange(k):
		os.write(f,'%d,'%cls[i]+''.join([',,' for j in xrange(i)])+\
			''.join(['%0.4f,%0.4f,'%(mtx_t[i][j],mtx_p[i][j]) for j in xrange(i+1,k)])+'\n')

	os.write(f,'\n')

	mtx_h=[['' for i in xrange(k)] for j in xrange(k)]
	for i in xrange(k):
		for j in xrange(i+1,k):
			if mtx_p[i][j]>pthresh:
				mtx_h[i][j]='Y'
			else:
				mtx_h[i][j]='N'

	os.write(f,','+','.join(['%d'%cl for cl in cls[1:]])+'\n')	
	for i in xrange(k):
		os.write(f,'%d,'%cls[i]+','.join(mtx_h[i][1:])+'\n')


	os.close(f)





'''--- VENUE TEMP CHECKIN ANALYSIS---'''
def venue_checkin_cls(fname,cls):
	print 'Extract venue checkin data'
	[checkin_dict,start,end]=extract_venue_checkin_cls(fname,cls)

	print 'Process venue temp checkin'
	day_n=(max(end)-min(start)).days
	ts=[min(start)+dt.timedelta(days=i) for i in xrange(day_n)]
	checkin_ts=init_clusters(len(cls),cls)
	for cl in cls:
		checkin_ts[cl]=process_venue_checkin_cls(ts,checkin_dict[cl])	

	print 'Write results'
	f=os.open('txt\\venue\\'+fname+'_checkin.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,,,'%cl for cl in cls])+'\n')
	os.write(f,'date,'+','.join(['n,senti,pos_n,neg_n' for cl in cls])+'\n')
	for i in xrange(day_n):
		os.write(f,'%s,'%ts[i].strftime("%Y-%m-%d")+','.join(['%d,%0.3f,%d,%d'%(
			checkin_ts[cl][i][0],checkin_ts[cl][i][1],checkin_ts[cl][i][2],checkin_ts[cl][i][3]
			) for cl in cls])+'\n')

	os.close(f)

def venue_checkin_cls_scale(fname,cls,scale):
	print 'Extract venue checkin data'
	checkin_dict=extract_venue_checkin_cls(fname,cls,scale)

	print 'Process venue temp checkin'
	if scale=='how':
		ts=range(24*7)
	else:
		ts=range(1,366)
	checkin_ts=init_clusters(len(cls),cls)
	for cl in cls:
		checkin_ts[cl]=process_venue_checkin_cls(ts,checkin_dict[cl])	

	print 'Write results'
	f=os.open('txt\\venue\\'+fname+'_checkin_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,'%scale+','.join(['cluster%d'%cl for cl in cls])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,'%ts[i]+','.join(['%d'%(checkin_ts[cl][i]) for cl in cls])+'\n')

	os.close(f)


def checkin_corroef_city_scale(fn_senti,fn_n,scale):
	print 'Extract city temp senti'
	#temp senti
	data=file('txt\\temp\\'+fn_senti+'_%s.txt'%scale).readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	pos_n=[int(data[i][1]) for i in xrange(n)]
	neg_n=[int(data[i][2]) for i in xrange(n)]
	senti=[float(data[i][-2]) for i in xrange(n)]

	print 'Extract city tweet and checkin'
	#temp n
	data=file('txt\\temp\\'+fn_n+'_%s.txt'%scale).readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	tweet=[int(data[i][1]) for i in xrange(n)]
	checkin=[int(data[i][2])+int(data[i][3]) for i in xrange(n)]

	senti_avg=[0 for i in xrange(n)]
	pos_p=[0 for i in xrange(n)]
	neg_p=[0 for i in xrange(n)]
	for i in xrange(n):
		if tweet[i]==0:
			senti_avg[i]=pos_p[i]=neg_p[i]=0
		else:
			senti_avg[i]=senti[i]/tweet[i]
			pos_p[i]=1.0*pos_n[i]/tweet[i]
			neg_p[i]=1.0*neg_n[i]/tweet[i]
	
	print 'Calculate corrcoefs and write results'
	f=os.open('txt\\venue\\'+fn_senti+'_corrs_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'sum_senti,avg_senti,pos_n,pos_%,neg_n,neg_%\n')
	os.write(f,','.join(['%0.4f'%np.corrcoef([x,checkin])[0][1] \
		for x in [senti,senti_avg,pos_n,pos_p,neg_n,neg_p]]))
	

def checkin_corroef_cls_scale(fn_senti,fn_checkin,cls,scale):
	print 'Extract cluster temp senti'
	data=file('txt\\temp\\'+fn_senti+'_%s.txt'%scale).readlines()[2:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	k=len(cls)
	senti_cl=init_clusters(k,cls)
	for i in xrange(k):
		tweet=[int(data[j][1+4*i]) for j in xrange(n)]
		senti=[float(data[j][2+4*i]) for j in xrange(n)]
		pos_n=[float(data[j][3+4*i]) for j in xrange(n)]
		neg_n=[float(data[j][4+4*i]) for j in xrange(n)]

		senti_cl[cls[i]]=[tweet,senti,pos_n,neg_n]

	print 'Extract cluster temp checkin'
	data=file('txt\\venue\\'+fn_checkin+'_%s.txt'%scale).readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	checkin_cl=init_clusters(k,cls)
	for i in xrange(k):
		checkin_cl[cls[i]]=[int(data[j][1+i]) for j in xrange(n)]	

	print 'Calculate corrcoef. and write results'
	f=os.open('txt\\venue\\'+fn_senti+'_corrs.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'cluster,sum_senti,avg_senti,pos_n,pos_%,neg_n,neg_%\n')
	#[sum_senti,avg_senti,pos_n,pos_%]
	for i in xrange(k):
		[tweet,senti,pos_n,neg_n]=senti_cl[cls[i]]
		checkin=checkin_cl[cls[i]]

		
		m=len(senti)
		[senti_avg,pos_p,neg_p]=[[0 for j in xrange(m)] for l in xrange(3)]
		for j in xrange(m):
			if tweet[j]==0:
				senti_avg[j]=pos_p[j]=neg_p[j]=0
			else:
				senti_avg[j]=senti[j]/tweet[j]
				pos_p[j]=pos_n[j]/tweet[j]
				neg_p[j]=neg_n[j]/tweet[j]

		if sum(checkin)>0:
			corrs=[np.corrcoef([x,checkin])[0][1] \
			for x in [senti,senti_avg,pos_n,pos_p,neg_n,neg_p]]
		else:
			corrs=[0,0,0,0,0,0]

		os.write(f,'%d,'%cls[i]+','.join(['%0.4f'%corr for corr in corrs])+'\n')

	os.close(f)


'''--- TEMP URBAN USE ANALYSIS---'''
def urban_use_city(fname,fn_cat,uses,scale=False):
	print 'Extract venue category data'
	[use_city,start,end]=extract_urban_use_city(fname,fn_cat,uses,scale)

	print 'Process temp urban use'
	if scale=='how':
		ts=range(24*7)
	elif scale=='doy':
		ts=range(1,366)	
	else:
		day_n=(end-start).days
		ts=[start+dt.timedelta(days=i) for i in xrange(day_n)]
		

	use_ts=process_urban_use(ts,use_city,uses)
	if scale:
		ts=['%d'%t for t in ts]
	else:
		ts=[t.strftime("%Y-%m-%d") for t in ts]
	
	
	print 'Write results'
	fname=fname.replace('_tweet_cls_new','_city_cat')
	if scale:
		ext='_'+scale
	else:
		ext=''
	f=os.open('txt\\venue\\urban-use\\'+fname+'%s.txt'%ext,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,'%ext.replace('_','')+','.join(uses)+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%s,'%ts[i]+','.join(['%d'%(use_ts[use][i]) for use in uses])+'\n')

	os.close(f)


def urban_use_cls(fname,fn_cat,cls,uses,scale):
	print 'Extract venue category data'
	use_cl=extract_urban_use_cls(fname,fn_cat,cls,uses,scale)

	print 'Process temp urban use by cluster'
	if scale=='how':
		ts=range(24*7)
	else:
		ts=range(1,366)

	use_ts=init_clusters(len(cls),cls)
	for cl in cls:
		print '-- Cluster%d'%cl
		use_ts[cl]=process_urban_use(ts,use_cl[cl],uses)	
	
	print 'Write results'
	for cl in cls:
		write_urban_use_cls(fname,cl,ts,use_ts[cl],uses,scale)


def urban_use_corroef_city(fn_senti,fn_use,uses,scale):
	print 'Extract city temp senti'
	#temp senti
	data=file('txt\\temp\\'+fn_senti+'_%s.txt'%scale).readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	pos_n=[int(data[i][1]) for i in xrange(n)]
	neg_n=[int(data[i][2]) for i in xrange(n)]
	senti=[float(data[i][-2]) for i in xrange(n)]
	

	print 'Extract urban use of city'
	#temp n
	data=file('txt\\venue\\urban-use\\'+fn_use+'_%s.txt'%scale).readlines()
	names=data[0][:-1].split(',')
	data=data[1:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	
	k=len(uses)
	use_dict=init_clusters(k,uses)
	for i in xrange(k):
		use_n=[int(data[j][names.index(uses[i])]) for j in xrange(n)]
		if sum(use_n)>0:
			use_dict[uses[i]]=use_n
		else:
			del use_dict[uses[i]]

		
	print 'Calculate corrcoefs and write results'
	f=os.open('txt\\venue\\urban-use\\'+fn_senti+'_corrs_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'use,sum_senti,pos_n,neg_n\n')
	uses=use_dict.keys()
	for use in uses:
		corrs=[np.corrcoef([x,use_dict[use]])[0][1] for x in [senti,pos_n,neg_n]]
		os.write(f,'%s,'%use+','.join(['%0.4f'%corr for corr in corrs])+'\n')


	os.close(f)

def urban_use_corroef_cls(fn_senti,fn_use,uses,cls,scale):
	print 'Extract cls temp senti'
	#temp senti
	data=file('txt\\temp\\'+fn_senti+'_%s.txt'%scale).readlines()	
	names=data[0][:-1].split(',')
	names=[names[i].replace('cluster','') for i in xrange(len(names))]
	idx=[names.index('%d'%cl) for cl in cls]

	data=data[2:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	
	k=len(cls)
	senti_cls=init_clusters(k,cls)
	for j in xrange(k):	
		senti=[float(data[i][idx[j]+1]) for i in xrange(n)]	
		pos_n=[float(data[i][idx[j]+2]) for i in xrange(n)]
		neg_n=[float(data[i][idx[j]+3]) for i in xrange(n)]
		senti_cls[cls[j]]=[senti,pos_n,neg_n]
	
	print 'Extract cls urban use'
	#temp n
	use_cls=init_clusters(k,cls)
	m=len(uses)
	for cl in cls:		
		fname=fn_use.replace('cl','cl%d'%cl)
		data=file('txt\\venue\\urban-use\\'+fname+'_%s.txt'%scale).readlines()
		names=data[0][:-1].split(',')
		data=data[1:]
		n=len(data)
		data=[data[i][:-1].split(',') for i in xrange(n)]
	
		use_dict=init_clusters(m,uses)
		for i in xrange(m):
			use_n=[int(data[j][names.index(uses[i])]) for j in xrange(n)]
			if sum(use_n)>0:
				use_dict[uses[i]]=use_n
			else:
				del use_dict[uses[i]]
		use_cls[cl]=use_dict

	print 'Calculate corrcoefs and write results'
	f=os.open('txt\\venue\\urban-use\\'+fn_senti+'_corrs_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	for cl in cls:
		os.write(f,'cluster%d\n'%cl)
		os.write(f,'use,sum_senti,pos_n,neg_n\n')
		use_dict=use_cls[cl]
		uses=use_dict.keys()
		for use in uses:
			corrs=[np.corrcoef([x,use_dict[use]])[0][1] for x in senti_cls[cl]]
			os.write(f,'%s,'%use+','.join(['%0.4f'%corr for corr in corrs])+'\n')


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

def color_generator(k):
	cache=cache_pre()
	if k<= len(cache):
		color=cache[:k]
		return color
	else:
		print "Number of clusters exceeds cache"
		return 

def cache_pre():
	cache=colors.ColorConverter.cache
	unique_color=[]
	for color in cache:
		if (cache[color] not in unique_color) and (cache[color]!=(0.0,0.0,0.0)) and (cache[color]!=(1.0,1.0,1.0)):
			unique_color+=[color]
	return unique_color

'''------------------------------------------------------------------'''
def export_tweet_all(pg,auto):
	pg.cur.execute('''
		select (date '2014-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2014 and auto_tweet %s
		group by temp order by temp;'''%(pg.tbname,auto))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n=[data[i][-1] for i in xrange(n)]
	temp=[str(data[i][0]) for i in xrange(n)]
	pg.cur.execute('''
		select (date '2015-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2015 and auto_tweet %s
		group by temp order by temp;'''%(pg.tbname,auto))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n+=[data[i][-1] for i in xrange(n)]
	temp+=[str(data[i][0]) for i in xrange(n)]
	tweet_dict=init_clusters(len(temp),temp,tweet_n)

	return tweet_dict

def export_tweet_all_scale(pg,auto,scale):
	pg.cur.execute('''
		select (%s) as temp,count(*) 
		from %s where auto_tweet %s
		group by temp order by temp;'''%(scale,pg.tbname,auto))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n=[data[i][-1] for i in xrange(n)]
	temp=[data[i][0] for i in xrange(n)]
	tweet_dict=init_clusters(len(temp),temp,tweet_n)

	return tweet_dict

'''------------------------------------------------------------------'''
def extract_venue_checkin_cls(fname,cls,scale=False,sep='\t'):
	data=file('txt\\venue\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]

	n=len(data)
	clid=[int(data[i][0]) for i in xrange(n)]
	data_dict=init_clusters(len(cls),cls)
	for cl in cls:
		data_dict[cl]=[data[i] for i in xrange(n) if clid[i]==cl]
		data_dict[cl]=extract_venue_checkin(data_dict[cl],var,scale)

	return data_dict

def extract_venue_checkin(data,var,scale):
	n=len(data)
	if scale=='doy':
		idx=var.index('doy')
		temp=[int(data[i][idx]) for i in xrange(n)]
	elif scale=='how':
		idx1=var.index('dow')
		idx2=var.index('hour')
		temp=[int(data[i][idx1])*24+int(data[i][idx2]) for i in xrange(n)]
	else:
		idx1=var.index('year')
		idx2=var.index('doy')
		year=[int(data[i][idx1]) for i in xrange(n)]
		temp=['2014-01-01' for i in xrange(n)]
		for i in xrange(n):
			if year[i]==2014:
				temp[i]=dt.datetime.strptime(temp[i], "%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)
			else:
				temp[i]=dt.datetime.strptime('2015-01-01',"%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)

	ts=np.sort(list(set(temp)))
	m=len(ts)
	checkin=init_clusters(m,ts)
	for i in xrange(m):
		checkin[ts[i]]=temp.count(ts[i])

	return checkin

def process_venue_checkin_cls(ts,checkin_dict):
	n=len(ts)
	checkin=[0 for i in xrange(n)]
	for i in xrange(n):
		if ts[i] in checkin_dict:
			checkin[i]=checkin_dict[ts[i]]

	return checkin


'''------------------------------------------------------------------'''
def extract_tweet_senti_norm_cls(fname,cls=False,scale=False,abs_senti=False,sep='\t'):
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]

	n=len(data)
	clid=[int(data[i][0]) for i in xrange(n)]

	if not cls:
		cls=range(max(clid)+1)
	data_dict=init_clusters(len(cls),cls)
	start=[]
	end=[]
	for cl in cls:
		print '-- cluster%d norm senti'%cl
		data_dict[cl]=[data[i] for i in xrange(n) if clid[i]==cl]
		[data_dict[cl],sta_cl,end_cl]=extract_tweet_senti_norm(data_dict[cl],var,scale,abs_senti)
		start+=[sta_cl]
		end+=[end_cl]

	return data_dict,start,end

def extract_tweet_senti_norm(data,var,scale,abs_senti):
	n=len(data)
	idx=var.index('user')
	user=[data[i][idx] for i in xrange(n)]
	senti=[float(data[i][-2]) for i in xrange(n)]
	happy=[int(data[i][-1]) for i in xrange(n)]
	[senti,pos,neg]=tweet_senti_norm([senti,happy,user])

	i_pos=0
	i_neg=0
	for i in xrange(n):
		if happy[i]==1:
			happy[i]=pos[i_pos]
			i_pos+=1
		elif happy[i]==-1:
			happy[i]=-1*neg[i_neg]
			i_neg+=1


	if scale=='doy':
		idx=var.index('doy')
		temp=[int(data[i][idx]) for i in xrange(n)]
	elif scale=='how':
		idx1=var.index('dow')
		idx2=var.index('hour')
		temp=[int(data[i][idx1])*24+int(data[i][idx2]) for i in xrange(n)]
	else:
		idx1=var.index('year')
		idx2=var.index('doy')
		year=[int(data[i][idx1]) for i in xrange(n)]
		temp=['2014-01-01' for i in xrange(n)]
		for i in xrange(n):
			if year[i]==2014:
				temp[i]=dt.datetime.strptime(temp[i], "%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)
			else:
				temp[i]=dt.datetime.strptime('2015-01-01',"%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)

	ts=np.sort(list(set(temp)))
	m=len(ts)
	senti_temp=init_clusters(m,ts)
	for i in xrange(m):
		senti_i=[senti[j] for j in xrange(n) if temp[j]==ts[i]]
		pos_i=[happy[j] for j in xrange(n) if temp[j]==ts[i] and happy[j]>0]
		neg_i=[-1*happy[j] for j in xrange(n) if temp[j]==ts[i] and happy[j]<0]
		if abs_senti:			
			senti_i=[abs(senti_i[j]) for j in xrange(len(senti_i))] #abs senti_val

		senti_temp[ts[i]]=[len(senti_i),sum(senti_i),sum(pos_i),sum(neg_i)]

	return senti_temp,ts[0],ts[-1]


def extract_tweet_senti_cls(fname,cls=False,scale=False,sep='\t'):
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]

	n=len(data)
	clid=[int(data[i][0]) for i in xrange(n)]
	if not cls:
		cls=range(max(clid)+1)
	data_dict=init_clusters(len(cls),cls)
	start=[]
	end=[]
	for cl in cls:
		data_dict[cl]=[data[i] for i in xrange(n) if clid[i]==cl]
		[data_dict[cl],sta_cl,end_cl]=extract_tweet_senti(data_dict[cl],var,scale)
		start+=[sta_cl]
		end+=[end_cl]

	return data_dict,start,end

def extract_tweet_senti(data,var,scale):
	n=len(data)
	if scale=='doy':
		idx=var.index('doy')
		temp=[int(data[i][idx]) for i in xrange(n)]
	elif scale=='how':
		idx1=var.index('dow')
		idx2=var.index('hour')
		temp=[int(data[i][idx1])*24+int(data[i][idx2]) for i in xrange(n)]
	else:
		idx1=var.index('year')
		idx2=var.index('doy')
		year=[int(data[i][idx1]) for i in xrange(n)]
		temp=['2014-01-01' for i in xrange(n)]
		for i in xrange(n):
			if year[i]==2014:
				temp[i]=dt.datetime.strptime(temp[i], "%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)
			else:
				temp[i]=dt.datetime.strptime('2015-01-01',"%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)

	ts=np.sort(list(set(temp)))
	m=len(ts)
	senti=init_clusters(m,ts)
	for i in xrange(m):
		senti_i=[float(data[j][-2]) for j in xrange(n) if temp[j]==ts[i]]
		happy_i=[int(data[j][-1]) for j in xrange(n) if temp[j]==ts[i]]
		senti[ts[i]]=[len(senti_i),sum(senti_i),happy_i.count(1),happy_i.count(-1)]

	return senti,ts[0],ts[-1]

def process_tweet_senti_cls(ts,senti_dict):
	n=len(ts)
	senti=[[0,0.0,0,0] for i in xrange(n)]
	for i in xrange(n):
		if ts[i] in senti_dict:
			senti[i]=senti_dict[ts[i]]

	return senti

'''------------------------------------------------------------------'''
def write_tweet_senti_cls_stats(fname,senti_cls):
	f=os.open('txt\\temp\\'+fname+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'cl,pos_%,neg_%,senti_avg,senti_std,senti_25%,senti_50%,senti_75%\n')
	for cl in senti_cls:
		[senti,pos,neg]=senti_cls[cl]
		n=len(senti)
		pos=100.0*sum(pos)/n
		neg=100.0*sum(neg)/n
		avg=np.mean(senti)
		std=np.std(senti)
		q25=np.percentile(senti,25)
		q50=np.percentile(senti,50)
		q75=np.percentile(senti,75)
		os.write(f,'%d,'%cl+','.join(['%0.4f'%var for var in [pos,neg,avg,std,q25,q50,q75]])+'\n')

	os.close(f)

def tweet_senti_norm(senti):
	[senti,happy,user]=senti
	n=len(senti)
	names=list(set(user))
	w=[[1,1] for name in names]
	w=init_clusters(len(names),names,w)

	for name in names:
		pos=sum([1 for i in xrange(n) if user[i]==name and happy[i]==1])
		neg=sum([1 for i in xrange(n) if user[i]==name and happy[i]==-1])
		w[name]=[pos,neg]

	pos90=np.percentile([w[name][0] for name in names if w[name][0]>0],90)
	neg90=np.percentile([w[name][1] for name in names if w[name][1]>0],90)
	print pos90,neg90

	for name in names:
		[pos,neg]=w[name]
		pos=np.exp(pos90-pos) if pos>pos90 else 1
		neg=np.exp(neg90-neg) if neg>neg90 else 1
		w[name]=[pos,neg]

	pos=[]
	neg=[]
	for i in xrange(n):
		if senti[i]>0:
			senti[i]*=w[user[i]][0]
		if senti[i]<0:
			senti[i]*=w[user[i]][1]
		if happy[i]==1:
			pos+=[w[user[i]][0]]
		if happy[i]==-1:
			neg+=[w[user[i]][1]]

	return [senti,pos,neg]


'''------------------------------------------------------------------'''
def extract_urban_use_city(fname,fn_cat,uses,scale,sep='\t'):
	cats=tweet_venue.extract_venue_cat(fn_cat)

	data=file('txt\\venue\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]
	
	return extract_urban_use(data,var,cats,uses,scale)

def extract_urban_use_cls(fname,fn_cat,cls,uses,scale,sep='\t'):
	cats=tweet_venue.extract_venue_cat(fn_cat)

	data=file('txt\\venue\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]

	n=len(data)
	clid=[int(data[i][0]) for i in xrange(n)]
	data_dict=init_clusters(len(cls),cls)
	for cl in cls:
		data_cl=[data[i] for i in xrange(n) if clid[i]==cl]
		data_dict[cl]=extract_urban_use(data_cl,var,cats,uses,scale)[0]

	return data_dict

def extract_urban_use(data,var,cat_dict,uses,scale):
	n=len(data)
	if scale=='doy':
		idx=var.index('doy')
		temp=[int(data[i][idx]) for i in xrange(n)]
	elif scale=='how':
		idx1=var.index('dow')
		idx2=var.index('hour')
		temp=[int(data[i][idx1])*24+int(data[i][idx2]) for i in xrange(n)]
	else:
		idx1=var.index('year')
		idx2=var.index('doy')
		year=[int(data[i][idx1]) for i in xrange(n)]
		temp=['2014-01-01' for i in xrange(n)]
		for i in xrange(n):
			if year[i]==2014:
				temp[i]=dt.datetime.strptime(temp[i], "%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)
			else:
				temp[i]=dt.datetime.strptime('2015-01-01',"%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)

	
	idx=var.index('cat')
	cats=[data[i][idx] for i in xrange(n)]

	for i in xrange(len(cats)):
		if cats[i]!='NA':
			cat=cats[i][cats[i].index('{')+1:-1]
			cats[i]=cat_dict[cat][1][0]

	cat_dict=init_clusters(len(uses),uses)
	for use in uses:
		cat_dict[use]=[temp[i] for i in xrange(n) if cats[i]==use]

	ts=np.sort(list(set(temp)))
	m=len(ts)
	use_dict=init_clusters(len(uses),uses)
	for use in uses:
		use_dict[use]=init_clusters(m,ts)
		for i in xrange(m):			
			use_dict[use][ts[i]]=cat_dict[use].count(ts[i])

	return use_dict,ts[0],ts[-1]


def process_urban_use(ts,use_dict,uses):
	use_ts=init_clusters(len(uses),uses)
	for use in uses:
		use_ts[use]=[0 for t in ts]
		for i in xrange(len(ts)):
			if ts[i] in use_dict[use]:
				use_ts[use][i]=use_dict[use][ts[i]]
	
	return use_ts


def write_urban_use_cls(fname,cl,ts,use_ts,uses,scale):
	fname=fname.replace('_tweet_cls_new','_cl%d_cat'%cl)
	f=os.open('txt\\venue\\urban-use\\'+fname+'_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,'%scale+','.join(uses)+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,'%ts[i]+','.join(['%d'%(use_ts[use][i]) for use in uses])+'\n')

	os.close(f)

'''TEST FUNCTION'''
'''
a=(1,2,3,4)
b=[0]+list(a[1:])
print b

a=[(1,2.0,3),(4,5.0,3),(2,3.0,4)]
a=np.array(a,dtype=[('clid','i'),('senti','f'),('doy','i')])
print np.random.permutation(a)[]

a=np.sort(a,order='doy')
print a
b=np.unique(a['doy'])
print b
print a[np.where(a['doy']==3)]


start=dt.datetime.strptime("2014-01-22", "%Y-%m-%d")
end=dt.datetime.strptime("2014-01-31", "%Y-%m-%d")
		
print type((end-start).days)
print start+dt.timedelta(days=1)


'''