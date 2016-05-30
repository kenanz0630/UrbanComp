import dbProcess, os
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats


'''--- MAIN FUNCTION ---'''
'''--- TWEET_N OF ENTIRE DATASET ---'''
def tweet_n_all(tbname='tweet_pgh',dbname='tweet_pgh',user='postgres'): # tweet_n_all by date
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export data'
	pg.cur.execute('''
		select date_trunc('day',created_at) as temp,count(*) 
		from %s group by temp order by temp;'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n=[data[i][1] for i in xrange(n)]
	temp=[str(data[i][0])[:len("2014-01-01")] for i in xrange(n)] #trunc date
	tweet_dict=init_clusters(n,temp,tweet_n)

	print 'Process data'
	start=dt.datetime.strptime(temp[0], "%Y-%m-%d")
	end=dt.datetime.strptime(temp[-1], "%Y-%m-%d")
	day_n=(end-start).days

	ts=[start+dt.timedelta(days=i) for i in xrange(day_n)]
	ts=[ts[i].strftime("%Y-%m-%d") for i in xrange(day_n)]
	tweet_ts=[0 for i in xrange(day_n)]
	for i in xrange(day_n):
		if ts[i] in tweet_dict:
			tweet_ts[i]=tweet_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_n_all.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,tweet_n\n')
	for i in xrange(day_n):
		os.write(f,'%s,%d\n'%(ts[i],tweet_ts[i]))

	os.close(f)

def tweet_n_all_doy(tbname='tweet_pgh',dbname='tweet_pgh',user='postgres'): # tweet_n_all by doy
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export data'
	pg.cur.execute('''
		select date_part('doy',created_at) as temp,count(*) 
		from %s group by temp order by temp;'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	tweet_n=[data[i][1] for i in xrange(n)]
	temp=[data[i][0] for i in xrange(n)] #trunc date
	tweet_dict=init_clusters(n,temp,tweet_n)


	ts=range(1,366)
	tweet_ts=[0 for i in xrange(365)]
	for i in xrange(365):
		if ts[i] in tweet_dict:
			tweet_ts[i]=tweet_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_n_all_doy.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,tweet_n\n')
	for i in xrange(365):
		os.write(f,'%d,%d\n'%(ts[i],tweet_ts[i]))

	os.close(f)

'''--- TWEET_N OF STUDY DATASET ---'''
def tweet_n_city(tbname,dbname='tweet_pgh',user='postgres'): # tweet_n_city by date
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export data'
	pg.cur.execute('''
		select (date '2014-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2014 group by temp order by temp;'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n=[data[i][-1] for i in xrange(n)]
	temp=[str(data[i][0]) for i in xrange(n)]
	pg.cur.execute('''
		select (date '2015-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2015 group by temp order by temp;'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n+=[data[i][-1] for i in xrange(n)]
	temp+=[str(data[i][0]) for i in xrange(n)]
	tweet_dict=init_clusters(len(temp),temp,tweet_n)

	print 'Process data'
	start=dt.datetime.strptime(temp[0], "%Y-%m-%d")
	end=dt.datetime.strptime(temp[-1], "%Y-%m-%d")
	day_n=(end-start).days

	ts=[start+dt.timedelta(days=i) for i in xrange(day_n)]
	ts=[ts[i].strftime("%Y-%m-%d") for i in xrange(day_n)]
	tweet_ts=[0 for i in xrange(day_n)]
	for i in xrange(day_n):
		if ts[i] in tweet_dict:
			tweet_ts[i]=tweet_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_n_city.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,tweet_n\n')
	for i in xrange(day_n):
		os.write(f,'%s,%d\n'%(ts[i],tweet_ts[i]))

	os.close(f)

def tweet_n_city_scale(tbname,scale,dbname='tweet_pgh',user='postgres'): # tweet_n_city by doy
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export data of time-scale %s'%scale
	if scale=='how':
		name='dow*24+hour'
		ts=range(24*7)
	else:
		name=scale
		ts=range(1,366)
	
	pg.cur.execute('''
		select %s,count(*) from %s group by %s order by %s;'''%(name,tbname,name,name))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	tweet_n=[data[i][1] for i in xrange(n)]
	temp=[data[i][0] for i in xrange(n)] 
	tweet_dict=init_clusters(n,temp,tweet_n)

	tweet_ts=[0 for i in xrange(len(ts))]
	for i in xrange(len(ts)):
		if ts[i] in tweet_dict:
			tweet_ts[i]=tweet_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_n_city_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,tweet_n\n'%scale)
	for i in xrange(len(ts)):
		os.write(f,'%d,%d\n'%(ts[i],tweet_ts[i]))

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
	pos_dict=init_clusters(n,pos_t,pos_n)

	
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
	neg_dict=init_clusters(n,neg_t,neg_n)


	print 'Process temp tweet senti'
	start=dt.datetime.strptime(min(pos_t[0],neg_t[0]), "%Y-%m-%d")
	end=dt.datetime.strptime(max(pos_t[-1],neg_t[-1]), "%Y-%m-%d")
	day_n=(end-start).days

	ts=[start+dt.timedelta(days=i) for i in xrange(day_n)]
	ts=[ts[i].strftime("%Y-%m-%d") for i in xrange(day_n)]
	tweet_ts=[[0,0] for i in xrange(day_n)]
	for i in xrange(day_n):
		if ts[i] in pos_dict:
			tweet_ts[i][0]=pos_dict[ts[i]]
		if ts[i] in neg_dict:
			tweet_ts[i][1]=neg_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_senti_city.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,pos_n,neg_n\n')
	for i in xrange(day_n):
		os.write(f,'%s,%d,%d\n'%(ts[i],tweet_ts[i][0],tweet_ts[i][1]))

	os.close(f)

def tweet_senti_city_scale(tbname,scale,dbname='tweet_pgh',user='postgres'):
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)
	
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

	print 'Process temp tweet senti'	
	if scale=='how':
		name='dow*24+hour'
		ts=range(24*7)
	else:
		name=scale
		ts=range(1,366)
	tweet_ts=[[0,0] for i in xrange(len(ts))]

	for i in xrange(len(ts)):
		if ts[i] in pos_dict:
			tweet_ts[i][0]=pos_dict[ts[i]]
		if ts[i] in neg_dict:
			tweet_ts[i][1]=neg_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_senti_city_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,pos_n,neg_n\n'%scale)
	for i in xrange(len(ts)):
		os.write(f,'%d,%d,%d\n'%(ts[i],tweet_ts[i][0],tweet_ts[i][1]))

	os.close(f)

'''--- GENU_TWEET TEMP SENTI OF CLUSTERS---'''
def tweet_senti_cls(fname,cls): #sum(senti_val) and pos_n of happy tweet clusters by date
	print 'Extract tweet clusters data'
	[tweet_dict,start,end]=extract_tweet_senti_cls(fname,cls)

	print 'Process tweet temp senti'
	day_n=(max(end)-min(start)).days
	ts=[min(start)+dt.timedelta(days=i) for i in xrange(day_n)]
	tweet_ts=init_clusters(len(cls),cls)
	for cl in cls:
		tweet_ts[cl]=process_tweet_cls_senti(ts,tweet_dict[cl])

	print 'Write results'
	f=os.open('txt\\'+fname+'_senti.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,'%cl for cl in cls])+'\n')
	os.write(f,'date,'+','.join(['senti,pos_n' for cl in cls])+'\n')
	for i in xrange(day_n):
		os.write(f,'%s,'%ts[i].strftime("%Y-%m-%d")+','.join(['%0.3f,%d'%(tweet_ts[cl][i][0],tweet_ts[cl][i][1]) for cl in cls])+'\n')

	os.close(f)

def tweet_senti_cls_scale(fname,cls,scale):
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
		tweet_ts[cl]=process_tweet_cls_senti(ts,tweet_dict[cl])

	print 'Write results'
	f=os.open('txt\\'+fname+'_senti_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,'%cl for cl in cls])+'\n')
	os.write(f,'%s,'%scale+','.join(['senti,pos_n' for cl in cls])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,'%ts[i]+','.join(['%0.3f,%d'%(tweet_ts[cl][i][0],tweet_ts[cl][i][1]) for cl in cls])+'\n')

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
def extract_tweet_senti_cls(fname,cls,scale=False,sep='\t'):
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	var=data[0]
	data=data[1:]

	n=len(data)
	clid=[int(data[i][0]) for i in xrange(n)]
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
				temp[i]=dt.datetime.strptime(temp[i], "%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2]))
			else:
				temp[i]=dt.datetime.strptime('2015-01-01',"%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2]))

	ts=np.sort(list(set(temp)))
	m=len(ts)
	senti=init_clusters(m,ts)
	for i in xrange(m):
		senti_i=[float(data[j][-1]) for j in xrange(n) if temp[j]==ts[i]]
		senti[ts[i]]=[sum(senti_i),len(senti_i)]

	return senti,ts[0],ts[-1]

def process_tweet_cls_senti(ts,senti_dict):
	n=len(ts)
	senti=[[0.0,0] for i in xrange(n)]
	for i in xrange(n):
		if ts[i] in senti_dict:
			senti[i]=senti_dict[ts[i]]

	return senti

'''------------------------------------------------------------------'''




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