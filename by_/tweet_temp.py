import dbProcess, os, copy
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats


'''--- MAIN FUNCTION ---'''
'''--- AUTO/GENU TWEET_N AT CITY LEVEL ---'''
def tweet_n_city(tbname,autos=['non','4sq','inst','job'],dbname='pittsburgh',user='postgres'):
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
	f=os.open('txt\\'+tbname+'_n_city.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,'+','.join(['%s_n'%auto for auto in autos])+'\n')
	for i in xrange(day_n):
		os.write(f,'%s,'%ts[i]+','.join(['%d'%tweet_ts[i][j] for j in xrange(k)])+'\n')

	os.close(f)

def tweet_n_city_scale(tbname,scale,autos=['non','4sq','inst','job'],dbname='pittsburgh',user='postgres'):
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
	f=os.open('txt\\'+tbname+'_n_city_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,'%scale+','.join(['%s_n'%auto for auto in autos])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%s,'%ts[i]+','.join(['%d'%tweet_ts[i][j] for j in xrange(k)])+'\n')

	os.close(f)

'''--- GENU TWEET_N AT CITY LEVEL---'''
"""
def tweet_n_city(tbname,dbname='pittsburgh',user='postgres'): # tweet_n_city by date
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export data'
	pg.cur.execute('''
		select (date '2014-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2014 and auto_tweet is Null
		group by temp order by temp;'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	tweet_n=[data[i][-1] for i in xrange(n)]
	temp=[str(data[i][0]) for i in xrange(n)]
	pg.cur.execute('''
		select (date '2015-01-01'+ doy -1) as temp,count(*) 
		from %s where year=2015 and auto_tweet is Null
		group by temp order by temp;'''%tbname)
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

def tweet_n_city_scale(tbname,scale,dbname='pittsburgh',user='postgres'): # tweet_n_city by doy
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
		select %s,count(*) from %s where auto_tweet is Null
		group by %s order by %s;'''%(name,tbname,name,name))
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
"""
'''--- GENU_TWEET TEMP SENTI AT CITY-LEVEL---'''
def tweet_senti_city(tbname,dbname='pittsburgh',user='postgres'):
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
		select (date '2014-01-01'+ doy -1) as temp,sum(senti_val) 
		from %s where year=2014 and auto_tweet is Null
		group by temp order by temp;'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	senti=[data[i][-1] for i in xrange(n)]
	senti_t=[str(data[i][0]) for i in xrange(n)]
	pg.cur.execute('''
		select (date '2015-01-01'+ doy -1) as temp,sum(senti_val) 
		from %s where year=2015 and auto_tweet is Null
		group by temp order by temp;'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	senti+=[data[i][-1] for i in xrange(n)]
	senti_t+=[str(data[i][0]) for i in xrange(n)]

	senti_dict=init_clusters(len(senti_t),senti_t,senti)

	print 'Process temp tweet senti'
	start=dt.datetime.strptime(min(pos_t[0],neg_t[0],senti_t[0]), "%Y-%m-%d")
	end=dt.datetime.strptime(max(pos_t[-1],neg_t[-1],senti_t[-1]), "%Y-%m-%d")
	day_n=(end-start).days

	ts=[start+dt.timedelta(days=i) for i in xrange(day_n)]
	ts=[ts[i].strftime("%Y-%m-%d") for i in xrange(day_n)]
	tweet_ts=[[0,0,0.0] for i in xrange(day_n)]
	for i in xrange(day_n):
		if ts[i] in pos_dict:
			tweet_ts[i][0]=pos_dict[ts[i]]
		if ts[i] in neg_dict:
			tweet_ts[i][1]=neg_dict[ts[i]]
		if ts[i] in senti_dict:
			tweet_ts[i][2]=senti_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_senti_city.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'date,pos_n,neg_n,sum_senti\n')
	for i in xrange(day_n):
		os.write(f,'%s,%d,%d,%0.3f\n'%(ts[i],tweet_ts[i][0],tweet_ts[i][1],tweet_ts[i][2]))

	os.close(f)

def tweet_senti_city_scale(tbname,scale,dbname='pittsburgh',user='postgres'):
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
		select %s,sum(senti_val) from %s where auto_tweet is Null
		group by %s order by %s;'''%(name,tbname,name,name))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount	
	senti=[data[i][1] for i in xrange(n)]
	senti_t=[data[i][0] for i in xrange(n)]
	senti_dict=init_clusters(len(senti_t),senti_t,senti)

	
	tweet_ts=[[0,0,0.0] for i in xrange(len(ts))]

	for i in xrange(len(ts)):
		if ts[i] in pos_dict:
			tweet_ts[i][0]=pos_dict[ts[i]]
		if ts[i] in neg_dict:
			tweet_ts[i][1]=neg_dict[ts[i]]
		if ts[i] in senti_dict:
			tweet_ts[i][2]=senti_dict[ts[i]]

	print 'Write results'
	f=os.open('txt\\'+tbname+'_senti_city_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,pos_n,neg_n,sum_senti\n'%scale)
	for i in xrange(len(ts)):
		os.write(f,'%d,%d,%d,%0.3f\n'%(ts[i],tweet_ts[i][0],tweet_ts[i][1],tweet_ts[i][2]))

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
		tweet_ts[cl]=process_tweet_senti_cls(ts,tweet_dict[cl])

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
		tweet_ts[cl]=process_tweet_senti_cls(ts,tweet_dict[cl])

	print 'Write results'
	f=os.open('txt\\'+fname+'_senti_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['cluster%d,'%cl for cl in cls])+'\n')
	os.write(f,'%s,'%scale+','.join(['senti,pos_n' for cl in cls])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,'%ts[i]+','.join(['%0.3f,%d'%(tweet_ts[cl][i][0],tweet_ts[cl][i][1]) for cl in cls])+'\n')

	os.close(f)

'''--- GENU_TWEET TEMP SENTI NORMALIZATION ---'''

def tweet_senti_cls_norm(fn_cls,fn_city,scale=False):
	print 'Extract senti data'
	if scale:
		name='_%s'%scale
	else:
		name=''
	senti_cls=extract_temp_senti_cls(fn_cls+name)
	senti_city=extract_temp_senti_city(fn_city+name)

	print 'Process senti data'
	for cl in senti_cls:
		senti_cls[cl]=tweet_senti_norm(senti_cls[cl],senti_city)

	print 'Write norm-senti data'
	f=os.open('txt\\'+fn_cls+name+'_norm.txt',os.O_RDWR|os.O_CREAT)
	cls=np.sort(senti_cls.keys())
	os.write(f,','+','.join(['cluster%d,'%cl for cl in cls])+'\n')
	if not scale:
		scale='date'
	os.write(f,'%s,'%scale+','.join(['senti_norm,pos_norm' for cl in cls])+'\n')
	n=len(senti_city)
	for i in xrange(n):
		os.write(f,'%s,'%senti_city[i][0]+','.join(['%0.5f,%0.5f'%(senti_cls[cl][i][1],senti_cls[cl][i][2]) for cl in cls])+'\n')

	os.close(f)

'''--- VENUE TEMP CHECKIN ---'''
def venue_checkin_cls(fname,cls,scale):
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
	fname=fname[:-6]
	f=os.open('txt\\auto-tweet\\'+fname+'_checkin_%s.txt'%scale,os.O_RDWR|os.O_CREAT)
	os.write(f,'%s,'%scale+','.join(['cluster%d'%cl for cl in cls])+'\n')
	for i in xrange(len(ts)):
		os.write(f,'%d,'%ts[i]+','.join(['%d'%(checkin_ts[cl][i]) for cl in cls])+'\n')

	os.close(f)

'''--- VENUE TEMP CHECKIN NORMALIZATION ---'''
def venue_checkin_cls_norm(fn_cls,fn_city,scale):
	print 'Extract checkin data'
	checkin_cls=extract_temp_checkin_cls(fn_cls+'_%s'%scale)
	checkin_city=extract_temp_checkin_city(fn_city+'_%s'%scale)

	print 'Process checkin data'
	for cl in checkin_cls:
		checkin_cls[cl]=venue_checkin_norm(checkin_cls[cl],checkin_city)

	print 'Write norm-checkin data'
	f=os.open('txt\\auto-tweet\\'+fn_cls+'_%s_norm.txt'%scale,os.O_RDWR|os.O_CREAT)
	cls=np.sort(checkin_cls.keys())
	os.write(f,'%s,'%scale+','.join(['cluster%d'%cl for cl in cls])+'\n')
	n=len(checkin_city)
	for i in xrange(n):
		os.write(f,'%s,'%checkin_city[i][0]+','.join(['%0.5f'%(checkin_cls[cl][i][-1]) for cl in cls])+'\n')

	os.close(f)


'''--- CORROEF OF TEMP SENTI AND CHECKIN ---'''
def corroef_city(fn_senti,fn_n,auto,scale):
	#temp senti
	data=file('txt\\'+fn_senti+'_%s.txt'%scale).readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	senti=[int(data[i][1]) for i in xrange(n)]

	#temp n
	data=file('txt\\'+fn_n+'_%s.txt'%scale).readlines()
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	var=data[0]	
	idx=var.index(auto+'_n')
	data=data[1:]
	n-=1

	norm=[int(data[i][1]) for i in xrange(n)]
	for i in xrange(n):
		if norm[i]==0:
			senti[i]=0
		else:
			senti[i]=1.0*senti[i]/norm[i]
	checkin=[int(data[i][idx]) for i in xrange(n)]
	
	print 'Corroef >>'
	print 'city-level %s %s'%(scale,auto),np.corrcoef([senti,checkin])[0][1]


def corroef_cls(fn_senti,fn_checkin,cls,names,scales,autos):	
	[m,n,k]=[len(autos),len(scales),len(names)]
	corrs=[[0 for i in xrange(m*n)] for p in xrange(k)]
	auto_0=autos[0]
	for i in xrange(m):
		fn_checkin=fn_checkin.replace(auto_0,autos[i])
		auto_0=autos[i]
		for j in xrange(n):
			print 'Corroef >> cluster-level %s %s >>'%(scales[j],autos[i])	
			print '-- Extract cluster temp senti'
			checkin_cl=extract_temp_checkin_cls(fn_checkin+'_%s_norm'%scales[j])	

			print '-- Extract cluster venue checkin'
			senti_cl=extract_temp_senti_cls(fn_senti+'_%s_norm'%scales[j])	

			print '-- Calculate corroef'			
			for p in xrange(k):
				N=len(checkin_cl[cls[p]])
				checkin=[float(checkin_cl[cls[p]][q][2]) for q in xrange(N)]
				senti=[float(senti_cl[cls[p]][q][1]) for q in xrange(N)]
				corrs[p][i*m+j]=np.corrcoef([senti,checkin])[0][1]
				
	print 'Write results'
	f=os.open('txt\\'+fn_senti+'_corrs.txt',os.O_RDWR|os.O_CREAT)
	pairs=[]
	for auto in autos:
		pairs+=['%s-%s'%(auto,scale) for scale in scales]
	os.write(f,','.join(['cluster,']+pairs)+'\n')
	for p in xrange(k):
		os.write(f,'%d,%s,'%(cls[p],names[p])+','.join(['%0.5f'%corr for corr in corrs[p]])+'\n')

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
def extract_venue_checkin_cls(fname,cls,scale,sep='\t'):
	data=file('txt\\auto-tweet\\'+fname+'.txt').readlines()
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
				temp[i]=dt.datetime.strptime(temp[i], "%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)
			else:
				temp[i]=dt.datetime.strptime('2015-01-01',"%Y-%m-%d")+dt.timedelta(days=int(data[i][idx2])-1)

	ts=np.sort(list(set(temp)))
	m=len(ts)
	senti=init_clusters(m,ts)
	for i in xrange(m):
		senti_i=[float(data[j][-1]) for j in xrange(n) if temp[j]==ts[i]]
		senti[ts[i]]=[sum(senti_i),len(senti_i)]

	return senti,ts[0],ts[-1]

def process_tweet_senti_cls(ts,senti_dict):
	n=len(ts)
	senti=[[0.0,0] for i in xrange(n)]
	for i in xrange(n):
		if ts[i] in senti_dict:
			senti[i]=senti_dict[ts[i]]

	return senti

'''------------------------------------------------------------------'''
def extract_temp_senti_cls(fname):
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	k=len(data[0])/2
	cls=[int(data[0][1+2*i].replace('cluster','')) for i in xrange(k)]
	data=data[2:]
	n=len(data)

	data_cl=init_clusters(k,cls)
	for cl in cls:
		idx=cls.index(cl)
		data_cl[cl]=[[data[i][0],data[i][1+2*idx],data[i][2+2*idx]] for i in xrange(n)]

	return data_cl

def extract_temp_senti_city(fname):
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	data=[[data[i][0],int(data[i][1])] for i in xrange(n)]

	return data

def tweet_senti_norm(cl,city):	
	if len(cl)!=len(city):
		print 'Error: diff data size'
	else:
		n=len(cl)
		for i in xrange(n):
			if cl[i][0]!=city[i][0]:
				print 'Error: diff temp notation'
			elif city[i][1]==0:
				cl[i][1]=0
				cl[i][2]=0
			else:
				cl[i][1]=float(cl[i][1])/city[i][1]
				cl[i][2]=1.0*int(cl[i][2])/city[i][1]

	return cl

'''------------------------------------------------------------------'''
def extract_temp_checkin_cls(fname):
	data=file('txt\\auto-tweet\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	k=len(data[0])-1
	cls=[int(data[0][1+i].replace('cluster','')) for i in xrange(k)]
	data=data[1:]
	n=len(data)

	data_cl=init_clusters(k,cls)
	for cl in cls:
		idx=cls.index(cl)
		data_cl[cl]=[[data[i][0],0,data[i][1+idx]] for i in xrange(n)]

	return data_cl

def extract_temp_checkin_city(fname):
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	var=data[0]
	if '4sq' in fname:
		idx=var.index('4sq_n')
	else:
		idx=var.index('inst_n')

	data=data[1:]
	n-=1
	data=[[data[i][0],int(data[i][idx])] for i in xrange(n)]

	return data

def venue_checkin_norm(cl,city):	
	
	return tweet_senti_norm(cl,city)
	



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