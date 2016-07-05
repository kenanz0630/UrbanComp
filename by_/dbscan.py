import math, os, copy, shapefile, dbProcess
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.cm as cmx
import numpy as np
from sklearn.cluster import DBSCAN
from collections import Counter



'''MAIN FUNCTION'''
def dbscan_auto_tweet(fn_cls,fn_auto,eps,minPts,sep='\t'): #assign auto-tweet to sup clusters
	print 'Extract auto tweets'
	data=file('txt\\venue\\'+fn_auto+'.txt').readlines()[1:]
	n=len(data)
	data_auto=[data[i][:-1].split(sep) for i in xrange(n)]
	coordi_auto=[[float(data_auto[i][1]),float(data_auto[i][2])] for i in xrange(n)]
	clid_auto=[-1 for i in xrange(n)]
	
	print 'Extract tweet-cls'
	data=file('txt\\'+fn_cls+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	data=[data[i] for i in xrange(n) if int(data[i][0])>-1]
	n=len(data)
	coordi=[[float(data[i][1]),float(data[i][2])] for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	k=max(clid)
	coordi_cl=init_clusters(k,range(k))
	for i in xrange(k):
		coordi_cl[i]=[coordi[j] for j in xrange(n) if clid[j]==i]
	
	print 'Assign auto-tweet to sup-clusters through DBSCAN Clustering'
	for i in xrange(k):
		coordis=coordi_cl[i]+coordi_auto
		[m,n]=[len(coordi_cl[i]),len(coordis)]
		label=DBSCAN(eps=eps,min_samples=minPts).fit_predict(coordis)
		if label[0]!=-1:		
			for j in xrange(m,n):
				if label[j]==label[0]:
					clid_auto[j-m]=i
			print 'Find %d venue auto-tweet in cluster%d'%(clid_auto.count(i),i)
		else:
			print 'Find 0 venue auto-tweet in cluster%d'%i

	print 'Process auto-tweet by cluster'
	n=len(clid_auto)
	cls=init_clusters(k,range(k))
	for i in xrange(k):
		cls[i]=[data_auto[j] for j in xrange(n) if clid_auto[j]==i]
		if len(cls[i])==0:
			del cls[i]

	print 'Write shp file'
	write_auto_tweet_shp(fn_auto,k,cls)


	print 'Draw map'
	draw_auto_tweet_map(fn_auto,k,cls)

	print 'Write results'
	write_auto_tweet_txt(fn_auto,k,cls)



def dbscan_venue(fn_cls,fn_venue,eps,minPts,sep='\t'): #assign venue to sup clusters 
	print 'Extract venue tweets'
	data=file('txt\\venue-profil\\'+fn_venue+'.txt').readlines()[1:]
	n=len(data)
	data_venue=[data[i][:-1].split(sep) for i in xrange(n)]
	coordi_venue=[[float(data_venue[i][1]),float(data_venue[i][2])] for i in xrange(n)]
	clid_venue=[-1 for i in xrange(n)]
	
	print 'Extract tweet-cls'
	data=file('txt\\'+fn_cls+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	data=[data[i] for i in xrange(n) if int(data[i][0])>-1]
	n=len(data)
	coordi=[[float(data[i][1]),float(data[i][2])] for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	k=max(clid)
	coordi_cl=init_clusters(k,range(k))
	for i in xrange(k):
		coordi_cl[i]=[coordi[j] for j in xrange(n) if clid[j]==i]
		

	print 'Assign venue to sup-clusters through DBSCAN Clustering'
	for i in xrange(k):
		coordis=coordi_cl[i]+coordi_venue
		[m,n]=[len(coordi_cl[i]),len(coordis)]
		label=DBSCAN(eps=eps,min_samples=minPts).fit_predict(coordis)
		if label[0]!=-1:		
			for j in xrange(m,n):
				if label[j]==label[0]:
					clid_venue[j-m]=i
			print 'Find %d venue in cluster%d'%(clid_venue.count(i),i)
		else:
			print 'Find 0 venue in cluster%d'%i

	print 'Process venue cluster data'
	n=len(clid_venue)
	cls=init_clusters(k,range(k))
	for i in xrange(k):
		cls[i]=[data_venue[j] for j in xrange(n) if clid_venue[j]==i]
		if len(cls[i])==0:
			del cls[i]

	print 'Draw cluster map'
	k=len(cls)
	#draw_venue_map(fn_venue,k,cls)

	print 'Write shp file'
	#write_venue_shp(fn_venue,k,cls)

	print 'Write results'
	write_venue_txt(fn_venue,k,cls)

#def dbscan_sample(tbname,eps,minPts,sample,season=False,dbname='tweet_pgh',user='postgres'): #cluster sample genu tweet
def dbscan_sample(pg,eps,minPts,sample,season=False):
	
	print 'Export genu-tweet and normalize senti_val'
	data=export_tweet(pg,season,sample)
	n=len(data)
	senti=[data[i][-1] for i in xrange(n)]

	pg.cur.execute('''
		select avg(senti_val),stddev(senti_val) 
		from %s where auto_tweet is Null;'''%pg.tbname)
	(avg,std)=pg.cur.fetchall()[0]
	print 'Genu-tweet senti_val -- avg %0.4f std %0.4f'%(avg,std)
	senti=[(senti[i]-avg)/std for i in xrange(n)]
	senti_bool=[0 for i in xrange(n)]
	for i in xrange(n):
		if senti[i]>1:
			senti_bool[i]=1
		elif senti[i]<-1:
			senti_bool[i]=-1
	data=[list(data[i])[:-1]+[senti[i],senti_bool[i]] for i in xrange(n)]
	print 'Genu-tweet -- all %d pos %d neg %d'%(n,senti_bool.count(1),senti_bool.count(-1))

	print 'DBSCAN Clustering'
	n=len(data)
	coordis=[[data[i][0],data[i][1]] for i in xrange(n)]
	label=DBSCAN(eps=eps,min_samples=minPts).fit_predict(coordis)
	k=max(label)+1
	print 'Clustering results -- find %d clusters'%k

	print 'Process cluster data'
	names=range(k)
	cls=init_clusters(k,names)
	count=[len([data[i] for i in xrange(n) if label[i]==cl]) for cl in names]
	order=np.argsort(count)[::-1]
	for cl in names:
		cls[cl]=[data[i] for i in xrange(n) if label[i]==order[cl]]

	tbname=pg.tbname+'_rand'
	print 'Draw cluster map'
	draw_cluster_map(tbname,k,cls,season)

	print 'Write shp file'
	write_cluster_shp(tbname,k,cls,season)

	print 'Write results'
	cls[-1]=[data[i] for i in xrange(n) if label[i]==-1] #include noise to txt file
	write_cluster_txt(tbname,k,cls,season)

def dbscan_sample_season(tbname,seasons,sample,dbname='tweet_pgh',user='postgres'): #cluster sample genu tweet by season
	print "Create pgcontroller"
	pg=dbProcess.PgController(tbname,dbname,user)

	pg.cur.execute('select count(*) from %s where auto_tweet is Null;'%tbname)
	N=pg.cur.fetchone()[0]

	for season in seasons:
		[name,sta,end,eps,minPts]=season
		if sta<end:
			pg.cur.execute('''
				select count(*) from %s 
				where auto_tweet is Null and doy <@ int4range(%d,%d)'''%(tbname,sta,end))
		else:
			pg.cur.execute('''
				select count(*) from %s where auto_tweet is Null and 
				(doy <@ int4range(0,%d) or doy <@ int4range(%d,366));'''%(tbname,end,sta))
		n=pg.cur.fetchone()[0]
		rand=(N*sample)/(4*n)	
		print 'DBSCAN clustering -- %s from %d to %d with random %0.3f'%(name,sta,end,rand)
		dbscan_sample(pg,eps,minPts,rand,season[:3])

def dbscan_happy(tbname,eps,minPts,season=False,dbname='tweet_pgh',user='postgres'): #cluster genu pos tweet
	print "Create pgcontroller"
	pg=dbProcess.PgController(tbname,dbname,user)
	
	print 'Export genu-tweet and normalize senti_val'
	data=export_tweet(pg,season)
	n=len(data)
	senti=[data[i][-1] for i in xrange(n)]

	pg.cur.execute('''
		select avg(senti_val),stddev(senti_val) 
		from %s where auto_tweet is Null;'''%pg.tbname)
	(avg,std)=pg.cur.fetchall()[0]
	print 'Genu-tweet senti_val -- avg %0.4f std %0.4f'%(avg,std)
	senti=[(senti[i]-avg)/std for i in xrange(n)]
	data=[list(data[i])[:-1]+[senti[i],1] for i in xrange(n) if senti[i]>1]
	print 'Genu-tweet -- all %d pos %d'%(n,len(data))

	print 'DBSCAN Clustering'
	n=len(data)
	coordis=[[data[i][0],data[i][1]] for i in xrange(n)]
	label=DBSCAN(eps=eps,min_samples=minPts).fit_predict(coordis)
	k=max(label)+1
	print 'Clustering results -- find %d clusters'%k

	print 'Process cluster data'
	names=range(k)
	cls=init_clusters(k,names)
	count=[len([data[i] for i in xrange(n) if label[i]==cl]) for cl in names]
	order=np.argsort(count)[::-1]
	for cl in names:
		cls[cl]=[data[i] for i in xrange(n) if label[i]==order[cl]]

	tbname+='_pos'
	print 'Draw cluster map'
	draw_cluster_map(tbname,k,cls,season)

	print 'Write shp file'
	write_cluster_shp(tbname,k,cls,season)

	print 'Write results'
	cls[-1]=[data[i] for i in xrange(n) if label[i]==-1] #include noise to txt file
	write_cluster_txt(tbname,k,cls,season)

def dbscan_happy_season(tbname,seasons,dbname='tweet_pgh',user='postgres'): #cluster genu pos tweet by season
	for season in seasons:
		[name,sta,end,eps,minPts]=season
		print 'DBSCAN clustering -- %s from %d to %d'%(name,sta,end)
		dbscan_happy(tbname,eps,minPts,season[:3])

def dbscan_sup(fname,exts,eps,minPts,eps_ns,minPts_ns,minCls):
	print 'Extract data from seasonal tweet clusters'
	[data_cl,data_ns]=extract_tweet_cls(fname,exts)

	print 'DBSCAN clustering of seasonal clusters'
	n=len(data_cl)
	coordis=[[float(data_cl[i][1]),float(data_cl[i][2])] for i in xrange(n)]
	label=DBSCAN(eps=eps,min_samples=minPts).fit_predict(coordis)
	k=max(label)+1
	print 'Clustering results -- find %d super clusters'%k

	print 'Init super clusters'
	names=range(k)
	cls=init_clusters(k,names)
	for cl in names:
		cls[cl]=[data_cl[i] for i in xrange(n) if label[i]==cl]
	data_ns+=[data_cl[i] for i in xrange(n) if label[i]==-1]

	print 'Add noise to super clusters'
	for cl in cls:
		n0=len(cls[cl])
		[cls,data_ns]=add_noise(cls,data_ns,cl,eps,minPts)
		n1=len(cls[cl])
		print '-- %d noise points are added to cluster%d, %d remained'%(n1-n0,cl,len(data_ns))

	print 'DBSCAN clustering of remaining noise data'
	n=len(data_ns)
	coordis=[[float(data_ns[i][1]),float(data_ns[i][2])] for i in xrange(n)]
	label=DBSCAN(eps=eps_ns,min_samples=minPts_ns).fit_predict(coordis)
	m=max(label)+1
	print 'Clustering results -- find %d clusters'%m

	names=range(k,k+m)
	for cl in xrange(m):
		data=[data_ns[i] for i in xrange(n) if label[i]==cl]
		if len(data)>minCls:
			cls[k+cl]=data
		else:
			break
	print '-- add %d new clusters with more than %d points'%(cl,minCls)
	

	print 'Cluster processing'
	cls_new=dict()
	names=range(len(cls))
	count=[len(cls[cl]) for cl in names]
	order=np.argsort(count)[::-1]
	for cl in cls:
		data=cls[names[order[cl]]]
		n=len(data)
		if n>minCls:
			cls_new[cl]=[data[i][1:] for i in xrange(n)]
		else:
			break
	print '-- %d clusters finally remained'%len(cls_new)

	fname=fname.replace('_tweet_cls','')
	ext=['sup']
	print 'Draw cluster map'
	#draw_cluster_map(fname,len(cls_new),cls_new,ext)

	print 'Write shp file'
	write_cluster_shp(fname,len(cls_new),cls_new,ext)

	print 'Write results'
	cls_new[-1]=[data_ns[i][1:] for i in xrange(n) if label[i]==-1] #include noise to txt file
	#write_sup_cluster_txt(fname,len(cls_new),cls_new)


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

def color_generator(k,senti=False):
	cmap=plt.get_cmap('jet')
	
	n=2*k if senti else k

	cNorm=colors.Normalize(vmin=0,vmax=n)
	scalar=cmx.ScalarMappable(norm=cNorm,cmap=cmap)
	hues=[scalar.to_rgba(i) for i in xrange(n)]

	if senti==1:
		return hues[k:]
	elif senti==-1:
		return hues[:k]
	else:
		return hues

'''------------------------------------------------------------------'''
def export_tweet(pg,season=False,sample=False):
	if sample:
		random='and random() <%f'%sample
	else:
		random=''
	if season:
		[d_min,d_max]=season[1:]
		if d_min<d_max:
			pg.cur.execute('''
				select lon,lat,id,username,year,doy,dow,hour,txt,term,emoji,tag,senti_val
				from %s where auto_tweet is Null and doy <@ int4range(%d,%d) %s;
				'''%(pg.tbname,d_min,d_max,random))
		else:
			pg.cur.execute('''
				select lon,lat,id,username,year,doy,dow,hour,txt,term,emoji,tag,senti_val
				from %s where auto_tweet is Null and 
				(doy <@ int4range(0,%d) or doy <@ int4range(%d,366)) %s;
				'''%(pg.tbname,d_max,d_min,random))
	else:
		pg.cur.execute('''
			select lon,lat,id,username,year,doy,dow,hour,txt,term,emoji,tag,senti_val
			from %s where auto_tweet is Null %s;'''%(pg.tbname,random))
	
	return pg.cur.fetchall()

def extract_tweet_cls(fname,exts):
	data_cl=[]
	data_ns=[]
	for ext in exts:
		[cl,ns]=extract_tweet(fname+'_'+ext)
		data_cl+=cl
		data_ns+=ns

	return data_cl,data_ns

def extract_tweet(fname,sep='\t'):
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	cl=[data[i] for i in xrange(n) if int(data[i][0])>-1]
	ns=[data[i] for i in xrange(n) if int(data[i][0])<0]

	return cl,ns


'''------------------------------------------------------------------'''
def add_noise(cls,data_ns,cl,eps,minPts):
	data=cls[cl]+data_ns
	m=len(cls[cl])
	n=len(data)
	coordis=[[float(data[i][1]),float(data[i][2])] for i in xrange(n)]
	label=DBSCAN(eps=eps,min_samples=minPts).fit_predict(coordis)
	idx=label[0] #cluster idx of points from original cluster
	if idx!=-1: #ensure the original cluster is recreated
		cls[cl]=[data[i] for i in xrange(n) if label[i]==idx]
		data_ns=[data[i] for i in xrange(m,n) if label[i]!=idx]

	return cls,data_ns

'''------------------------------------------------------------------'''
def draw_auto_tweet_map(fname,k,cls):
	color=color_generator(k)
	color=np.random.permutation(color)
	keys=cls.keys()
	plt.figure()
	for cl in cls:
		data=cls[cl]
		n=len(data)
		x=[float(data[i][1]) for i in xrange(n)]
		y=[float(data[i][2]) for i in xrange(n)]
		plt.scatter(x,y,c=color[keys.index(cl)],marker='o')
		print '-- draw %d points of Cluster%d'%(n,cl)

	plt.xlabel('lon')
	plt.ylabel('lat')
	plt.xlim(-80.10,-79.86)
	plt.ylim(40.36,40.51)
	plt.title('Venue Auto-Tweet Clusters')

	plt.savefig('png\\'+fname+'_cls.png')


def write_auto_tweet_shp(fname,k,cls):
	fields=[['ID','N',8],['CLUSTER','N',8],['VENUE','C',50],
	['CATEGORY','C',50],['USER','C',50],
	['YEAR','N',8],['DOY','N',8],['DOW','N',8],['HOUR','N',8]]
	f=shapefile.Writer(shapeType=1)
	f.autoBalance = 1
	for field in fields:
		f.field(field[0],field[1],field[2])

	id=1
	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			lon=float(data[i][1])
			lat=float(data[i][2])
			venue=data[i][0]
			cat=data[i][3]			
			user=data[i][4]
			[year,doy,dow,hour]=[int(data[i][j]) for j in xrange(5,9)]
			f.point(lon,lat)
			f.record(ID=id,CLUSTER=cl,VENUE=venue,CATEGORY=cat,USER=user,\
				YEAR=year,DOY=doy,DOW=dow,HOUR=hour)
			id+=1

	
	f.save('shp\\'+fname+'.shp')	

def write_auto_tweet_txt(fname,k,cls,sep='\t'):
	f=os.open('txt\\venue\\'+fname+'_cls.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid,venue,lon,lat,category,user,year,doy,dow,hour,txt\n'.replace(',',sep))

	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			os.write(f,sep.join(['%d'%cl]+data[i])+'\n')

	os.close(f)

'''------------------------------------------------------------------'''
def draw_venue_map(fname,k,cls):
	color=color_generator(k)
	keys=cls.keys()
	plt.figure()
	for cl in cls:
		data=cls[cl]
		n=len(data)
		x=[float(data[i][1]) for i in xrange(n)]
		y=[float(data[i][2]) for i in xrange(n)]
		plt.scatter(x,y,c=color[keys.index(cl)],marker='o')
		print '-- draw %d points of Cluster%d'%(n,cl)

	plt.xlabel('lon')
	plt.ylabel('lat')
	plt.xlim(-80.10,-79.86)
	plt.ylim(40.36,40.51)
	plt.title('Venue Clusters')

	plt.savefig('png\\'+fname+'.png')

def write_venue_shp(fname,k,cls):
	fields=[['ID','N',8],['CLUSTER','N',8],['VENUE','C',50],
	['CATEGORY','C',50],['CHECKIN','N',8],['USER','N',8]]
	f=shapefile.Writer(shapeType=1)
	f.autoBalance = 1
	for field in fields:
		f.field(field[0],field[1],field[2])

	id=1
	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			lon=float(data[i][1])
			lat=float(data[i][2])
			venue=data[i][0]
			cat=data[i][3]
			checkin=int(data[i][-2])
			user=int(data[i][-1])
			f.point(lon,lat)
			f.record(ID=id,CLUSTER=cl,VENUE=venue,CATEGORY=cat,CHECKIN=checkin,USER=user)
			id+=1

	
	f.save('shp\\'+fname+'.shp')	

def write_venue_txt(fname,k,cls,sep='\t'):
	f=os.open('txt\\venue-profil\\'+fname+'_cls.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid,venue,lon,lat,cat_name,cat_id,user,year,doy,dow,hour\n'.replace(',',sep))

	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			os.write(f,sep.join(['%d'%cl]+data[i][:-1])+'\n')

	os.close(f)

'''------------------------------------------------------------------'''
def draw_cluster_map(fname,k,cls,ext):
	color=color_generator(k)
	plt.figure()
	for cl in cls:
		data=cls[cl]
		n=len(data)
		x=[float(data[i][0]) for i in xrange(n)]
		y=[float(data[i][1]) for i in xrange(n)]
		plt.scatter(x,y,c=color[cl],marker='o')
		print '-- draw %d points of Cluster%d'%(n,cl)

	plt.xlabel('lon')
	plt.ylabel('lat')
	plt.xlim(-80.10,-79.86)
	plt.ylim(40.36,40.51)
	plt.title('Happy Tweet Clusters')

	fname+='_tweet_cls'
	if ext:
		fname+='_%s'%ext[0][:3]
	plt.savefig('png\\'+fname+'.png')


def write_cluster_shp(fname,k,cls,ext):
	fields=[['ID','N',8],['CLUSTER','N',8],['USER','C',50],
	['YEAR','N',8],['DOY','N',8],['DOW','N',8],['HOUR','N',8],['HAPPY','N',8]]
	f=shapefile.Writer(shapeType=1)
	f.autoBalance = 1
	for field in fields:
		f.field(field[0],field[1],field[2])

	field=['SENTI','N',8,3]
	f.field(field[0],field[1],field[2],field[3])

	id=1
	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			lon=float(data[i][0])
			lat=float(data[i][1])
			user=data[i][3]
			[year,doy,dow,hour]=[int(data[i][j]) for j in xrange(4,8)]
			senti=round(float(data[i][-2]),3)
			happy=int(data[i][-1])
			f.point(lon,lat)
			f.record(ID=id,CLUSTER=cl,USER=user,YEAR=year,DOY=doy,DOW=dow,HOUR=hour,SENTI=senti,HAPPY=happy)
			id+=1

	fname+='_tweet_cls'
	if ext:
		fname+='_%s'%ext[0][:3]
	
	f.save('shp\\'+fname+'.shp')	


def write_cluster_txt(fname,k,cls,ext,sep='\t'): #data from database
	fname+='_tweet_cls'
	if ext:
		fname+='_%s'%ext[0][:3]
	
	f=os.open('txt\\'+fname+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid,lon,lat,id,user,year,doy,dow,hour,txt,term,emoji,tag,senti_val,senti_bool\n'.replace(',',sep))

	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			(lon,lat,id,username,year,doy,dow,hour,txt,term,emoji,tag,senti,happy)=data[i]
			if emoji==None:
				emoji=''
			if tag==None:
				tag=''		
			line='%d,%0.4f,%0.4f,%s,%s,%d,%d,%d,%d'\
				%(cl,lon,lat,id,username,year,doy,dow,hour)
			#replace 'amp' by '&'
			if 'amp' in txt:
				txt=txt.replace(' amp ',' & ')
				term=term.replace(',amp,',',&,')
			os.write(f,line.replace(',',sep)+sep+sep.join([txt,term,emoji,tag,
				'%0.3f'%senti,'%d'%happy])+'\n')

	os.close(f) 

def write_sup_cluster_txt(fname,k,cls,sep='\t'): #data from seasonal cluster txt file
	fname+='_tweet_cls_sup'
	f=os.open('txt\\'+fname+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid,lon,lat,id,user,year,doy,dow,hour,txt,term,emoji,tag,senti_val,senti_bool\n'.replace(',',sep))

	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			os.write(f,sep.join(['%d'%cl]+data[i])+'\n')

	os.close(f)
	
	