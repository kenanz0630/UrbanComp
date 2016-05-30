import math, os, copy, shapefile, dbProcess
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import numpy as np
from sklearn.cluster import DBSCAN
from collections import Counter

'''MAIN FUNCTION'''
def dbscan(tbname,eps,minPts,season=False,dbname='tweet_pgh',user='postgres'): #cluster non-auto pos tweet
	print "Create pgcontroller"
	pg=dbProcess.PgController(tbname,dbname,user)
	
	print 'Export non-auto tweets and normalize senti_val'
	data=export_tweet(pg,season)
	n=len(data)
	senti=[data[i][-1] for i in xrange(n)]

	pg.cur.execute('''
		select avg(senti_val),stddev(senti_val) 
		from %s where auto_tweet is Null;'''%pg.tbname)
	(avg,std)=pg.cur.fetchall()[0]
	print 'Non-auto tweet senti_val -- avg %0.4f std %0.4f'%(avg,std)
	senti=[(senti[i]-avg)/std for i in xrange(n)]
	data=[list(data[i])[:-1]+[senti[i]] for i in xrange(n) if senti[i]>1]
	print 'Non-auto tweet -- all %d pos %d'%(n,len(data))

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

	print 'Draw cluster map'
	draw_cluster_map(tbname,k,cls,season)

	print 'Write shp file'
	write_cluster_shp(tbname,k,cls,season)

	print 'Write results'
	cls[-1]=[data[i] for i in xrange(n) if label[i]==-1] #include noise to txt file
	write_cluster_txt(tbname,k,cls,season)

def dbscan_season(tbname,seasons,dbname='tweet_pgh',user='postgres'): #cluster non-auto pos tweet by season
	for season in seasons:
		[name,sta,end,eps,minPts]=season
		print 'DBSCAN clustering -- %s from %d to %d'%(name,sta,end)
		dbscan(tbname,eps,minPts,season[:3])

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
	draw_cluster_map(fname,len(cls_new),cls_new,ext)

	print 'Write shp file'
	write_cluster_shp(fname,len(cls_new),cls_new,ext)

	print 'Write results'
	cls_new[-1]=[data_ns[i] for i in xrange(n) if label[i]==-1] #include noise to txt file
	write_sup_cluster_txt(fname,len(cls_new),cls_new)


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
def export_tweet(pg,season):
	if season:
		[d_min,d_max]=season[1:]
		if d_min<d_max:
			pg.cur.execute('''
				select lon,lat,id,username,year,doy,dow,hour,txt,term,emoji,tag,senti_val
				from %s where auto_tweet is Null and doy <@ int4range(%d,%d);
				'''%(pg.tbname,d_min,d_max))
		else:
			pg.cur.execute('''
				select lon,lat,id,username,year,doy,dow,hour,txt,term,emoji,tag,senti_val
				from %s where auto_tweet is Null and 
				(doy <@ int4range(0,%d) or doy <@ int4range(%d,366));
				'''%(pg.tbname,d_max,d_min))
	else:
		pg.cur.execute('''
			select lon,lat,id,username,year,doy,dow,hour,txt,term,emoji,tag,senti_val
			from %s where auto_tweet is Null;'''%pg.tbname)
	
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
	fields=[['ID','N',8],['CLUSTER','N',8],['USER','C',50]]
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
			senti=round(float(data[i][-1]),3)
			f.point(lon,lat)
			f.record(ID=id,CLUSTER=cl,USER=user,SENTI=senti)
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
	os.write(f,'clid,lon,lat,id,user,year,doy,dow,hour,txt,term,emoji,tag,senti_val\n'.replace(',',sep))

	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			(lon,lat,id,username,year,doy,dow,hour,txt,term,emoji,tag,senti)=data[i]
			if emoji==None:
				emoji=''
			if tag==None:
				tag=''		
			line='%d,%0.4f,%0.4f,%s,%s,%d,%d,%d,%d'\
				%(cl,lon,lat,id,username,year,doy,dow,hour)
			os.write(f,line.replace(',',sep)+sep+sep.join([txt,term,emoji,tag,'%0.3f'%senti])+'\n')

	os.close(f) 

def write_sup_cluster_txt(fname,k,cls,sep='\t'): #data from seasonal cluster txt file
	fname+='_tweet_cls_sup'
	f=os.open('txt\\'+fname+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid,lon,lat,id,user,year,doy,dow,hour,txt,term,emoji,tag,senti_val\n'.replace(',',sep))

	for cl in cls:
		data=cls[cl]
		n=len(data)
		for i in xrange(n):
			os.write(f,sep.join(['%d'%cl]+data[i])+'\n')

	os.close(f)
	
	