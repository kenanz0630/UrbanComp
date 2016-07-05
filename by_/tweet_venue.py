import dbProcess
import os, foursquare, shapefile
import numpy as np
import preprocessing as pre
import matplotlib.pyplot as plt
from geopy.distance import great_circle



'''--- MAIN FUNCTION ---'''
def export_venue(tbname,dbname='tweet_pgh',user='postgres',sep='\t'):
	#extract venue name and coordis from 4sq/inst auto-tweet txt
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Extract venue names from auto-tweets'
	tweet=[]
	venue_all=[]
	for auto in ['4sq','inst']:
		print '-- Export %s auto-tweet txt'%auto
		pg.cur.execute('''
			select txt,lon,lat,username,year,doy,dow,hour 
			from %s where auto_tweet='%s';'''%(tbname,auto))
		data=pg.cur.fetchall()
		n=pg.cur.rowcount
		print '-- Process venue name'
		names=[process_venue_name(data[i][0],auto) for i in xrange(n)]
		tweet+=[list(data[i]) for i in xrange(n) if names[i]!='']
		venue_all+=[(names[i],data[i][1],data[i][2]) for i in xrange(n) if names[i]!='']
	
	venues=list(set(venue_all))
	print '-- Find %d venues with unique name and coordis from auto-tweets'%len(venues)
	

	print 'Merge venues with same location and similar names'	
	venues=merge_venue(venues)
	print '-- Find %d unique venues'%(len(venues))

	print 'Cluster auto-tweet by venues'
	venue_tweet=init_clusters(len(venues),venues)
	n=len(tweet)
	for venue in venues:
		venue_tweet[venue]=[tweet[i][3:]+[tweet[i][0]] for i in xrange(n) \
		if toMerge(venue_all[i][0],venue[0]) and isNeighbor([venue_all[i][1],venue_all[i][2]],[venue[1],venue[2]])]
		print '-- %d auto-tweet of venue %s %s'%(len(venue_tweet[venue]),venue[0],[venue[1],venue[2]])

	
	print 'Draw map'
	x=[venue[1] for venue in venues]
	y=[venue[2] for venue in venues]
	plt.scatter(x,y,marker='o')

	plt.xlabel('lon')
	plt.ylabel('lat')
	plt.xlim(-80.10,-79.86)
	plt.ylim(40.36,40.51)
	plt.title('Venue Auto-Tweet')

	plt.savefig('png\\'+tbname+'_venue_tweet.png')


	print 'Write results'
	f=os.open('txt\\venue\\'+tbname+'_venue_tweet.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,lon,lat,category,user,year,doy,dow,hour,txt\n'.replace(',',sep))

	for venue in venues:
		n=len(venue_tweet[venue])
		for i in xrange(n):
			(user,year,doy,dow,hour,txt)=venue_tweet[venue][i]
			name=venue[0].replace('&amp;','&')
			[lon,lat]=[venue[1],venue[2]]
			cat='NA'
			os.write(f,'%s'%name+(',%0.4f,%0.4f,%s,%s,%d,%d,%d,%d,'\
				%(lon,lat,cat,user,year,doy,dow,hour)).replace(',',sep)+'%s\n'%txt)

	os.close(f) 

def venue_checkin_sum(fname,sep='\t'):
	print 'Extract venue checkin by cls'
	data=file('txt\\venue\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	foursq=[clid[i] for i in xrange(n) if "I'm" in data[i][-1]]
	inst=[clid[i] for i in xrange(n) if "photo" in data[i][-1]]
	k=max(clid)+1

	print 'Write result'
	f=os.open('txt\\venue\\'+fname+'_checkin_sum.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid,all,4sq,inst\n')	
	for i in xrange(k):
		[n_foursq,n_inst]=[foursq.count(i),inst.count(i)]
		os.write(f,'%d,%d,%d,%d\n'%(i,n_foursq+n_inst,n_foursq,n_inst))

	os.close(f)


def search_venue(fname,foursq,maxDist,sep='\t'):
	#search venue in 4sq venue database, match with auto-tweet
	#rewrite venue_tweet with venue category name and id
	#write removed venues
	print 'Extract venues by cluster'
	venue_cl=extract_venue_cls(fname)

	print 'Connect to 4sq database'
	client=foursquare.Foursquare(client_id=foursq['id'],client_secret=foursq['secret'])

	print 'Search and match venue by cluster'
	cls=venue_cl.keys()
	venue_remove=init_clusters(len(cls),cls)
	for cl in cls:
		[venue_cl[cl],venue_remove[cl]]=search_venue_cl(client,venue_cl[cl],maxDist)

	print 'Rewrite venue auto-tweet by cluster'
	data=file('txt\\venue\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	
	tweet_cl=init_clusters(len(cls),cls)
	for cl in cls:
		for venue in venue_cl[cl]:
			(name,lon,lat,cat)=venue
			tweet_cl[cl]+=[[name,'%0.4f'%lon,'%0.4f'%lat,cat]+data[i][5:] for i in xrange(n)\
			if data[i][1]==name and isNeighbor([float(data[i][2]),float(data[i][3])],[lon,lat])]
		for venue in venue_remove[cl]:
			(name,lon,lat)=venue
			tweet_cl[cl]+=[[name,'%0.4f'%lon,'%0.4f'%lat]+data[i][4:] for i in xrange(n)\
			if data[i][1]==name and isNeighbor([float(data[i][2]),float(data[i][3])],[lon,lat])]
		
	f=os.open('txt\\venue\\'+fname+'_new.txt',os.O_RDWR|os.O_CREAT)

	os.write(f,'clid,venue,lon,lat,cat,user,year,doy,dow,hour,txt\n'.replace(',',sep))
	for cl in cls:
		for tweet in tweet_cl[cl]:
			os.write(f,sep.join(['%d'%cl]+tweet)+'\n')

	os.close(f)
	'''
	print 'Write remove venues'
	f=os.open('txt\\venue\\'+fname+'_remove.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid,venue,lon,lat\n'.replace(',',sep))
	for cl in venue_remove:
		for venue in venue_remove[cl]:
			os.write(f,sep.join(['%d'%cl,venue[0],'%0.4f'%venue[1],'%0.4f'%venue[2]])+'\n')

	os.close(f)
	'''


def venue_category_hier(foursq):
	#get and process 4sq category hierachy 
	print 'Connect to 4sq database'
	client=foursquare.Foursquare(client_id=foursq['id'],client_secret=foursq['secret'])

	print 'Get and process categories'
	cat=client.venues.categories()['categories']
	cat=process_category_hier(cat)

	print 'Write category hierachy and ID'
	write_venue_category_hier(cat)
	

def venue_category_sum(tbname,cls,uses,dbname='tweet_pgh',user='postgres'):
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Extract venue numbers by category and cluster'
	cat_cl=init_clusters(len(uses),uses,[[0]+[0 for cl in cls] for use in uses])
	pg.cur.execute('''
		select cat,count(*) from %s group by cat;
		'''%tbname)
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	if n>0:
		for i in xrange(n):
			cat_cl[data[i][0]][0]=data[i][1]
		
	for cl in cls:
		pg.cur.execute('''
			select cat,count(*) from %s where clid=%d group by cat;
			'''%(tbname,cl))
		data=pg.cur.fetchall()
		n=pg.cur.rowcount
		if n>0:
			for i in xrange(n):
				cat_cl[data[i][0]][cls.index(cl)+1]=data[i][1]
		

	print 'Write results'
	f=os.open('txt\\venue\\urban-use\\'+tbname+'_cat_sum.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'cat,pitt,'+','.join(['%d'%cl for cl in cls])+'\n')
	for use in uses:
		os.write(f,'%s,'%use+','.join(['%d'%n for n in cat_cl[use]])+'\n')

	os.close(f)

def venue_profile(fname,fn_cat,sep='\t'):
	#profile venue by clid, name, coordi, cat, #_checkin, #_user
	#write .shp file
	print 'Extract venue category hierachy'
	cats=extract_venue_cat(fn_cat)
	

	print 'Extract venue checkin data by cluster'
	data=file('txt\\venue\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	cls=range(max(clid)+1)

	print 'Process venue profile by cluster'
	venue_cl=init_clusters(len(cls),cls)
	for cl in cls:
		venue=[data[i][1:] for i in xrange(n) if clid[i]==cl]
		venue_cl[cl]=process_venue_profile(venue,cats)	


	print 'Write results'
	fname=fname.replace('tweet_cls','cls')
	f=os.open('txt\\venue\\'+fname+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,sep.join(['clid','venue','lon','lat','cat','user','checkin'])+'\n')	
	for cl in cls:
		venues=venue_cl[cl]
		for venue in venues:
			[name,lon,lat,cat,user,checkin]=venue
			os.write(f,sep.join(['%d'%cl,name,lon,lat,cat,'%d'%user,'%d'%checkin])+'\n')

	os.close(f)
 
	print 'Write .shp file'
	write_venue_shp(fname,venue_cl,cls)

def pop_venue_cls(fname,cls,m,sep='\t'):
	#sort venues by #_checkin
	print 'Extract venue checkin by cluster'
	data=file('txt\\venue\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	
	print 'Sort checkin/user by cluster'
	venue_cl=init_clusters(len(cls),cls)
	for cl in cls:
		venue=[data[i][1:] for i in xrange(n) if clid[i]==cl]
		venue_cl[cl]=process_pop_venue(venue,m)	

	print 'Write results'
	f_checkin=os.open('txt\\venue\\'+fname+'_pop_checkin.txt',os.O_RDWR|os.O_CREAT)
	f_user=os.open('txt\\venue\\'+fname+'_pop_user.txt',os.O_RDWR|os.O_CREAT)

	os.write(f_checkin,','.join(['cluster%d,'%cl for cl in cls])+'\n')	
	os.write(f_user,','.join(['cluster%d,'%cl for cl in cls])+'\n')	
	
	for i in xrange(m):
		os.write(f_checkin,','.join(['%s,%d'%(venue_cl[cl][0]['venue'][i],venue_cl[cl][0]['checkin'][i])\
		 for cl in cls])+'\n')
		os.write(f_user,','.join(['%s,%d'%(venue_cl[cl][1]['venue'][i],venue_cl[cl][1]['user'][i])\
		 for cl in cls])+'\n')

	os.close(f_checkin)
	os.close(f_user)
 
def venue_cat_entropy(tbname,cls,dbname='tweet_pgh',user='postgres'):
	#venue_cat entropy
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Extract cluster urban use by how'
	entro_cls=init_clusters(len(cls),cls)
	for cl in cls:
		pg.cur.execute('''
			select cat_main,count(*) from %s
			where clid=%d group by cat_main;'''%(tbname,cl))
		data=pg.cur.fetchall()
		n=pg.cur.rowcount
		uses=[data[i][1] for i in xrange(n)]
		entro_cls[cl]=entropy(uses)

	print 'Write results'
	fname=tbname.replace('tweet_cls','cls_cat')
	f=os.open('txt\\venue\\urban-use\\'+fname+'_entro.txt',os.O_RDWR|os.O_CREAT)
	entros=[(cl,entro_cls[cl]) for cl in cls]
	entros=np.array(entros,dtype=[('clid','i'),('entro','f')])
	entros=np.sort(entros,order='entro')[::-1]
	os.write(f,'clid,entro\n')
	for entro in entros:
		os.write(f,'%d,%0.4f\n'%(entro[0],entro[1]))
	os.close(f)


	

def venue_cat_temp_entropy(tbname,cls,uses,dbname='tweet_pgh',user='postgres'):
	#venue_cat sum(entropy) by how
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Extract cluster urban use by how'
	entro_cls=init_clusters(len(cls),cls)
	for cl in cls:
		pg.cur.execute('''
			select dow*24+hour as how,count(*) from %s
			where clid=%d group by how order by how;'''%(tbname,cl))
		n=pg.cur.rowcount
		if n>0:			
			how=pg.cur.fetchall()
			how=[how[i][0] for i in xrange(n)]
			entro_cls[cl]=entropy_cl(pg,cl,uses,how)
		else:
			entro_cls[cl]=0


	print 'Write results'
	fname=tbname.replace('tweet_cls','cls_cat')
	f=os.open('txt\\venue\\urban-use\\'+fname+'_temp_entro.txt',os.O_RDWR|os.O_CREAT)
	entros=[(cl,entro_cls[cl]) for cl in cls]
	entros=np.array(entros,dtype=[('clid','i'),('entro','f')])
	entros=np.sort(entros,order='entro')[::-1]
	os.write(f,'clid,entro\n')
	for entro in entros:
		os.write(f,'%d,%0.4f\n'%(entro[0],entro[1]))
	os.close(f)

def venue_category_repr(tbname,uses,use_cl,cls,dbname='tweet_pgh',user='postgres'):
	#repr cluster urban use by specific venue categories
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export repr venue categories and write results'
	fname=tbname.replace('tweet_cls','cls_cat')	
	f=os.open('txt\\venue\\urban-use\\'+fname+'_repr.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'clid\tuse_repr\n')
	for cl in cls:
		cats=[]
		for group in use_cl:
			if cl in group[0]:
				cat_n=group[1]
				pg.cur.execute('''
					select cat_main,count(*) from %s
					where clid=%d group by cat_main order by count(*) desc;
					'''%(tbname,cl))
				data=pg.cur.fetchall()
				cats=[str(uses.index(data[i][0])) for i in xrange(cat_n)]
		os.write(f,'%d\t'%cl+','.join(cats)+'\n')

	os.close(f)

def rewrite_ttest(fname,cls):\
	#rewrite ttest in order of studied cls
	print 'Extract ttest results'
	data=file('txt\\temp\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	m=data.index([''])


	print 'Construct matrix'
	cls_0=[int(data[i][0]) for i in xrange(m)]
	k=len(cls_0)
	mtx_t=np.zeros((k,k))
	mtx_p=np.zeros((k,k))
	for i in xrange(k):
		for j in xrange(i+1,k):
			#print cls_0[i],cls_0[j]
			mtx_t[cls_0[i]][cls_0[j]]=mtx_t[cls_0[j]][cls_0[i]]=float(data[i][1+2*(j-1)])
			mtx_p[cls_0[i]][cls_0[j]]=mtx_p[cls_0[j]][cls_0[i]]=float(data[i][1+2*(j-1)+1])


	mtx_h=[['' for i in xrange(k)] for j in xrange(k)]
	data=data[m+1:]
	for i in xrange(k):
		for j in xrange(i+1,k):
			mtx_h[cls_0[i]][cls_0[j]]=mtx_h[cls_0[j]][cls_0[i]]=data[i][j]

	print 'Rewrite ttest results'
	f=os.open('txt\\venue\\urban-use\\'+fname+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,','+','.join(['%d,'%cl for cl in cls[1:]])+'\n')
	k=len(cls)
	for i in xrange(k):
		os.write(f,'%d,'%cls[i]+''.join([',,' for j in xrange(i)])+\
			''.join(['%0.4f,%0.4f,'%(mtx_t[cls[i]][cls[j]],mtx_p[cls[i]][cls[j]])\
			 for j in xrange(i+1,k)])+'\n')

	os.write(f,'\n')

	os.write(f,','+','.join(['%d'%cl for cl in cls[1:]])+'\n')	
	for i in xrange(k):
		print cls[i],[cls[j] for j in xrange(i+1,k)]
		print cls[i],[mtx_h[cls[i]][cls[j]] for j in xrange(i+1,k)]
		os.write(f,'%d,'%cls[i]+''.join([',' for i in xrange(i)])+\
			','.join([mtx_h[cls[i]][cls[j]] for j in xrange(i+1,k)])+'\n')


	os.close(f)


def senti_scale_stats(fname,cls,scale):
	#rewrite senti_sum in order of studied cls
	data=file('txt\\temp\\'+fname+'.txt').readlines()
	n=len(data)
	data=[data[i][:-1].split(',') for i in xrange(n)]
	clid=[name.replace('cluster','') for name in data[0]]
	idx=[i for i in xrange(len(clid)) if clid[i]!='' and int(clid[i]) in cls]
	
	data=data[2:]
	n-=2
	k=len(cls)
	senti_cls=init_clusters(k,cls,[[0 for i in xrange(n)] for j in xrange(k)])
	for i in xrange(k):
		for j in xrange(n):
			if int(data[j][idx[i]])>0:
				senti_cls[cls[i]][j]=float(data[j][idx[i]+1])/int(data[j][idx[i]])


	f=os.open('txt\\venue\\urban-use\\'+fname+'_sum.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'cl,senti_avg,senti_std\n')
	for cl in cls:
		avg=np.mean(senti_cls[cl])
		std=np.std(senti_cls[cl])
		os.write(f,'%d,%0.4f,%0.4f\n'%(cl,avg,std))

	os.close(f)


def repr_venue_cls(tbname,cls,uses,minCheckin,minUser,maxN,\
	dbname='tweet_pgh',user='postgres'):
	#extract most popular venue of repr cats of each cluster
	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export pop venue of repr use'
	venue_cl=init_clusters(len(cls),cls)
	for cl in cls:
		mtx_checkin=export_pop_venue_cls(pg,cl,uses,minCheckin,maxN,'checkin')
		mtx_user=export_pop_venue_cls(pg,cl,uses,minCheckin,maxN,'user_n')
		venue_cl[cl]=[mtx_checkin,mtx_user]

	print 'Fill-up blanks'
	m_checkin=max([len(venue_cl[cl][0]) for cl in cls])
	m_user=max([len(venue_cl[cl][1]) for cl in cls])
	for cl in cls:
		if len(venue_cl[cl][0])<m_checkin:
			n=len(venue_cl[cl][0])
			venue_cl[cl][0]=list(venue_cl[cl][0])+[('','',0) for i in xrange(n,m_checkin)]
		if len(venue_cl[cl][1])<m_user:
			n=len(venue_cl[cl][1])
			venue_cl[cl][1]=list(venue_cl[cl][1])+[('','',0) for i in xrange(n,m_user)]
		

	print 'Write results'
	f_checkin=os.open('txt\\venue\\urban-use\\'+tbname+'_pop_checkin.txt',os.O_RDWR|os.O_CREAT)
	f_user=os.open('txt\\venue\\urban-use\\'+tbname+'_pop_user.txt',os.O_RDWR|os.O_CREAT)

	os.write(f_checkin,','.join(['cluster%d,,'%cl for cl in cls])+'\n')	
	os.write(f_user,','.join(['cluster%d,,'%cl for cl in cls])+'\n')	
	
	for i in xrange(m_checkin):
		os.write(f_checkin,','.join(['%s,%s,%d'%(venue_cl[cl][0][i][0],venue_cl[cl][0][i][1],venue_cl[cl][0][i][2])\
		 for cl in cls])+'\n')
	for i in xrange(m_user):
		os.write(f_user,','.join(['%s,%s,%d'%(venue_cl[cl][1][i][0],venue_cl[cl][1][i][1],venue_cl[cl][1][i][2])\
		 for cl in cls])+'\n')

	os.close(f_checkin)
	os.close(f_user)
	

	

	



def venue_genu_tweet(fn_venue,tbname,minCheckin=10,maxDist=50,dbname='tweet_pgh',user='postgres'):
	#search venue related tweet by keyword(venue name) and dist
	print 'Extract venue name and coordis with mincheckin of %d'%minCheckin
	venues=extract_cls_venue(fn_venue,minCheckin)

	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export possible venue related tweets'
	data=init_clusters(len(venues),venues.keys())
	for venue in venues:
		pg.cur.execute('''
			select * from %s where txt like '%%%s%%';'''%(tbname,venue.replace("'","''")))
		data[venue]=pg.cur.fetchall()

	print 'Filter venue related tweets'
	for venue in venues:
		coordi=venues[venue]
		data[venue]=process_venue_tweet(data[venue],coordi,maxDist)

	print 'Write results'
	write_venue_tweet(fn_venue,data)




	
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
def process_venue_name(tweet,auto):
	#format: I'm at .... - for #activity/#neighborhood ... 
	#(Pittsburgh/#other_city_name, PA)/in Pittsburgh/#orther_city_name,PA w/ #number others
	venues=[]
	if auto=='4sq':
		tweet=tweet.replace("I'm at ",'')
		tweet=tweet.strip()
		if len(tweet)>0:
			tweet=tweet.split(' ')
			if '' in tweet:
				tweet.remove('')
			i=0
			venue=''
			while i<len(tweet):
				if tweet[i]=='-':
					break
				elif '(' in tweet[i]:
					break
				elif tweet[i]=='w/':
					break
				elif tweet[i]=='in':
					break
				elif tweet[i]=='for':
					break
				elif tweet[i][-1]==',':
					venue+=' %s'%tweet[i][:-1]
					break
				else:
					venue+=' %s'%tweet[i]		
				i+=1
			return venue.strip()
		else:
			return ''
	elif auto=='inst':
		tweet=tweet.replace('Just posted a photo ','')
		tweet=tweet.strip()
		if len(tweet)>0:
			tweet=tweet.split(' ') 
			if '' in tweet:
				tweet.remove('')
			i=0
			venue=''
			while i<len(tweet):
				if tweet[i]=='-':
					break
				elif '(' in tweet[i]:
					break
				elif tweet[i][-1]==',':
					venue+=' %s'%tweet[i][:-1]
					break				
				else:
					venue+=' %s'%tweet[i]
				i+=1
			return venue.strip()

		else:
			return ''

def merge_venue(venues):
	venue_merge=[venues[0]]

	for venue_1 in venues[1:]:
		add=venue_1
		remove=False
		name_1=venue_1[0]
		coordi_1=[venue_1[1],venue_1[2]]
		for venue_2 in venue_merge:
			name_2=venue_2[0]
			coordi_2=[venue_2[1],venue_2[2]]		
			if toMerge(name_1,name_2) and isNeighbor(coordi_1,coordi_2):				
				name=toMerge(name_1,name_2)
				coordi=isNeighbor(coordi_1,coordi_2)				
				add=(name,coordi[0],coordi[1])
				remove=venue_2
				print "-- Merge '%s' %s and '%s' %s to '%s' %s"\
					%(name_2,coordi_2,name_1,coordi_1,name,coordi)
				break
		if remove:
			venue_merge.remove(remove)
		venue_merge+=[add]

	return venue_merge

def toMerge(name,name_0):
	term=name.split(' ')
	term_0=name_0.split(' ')
	if len(term)==1 and len(term_0)==1: #unigram term with same word stem 
		if pre.stemming(term[0])==pre.stemming(term_0[0]):
			#print '>> MAY MERGE %s AND %s DUE TO SAME WORD-STEM'%(name,name_0)
			return name
		else:
			return False
	elif contain(term,term_0):#term contains term_0
		#print '>> MAY MERGE %s AND %s DUE TO INCLUSION'%(name,name_0)
		return name
	elif contain(term_0,term): #term contained by term_0
		#print '>> MAY MERGE %s AND %s DUE TO INCLUSION'%(name,name_0)
		return name_0
	elif intersect(term,term_0): 
	#term intersects with term_0, at least half of the longer term is same	
		#print '>> MAY MERGE %s AND %s DUE TO INTERSECTION'%(name,name_0)
		return intersect(term,term_0)	
	else:
		return False

def contain(term,term_0):
	if len(term)>len(term_0):
		for word in term_0:
			if word not in term:
				return False
		return True
	else:
		return False

def intersect(term,term_0):
	same=set(term)&set(term_0)
	if len(same)==0:
		return False
	elif len(same)<min(len(term),len(term_0))/2:
		return False
	else:
		idx=[i for i in xrange(len(term)) if term[i] in same]
		idx_0=[i for i in xrange(len(term_0)) if term_0[i] in same]
		if join(idx,idx_0,term,term_0):
			return join(idx,idx_0,term,term_0)
		elif join(idx_0,idx,term_0,term):
			return join(idx_0,idx,term_0,term)
		else:
			return False

def join(idx,idx_0,term,term_0):
	if idx[0]==0 and idx_0[-1]==len(term_0)-1:
		sta=term_0[:idx_0[0]]
		end=term[idx[-1]+1:]
		if term_0[idx_0[0]:]==term[:idx[-1]+1]:
			multi=sta+term[:idx[-1]+1]+end
			return ' '.join(multi)
		else:
			return False

def isNeighbor(coordi1,coordi2,thresh=10):
	coordi1=[coordi1[1],coordi2[0]]
	coordi2=[coordi2[1],coordi2[0]]

	if great_circle(coordi1,coordi2).meters>thresh:
		return False
	else:
		return [(coordi1[1]+coordi2[1])/2,(coordi1[0]+coordi2[0])/2]

def dist(coordi1,coordi2):
	coordi1=[coordi1[1],coordi1[0]]
	coordi2=[coordi2[1],coordi2[0]]

	return great_circle(coordi1,coordi2).meters


'''------------------------------------------------------------------'''
def extract_venue_cls(fname,cat=False,sep='\t'):
	data=file('txt\\venue\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	if cat:
		venues=[(data[i][1],float(data[i][2]),float(data[i][3]),data[i][4]) for i in xrange(n)]
	else:
		venues=[(data[i][1],float(data[i][2]),float(data[i][3])) for i in xrange(n)]
	clid=[int(data[i][0]) for i in xrange(n)]
	k=max(clid)+1

	venue_cl=init_clusters(k,range(k))
	for cl in xrange(k):
		venue_cl[cl]=list(set([venues[i] for i in xrange(n) if clid[i]==cl]))
		if venue_cl[cl]==[]:
			del venue_cl[cl] #remove cluster with no venue

	return venue_cl


def search_venue_cl(client,venues,maxDist):
	venue_remove=[]
	venue_new=[]
	for venue in venues:
		(name,lon,lat)=venue
		coordi_0=[lon,lat]
		search=client.venues.search(params={'query':"%s"%name,'near':'pittsburgh'})
		remove=True
		if len(search['venues'])>0:
			venue_4sq=best_match(name,coordi_0,search['venues'])
			coordi=[venue_4sq['location']['lng'],venue_4sq['location']['lat']]
			d=dist(coordi_0,coordi)
			if d<maxDist:
				if len(venue_4sq['categories'])>0:
					cat=venue_4sq['categories'][0]
					[lon,lat]=coordi
					venue_new+=[(name,lon,lat,'%s {%s}'%(cat['name'].encode('utf-8'),cat['id'].encode('utf-8')))]
					remove=False
				
		if remove:
			print '-- cannot find venue %s near (%0.4f,%0.4f)'%(name,lon,lat)
			venue_remove+=[venue]

	return venue_new,venue_remove

def best_match(name_0,coordi_0,venues):
	coordis=[[venue['location']['lng'],venue['location']['lat']] for venue in venues]
	dists=[dist(coordi_0,coordi) for coordi in coordis]
	idx_d=list(np.argsort(dists))
	
	names=[venue['name'] for venue in venues]
	same=[len(set(name_0.split(' '))&set(name.split(' '))) for name in names]
	same_uni=list(set(same))
	idx_n=list(np.argsort(same_uni)[::-1])
	
	n=len(venues)
	rank=[idx_d.index(i)+idx_n.index(same_uni.index(same[i])) for i in xrange(n)]
	idx=rank.index(min(rank))
	
	return venues[idx]

'''------------------------------------------------------------------'''
def process_category_hier(cat_dict,cat_hier=''):
	cat_list=[]
	for cat in cat_dict:
		hier=cat_hier+'\%s'%cat['name']
		cat_list+=[[cat['name'],hier,cat['id']]]
		if cat['categories']!=[]:
			cat_list+=process_category_hier(cat['categories'],hier)

	return cat_list

def write_venue_category_hier(cat_list,sep='\t'):
	f=os.open('txt\\venue\\venue_cat_hier.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'name,hier,id\n'.replace(',',sep))
	
	for cat in cat_list:
		[name,hier,id]=cat
		os.write(f,('%s,%s,%s\n'%(name.encode('utf-8'),hier.encode('utf-8'),
			id.encode('utf-8'))).replace(',',sep))

	os.close(f)

'''------------------------------------------------------------------'''
def extract_venue_cat(fname,sep='\t'):
	data=file('txt\\venue\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	name=[data[i][0] for i in xrange(n)]
	hier=[data[i][1][1:].split('\\') for i in xrange(n)]
	ID=[data[i][2] for i in xrange(n)]

	return init_clusters(len(ID),ID,[[name[i],hier[i]] for i in xrange(n)])

def venue_category_sum_cl(venues,cats,uses):
	venues=[venue[-1] for venue in venues]
	venue_id=[venue[venue.index('{')+1:-1] for venue in venues]
	#cat[id]=[name,hier]
	venue_cat=[cats[ID][1][0] for ID in venue_id]


	return [venue_cat.count(use) for use in uses]	

'''------------------------------------------------------------------'''
def process_venue_profile(data,cat_dict):
	n=len(data)
	venue_profile=[]
	if n>0:
		cats=[data[i][3] for i in xrange(n)]
		use=['' for cat in cats]
		for i in xrange(len(use)):
			if cats[i]=='NA':
				use[i]=cats[i]
			else:
				cat_id=cats[i][cats[i].index('{')+1:-1]
				use[i]=cat_dict[cat_id][1][0]
		
		checkin=[tuple(data[i][:3]+[use[i]]) for i in xrange(n)]
		venues=list(set(checkin))
		
		for venue in venues:
			user=[data[i][4] for i in xrange(n) if checkin[i]==venue]
			venue_profile+=[list(venue)+[len(set(user)),len(user)]]

	return venue_profile
	


def write_venue_shp(fname,venue_cl,cls):
	fields=[['ID','N',8],['CLUSTER','N',8],['VENUE','C',50],
	['CATEGORY','C',50],['USER','N',8],['CHECKIN','N',8]]
	f=shapefile.Writer(shapeType=1)
	f.autoBalance = 1
	for field in fields:
		f.field(field[0],field[1],field[2])

	id=1
	for cl in cls:
		data=venue_cl[cl]
		n=len(data)
		for i in xrange(n):
			lon=float(data[i][1])
			lat=float(data[i][2])
			venue=data[i][0]
			cat=data[i][3]			
			user=data[i][4]
			checkin=data[i][5]
			f.point(lon,lat)
			f.record(ID=id,CLUSTER=cl,VENUE=venue,CATEGORY=cat,USER=user,CHECKIN=checkin)
			id+=1

	
	f.save('shp\\'+fname+'.shp')	

'''------------------------------------------------------------------'''	
def process_pop_venue(venues,m):
	name=[venue[0] for venue in venues]
	checkin=[int(venue[-1]) for venue in venues]
	user=[int(venue[-2]) for venue in venues]
	n=len(venues)
	venue_checkin=[(name[i],checkin[i]) for i in xrange(n)]
	venue_checkin=np.array(venue_checkin,dtype=[('venue','S50'),('checkin','i')])
	venue_checkin=np.sort(venue_checkin,order='checkin')[::-1]

	venue_user=[(name[i],user[i]) for i in xrange(n)]
	venue_user=np.array(venue_user,dtype=[('venue','S50'),('user','i')])
	venue_user=np.sort(venue_user,order='user')[::-1]
	if n>m:
		return [venue_checkin[:m],venue_user[:m]]
	else:
		venue_checkin=np.concatenate((venue_checkin,[('',0) for i in xrange(n,m)]),axis=0)
		venue_user=np.concatenate((venue_user,[('',0) for i in xrange(n,m)]),axis=0)
		return [venue_checkin,venue_user]

'''------------------------------------------------------------------'''

def entropy_cl(pg,cl,uses,how):
	use_how=init_clusters(len(how),how,[[0 for use in uses] for t in how])
	for use in uses:
		pg.cur.execute('''
			select dow*24+hour as how,count(*) from %s
			where clid=%d and cat_main='%s' group by how order by how;
			'''%(pg.tbname,cl,use))
		n=pg.cur.rowcount
		if n>0:
			data=pg.cur.fetchall()
			for pair in data:
				use_how[pair[0]][uses.index(use)]=int(pair[1])

	entros=[entropy(use_how[t]) for t in how]

	return sum(entros)

def entropy(uses):
	pk=1.0*np.array(uses)/np.sum(uses)
	entro=-1*sum([p*np.log2(p) for p in pk if p>0])

	return entro
'''------------------------------------------------------------------'''
def export_pop_venue_cls(pg,cl,uses,minN,maxN,order):
	mtx=[]
	for use in uses:
		pg.cur.execute('''
			select venue,cat,%s from %s where clid=%d and cat='%s' and %s>=%d
			order by %s desc;
			'''%(order,pg.tbname,cl,use,order,minN,order))
		n=pg.cur.rowcount
		if n>0:
			mtx+=pg.cur.fetchall()

	mtx=np.array(mtx,dtype=[('venue','S50'),('cat','S50'),('N','i')])
	mtx=np.sort(mtx,order='N')[::-1]
	if len(mtx)>maxN:
		mtx=mtx[:maxN]

	return mtx



'''--- TEST FUNCTION ---'''
'''
cat=[dict(),dict()]
cat_1=dict({'name':'aa','id':'001','categories':[]})
cat_2=dict({'name':'ab','id':'002','categories':[]})
cat[0]={'name':'a','id':'01','categories':[cat_1,cat_2]}
cat[1]={'name':'b','id':'02','categories':[]}
print process_category_hier(cat)


ID='CLLMAZ0UKCRB2NFMQPB1MVM3VVAFYOABWIFZO3PQC4QRKVNB'
secret='YTNNANYZHG0C2G1TRHQTMZDSWZDRDXDY04KI4DRNL3JBGQEY'

client=foursquare.Foursquare(client_id=ID,client_secret=secret)
coordi_0=[-80.0056,40.4469]
name_0="PNC Park"
search=client.venues.search(params={'query':name_0,'near':'pittsburgh'})
if len(search['venues'])>0:
	for venue in search['venues']:
		coordi=[venue['location']['lng'],venue['location']['lat']]
		print '%s,%s,%0.5f'%(venue['name'],venue['categories'][0]['name'],dist(coordi_0,coordi))
	venue=best_match(name_0,coordi_0,search['venues'])
	coordi=[venue['location']['lng'],venue['location']['lat']]
	print '---'
	print '%s,%s,%0.5f'%(venue['name'],venue['categories'][0]['name'],dist(coordi_0,coordi))
	

search=client.venues.search(params={'query':'joe mama','near':'pittsburgh'})
coordi_0=[-80.0047,40.4415]
if len(search['venues'])>0:
	coordis=[[venue_4sq['location']['lng'],venue_4sq['location']['lat']] for venue_4sq in search['venues']]
	dists=[dist(coordi_0,coordi) for coordi in coordis]
	print dists
	idx=dists.index(min(dists)) #find the nearest searched venue
	print idx
	if isNeighbor(coordi_0,coordis[idx],thresh=50): #<50m 
		venue_4sq=search['venues'][idx]
		if len(venue_4sq['categories'])>0:
			cat=venue_4sq['categories'][0]
			[lon,lat]=coordis[idx]
			print lon,lat
			venue_new=[(name,lon,lat,'%s {%s}'%(cat['name'].encode('utf-8'),cat['id'].encode('utf-8')))]
			print venue_new
			

#coordis (40.4463,-80.0156)
def test(tweet):
	tweet=tweet.replace('Just posted a photo ','')
	tweet=tweet.strip()
	if len(tweet)>0:
		tweet=tweet.split(' ') 
		i=0
		venue=''
		print tweet
		while i<len(tweet):
			print i,tweet[i]
			if '(' in tweet[i]:
				break
			elif tweet[i]=='-':
				break
			elif tweet[i][-1]==',' or tweet[i][-1]=='.':
				venue+=' %s'%tweet[i][:-1]
				break			
			else:
				venue+=' %s'%tweet[i]
			i+=1
		print venue.strip()

tweet='Just posted a photo Duquesne Incline (Mt. Washington)'
test(tweet)

'''