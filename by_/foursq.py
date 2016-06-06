import dbProcess, os, math, shapefile
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt



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
			select lon,lat,username,doy,dow,hour,senti_val from %s 
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
	os.write(f,'venue,lon,lat,user,doy,dow,hour,senti_val\n')

	for venue in venues:
		n=len(data[venue])
		print 'Venue: %s %d'%(venue,n)
		for i in xrange(n):
			(user,year,doy,dow,hour,lon,lat,senti_val)=data[venue][i]
			os.write(f,'%s,%0.4f,%0.4f,%s,%d,%d,%d,%0.3f\n'%(venue,lon,lat,user,year,doy,dow,hour,senti_val))

	os.close(f) 

def foursq_venue(fname,sep=','):
	print 'Extract 4sq tweets'
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	venues=list(set([data[i][0].replace('&amp; ','') for i in xrange(n)]))

	print 'Init venue book'
	book=init_clusters(len(venues),venues)
	for i in xrange(n):
		book[data[i][0].replace('&amp; ','')]+=[data[i][1:]]

	print 'Search and redefine diff venues with same name'
	for venue in venues:
		 if find_diff_venue(book[venue]):
		 	[book,n]=redef_venue(book,venue)
		 	print '-- %d venues named %s'%(n,venue)

	print 'Merge venues with similar name'
	venues=book.keys()
	while len(venues)>1:
		venue_0=venues[0]
		venue_remove=[venue_0]
		for venue in venues[1:]:
			if to_merge(venue_0,venue,book):
				print '-- merge %s with %s'%(venue_0,venue)
				name=merge_venue_name(venue_0,venue)
				book[name]=book[venue]+book[venue_0]
				del book[venue]
				venue_remove+=[venue]
		if len(venue_remove)>1:
			del book[venue_0]
		for venue in venue_remove:
			venues.remove(venue)

	print 'Process venue stats'
	venues=book.keys()
	stats=init_clusters(len(venues),venues)
	for venue in venues:
		stats[venue]=process_venue_stats(book[venue])

	print 'Sort venue by check-in'
	checkin=[stats[venue][2] for venue in venues]
	idx=np.argsort(checkin)[::-1]
	venues=[venues[i] for i in idx]

	print 'Write results'
	f=os.open('txt\\'+fname[:5]+fname[5:].replace('tweet','venue')+'.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,lon,lat,checkin_n,user_n,doy_mode,dow_mode,hour_mode,senti_avg\n')

	for venue in venues:
		(lon,lat,checkin,user,doy,dow,hour,senti)=stats[venue]
		os.write(f,'%s,%0.4f,%0.4f,%d,%s,%d,%d,%d,%0.3f\n'%(venue,lon,lat,checkin,user,doy,dow,hour,senti))

	os.close(f) 

def foursq_venue_map(fname,minCheckin=1,sep=','): #draw map and write gis shapefile of venues with minCheckin
	print 'Extract 4sq venues'
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	data=[data[i] for i in xrange(n) if int(data[i][3])>minCheckin]
	
	print 'Draw png map'
	draw_venue_map(fname,data)

	print 'Write shp file'
	write_venue_shp(fname,data)

def foursq_err_tweet(fname,tbname,dbname='tweet_pgh',user='postgres',sep=',',thresh=0.0001): #search venue with non-zero senti
	print 'Extract 4sq venues and search venues with error senti'
	data=file('txt\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	venues=[tuple(data[i][:3]) for i in xrange(n) if abs(float(data[i][-1])-0)>thresh]

	print 'Create pgcontroller'
	pg=dbProcess.PgController(tbname,dbname,user)

	print 'Export auto-tweet of err venues'
	data=init_clusters(len(venues),venues)
	venue_remove=[]
	for venue in venues:
		(name,lon,lat)=venue
		if '_' in name:
			name=name[:name.find('_')]
		coordi=(float(lon),float(lat))
		data[venue]=export_err_tweet(pg,name,coordi)
		if len(data[venue])>0:
			print '-- venue %s err tweet-txt %d'%(venue[0],len(data[venue]))
		else:
			print '-- venue %s no tweet found'%venue[0]
			del data[venue]
			venue_remove+=[venue]

	for venue in venue_remove:
		venues.remove(venue)

	print 'Write results'
	f=os.open('txt\\'+fname.replace('venue','tweet')+'_err_senti.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue,lon,lat,txt,senti\n')

	for venue in venues:
		tweets=data[venue]
		for tweet in tweets:
			os.write(f,','.join(list(venue))+',%s,%0.4f'%(tweet[0],tweet[1])+'\n')		

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
def process_venue_name(txt):
	#format: I'm at .... - for #activity/#neighborhood ... 
	#(Pittsburgh/#other_city_name, PA)/in Pittsburgh/#orther_city_name,PA w/ #number others
	venues=[]
	for tweet in txt:
		tweet=tweet.replace("I'm at ",'')
		tweet=tweet.split(' ')
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
			else:
				venue+=' %s'%tweet[i]
				i+=1
		venues+=[venue[1:]]

	return list(set(venues))

'''------------------------------------------------------------------'''
def find_diff_venue(data,d=0.01):
	n=len(data)
	coordi_0=(float(data[0][0]),float(data[0][1]))
	i=1
	while i<n:
		coordi=(float(data[i][0]),float(data[i][1]))
		if dist(coordi,coordi_0)>d:
			return True
		i+=1
	return False

def redef_venue(book,venue,d=0.01):
	data=book[venue]
	n=len(data)
	coordis=venue_coordis([(float(data[i][0]),float(data[i][1])) for i in xrange(n)],d)
	m=len(coordis)
	for i in xrange(m):
		coordi=coordis[i]
		name='%s_%d'%(venue,i+1)
		book[name]=[data[j] for j in xrange(n) if dist((float(data[j][0]),float(data[j][1])),coordi)<d]
	del book[venue]

	return book,m

def venue_coordis(coordis,d):
	venues=[[coordis[0],1]]
	for coordi in coordis[1:]:
		new=True
		for venue in venues:
			if dist(coordi,venue[0])<d:
				(lon_0,lat_0)=venue[0]
				n=venue[1]
				(lon,lat)=coordi
				venue[0]=((lon_0*n+lon)/(n+1),(lat_0*n+lat)/(n+1))
				venue[1]+=1
				new=False			
		if new:
			venues+=[[coordi,1]]

	return [venue[0] for venue in venues]

def dist(coordi,coordi_0):
	(lon,lat)=coordi
	(lon_0,lat_0)=coordi_0
	return math.sqrt((lon-lon_0)**2+(lat-lat_0)**2)

def to_merge(venue_0,venue,book,d=0.01):
	if merge_venue_name(venue_0,venue):
		coordi_0=venue_center(book[venue_0])
		coordi=venue_center(book[venue])
		if dist(coordi_0,coordi)<d:
			return True
	return False
	
def merge_venue_name(venue_0,venue):
	name=venue[:venue.find('_')] if '_' in venue else venue
	name_0=venue_0[:venue_0.find('_')] if '_' in venue_0 else venue_0
	if name_0.find(' ')>0 and name in name_0:
		return venue_0
	elif name.find(' ')>0 and name_0 in name:
		return venue
	return False


def process_venue_stats(data):
	n=len(data)
	(lon,lat)=venue_center(data)
	checkin=n
	user=len(list(set([data[i][2] for i in xrange(n)])))
	[doy,dow,hour]=[stats.mode([int(data[i][j]) for i in xrange(n)])[0][0] for j in xrange(3,6)]
	senti=np.mean([float(data[i][-1]) for i in xrange(n)])

	return [lon,lat,checkin,user,doy,dow,hour,senti]

def venue_center(data):
	n=len(data)
	lon=np.mean([float(data[i][-3]) for i in xrange(n)])
	lat=np.mean([float(data[i][-2]) for i in xrange(n)])
	return (lon,lat)

'''------------------------------------------------------------------'''
def draw_venue_map(fname,data):
	plt.figure()
	n=len(data)
	x=[float(data[i][1]) for i in xrange(n)]
	y=[float(data[i][2]) for i in xrange(n)]
	plt.scatter(x,y,marker='o')
	plt.xlabel('lon')
	plt.ylabel('lat')
	plt.xlim(-80.10,-79.86)
	plt.ylim(40.36,40.51)
	plt.title('Foursquare Venues')
	plt.savefig('png\\'+fname+'.png')

def write_venue_shp(fname,data):
	fields=[['NAME','C',50],['CHECKIN','N',8],['USER','N',8]]
	f=shapefile.Writer(shapeType=1)
	f.autoBalance = 1
	for field in fields:
		f.field(field[0],field[1],field[2])

	n=len(data)
	for i in xrange(n):
		name=data[i][0]
		lon=float(data[i][1])
		lat=float(data[i][2])
		checkin=int(data[i][3])
		user=int(data[i][4])
		f.point(lon,lat)
		f.record(NAME=name,CHECKIN=checkin,USER=user)
	f.save('shp\\'+fname+'.shp')	

'''------------------------------------------------------------------'''
def export_err_tweet(pg,name,coordi,d=0.02,thresh=0.0001):
	name=name.replace("'","''")
	query=create_venue_query(name)
	pg.cur.execute('''
		select lon,lat,txt,senti_val from %s 
		where auto_tweet='4sq' and %s and abs(senti_val-0)>%f;
		'''%(pg.tbname,query,thresh))
	data=pg.cur.fetchall()
	n=pg.cur.rowcount
	coordis=[(data[i][0],data[i][1]) for i in xrange(n)]
	data=[data[i][2:] for i in xrange(n) if dist(coordis[i],coordi)<d]
	n=len(data)

	tweet=[]
	if len(data)>0:
		txts=list(set([data[i][0] for i in xrange(n)]))		
		for txt in txts:
			senti=np.mean([data[i][1] for i in xrange(n) if data[i][0]==txt])
			tweet+=[[txt,senti]]

	return tweet
	
		
def create_venue_query(name):
	name=name.split(' ')
	n=len(name)
	if n==1:
		return "txt like '%%I''m at %s%%'"%name[0]
	else:
		query="("
		for i in xrange(n-1):
			pair=' '.join([name[i],name[i+1]])
			query+="txt like '%%I''m at %s%%' or "%pair
		return query[:-4]+")"


'''--- TEST FUNCTION ---'''
'''
txt=["I'm at AMC Loews Waterfront 22 - for The LEGO Movie (Homestead, PA) w/ 3 others",
"I'm at Goodwill / ComputerWorks - Lawrenceville - (Pittsburgh, PA)",
"I'm at Sienna Mercato w/",
"I'm at AMC Loews Waterfront 22 - for The LEGO Movie (Homestead, PA) w/ 3 others"]

print process_venue_name(txt)

print set([(0,1),(0,2),(1,2),(1,2)])

a=dict()
a[(0,1)]=[0]
print a

print merge_venue_name("Pittsburgh_9","Yolanda's Hair Design")
print dist((-80.0039,40.4468),(-79.9843,40.4514))

print create_venue_query('AMC Loews Waterfront 22')

'''