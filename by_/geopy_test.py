
from geopy.distance import great_circle
import foursquare
import numpy as np



coordi1=(40.458221,-79.990604)
coordi2=(40.4567, -79.9906)
coordi3=(40.4281997680664,-79.9654006958008)

print great_circle(coordi1,coordi2).meters
print np.sqrt((coordi1[0]-coordi2[0])**2+(coordi1[1]-coordi2[1])**2)
'''
-79.9527	40.4446
-79.9538	40.4457
-79.9566	40.4419
-79.9603	40.4309

-79.9654998779297;40.4281005859375
-79.9654006958008;40.4281997680664
-79.9654006958008;40.4281997680664
'''
print 'Connect to 4sq database'
ID='CLLMAZ0UKCRB2NFMQPB1MVM3VVAFYOABWIFZO3PQC4QRKVNB'
secret='YTNNANYZHG0C2G1TRHQTMZDSWZDRDXDY04KI4DRNL3JBGQEY'

'''
client=foursquare.Foursquare(client_id=ID,client_secret=secret)

#print client.venues.categories()
print 'Search category of each venue'


search=client.venues.search(params={'query':'AMC Loews Waterfront','city':'pittsburgh',\
	'll':'40.4066,-79.9177','radius':'500'})
for venue in search['venues']:
	print venue['name']
	print venue['location']
	cat=venue['categories'][0]
	for item in cat:		
		print item,cat[item],type(cat[item])

cats=client.venues.categories()['categories']
for cat in cats:
	print cat,cats[cat]
'''

def venue_search_again(fname,foursq,sep='\t'):
	#search removed venue in 4sq venue database only with venue names
	print 'Extract removed venues'
	data=file('txt\\venue-profil\\'+fname+'.txt').readlines()[1:]
	n=len(data)
	data=[data[i][:-1].split(sep) for i in xrange(n)]
	names=[data[i][0] for i in xrange(n)]
	coordi=[data[i][1:3] for i in xrange(n)]

	venues=list(set(names))
	venues=init_clusters(len(venues),venues)
	for venue in venues:
		venues[venue]=[coordi[i] for i in xrange(n) if names[i]==venue][0]	

	print 'Connect to 4sq database'
	client=foursquare.Foursquare(client_id=foursq['id'],client_secret=foursq['secret'])

	print 'Search removed venue'
	remove=[]
	for venue in venues:
		coordi_0=venues[venue]
		name=venue.replace('&amp;','&')
	
		search=client.venues.search(params={'query':'%s'%name,'near':'pittsburgh'})

		coordis=[[venue_4sq['location']['lng'],venue_4sq['location']['lat']] for venue_4sq in search['venues']]
		if len(coordis)>0:
			dists=[dist(coordi_0,coordi) for coordi in coordis]
			venue_4sq=search['venues'][dists.index(min(dists))]
			venues[venue]+=[venue_4sq['name'],venue_4sq['location']['lng'],venue_4sq['location']['lat'],min(dists)]
		else:
			print '-- cannot find venue %s near Pittsburgh'%name
			remove+=[venue]

	print 'Delete %d venues'%len(remove)
	for venue in remove:
		del venues[venue]

	print 'Write new found venue'
	f=os.open('txt\\venue-profil\\'+fname+'_new.txt',os.O_RDWR|os.O_CREAT)
	os.write(f,'venue_remove,lon,lat,venue_found,lon,lat,dist\n'.replace(',',sep))

	for venue in venues:
		[name,lon,lat,d]=venues[venue][2:]
		os.write(f,sep.join([venue]+venues[venue][:2]+[name.encode('utf-8'),'%f'%lon,'%f'%lat,'%0.3f'%d])+'\n')

	os.close(f)
'''
fname=tbname+'_venue_tweet_remove'
venue_profil.venue_search_again(fname,foursq)
'''