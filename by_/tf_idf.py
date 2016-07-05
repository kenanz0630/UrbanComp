#import tweet_emoji
import copy
import numpy as np
import preprocessing as pre
from collections import Counter

'''--- MAIN FUNCTION ---'''
def tf(counter,data,type): 
	n=len(data)
	if type=='term':
		for i in xrange(n):
			counter.update(data[i])
	if type=='emoji':
		ref=tweet_emoji.init_emoji()
		for i in xrange(n):
			if data[i]!=None:
				emoji=unicode(data[i],'utf-8')
				counter.update(re.findall(ref,emoji))
	if type=='tag':
		for i in xrange(n):
			counter.update(data[i])

	return counter


def tf_idf(counter,counter_cl,N):
	tfIdf=dict()
	for term in counter_cl:
		#print term,counter[term],counter_cl[term]
		tfIdf[term]=np.log(1.0*N/counter[term]+1)*counter_cl[term]

	return tfIdf

def screen(mtx,m,type):
	k=len(mtx)
	if type=='term': 
		print '-- Init self-merged'
		mtx_sort=[np.array(mtx[i],dtype=[('term','S50'),('tf-idf','f')]) for i in xrange(k)]
		mtx_sort=[np.sort(mtx_sort[i],order='tf-idf')[::-1] for i in xrange(k)]
		mtx=[[] for i in xrange(k)]
		for i in xrange(k):
			[mtx[i],mtx_sort[i]]=self_merge(mtx_sort[i],m)
		
		print '-- Iteratively remove shared terms and self merge'
		mtx=no_common(mtx,mtx_sort,m,1)	

		print 'Self cleaning'
		mtx=[self_clean(mtx[i]) for i in xrange(k) if len(mtx[i])>0]
		
		return mtx
	
	if type=='tag':
		mtx_sort=[np.array([(data[j],mtx[i][j]) for j in xrange(n)],dtype=[('tag','S50'),('tf-idf','f')]) for i in xrange(k)]
		mtx_sort=[np.sort(mtx_sort[i],order='tf-idf')[::-1] for i in xrange(k)]
		
		return mtx
	


'''--- HELPER FUNCTION ---'''
'''------------------------------------------------------------------'''
def self_merge(mtx,m):
	'''term self-merge of multi-gram'''
	mtx_merge=[mtx[0]]
	term_merge=[mtx['term'][0]]
	
	while len(term_merge)<m: 
		#update mtx
		mtx=mtx[1:]

		if len(mtx)==0: #no more term to be merged
			mtx_merge+=[('',0.0) for k in xrange(len(term_merge),m)]
			break
		
		#new term merging
		term=mtx['term'][0]	
		merge_result=self_merge_one(term_merge,term)
		[term_merge,mtx_merge]=self_merge_two(term_merge,mtx_merge,term,mtx,merge_result)		
		
		#existing term merging
		term_copy=copy.copy(term_merge[1:])
		mtx_copy=copy.copy(mtx_merge[1:])
		term_merge=[term_merge[0]]
		mtx_merge=[mtx_merge[0]]
		for i in xrange(len(term_copy)):
			term=term_copy[i]
			merge_result=self_merge_one(term_merge,term)
			[term_merge,mtx_merge]=self_merge_two(term_merge,mtx_merge,term,mtx_copy,merge_result)		
			mtx_copy=mtx_copy[1:]

	mtx_merge=np.array(mtx_merge,dtype=[('term','S50'),('tf-idf','f')])
		
	return mtx_merge,mtx

def self_merge_one(term_merge,term_0):	
	remove=[]
	merge=False #whether term_0 merges with any term in term_merg
	replace=False #term_merge idx if term_0 shall be inserted
	new_term=False
	for term in term_merge: #check all terms in current term_merge and store merge-term idxs in remove
		to_merge=toMerge(term,term_0)
		if to_merge:
			merge=True
			if to_merge=='be_contained': #term is contained by term_0, shall be replaced
				idx=term_merge.index(term)
				if replace is False: #term_0 has not replace any previous term in term_merge
					replace=idx
				else:
					remove+=[idx]
				print '-- %s IS CONTAINED BY %s'%(term,term_0)
			elif to_merge=='contain':
			 	print '-- %s CONTAINS %s'%(term,term_0)
			elif to_merge=='same_stem':
				print '-- %s and %s HAS SAME WORD STEM'%(term_0,term)
			else:
				print '-- %s and %s MERGE TO BE %s'%(term_0,term,to_merge) 
				term_0=to_merge
				new_term=to_merge
				idx=term_merge.index(term)
				if not replace:						
					replace=idx
				else:
					remove+=[idx]
	
	return merge,replace,remove,new_term

def self_merge_two(term_merge,mtx_merge,term,mtx,merge_result):
	[merge,replace,remove,new_term]=merge_result
	if merge:
		if replace is not False: #replace term
			if new_term:										
				term=new_term
			print '>> REPLACE %s BY %s'%(term_merge[replace],term)
			term_merge=term_merge[:replace]+[term]+term_merge[replace+1:]
			mtx_merge[replace][0]=term
		if len(remove)>0: #remove terms
			print '>> REMOVE %s'%','.join([term_merge[i] for i in remove])		
			for i in remove:
				term_merge.remove(term_merge[i])
				mtx_merge.remove(mtx_merge[i])
			#print '>> TEST: %s exist?'%test,(test in term_merge)
	else:
		term_merge+=[term]
		mtx_merge+=[mtx[0]]

	return term_merge,mtx_merge

def toMerge(term,term_0):
	term=term.split(' ')
	term_0=term_0.split(' ')
	if len(term)==1 and len(term_0)==1: #unigram term with same word stem 
		if pre.stemming(term[0])==pre.stemming(term_0[0]):
			return 'same_stem'
		else:
			return False
	elif contain(term,term_0):#term contains term_0
		return 'contain'
	elif contain(term_0,term): #term contained by term_0
		return 'be_contained'		
	elif intersect(term,term_0): 
	#term intersects with term_0, at least half of the longer term is same		
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
	elif len(same)<min(len(term)/2,len(term_0)/2): #at least half of the shorter term is same
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

'''------------------------------------------------------------------'''		
def no_common(mtx,mtx_sort,m,iter,maxTf=2,limit=20):
	if iter>limit:
		return mtx
	print 'Iteration %d'%iter	
	mtx=remove_common(mtx,maxTf) #remove terms appear in cls for more than maxTf times

	fill=False #fill empty space in cls after removing common terms
	for i in xrange(len(mtx)):
		if len(mtx[i])<m:
			if len(mtx_sort[i])>0:
				print 'Fillup cluster%d'%(i+1)
				mtx[i]=np.concatenate((mtx[i],mtx_sort[i]),axis=0)
				[mtx[i],mtx_sort[i]]=self_merge(mtx[i],m)
				fill=True
			else:
				empty=[('',0.0) for j in xrange(len(mtx[i]),m)]
				empty=np.array(empty,dtype=[('term','S50'),('tf-idf','f')])
				mtx[i]=np.concatenate((mtx[i],empty),axis=0)
			
	if fill:
		return no_common(mtx,mtx_sort,m,iter+1)
	else:
		return mtx

def remove_common(mtx,maxTf):
	k=len(mtx)
	counter=Counter()
	for i in xrange(k):
		counter.update(mtx[i]['term'])
	remove=[item[0] for item in counter.items() if item[1]>maxTf and item[0]!='']
	if len(remove)>0:
		print '>> REMOVE TERMS SHARED BY CLUSTER %s'%','.join(remove)
		for i in xrange(k):
			m=len(mtx[i])
			mtx[i]=[mtx[i][j] for j in xrange(m) if mtx[i]['term'][j] not in remove]
			mtx[i]=np.array(mtx[i],dtype=[('term','S50'),('tf-idf','f')])

	return mtx


def self_clean(mtx,maxTf=3):
	counter=Counter()
	n=len(mtx)	
	term=[mtx['term'][i].split(' ') for i in xrange(n)]	
	for i in xrange(n):
		counter.update(term[i])
	remove=[item[0] for item in counter.items() if item[1]>maxTf]
	toRemove=[False for word in remove]
	m=len(remove)

	for i in xrange(n):
		term=mtx['term'][i].split(' ')				
		for j in xrange(m):
			if remove[j] in term and toRemove[j]:
				term.remove(remove[j])
			else:	
				toRemove[j]=True
		mtx['term'][i]=' '.join(term)
				

	mtx=[mtx[i] for i in xrange(n) if mtx['term'][i]!='']
	mtx=np.array(mtx,dtype=[('term','S50'),('tf-idf','f')])


	return mtx


'''------------------------------------------------------------------'''

'''--- TEST FUNCTION ---'''
'''
print toMerge('peace love little','little donuts')

a=[('hellow',12),('hellow world',10),('morning',8),('night',6),('noon',10)]
b=[('hellow world',11),('morning world',10),('good night',8),('night',7)]
a=np.array(a,dtype=[('term','S50'),('tf-idf','f')])
b=np.array(b,dtype=[('term','S50'),('tf-idf','f')])
mtx=[a[:2],b[:2]]
mtx_sort=[a[2:],b[2:]]
print no_common(mtx,mtx_sort,2,1,maxTf=1)


mtx=screen(mtx,2,'term')
for m in mtx:
	print m

mtx=np.array([[1,2,3,3],[2,3,4,4]])
print mtx
print mtx/np.reshape([1,2],(2,1))


[a,b]=init_default([])
l=['apple','like']
print process(l,b,a)


data=file('txt\\tweet_oneyear_pos_sup_term_tfidf.txt').readlines()[2:]
n=len(data)
data=[data[i][:-2].split(',') for i in xrange(n)]
k=len(data[0])/2
mtx=[[] for i in xrange(k)]
for i in xrange(k):
	mtx[i]=[(data[j][2*i],float(data[j][2*i+1])) for j in xrange(n)]
	mtx[i]=np.array(mtx[i],dtype=[('term','S50'),('tf-idf','f')])

mtx=self_clean(mtx)
mtx=remove_same(mtx)
mtx=remove_common(mtx)

for term in mtx[2]:
	print term
#for term in mtx[25]:
#	print term

print pre.stemming(['pittsburgh','Pittsburgh'])

mtx=[('museum natural history',1),('you',2),('carnegie museum natural',3)]
mtx=np.array(mtx,dtype=[('term','S50'),('tf','i')])
print self_clean_merge(mtx)

print pre.stemming(['real'])
'''