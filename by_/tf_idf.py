import tweet_emoji
import copy
import numpy as np
import preprocessing as pre
from collections import Counter

'''
import os, re
import nltk.tag
from nltk.tag.perceptron import PerceptronTagger
import dbProcess

'''
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
	pairs=counter.items()
	n=len(pairs)
	term=[pairs[i][0] for i in xrange(n)]
	idf=np.array([pairs[i][1] for i in xrange(n)])
	idf=np.log(1.0*N/idf+1)

	tf=[counter_cl[term[i]] for i in xrange(n)]
	tfIdf=np.array(tf)*idf

	return term,tfIdf

def screen(data,mtx,m,type):
	(n,k)=(len(data),len(mtx))
	if type=='term': #term_
		mtx_sort=[np.array([(data[j],mtx[i][j]) for j in xrange(n)],dtype=[('term','S50'),('tf-idf','f')]) for i in xrange(k)]
		mtx_sort=[np.sort(mtx_sort[i],order='tf-idf')[::-1] for i in xrange(k)]
		mtx=[self_merge(mtx_sort[i],m) for i in xrange(k)]
		mtx=[np.array(mtx[i],dtype=[('term','S50'),('tf-idf','f')]) for i in xrange(k)]

		mtx=no_common(mtx,mtx_sort,m,1)	
		print 'Self cleaning & merging'
		mtx=self_clean(mtx)
		
		return mtx

	if type=='emoji':
		mtx_sort=[np.array([(data[j].encode('utf-8'),mtx[i][j]) for j in xrange(n)],dtype=[('emoji','S50'),('tf-idf','f')]) for i in xrange(k)]
		mtx_sort=[np.sort(mtx_sort[i],order='tf-idf')[::-1] for i in xrange(k)]
		mtx=[mtx_sort[i][:m] for i in xrange(k)]

		return [mtx_sort[i][:m] for i in xrange(k)]

	if type=='tag':
		mtx_sort=[np.array([(data[j],mtx[i][j]) for j in xrange(n)],dtype=[('tag','S50'),('tf-idf','f')]) for i in xrange(k)]
		mtx_sort=[np.sort(mtx_sort[i],order='tf-idf')[::-1] for i in xrange(k)]
		mtx=[self_merge_tag(mtx_sort[i],m) for i in xrange(k)]
		mtx=[np.array(mtx[i],dtype=[('tag','S50'),('tf-idf','f')]) for i in xrange(k)]

		return mtx
		
'''--- HELPER FUNCTION ---'''
'''------------------------------------------------------------------'''
def self_merge_tag(mtx,m):
	'''tag self-merge of upper/lower case'''
	mtx_merge=[mtx[0]]
	tag_merge=[mtx['tag'][0]]
	i=0
	j=1
	while i<m:
		if j>=len(mtx):
			mtx_merge+=[('',0.0) for k in xrange(i,m)]
			break		
		tag_0=mtx['tag'][j]
		merge=False
		tag_curr=tag_merge
		for tag in tag_curr:
			try:
				if pre.stemming(pre.tokenize(tag[1:])[0])[0]==pre.stemming(pre.tokenize(tag_0[1:])[0])[0]:
				 #remove#, lowercase, stem
					merge=True
					#print 'Self-merge: merge %s to %s'%(tag_0,tag)
			except:
				print 'Error:',tag_0,tag

		if not merge:
			tag_merge+=[tag_0]
			mtx_merge+=[mtx[j]]
			i+=1
		j+=1

	return mtx_merge

def self_merge(mtx,m):
	'''term self-term of multi-gram'''

	mtx_merge=[mtx[0]]
	term_merge=[mtx['term'][0]]
	i=0
	j=1
	while i<m:
		if j >=len(mtx):
			mtx_merge+=[('',0.0) for k in xrange(i,m)]
			break
		term_0=mtx['term'][j]
		merge=False
		replace=False
		term_curr=[term for term in term_merge]
		for term in term_curr:
			[tomerge,mtype]=toMerge(term,term_0)
			if tomerge:
				merge=True
				idx=term_merge.index(term)		
				#if abs(mtx['tf-idf'][j]-mtx_merge[idx][1])<0.01 and len(term_0)>len(term):
				if mtype=='contained': #term is contained by term_0
					#print "Self-merge: Replace %s by %s"%(term,term_0)
					term_merge.remove(term_merge[idx])
					mtx_merge.remove(mtx_merge[idx])
					if not replace:
						term_merge+=[term_0]
						mtx_merge+=[mtx[j]]
						replace=True				
		if not merge:
			term_merge+=[term_0]
			mtx_merge+=[mtx[j]]
			i+=1
		j+=1

	return mtx_merge

def toMerge(term,term_0):
	n=term.count(' ')
	n_0=term_0.count(' ')
	if n==n_0:
		if n==2: #tri-gram
			term=term.split(' ')
			term_0=term_0.split(' ')
			return (term[-2:]==term_0[:2] or term[:2]==term_0[1:]),'tri-gram'
		elif n>0:
			return term==term_0,'same'
		else:
			return pre.stemming(term)==pre.stemming(term_0),'stem'
	elif n>n_0:
		return term_0 in term,'contain'
	else:
		return term in term_0,'contained'

def multi_gram(term,term_0):
	if term[1]==term_0[-1]:
		return ' '.join([term_0[0]]+term)
	else:
		return ' '.join(term+[term_0[-1]])

'''------------------------------------------------------------------'''		
def no_common(mtx,mtx_sort,m,iter,limit=20):
	if iter>limit:
		return mtx
	print 'Iteration %d'%iter	
	mtx=remove_same(mtx,maxTf=2)
	fill=False
	for i in xrange(len(mtx)):
		if len(mtx[i])<m:
			print 'Fillup cluster%d'%(i+1)
			mtx[i]=fill_up(mtx[i],mtx_sort[i],m)
			fill=True
	if fill:
		return no_common(mtx,mtx_sort,m,iter+1)
	else:
		return mtx

def remove_same(mtx_term,maxTf):
	k=len(mtx_term)
	n=[len(mtx_term[i]) for i in xrange(k)]
	term=[[mtx_term[i]['term'][j] for j in xrange(n[i])] for i in xrange(k)]
	mtx_term=[remove_same_part(mtx_term[i],term[:i]+term[i+1:],maxTf) for i in xrange(k)]
	return mtx_term

def remove_same_part(self,other,maxTf):
	k=len(other)
	n=len(self)
	pool=[]
	for i in xrange(k):
		pool+=other[i]
	idx=range(n)
	for i in xrange(n):
		if pool.count(self['term'][i])>maxTf:
			idx.remove(i)
	self=[self[idx[i]] for i in xrange(len(idx))]
	return np.array(self,dtype=[('term','S50'),('tf-idf','f')])

def fill_up(mtx,mtx_sort,m):
	k=len(mtx)
	if k>0:
		idx=np.where(mtx_sort['term']==mtx['term'][-1])[0][0]
	else:
		idx=0	
	mtx=self_merge(np.concatenate((mtx,mtx_sort[idx+1:]),axis=0),m)

	return np.array(mtx,dtype=[('term','S50'),('tf-idf','f')])

def self_clean(mtx,maxTf=3):
	k=len(mtx)
	for i in xrange(k):
		mtx[i]=self_clean_merge(mtx[i])
		mtx[i]=self_clean_remove(mtx[i],maxTf)
		mtx[i]=np.array(mtx[i],dtype=[('term','S50'),('tf-idf','f')])

	return mtx

def self_clean_remove(mtx,maxTf):
	counter=Counter()
	n=len(mtx)	
	terms=[mtx['term'][i].split(' ') for i in xrange(n)]	
	for i in xrange(n):
		counter.update(terms[i])
	counter=counter.most_common()
	clean=[]
	i=0
	while counter[i][1]>maxTf:
		clean+=[counter[i][0]]
		i+=1
	toClean=[False for word in clean]
	
	for i in xrange(n):
		term_c=copy.copy(terms[i])
		for word in terms[i]:
			if word in clean:
				idx=clean.index(word)
				if toClean[idx]:
					term_c.remove(word)
				else:
					toClean[idx]=True
		mtx['term'][i]=' '.join(term_c)

	mtx=[mtx[i] for i in xrange(n) if mtx['term'][i]!='']
	mtx=np.array(mtx,dtype=[('term','S50'),('tf-idf','f')])

	return mtx

def self_clean_merge(mtx):
	mtx_merge=[mtx[0]]
	term_merge=[mtx['term'][0]]
	n=len(mtx)
	for i in xrange(1,n):
		term_0=mtx['term'][i]
		toAdd=True
		for term in term_merge:
			[tomerge,mtype]=toMerge(term,term_0)
			if tomerge:
				idx=term_merge.index(term)
				if mtype=='containted':
					term_merge.remove(term_merge[idx])
					mtx_merge.remove(mtx_merge[idx])
				elif mtype=='tri-gram': #tri-gram merge
					term_0=multi_gram(term.split(' '),term_0.split(' '))
					print 'Multi-gram merge: Replace %s by %s'%(term,term_0)
					term_merge.remove(term_merge[idx])
					mtx_merge.remove(mtx_merge[idx])
					mtx[i][0]=term_0
				else:
					toAdd=False
		if toAdd:
			term_merge+=[term_0]
			mtx_merge+=[mtx[i]]

	mtx=np.array(mtx_merge,dtype=[('term','S50'),('tf-idf','f')])
	
	return mtx

def remove_common(mtx,ext):
	common=init_default([],ext)[0]
	k=len(mtx)
	for i in xrange(k):
		mtx[i]=remove_common_part(mtx[i],common)

	return mtx

def remove_common_part(mtx,common):
	n=len(mtx)
	terms=mtx['term']
	for term in terms:
		if toRemove(term,common):
			idx=np.where(mtx['term']==term)
			mtx=np.delete(mtx,idx,0)

	return mtx

'''------------------------------------------------------------------'''

'''--- TEST FUNCTION ---'''
'''
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