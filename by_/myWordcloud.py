from wordcloud import WordCloud
from wordcloud import get_single_color_func
from PIL import ImageColor
import os, colorsys
import dbscan, dbProcess
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as colors
import matplotlib.cm as cmx


'''--- MAIN FUNCTION ---'''
def wordCloud_single(fname,name,color):
	print 'Extract data'
	[data,senti]=extract_data_single(fname)
	k=len(senti)

	print 'Process tf and senti'
	data=[process_data_single(data[i]) for i in xrange(k)]

	print 'Process colors'
	colors=process_color(senti,color)

	for i in xrange(k):
		key=name+'%d'%(i+1)
		print "Draw wordcloud of %s"%key
		draw_wordcloud_single(fname+'_%s'%key,key,colors[i],data[i])

def wordCloud_use_name(uses):
	print 'Generate colors'
	k=len(uses)
	hues=color_generator(k,cmap='gist_rainbow')

	use_color=init_clusters(k,uses)
	for use in uses[:-1]:
		use_color[use]=hues[uses.index(use)]
	use_color['NA']=(150./255,150./255,150./255)

	fname='wordcloud_uses'
	mtx=[(use,use,0.8) for use in uses]
	print 'Draw wordcloud'
	wordcloud=WordCloud(
		prefer_horizontal=1.0,color_func=my_color_func_2(use_color),
		max_font_size=50, relative_scaling=0.3,font_path='font\\verdanab.ttf',
		width=600
		).generate_from_tfIdf_use(mtx)
	plt.figure()
	plt.imshow(wordcloud)
	plt.axis("off")
	plt.title('Venue Categories WordCloud')
	plt.savefig('png\\'+fname+'.png')
	plt.close()

def wordCloud_venue_use(fname,cls,names,type,uses):
	#draw wordcloud of venue
	#term size based on checkin/user counts
	#color based on venue cateogry
	print 'Extract venues'
	data=extract_venue_data(fname,cls)

	print 'Process venues'
	for cl in cls:
		data[cl]=process_venue_use(data[cl])

	print 'Generate colors'
	k=len(uses)
	hues=color_generator(k,cmap='gist_rainbow')

	use_color=init_clusters(k,uses)
	for use in uses[:-1]:
		use_color[use]=hues[uses.index(use)]
	use_color['NA']=(150./255,150./255,150./255)

	for cl in cls:
		name=names[cls.index(cl)]
		print 'Draw wordcloud of cluster%d %s'%(cl,name)
		fn=fname.replace('_pop_%s'%type,'%d_%s'%(cl,name))
		draw_wordcloud_colors('wordcloud_venue_use_%s\\'%type+fn,name,use_color,data[cl],type)


def wordCloud_venue(fname,cls,names,type):
	#draw wordcloud of venue
	#term size based on checkin/user counts
	#single hue
	print 'Extract venues'
	data=extract_data_part('venue\\'+fname,cls)

	print 'Process venues'
	data=[process_venue(data[cl]) for cl in cls]

	print 'Generate different colors'
	k=len(cls)
	hues=color_generator(k)

	for i in xrange(k):		
		print 'Draw wordcloud of cluster%d %s'%(cls[i],names[i])
		print '>> hue',hues[i]
		fn=fname.replace('_pop_%s'%type,'%d_%s'%(cls[i],names[i]))
		draw_wordcloud('wordcloud_venue_%s\\'%type+fn,names[i],hues[i],data[i],wc_type='Venue %s'%type)


def wordCloud_cls(fname,cls,names,senti=False,ext_tfIdf='_tfIdf',ext_senti='_senti'):
	print "Extract term"
	fn_tfIdf=fname+ext_tfIdf
	fn_senti=fname+ext_senti
	data=extract_data(fn_tfIdf,fn_senti,cls)
	k=len(cls)

	print 'Process tfIdf and senti'
	data=[process_data(data[cls[i]]) for i in xrange(k)]

	print 'Generate different colors'
	hues=color_generator(k,senti)

	if senti is False:
		ext=''
	else:
		ext='_pos' if senti==1 else '_neg'

	fname='wordCloud_cls%s\\'%ext+fname
	for i in xrange(k):
		key='cluster%d'%cls[i]
		name=names[i]
		print "Draw wordcloud of %s"%key
		print '>> hue',hues[i]
		draw_wordcloud(fname+'_%s'%key,name,hues[i],data[i])
	

'''--- HELPER FUNCTION ---'''
def init_clusters(k,names):
	d=dict()
	for i in xrange(k):
		key=names[i]
		d[key]=[]
	return d

def color_generator(k,cmap='jet',senti=False):
	cmap=plt.get_cmap(cmap)
	
	if senti:
		n=2*k
	else:
		n=k

	cNorm=colors.Normalize(vmin=0,vmax=n)
	scalar=cmx.ScalarMappable(norm=cNorm,cmap=cmap)
	hues=[scalar.to_rgba(i) for i in xrange(n)]

	if senti==1:
		return hues[k:]
	elif senti==-1:
		return hues[:k]
	else:
		return hues
	

def my_color_func(color):
	#old_r, old_g, old_b = ImageColor.getrgb(color)
	(old_r, old_g, old_b)=color[:3]
	rgb_max = 255.
	h, s, v = colorsys.rgb_to_hsv(old_r/rgb_max, old_g/rgb_max, old_b/rgb_max)
	def senti_color(senti):
		r,g,b=colorsys.hsv_to_rgb(h,s,0.2+0.8*senti)
		return 'rgb({:.0f}, {:.0f}, {:.0f})'.format(r * rgb_max, g * rgb_max, b * rgb_max)

	return senti_color

def my_color_func_2(colors):
	rgb_max = 255.
	def use_color(use):
		(r,g,b)=colors[use][:3]
		return 'rgb({:.0f}, {:.0f}, {:.0f})'.format(r * rgb_max, g * rgb_max, b * rgb_max)

	return use_color

'''------------------------------------------------------------------'''
def extract_data_single(fname):
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)	
	data=[data[i][:-1].split(',') for i in xrange(n)]
	k=len(data[0])/2
	data_ex=init_clusters(k,range(k))

	senti=[float(data[0][2*i+1]) for i in xrange(k)]

	for i in xrange(k):
		data_ex[i]=[[data[j][2*i],float(data[j][2*i+1])] for j in xrange(2,n) if data[j][2*i]!='']

	return [data_ex,senti]

def extract_data(fn_tfIdf,fn_senti,cls):
	print 'Extract tfidf'
	tfIdf=extract_data_part(fn_tfIdf,cls)
	print 'Extract senti'
	senti=extract_data_part(fn_senti,cls)

	k=len(cls)
	data=init_clusters(k,cls)
	for i in xrange(k):
		tfIdf_cl=tfIdf[cls[i]]
		senti_cl=senti[cls[i]]
		n=len(tfIdf_cl)
		# term in _tfidf may change, use the one in _senti
		data[cls[i]]=[[senti_cl[j][0],tfIdf_cl[j][1],senti_cl[j][1]] for j in xrange(n)\
					if senti_cl[j][1]!=0]

	return data

def extract_data_part(fname,cls):	
	data=file('txt\\'+fname+'.txt').readlines()
	n=len(data)	
	data=[data[i][:-1].split(',') for i in xrange(n)]
	data_ex=init_clusters(len(cls),cls)

	for i in xrange(len(cls)):
		data_ex[cls[i]]=[[data[j][2*i],float(data[j][2*i+1])] for j in xrange(2,n) if data[j][2*i]!='']

	return data_ex

def extract_venue_data(fname,cls):
	data=file('txt\\venue\\urban-use\\'+fname+'.txt').readlines()
	n=len(data)	
	data=[data[i][:-1].split(',') for i in xrange(n)]
	data_ex=init_clusters(len(cls),cls)

	for i in xrange(len(cls)):
		data_ex[cls[i]]=[[data[j][3*i],data[j][3*i+1],float(data[j][3*i+2])] for j in xrange(1,n) if data[j][3*i]!='']

	return data_ex


'''------------------------------------------------------------------'''
def process_color(senti,color):
	senti=(np.array(senti)-min(senti))/(max(senti)-min(senti))
	k=len(senti)

	(r,g,b)=color
	print r,g,b
	rgb_max = 255.
	h, s, v = colorsys.rgb_to_hsv(r/rgb_max, g/rgb_max, b/rgb_max)
	colors=[colorsys.hsv_to_rgb(h,s,0.2+0.8*senti[i]) for i in xrange(k)]
	colors=['#%02x%02x%02x'%(colors[i][0]*rgb_max,colors[i][1]*rgb_max,colors[i][2]*rgb_max) for i in xrange(k)]

	return colors

def process_data_single(data):
	n=len(data)
	tf=[data[i][1] for i in xrange(n)]
	tf=np.array(tf)/min(tf)

	return [[data[i][0].title(),tf[i]] for i in xrange(n)]

def process_data(data):
	n=len(data)
	tfIdf=[data[i][1] for i in xrange(n)]
	tfIdf=np.array(tfIdf)/min(tfIdf)
	senti=[data[i][2] for i in xrange(n)]
	senti=(np.array(senti)-min(senti))/(max(senti)-min(senti))

	return [[data[i][0].title(),tfIdf[i],senti[i]] for i in xrange(n)]

def process_venue(data):
	n=len(data)	
	tfIdf=[data[i][1] for i in xrange(n)]
	tfIdf=np.array(tfIdf)/min(tfIdf)
	senti=[0.8 for i in xrange(n)]

	return [[data[i][0].title(),tfIdf[i],senti[i]] for i in xrange(n)]

def process_venue_use(data):
	n=len(data)	
	tfIdf=[data[i][2] for i in xrange(n)]
	tfIdf=np.array(tfIdf)/min(tfIdf)

	return [[data[i][0].title(),data[i][1],tfIdf[i]] for i in xrange(n)]


	

'''------------------------------------------------------------------'''
def draw_wordcloud_single(fname,name,color,mtx):
	wordcloud=WordCloud(
		prefer_horizontal=1.0,color_func=get_single_color_func(color),
		max_font_size=50, relative_scaling=0.3,font_path='font\\verdanab.ttf',
		width=600,background_color=None,mode='RGBA'
		).generate_from_frequencies(mtx)
	plt.figure()
	plt.imshow(wordcloud)
	plt.axis("off")
	plt.savefig('png\\'+fname+'_wcloud.png')
	plt.close()


def draw_wordcloud(fname,name,color,mtx,wc_type='Tweet'):
	wordcloud=WordCloud(
		prefer_horizontal=1.0,color_func=my_color_func(color),
		max_font_size=50, relative_scaling=0.3,font_path='font\\verdanab.ttf',
		width=600
		).generate_from_tfIdf_senti(mtx)
	plt.figure()
	plt.imshow(wordcloud)
	plt.axis("off")
	plt.title('%s WordCloud %s'%(wc_type,name.title()))
	plt.savefig('png\\'+fname+'.png')
	plt.close()

def draw_wordcloud_colors(fname,name,colors,mtx,type):
	wordcloud=WordCloud(
		prefer_horizontal=1.0,color_func=my_color_func_2(colors),
		max_font_size=50, relative_scaling=0.3,font_path='font\\verdanab.ttf',
		width=600
		).generate_from_tfIdf_use(mtx)
	plt.figure()
	plt.imshow(wordcloud)
	plt.axis("off")
	plt.title('%s WordCloud %s'%(type,name.title()))
	plt.savefig('png\\'+fname+'.png')
	plt.close()


'''
hues=color_generator(15)
for hue in hues:
	print hue,ImageColor.getrgb(hue)
'''