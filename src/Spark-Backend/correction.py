f=open('/home/ubuntu/music_data_clustered_updated_v3.txt/part-00000','r')
g=open('/home/ubuntu/spark-1.2.0.2.2.0.0-82-bin-2.6.0.2.2.0.0-2041/save_v3.csv',"w+")
r=f.readlines()
for line in r:
	line1=line.replace('(','')
	#line2=line1.replace(" ",'')
	l3=line1.replace(')','')
	l4=l3.strip("''")
	l5=l4.replace('\'',"")
	g.write(l5)	
	print l5
f.close()
g.close()
