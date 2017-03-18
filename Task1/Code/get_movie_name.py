#!/usr/bin/env python

with open('output_top20.txt', 'r') as file1:
    with open('u.item', 'r') as file2:
        for line1 in file1:
        	line1 =line1.strip()
        	movie_1, movie_2, count = line1.split(' ')
        	for line2 in file2:
        		line2 =line2.strip()
        		movie, name, dummy= line2.split('|',2)
        		if (movie_1 == movie):
        			m1=name
        		else:
        			continue
        	file2.seek(0)
        	for line3 in file2:
        		line3 =line3.strip()
        		m, n, d= line3.split('|',2)
        		if (movie_2 == m):
        			m2=n
        		else:
        			continue
        	file2.seek(0)
        	print m1,m2,'\t-->',count


#same.discard('\n')

#with open('some_output_file.txt', 'w') as file_out:
#    for line in same:
#        file_out.write(line)