import os
import sys
import glob
import time
import datetime
import sqlite3
import numpy as np
import csv

msd_subset_path='/home/ubuntu/MillionSongSubset'
msd_subset_data_path=os.path.join(msd_subset_path,'data')
msd_subset_addf_path=os.path.join(msd_subset_path ,'AdditionalFiles')
assert os.path.isdir (msd_subset_path),'wrong path' # sanity check

msd_code_path='/home/ubuntu/MSongsDB-master'
assert os.path.isdir (msd_code_path),'wrong path' # sanity check

sys.path.append(os.path.join(msd_code_path,'PythonSrc'))

import hdf5_getters as GETTERS
def strtimedelta(starttime,stoptime):
    return str(datetime.timedelta(seconds=stoptime-starttime))

# we define this very useful function to iterate the files
def apply_to_all_files(basedir,func=lambda x: x,ext='.h5'):
    """
    From a base directory, go through all subdirectories,
    find all files with the given extension, apply the
    given function 'func' to all of them.
    If no 'func' is passed, we do nothing except counting.
    INPUT
       basedir  - base directory of the dataset
       func     - function to apply to all filenames
       ext      - extension, .h5 by default
    RETURN
       number of files
    """
    cnt = 0
    cnt2= 0
    #print 'Entering apply to all files'
    # iterate over all files in all subdirectories
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        # count files
        cnt += len(files)
        #print(cnt)
        # apply function to all files
        for f in files :
            #cnt2=cnt2+1
            #if cnt2<50:
            func(f)    
    return

# we can now easily count the number of files in the dataset
print 'number of song files:',apply_to_all_files(msd_subset_data_path)
print 'abcd'
all_artist_id = []
all_artist_names = []
all_songs_id = []
all_songs_names = []
#all_danceability = []
all_song_hottness = []
all_tempo = []
all_loudness = []
all_key_confidence = []
all_mode_confidence = []
all_year =[]
#count=0

# we define the function to apply to all files
def func_to_get_artist_name(filename):
    """
    This function does 3 simple things:
    - open the song file
    - get artist ID and put it
    - close the file
    """
    #global count
    #count=count+1
    #print count
    #print(count)	
    h5 = GETTERS.open_h5_file_read(filename)
    
    artist_id = GETTERS.get_artist_id(h5)
    all_artist_id.append(artist_id)
 
    artist_name = GETTERS.get_artist_name(h5)
    all_artist_names.append(artist_name)
    #print(artist_name)
    
    song_id = GETTERS.get_song_id(h5)
    all_songs_id.append(song_id) 
    
    song_name = GETTERS.get_title(h5)
    all_songs_names.append(song_name)

    #danceability = GETTERS.get_danceability(h5)
    #all_danceability.append(danceability)
    #print 'Danceablity is ',str(danceability),' and type is ',type(danceability)
    loudness = GETTERS.get_loudness(h5)
    all_loudness.append(loudness)
    #print 'Loudness is ',str(loudness),' and type is ',type(loudness)
    song_hottness = GETTERS.get_song_hotttnesss(h5)
    all_song_hottness.append(song_hottness)
    #print 'Song hottness is ',str(song_hottness),' and type is ',type(song_hottness)
    tempo = GETTERS.get_tempo(h5)
    all_tempo.append(tempo)
    key_confidence = GETTERS.get_key_confidence(h5)
    all_key_confidence.append(key_confidence)
    #print 'key_confidence is ',str(key_confidence),' and type is ',type(key_confidence)    
    mode_confidence = GETTERS.get_mode_confidence(h5)
    all_mode_confidence.append(mode_confidence)
    year=GETTERS.get_year(h5)
    all_year.append(year)
    print 'Year = ', year
    #time_sig=GETTERS.get_time_signature(h5)
    #print 'Time Sig is ',str(time_sig),' and type is ',type(time_sig)
    #tatums_start=GETTERS.get_tatums_start(h5)
    #print 'Tatums Start is ',str(len(tatums_start)),' and type is ',type(tatums_start)
    #segments_start=GETTERS.get_segments_start(h5)
    #print 'Segments Start is ',str(segments_start),' and type is ',type(segments_start)
    #start_of_fade_out=GETTERS.get_start_of_fade_out(h5)
    #print 'start_of_fade_out is ',str(start_of_fade_out),' and type is ',type(start_of_fade_out)
    #key=GETTERS.get_key(h5)
    #print 'Key is ',str(key),' and type is ',type(key)
    #energy=GETTERS.get_energy(h5)
    #print 'Energy is ',str(energy),' and type is ',type(energy)
    #S_A=GETTERS.get_similar_artists(h5)
    #print 'Similar Artist is ',str(S_A)
    #print(song_id)
    #print(count)
    #all_artist_names.add( artist_name )
    h5.close()

t1 = time.time()
global count
apply_to_all_files(msd_subset_data_path,func=func_to_get_artist_name)
t2 = time.time()
print 'all artist names extracted in:',strtimedelta(t1,t2)
# let's see some of the content of 'all_artist_names'
print 'found',len(all_artist_names),'unique artist names'
#for k in range(len(list(all_artist_names))):
path = "music_data_updated_v2.csv"
c = csv.writer(open(path, "w+"))
#print 'Length of Danceability is ',len(all_danceability)
#print 'Length of Loudness is ',len(all_loudness)
#print 'Length of Song hottness is ',len(all_song_hottness)
#print 'Length of Tempo is ',len(all_tempo)

#for k in range(5):
for k in range(len(list(all_artist_names))):
    print 'K is ',k
    artist_id=list(all_artist_id)[k]
    artist_name=list(all_artist_names)[k]
    song_id=list(all_songs_id)[k]
    song_name=list(all_songs_names)[k]
    #danceability=list(all_danceability)[k]
    loudness=list(all_loudness)[k]
    song_hottness=list(all_song_hottness)[k]
    tempo=list(all_tempo)[k]
    key_confidence=list(all_key_confidence)[k]
    mode_confidence=list(all_mode_confidence)[k]
    year=list(all_year)[k]
    c.writerow([artist_id,artist_name,song_id,song_name,loudness,tempo,key_confidence,mode_confidence,year])
    #c.writerow([artist_name,song_name,danceability,loudness,song_hottness,tempo])
    #print artist_name
    #print song_name

'''


# this is too long, and the work of listing artist names has already
# been done. Let's redo the same task using an SQLite database.
# We connect to the provided database: track_metadata.db
conn = sqlite3.connect(os.path.join(msd_subset_addf_path,
                                    'subset_track_metadata.db'))
# we build the SQL query
q = "SELECT DISTINCT artist_name FROM songs"
# we query the database
t1 = time.time()
res = conn.execute(q)
all_artist_names_sqlite = res.fetchall()
t2 = time.time()
print 'all artist names extracted (SQLite) in:',strtimedelta(t1,t2)
# we close the connection to the database
conn.close()
# let's see some of the content
for k in range(5):
    print all_artist_names_sqlite[k][0]

# now, let's find the artist that has the most songs in the dataset
# what we want to work with is artist ID, not artist names. Some artists
# have many names, usually because the song is "featuring someone else"
conn = sqlite3.connect(os.path.join(msd_subset_addf_path,
                                    'subset_track_metadata.db'))
q = "SELECT DISTINCT artist_id FROM songs"
res = conn.execute(q)
all_artist_ids = map(lambda x: x[0], res.fetchall())
conn.close()

# The Echo Nest artist id look like:
for k in range(4):
    print all_artist_ids[k]

# let's count the songs from each of these artists.
# We will do it first by iterating over the dataset.
# we prepare a dictionary to count files
files_per_artist = {}
for aid in all_artist_ids:
    files_per_artist[aid] = 0

# we prepare the function to check artist id in each file
def func_to_count_artist_id(filename):
    """
    This function does 3 simple things:
    - open the song file
    - get artist ID and put it
    - close the file
    """
    h5 = GETTERS.open_h5_file_read(filename)
    artist_id = GETTERS.get_artist_id(h5)
    files_per_artist[artist_id] += 1
    h5.close()

# we apply this function to all files
apply_to_all_files(msd_subset_data_path,func=func_to_count_artist_id)

# the most popular artist (with the most songs) is:
most_pop_aid = sorted(files_per_artist,
                      key=files_per_artist.__getitem__,
                      reverse=True)[0]
print most_pop_aid,'has',files_per_artist[most_pop_aid],'songs.'

# of course, it is more fun to have the name(s) of this artist
# let's get it using SQLite
conn = sqlite3.connect(os.path.join(msd_subset_addf_path,
                                    'subset_track_metadata.db'))
q = "SELECT DISTINCT artist_name FROM songs"
q += " WHERE artist_id='"+most_pop_aid+"'"
res = conn.execute(q)
pop_artist_names = map(lambda x: x[0], res.fetchall())
conn.close()
print 'SQL query:',q
print 'name(s) of the most popular artist:',pop_artist_names

# let's redo all this work in SQLite in a few seconds
t1 = time.time()
conn = sqlite3.connect(os.path.join(msd_subset_addf_path,
                                    'subset_track_metadata.db'))
q = "SELECT DISTINCT artist_id,artist_name,Count(track_id) FROM songs"
q += " GROUP BY artist_id"
res = conn.execute(q)
pop_artists = res.fetchall()
conn.close()
t2 = time.time()
print 'found most popular artist in',strtimedelta(t1,t2)
print sorted(pop_artists,key=lambda x:x[2],reverse=True)[0]


'''



