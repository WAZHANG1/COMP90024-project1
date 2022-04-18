#!/usr/bin/env python
# coding: utf-8

# In[38]:


import json
import numpy as np
import pandas as pd
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from collections import Counter
from itertools import chain
from mpi4py import MPI
import timeit
import subprocess


# In[2]:


with open('sydGrid.json', 'r') as file1:
    grid = json.load(file1)

with open('tinyTwitter.json', 'r') as file2:
    twitter_t = json.load(file2)
    
with open('smallTwitter.json', 'r') as file3:
    twitter_s = json.load(file3)


# In[3]:


# Read the tweet data line by line
class lessReader:

    def __init__(self, name):
        self.target = open(name, 'r')
    
    def __iter__(self):
        return self
    
    def __next__(self):
        try:
            line = next(self.target)
            if len(line) > 10:
                return line
            else:
                return "EOF"
        except StopIteration:
            return "EOF"

if __name__ == "__main__":
    import subprocess
    out = subprocess.Popen(["wc", "-l", "bigTwitter.json"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()


# In[37]:


# Split out each tweet
def chunck_line(line):
    line = line.strip()
    if line[-1] == ",":
        return line[:-1]
    
    return line


# In[6]:


# Extact language out
def process_lang(line):
    lang = line["doc"]["lang"]


# In[8]:


cell_name = {'A1':1, 'A2':5, 'A3':9, 'A4':13, 'B1':2, 'B2':6, 'B3':10, 'B4':14, 'C1':3, 'C2':7, 'C3':11, 'C4':15, 'D1':4, 'D2':8, 'D3':12, 'D4':16}


# In[10]:


lang_name = {'English':'en', 'Arabic':'ar', 'Bengali':'bn', 'Czech':'cs', 'Danish':'da', 'German':'de', 'Greek':'el', 
             'Spanish':'es', 'Persian':'fa', 'Finnish':'fi', 'Filipino':'fil', 'French':'fr', 'Hebrew':'he', 'Hindi':'hi',
             'Hungarian':'hu', 'Indonesian':'id', 'Italian':'it', 'Japanese':'ja', 'Korean':'ko', 'Malay':'msa', 'Dutch':'nl', 
             'Norwegian':'no', 'Polish':'pl', 'Portuguese':'pt','Romanian':'ro', 'Russian':'ru', 'Swedish':'sv', 'Thai':'th', 
             'Turkish':'tr', 'Ukrainian':'uk', 'Urdu':'ur', 'Vietnamese':'vi', 'Chinese':'zh-cn', 'Chinese':'zh-tw', 'Indonesian':'in'}


# -------------------------------------------------------------------------------------------------------

# In[14]:


# Divide cell into different region
# Extract its language
def create_cell_dict(line):
    Dict = {}
    if line["doc"]["coordinates"] != None:
        point = Point(line["doc"]["coordinates"]["coordinates"]) # each row doc-coord-coord
        #print(point)

        index = []
        for idx, val in enumerate(grid["features"]):
            polygon = Polygon(val["geometry"]["coordinates"][0])
            #if point.intersects(polygon):

            if point.within(polygon) or polygon.touches(point): # check if polygon contains point
                index.append(idx+1)

        if len(index) != 0:
            # check if the point lay between two point
            if len(index) == 1:
                if index[0] not in Dict:
                    Dict[index[0]] = []
                Dict[index[0]].append(line["doc"]["lang"])
            else:
                g1 = grid["features"][index[0]]["geometry"]["coordinates"][0]
                g2 = grid["features"][index[1]]["geometry"]["coordinates"][0]

                min_g1_lo = min([x[0] for x in g1])
                min_g2_lo = min([x[0] for x in g2])
                min_g1_la = min([y[1] for y in g1])
                min_g2_la = min([y[1] for y in g2])

                left = None
                down = None
                same_lo = None
                same_la = None

                if min_g1_lo < min_g2_lo:
                    left = True
                if min_g1_la < min_g2_la:
                    down = True
                if min_g1_lo == min_g2_lo:
                    same_lo = True
                if min_g1_la == min_g2_la:
                    same_la = True

                if left and same_la:
                    if index[0] not in Dict:
                        Dict[index[0]] = []
                    Dict[index[0]].append(line["doc"]["lang"])
                    #print(index[0])
                elif down and same_lo:
                    if index[1] not in Dict:
                        Dict[index[1]] = []
                    Dict[index[1]].append(line["doc"]["lang"])
                    #print(index[1])
        
    return Dict

# #cell_dict = dict(sorted(create_cell_dict(twitter_t).items()))
# #print(cell_dict)


# In[15]:


# def range_list(grid_file):
#     bounds_per_cell = []
#     for idx, val in enumerate(grid_file["features"]):
#         bounds_per_cell.append(list(Polygon(val["geometry"]["coordinates"][0]).bounds))
    
#     lon_list = []
#     lat_list = []
#     for coords in bounds_per_cell:
#         for num in coords:
#             if num > 0:
#                 lon_list.append(num)
#             else:
#                 lat_list.append(num)
 
#     lat_distinct = list(sorted(set(lat_list), reverse=True))
#     lon_distinct = list(sorted(set(lon_list)))[0:3]+list(sorted(set(lon_list)))[4:6]
    
                     
#     return lat_distinct,lon_distinct

# range_list(grid)
# lat_range = list(range_list(grid)[0])
# lon_range = list(range_list(grid)[1])


# In[16]:


cell_name_list = ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 
             'C1', 'C2', 'C3', 'C4', 'D1', 'D2', 'D3', 'D4']

# def create_rule(cell_name_list, lat_range, lon_range):
#     grid_dict = {name:[] for name in cell_name_list}
#     c = 0
#     while (c < len(grid_dict)):
#         for i in range(1,len(lat_range)):
#             for j in range(1,len(lon_range)):
#                 grid_dict[cell_name_list[c]] = [lat_range[i],lat_range[i-1] , lon_range[j-1], lon_range[j]]
#                 c += 1
#     return grid_dict
     
# #grid_dict

# create_rule(cell_name_list, lat_range, lon_range)


# In[17]:


cell_name = {'A1':1, 'A2':5, 'A3':9, 'A4':13, 'B1':2, 'B2':6, 'B3':10, 'B4':14, 'C1':3, 'C2':7, 'C3':11, 'C4':15, 'D1':4, 'D2':8, 'D3':12, 'D4':16}


# In[18]:


# change the dict index from default to cell name
def change_index(dict_name):
    cell_dict2 = {}
    for key, value in dict_name.items():
        #print(key)
        for key1, value1 in cell_name.items():
            if key == value1:
                cell_dict2[key1] = value

    return cell_dict2


# ### Create Output

# In[19]:


# Count total tweet number
def total_tweet(dict_name):
    total_t = [len(x) for x in dict_name.values()]
    return total_t


# In[20]:


# Convert language code to language name
def process_language(dict_name):
    for keys, values in lang_name.items():
        for k, v in dict_name.items():
            for i in range(len(v)):
                if v[i] == values:
                    dict_name[k][i] = keys
            if 'und' in v:
                v.remove('und')

    return dict_name


# In[21]:


# Develop top 10 language
def top10(dict_name):
    cell_langs = {}
    lst = []
    for kk, vv in dict_name.items():
        tuples = Counter(vv).most_common(10)
        zone = []
        for i in range(len(tuples)):
            item = tuples[i]
            zone.append("%s-%d"%(item[0],item)[1])
        lst.append(zone)
    
    lst1 = []
    for i in range(len(lst)):
        line = ", ".join(j for j in lst[i])
        lst1.append("(%s)"%line)
    return lst1


# In[22]:


# Count number of language used in the region
def num_lang(dict_name):
    lang_num = []
    for i in range(len(dict_name)):
        lang_num.append(len(np.unique(list(process_language(dict_name).values())[i])))
    return lang_num


# In[28]:


# Print out the result
def print_result(dict_name):
    df = pd.DataFrame(columns=['Cell','#Total Tweets','#Number of Languages Used','#Top 10 Languages & #Tweets)'])
    df['Cell'] = list(dict_name.keys())
    df['#Total Tweets'] = total_tweet(dict_name)
    df['#Number of Languages Used'] = num_lang(dict_name)
    df['#Top 10 Languages & #Tweets)']  = top10(dict_name)
    df.reset_index()
    return df


# In[26]:


# Parallel the data by using mpi
def Parallel_MPI(filename):

    comm = MPI.COMM_WORLD
    size = comm.Get_size() # default 1
    rank = comm.Get_rank() # default 0

    file_name = "bigTwitter.json"
    
    grid_coord = {}
    cell_dict = {}
    with open('sydGrid.json', 'r') as file1:
        grid = json.load(file1)

    for feature in grid['features']:
        grid_coord[feature['properties']['id']] = list(Polygon(feature["geometry"]["coordinates"][0]).bounds)
     
    # single core 
    if size == 1:
        lr = lessReader(file_name)
        header = next(lr)
        line = next(lr)
        start = timeit.default_timer()
        processed_line = 0
        while line != 'EOF':
            try: 
                data = json.loads(chunck_line(line))
                if data['doc']['coordinates'] is not None:
                    line_dict = create_cell_dict(data)
                    for idx, val in line_dict.items():
                        if idx not in cell_dict:
                            cell_dict[idx] = val
                        else:
                            cell_dict[idx].append(val[0])
                line = next(lr)
                processed_line +=1
            except json.decoder.JSONDecodeError:
                break 
        cell_dict_new = change_index(cell_dict)
        end = timeit.default_timer()
        print("rank", rank, "has processed", processed_line, "lines", "uses", end-start)
        return print_result(cell_dict_new)
        
    lr = lessReader(file_name)
    header = next(lr)    
    
    if rank == 0:
        import subprocess
        out = subprocess.Popen(["wc", "-l", 'bigTwitter.json'],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = out.communicate()
        file_length = int(stdout.split()[0])-1
    else:
        file_length = None  
    
    file_length = comm.bcast(file_length, root=0)
    
    row_num = file_length // size
    start_line = row_num*rank
    end_line = row_num*(rank+1)
    
    if rank == size-1:
        end_line = file_length+1
    print("This is rank", rank, "which reading from",
          start_line, "to", end_line)
    
    processed_line = 0
    start = timeit.default_timer()
    for line_count, line in enumerate(lr):
        if line == "EOF":
            break
        if line_count >= end_line:
            break
        elif line_count < start_line:
            continue
        else:
            processed_line += 1
            try:
                data = json.loads(chunck_line(line))
                if data['doc']['coordinates'] is not None:
                    line_dict = create_cell_dict(data)
                    for idx, val in line_dict.items():
                        if idx not in cell_dict:
                            cell_dict[idx] = val
                        else:
                            cell_dict[idx].append(val[0])
                else:
                    continue
            except json.decoder.JSONDecodeError:
                break 

    end = timeit.default_timer()
    print("rank", rank, "has processed", processed_line, "lines", "takes", end-start)
    cell_dict_new = change_index(cell_dict)
    final_dict = comm.gather(cell_dict_new, root=0)
    
    if rank == 0:
        dict_print = {}
        for dictionary in final_dict:
            for idx, val in dictionary.items():
                if idx not in dict_print:
                    dict_print[idx] = val
                else:
                    dict_print[idx] += val
  
        return print_result(dict_print)


# In[29]:


print(Parallel_MPI("bigTwitter.json"))

