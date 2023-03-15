import os
import json
from pathlib import Path as Data_Path
from os import listdir
from os.path import isfile, join
from tqdm import tqdm 

import networkx as nx
import pandas as pd 
import numpy as np 

import torch 

os.chdir("drive/MyDrive/CS 224W Project")
DATA_PATH = Data_Path('spotify_million_playlist_sample/data')

# check if path exists 
if not DATA_PATH.exists():
    print("Oops, file path doesn't exist!")


N_FILES_TO_USE = 10 

file_names = sorted(os.listdir(DATA_PATH))
file_names_to_use = file_names[:N_FILES_TO_USE]


def process_playlist(G, playlist_json, playlist_index):
	# Create a graph with: 
	#   1. nodes for each playlist, artist, track 
	#   2. edges connecting playlist - track, playlist - artist, artist - track

	# Create a pandas df for each edge 


	tracks = playlist_json["tracks"]
	track_uris = [x["track_uri"] for x in tracks]
	artist_uris = [x["artist_uri"] for x in tracks]

	playlist_node = (f"playlist_{playlist_index}", {'name': playlist_json["name"], "node_type" : "playlist"})

	# initialize node list, edge list 
	node_list = [playlist_node]
	edge_list = [(f'playlist_{playlist_index}', x) for x in track_uris]
	edge_list += [(f'playlist_{playlist_index}', x) for x in artist_uris]

	data_rows = []

	for track in tracks: 
		# add to edge, node lists for graph 
		track_node = (track['track_uri'], {'name': track['track_name'], 'node_type': 'track'})
		artist_node = (track['artist_uri'], {'name': track['artist_name'], 'node_type': 'artist'})

		node_list.append(track_node)
		node_list.append(artist_node)

		edge_list.append((track['artist_uri'], track['track_uri']))

		# add to row list 
		row = [
			f'playlist_{playlist_index}', 
			playlist_json["name"], 
			track['track_uri'], 
			track['track_name'], 
			track['artist_uri'],
			track['artist_name']
		]
		data_rows.append(row)

	G.add_nodes_from(node_list)
	G.add_edges_from(edge_list)

	cols = ['playlist_id', 'playlist_name', 'track_id', 'track_name', 'artist_id', 'artist_name']
	return pd.DataFrame(data = data_rows, columns = cols)


def process_file(G, start_index, file_json):
	playlist_dfs = []
	for i, playlist_json in enumerate(file_json['playlists']):
		playlist_df = process_playlist(G, playlist_json, start_index + i)
		playlist_dfs.append(playlist_df)
	return pd.concat(playlist_dfs)


def process_data(G, data_path, file_names, start_index = 0, end_index = 10):

	edge_dfs = []
	n_playlists = start_index 

	for file_name in tqdm(file_names, desc='Files processed: ', unit='files', total=len(file_names)):
		with open(join(data_path, file_name)) as json_file:
			json_data = json.load(json_file)

		edge_df = process_file(G, n_playlists, json_data)
		n_playlists += len(json_data['playlists'])
		edge_dfs.append(edge_df)

	return pd.concat(edge_dfs)


G = nx.Graph()
edge_df = process_data(G, DATA_PATH, file_names_to_use, 0, 10)
print('Num nodes:', G.number_of_nodes(), '. Num edges:', G.number_of_edges())


def get_top_n_track(edge_df, n = 5):

	track_count = edge_df.groupby(["track_id"])["track_name"].count().reset_index()
	track_count = track_count.rename(columns = {'track_name':'count'})

	edge_df = edge_df.merge(track_count, how = "left", on = "track_id")

	# get top playlists 
	top_tracks = edge_df.groupby(["playlist_id"])["count"].mean().sort_values(ascending = False)

	return top_tracks.index[:5]

top5_track = get_top_n_track(edge_df, 5)
top_playlist = top5_track[0]

# generate ego graph
neighbor_distance = 3 
G_ego = nx.generators.ego.ego_graph(G, top_playlist, neighbor_distance, undirected = True)
print('Num nodes:', G_ego.number_of_nodes(), '. Num edges:', G_ego.number_of_edges())

# get largest graph component -- we will use this for our data  
largest_cc = max(nx.connected_components(G.to_undirected()), key=len)
G_comp = nx.Graph(G_orig.subgraph(largest_cc))

playlists = [label for label, attr in G_comp.nodes(data=True) if attr['node_type'] == 'playlist']
artists = [label for label, attr in G_comp.nodes(data=True) if attr['node_type'] == 'artist']
tracks = [label for label, attr in G_comp.nodes(data=True) if attr['node_type'] == 'track']
print(len(playlists), len(artists), len(tracks))

# convert to HeteroData class 
playlist_ids = [int(s.split("_")[1]) for s in playlists]
playlist2id = dict(zip(playlists, playlist_ids))
artist2id = dict(zip(artists, np.arange(len(artists))))
track2id = dict(zip(tracks, np.arange(len(tracks))))
artist_ids = list(artist2id.values())
track_ids = list(track2id.values())


















