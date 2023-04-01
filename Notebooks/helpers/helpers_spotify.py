##############################################################################
# IMPORT LIBRARIES
##############################################################################

# Standard libraries
from itertools import chain

# Scappring library
from bs4 import BeautifulSoup
import requests
import re

# Parallelization librairies
from joblib import Memory  # , Parallel, delayed
from multiprocessing import cpu_count

# Helper functions
from helpers.helpers_pandas import jenks_filter


##############################################################################
# UTILITY FUNCTIONS
##############################################################################


def chunk_list(lst, max_items=50):
    n = len(lst)
    results = []
    i = 0
    while i < n / max_items:
        results.append(lst[i * max_items : (i + 1) * max_items])
        i += 1
    if len(list(chain.from_iterable(results))) != n:
        raise ValueError("missing elements in chunked lists")
    return results


##############################################################################
# JOBLIB
##############################################################################

# Cache settings
location = "./cachedir"
memory = Memory(location, verbose=0)


# -------------------------------------------------------------------------


def clear_cache(warn=True):
    """Clear the Joblib memory cache"""
    memory.clear(warn=warn)


##############################################################################
# SPOTIPY
##############################################################################


@memory.cache(ignore=["spotipy_client"])
def request_tracks(tracks_id, spotipy_client):
    """_summary_

    Args:
        tracks_id (_type_): _description_
        spotipy_client (_type_): _description_

    Returns:
        _type_: _description_
    """
    if isinstance(tracks_id, str):
        tracks_id = [tracks_id]
    return spotipy_client.tracks(tracks_id)["tracks"]


# -------------------------------------------------------------------------


@memory.cache(ignore=["spotipy_client"])
def request_artist_genre(artist_id, spotipy_client):
    """Get the spotify genres associated with an artist.
    Warning, many artists do not have any genres associated with them

    Args:
        artist_id (str): spotify id of the artist
        spotipy_client (spotipy.client.Spotify): spotify client

    Returns:
        list[str]: list of spotify genres
    """
    return spotipy_client.artist(artist_id)["genres"]


# -------------------------------------------------------------------------


@memory.cache(ignore=["spotipy_client"])
def request_audio_features(track_id, audio_keys, spotipy_client):
    """Get the audio features for a track as definied by audio keys

    Args:

        track_id (str): spotify id of the track
        audio_keys (list[str]): list of features to request
        spotipy_client (spotipy.client.Spotify): spotify client

    Returns:
        dict: dict of audio values for each feature requested
    """
    return {key: spotipy_client.audio_features(track_id)[0][key] for key in audio_keys}


# -------------------------------------------------------------------------


@memory.cache(ignore=["spotipy_client"])
def request_recommendations(
    seed_artists, seed_tracks, spotipy_client, limit=10, repeat=cpu_count()
):
    """Get similar tracks given an artist_id or a track_id. Recommendation being stochastics,
    the process is repeated several times.

    Args:
        seed_artists (list[str]):  spotify id of the artist
        seed_tracks (list[str]):  spotify id of the track
        spotipy_client (spotipy.client.Spotify): spotify client
        limit (int, optional): number of tracks per recommendation. Defaults to 10.
        repeat (int, optional): number of time to request recommendation. Defaults to cpu_count().

    Returns:
        list: list of 'tracks' metadata as returned by the Spotify API
    """
    # convert ids into list (only type accepted by spotify.recommendations())
    if isinstance(seed_artists, str):
        seed_artists = [seed_artists]
    if isinstance(seed_tracks, str):
        seed_tracks = [seed_tracks]
    result = []
    for _ in range(repeat):
        result.extend(
            spotipy_client.recommendations(
                seed_artists=seed_artists, seed_tracks=seed_tracks, limit=limit
            )["tracks"]
        )
    return result


# -------------------------------------------------------------------------


@memory.cache(ignore=["spotipy_client"])
def get_similar_artists(
    artist_id, track_id, spotipy_client, limit=10, repeat=cpu_count()
):
    """Get similar artists given an artist_id or a track_id. Recommendation being stochastics,
    the process is repeated several times.


    Args:
        artist_id (str | list[str]):  spotify id of the artist
        track_id (str | list[str]):  spotify id of the track
        spotipy_client (spotipy.client.Spotify): spotify client
        limit (int, optional): number of tracks per recommendation. Defaults to 10.
        repeat (int, optional): number of time to request recommendation. Defaults to cpu_count().

    Returns:
        list: list of artist id
    """
    # get spotify tracks recommendations
    playlist = request_recommendations(
        seed_artists=artist_id,
        seed_tracks=track_id,
        spotipy_client=spotipy_client,
        limit=limit,
        repeat=repeat,
    )
    # exrtact artists from the track playlist recommended
    sim_artists_set = set(
        artist["id"] for track in playlist for artist in track["artists"]
    )
    # do not include itself in the similar artists
    self_set = set(chain.from_iterable(filter(None, (artist_id, track_id))))
    return list(sim_artists_set - self_set)


# -------------------------------------------------------------------------


@memory.cache(ignore=["spotipy_client"])
def approximate_genres(
    artist_id, track_id, spotipy_client, limit=10, repeat=cpu_count()
):
    """Approximate the genre of an artist. This is relevant when request_artist_genre() returns []

    Args:
        artist_id (str | list[str]):  spotify id of the artist
        track_id (str | list[str]):  spotify id of the track
        spotipy_client (spotipy.client.Spotify): spotify client
        limit (int, optional): number of tracks per recommendation. Defaults to 10.
        repeat (int, optional): number of time to request recommendation. Defaults to cpu_count().

    Returns:
        list[str]: list of related spotify genres
    """
    # get similar artists
    sim_artists_id = get_similar_artists(
        artist_id=artist_id,
        track_id=track_id,
        spotipy_client=spotipy_client,
        limit=limit,
        repeat=repeat,
    )
    # get the genres of all those similar artists
    gen_similar_genres = [
        request_artist_genre(artist_id=artist_id, spotipy_client=spotipy_client)
        for artist_id in sim_artists_id
    ]
    # flatten the nested list
    sim_genres = list(chain.from_iterable(filter(None, gen_similar_genres)))
    # only return the dominant ones
    return jenks_filter(sim_genres)


# -------------------------------------------------------------------------


def get_single_artist_genres(artist_id, spotipy_client):
    """Get the genre of a single artist. Approximate it if spotify returns []

    Args:
        artist_id (str):  spotify id of the artist
        spotipy_client (spotipy.client.Spotify): spotify client

    Returns:
        list[str]: list of related spotify genres
    """
    # print("artist id: ", artist_id)
    genres = request_artist_genre(artist_id=artist_id, spotipy_client=spotipy_client)
    if not genres:
        # print("genres = []")
        genres = approximate_genres(
            artist_id=artist_id, track_id=None, spotipy_client=spotipy_client
        )
        if not genres:
            # print(f"Could not find related genres for artist: {artist_id}")
            pass
    return genres


# -------------------------------------------------------------------------


def get_artist_genres(artist_id, spotipy_client):
    """Get the genre of a list of artist.

    Args:
        artist_id (list[str]): spotify id of the artist
        spotipy_client (spotipy.client.Spotify): spotify client


    Returns:
        list: list of related spotify genres
    """
    if isinstance(artist_id, str):
        return get_single_artist_genres(
            artist_id=artist_id, spotipy_client=spotipy_client
        )
    genres = (
        get_single_artist_genres(artist_id=id, spotipy_client=spotipy_client)
        for id in artist_id
    )
    return list(set(chain.from_iterable(genres)))


# -------------------------------------------------------------------------


def get_tracks_from_playlist(playlist_metadata):
    """Return track list from playlist metadata. By default playlist_metadata has keys
    ('added_at', track'). This function extracts sub-dictionary 'track'.

    Args:
        spotify_playlist_metadata (dict):  playlist metadata as return by the Spotify APi

    Returns:
        dict: track metadata as return by the Spotify APi
    """
    if isinstance(playlist_metadata, dict):
        playlist_metadata = [playlist_metadata]
    return (item["track"] for item in playlist_metadata)


# -------------------------------------------------------------------------


def get_tracks_from_id(tracks_id, spotipy_client):
    """Return track list from track ids.

    Args:
        tracks_id (str | list[str]): track id

    Returns:
        list: list of track metadata as return by the Spotify APi
    """
    if isinstance(tracks_id, str):
        tracks_id = [tracks_id]
    chunked_tracks_id = chunk_list(tracks_id, max_items=50)
    results = []
    for i, lst in enumerate(chunked_tracks_id):
        print(i)
        results.extend(request_tracks(lst, spotipy_client))
    return results


# -------------------------------------------------------------------------


def cleaned_track_data(track_metadata, audio_keys, spotipy_client):
    """Extract the info related to a track. For audio keys, check
    https://developer.spotify.com/documentation/web-api/reference/#/operations/get-audio-features

    Args:
        track_metadata (dict): track metadata as return by the Spotify APi
        audio_keys (Iterable[str]): sequence of audio features to extract
        spotipy_client (spotipy.client.Spotify): spotify client

    Returns:
        dict: info about the track organised in dictionary
    """
    # Get the metadata usings the functions defined above
    track_id = track_metadata["id"]
    track_title = track_metadata["name"]
    track_artists = [artist["name"] for artist in track_metadata["artists"]]
    track_artists_id = [artist["id"] for artist in track_metadata["artists"]]
    popularity = track_metadata["popularity"]
    audio_features = request_audio_features(
        track_id=track_id, audio_keys=audio_keys, spotipy_client=spotipy_client
    )
    track_genres = get_artist_genres(
        artist_id=track_artists_id, spotipy_client=spotipy_client
    )
    # If we could not find related genres for any of the artists of the tack
    # We approximate the genres of the track itself (by getting similar tracks
    # and then checking the genres associated with those similar artists)
    if not track_genres:
        track_genres = list(
            set(
                approximate_genres(
                    artist_id=None,
                    track_id=track_id,
                    spotipy_client=spotipy_client,
                    limit=50,
                )
            )
        )
    # Flag the track if we could still not find any genres for it
    if not track_genres:
        print(
            f"Could not find related genres for the track: '{track_title}' (id: '{track_id}')"
        )
    else:
        track_genres.sort()

    return {
        "spotify_id": track_id,
        "title": track_title,
        "artists": track_artists,
        "artists_id": track_artists_id,
        "genres": track_genres,
        "popularity": popularity,
        **audio_features,
    }


##############################################################################
# EVERYNOISE SCRAPPER
##############################################################################


@memory.cache
def get_enao_genre_data(genre):
    """Get everynoise data for a given spotify genre.
    adapted from https://github.com/aweitz/EveryNoise/blob/master/scrapGenres.ipynb

    Args:
        genre (str): spotify genres

    Returns:
        dict: scrapped data from https://everynoise.com/research.cgi?mode=genre
    """
    # Get url and scrap page info
    genre_page = (
        "http://everynoise.com/engenremap-" + re.sub("[:'+»&\s-]", "", genre) + ".html"
    )
    request = requests.get(genre_page)
    soup = BeautifulSoup(request.text, "html.parser")

    # Exctract relevant HTML div
    playlist_url = soup.find_all("a", text="playlist")[0]["href"]
    allArtistDivs = set(soup.find_all("div", "genre scanme"))
    allGenresRelated = set(soup.find_all("div", "genre"))
    allGenresRelated = allGenresRelated - allArtistDivs

    # Get similar / opposite genres
    sim_weights = []
    opp_weights = []
    opp_genres = []
    sim_genres = []
    for other_genre in allGenresRelated:
        weight = int(other_genre["style"].split()[-1].replace("%", ""))
        if "nearby" in other_genre["id"]:
            g = other_genre.text.strip().replace("»", "")
            if g != genre:
                sim_weights.append(weight)
                sim_genres.append(g)
        elif "mirror" in other_genre["id"]:
            opp_weights.append(weight)
            opp_genres.append(other_genre.text.strip().replace("»", ""))

    # Get artists associated with the genre
    main_artists = []
    artists_weights = []
    for artist in allArtistDivs:
        weight = int(artist["style"].split()[-1].replace("%", ""))
        artistName = artist.text.strip().replace("»", "")
        if not (artistName.isspace()):
            main_artists.append(artistName)
            artists_weights.append(weight)

    return {
        "genre": genre,
        "sim_genres": sim_genres,
        "sim_weights": sim_weights,
        "opp_genres": opp_genres,
        "opp_weights": opp_weights,
        "main_artists": main_artists,
        "artists_weights": artists_weights,
        "spotify_url": playlist_url,
    }
