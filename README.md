# Spotify

This project involves the development of a Python-based module that automates the creation of playlists in Spotify, utilizing a range of computational methodologies such as high-dimensional data clustering, network science, and machine learning. The project comprises several scripts:

- [Spotify scrapper](Notebooks/01_Spotify-Scrapper.ipynb): extract metadata about saved tracks using the [Spotify API](https://developer.spotify.com/documentation/web-api);
- [EveryNoise-Scrapper](Notebooks/02_EveryNoise-Scrapper.ipynb): retrieved genre metadata from [Every Noise at Once](https://everynoise.com/)
- [Community detection](Notebooks/03_Community-Detection.ipynb) use the [Leiden algorithm](https://github.com/vtraag/leidenalg) to cluster songs based on the similarities between genres (i.e. in 'communities', more information [here](https://doi.org/10.1038/s41598-019-41695-z)) 
- [Playlist creation](Notebooks/04_Playlists-Creation.ipynb) is currently a work-in-progress and aim to apply machine learning techniques to further cluster the songs in each community based on emerging patterns from their audio features.
