# Clash Royale Data Project

Data pipeline involving extract, load, and transformation of Clash Royale data with the end result being an analysis friendly mini data warehouse and an analysis dashboard build on top of the dw.

The data is sourced from [Kaggle](https://www.kaggle.com/datasets/bwandowando/clash-royale-season-18-dec-0320-dataset) as well as the official [Clash Royale API](https://developer.clashroyale.com/#/). The Kaggle data was sourced due to API changes that made it difficult to ingest player vs. player data directly from the API without a cloud-hosted compute platform. Regardless, the data hosted on Kaggle is of fairly large size (>20 GB) total.