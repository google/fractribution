## Fractribution code base

## Attribution Overview

Users might click on multiple ads before converting. This can make it
challenging to assign proper credit to the different marketing channels. Should
all the credit go to the last ad the user saw, or the first ad? Or should all
the ads share in the credit equally.

## Data-driven attribution (DDA)
DDA attempts to work out a fair weighing of credit among marketing channels. For
example, a particular display ad might not convert immediately, but users who
click the display ad might be much more likely to convert later on. In this
case, the display ad should get credit, even though it may not be the first or
last ad on a user's path to conversion. Google Marketing Platform products
includes DDA as an option.

## Fractribution Package
This Fractribution package is a DDA algorithm that generates
**user-level fractional attribution values** for each conversion. The
advantage of user-level attribution is that the attribution values can later be
joined with custom user-level data (e.g. transaction value, lifetime value etc).
This can be useful when regulation or data policy prevents ecommerce/revenue
events from being shared with the Google Marketing Platform.

Please see this
[fractribution slide deck](https://storage.cloud.google.com/fractribution-external-share/fractribution-introduction.pdf)
for more background on use cases and details on the DDA algorithm.

## Using Fractribution
There are two stages to running Fractribution:

* Stage 1: Data Preparation in **py/fractribution_data**
This stage generates the paths-to-conversion and paths-to-non-conversion via
an end-to-end BigQuery and analytics pipeline.
* Stage 2: Model fitting in **py/fractribution_model**
This stage runs a simplified Shapley Value DDA algorithm over the data prepared
in Stage 1 to generate the fractional attribution values.

To use Fractribution, either:

* Install the source code as a Cloud Function on GCP, or
* Deploy the Fractribution Docker image on GCP.

## Directory structure

```bash
fractribution
├── README.md
├── py
├──── README.md
├──── Dockerfile
├──── start.py
├──── fractribution_data
├──────── README.md
├──── fractribution_model
├──────── README.md
└── README.md
```
