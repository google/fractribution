# Fractribution code base

## Attribution Overview

In a marketing context, Attribution involves identifying the set of user actions
("events" or "touchpoints") that contribute in some manner to a desired outcome,
and then assigning a value to each of these events. Users might click on
multiple ads before converting. This can make it challenging to assign proper
credit to the different marketing channels. For example, should all the credit
go to the last ad the user saw, or the first ad? Should all the ads share in the
credit equally? Or should some other rule be used to determine how to distribute
credit?

## Data-driven attribution (DDA)

DDA attempts to algorithmically work out a fair weighting of credit among
marketing channels. For example, a particular display ad might not convert
immediately, but users who click the display ad might be much more likely to
convert later on. In this case, the display ad should get credit, even though
it may not be the first or last ad on a user's path to conversion.

## Fractribution Package

Google Marketing Platform products already support DDA. This Fractribution
package is a DDA algorithm that generates **user-level fractional attribution
values** for each conversion. The advantage of user-level attribution is that
the attribution values can later be joined with custom user-level data (e.g.
transaction value, lifetime value etc). This can be useful when regulation or
data policy prevents ecommerce/revenue events from being shared with the Google
Marketing Platform.

Please see Fractribution_Slides.pdf file in this directory for more background
on use cases and details on the DDA algorithm.

## Using Fractribution

For more instructions, including a tutorial for running Fractribution over
sample GA360 data from the Google Merchandise Store, from see py/README.md.

## Directory structure

```bash
fractribution
├── README.md
├── py
├──── README.md
├──── main.py
├──── fractribution.py
├──── templates/
├──── Dockerfile
└──── start.py
```

Disclaimer: This is not an officially supported Google product.
