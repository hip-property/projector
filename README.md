# Projector

Projector provides a simple way to create projections from streams of data

It's designed to allow the sharable creation of `ProjectorSpec`'s, which can be defined in a common
library and shared between multiple consumers.

This segregates the definition of how to use a stream o fdata, from the actual consumption of one.

Here's an example:

