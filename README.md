# nocomment-master

This is the central controller of nocom.

Q: "What is this?"
A: See [here](https://github.com/nerdsinspace/nocom-explanation/blob/main/README.md)

Q: "Where are the interesting parts?"
A: All of it is interesting! But here are some *especially* interesting parts:
* [The code that defined what areas were scanned on what schedule](src/main/java/nocomment/master/tracking/TrackyTrackyManager.java#L35-L67)
* [The Monte Carlo particle filter that actually tracked everyone](src/main/java/nocomment/master/tracking/MonteCarloParticleFilterMode.java#L192-L394)
* The remote base downloader [Part 1](src/main/java/nocomment/master/slurp/BlockCheckManager.java) and [Part 2](src/main/java/nocomment/master/slurp/SlurpManager.java)
* The clusterer, which took in a stream of hits and decided what was or wasn't a base [Part 1](src/main/java/nocomment/master/clustering/Aggregator.java) and [Part 2](src/main/java/nocomment/master/clustering/DBSCAN.java)
* [The code that associatied players with bases](src/main/java/nocomment/master/util/Associator.java#L53-L127)

Q: "How do I run this?"
A: You don't. It's far too heavily specialized for some 2b2t-specific behaviors, such as how long a chunk stays loaded after a player walks away, how large an area a player loads around themselves, how long a player can stay AFK without being kicked, etc. And regardless, the worker bot code is not being released, so you'd have to reverse engineer their connection protocol and guess how to make a functional AFK bot that speaks that language.
