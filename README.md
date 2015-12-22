This package contains the following tools:

**1. Linking Analyzer**

```
linking-analyzer -s server_url -u user -p password -o output_file -g granularity -t timeout
```
Linking Analyzer produces an indented HTML file where, for each DBpedia Ontology class, the number (and percentage over the total) of instances of that class having some mention in a document is reported, as well as the number (and percentage over the total) of documents containing mentions of instances of that class. Granularity refers to the minimum number of mentions an entity must have to be considered in the report (i.e., granularity 100 means entities with at least 100 mentions each are included in the report).

**2. KS Content Report**

```
KSContentReport -s server_url -u user -p password -q query_folder -o output_file -t timeout
```
KS Content Report runs arbitrary analytic queries against a running KnowledgeStore instance, such as the most frequent FrameNet frame extracted in the news, the most cited DBpedia person, organization, and location in the news, etc. Queries are read at run-time from a folder, where each SPARQL query should be inserted in a single text file with extension ".qry". A predefined set of queries is available in /src/main/config/KSContentReport-queries .

```
ks-dump -s server_url -o output_file -u user -p password [-r] [-m]
```
Downloads the content of a running KnowledgeStore instance, either Resource layer (r) or Mention layer (m), producing an RDF file with the required content.
