I comandi per usare i tool sono:

```
linking-analyzer -t taxonomy_file -i instances_file -n num_documents
```
crea report HTML su linking a partire dai dati nei file passati,
i quali si ottengono eseguendo due query SPARQL sul KS e salvando
relativo output come tsv o csv. Le query le potete vedere facendo
linking-analyzer --help

```
ks-dump -s server_url -o file.tql.gz [-u user] [-p pwd] [-r] [-m]
```
scarica il contenuto dei layer risorse (-r) o menzioni (-m) in un
file RDF, passando tramite l'API crud del KS. Specificando
username e password (opzionali) è possibile scaricare
direttamente da knowledgestore2.fbk.eu
