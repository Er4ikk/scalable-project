<h1>Build</h1>

<p>Per fare la build ed il deploy su dataproc bisogna scaricare il codice sorgente sottoforma di archivio zip.
Una volta scaricato, decomprimerlo e aprire la cartella rimanente con Intellij.</p>

<p>Attendere la fine dell'indcizzazione da parte di Intellij ed assicurarsi che abbia scaricato i seguenti pacchetti:</p>

<ul>
  <li>Scala version 2.12.18</li>
  <li>Spark version 3.5.3</li>
</ul>

<p> Questi pacchetti sono necessari per far funzionare lo script su ambiente dataproc.</p>

<p>Per creare il fat jar da eseguire su data proc, andare su: "File > Project Structure > Artifacts"  creare un nuovo artefatto di tipo jar se non presente usando il modulo "example-project-2". </p>

<p>Una volta creato l'artefatto assicurarsi che vi sia al suo interno la cartella META-INF con il relativo file di manifest (vedere nella tab Output Layout). Se è presente allora cliccare su "Ok"</p>

<p>Per eseguire la build del fat jar andare su "Build > Build artifacts... > nome-del-jar > build" . Inizierà il processo di building, il fat jar di output dovrebbe pesare attorno ai 357MB.</p>

<h1>Deploy</h1>
<p> Una vota ottenuto il fat jar, andare su Google CLoud > dataproc > create new cluster. Per le prove è stato utilizzato il cluster base di dataproc(N4 con debian 2.2.64) e se si ha una sottoscrizione come studente fare attenzione a non  superare i 500GB di archiviazione.</p>
<p> Creare un bucket andando su Google Cloud console > barra di ricerca > scrivere bucket > creare un bucket chiamato "example-spark". Dopodichè caricare il fat jar ed il csv da analizzare.</p>

<h1> RUN</h1>
<p> Per eseguire un job con il fat jar assicurarsi di aver fatto i 2 step precedenti. Andare poi all'interno del cluster e lanciare un nuovo job. Compilare i campi per eseguire il job, e nel form "Argomenti" inserire nella seguente forma gli argomenti:</p>
<code> percorso-file-csv-gcs 1 4 -cloud</code>

<p> Il primo argomento è il percorso del file csv in formato gcs. Es: gs://example-spark/example.csv</p>
<p> Il secondo argomento è la dimensione che si vuole analizzare del file. E sono disponibili i seguenti valori:</p>
<ul>
  <li>1 -> un quarto di dataset</li>
  <li>2 -> metà dataset</li>
  <li>3 -> tre quarti di dataset</li>
  <li>4 -> dataset completo</li>
  
</ul>


<p> Il terzo argomento indica invece l'algoritmo da utilizzare. Si possono usare in ambiente di dataproc solo gli algoritmi 3 e 4</p>

<p> Il quarto argomento indica l'esecuzione cloud (facoltativa se si è in locale)</p>

<h1> Prove in ambiente locale</h1>
<p> se si voglia testare il progetto in locale, la procedura è  la seguente</p>
<p> andare nel file "build.sbt" e rimuovere "%provided" dalle dipendenze di spark-sql e spark-core e ribuildare tramite intellij il file ,sbt; questo serve per includere le dipendenze spark per l'esecuzione in locale. </p>
<p> Andare nel Main e  sovrascrivere la variabile csvOut con il valore "output"</p>

<p> Una volta fatte queste modifiche creare un profilo di configurazione su Intellij. Andare nel rettangolino a sinistra del pulsante play e creare nuova configuarazione come applicazione.</p>
<p> Inserire nelle impostazioni vm le i seguenti parametri:</p>
<p>
  --add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.misc=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
-Xmx8g
</p>

<p> Negli argomenti dell'esecuzione programma inserire: percorso-csv dimensione-da-analizzare algoritmo </p>
<p> Es: "/home/example/example.csv" 1 1 </p>
