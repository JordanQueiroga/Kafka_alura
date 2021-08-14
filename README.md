<h4>DATELHES IMPORTANTES, ANTES DE INICIAR O KAFKA TEMOS QUE INICIAR O ZOOKEPER, NO CMD RODE OS SEGUINTES COMANDOS:</h4>

<h5>RODAR O ZOOKEPER:</h5>
<p>
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
</p>

<h5>RODAR O KAFKA:</h5>
<p>
.\bin\windows\kafka-server-start.bat .\config\server.properties
</p>

<h5>CRIAR UM TOPICO:</h5>
<p>
.\bin\kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic <NOME_TOPICO(sujestão do kafka é não misturar o '_' com '.')>
</p>

<h5>LISTAR OS TOPICOS:</h5>
<p>
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
</p>

<h5>DESCREVER OS TOPICOS:</h5>
<p>
.\bin\windows\kafka-consumer-groups.bat --describe --groups --bootstrap-server localhost:9092 
</p>

<h5>VERIFICA A SITUAÇÃO DOS TOPICOS:</h5>
<p>
.\bin\windows\kafka-topics.bat --describe --bootstrap-server localhost:9092 
</p>

<h5>LISTA OS GRUPOS:</h5>
<p>
.\bin\windows\kafka-consumer-groups.bat --describe --all-groups --bootstrap-server localhost:9092
</p>

<h5>CRIAR PARTIÇÃO NO TÓPICO:</h5>
<p>
.\bin\windows\kafka-topics.bat --alter --topic  <NOME_TOPICO> --partitions 2 --zookeeper localhost:2181
</p>

<h5>CRIAR UM PRODUTOR:</h5>
<p>
.\bin\windows\kafka-console-producer.bat --brocker-list localhost:9092 --topic <NOME_TOPICO>
</p>

<h5>CRIAR UM CONSUMIDOR:</h5>
<p>
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic <NOME_TOPICO> <--from-beginning(opcional, ele busca as mensagens desde o inicío)> --partition <numero da partição>
</p>


<h5>CRIAR UM CONSUMIDOR:</h5>
<p>
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic <NOME_TOPICO> <--from-beginning(opcional, ele busca as mensagens desde o inicío)> --partition <numero da partição>
</p>

