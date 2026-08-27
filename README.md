## 환경 설정
**Server**
- VirtualBox 6.1.36
- Ubuntu 20.04.6
- Confluent Kafka 7.1X(Apache Kafka 3.1.X 호환)
- PostgreSQL
- JDK 11

**Cleint**
- JDK 17
- Multi Module
<br><br>


## 개발 환경 설정
**윈도우 환경에서 편리하게 개발하기 위함**
- 고정 IP
- putty
- mtputty
<br><br>

## 설치 가이드
- [Virtual Box](https://www.oracle.com/kr/virtualization/technologies/vm/downloads/virtualbox-downloads.html)
- [Ubuntu](https://ubuntu.com/download/server)
- [Confluent Kafka](https://www.confluent.io/previous-versions/)
- [putty](https://putty.org/index.html)
- mtputty
```
https://ttyplus.com/download/mtputty_setup.exe
# 주의 설치 전 확인
```
<br><br>

## 터미널 가이드
``` bash
$ sudo apt update
$ sudo apt ugrade

#JDK 설치
$ sudo apt install openjdk-11-jdk
#JDK 설치 확인
$ java -version

# Confulent Kafka Linux 웹을 사용해서 다운 받거나 경로를 사용해 아래처럼 다운로드
$ wget [Confluent Kafka DownLoad URL]

# 설치 받은 경로에서
$ tar -xvf [DownLoad Kafka File] 

# 마지막 문단에 추가
$ vi .bashrc
#export CONFLUENT_HOME=/home/[사용자이름]/confluent
# export PATH=.:$CONFLUENT_HOME/bin
$ . .bashrc

# 확인
$ echo $CONFLUENT_HOME


# postgresql 설치
$ sudo apt install postgresql postgresql-client

#확인
$ sudo systemctl status postgresql

$ su - postgres
postgres$ psql

# 확인
postgres=# \l
postgres=# exit

# OS User가 아닌 Super User 비밀번호 변경
postgres$ psql -c "alter user postgres with passowrd '비밀번호'"

postgres$ cd etc/postgresql/12/main
postgres$ vi postgresql.conf
# 수정
# Listen_address = '*'

postgres$ vi pg_hba.conf
# IPv4 local connection수정
# 0.0.0.0/0

postgres$ exit
$ sudo systemctl restart postgresql

```  
<br><br>
 
## 서버 실행
```bash
# 카프카는 주키퍼를 의존하기 때문에 주키퍼 먼저 실행해야 함

# 주키퍼 실행
/confluent/bin$ zookeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties

# 카프카 실행
/confluent/bin$ kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties


# sh를 만들어서 편리하게 사용
# Home Directory에서 작성
$ vi zoo_start.sh
# $CONFLUENT_HOME/bin/zookeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties

$ vi kafka_start.sh
# $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties

# 다른 sh 파일이 있다면 이름 명시
$ chmod +x *.sh

# 사용
$ zoo_start.sh
$ kafka_start.sh
```
<br><br>
## 추가 설정
```bash
$ mkdir data
$ cd data

/data$ mkdir kafka-logs
/data$ mkdir zookeeper

$ cd $CONFLUENT_HOME/etc/kafka
/confluent/etc/kafka$ vi server.properties

# VM이 재가동 되면 로그가 삭제되기 때문에 로그 경로 수정
# Log Basics 부분에서 수정
# log.dirs=/home/[사용자명]/data/kafka-logs

/confluent/etc/kafka$ vi zookeeper.properties
# the direcotry where the snapshot is stored 부분수정
# dataDir=/home/[사용자명]/data/zookeeper 
```
<br><br>
## Kafka CLI 명령어
```bash
# Topic 생성
#기본 파티션 1개 []는 부가 기능
$ kafka-topics --bootstrap-server localhost:9092 --create --topic topic_name [--partitions 3] [--replication-factor 2]


# Porducer
kafka-console-producer --bootstrap localhost:9092 --topic topic_name [--property key.separator=: --property parse.key=true]



# Consumer
kafka-console-consumer --bootstrap-server localhost:9092 --topic topic_name [--property print.key=true --property print.value=true] [--from-beginning] [--property print.partition=true]

#Key가 String인 경우 
# --property pirnt.. 작성과 함께 
# --key-deserializer "org.apache.kafka.common.serialization.IntegerDeserializer"


# log 파일  확인
kafka-dump-log --deep-iteration --files /home/[사용자명]/data/kafka-logs/[topic_name]/00000000000000000000.log --print-data-log


#Config/Partition/offset 별 설정 논외
```
<br><br>
## 카프카 멀티 브로커
- 멀티 노드 카프카 클러스터가 아닌 멀티 브로커를 구축한다.
- 그 이유는 하나의 VM이라 멀티 노드가 아닌 하나의 VM으로 멀티 브로커를 사용
```
$ cd data
$ mkdir kafka-logs-01
$ mkdir kafka-logs-02
$ mkdir kafka-logs-03
$ mkdir zookeeper_m

$ cd $CONFLUENT_HOME/bin/kafka

$ cp server.properties server_01.properties
$ cp server.properties server_02.properties
$ cp server.properties server_03.properties


# vi로 버전들 수정
# brocker.id=1 <버전 별 숫자로>
# 주석 해제
# listeners=PLAINTEXT://:9092
<버전 별로 끝 번호 수정 9092 -> 9093...>

# 수정
logs.dirs=/home/[사용자명]/data/kafka-logs-01 <버전 별 숫자로>


$ cd
$ cp kafka_start.sh kafka_start_01.sh
$ cp kafka_start.sh kafka_start_02.sh
$ cp kafka_start.sh kafka_start_03.sh

# vi로 각 파일
$CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server_<버전에 맞게 번호로>.properties


$ cd $CONFLUENT_HOME/etc/kafka
$ cp zookeeper.properties zookeeper_m.properties

# 동일한 Topic Name 때문에 충돌이 일어날 수도 있으니 수정
$ vi zookeeper_m.properties
# dataDir=/home/yoon/data/zookeeper_m  

$ cd
$ cp zoo_start.sh zoo_start_m.sh

# 경로 수정
$ vi zoo_start_m.sh
# $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper_m.properties


```


