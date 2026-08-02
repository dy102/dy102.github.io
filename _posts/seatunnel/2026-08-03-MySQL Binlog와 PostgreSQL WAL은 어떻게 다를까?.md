---
toc: true
title: "MySQL Binlog와 PostgreSQL WAL은 어떻게 다를까?"
---
# MySQL Binlog와 PostgreSQL WAL은 어떻게 다를까?

CDC(Change Data Capture)는 데이터베이스에서 발생한 변경을 감지해 다른 시스템으로 전달하는 기술이다.

Debezium 같은 CDC 도구는 데이터베이스를 반복 조회하는 대신, 데이터베이스가 내부적으로 기록하는 로그를 읽는다.

MySQL에서는 주로 **binlog**, PostgreSQL에서는 **WAL**을 사용한다.

둘 다 데이터 변경을 기록하지만, 로그의 역할과 Debezium이 이를 소비하는 방식은 다르다.

---

## MySQL에는 redo log와 binlog가 따로 있다

MySQL에서 로컬 장애 복구는 주로 InnoDB의 **redo log**가 담당한다.

서버가 갑자기 종료됐을 때, 디스크에 완전히 반영되지 않은 변경을 다시 적용하기 위한 내부 로그다.

반면 **binlog**는 MySQL 서버 수준에서 발생한 변경을 event 형태로 기록한다. 주로 replication과 point-in-time recovery에 사용되며, Debezium도 이 binlog를 읽는다.

```text
InnoDB redo log
→ crash recovery

MySQL binlog
→ replication
→ point-in-time recovery
→ CDC
```

Debezium은 일반적으로 row-based binlog를 사용한다.

예를 들어 다음 SQL이 실행되면:

```sql
UPDATE users
SET name = 'Kim'
WHERE id = 1;
```

Debezium은 원본 SQL보다 실제 row 변경에 가까운 정보를 얻는다.

```text
before:
  id: 1
  name: Lee

after:
  id: 1
  name: Kim
```

테이블 구조를 변경하는 DDL은 binlog의 query event를 통해 전달될 수 있다.

```sql
ALTER TABLE users
ADD COLUMN email VARCHAR(255);
```

즉 MySQL binlog는 Debezium이 row 변경과 DDL을 비교적 직접적으로 처리할 수 있는 event stream에 가깝다.

---

## PostgreSQL은 WAL을 중심으로 동작한다

PostgreSQL의 WAL(Write-Ahead Log)은 장애 복구의 핵심 로그다.

PostgreSQL은 데이터 파일을 변경하기 전에 관련 정보를 WAL에 먼저 기록한다. 서버가 비정상 종료되면 WAL을 재생해 데이터베이스를 일관된 상태로 복구한다.

하지만 WAL은 SQL 로그가 아니다.

예를 들어 다음 SQL이 실행되더라도:

```sql
ALTER TABLE users
ADD COLUMN email VARCHAR(255);
```

WAL에 이 SQL 문자열이 그대로 저장된다고 볼 수는 없다.

대신 PostgreSQL 내부에서 발생한 변경이 binary record 형태로 남는다.

```text
시스템 카탈로그 변경
heap 또는 index page 변경
트랜잭션 commit 정보
```

WAL은 사람이 읽기 좋은 변경 이벤트라기보다, PostgreSQL 엔진이 내부 상태를 다시 재현할 수 있도록 만든 redo log에 가깝다.

---

## PostgreSQL의 복제도 WAL을 사용한다

WAL이 복구용이라고 해서 PostgreSQL이 복제에 불리한 것은 아니다.

PostgreSQL의 physical replication은 primary의 WAL을 standby 서버가 받아 그대로 재생하는 방식이다.

```text
Primary
  → WAL 생성
  → Standby가 WAL 재생
```

같은 PostgreSQL 서버를 거의 동일하게 복제하는 데에는 이 방식이 자연스럽다. DDL, index, 시스템 카탈로그 변경도 WAL 재생 과정에서 함께 반영된다.

다만 CDC처럼 row-level 변경을 외부 시스템에 전달하려면 WAL을 그대로 사용할 수 없다. WAL이 PostgreSQL 내부 형식이기 때문이다.

그래서 PostgreSQL은 **logical decoding**이라는 계층을 제공한다.

---

## pgoutput은 무엇인가?

PostgreSQL CDC의 흐름은 대략 다음과 같다.

```text
PostgreSQL WAL
→ logical decoding
→ pgoutput
→ Debezium
```

`pgoutput`은 PostgreSQL 서버에 내장된 logical decoding output plugin이다.

WAL의 저수준 변경을 다음과 같은 logical replication message로 변환한다.

```text
BEGIN
RELATION
INSERT
UPDATE
DELETE
TRUNCATE
COMMIT
```

`INSERT`, `UPDATE`, `DELETE`는 row 변경을 나타낸다.

`RELATION` 메시지는 해당 row가 속한 테이블의 구조를 설명한다.

```text
relation OID
schema name
table name
column name
type OID
type modifier
key 여부
```

Debezium PostgreSQL connector는 이 메시지들을 받아 Kafka Connect의 `SourceRecord` 형태로 변환한다.

---

## Debezium이 두 데이터베이스에서 보는 것

MySQL에서는 Debezium이 binlog event를 직접 소비한다.

```text
MySQL binlog
→ QueryEvent / TableMapEvent / RowsEvent
→ Debezium
```

PostgreSQL에서는 WAL을 직접 읽지 않고, `pgoutput`이 만든 logical replication message를 소비한다.

```text
PostgreSQL WAL
→ logical decoding
→ pgoutput
→ Relation / Insert / Update / Delete
→ Debezium
```

따라서 두 connector가 받는 입력은 처음부터 다르다.

| 구분          | MySQL              | PostgreSQL           |
| ----------- | ------------------ | -------------------- |
| 로컬 장애 복구    | InnoDB redo log    | WAL                  |
| 복제에 사용되는 로그 | binlog             | WAL                  |
| Debezium 입력 | binlog event       | pgoutput message     |
| row 변경      | RowsEvent          | Insert/Update/Delete |
| 테이블 구조 정보   | TableMapEvent, DDL | Relation message     |
| 원본 DDL      | query event로 접근 가능 | pgoutput이 직접 제공하지 않음 |

---

## 정리

MySQL과 PostgreSQL 모두 로그 기반 CDC를 지원하지만, 내부 구조는 다르다.

MySQL은 로컬 장애 복구를 위한 InnoDB redo log와, replication 및 CDC를 위한 binlog를 별도로 사용한다.

PostgreSQL은 WAL을 장애 복구와 physical replication의 공통 기반으로 사용하고, logical decoding과 `pgoutput`을 통해 외부 시스템이 이해할 수 있는 변경 메시지를 만든다.

```text
MySQL:
binlog
→ Debezium

PostgreSQL:
WAL
→ logical decoding
→ pgoutput
→ Debezium
```

따라서 CDC를 이해할 때는 단순히 “둘 다 데이터베이스 로그를 읽는다”고 보는 것보다, 각 로그가 어떤 목적으로 설계됐고 어떤 변환 계층을 거쳐 외부에 노출되는지를 함께 보는 것이 중요하다.
