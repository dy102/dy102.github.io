---
toc: true
title: "Checkpoint와 BigQuery Buffered Stream으로 Exactly-Once Sink 구현하기"
---
# Checkpoint와 BigQuery Buffered Stream으로 Exactly-Once Sink 구현하기

스트리밍 시스템에서 checkpoint를 지원하면 자연스럽게 exactly-once도 보장될 것이라고 생각하기 쉽다.

하지만 BigQuery Sink를 구현하면서 알게 된 것은, checkpoint만으로는 외부 시스템에 대한 exactly-once를 완성할 수 없다는 점이었다.

Checkpoint는 스트리밍 엔진이 장애 이후 **어디서부터 처리를 다시 시작해야 하는지**를 저장한다. 하지만 데이터베이스나 외부 스토리지에 이미 발생한 쓰기까지 되돌려주지는 않는다.

따라서 exactly-once sink를 구현하려면 두 가지가 함께 필요하다.

- 스트리밍 엔진의 처리 상태를 저장하는 checkpoint
- checkpoint 완료 여부와 외부 데이터의 공개 시점을 연결하는 commit protocol

이 글에서는 Apache SeaTunnel의 BigQuery Sink가 checkpoint와 BigQuery Buffered Stream을 이용해 이 문제를 어떻게 해결하는지 살펴본다. 또한 구현을 다시 검토하는 과정에서 발견한 offset 처리 문제와 수정 과정도 함께 소개한다.

---

## Checkpoint만으로는 부족한 이유

다음과 같은 실행 흐름을 생각해보자.

```
1. 마지막 성공 checkpoint는 CP-9다.
2. 레코드 A를 외부 시스템에 기록한다.
3. CP-10이 실패한다.
4. 작업이 CP-9에서 복구된다.
5. 레코드 A가 다시 처리된다.
```

외부 시스템에 대한 첫 번째 쓰기가 이미 공개된 상태라면, 복구 후 A가 다시 기록되면서 중복이 발생할 수 있다.

Checkpoint는 엔진의 source offset과 operator state는 되돌릴 수 있지만, 외부 시스템에 이미 반영된 side effect를 자동으로 rollback하지는 않는다.

그래서 exactly-once sink는 일반적으로 외부 쓰기를 두 단계로 나눈다.

```
Prepare:
    데이터를 외부 시스템에 준비하지만 아직 공개하지 않는다.

Commit:
    checkpoint가 성공한 이후 데이터를 공개한다.
```

전통적인 분산 트랜잭션의 2PC와 완전히 같은 구현일 필요는 없다. 외부 시스템이 제공하는 transaction, atomic rename, idempotent upsert 또는 visibility control을 이용할 수도 있다.

중요한 것은 **checkpoint의 성공 경계와 외부 데이터의 commit 경계를 일치시키는 것**이다.

---

## BigQuery Buffered Stream의 역할

BigQuery Storage Write API의 Buffered Stream에 append된 데이터는 즉시 query 결과에 노출되지 않는다.

Writer는 먼저 데이터를 Buffered Stream에 append하고, 이후 `FlushRows`를 호출해 특정 offset까지의 데이터를 visible하게 만든다.

SeaTunnel BigQuery Sink는 이 특성을 checkpoint lifecycle과 연결한다.

```
Writer
  └─ AppendRows(offset)

Checkpoint
  └─ streamName + nextOffset 저장

Prepare commit
  └─ streamName + flushOffset 생성

Checkpoint completed
  └─ FlushRows(streamName, flushOffset)

BigQuery readers
  └─ 해당 offset까지의 데이터 확인 가능
```

각 append에는 명시적인 offset이 사용된다.

```java
long appendOffset = nextOffset;
return streamWriter.append(jsonRows, appendOffset);
```

성공한 경우에만 다음 offset으로 이동한다.

```java
public void onAppendSuccess(int rowCount) {
    nextOffset += rowCount;
}
```

Checkpoint state에는 row 자체가 아니라 다음 정보가 저장된다.

```
streamName
nextOffset
checkpointId
```

장애가 발생하면 같은 stream과 checkpoint 당시의 `nextOffset`을 이용해 writer를 복원한다.

---

## `prepareCommit()`은 정말 side effect가 없을까?

처음에는 `prepareCommit()`이 외부 side effect를 만들지 않는다고 단순하게 이해하기 쉬웠다.

하지만 실제로는 그렇지 않다.

```java
@Override
public Optional<BigQueryCommitInfo> prepareCommit() {
    flush();

    long flushOffset = batchWriter.getNextOffset() - 1;
    return Optional.of(
            new BigQueryCommitInfo(
                    batchWriter.getStreamName(),
                    flushOffset));
}
```

`prepareCommit()`은 내부에서 `flush()`를 호출하고, 실제로 BigQuery Buffered Stream에 데이터를 append한다.

즉 외부 시스템의 상태는 이미 변한다.

다만 아직 `FlushRows`가 호출되지 않았기 때문에 일반 reader에게 보이지 않을 뿐이다.

따라서 더 정확한 표현은 다음과 같다.

> `prepareCommit()`에는 외부 side effect가 없지 않다. 다만 checkpoint 완료 전에는 그 side effect가 committed, 즉 visible 상태가 되지 않는다.
> 

Exactly-once에서 중요한 것은 side effect 자체를 완전히 없애는 것이 아니다.

실패한 checkpoint에서 발생한 side effect가 최종 결과에 잘못 노출되거나, 복구 후 중복으로 반영되지 않도록 제어하는 것이 중요하다.

---

## Append offset이 중복을 방지하는 방법

예를 들어 checkpoint state에 다음 값이 저장되어 있다고 하자.

```
streamName = stream-A
nextOffset = 100
```

Checkpoint 이후 writer가 offset 100부터 데이터를 append했지만, checkpoint가 완료되기 전에 장애가 발생할 수 있다.

BigQuery stream에는 데이터가 append되었지만 아직 `FlushRows`가 호출되지 않았기 때문에 visible하지 않은 상태다.

복구 후 writer는 checkpoint state를 기준으로 다시 offset 100부터 append를 시도한다.

이때 BigQuery는 이미 해당 offset이 사용되었다는 사실을 알고 있으므로 `ALREADY_EXISTS`를 반환할 수 있다.

```
BigQuery stream end = 120
retry offset = 100

→ ALREADY_EXISTS
```

반대로 요청 offset이 실제 stream 끝보다 앞서 있다면 `OUT_OF_RANGE`가 발생한다.

```
BigQuery stream end = 100
requested offset = 120

→ OUT_OF_RANGE
```

두 오류는 모두 offset과 관련되어 있지만 의미는 정반대다.

| 오류 | 의미 |
| --- | --- |
| `ALREADY_EXISTS` | 요청한 offset에 데이터가 이미 존재한다 |
| `OUT_OF_RANGE` | 요청 offset이 현재 stream 끝보다 앞서 있다 |

---

## 잘못된 offset conflict 처리

초기 구현에서는 두 오류를 모두 동일한 offset conflict로 처리했다.

```java
return code == Code.ALREADY_EXISTS_VALUE
        || code == Code.OUT_OF_RANGE_VALUE;
```

그리고 둘 중 하나가 발생하면 기존 stream을 버리고 새로운 Buffered Stream을 생성한 뒤 현재 batch를 다시 append했다.

```
Offset conflict
  → 기존 stream 종료
  → 새로운 Buffered Stream 생성
  → 현재 batch 재전송
```

처음에는 복구 과정에서 충돌한 stream을 버리고 새 stream에서 다시 시작하면 안전하다고 생각했다.

하지만 `ALREADY_EXISTS`의 의미를 다시 살펴보면서 이 처리가 위험할 수 있다는 것을 발견했다.

`ALREADY_EXISTS`는 이전 append가 이미 BigQuery에 성공했지만 응답이 유실되었거나, 복구 후 동일 offset을 다시 요청했다는 의미일 수 있다.

이 경우 기존 stream의 append를 성공으로 인정해야 한다.

그런데 새로운 stream을 만들어 동일 데이터를 다시 append하면, 기존 stream의 데이터와 새 stream의 데이터가 모두 나중에 visible해져 중복이 발생할 수 있다.

---

## `ALREADY_EXISTS`와 `OUT_OF_RANGE`를 분리하기

이를 수정하기 위해 두 오류를 다른 방식으로 처리하도록 변경했다.

### `ALREADY_EXISTS`

동일 offset에 데이터가 이미 존재한다면 append가 이미 성공한 것으로 간주하고 로컬 offset만 전진시킨다.

```java
if (isAlreadyExists(response)) {
    markAppendAsSuccessful(dataToSend);
    return;
}
```

```java
private void markAppendAsSuccessful(JSONArray dataToSend) {
    streamWriter.onAppendSuccess(dataToSend.length());
}
```

이제 기존 stream을 유지하므로, 이미 append된 데이터를 새로운 stream에 다시 작성하지 않는다.

### `OUT_OF_RANGE`

`OUT_OF_RANGE`는 로컬 `nextOffset`이 BigQuery의 실제 stream 위치보다 앞서 있다는 의미다.

이 상태에서는 이전 batch가 누락되었거나 writer state가 stream과 일치하지 않을 수 있다.

현재 batch만 새로운 stream에 재전송하면 이전 데이터 누락을 숨길 수 있기 때문에, 자동으로 우회하지 않고 안전하게 실패하도록 변경했다.

```
OUT_OF_RANGE
  → offset state 불일치 가능성
  → stream을 재생성하지 않음
  → append 실패 처리
```

이 수정은 Apache SeaTunnel PR #11561에서 진행했다.

---

## Parallel Writer와 atomic visibility

Sink parallelism이 2 이상이라면 각 writer는 별도의 Buffered Stream을 가질 수 있다.

```
Writer 0 → Stream A
Writer 1 → Stream B
Writer 2 → Stream C
```

Committer는 각 stream에 대해 `FlushRows`를 호출한다.

```
FlushRows(Stream A)
FlushRows(Stream B)
FlushRows(Stream C)
```

이 호출들은 하나의 BigQuery transaction으로 atomic하게 묶이지 않는다.

따라서 A와 B의 flush는 성공했지만 C의 flush가 실패하면, 일시적으로 A와 B의 데이터만 visible한 partial visibility가 발생할 수 있다.

```
A commit 성공
B commit 성공
C commit 실패
```

이 점에서 Buffered Stream은 여러 stream을 동시에 원자적으로 공개하는 기능을 제공하지 않는다.

하지만 partial visibility와 duplicate final result는 구분할 필요가 있다.

Commit이 실패하면 SeaTunnel은 commit을 재시도하거나 작업을 복구한다. 각 stream은 명시적인 offset을 사용하므로 동일 stream에서 무작정 같은 데이터를 다시 append하지 않는다.

특히 `ALREADY_EXISTS`를 성공으로 처리하면, 이미 append된 데이터를 새 stream에 복제하지 않고 기존 stream의 진행 상태를 그대로 인정할 수 있다.

따라서 여러 stream이 한순간에 모두 visible해지는 강한 atomic visibility는 제공하지 않더라도, 복구가 완료된 최종 결과에서는 데이터가 정확히 한 번 반영되도록 만들 수 있다.

```
Atomic visibility:
    checkpoint의 모든 결과가 같은 순간에 보이는가?

Exactly-once final result:
    복구 이후 최종적으로 누락과 중복 없이 반영되는가?
```

두 보장은 관련되어 있지만 완전히 같은 개념은 아니다.

---

## Iceberg Sink에서 발견한 다른 형태의 문제

비슷한 시기에 Iceberg Sink의 exactly-once 문제를 수정하는 PR #10714도 리뷰했다.

Iceberg에서는 각 writer가 data file을 생성하고, 그 결과를 `WriteResult`로 전달한다.

기존 구현은 각 worker의 결과를 따로 commit했다.

```
Checkpoint 42

Worker 0 files → Snapshot A
Worker 1 files → Snapshot B
Worker 2 files → Snapshot C
```

중간 snapshot commit 이후 실패하면 일부 worker의 데이터만 visible해질 수 있었다.

이를 해결하기 위해 동일 checkpoint에 속한 모든 worker의 `WriteResult`를 모아 snapshot 하나로 commit하도록 변경했다.

```
Worker 0 files ┐
Worker 1 files ├─→ One Iceberg snapshot
Worker 2 files ┘
```

또한 snapshot metadata에 checkpoint ID를 기록하고, 복구 시 이미 commit된 checkpoint라면 다시 commit하지 않도록 만들었다.

BigQuery와 Iceberg는 외부 시스템의 commit primitive가 다르다.

- BigQuery Buffered Stream은 stream별 offset과 visibility를 관리한다.
- Iceberg는 여러 writer의 파일을 table snapshot 하나로 묶을 수 있다.

구현 방식은 달라도 공통 원칙은 같다.

> 외부 시스템이 제공하는 atomicity와 idempotency 단위를 이해하고, 이를 스트리밍 엔진의 checkpoint 단위와 연결해야 한다.
> 

---

## Exactly-once는 하나의 옵션이 아니라 프로토콜이다

이번 구현을 다시 검토하면서 exactly-once는 단순히 checkpoint 옵션을 활성화하거나 commit API 하나를 호출한다고 만들어지는 보장이 아니라는 점을 다시 확인했다.

Checkpoint는 다음 질문에 답한다.

> 장애 후 어디서부터 다시 처리할 것인가?
> 

Writer state는 다음 질문에 답한다.

> 외부 시스템의 어느 위치에서 쓰기를 계속할 것인가?
> 

Commit protocol은 다음 질문에 답한다.

> 어느 checkpoint의 결과까지 외부 사용자에게 공개할 것인가?
> 

그리고 idempotency 처리는 다음 질문에 답한다.

> 성공 여부가 불확실한 작업을 재시도해도 결과가 달라지지 않는가?
> 

BigQuery Sink에서는 다음 요소들이 함께 작동한다.

```
Checkpoint state
    + Buffered Stream
    + Explicit offset
    + FlushRows
    + Idempotent retry handling
```

결국 exactly-once sink를 구현한다는 것은 중복 제거 로직 하나를 추가하는 일이 아니다.

스트리밍 엔진의 복구 모델과 외부 시스템의 쓰기·공개·재시도 의미를 하나의 일관된 프로토콜로 연결하는 일이다.
