# 커피 주문 시스템: 코드 바로 위에 주석을 단 입문 강의

**대상:** 프로그래밍을 막 배우기 시작한 학부 1~2학년. Java의 `class`, 함수, SQL의 `SELECT`를 처음 봐도 읽을 수 있게 작성했다.

**읽는 법:** 각 코드 블록에는 현재 프로젝트의 **전체 원본 코드 줄이 순서대로 들어 있다.** 설명이 필요한 코드 바로 위에 해당 언어의 주석을 덧붙였다. 따라서 코드와 설명을 한 화면에서 같이 읽을 수 있다. Java·Gradle은 `//`, SQL은 `--`, YAML·설정 파일은 `#`가 올바른 주석 문법이다. **실행 중인 원본 파일은 고치지 않았다.**

Gradle이 자동 생성한 실행 스크립트 `gradlew`, `gradlew.bat`, 바이너리 JAR와 빌드 결과는 제외했다. 기존 `README.md`는 설계 설명이라 중복 복사하지 않았다. 아래에는 사람이 작성한 소스·설정·SQL·테스트 41개 파일을 모두 담았다.

## 코드를 보기 전에: 프로그램이 하는 일

1. 사용자는 `POST /api/points/charges`로 포인트를 충전한다. 1원은 1P다.
2. 사용자는 커피 메뉴 ID와 `Idempotency-Key`를 보내 주문한다. 이 키는 “이번 결제 시도의 이름”이다.
3. 서버는 **MySQL에 있는 가격**을 확인하고 잔액을 뺀 뒤, 주문과 Kafka 발행 대기 기록을 함께 저장한다.
4. 별도 작업이 발행 대기 기록을 Kafka로 보낸다. 다른 작업은 그 메시지를 받아 수집 표에 저장한다.
5. 인기 메뉴 API는 MySQL 주문을 세어 정확한 상위 세 메뉴를 돌려준다. Redis에는 조회를 돕는 메뉴 캐시와 참고용 순위 복사본을 둔다.

### 처음 보는 말 풀이

| 말 | 쉬운 뜻 | 코드에서 찾을 곳 |
| --- | --- | --- |
| API | 다른 프로그램이 정해진 주소로 요청할 수 있게 만든 입구 | `@GetMapping`, `@PostMapping` |
| HTTP | 요청과 응답을 주고받는 규칙. GET은 주로 조회, POST는 주로 새 작업이다 | `MenuController`, `OrderController` |
| JSON | `{"userId":1}`처럼 이름과 값을 적는 데이터 형식 | 요청·응답 DTO `record` |
| class | 데이터와 함수를 묶은 Java 설계도 | `OrderController` 등 |
| 메서드 | 클래스 안에 선언한 함수 | `place()`, `charge()` |
| 어노테이션 | `@`로 시작하는 표시. Spring에 “이 함수를 API로 사용해” 같은 지시를 준다 | `@RestController`, `@Transactional` |
| Bean | Spring이 생성하고 관리하는 Java 객체 | 생성자에 전달되는 `JdbcTemplate` 등 |
| SQL | DB에 “읽어라/넣어라/바꿔라”라고 명령하는 언어 | `SELECT`, `INSERT`, `UPDATE` |
| 트랜잭션 | 여러 DB 명령을 하나의 묶음으로 취급한다. 전부 성공하면 커밋, 실패하면 롤백한다 | `@Transactional` |
| 동시성 | 여러 요청이 거의 동시에 실행되는 상황 | 계정 행 `FOR UPDATE` |
| 메시지 | 다른 프로그램에 전달하는 사건 기록 | `OrderEvent` |
| 캐시 | 다시 계산하거나 DB를 읽지 않으려고 잠시 보관한 복사본 | Redis 메뉴 캐시 |

### 숫자로 이해하는 결제

아메리카노 가격이 **4,500P**, 충전한 잔액이 **10,000P**라고 하자.

```http
POST /api/orders
Content-Type: application/json
Idempotency-Key: my-first-order

{"userId":1,"menuId":1}
```

| 시점 | 잔액 | 주문 수 | 아직 Kafka 확인을 기다리는 outbox 수 |
| --- | ---: | ---: | ---: |
| 주문 전 | 10,000P | 0 | 0 |
| 첫 주문 직후, 발행 작업 전 | 5,500P | 1 | 1 |
| **같은 키로 재시도** | 5,500P | 1 | 1 |
| Kafka 확인 후 | 5,500P | 1 | 0 |

같은 키로 다시 보낸 요청은 “새 커피 한 잔”이 아니라 “아까 결제가 되었는지 다시 알려 줘”라는 뜻으로 처리한다. 이것이 **멱등성**이다. 새 커피를 주문하려면 새 키를 만들어야 한다. 발행 작업이 빠르게 실행되면 첫 HTTP 응답 시점에 이미 outbox가 비어 있을 수도 있다.

### 왜 DB 트랜잭션과 행 잠금이 둘 다 필요한가?

트랜잭션은 **한 주문 안에서** 잔액 차감·주문 생성·발행 기록이 함께 성공하도록 한다. 행 잠금은 **서로 다른 두 주문 사이에서** 같은 사용자 잔액을 동시에 함부로 바꾸지 못하게 한다. 역할이 다르다.

예를 들어 서버 A와 B가 같은 사용자의 주문을 동시에 받으면 A가 `point_accounts`의 사용자 행을 잠근다. B는 기다린다. A가 결제를 끝내고 잠금을 풀면 B는 바뀐 잔액과 이미 사용된 요청 키를 확인한다. Java의 `synchronized`는 서버 한 대의 메모리에서만 작동하지만 MySQL 행 잠금은 여러 서버가 공유한다.

### 왜 Kafka에 곧바로 보내지 않고 outbox를 쓰나?

DB에는 주문이 저장됐는데 Kafka에 보내기 직전 서버가 꺼질 수 있다. 주문과 **“보내야 할 메시지”**를 같은 DB 트랜잭션에 저장하면 다시 켰을 때 outbox에서 메시지를 찾아 보낼 수 있다. 반대로 Kafka에는 보냈지만 outbox를 지우기 전에 꺼지면 같은 메시지를 다시 보낼 수 있다. 그래서 받는 쪽은 `event_id`로 중복을 걸러낸다. 이를 **최소 한 번 전달(at least once)**이라고 부른다.

### Redis ZSET은 왜 정확한 인기 메뉴 API가 아닌가?

ZSET은 각 메뉴에 주문 횟수 점수를 붙인 Redis 자료형이다. 10초마다 MySQL 주문을 다시 세어 복사하므로 그 사이에는 숫자가 이전 값일 수 있다. 따라서 `GET /api/menus/popular`은 MySQL을 직접 조회한다. 또한 “최근 7일”은 시간이 지나면서 오래된 주문이 빠져야 하므로 새 주문마다 점수를 1씩 올리는 방식만으로는 정확하지 않다.

## 기능과 역할에 따라 패키지를 나눈 이유

**패키지(package)**는 관련 Java 파일을 넣는 폴더이자 이름이다. 먼저 `menu`, `point`, `order`, `analytics`라는 **기능**으로 나누고, 각 기능 안에서 역할에 따라 `controller`, `service`, `dto` 등으로 한 번 더 나눴다.

```text
com.example.coffee
├─ CoffeeOrderApplication.java        ← 프로그램 시작
├─ common/
│  ├─ dto/                           ← 공통 오류 응답 모양
│  └─ error/                         ← 공통 오류 처리
├─ config/
│  ├─ kafka/                         ← Kafka 토픽 설정
│  └─ redis/                         ← Redis 캐시 오류 처리
├─ menu/
│  ├─ controller/                    ← 메뉴 HTTP 주소
│  ├─ service/                       ← 메뉴 조회·정확한 7일 인기 집계
│  ├─ projection/                    ← Redis ZSET에 주문 횟수 기록
│  └─ dto/                           ← 메뉴 데이터를 담는 객체
├─ point/
│  ├─ controller/                    ← 충전 HTTP 주소
│  ├─ service/                       ← 잔액 충전 SQL과 트랜잭션
│  └─ dto/                           ← 충전 요청·응답
├─ order/
│  ├─ controller/                    ← 주문 HTTP 주소
│  ├─ service/                       ← 결제 규칙과 이벤트 발행 약속
│  ├─ outbox/                        ← DB 기록을 Kafka로 옮기는 작업
│  ├─ kafka/                         ← Kafka 전송 구현
│  └─ dto/                           ← 주문 요청·응답·이벤트
└─ analytics/
   ├─ consumer/                      ← Kafka 메시지 수신
   └─ service/                       ← DB 저장과 중복 처리
```

**Controller**는 요청을 받아 입력을 검사하고 HTTP 응답을 돌려준다. **Service**는 실제 일을 한다. 예를 들어 `OrderService`가 잔액을 빼고 주문을 DB에 저장한다. **DTO**는 값을 담아 다른 곳으로 전달한다. 예를 들어 `OrderRequest`는 `userId`와 `menuId`를 담는다. `config`는 프로그램을 켤 때 필요한 Kafka·Redis 설정이다.

컨트롤러에서 SQL을 없앤 이유는 HTTP 처리와 결제 규칙을 분리하기 위해서다. 결제 트랜잭션을 `OrderService`에 두면 여러 입력 방식이 생기더라도 같은 결제 규칙을 사용할 수 있다. `@Transactional`은 Spring이 관리하는 서비스 메서드에 붙여야 DB 작업 전체를 묶는다. 다른 패키지의 클래스가 필요할 때는 파일 위쪽의 `import`가 위치를 알려 준다.

`PopularMenuService`는 MySQL 주문 내역을 읽고 최근 7일 상위 3개를 정확히 계산하므로 `menu/service`에 둔다. `PopularMenuZsetProjection`은 주문 수를 Redis ZSET에 미리 기록하는 별도 작업이므로 `menu/projection`에 둔다. `consumer`, `outbox`, `kafka`는 메시지를 받아들이거나 내보내는 역할을 이름으로 드러낸 패키지다. `AnalyticsConsumer`는 Kafka 메시지를 받고, `AnalyticsService`는 수집 내역을 MySQL에 저장한다. 같은 메시지가 다시 와도 `event_id`가 중복 저장되지 않도록 DB가 검사한다.

패키지를 옮긴 뒤 Kafka 주문 이벤트 DTO는 `order.dto`가 되었다. 그래서 JSON 역직렬화가 신뢰하는 패키지도 바꿨다. Redis의 옛 메뉴 캐시 객체와 섞이지 않도록 캐시 이름은 `menus-v2`를 사용한다.

## 전체 코드: 설명은 해당 줄 바로 위에 있다

다음 코드 블록의 원본 줄은 파일과 같은 순서다. 설명 주석은 읽는 데 도움을 주려고 **복사본에만** 삽입했다. 각 파일의 역할을 이해하고 다음 파일로 넘어가자.


### 1. `settings.gradle`

```groovy
// 이 프로그램의 이름을 Gradle에 알려 줍니다. Gradle은 코드를 빌드하고 테스트하는 도구입니다.
rootProject.name = 'coffee-order'
```

### 2. `build.gradle`

```groovy
// 플러그인은 Gradle에 기능을 더합니다. Java 컴파일과 Spring Boot 실행 기능을 여기서 켭니다.
plugins {
    id 'java'
    // Spring Boot는 웹 서버를 쉽게 만드는 Java 도구입니다. 버전을 적어 같은 환경으로 빌드합니다.
    id 'org.springframework.boot' version '3.5.7'
    id 'io.spring.dependency-management' version '1.1.7'
}

group = 'com.example'
version = '0.0.1-SNAPSHOT'

java {
    // 이 프로젝트가 Java 17 문법을 사용한다는 뜻입니다.
    sourceCompatibility = JavaVersion.VERSION_17
}

repositories {
    // 필요한 라이브러리를 내려받을 공개 저장소입니다.
    mavenCentral()
}

dependencies {
    // HTTP 요청을 받고 JSON 응답을 보내는 기능입니다.
    implementation 'org.springframework.boot:spring-boot-starter-web'
    // Java 코드에서 SQL을 실행하는 기능입니다. 잔액을 바꾸는 SQL을 직접 쓰기 위해 선택했습니다.
    implementation 'org.springframework.boot:spring-boot-starter-jdbc'
    // 사용자 ID와 금액이 올바른지 @Min, @Max 등으로 검사하는 기능입니다.
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-cache'
    // Redis에 연결합니다. Redis는 빠른 조회용 저장소이고 결제의 최종 기록은 MySQL에 둡니다.
    implementation 'org.springframework.boot:spring-boot-starter-data-redis'
    // Kafka로 주문 완료 메시지를 보내고 받는 기능입니다.
    implementation 'org.springframework.kafka:spring-kafka'
    // Flyway는 SQL 파일을 번호 순서대로 실행해 DB 테이블을 만듭니다.
    implementation 'org.flywaydb:flyway-core'
    implementation 'org.flywaydb:flyway-mysql'
    // 실행할 때 MySQL에 접속하는 드라이버가 필요합니다.
    runtimeOnly 'com.mysql:mysql-connector-j'
    compileOnly 'org.projectlombok:lombok'
    // Lombok이 컴파일 중 builder() 코드를 자동 생성합니다. 코드에 직접 적지 않아도 호출할 수 있습니다.
    annotationProcessor 'org.projectlombok:lombok'

    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    // 자동 테스트에서는 설치가 간단한 메모리 DB인 H2를 사용합니다.
    testRuntimeOnly 'com.h2database:h2'
}

tasks.withType(JavaCompile).configureEach {
    // 더 최신 JDK로 빌드하더라도 Java 17에 없는 기능을 잘못 쓰지 않도록 막습니다.
    options.release = 17
}

tasks.named('test') {
    // JUnit은 자동 테스트를 실행하는 도구입니다. 이 설정이 JUnit 5 테스트를 켭니다.
    useJUnitPlatform()
}
```

### 3. `gradle/wrapper/gradle-wrapper.properties`

```properties
distributionBase=GRADLE_USER_HOME
distributionPath=wrapper/dists
# 팀원이 같은 Gradle 버전을 쓰게 합니다. 컴퓨터마다 빌드 결과가 달라질 가능성을 줄입니다.
distributionUrl=https\://services.gradle.org/distributions/gradle-8.14.3-bin.zip
networkTimeout=10000
validateDistributionUrl=true
zipStoreBase=GRADLE_USER_HOME
zipStorePath=wrapper/dists
```

### 4. `compose.yaml`

```yaml
# Docker Compose가 함께 실행할 프로그램들을 나열합니다. 여기서는 MySQL, Kafka, Redis입니다.
services:
  mysql:
    # MySQL 8.4를 사용합니다. MySQL에는 잔액, 주문, 발행 대기 메시지를 저장합니다.
    image: mysql:8.4
    environment:
      MYSQL_DATABASE: coffee
      MYSQL_USER: coffee
      # 로컬 연습용 비밀번호입니다. 실제 서비스에서는 코드에 비밀번호를 그대로 두면 안 됩니다.
      MYSQL_PASSWORD: coffee
      MYSQL_ROOT_PASSWORD: root
    ports:
      # 왼쪽 13306은 내 컴퓨터의 포트, 오른쪽 3306은 컨테이너 안의 포트입니다.
      - "13306:3306"
    volumes:
      # DB 파일을 Docker 볼륨에 보관합니다. 컨테이너를 다시 만들어도 실습 데이터가 남을 수 있습니다.
      - mysql_data:/var/lib/mysql
    healthcheck:
      test: ["CMD-SHELL", "mysqladmin ping -h localhost -u root -proot"]
      interval: 3s
      timeout: 3s
      retries: 10

  kafka:
    # Kafka는 주문 완료 사건을 다른 프로그램에 전달합니다. 이 파일은 브로커 한 대만 실행합니다.
    image: apache/kafka:4.1.2
    ports:
      - "9092:9092"

  redis:
    # Redis는 메뉴 캐시와 최근 주문 횟수 ZSET을 저장합니다. Redis 데이터가 없어져도 MySQL에서 다시 만들 수 있습니다.
    image: redis:7.4.11-alpine3.21
    ports:
      - "6379:6379"
    healthcheck:
      # healthcheck는 Redis가 명령에 응답할 준비가 됐는지 확인합니다.
      test: ["CMD", "redis-cli", "ping"]
      interval: 3s
      timeout: 3s
      retries: 10

volumes:
  mysql_data:
```

### 5. `src/main/resources/application.yml`

```yaml
# Spring Boot의 실행 설정입니다. 들여쓰기로 어떤 설정이 어느 기능에 속하는지 구분합니다.
spring:
  datasource:
    # ${DB_URL:기본값}은 환경변수 DB_URL이 있으면 그 값을 쓰고, 없으면 기본값을 쓴다는 뜻입니다.
    # 시간을 UTC로 맞춰 서버마다 주문 시각과 7일 계산이 달라지지 않게 합니다.
    url: ${DB_URL:jdbc:mysql://localhost:13306/coffee?connectionTimeZone=UTC&forceConnectionTimeZoneToSession=true}
    username: ${DB_USER:coffee}
    password: ${DB_PASSWORD:coffee}
  # 앱이 시작할 때 번호가 붙은 DB 변경 SQL을 실행합니다.
  flyway:
    enabled: true
  kafka:
    # Kafka가 실행 중인 주소입니다.
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      # 처음 소비하는 그룹이라 읽은 위치 기록이 없으면 가장 오래된 메시지부터 읽습니다.
      auto-offset-reset: earliest
      properties:
        # 주문 이벤트 DTO가 옮겨진 새 패키지에서 Kafka JSON 객체를 읽습니다.
        # JSON 메시지를 Java 객체로 바꿀 때 허용할 코드 패키지를 제한합니다.
        spring.json.trusted.packages: com.example.coffee.order.dto
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      # Kafka가 메시지를 받았다고 확인할 때까지 기다리도록 합니다. 브로커가 한 대이므로 복제 내구성은 별개입니다.
      acks: all
      properties:
        # Kafka 생산자 내부의 재시도 중복을 줄입니다. HTTP 결제 요청 중복 방지와는 다른 기능입니다.
        enable.idempotence: true
  data:
    redis:
      # Redis 주소도 환경변수로 바꿀 수 있습니다.
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      connect-timeout: 2s
      timeout: 2s
  cache:
    type: redis
    # 이전 메뉴 객체를 담은 Redis 캐시와 새 DTO 패키지의 캐시를 분리합니다.
    cache-names: menus-v2
    redis:
      # 캐시 값을 5분 뒤 만료시킵니다. 만료되면 다음 조회에서 MySQL을 다시 읽습니다.
      time-to-live: 5m

orders:
  # 주문 완료 메시지를 보낼 Kafka 토픽 이름입니다. 토픽은 메시지를 담는 통로입니다.
  topic: orders.paid

# outbox 발행과 Redis 집계를 실행할 예약 작업 스레드를 둘로 설정합니다.
spring.task.scheduling.pool.size: 2
```

### 6. `src/main/resources/db/migration/V1__init.sql`

```sql
-- 메뉴 표를 만듭니다. 한 행이 커피 메뉴 하나입니다.
CREATE TABLE menus (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    -- 가격을 정수 원 단위로 저장합니다. 소수점 반올림 오류를 피하기 쉽습니다.
    price BIGINT NOT NULL CHECK (price > 0)
);

-- 사용자 한 명의 현재 포인트 잔액을 저장합니다.
CREATE TABLE point_accounts (
    user_id BIGINT PRIMARY KEY,
    -- CHECK는 잔액이 음수가 되거나 정한 최대값을 넘지 못하게 DB에서도 검사합니다.
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0 AND balance <= 1000000000000)
);

-- 결제가 끝난 주문을 저장합니다. 메뉴 가격이 나중에 변해도 당시 결제액을 기억해야 합니다.
CREATE TABLE orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- REFERENCES는 해당 사용자 계정이 실제로 존재해야 한다는 외래키 규칙입니다.
    user_id BIGINT NOT NULL REFERENCES point_accounts(user_id),
    menu_id BIGINT NOT NULL REFERENCES menus(id),
    paid_amount BIGINT NOT NULL CHECK (paid_amount > 0),
    ordered_at TIMESTAMP(6) NOT NULL
);

-- 인덱스는 책의 색인처럼 최근 주문을 찾는 작업을 돕습니다. 대신 저장 공간과 쓰기 비용이 듭니다.
CREATE INDEX idx_orders_recent ON orders(ordered_at, menu_id);

-- Kafka에 아직 보내지 않았거나 확인받지 못한 주문 사건을 잠시 보관합니다.
CREATE TABLE order_outbox (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- 주문 하나에 발행 대기 기록이 중복 생성되지 않게 UNIQUE를 둡니다.
    order_id BIGINT NOT NULL UNIQUE REFERENCES orders(id),
    user_id BIGINT NOT NULL,
    menu_id BIGINT NOT NULL,
    paid_amount BIGINT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP(6) NOT NULL
);

-- 다음 전송 시간이 된 outbox 행을 빨리 찾기 위한 색인입니다.
CREATE INDEX idx_order_outbox_ready ON order_outbox(next_attempt_at, id);

-- 처음 실행할 때 사용할 커피 메뉴 네 개를 넣습니다.
INSERT INTO menus(name, price) VALUES
    ('Americano', 4500),
    ('Cafe Latte', 5000),
    ('Cappuccino', 5500),
    ('Cold Brew', 6000);
```

### 7. `src/main/resources/db/migration/V2__collected_order_events.sql`

```sql
-- Kafka 메시지를 받은 쪽을 흉내 낸 표입니다. 실제 분석 플랫폼의 역할을 실습용 DB로 표현했습니다.
CREATE TABLE collected_order_events (
    -- 같은 사건이 Kafka에서 두 번 오더라도 같은 event_id는 한 행만 저장합니다.
    event_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    menu_id BIGINT NOT NULL,
    paid_amount BIGINT NOT NULL,
    -- 메시지를 받은 시각입니다. 커피가 결제된 시각과는 다릅니다.
    collected_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
```

### 8. `src/main/resources/db/migration/V3__order_idempotency.sql`

```sql
-- 클라이언트가 붙인 결제 시도 이름을 저장합니다. 이미 있던 주문은 이 값이 없어서 NULL을 허용합니다.
ALTER TABLE orders ADD COLUMN request_key VARCHAR(128);
-- 같은 사용자와 같은 요청 키로 주문을 두 개 만들지 못하게 DB가 막습니다. 서버가 여러 대여도 이 규칙은 공유됩니다.
CREATE UNIQUE INDEX uq_orders_user_request_key ON orders(user_id, request_key);
```

### 9. `src/main/java/com/example/coffee/CoffeeOrderApplication.java`

```java
package com.example.coffee;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

// 이 클래스가 Spring Boot 프로그램의 시작점입니다. Spring이 주변의 Controller와 Service를 찾아 준비합니다.
@SpringBootApplication
// @Scheduled가 붙은 함수를 정해진 간격으로 실행하도록 켭니다.
@EnableScheduling
public class CoffeeOrderApplication {
    public static void main(String[] args) {
        // 웹 서버와 필요한 객체를 실제로 시작합니다.
        SpringApplication.run(CoffeeOrderApplication.class, args);
    }

    @Bean
    // 현재 시간을 주는 객체를 한 곳에서 만듭니다. 테스트에서는 시간을 고정한 가짜 시계로 바꿀 수 있습니다.
    Clock clock() {
        return Clock.systemUTC();
    }
}
```

### 10. `src/main/java/com/example/coffee/config/kafka/OrderTopicConfig.java`

```java
// 이 파일은 config 기능의 kafka 패키지에 속합니다.
package com.example.coffee.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

// Spring 설정을 담은 클래스입니다.
@Configuration
public class OrderTopicConfig {
    @Bean
    NewTopic orderPaidTopic(@Value("${orders.topic}") String topic) {
        // 하나의 Kafka 토픽을 세 갈래로 나눠 소비자 세 개가 동시에 처리할 수 있게 합니다.
        // 브로커가 한 대인 연습 환경이라 복제본은 하나입니다. 장애에 안전한 운영 구성을 뜻하지는 않습니다.
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }
}
```

### 11. `src/main/java/com/example/coffee/config/redis/MenuCacheConfig.java`

```java
// 이 파일은 config 기능의 redis 패키지에 속합니다.
package com.example.coffee.config.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.context.annotation.Configuration;

@Configuration
// @Cacheable 기능을 켭니다.
@EnableCaching
public class MenuCacheConfig implements CachingConfigurer {
    private static final Logger log = LoggerFactory.getLogger(MenuCacheConfig.class);

    @Override
    // Redis 캐시가 고장 났을 때 어떻게 할지 정합니다. 아래 함수들은 오류를 기록하고 요청을 계속 진행하게 합니다.
    public CacheErrorHandler errorHandler() {
        return new CacheErrorHandler() {
            @Override
            // 캐시 읽기가 실패해도 예외를 다시 던지지 않습니다. 메뉴 목록은 MySQL에서 읽을 수 있습니다.
            public void handleCacheGetError(RuntimeException exception, Cache cache, Object key) {
                log.warn("Menu cache read failed: {}", exception.toString());
            }

            @Override
            // MySQL에서 읽은 값을 Redis에 저장하지 못해도 사용자에게 메뉴 응답을 보냅니다.
            public void handleCachePutError(RuntimeException exception, Cache cache, Object key, Object value) {
                log.warn("Menu cache write failed: {}", exception.toString());
            }

            @Override
            public void handleCacheEvictError(RuntimeException exception, Cache cache, Object key) {
                log.warn("Menu cache eviction failed: {}", exception.toString());
            }

            @Override
            public void handleCacheClearError(RuntimeException exception, Cache cache) {
                log.warn("Menu cache clear failed: {}", exception.toString());
            }
        };
    }
}
```

### 12. `src/main/java/com/example/coffee/common/dto/ErrorResponse.java`

```java
// 이 파일은 common 기능의 dto 패키지에 속합니다.
package com.example.coffee.common.dto;

import lombok.Builder;

@Builder
// 오류 응답에 들어갈 코드와 설명을 담습니다. Controller의 오류 형식을 일정하게 만듭니다.
public record ErrorResponse(String code, String message) {}
```

### 13. `src/main/java/com/example/coffee/common/error/ApiException.java`

```java
// 이 파일은 common 기능의 error 패키지에 속합니다.
package com.example.coffee.common.error;

import org.springframework.http.HttpStatus;

// API 규칙 위반을 알려 주는 예외입니다. 이 예외가 결제 함수 밖으로 나가면 DB 변경을 취소할 수 있습니다.
public class ApiException extends RuntimeException {
    // 404나 409 같은 HTTP 상태를 기억합니다.
    private final HttpStatus status;
    // INSUFFICIENT_POINTS처럼 프로그램이 구별하기 쉬운 오류 이름을 기억합니다.
    private final String code;

    public ApiException(HttpStatus status, String code, String message) {
        // 부모 예외에 사람이 읽을 오류 설명을 전달합니다.
        super(message);
        this.status = status;
        this.code = code;
    }

    public HttpStatus status() {
        return status;
    }

    public String code() {
        return code;
    }
}
```

### 14. `src/main/java/com/example/coffee/common/error/ApiErrorHandler.java`

```java
// 이 파일은 common 기능의 error 패키지에 속합니다.
package com.example.coffee.common.error;

import com.example.coffee.common.dto.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

// 여러 API에서 나온 오류를 한곳에서 JSON 응답으로 바꿉니다.
@RestControllerAdvice
public class ApiErrorHandler {
    // 직접 만든 ApiException을 상태 코드와 오류 이름이 있는 응답으로 바꿉니다.
    @ExceptionHandler(ApiException.class)
    ResponseEntity<ErrorResponse> api(ApiException exception) {
        return ResponseEntity.status(exception.status())
                .body(ErrorResponse.builder().code(exception.code()).message(exception.getMessage()).build());
    }

    // @Valid가 금액 0처럼 잘못된 입력을 발견하면 400을 보냅니다.
    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ErrorResponse> invalid(MethodArgumentNotValidException exception) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.builder().code("INVALID_REQUEST")
                        .message("Request fields are invalid").build());
    }

    // Idempotency-Key 헤더가 빠지면 400 INVALID_REQUEST를 보냅니다.
    @ExceptionHandler(MissingRequestHeaderException.class)
    ResponseEntity<ErrorResponse> missingHeader(MissingRequestHeaderException exception) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.builder().code("INVALID_REQUEST")
                        .message("Required request header is missing").build());
    }
    // JSON 문법이 틀려서 요청을 읽지 못해도 400을 보냅니다.
    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ErrorResponse> unreadable(HttpMessageNotReadableException exception) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.builder().code("INVALID_REQUEST")
                        .message("Request body is invalid").build());
    }

}
```

### 15. `src/main/java/com/example/coffee/menu/dto/Menu.java`

```java
// 이 파일은 menu 기능의 dto 패키지에 속합니다.
package com.example.coffee.menu.dto;

import java.io.Serializable;
import lombok.Builder;

// 객체를 만들 때 Menu.builder().id(...).name(...).price(...).build()처럼 각 값의 이름을 적을 수 있습니다.
@Builder
// record는 데이터 묶음입니다. 메뉴 ID, 이름, 가격을 함께 다니게 합니다.
// 메뉴 객체를 Redis 캐시에 저장할 수 있도록 바이트 형태로 바꾸는 기능을 허용합니다.
public record Menu(long id, String name, long price) implements Serializable {}
```

### 16. `src/main/java/com/example/coffee/menu/dto/PopularMenu.java`

```java
// 이 파일은 menu 기능의 dto 패키지에 속합니다.
package com.example.coffee.menu.dto;

import lombok.Builder;

@Builder
// 메뉴 이름·가격과 최근 주문 횟수를 함께 담아 API 응답으로 보냅니다.
public record PopularMenu(long menuId, String name, long price, long orderCount) {}
```

### 17. `src/main/java/com/example/coffee/menu/controller/MenuController.java`

```java
// 이 파일은 menu 기능의 controller 패키지에 속합니다.
package com.example.coffee.menu.controller;

import com.example.coffee.menu.dto.Menu;
import com.example.coffee.menu.service.MenuService;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// HTTP 요청을 받는 클래스입니다. 반환한 Java 객체는 보통 JSON으로 바뀝니다.
@RestController
// 이 클래스의 주소 앞부분을 /api/menus로 정합니다.
@RequestMapping("/api/menus")
public class MenuController {
    // final은 이 참조를 생성자에서 받은 뒤 다른 객체로 바꾸지 않겠다는 뜻입니다.
    private final MenuService menus;

    // Spring이 MenuService를 만들어 생성자에 전달합니다. 이를 생성자 주입이라고 합니다.
    public MenuController(MenuService menus) {
        this.menus = menus;
    }

    // GET /api/menus 요청이 아래 list() 함수를 실행합니다.
    @GetMapping
    public ResponseEntity<List<Menu>> list() {
        // 조회 성공 상태 200과 메뉴 목록을 함께 반환합니다. 꺾쇠 안 List<Menu>는 응답 내용의 타입입니다.
        return ResponseEntity.ok(menus.list());
    }
}
```

### 18. `src/main/java/com/example/coffee/menu/controller/PopularMenuController.java`

```java
// 이 파일은 menu 기능의 controller 패키지에 속합니다.
package com.example.coffee.menu.controller;

import com.example.coffee.menu.dto.PopularMenu;
import com.example.coffee.menu.service.PopularMenuService;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
// GET /api/menus/popular로 최근 인기 메뉴를 조회합니다.
@RequestMapping("/api/menus/popular")
public class PopularMenuController {
    // 실제 DB 집계는 Service에 맡기고 Controller는 HTTP 요청과 응답만 처리합니다.
    private final PopularMenuService popularMenus;

    public PopularMenuController(PopularMenuService popularMenus) {
        this.popularMenus = popularMenus;
    }

    @GetMapping
    public ResponseEntity<List<PopularMenu>> list() {
        // Service에서 받은 결과를 HTTP 200과 함께 반환합니다.
        return ResponseEntity.ok(popularMenus.list());
    }
}
```

### 19. `src/main/java/com/example/coffee/menu/service/MenuService.java`

```java
// 이 파일은 menu 기능의 service 패키지에 속합니다.
package com.example.coffee.menu.service;

import com.example.coffee.menu.dto.Menu;
import java.util.List;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

// Controller가 사용할 실제 메뉴 조회 기능을 가진 Spring 객체라는 표시입니다.
@Service
public class MenuService {
    private final JdbcTemplate jdbc;

    public MenuService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // 같은 메뉴 목록을 이미 Redis에서 찾으면 아래 SQL을 다시 실행하지 않고 그 값을 돌려줍니다.
    @Cacheable(cacheNames = "menus-v2", key = "'all'")
    public List<Menu> list() {
        // SQL로 DB 행을 읽습니다. ?가 없는 고정 메뉴 조회이고, 각 행을 아래의 Menu 객체로 바꿉니다.
        return jdbc.query("SELECT id, name, price FROM menus ORDER BY id",
                // rs는 지금 읽은 DB 한 행입니다. 그 행의 id, name, price를 꺼내 Menu를 만듭니다.
                (rs, rowNum) -> Menu.builder().id(rs.getLong("id"))
                        .name(rs.getString("name")).price(rs.getLong("price")).build());
    }
}
```

### 20. `src/main/java/com/example/coffee/menu/service/PopularMenuService.java`

```java
// 이 파일은 menu 기능의 service 패키지에 속합니다.
package com.example.coffee.menu.service;

import com.example.coffee.menu.dto.PopularMenu;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class PopularMenuService {
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public PopularMenuService(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    // DB의 결제 완료 주문을 최근 7일 범위로 묶어 정확한 인기 메뉴를 계산합니다.
    public List<PopularMenu> list() {
        // 지금 시각을 한 번만 읽어 7일의 시작과 끝을 같은 기준으로 계산합니다.
        Instant now = clock.instant();
        // COUNT는 DB 행의 개수를 셉니다. 주문 한 행이 메뉴 주문 한 번입니다.
        return jdbc.query("SELECT m.id, m.name, m.price, COUNT(o.id) AS order_count " +
                        // 주문 표와 메뉴 표를 연결해 메뉴 이름과 가격도 함께 가져옵니다.
                        "FROM orders o JOIN menus m ON m.id = o.menu_id " +
                        "WHERE o.ordered_at >= ? AND o.ordered_at <= ? " +
                        // 같은 메뉴의 주문을 한 그룹으로 묶어 COUNT가 메뉴별 숫자가 되게 합니다.
                        "GROUP BY m.id, m.name, m.price " +
                        // 많이 주문된 순서로 정렬하고, 수가 같으면 메뉴 ID가 작은 것이 먼저 오게 합니다.
                        // 상위 세 메뉴만 반환합니다.
                        "ORDER BY order_count DESC, m.id ASC LIMIT 3",
                (rs, rowNum) -> PopularMenu.builder().menuId(rs.getLong("id"))
                        .name(rs.getString("name")).price(rs.getLong("price"))
                        .orderCount(rs.getLong("order_count")).build(),
                // 현재 시각에서 정확히 7일, 즉 168시간 전을 계산합니다.
                Timestamp.from(now.minus(7, ChronoUnit.DAYS)), Timestamp.from(now));
    }
}
```

### 21. `src/main/java/com/example/coffee/menu/projection/PopularMenuZsetProjection.java`

```java
// 이 파일은 menu 기능의 projection 패키지에 속합니다.
package com.example.coffee.menu.projection;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

// Spring이 이 객체를 만들어 예약 작업으로 실행할 수 있게 합니다.
@Component
// 테스트에서는 이 작업을 끌 수 있고, 실제 실행에서는 기본으로 켭니다.
@ConditionalOnProperty(name = "popularity.zset.enabled", havingValue = "true", matchIfMissing = true)
public class PopularMenuZsetProjection {
    private static final Logger log = LoggerFactory.getLogger(PopularMenuZsetProjection.class);
    // Redis에서 인기 메뉴 순위를 저장할 이름입니다.
    static final String KEY = "popular:7d:counts";

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final Clock clock;

    public PopularMenuZsetProjection(JdbcTemplate jdbc, StringRedisTemplate redis, Clock clock) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.clock = clock;
    }

    // 앞선 실행이 끝난 뒤 기본 10초를 기다리고 다시 집계합니다.
    @Scheduled(fixedDelayString = "${popularity.zset.refresh-ms:10000}")
    public void refresh() {
        Instant now = clock.instant();
        List<MenuCount> counts = jdbc.query(
                "SELECT menu_id, COUNT(*) AS order_count FROM orders " +
                        // MySQL에서 최근 7일 주문을 메뉴별로 다시 셉니다. 7일 지난 주문을 자동으로 빼기 위해 전체를 다시 계산합니다.
                        "WHERE ordered_at >= ? AND ordered_at <= ? GROUP BY menu_id",
                (rs, rowNum) -> new MenuCount(rs.getLong("menu_id"), rs.getLong("order_count")),
                Timestamp.from(now.minus(7, ChronoUnit.DAYS)), Timestamp.from(now));
        try {
            // 최근 주문이 없다면 Redis에 남은 오래된 순위를 삭제합니다.
            if (counts.isEmpty()) {
                redis.delete(KEY);
                return;
            }
            // 새 순위를 만드는 동안 쓸 임시 이름입니다. 여러 서버가 동시에 만들어도 이름이 겹치지 않게 합니다.
            String temporaryKey = KEY + ":building:" + UUID.randomUUID();
            for (MenuCount count : counts) {
                // ZSET은 점수를 가진 집합입니다. 메뉴 ID를 항목으로, 주문 횟수를 점수로 넣습니다.
                redis.opsForZSet().add(temporaryKey, Long.toString(count.menuId()), count.orderCount());
            }
            // 임시 키가 오래 남지 않도록 2분 뒤 만료되게 합니다.
            redis.expire(temporaryKey, Duration.ofMinutes(2));
            // 완성된 임시 순위를 공개 이름으로 한 번에 바꿉니다. 만드는 중인 순위가 보이지 않게 합니다.
            redis.rename(temporaryKey, KEY);
        // Redis가 고장 나면 경고만 남깁니다. 정확한 인기 메뉴 API는 MySQL을 읽으므로 이 실패에 영향을 받지 않습니다.
        } catch (RuntimeException exception) {
            log.warn("Popularity ZSET refresh failed: {}", exception.toString());
        }
    }

    private record MenuCount(long menuId, long orderCount) {}
}
```

### 22. `src/main/java/com/example/coffee/point/dto/ChargeRequest.java`

```java
// 이 파일은 point 기능의 dto 패키지에 속합니다.
package com.example.coffee.point.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;

@Builder
// 사용자가 보낸 사용자 ID와 충전액을 담습니다. @Min/@Max는 허용 범위를 검사합니다.
public record ChargeRequest(@Min(1) long userId, @Min(1) @Max(1000000000000L) long amount) {}
```

### 23. `src/main/java/com/example/coffee/point/dto/ChargeResponse.java`

```java
// 이 파일은 point 기능의 dto 패키지에 속합니다.
package com.example.coffee.point.dto;

import lombok.Builder;

@Builder
// 충전 후 사용자에게 돌려줄 ID와 새 잔액입니다.
// 응답에는 사용자 ID와 새 잔액만 담습니다. 충전 POST를 재시도하면 다시 충전될 수 있다는 점은 별도로 기억하세요.
public record ChargeResponse(long userId, long balance) {}
```

### 24. `src/main/java/com/example/coffee/point/controller/PointController.java`

```java
// 이 파일은 point 기능의 controller 패키지에 속합니다.
package com.example.coffee.point.controller;

import com.example.coffee.point.dto.ChargeRequest;
import com.example.coffee.point.dto.ChargeResponse;
import com.example.coffee.point.service.PointService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
// 이 클래스의 주소 앞부분은 /api/points입니다.
@RequestMapping("/api/points")
public class PointController {
    // 실제 충전은 Service가 수행합니다. Controller는 HTTP 입구 역할만 합니다.
    private final PointService points;

    public PointController(PointService points) {
        this.points = points;
    }

    // POST /api/points/charges 요청이 충전 함수를 실행합니다.
    @PostMapping("/charges")
    // JSON 본문을 ChargeRequest로 바꾸고 @Min, @Max 규칙을 검사합니다.
    public ResponseEntity<ChargeResponse> charge(@Valid @RequestBody ChargeRequest request) {
        // Service 결과를 HTTP 200 응답에 담습니다.
        return ResponseEntity.ok(points.charge(request));
    }
}
```

### 25. `src/main/java/com/example/coffee/point/service/PointService.java`

```java
// 이 파일은 point 기능의 service 패키지에 속합니다.
package com.example.coffee.point.service;

import com.example.coffee.common.error.ApiException;
import com.example.coffee.point.dto.ChargeRequest;
import com.example.coffee.point.dto.ChargeResponse;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PointService {
    private final JdbcTemplate jdbc;

    public PointService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // 계정 생성과 잔액 충전을 한 묶음으로 처리합니다. 실패하면 둘 다 취소됩니다.
    @Transactional
    public ChargeResponse charge(ChargeRequest request) {
        // 처음 충전하는 사용자면 잔액 0인 계정을 먼저 만듭니다.
        jdbc.update("INSERT INTO point_accounts(user_id, balance) VALUES (?, 0) " +
                        // 이미 계정이 있으면 새 계정을 만들지 않고 그대로 둡니다. 동시에 첫 충전이 들어올 때도 도움이 됩니다.
                        "ON DUPLICATE KEY UPDATE user_id = user_id",
                request.userId());
        // DB가 직접 잔액에 더합니다. 동시에 충전해도 한 요청의 값을 덮어쓰지 않게 합니다.
        int updated = jdbc.update("UPDATE point_accounts SET balance = balance + ? " +
                        // 더하기 전에 최대 잔액을 넘는지 검사합니다. ?에는 메서드의 금액 값이 안전하게 들어갑니다.
                        "WHERE user_id = ? AND balance <= 1000000000000 - ?",
                request.amount(), request.userId(), request.amount());
        // 조건에 맞는 행을 바꾸지 못했다면 한도를 넘는 충전입니다. 409 오류를 보냅니다.
        if (updated == 0) {
            throw new ApiException(HttpStatus.CONFLICT, "POINT_LIMIT_EXCEEDED", "Point balance limit exceeded");
        }
        // 충전 뒤 최종 잔액을 읽어 응답에 넣습니다.
        Long balance = jdbc.queryForObject("SELECT balance FROM point_accounts WHERE user_id = ?",
                Long.class, request.userId());
        return ChargeResponse.builder().userId(request.userId()).balance(balance).build();
    }
}
```

### 26. `src/main/java/com/example/coffee/order/dto/OrderRequest.java`

```java
// 이 파일은 order 기능의 dto 패키지에 속합니다.
package com.example.coffee.order.dto;

import jakarta.validation.constraints.Min;
import lombok.Builder;

@Builder
// 주문할 사람과 메뉴를 담습니다. 금액은 보내지 않고 서버가 메뉴 가격을 조회합니다.
public record OrderRequest(@Min(1) long userId, @Min(1) long menuId) {}
```

### 27. `src/main/java/com/example/coffee/order/dto/OrderResponse.java`

```java
// 이 파일은 order 기능의 dto 패키지에 속합니다.
package com.example.coffee.order.dto;

import java.time.Instant;
import lombok.Builder;

@Builder
// 주문 번호, 결제액, 주문 시각을 사용자에게 돌려줍니다.
public record OrderResponse(long orderId, long userId, long menuId, long paidAmount, Instant orderedAt) {}
```

### 28. `src/main/java/com/example/coffee/order/dto/OrderEvent.java`

```java
// 이 파일은 order 기능의 dto 패키지에 속합니다.
package com.example.coffee.order.dto;

import lombok.Builder;

@Builder
// Kafka로 보낼 사건입니다. eventId는 같은 메시지가 다시 왔는지 확인하는 번호입니다.
public record OrderEvent(long eventId, long orderId, long userId, long menuId, long paidAmount) {}
```

### 29. `src/main/java/com/example/coffee/order/controller/OrderController.java`

```java
// 이 파일은 order 기능의 controller 패키지에 속합니다.
package com.example.coffee.order.controller;

import com.example.coffee.order.dto.OrderRequest;
import com.example.coffee.order.dto.OrderResponse;
import com.example.coffee.order.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
// 주문 API의 공통 주소는 /api/orders입니다.
@RequestMapping("/api/orders")
public class OrderController {
    // 결제 SQL과 멱등성 규칙은 Service에 둡니다.
    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> place(@Valid @RequestBody OrderRequest request,
                                               // 같은 결제를 다시 확인할 때 같은 요청 키를 보냅니다.
                                               @RequestHeader("Idempotency-Key") String requestKey) {
        // 서비스가 만든 주문 결과를 HTTP 201로 반환합니다.
        return ResponseEntity.status(HttpStatus.CREATED).body(orders.place(request, requestKey));
    }
}
```

### 30. `src/main/java/com/example/coffee/order/service/OrderService.java`

```java
// 이 파일은 order 기능의 service 패키지에 속합니다.
package com.example.coffee.order.service;

import com.example.coffee.common.error.ApiException;
import com.example.coffee.order.dto.OrderRequest;
import com.example.coffee.order.dto.OrderResponse;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {
    // JdbcTemplate은 Java에서 SQL을 실행하게 해 주는 Spring 도구입니다.
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public OrderService(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    // 잔액 차감·주문 저장·outbox 저장이 함께 커밋되거나 함께 취소됩니다.
    @Transactional
    public OrderResponse place(OrderRequest request, String requestKey) {
        // 키가 공백뿐이거나 너무 길면 먼저 400 오류로 거절합니다.
        if (requestKey.isBlank() || requestKey.length() > 128) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST",
                    "Idempotency-Key must contain 1 to 128 characters");
        }
        // 가격은 사용자가 보내지 않고 서버가 DB에서 읽습니다. 사용자가 가격을 낮춰 보내는 일을 막습니다.
        Long price = jdbc.query("SELECT price FROM menus WHERE id = ?",
                rs -> rs.next() ? rs.getLong(1) : null, request.menuId());
        // 없는 메뉴 ID라면 결제하지 않고 404 오류를 돌려줍니다.
        if (price == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "MENU_NOT_FOUND", "Menu does not exist");
        }
        // 이 사용자의 잔액 행을 잠급니다. 다른 서버가 같은 사용자의 결제를 동시에 진행하면 여기서 기다립니다.
        Long balance = jdbc.query("SELECT balance FROM point_accounts WHERE user_id = ? FOR UPDATE",
                rs -> rs.next() ? rs.getLong(1) : null, request.userId());
        // 충전한 계정이 아직 없다면 결제할 포인트가 없으므로 409 오류를 냅니다.
        if (balance == null) {
            throw new ApiException(HttpStatus.CONFLICT, "INSUFFICIENT_POINTS", "Insufficient points");
        }
        OrderResponse previous = jdbc.query(
                // 다른 서버가 같은 키의 주문을 방금 끝냈다면 그 최신 결과를 다시 읽습니다.
                "SELECT id, menu_id, paid_amount, ordered_at FROM orders WHERE user_id = ? AND request_key = ? FOR UPDATE",
                rs -> rs.next() ? OrderResponse.builder()
                        .orderId(rs.getLong("id")).userId(request.userId())
                        .menuId(rs.getLong("menu_id")).paidAmount(rs.getLong("paid_amount"))
                        .orderedAt(rs.getTimestamp("ordered_at").toInstant()).build() : null,
                request.userId(), requestKey);
        // 예전 주문을 찾았다면 아래에서는 잔액을 다시 빼지 않습니다.
        if (previous != null) {
            // 같은 키로 다른 메뉴를 주문하는 것은 모순이므로 409 오류를 냅니다.
            if (previous.menuId() != request.menuId()) {
                throw new ApiException(HttpStatus.CONFLICT, "IDEMPOTENCY_KEY_REUSED",
                        "Idempotency-Key was already used for another menu");
            }
            // 같은 키로 재시도했다면 다시 결제하지 않고 원래 주문을 돌려줍니다.
            return previous;
        }
        // 현재 잔액에서 메뉴 가격을 DB 안에서 뺍니다.
        int debited = jdbc.update("UPDATE point_accounts SET balance = balance - ? " +
                        // 돈이 충분할 때만 빼는 조건을 같은 UPDATE 문장에 둡니다. 동시 요청이 잔액보다 많이 쓰는 것을 막습니다.
                        "WHERE user_id = ? AND balance >= ?", price, request.userId(), price);
        // 차감한 행이 없다면 잔액 부족입니다. 예외가 발생하고 이 트랜잭션은 취소됩니다.
        if (debited == 0) {
            throw new ApiException(HttpStatus.CONFLICT, "INSUFFICIENT_POINTS", "Insufficient points");
        }

        // 주문 시각을 한 번 정합니다. 주문 행과 outbox가 같은 기준 시각을 사용합니다.
        Instant now = clock.instant();
        // DB가 자동으로 만든 주문 번호를 받아올 빈 상자입니다.
        KeyHolder key = new GeneratedKeyHolder();
        jdbc.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    // 결제한 주문을 저장합니다. request_key도 함께 저장해야 다음 재시도를 알아볼 수 있습니다.
                    "INSERT INTO orders(user_id, menu_id, paid_amount, ordered_at, request_key) VALUES (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, request.userId());
            statement.setLong(2, request.menuId());
            // ? 자리에 DB에서 읽은 실제 가격을 넣습니다. 문자열을 직접 이어 붙이지 않는 방식입니다.
            statement.setLong(3, price);
            statement.setTimestamp(4, Timestamp.from(now));
            statement.setString(5, requestKey);
            return statement;
        }, key);
        // 방금 DB가 만든 주문 ID를 꺼냅니다.
        long orderId = key.getKey().longValue();
        // Kafka에 보낼 기록도 같은 트랜잭션에 남깁니다.
        jdbc.update("INSERT INTO order_outbox(order_id, user_id, menu_id, paid_amount, next_attempt_at) " +
                "VALUES (?, ?, ?, ?, ?)", orderId, request.userId(), request.menuId(), price, Timestamp.from(now));
        return OrderResponse.builder().orderId(orderId).userId(request.userId()).menuId(request.menuId())
                .paidAmount(price).orderedAt(now).build();
    }
}
```

### 31. `src/main/java/com/example/coffee/order/service/OrderEventSender.java`

```java
// 이 파일은 order 기능의 service 패키지에 속합니다.
package com.example.coffee.order.service;

import com.example.coffee.order.dto.OrderEvent;

// 인터페이스는 '이 기능을 제공하라'는 약속입니다. 실제 Kafka 구현과 테스트용 가짜 구현을 바꿔 끼울 수 있습니다.
public interface OrderEventSender {
    // 사건을 보내는 약속입니다. Kafka 구현과 테스트용 가짜 구현이 같은 모양을 따릅니다.
    void send(OrderEvent event);
}
```

### 32. `src/main/java/com/example/coffee/order/kafka/KafkaOrderEventSender.java`

```java
// 이 파일은 order 기능의 kafka 패키지에 속합니다.
package com.example.coffee.order.kafka;

import com.example.coffee.order.service.OrderEventSender;
import com.example.coffee.order.dto.OrderEvent;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
// 위의 발행 약속을 Kafka로 실제 수행하는 클래스입니다.
public class KafkaOrderEventSender implements OrderEventSender {
    // Spring Kafka가 제공하는 전송 도구입니다. 문자열 키와 주문 이벤트 값을 보냅니다.
    private final KafkaTemplate<String, OrderEvent> kafka;
    private final String topic;

    public KafkaOrderEventSender(KafkaTemplate<String, OrderEvent> kafka,
                                 @Value("${orders.topic}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    @Override
    public void send(OrderEvent event) {
        try {
            // 사용자 ID를 메시지 키로 씁니다. 같은 사용자의 메시지가 같은 파티션으로 가기 쉽게 합니다.
            // Kafka가 받았다고 확인할 때까지 최대 5초 기다립니다. 확인 전에는 outbox를 지우면 안 됩니다.
            kafka.send(topic, Long.toString(event.userId()), event).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            // 실행 중단 신호가 오면 그 신호를 잃지 않도록 다시 표시합니다.
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka publication interrupted", exception);
        // Kafka가 실패하거나 5초 동안 답이 없으면 오류를 던져 outbox가 나중에 재시도하게 합니다.
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Kafka publication failed", exception);
        }
    }
}
```

### 33. `src/main/java/com/example/coffee/order/outbox/OutboxPublisher.java`

```java
// 이 파일은 order 기능의 outbox 패키지에 속합니다.
package com.example.coffee.order.outbox;

import com.example.coffee.order.service.OrderEventSender;
import com.example.coffee.order.dto.OrderEvent;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

// DB에 저장된 발행 대기 사건을 Kafka로 보내는 일을 맡습니다.
@Service
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final JdbcTemplate jdbc;
    private final OrderEventSender sender;
    private final Clock clock;

    public OutboxPublisher(JdbcTemplate jdbc, OrderEventSender sender, Clock clock) {
        this.jdbc = jdbc;
        this.sender = sender;
        this.clock = clock;
    }

    // 대기 행을 잡고 삭제하거나 다음 시도를 기록할 때까지 한 DB 트랜잭션으로 처리합니다.
    @Transactional
    public boolean publishOne() {
        Instant now = clock.instant();
        List<OrderEvent> ready = jdbc.query(
                "SELECT id, order_id, user_id, menu_id, paid_amount FROM order_outbox " +
                        // 지금 보내도 되는 사건 가운데 가장 오래된 한 건을 찾습니다.
                        // 다른 서버가 이미 잡은 행은 건너뜁니다. 같은 사건을 두 게시기가 동시에 집지 않게 합니다.
                        "WHERE next_attempt_at <= ? ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",
                (rs, rowNum) -> OrderEvent.builder()
                        .eventId(rs.getLong("id")).orderId(rs.getLong("order_id"))
                        .userId(rs.getLong("user_id")).menuId(rs.getLong("menu_id"))
                        .paidAmount(rs.getLong("paid_amount")).build(),
                Timestamp.from(now));
        // 지금 보낼 사건이 없으면 false를 반환해 반복 작업을 멈춥니다.
        if (ready.isEmpty()) {
            return false;
        }

        OrderEvent event = ready.get(0);
        try {
            // Kafka에 보내고 확인을 기다립니다. 이 동안 DB 행 잠금을 쥐고 있어 오래 걸리면 성능 비용이 있습니다.
            sender.send(event);
            // Kafka 확인을 받았으므로 발행 대기 행을 지웁니다.
            jdbc.update("DELETE FROM order_outbox WHERE id = ?", event.eventId());
        } catch (RuntimeException exception) {
            // 전송이 실패하면 시도 횟수를 올리고 5초 뒤 다시 시도하도록 남겨 둡니다.
            jdbc.update("UPDATE order_outbox SET attempts = attempts + 1, next_attempt_at = ? WHERE id = ?",
                    Timestamp.from(now.plus(Duration.ofSeconds(5))), event.eventId());
            log.warn("Could not publish order event {}: {}", event.eventId(), exception.toString());
        }
        // 사건 한 건을 처리했다는 뜻입니다. Kafka 전송에 성공했다는 뜻으로만 해석하면 안 됩니다.
        return true;
    }
}
```

### 34. `src/main/java/com/example/coffee/order/outbox/OutboxScheduler.java`

```java
// 이 파일은 order 기능의 outbox 패키지에 속합니다.
package com.example.coffee.order.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
// 테스트에서는 자동 발행을 끄고 필요한 순간 직접 publishOne()을 호출할 수 있습니다.
@ConditionalOnProperty(name = "analytics.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxScheduler {
    private final OutboxPublisher publisher;

    public OutboxScheduler(OutboxPublisher publisher) {
        this.publisher = publisher;
    }

    // 기본 1초 간격으로 outbox에 보낼 사건이 있는지 확인합니다.
    @Scheduled(fixedDelayString = "${analytics.publisher.delay-ms:1000}")
    public void publish() {
        // 보낼 사건이 여러 개면 한 번 깨어난 김에 준비된 것들을 이어서 처리합니다.
        while (publisher.publishOne()) {
            // Drain the ready queue without waiting for another scheduled run.
        }
    }
}
```

### 35. `src/main/java/com/example/coffee/analytics/consumer/AnalyticsConsumer.java`

```java
// 이 파일은 analytics 기능의 consumer 패키지에 속합니다.
package com.example.coffee.analytics.consumer;

import com.example.coffee.analytics.service.AnalyticsService;
import com.example.coffee.order.dto.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsConsumer {
    // Kafka에서 받은 메시지의 저장 작업은 서비스에 맡깁니다.
    private final AnalyticsService analytics;

    public AnalyticsConsumer(AnalyticsService analytics) {
        this.analytics = analytics;
    }

    // Kafka의 orders.paid 메시지가 오면 이 함수를 실행합니다. concurrency=3은 소비자 세 개를 뜻합니다.
    @KafkaListener(topics = "${orders.topic}", groupId = "coffee-analytics", concurrency = "3")
    public void collect(OrderEvent event) {
        // 서비스가 DB에 저장한 뒤 돌아오므로, 저장 실패가 Kafka 처리 실패로 이어집니다.
        analytics.collect(event);
    }
}
```

### 36. `src/main/java/com/example/coffee/analytics/service/AnalyticsService.java`

```java
// 이 파일은 analytics 기능의 service 패키지에 속합니다.
package com.example.coffee.analytics.service;

import com.example.coffee.order.dto.OrderEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AnalyticsService {
    private final JdbcTemplate jdbc;

    public AnalyticsService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // 메시지 한 건의 저장을 DB 트랜잭션으로 처리합니다. 실패하면 저장 결과를 취소합니다.
    @Transactional
    public void collect(OrderEvent event) {
        // 사용자·메뉴·결제액을 실습용 수집 테이블에 보관합니다.
        jdbc.update("INSERT INTO collected_order_events(event_id, order_id, user_id, menu_id, paid_amount) " +
                        // 이미 저장한 event_id가 다시 와도 행을 추가하지 않습니다.
                        "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event_id = event_id",
                event.eventId(), event.orderId(), event.userId(), event.menuId(), event.paidAmount());
    }
}
```

### 37. `src/test/resources/application.yml`

```yaml
spring:
  datasource:
    # 자동 테스트는 빠른 메모리 DB인 H2를 사용하되 MySQL과 비슷한 SQL 문법으로 실행합니다.
    url: jdbc:h2:mem:coffee;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1
    username: sa
    password:
  flyway:
    enabled: true
  kafka:
    admin:
      # 테스트 때 실제 Kafka 토픽을 만들지 않습니다.
      auto-create: false
    listener:
      # 테스트 때 실제 Kafka 소비자를 시작하지 않습니다.
      auto-startup: false
  cache:
    # 대부분의 테스트는 Redis 연결 없이 DB 기능을 검증합니다.
    type: none

analytics:
  # 자동 outbox 발행을 꺼 두고 테스트가 원하는 시점에 직접 실행합니다.
  publisher:
    enabled: false

orders:
  topic: orders.paid

popularity:
  # Redis ZSET 자동 갱신을 꺼 테스트 시각과 외부 Redis 상태의 영향을 줄입니다.
  zset:
    enabled: false
```

### 38. `src/test/java/com/example/coffee/CoffeeOrderIntegrationTest.java`

```java
package com.example.coffee;

import com.example.coffee.analytics.consumer.AnalyticsConsumer;
import com.example.coffee.common.error.ApiException;
import com.example.coffee.menu.controller.PopularMenuController;
import com.example.coffee.menu.dto.PopularMenu;
import com.example.coffee.menu.projection.PopularMenuZsetProjection;
import com.example.coffee.order.controller.OrderController;
import com.example.coffee.order.dto.OrderEvent;
import com.example.coffee.order.dto.OrderRequest;
import com.example.coffee.order.dto.OrderResponse;
import com.example.coffee.order.service.OrderEventSender;
import com.example.coffee.order.outbox.OutboxPublisher;
import com.example.coffee.point.controller.PointController;
import com.example.coffee.point.dto.ChargeRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

// 실제 Spring 객체들을 함께 띄워 API·DB 흐름을 검사합니다. 이것을 통합 테스트라고 합니다.
@SpringBootTest
// 실제 네트워크 포트를 열지 않고 HTTP 요청처럼 테스트할 도구를 만듭니다.
@AutoConfigureMockMvc
class CoffeeOrderIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-09-28T00:00:00Z");

    @Autowired JdbcTemplate jdbc;
    @Autowired PointController points;
    @Autowired OrderController orders;
    @Autowired PopularMenuController popular;
    @Autowired OutboxPublisher publisher;
    @Autowired AnalyticsConsumer analytics;
    @Autowired MockMvc mvc;
    @MockitoBean OrderEventSender sender;

    @TestConfiguration
    static class FixedClockConfig {
        @Bean
        @Primary
        Clock testClock() {
            // 시간을 2026-09-28로 고정합니다. 그래야 7일 전 경계 테스트가 언제 돌려도 같습니다.
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }
    }

    @BeforeEach
    // 테스트마다 이전 주문과 잔액을 지워 서로 영향을 주지 않게 합니다.
    void clearData() {
        jdbc.update("DELETE FROM collected_order_events");
        jdbc.update("DELETE FROM order_outbox");
        jdbc.update("DELETE FROM orders");
        jdbc.update("DELETE FROM point_accounts");
    }

    @Test
    // 충전→주문→잔액→인기 메뉴→발행 대기 기록이 서로 맞는지 한 흐름으로 확인합니다.
    void chargeOrderAndPublishAreConsistent() {
        assertThat(points.charge(ChargeRequest.builder()
                .userId(1).amount(5000).build()).getBody().balance()).isEqualTo(5000);

        OrderResponse order = orders.place(OrderRequest.builder()
                .userId(1).menuId(1).build(), "charge-order-1").getBody();
        assertThat(order.paidAmount()).isEqualTo(4500);
        assertThat(balance(1)).isEqualTo(500);
        assertThat(popular.list().getBody()).extracting(PopularMenu::orderCount)
                .containsExactly(1L);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox", Long.class)).isEqualTo(1);

        long eventId = jdbc.queryForObject("SELECT id FROM order_outbox", Long.class);
        assertThat(publisher.publishOne()).isTrue();
        verify(sender).send(new OrderEvent(eventId, order.orderId(), 1, 1, 4500));
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox", Long.class)).isZero();
    }

    @Test
    // GET 메뉴 API가 처음 넣은 메뉴 네 개를 반환하는지 확인합니다.
    void menuApiReturnsSeededMenus() throws Exception {
        mvc.perform(get("/api/menus"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(4))
                .andExpect(jsonPath("$[0].name").value("Americano"));
    }

    @Test
    // 20개 주문을 동시에 보내도 45,000P로는 4,500P 메뉴를 정확히 10개만 살 수 있는지 확인합니다.
    void concurrentOrdersCannotOverspend() throws Exception {
        points.charge(ChargeRequest.builder().userId(2).amount(45000).build());
        // 각 주문에 서로 다른 키를 만들기 위해 여러 스레드가 안전하게 증가시키는 숫자입니다.
        AtomicInteger requestNumber = new AtomicInteger();
        int succeeded = runConcurrently(20, () -> {
            try {
                orders.place(OrderRequest.builder().userId(2).menuId(1).build(),
                        "concurrent-order-" + requestNumber.incrementAndGet());
                return true;
            } catch (ApiException exception) {
                assertThat(exception.code()).isEqualTo("INSUFFICIENT_POINTS");
                return false;
            }
        });
        assertThat(succeeded).isEqualTo(10);
        assertThat(balance(2)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = 2", Long.class))
                .isEqualTo(10);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox WHERE user_id = 2", Long.class))
                .isEqualTo(10);
    }

    @Test
    // 20개 스레드가 100P씩 충전하면 2,000P가 되어야 합니다. 동시 충전 누락을 검사합니다.
    void concurrentChargesDoNotLoseUpdates() throws Exception {
        int succeeded = runConcurrently(20, () -> {
            points.charge(ChargeRequest.builder().userId(3).amount(100).build());
            return true;
        });
        assertThat(succeeded).isEqualTo(20);
        assertThat(balance(3)).isEqualTo(2000);
    }

    @Test
    // 정확히 7일 전 주문은 포함하고 그보다 1초 오래된 주문은 빼는지 확인합니다.
    void popularMenusUseRollingSevenDaysAndDeterministicTies() {
        points.charge(ChargeRequest.builder().userId(4).amount(100).build());
        addOrder(4, 1, NOW.minus(Duration.ofDays(7)));
        addOrder(4, 1, NOW);
        addOrder(4, 2, NOW.minus(Duration.ofDays(1)));
        addOrder(4, 2, NOW.minus(Duration.ofDays(2)));
        addOrder(4, 3, NOW.minus(Duration.ofDays(3)));
        addOrder(4, 4, NOW.minus(Duration.ofDays(7)).minusSeconds(1));

        List<PopularMenu> result = popular.list().getBody();
        assertThat(result).extracting(PopularMenu::menuId)
                .containsExactly(1L, 2L, 3L);
        assertThat(result).extracting(PopularMenu::orderCount)
                .containsExactly(2L, 2L, 1L);
    }

    @Test
    // 첫 Kafka 전송을 실패시킨 뒤 outbox가 남고 다음 시도에서 없어지는지 확인합니다.
    void failedDeliveryStaysInOutboxForRetry() {
        points.charge(ChargeRequest.builder().userId(5).amount(4500).build());
        orders.place(OrderRequest.builder().userId(5).menuId(1).build(), "delivery-retry");
        doThrow(new IllegalStateException("platform unavailable")).doNothing().when(sender).send(any());

        assertThat(publisher.publishOne()).isTrue();
        assertThat(jdbc.queryForObject("SELECT attempts FROM order_outbox", Integer.class)).isEqualTo(1);
        assertThat(publisher.publishOne()).isFalse();
        jdbc.update("UPDATE order_outbox SET next_attempt_at = ?", Timestamp.from(NOW));
        assertThat(publisher.publishOne()).isTrue();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox", Long.class)).isZero();
    }

    @Test
    // 게시기 둘이 동시에 같은 메시지를 집지 못하는지 검사합니다.
    void concurrentPublishersDoNotClaimTheSameEvent() throws Exception {
        points.charge(ChargeRequest.builder().userId(6).amount(4500).build());
        orders.place(OrderRequest.builder().userId(6).menuId(1).build(), "publish-concurrent");
        // 첫 게시기를 잠시 멈춰 두어 두 번째 게시기와 정말 겹치게 만드는 테스트 도구입니다.
        CountDownLatch sending = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            sending.countDown();
            release.await(5, TimeUnit.SECONDS);
            return null;
        }).when(sender).send(any());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = pool.submit(publisher::publishOne);
            assertThat(sending.await(5, TimeUnit.SECONDS)).isTrue();
            Future<Boolean> second = pool.submit(publisher::publishOne);
            assertThat(second.get(5, TimeUnit.SECONDS)).isFalse();
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
            verify(sender, times(1)).send(any());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    // 잘못된 금액, 잔액 부족, 없는 메뉴가 정한 HTTP 오류를 내는지 확인합니다.
    void invalidRequestsAndInsufficientPointsReturnDefinedErrors() throws Exception {
        mvc.perform(post("/api/points/charges").contentType("application/json")
                        .content("{\"userId\":1,\"amount\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mvc.perform(post("/api/orders").header("Idempotency-Key", "invalid-request-test")
                        .contentType("application/json")
                        .content("{\"userId\":99,\"menuId\":1}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("INSUFFICIENT_POINTS"));
        mvc.perform(post("/api/orders").header("Idempotency-Key", "invalid-request-test")
                        .contentType("application/json")
                        .content("{\"userId\":99,\"menuId\":999}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("MENU_NOT_FOUND"));
    }

    @Test
    // 같은 키로 다시 주문해도 주문 ID가 같고 한 번만 차감되는지 확인합니다.
    void repeatedOrderRequestReturnsOriginalWithoutSecondDebitOrEvent() {
        points.charge(ChargeRequest.builder().userId(8).amount(4500).build());
        OrderRequest request = OrderRequest.builder()
                .userId(8).menuId(1).build();

        OrderResponse first = orders.place(request, "retry-8").getBody();
        OrderResponse replay = orders.place(request, "retry-8").getBody();

        assertThat(replay).isEqualTo(first);
        assertThat(balance(8)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = 8", Long.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox WHERE user_id = 8", Long.class))
                .isEqualTo(1);
    }

    @Test
    // 같은 키를 10개 스레드가 동시에 보내도 주문이 한 건인지 확인합니다.
    void concurrentRetriesCreateOnlyOneOrder() throws Exception {
        points.charge(ChargeRequest.builder().userId(9).amount(4500).build());
        OrderRequest request = OrderRequest.builder()
                .userId(9).menuId(1).build();
        ConcurrentLinkedQueue<Long> orderIds = new ConcurrentLinkedQueue<>();

        assertThat(runConcurrently(10, () -> {
            orderIds.add(orders.place(request, "same-key-9").getBody().orderId());
            return true;
        })).isEqualTo(10);
        assertThat(orderIds).hasSize(10).containsOnly(orderIds.peek());
        assertThat(balance(9)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM orders WHERE user_id = 9", Long.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM order_outbox WHERE user_id = 9", Long.class))
                .isEqualTo(1);
    }

    @Test
    // 키를 다른 메뉴에 다시 쓰면 409를 내고 추가 차감을 하지 않는지 확인합니다.
    void reusedKeyForDifferentMenuConflicts() throws Exception {
        points.charge(ChargeRequest.builder().userId(10).amount(10000).build());
        orders.place(OrderRequest.builder().userId(10).menuId(1).build(), "menu-choice");

        mvc.perform(post("/api/orders").header("Idempotency-Key", "menu-choice")
                        .contentType("application/json")
                        .content("{\"userId\":10,\"menuId\":2}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_REUSED"));
        assertThat(balance(10)).isEqualTo(5500);
    }

    @Test
    // 키가 없거나 공백이거나 너무 길 때 400을 내는지 확인합니다.
    void missingIdempotencyKeyReturnsBadRequest() throws Exception {
        mvc.perform(post("/api/orders").contentType("application/json")
                        .content("{\"userId\":11,\"menuId\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mvc.perform(post("/api/orders").header("Idempotency-Key", " ")
                        .contentType("application/json")
                        .content("{\"userId\":11,\"menuId\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mvc.perform(post("/api/orders").header("Idempotency-Key", "x".repeat(129))
                        .contentType("application/json")
                        .content("{\"userId\":11,\"menuId\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }
    private long balance(long userId) {
        return jdbc.queryForObject("SELECT balance FROM point_accounts WHERE user_id = ?", Long.class, userId);
    }

    private void addOrder(long userId, long menuId, Instant when) {
        jdbc.update("INSERT INTO orders(user_id, menu_id, paid_amount, ordered_at) VALUES (?, ?, ?, ?)",
                userId, menuId, 100, Timestamp.from(when));
    }

    // 여러 작업을 동시에 시작시키는 공통 함수입니다. 그래야 경쟁 상황이 생길 가능성이 높아집니다.
    private int runConcurrently(int count, Callable<Boolean> action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(count);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return action.call();
                }));
            }
            start.countDown();
            int succeeded = 0;
            for (Future<Boolean> future : futures) {
                if (future.get()) {
                    succeeded++;
                }
            }
            return succeeded;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    // 같은 Kafka 사건을 두 번 넣어도 수집 표에는 한 행만 남는지 확인합니다.
    void analyticsConsumerDeduplicatesKafkaRedelivery() {
        OrderEvent event = OrderEvent.builder()
                .eventId(77).orderId(88).userId(99).menuId(1).paidAmount(4500).build();

        analytics.collect(event);
        analytics.collect(event);

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM collected_order_events WHERE event_id = 77",
                Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT paid_amount FROM collected_order_events WHERE event_id = 77",
                Long.class)).isEqualTo(4500);
    }

    @Test
    @SuppressWarnings("unchecked")
    // Redis를 가짜 객체로 바꿔 정확한 점수를 ZSET에 쓰는지 확인합니다.
    void zsetProjectionUsesRollingSevenDayCounts() {
        points.charge(ChargeRequest.builder().userId(7).amount(100).build());
        addOrder(7, 1, NOW);
        addOrder(7, 1, NOW.minus(Duration.ofDays(7)));
        addOrder(7, 2, NOW.minus(Duration.ofDays(7)).minusSeconds(1));
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ZSetOperations<String, String> zset = mock(ZSetOperations.class);
        when(redis.opsForZSet()).thenReturn(zset);

        new PopularMenuZsetProjection(jdbc, redis, Clock.fixed(NOW, ZoneOffset.UTC)).refresh();

        verify(zset).add(anyString(), eq("1"), eq(2.0));
        verify(redis).rename(anyString(), eq("popular:7d:counts"));
    }
}
```

### 39. `src/test/java/com/example/coffee/order/kafka/KafkaOrderEventSenderTest.java`

```java
package com.example.coffee.order.kafka;

import com.example.coffee.order.service.OrderEventSender;
import com.example.coffee.order.dto.OrderEvent;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaOrderEventSenderTest {
    @SuppressWarnings("unchecked")
    // 진짜 Kafka 서버 대신 호출 내용을 검사할 가짜 전송 도구를 만듭니다.
    private final KafkaTemplate<String, OrderEvent> kafka = mock(KafkaTemplate.class);
    private final KafkaOrderEventSender sender = new KafkaOrderEventSender(kafka, "orders.paid");
    private final OrderEvent event = OrderEvent.builder()
            .eventId(7).orderId(8).userId(9).menuId(10).paidAmount(4500).build();

    @Test
    void publishesUserMenuAndAmountAfterBrokerAcknowledgement() {
        // Kafka가 전송 성공을 바로 알려 준 상황을 만듭니다.
        when(kafka.send("orders.paid", "9", event)).thenReturn(CompletableFuture.completedFuture(null));

        sender.send(event);

        verify(kafka).send("orders.paid", "9", event);
    }

    @Test
    void failedAcknowledgementKeepsOutboxEligibleForRetry() {
        when(kafka.send("orders.paid", "9", event))
                // Kafka가 실패한 상황을 만듭니다. 전송 함수가 오류를 내야 outbox가 재시도할 수 있습니다.
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        assertThatThrownBy(() -> sender.send(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Kafka publication failed");
    }
}
```

### 40. `src/test/java/com/example/coffee/menu/service/MenuCacheFailureTest.java`

```java
package com.example.coffee.menu.service;

import com.example.coffee.order.service.OrderEventSender;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

// 캐시 기능을 포함한 Spring 실행 상태에서 메뉴 조회를 검사합니다.
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:coffee_cache;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "spring.cache.type=redis",
        "spring.data.redis.host=127.0.0.1",
        // 열려 있지 않은 포트를 지정해 Redis 장애를 흉내 냅니다.
        "spring.data.redis.port=63999"
})
class MenuCacheFailureTest {
    @Autowired MenuService menus;
    @MockitoBean OrderEventSender sender;

    @Test
    void menusRemainAvailableWhenRedisIsDown() {
        // Redis가 죽어도 MySQL에서 메뉴 네 개를 읽을 수 있어야 합니다.
        assertThat(menus.list()).hasSize(4);
    }
}
```

### 41. `.gitignore`

```text
# 빌드 도구가 만든 임시 파일은 Git에 올리지 않습니다.
.gradle/
.gradle-user/
# 컴파일 결과는 다시 만들 수 있으므로 소스와 함께 관리하지 않습니다.
build/
out/
.idea/
*.iml
```

## 코드 리뷰를 마치며: 현재 설계의 경계

- 이 과제에는 로그인·인증이 없다. 사용자가 보낸 `userId`가 정말 그 사용자인지는 확인하지 않는다. 실제 결제 서비스라면 인증이 필요하다.
- **주문은** 키로 중복 결제를 막지만 **포인트 충전은** 같은 요청을 다시 보내면 또 충전될 수 있다.
- MySQL의 문자열 비교 규칙에 따라 요청 키 `A`와 `a`가 같은 값처럼 비교될 수 있다. 키 형식이나 DB 비교 규칙을 운영 전에 정해야 한다.
- Kafka는 로컬에서 브로커 한 대, 토픽 파티션 세 개다. 소비자 세 개가 병렬로 일할 수 있다는 뜻이지 브로커 장애에 안전하다는 뜻은 아니다.
- 자동 테스트의 H2는 MySQL과 완전히 같지 않다. 특히 잠금과 동시성은 실제 MySQL에서 확인해야 한다. 이 프로젝트는 두 앱 인스턴스에 같은 키로 20건을 보내 같은 주문 한 건이 되는 것을 따로 확인했다.
- Redis ZSET은 갱신이 늦을 수 있다. 정확한 인기 메뉴 API는 MySQL을 사용한다.

## 이해 확인 문제

1. `@Transactional`이 없다면 잔액만 빠지고 주문 저장이 실패했을 때 무엇이 남을까?
2. 10,000P로 4,500P 메뉴를 **같은 키**로 두 번 주문하면 잔액은 얼마이고 주문은 몇 건일까?
3. 같은 Kafka 사건이 두 번 전달되면 왜 수집 표에는 한 행만 남을까?
4. Redis ZSET 점수가 3인데 MySQL의 최근 주문이 4건일 수 있는 이유는 무엇일까?

**확인:** 1) 부분 결제 위험, 2) 5,500P와 1건, 3) `event_id` 기본키와 중복 처리 SQL, 4) ZSET의 주기적 갱신 지연.

## 원본 코드 확인용 SHA-256

해시는 **주석을 붙이기 전 원본 파일**의 값이다. 이 문서를 만든 뒤 원본 코드를 바꾸면 주석이 설명하는 줄도 다시 확인해야 한다.

| 파일 | SHA-256 |
| --- | --- |
| `settings.gradle` | `cbf9a94537bbcb0a97d9dc746b8b49c814d3c1c48c292a9f693e2dab347ec7b1` |
| `build.gradle` | `298db43900a8781e3be7514f5a6723a5d77296f4d32ad361d11a6b701d2edb45` |
| `gradle/wrapper/gradle-wrapper.properties` | `7e0821d895908883350587c74476b717016ab416320c44dc82a10def916c6fb5` |
| `compose.yaml` | `7bf30651e83a5bd40ada052be40ad977ae02b1ba337522b12cb0833aa519c349` |
| `src/main/resources/application.yml` | `606a0a353cfe0d1d2352bbc6d4db5b0f8ef61a5c2047b65472b379adeb577c84` |
| `src/main/resources/db/migration/V1__init.sql` | `8f3e9af804cecfa591aadba1defd8a99945d2043bb311ceb04be74b883521d3b` |
| `src/main/resources/db/migration/V2__collected_order_events.sql` | `1c0bc2c5ce5d1316c9bc79805fa5ed479e29b65d009cd84d75acdd6d56f5ad07` |
| `src/main/resources/db/migration/V3__order_idempotency.sql` | `1c7d5348ff90735fc91a6bb1b287fdc20bf43c1209205cb6fd63a4a3a5aad3a8` |
| `src/main/java/com/example/coffee/CoffeeOrderApplication.java` | `5f19247eeec35f946c6107b1266ff2c08a93c5c0466ae9d51205d34af50088f3` |
| `src/main/java/com/example/coffee/config/kafka/OrderTopicConfig.java` | `6693d7bb3f8b6f2a5f28907fe9cab7fa48be665824aa8061760700d289dd1e47` |
| `src/main/java/com/example/coffee/config/redis/MenuCacheConfig.java` | `e8f910cff75f2705396f8017e62ae6ec919613b349cf30695481459fc3b34cee` |
| `src/main/java/com/example/coffee/common/dto/ErrorResponse.java` | `5181079aa18c1103ec43a34010f5e3f6a2775985cbeda13d9645fec87cd7cdbf` |
| `src/main/java/com/example/coffee/common/error/ApiException.java` | `aa2014d956bfbb4b38c55ece23a18b82c6479e4b7483bc32b4c62f0c02131934` |
| `src/main/java/com/example/coffee/common/error/ApiErrorHandler.java` | `4dc5f4df3cf8f8d266038628013849b8158878c5f2824739a7030bdab071343f` |
| `src/main/java/com/example/coffee/menu/dto/Menu.java` | `5754ba59bad1d027748b92108965390fdddbdcad445a72af9393b1db688a05f2` |
| `src/main/java/com/example/coffee/menu/dto/PopularMenu.java` | `a5c6a9709d0dff876de1241ea83ce3e61a0033fffcea6daeb936d01f029ac943` |
| `src/main/java/com/example/coffee/menu/controller/MenuController.java` | `64dc2574cbe97202578ee118ee8f826e366962b538797c1ffaad0bdee3b8978a` |
| `src/main/java/com/example/coffee/menu/controller/PopularMenuController.java` | `c46e5e578fa693f008134a5c54c5637a510d3cb32a0cc6d9763b3ea75f39820f` |
| `src/main/java/com/example/coffee/menu/service/MenuService.java` | `6fa26a5d26ed1a01e2d8412061873823abbd6e3f708db2b7368727d6f58ba371` |
| `src/main/java/com/example/coffee/menu/service/PopularMenuService.java` | `46f216726281afe5751e06c1ac0422897fdc6913c57391e92060f80984c8fadd` |
| `src/main/java/com/example/coffee/menu/projection/PopularMenuZsetProjection.java` | `2f066406b41af110d1a0b6e03165543b98264695065ae7027c8dbe05bb05a829` |
| `src/main/java/com/example/coffee/point/dto/ChargeRequest.java` | `2c3a6375b6a3d903373cebd202fa2b5cd58ca1adf42e5b3504082abd14251093` |
| `src/main/java/com/example/coffee/point/dto/ChargeResponse.java` | `50097cc42a280f74f19e7d3a30ee7e120d9e8a4d059b508d49e81f39373b29a3` |
| `src/main/java/com/example/coffee/point/controller/PointController.java` | `b16e451b3393f616a95783e8c850ef338658f6e15e1a130d7e171502c1614636` |
| `src/main/java/com/example/coffee/point/service/PointService.java` | `aa0cd981785da43d0068cc0fdfe701f10686d814e3a6de3274d2cfb2a8812282` |
| `src/main/java/com/example/coffee/order/dto/OrderRequest.java` | `d8c1e482e42094940eb73fc590eec0b6efd285bb39b12f3ebd33f84fb8df9ad5` |
| `src/main/java/com/example/coffee/order/dto/OrderResponse.java` | `1d51d5cab0a86cb0075bef8543dbfb9f41b9e8239435e13a0ebd7d693fa03968` |
| `src/main/java/com/example/coffee/order/dto/OrderEvent.java` | `be1c8a70b2d419399d475ea6b9b3db65a6a5826f4947888eec09c0e2ba1167f9` |
| `src/main/java/com/example/coffee/order/controller/OrderController.java` | `3f0d855137413dd937114cd232c3111aac51566cd1459b568aeddfcda4c85567` |
| `src/main/java/com/example/coffee/order/service/OrderService.java` | `b5831b8e343b40f152a63f6071ddeb751fa02c986f8267ace99b973cdd97054a` |
| `src/main/java/com/example/coffee/order/service/OrderEventSender.java` | `c3c2ead9c05e914654138be856db18e4b2ab353bd5ab80981a29540927921de8` |
| `src/main/java/com/example/coffee/order/kafka/KafkaOrderEventSender.java` | `4e565a15671992cce2f095447471aac2eb8a99f1c31cf0ff525e91b589443de4` |
| `src/main/java/com/example/coffee/order/outbox/OutboxPublisher.java` | `780b4f3a36e34c29623dddc49bd89514eff4a0268f9fa64faa52156b42808d0d` |
| `src/main/java/com/example/coffee/order/outbox/OutboxScheduler.java` | `12f087b0f2c41fc3eecffc4a42707f4b8183f28f01b6adc1c3f2bf4ed1526361` |
| `src/main/java/com/example/coffee/analytics/consumer/AnalyticsConsumer.java` | `eb5657a8ab621e1633aa063f51d3d6c4df2399ccf96ddae508dfd1fbbe2913f7` |
| `src/main/java/com/example/coffee/analytics/service/AnalyticsService.java` | `517ea8aef15188989fc3b9b8e34da05499cbe7a6f27cb68ab3861d70fdd51204` |
| `src/test/resources/application.yml` | `f9fdb52e73a6156063fb5fc61719bb11ec8d8983579e276421b06576e95404a9` |
| `src/test/java/com/example/coffee/CoffeeOrderIntegrationTest.java` | `727235c6320f5b6508f06c22e5697345f0adb54d0bb4a7d5148b7a7228723a4e` |
| `src/test/java/com/example/coffee/order/kafka/KafkaOrderEventSenderTest.java` | `cf392a5dd60225e9c1e50f9e9b3d83708a73837a3eb86d7d14669c8f1cfb2d42` |
| `src/test/java/com/example/coffee/menu/service/MenuCacheFailureTest.java` | `9c8a52cd758c88a8c4d3e4a37ae6f087d739f96ccbf58a412b96b47cb09da1a6` |
| `.gitignore` | `939062c2f4dfde8979aeb706a9599dfe6896a1a14272d7b0fe2723eb2f955b8f` |
