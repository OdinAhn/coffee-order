# Kafka 전용 코드 리뷰: 주문 사건이 수집되기까지

대상은 학부 1~2학년이다. 아래 코드 블록은 관련 **실제 파일 전체**를 담고, 설명은 해당 코드 바로 위에 주석으로 달았다. 주석은 학습 문서에만 있으며 기존 소스와 종합 코드 리뷰 문서는 고치지 않았다.

## 1. 메시지의 길

```text
POST /api/orders → OrderService → MySQL 주문 + outbox (같은 DB 트랜잭션)
OutboxScheduler → OutboxPublisher → KafkaOrderEventSender → orders.paid 토픽
orders.paid 토픽 → AnalyticsConsumer → AnalyticsService → MySQL 수집 기록
```

**토픽**은 Kafka의 메시지 이름표가 붙은 저장 공간이다. **파티션**은 한 토픽을 나눈 줄이다. **프로듀서**는 메시지를 보내고 **컨슈머**는 읽는다. 이 코드에서는 `OrderEvent` 한 개가 주문 완료 사건 한 개를 나타낸다.

## 2. 주문과 Kafka를 한 번에 직접 처리하지 않는 이유

MySQL과 Kafka는 서로 다른 시스템이다. 주문을 DB에 저장한 직후 서버가 꺼지면 Kafka 전송이 빠질 수 있다. 그래서 `OrderService`가 주문과 outbox 행을 **같은 MySQL 트랜잭션**에 저장한다. 예약 작업이 나중에 outbox를 읽어 Kafka로 보낸다. Kafka 확인을 받은 뒤에만 대기 행을 지운다.

| 서버가 멈춘 시점 | 남는 것 | 다시 시작하면 |
| --- | --- | --- |
| DB 트랜잭션이 끝나기 전 | 주문과 outbox가 함께 취소됨 | 새 요청으로 다시 시도 |
| DB 커밋 후 Kafka 전송 전 | 주문과 outbox가 남음 | 예약 작업이 전송 |
| Kafka 확인 후 outbox 삭제 전 | Kafka 메시지와 outbox가 모두 남을 수 있음 | 다시 전송될 수 있어 수신자가 `event_id`로 중복 저장을 막음 |

이 방식은 **최소 한 번 전달**이다. 결제 요청을 한 번만 처리하는 `Idempotency-Key`, Kafka 프로듀서 설정의 `enable.idempotence`, 수집 표의 `event_id` 중복 방지는 각각 다른 구간의 중복을 다룬다.

## 3. 파티션 3개와 소비자 3개를 읽는 법

`OrderTopicConfig`는 `orders.paid` 토픽에 파티션 3개를 요청한다. `AnalyticsConsumer`의 `concurrency = "3"`은 **애플리케이션 인스턴스 한 대에서** 최대 세 소비자를 만든다는 뜻이다. 여러 인스턴스가 모두 `coffee-analytics` 그룹을 사용하면 파티션 3개를 나눠 읽는다. 이 그룹에서 동시에 유효한 파티션 담당자는 최대 3개다. 이 로컬 구성은 Kafka 브로커가 한 대이고 복제본도 한 개라 브로커 장애에 대비한 구성은 아니다.

## 4. 코드와 줄 바로 위 설명

### 1. `build.gradle`

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

### 2. `compose.yaml`

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

### 3. `src/main/resources/application.yml`

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
      # 이 소비자 그룹에 저장된 읽기 위치가 없을 때만 토픽 앞에서부터 읽습니다.
      # 처음 소비하는 그룹이라 읽은 위치 기록이 없으면 가장 오래된 메시지부터 읽습니다.
      auto-offset-reset: earliest
      properties:
        # JSON의 타입 정보를 Java 객체로 바꿀 때 허용하는 패키지를 좁힙니다.
        # 주문 이벤트 DTO가 옮겨진 새 패키지에서 Kafka JSON 객체를 읽습니다.
        # JSON 메시지를 Java 객체로 바꿀 때 허용할 코드 패키지를 제한합니다.
        spring.json.trusted.packages: com.example.coffee.order.dto
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      # 설정된 복제본들이 기록을 확인해야 전송 성공으로 간주합니다. 이 실습은 복제본이 1개입니다.
      # Kafka가 메시지를 받았다고 확인할 때까지 기다리도록 합니다. 브로커가 한 대이므로 복제 내구성은 별개입니다.
      acks: all
      properties:
        # 프로듀서의 재전송 때문에 같은 레코드가 중복 기록되는 일을 줄입니다. 결제 요청 멱등성과는 별개입니다.
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
  # 주문 완료 사건이 지나가는 Kafka 토픽의 이름입니다.
  # 주문 완료 메시지를 보낼 Kafka 토픽 이름입니다. 토픽은 메시지를 담는 통로입니다.
  topic: orders.paid

# outbox 발행과 Redis 집계를 실행할 예약 작업 스레드를 둘로 설정합니다.
spring.task.scheduling.pool.size: 2
```

### 4. `src/main/resources/db/migration/V1__init.sql`

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

### 5. `src/main/resources/db/migration/V2__collected_order_events.sql`

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

### 6. `src/main/resources/db/migration/V3__order_idempotency.sql`

```sql
-- 클라이언트가 붙인 결제 시도 이름을 저장합니다. 이미 있던 주문은 이 값이 없어서 NULL을 허용합니다.
ALTER TABLE orders ADD COLUMN request_key VARCHAR(128);
-- 같은 사용자와 같은 요청 키로 주문을 두 개 만들지 못하게 DB가 막습니다. 서버가 여러 대여도 이 규칙은 공유됩니다.
CREATE UNIQUE INDEX uq_orders_user_request_key ON orders(user_id, request_key);
```

### 7. `src/main/java/com/example/coffee/config/kafka/OrderTopicConfig.java`

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
        // 로컬 Kafka 브로커가 한 대여서 복제본도 한 개입니다. 브로커 장애를 견디는 구성은 아닙니다.
        // 메시지 저장 줄을 세 개로 나눠 한 그룹에서 최대 세 소비자가 병렬 처리하게 합니다.
        // 하나의 Kafka 토픽을 세 갈래로 나눠 소비자 세 개가 동시에 처리할 수 있게 합니다.
        // 브로커가 한 대인 연습 환경이라 복제본은 하나입니다. 장애에 안전한 운영 구성을 뜻하지는 않습니다.
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }
}
```

### 8. `src/main/java/com/example/coffee/order/dto/OrderRequest.java`

```java
// 이 파일은 order 기능의 dto 패키지에 속합니다.
package com.example.coffee.order.dto;

import jakarta.validation.constraints.Min;
import lombok.Builder;

@Builder
// 주문할 사람과 메뉴를 담습니다. 금액은 보내지 않고 서버가 메뉴 가격을 조회합니다.
public record OrderRequest(@Min(1) long userId, @Min(1) long menuId) {}
```

### 9. `src/main/java/com/example/coffee/order/dto/OrderEvent.java`

```java
// 이 파일은 order 기능의 dto 패키지에 속합니다.
package com.example.coffee.order.dto;

import lombok.Builder;

@Builder
// Kafka로 보낼 사건입니다. eventId는 같은 메시지가 다시 왔는지 확인하는 번호입니다.
public record OrderEvent(long eventId, long orderId, long userId, long menuId, long paidAmount) {}
```

### 10. `src/main/java/com/example/coffee/order/controller/OrderController.java`

```java
// 주문 HTTP 요청을 받아 Service로 전달하는 입구입니다.
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
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    // JSON의 userId와 menuId를 OrderRequest로 받고 입력 조건을 검사합니다.
    public ResponseEntity<OrderResponse> place(@Valid @RequestBody OrderRequest request,
                                               // Idempotency-Key는 재시도해도 같은 결제를 가리키게 하는 요청별 이름입니다.
                                               @RequestHeader("Idempotency-Key") String requestKey) {
        // 새 주문을 만드는 API이므로 HTTP 201과 주문 결과를 돌려줍니다.
        return ResponseEntity.status(HttpStatus.CREATED).body(orders.place(request, requestKey));
    }
}
```

### 11. `src/main/java/com/example/coffee/order/service/OrderService.java`

```java
// 결제 규칙은 이 서비스에 모읍니다. Controller에는 SQL을 넣지 않습니다.
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
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public OrderService(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    // 주문·잔액 차감·outbox 기록을 하나로 묶습니다. 셋 중 하나라도 실패하면 함께 취소됩니다.
    // 잔액 차감·주문·outbox 기록이 모두 성공하거나 모두 취소됩니다.
    @Transactional
    public OrderResponse place(OrderRequest request, String requestKey) {
        // 빈 키나 지나치게 긴 키는 거절합니다. DB 열의 최대 길이도 128입니다.
        if (requestKey.isBlank() || requestKey.length() > 128) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST",
                    "Idempotency-Key must contain 1 to 128 characters");
        }
        // 클라이언트가 보낸 가격은 믿지 않고 DB의 메뉴 가격을 읽습니다.
        Long price = jdbc.query("SELECT price FROM menus WHERE id = ?",
                rs -> rs.next() ? rs.getLong(1) : null, request.menuId());
        // 존재하지 않는 메뉴는 결제 전에 404 오류로 끝냅니다.
        if (price == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "MENU_NOT_FOUND", "Menu does not exist");
        }
        // 사용자 잔액 행을 잠급니다. 다른 서버의 같은 사용자 결제가 여기서 기다립니다.
        Long balance = jdbc.query("SELECT balance FROM point_accounts WHERE user_id = ? FOR UPDATE",
                rs -> rs.next() ? rs.getLong(1) : null, request.userId());
        // 충전 계정이 없는 사용자는 결제할 포인트가 없습니다.
        if (balance == null) {
            throw new ApiException(HttpStatus.CONFLICT, "INSUFFICIENT_POINTS", "Insufficient points");
        }
        OrderResponse previous = jdbc.query(
                // 같은 요청 키로 완료된 주문이 있는지 잠금을 잡은 상태에서 확인합니다.
                "SELECT id, menu_id, paid_amount, ordered_at FROM orders WHERE user_id = ? AND request_key = ? FOR UPDATE",
                rs -> rs.next() ? OrderResponse.builder()
                        .orderId(rs.getLong("id")).userId(request.userId())
                        .menuId(rs.getLong("menu_id")).paidAmount(rs.getLong("paid_amount"))
                        .orderedAt(rs.getTimestamp("ordered_at").toInstant()).build() : null,
                request.userId(), requestKey);
        // 이미 처리한 요청이면 새 주문을 만들지 않고 이전 결과를 사용합니다.
        if (previous != null) {
            // 같은 키를 다른 메뉴 주문에 재사용하면 실수로 보고 409로 거절합니다.
            if (previous.menuId() != request.menuId()) {
                throw new ApiException(HttpStatus.CONFLICT, "IDEMPOTENCY_KEY_REUSED",
                        "Idempotency-Key was already used for another menu");
            }
            // 여기로 오면 잔액을 다시 빼지 않습니다. 이것이 결제 요청의 멱등성입니다.
            return previous;
        }
        // DB 안에서 잔액을 뺍니다. 충분한 포인트가 있을 때만 UPDATE가 성공합니다.
        int debited = jdbc.update("UPDATE point_accounts SET balance = balance - ? " +
                        "WHERE user_id = ? AND balance >= ?", price, request.userId(), price);
        // 차감한 행이 0개면 돈이 부족하므로 예외를 던져 전체 트랜잭션을 취소합니다.
        if (debited == 0) {
            throw new ApiException(HttpStatus.CONFLICT, "INSUFFICIENT_POINTS", "Insufficient points");
        }

        // 주문 시각을 한 번 정해 주문 행과 발행 대기 기록에 같이 씁니다.
        Instant now = clock.instant();
        // DB가 새 주문에 붙인 자동 증가 ID를 나중에 꺼내기 위한 보관함입니다.
        KeyHolder key = new GeneratedKeyHolder();
        jdbc.update(connection -> {
            // ? 자리에 값을 넣을 INSERT를 준비하고, 새 주문 ID를 돌려달라고 요청합니다.
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO orders(user_id, menu_id, paid_amount, ordered_at, request_key) VALUES (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            statement.setLong(1, request.userId());
            statement.setLong(2, request.menuId());
            statement.setLong(3, price);
            statement.setTimestamp(4, Timestamp.from(now));
            statement.setString(5, requestKey);
            return statement;
        }, key);
        // 새 주문의 ID를 얻습니다. 다음 outbox 행이 어느 주문의 메시지인지 연결할 때 씁니다.
        long orderId = key.getKey().longValue();
        // Kafka로 바로 보내기 전, 보낼 사건을 주문과 같은 MySQL 거래에 남깁니다.
        // Kafka에 보낼 내용을 주문과 같은 DB 트랜잭션에 저장합니다.
        jdbc.update("INSERT INTO order_outbox(order_id, user_id, menu_id, paid_amount, next_attempt_at) " +
                "VALUES (?, ?, ?, ?, ?)", orderId, request.userId(), request.menuId(), price, Timestamp.from(now));
        // 결제한 실제 금액과 주문 시각을 HTTP 응답용 객체로 만듭니다.
        return OrderResponse.builder().orderId(orderId).userId(request.userId()).menuId(request.menuId())
                .paidAmount(price).orderedAt(now).build();
    }
}
```

### 12. `src/main/java/com/example/coffee/order/service/OrderEventSender.java`

```java
// interface는 구현 방법 대신 사용할 메서드의 모양을 정한 약속입니다.
package com.example.coffee.order.service;

import com.example.coffee.order.dto.OrderEvent;

public interface OrderEventSender {
    // outbox는 이 약속에만 의존하므로 테스트에서는 가짜 전송 객체를 넣을 수 있습니다.
    void send(OrderEvent event);
}
```

### 13. `src/main/java/com/example/coffee/order/kafka/KafkaOrderEventSender.java`

```java
// OrderEventSender 약속을 Kafka로 실제 전송하는 코드입니다.
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
public class KafkaOrderEventSender implements OrderEventSender {
    // KafkaTemplate은 Spring이 제공하는 Kafka 전송 도구입니다.
    private final KafkaTemplate<String, OrderEvent> kafka;
    private final String topic;

    public KafkaOrderEventSender(KafkaTemplate<String, OrderEvent> kafka,
                                 // 토픽 이름을 application.yml 설정에서 읽습니다.
                                 @Value("${orders.topic}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    // interface의 send 약속을 이 클래스가 구현한다는 표시입니다.
    @Override
    public void send(OrderEvent event) {
        try {
            // Kafka의 확인을 최대 5초 기다린 뒤에만 발행 성공으로 처리합니다.
            // 사용자 ID를 Kafka 키로 씁니다. 같은 사용자 사건은 같은 파티션에 배치됩니다.
            // 사용자 ID를 키로 보내 같은 사용자의 메시지가 같은 파티션에 가도록 합니다.
            // Kafka 확인을 최대 5초 기다립니다. 확인 전에는 outbox 기록을 지우지 않습니다.
            kafka.send(topic, Long.toString(event.userId()), event).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            // 대기 중 중단되었다는 신호를 원래 스레드에 다시 남깁니다.
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka publication interrupted", exception);
        // 실패나 시간 초과를 outbox로 알려 발행 대기 행을 남깁니다.
        // Kafka 오류나 시간 초과를 상위 outbox 작업에 알려 재시도하게 합니다.
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Kafka publication failed", exception);
        }
    }
}
```

### 14. `src/main/java/com/example/coffee/order/outbox/OutboxPublisher.java`

```java
// outbox는 DB에 저장한 'Kafka에 아직 보낼 일' 목록입니다.
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

    // outbox 행을 잠그고 Kafka 확인까지 기다립니다. 이 동안 DB 잠금도 유지됩니다.
    // 행을 잡은 상태에서 Kafka 확인까지 기다리고, 성공하면 삭제합니다. 이 동안 DB 잠금이 유지됩니다.
    @Transactional
    public boolean publishOne() {
        // 재시도할 수 있는 시각이 되었는지 비교할 현재 시각입니다.
        Instant now = clock.instant();
        List<OrderEvent> ready = jdbc.query(
                // 아직 발행하지 않은 주문 사건 하나를 DB에서 읽습니다.
                "SELECT id, order_id, user_id, menu_id, paid_amount FROM order_outbox " +
                        // 다른 서버가 이미 선택한 행은 건너뛰어 같은 사건을 동시에 집지 않게 합니다.
                        // 재시도 대기 시간이 지난 행 가운데 가장 오래된 하나를 선택합니다.
                        // 다른 서버가 잡은 행은 건너뜁니다. 서버 여러 대가 같은 행을 동시에 보내지 않게 돕습니다.
                        "WHERE next_attempt_at <= ? ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",
                (rs, rowNum) -> OrderEvent.builder()
                        .eventId(rs.getLong("id")).orderId(rs.getLong("order_id"))
                        .userId(rs.getLong("user_id")).menuId(rs.getLong("menu_id"))
                        .paidAmount(rs.getLong("paid_amount")).build(),
                Timestamp.from(now));
        // 보낼 일이 없다는 뜻으로 false를 반환하면 Scheduler의 반복이 멈춥니다.
        if (ready.isEmpty()) {
            return false;
        }

        OrderEvent event = ready.get(0);
        try {
            // Kafka 확인이 끝나야 다음 줄에서 대기 행을 지웁니다.
            // Kafka가 받았다는 확인까지 기다립니다. 실패하면 아래 catch로 갑니다.
            sender.send(event);
            // 전송이 확인된 사건만 대기 목록에서 제거합니다.
            // 성공을 확인한 사건만 발행 대기 목록에서 지웁니다.
            jdbc.update("DELETE FROM order_outbox WHERE id = ?", event.eventId());
        } catch (RuntimeException exception) {
            // 실패 횟수를 저장하고 5초 뒤에 다시 시도합니다.
            // 실패 횟수를 올리고 다음 시도를 5초 뒤로 미룹니다.
            jdbc.update("UPDATE order_outbox SET attempts = attempts + 1, next_attempt_at = ? WHERE id = ?",
                    Timestamp.from(now.plus(Duration.ofSeconds(5))), event.eventId());
            log.warn("Could not publish order event {}: {}", event.eventId(), exception.toString());
        }
        // 한 건을 처리했으니 Scheduler가 이어서 다음 건을 확인할 수 있게 합니다.
        return true;
    }
}
```

### 15. `src/main/java/com/example/coffee/order/outbox/OutboxScheduler.java`

```java
// 예약 작업을 시작하는 클래스입니다. 발행 규칙은 Publisher에 있습니다.
package com.example.coffee.order.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
// 테스트에서 자동 발행을 끄고 원하는 시점에 수동으로 검증할 수 있습니다.
@ConditionalOnProperty(name = "analytics.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxScheduler {
    private final OutboxPublisher publisher;

    public OutboxScheduler(OutboxPublisher publisher) {
        this.publisher = publisher;
    }

    // 기본적으로 이전 실행이 끝난 뒤 1초가 지나면 다시 발행을 시작합니다.
    // 기본적으로 이전 실행이 끝난 뒤 1초가 지나면 실행합니다.
    @Scheduled(fixedDelayString = "${analytics.publisher.delay-ms:1000}")
    public void publish() {
        // 준비된 사건이 없을 때까지 한 건씩 발행합니다.
        // 준비된 메시지가 계속 있으면 한 건씩 보내고, 없으면 멈춥니다.
        while (publisher.publishOne()) {
            // Drain the ready queue without waiting for another scheduled run.
        }
    }
}
```

### 16. `src/main/java/com/example/coffee/analytics/consumer/AnalyticsConsumer.java`

```java
// Kafka 메시지를 받는 입구입니다. DB 저장은 Service에 맡깁니다.
package com.example.coffee.analytics.consumer;

import com.example.coffee.analytics.service.AnalyticsService;
import com.example.coffee.order.dto.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsConsumer {
    // 생성자로 서비스를 받아 Spring이 두 객체를 연결합니다.
    private final AnalyticsService analytics;

    public AnalyticsConsumer(AnalyticsService analytics) {
        this.analytics = analytics;
    }

    // 이 인스턴스에서 소비자를 최대 세 개 만듭니다. 여러 인스턴스가 같은 그룹이면 파티션 세 개를 나눠 가집니다.
    // orders.paid 토픽을 같은 그룹에서 소비합니다. concurrency=3은 이 인스턴스의 소비자 최대 세 개입니다.
    @KafkaListener(topics = "${orders.topic}", groupId = "coffee-analytics", concurrency = "3")
    public void collect(OrderEvent event) {
        // DB 저장을 Service에 맡깁니다. 오류가 나면 성공 처리하지 않고 재시도될 수 있습니다.
        // Service가 저장을 마친 뒤 돌아옵니다. 오류가 나면 Kafka 처리도 실패해 재전달될 수 있습니다.
        analytics.collect(event);
    }
}
```

### 17. `src/main/java/com/example/coffee/analytics/service/AnalyticsService.java`

```java
// 받은 주문 사건을 수집 표에 저장하는 일을 담당합니다.
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

    // 이벤트 한 건의 DB 저장을 거래로 묶습니다.
    // 저장 SQL이 실패하면 DB 작업을 취소하고 오류를 소비자까지 전달합니다.
    @Transactional
    public void collect(OrderEvent event) {
        // event_id, 주문 번호, 사용자, 메뉴, 결제 금액을 남깁니다.
        jdbc.update("INSERT INTO collected_order_events(event_id, order_id, user_id, menu_id, paid_amount) " +
                        // 같은 event_id가 다시 도착하면 새 행을 만들지 않습니다. Kafka 재전달에 대비합니다.
                        // event_id는 기본 키입니다. 같은 Kafka 사건이 다시 오면 두 번째 행을 만들지 않습니다.
                        "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE event_id = event_id",
                // ? 다섯 곳에 사건의 값을 순서대로 넣습니다. 문자열 결합보다 안전합니다.
                event.eventId(), event.orderId(), event.userId(), event.menuId(), event.paidAmount());
    }
}
```

### 18. `src/test/java/com/example/coffee/order/kafka/KafkaOrderEventSenderTest.java`

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
        // Kafka가 확인을 보냈다고 가정하는 가짜 결과입니다. 실제 브로커를 띄우는 테스트는 아닙니다.
        // Kafka가 전송 성공을 바로 알려 준 상황을 만듭니다.
        when(kafka.send("orders.paid", "9", event)).thenReturn(CompletableFuture.completedFuture(null));

        sender.send(event);

        // 어떤 토픽·키·이벤트를 전송했는지 확인합니다.
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

## 5. 테스트에서 확인할 것

- `KafkaOrderEventSenderTest`는 토픽·사용자 키·이벤트 값 전송과 전송 오류를 가짜 `KafkaTemplate`으로 검사한다. 실제 브로커 연결 검사는 아니다.
- [통합 테스트의 `failedDeliveryStaysInOutboxForRetry`](../src/test/java/com/example/coffee/CoffeeOrderIntegrationTest.java)는 전송 실패 시 대기 행이 남는지 확인한다.
- [통합 테스트의 `concurrentPublishersDoNotClaimTheSameEvent`](../src/test/java/com/example/coffee/CoffeeOrderIntegrationTest.java)는 서버 역할의 두 발행 작업이 같은 행을 동시에 선택하지 않는지 확인한다.
- [통합 테스트의 `analyticsConsumerDeduplicatesKafkaRedelivery`](../src/test/java/com/example/coffee/CoffeeOrderIntegrationTest.java)는 같은 `event_id`를 다시 수집해도 행이 하나인지 확인한다.

## 6. 이 구현을 읽을 때 기억할 점

- `OutboxPublisher`는 Kafka 확인을 기다리는 최대 5초 동안 DB 행 잠금을 유지한다. 주문량이 커지면 처리량과 잠금 시간을 측정해야 한다.
- 전송이 확인된 뒤 삭제 전에 장애가 나면 재전송이 가능하다. 수집 표의 기본 키는 **저장 결과의 중복**을 막지만, Kafka에 메시지가 한 번만 도착한다고 보장하지는 않는다.
- Kafka 메시지에 `userId`, `menuId`, `paidAmount`가 포함되어 데이터 수집 플랫폼에 필요한 값을 전달한다.

## 7. 직접 설명해 보기

1. `OrderService`가 주문과 outbox를 하나의 트랜잭션에 쓰는 이유는 무엇인가?
2. `FOR UPDATE SKIP LOCKED`가 여러 서버의 발행 작업에서 하는 일은 무엇인가?
3. `kafka.send(...).get(...)`이 실패하면 outbox 행은 어떻게 되는가?
4. Kafka에 같은 사건이 두 번 도착해도 수집 표에 한 행만 남는 근거는 무엇인가?

## 원본 확인용 SHA-256

| 파일 | SHA-256 |
| --- | --- |
| `build.gradle` | `298db43900a8781e3be7514f5a6723a5d77296f4d32ad361d11a6b701d2edb45` |
| `compose.yaml` | `7bf30651e83a5bd40ada052be40ad977ae02b1ba337522b12cb0833aa519c349` |
| `src/main/resources/application.yml` | `606a0a353cfe0d1d2352bbc6d4db5b0f8ef61a5c2047b65472b379adeb577c84` |
| `src/main/resources/db/migration/V1__init.sql` | `8f3e9af804cecfa591aadba1defd8a99945d2043bb311ceb04be74b883521d3b` |
| `src/main/resources/db/migration/V2__collected_order_events.sql` | `1c0bc2c5ce5d1316c9bc79805fa5ed479e29b65d009cd84d75acdd6d56f5ad07` |
| `src/main/resources/db/migration/V3__order_idempotency.sql` | `1c7d5348ff90735fc91a6bb1b287fdc20bf43c1209205cb6fd63a4a3a5aad3a8` |
| `src/main/java/com/example/coffee/config/kafka/OrderTopicConfig.java` | `6693d7bb3f8b6f2a5f28907fe9cab7fa48be665824aa8061760700d289dd1e47` |
| `src/main/java/com/example/coffee/order/dto/OrderRequest.java` | `d8c1e482e42094940eb73fc590eec0b6efd285bb39b12f3ebd33f84fb8df9ad5` |
| `src/main/java/com/example/coffee/order/dto/OrderEvent.java` | `be1c8a70b2d419399d475ea6b9b3db65a6a5826f4947888eec09c0e2ba1167f9` |
| `src/main/java/com/example/coffee/order/controller/OrderController.java` | `3f0d855137413dd937114cd232c3111aac51566cd1459b568aeddfcda4c85567` |
| `src/main/java/com/example/coffee/order/service/OrderService.java` | `b5831b8e343b40f152a63f6071ddeb751fa02c986f8267ace99b973cdd97054a` |
| `src/main/java/com/example/coffee/order/service/OrderEventSender.java` | `c3c2ead9c05e914654138be856db18e4b2ab353bd5ab80981a29540927921de8` |
| `src/main/java/com/example/coffee/order/kafka/KafkaOrderEventSender.java` | `4e565a15671992cce2f095447471aac2eb8a99f1c31cf0ff525e91b589443de4` |
| `src/main/java/com/example/coffee/order/outbox/OutboxPublisher.java` | `780b4f3a36e34c29623dddc49bd89514eff4a0268f9fa64faa52156b42808d0d` |
| `src/main/java/com/example/coffee/order/outbox/OutboxScheduler.java` | `12f087b0f2c41fc3eecffc4a42707f4b8183f28f01b6adc1c3f2bf4ed1526361` |
| `src/main/java/com/example/coffee/analytics/consumer/AnalyticsConsumer.java` | `eb5657a8ab621e1633aa063f51d3d6c4df2399ccf96ddae508dfd1fbbe2913f7` |
| `src/main/java/com/example/coffee/analytics/service/AnalyticsService.java` | `517ea8aef15188989fc3b9b8e34da05499cbe7a6f27cb68ab3861d70fdd51204` |
| `src/test/java/com/example/coffee/order/kafka/KafkaOrderEventSenderTest.java` | `cf392a5dd60225e9c1e50f9e9b3d83708a73837a3eb86d7d14669c8f1cfb2d42` |
