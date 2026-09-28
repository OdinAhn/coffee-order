# Redis 전용 코드 리뷰: 캐시와 ZSET을 구분해서 읽기

대상은 학부 1~2학년이다. 이 문서에 들어 있는 코드 블록은 아래에 적힌 **실제 파일 전체**를 복사한 것이다. 설명은 궁금한 코드 바로 위에 주석으로 달았다. `//`와 `#`, `--` 주석은 이 문서의 학습용 설명이며 원본 파일을 고치지 않았다.

## 1. Redis가 맡는 두 가지 일

```text
GET /api/menus → MenuController → MenuService → Redis 메뉴 캐시
                                           └─ 캐시가 비었거나 고장 나면 MySQL menus

예약 작업 → PopularMenuZsetProjection → MySQL orders 집계 → Redis ZSET 복사본
GET /api/menus/popular → PopularMenuService → MySQL orders 직접 집계
```

| 저장 대상 | Redis 자료형과 키 | 언제 바뀌는가 | API의 정확한 근거인가 |
| --- | --- | --- | --- |
| 메뉴 전체 목록 | Spring Cache의 `menus-v2::all` | 첫 조회 뒤 캐시 저장, 5분 TTL | 메뉴 원본은 MySQL |
| 최근 7일 주문 횟수 | ZSET `popular:7d:counts` | 예약 작업이 기본 10초마다 다시 계산 | 아니요. 인기 API는 MySQL을 직접 읽음 |

**캐시(cache)**는 원본 DB를 자주 읽지 않도록 두는 복사본이다. **TTL**은 자동 만료 시간이다. **ZSET**은 메뉴 ID 같은 멤버에 숫자 점수를 붙여 순위를 보관한다. `menus-v2`와 `popular:7d:counts`는 용도와 갱신 방식이 다른 두 저장 공간이다.

## 2. 메뉴 요청 한 번이 처리되는 순서

1. 처음 `GET /api/menus`를 호출하면 Spring의 `@Cacheable`이 Redis에서 `menus-v2::all`을 찾는다.
2. 값이 없으면 `MenuService.list()`가 MySQL에서 메뉴를 읽고 결과를 Redis에 보관한다.
3. 만료 전 같은 요청은 Redis에 저장한 목록을 사용할 수 있다. 다른 서버 인스턴스도 같은 Redis를 쓰면 캐시를 공유한다.
4. Redis 읽기·쓰기 오류는 `MenuCacheConfig`의 오류 처리기가 기록한다. 예외를 다시 던지지 않아 메뉴 조회가 MySQL로 이어지거나 DB 결과를 반환할 수 있다.
5. 메뉴 수정 API가 없는 현재 과제에서는 5분 TTL만으로도 캐시 갱신 시점을 정할 수 있다. 메뉴 수정 기능을 만들면 캐시 삭제 규칙도 설계해야 한다.

## 3. ZSET이 있는데도 인기 API가 MySQL을 읽는 이유

`PopularMenuZsetProjection`은 DB에서 최근 7일 주문을 세어 Redis에 복사한다. 복사는 기본 10초 간격이므로 새 주문 직후에는 점수가 이전 값일 수 있다. 7일이 지난 주문은 순위에서 빠져야 하므로, 새 주문이 올 때 점수만 `+1`하는 방법으로는 정확한 구간을 유지할 수 없다. 그래서 `PopularMenuService`의 API는 MySQL을 직접 조회한다.

복사본을 새로 만들 때 임시 키를 사용하고, 완성 후 `rename`으로 최종 키를 교체한다. 갱신 중인 반쪽 순위를 노출하지 않기 위한 선택이다. Redis 갱신이 실패하면 이전 ZSET이 남을 수 있으므로 이 키를 정확한 결제 통계의 근거로 사용하지 않는다.

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
      # Redis 서버 주소를 환경변수로 바꿀 수 있게 합니다. 기본은 이 컴퓨터입니다.
      # Redis 주소도 환경변수로 바꿀 수 있습니다.
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      connect-timeout: 2s
      # Redis 응답을 무한히 기다리지 않도록 제한합니다. 위 connect-timeout은 연결 시간입니다.
      timeout: 2s
  cache:
    type: redis
    # 메뉴 목록 캐시의 이름입니다. 실제 저장 키에는 뒤에 ::all이 붙습니다.
    # 이전 메뉴 객체를 담은 Redis 캐시와 새 DTO 패키지의 캐시를 분리합니다.
    cache-names: menus-v2
    redis:
      # 캐시가 5분 뒤 사라져 다음 조회에서 MySQL 값을 다시 읽게 합니다.
      # 캐시 값을 5분 뒤 만료시킵니다. 만료되면 다음 조회에서 MySQL을 다시 읽습니다.
      time-to-live: 5m

orders:
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

### 5. `src/main/java/com/example/coffee/config/redis/MenuCacheConfig.java`

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
// @Cacheable을 읽게 합니다. 이 표시가 없으면 메뉴 조회 캐시가 동작하지 않습니다.
// @Cacheable 기능을 켭니다.
@EnableCaching
public class MenuCacheConfig implements CachingConfigurer {
    private static final Logger log = LoggerFactory.getLogger(MenuCacheConfig.class);

    @Override
    // 캐시 작업이 실패했을 때 Spring이 호출할 네 가지 처리 함수를 제공합니다.
    // Redis 캐시가 고장 났을 때 어떻게 할지 정합니다. 아래 함수들은 오류를 기록하고 요청을 계속 진행하게 합니다.
    public CacheErrorHandler errorHandler() {
        return new CacheErrorHandler() {
            @Override
            // 캐시 읽기 실패를 기록하고 예외를 다시 던지지 않아 DB 조회로 이어지게 합니다.
            // 캐시 읽기가 실패해도 예외를 다시 던지지 않습니다. 메뉴 목록은 MySQL에서 읽을 수 있습니다.
            public void handleCacheGetError(RuntimeException exception, Cache cache, Object key) {
                log.warn("Menu cache read failed: {}", exception.toString());
            }

            @Override
            // DB에서 읽은 값을 Redis에 못 써도 HTTP 응답은 반환할 수 있게 합니다.
            // MySQL에서 읽은 값을 Redis에 저장하지 못해도 사용자에게 메뉴 응답을 보냅니다.
            public void handleCachePutError(RuntimeException exception, Cache cache, Object key, Object value) {
                log.warn("Menu cache write failed: {}", exception.toString());
            }

            @Override
            // 캐시 삭제 실패도 기록합니다. 이 프로젝트에는 메뉴 수정 API가 없어 평소 호출되지 않습니다.
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

### 6. `src/main/java/com/example/coffee/menu/dto/Menu.java`

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

### 7. `src/main/java/com/example/coffee/menu/controller/MenuController.java`

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

### 8. `src/main/java/com/example/coffee/menu/service/MenuService.java`

```java
// 이 파일은 메뉴를 읽는 기능입니다. HTTP 주소를 다루는 Controller와 분리했습니다.
package com.example.coffee.menu.service;

import com.example.coffee.menu.dto.Menu;
import java.util.List;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

// Spring이 이 객체를 만들어 Controller에 넣어 줍니다.
@Service
public class MenuService {
    // JdbcTemplate은 SQL을 DB에 보내고 결과를 받는 도구입니다.
    private final JdbcTemplate jdbc;

    public MenuService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // 메뉴 전체 목록 하나만 캐시하므로 고정 키 all을 씁니다.
    // 첫 호출은 MySQL을 읽고 결과를 저장합니다. 같은 키의 다음 호출은 Redis 값을 돌려줄 수 있습니다.
    // 같은 목록을 다시 요청하면 먼저 Redis 캐시를 찾아봅니다. 처음에는 아래 메서드가 실행됩니다.
    @Cacheable(cacheNames = "menus-v2", key = "'all'")
    public List<Menu> list() {
        // Redis가 비었거나 실패하면 메뉴 정보의 원본인 MySQL에서 읽습니다.
        // 캐시에 값이 없으면 MySQL의 menus 표에서 목록을 읽습니다.
        return jdbc.query("SELECT id, name, price FROM menus ORDER BY id",
                // DB의 한 행을 Menu 객체 하나로 바꿉니다. 행이 여러 개면 목록이 됩니다.
                (rs, rowNum) -> Menu.builder().id(rs.getLong("id"))
                        .name(rs.getString("name")).price(rs.getLong("price")).build());
    }
}
```

### 9. `src/main/java/com/example/coffee/menu/service/PopularMenuService.java`

```java
// 이 파일은 인기 메뉴 API에 줄 정확한 결과를 계산합니다.
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
    // 시간을 외부에서 받으면 테스트에서는 날짜를 고정해 7일 경계를 검사할 수 있습니다.
    private final Clock clock;

    public PopularMenuService(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    // 이 API는 Redis ZSET을 읽지 않습니다. 지금 시각 기준의 정확한 주문 횟수를 MySQL에서 계산합니다.
    // 호출할 때마다 MySQL 주문을 다시 세어 현재 시점의 결과를 만듭니다.
    public List<PopularMenu> list() {
        // 이번 조회의 기준 시각을 한 번만 정합니다. 시작·끝에 같은 기준을 씁니다.
        Instant now = clock.instant();
        // 주문 한 행을 주문 한 번으로 세고 메뉴별 횟수를 만듭니다.
        return jdbc.query("SELECT m.id, m.name, m.price, COUNT(o.id) AS order_count " +
                        // 주문 표의 menu_id로 메뉴 이름과 현재 가격을 함께 읽습니다.
                        "FROM orders o JOIN menus m ON m.id = o.menu_id " +
                        // 최근 7일에 들어온 결제 완료 주문만 세기 위한 시간 범위입니다.
                        // 기준 시각에서 7일 전부터 지금까지의 주문만 포함합니다. ?는 아래 Timestamp 값입니다.
                        "WHERE o.ordered_at >= ? AND o.ordered_at <= ? " +
                        // 같은 메뉴의 주문을 한 묶음으로 만들어 COUNT를 계산합니다.
                        "GROUP BY m.id, m.name, m.price " +
                        // 주문 수가 같으면 메뉴 ID가 작은 순서로 정합니다. LIMIT 3은 세 개만 남깁니다.
                        "ORDER BY order_count DESC, m.id ASC LIMIT 3",
                // SQL 결과 한 행을 API 응답 값으로 옮깁니다.
                (rs, rowNum) -> PopularMenu.builder().menuId(rs.getLong("id"))
                        .name(rs.getString("name")).price(rs.getLong("price"))
                        .orderCount(rs.getLong("order_count")).build(),
                Timestamp.from(now.minus(7, ChronoUnit.DAYS)), Timestamp.from(now));
    }
}
```

### 10. `src/main/java/com/example/coffee/menu/projection/PopularMenuZsetProjection.java`

```java
// projection은 원본 주문 데이터를 Redis의 읽기용 복사본으로 옮기는 작업입니다.
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

@Component
// 테스트에서는 이 예약 작업을 설정으로 끌 수 있습니다.
@ConditionalOnProperty(name = "popularity.zset.enabled", havingValue = "true", matchIfMissing = true)
public class PopularMenuZsetProjection {
    private static final Logger log = LoggerFactory.getLogger(PopularMenuZsetProjection.class);
    // ZSET 이름입니다. 각 멤버는 메뉴 ID 문자열, 점수는 최근 7일 주문 횟수입니다.
    // Redis ZSET을 저장할 이름입니다. 멤버는 메뉴 ID, 점수는 주문 횟수입니다.
    static final String KEY = "popular:7d:counts";

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final Clock clock;

    public PopularMenuZsetProjection(JdbcTemplate jdbc, StringRedisTemplate redis, Clock clock) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.clock = clock;
    }

    // 기본 10초마다 DB 주문을 다시 세어 Redis 복사본을 갱신합니다. 그 사이 점수는 낡을 수 있습니다.
    // 기본적으로 이전 실행이 끝난 뒤 10초가 지나면 다시 계산합니다.
    @Scheduled(fixedDelayString = "${popularity.zset.refresh-ms:10000}")
    public void refresh() {
        // 정확히 어떤 7일 구간을 복사할지 기준 시각을 정합니다.
        Instant now = clock.instant();
        // 7일이 지난 주문도 빼야 하므로 ZSET 점수만 계속 올리지 않고 DB에서 재계산합니다.
        // Redis 점수를 하나씩 더하지 않고 MySQL 주문을 다시 세어 만료된 주문도 제외합니다.
        List<MenuCount> counts = jdbc.query(
                "SELECT menu_id, COUNT(*) AS order_count FROM orders " +
                        "WHERE ordered_at >= ? AND ordered_at <= ? GROUP BY menu_id",
                (rs, rowNum) -> new MenuCount(rs.getLong("menu_id"), rs.getLong("order_count")),
                Timestamp.from(now.minus(7, ChronoUnit.DAYS)), Timestamp.from(now));
        try {
            // 최근 7일 주문이 없으면 이전 순위가 남지 않도록 키를 지웁니다.
            if (counts.isEmpty()) {
                redis.delete(KEY);
                return;
            }
            // 최종 키에 바로 쓰지 않고 임시 키를 만듭니다. 갱신 중 반쯤 만든 순위를 노출하지 않습니다.
            // 새 순위를 임시 키에 만듭니다. 만드는 도중 독자가 반쪽 결과를 보지 않게 합니다.
            String temporaryKey = KEY + ":building:" + UUID.randomUUID();
            for (MenuCount count : counts) {
                // ZSET에 메뉴 ID와 주문 횟수 점수를 넣습니다.
                redis.opsForZSet().add(temporaryKey, Long.toString(count.menuId()), count.orderCount());
            }
            // 실패로 남은 임시 키가 자동으로 없어지게 2분 만료 시간을 줍니다.
            // 중간에 작업이 끊기면 임시 키가 오래 남지 않도록 만료 시간을 둡니다.
            redis.expire(temporaryKey, Duration.ofMinutes(2));
            // 완성된 순위를 한 번에 최종 키로 교체합니다.
            // 완성한 임시 키를 최종 키로 교체합니다.
            redis.rename(temporaryKey, KEY);
        // Redis 갱신 실패를 기록합니다. 정확한 인기 API는 MySQL을 읽으므로 계속 사용할 수 있습니다.
        // Redis가 잠시 실패해도 주문·결제와 정확한 MySQL 인기 API는 계속 동작합니다.
        } catch (RuntimeException exception) {
            log.warn("Popularity ZSET refresh failed: {}", exception.toString());
        }
    }

    private record MenuCount(long menuId, long orderCount) {}
}
```

### 11. `src/test/java/com/example/coffee/menu/service/MenuCacheFailureTest.java`

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
        // 연결할 수 없는 포트를 지정해 Redis 장애 상황을 만듭니다.
        // 열려 있지 않은 포트를 지정해 Redis 장애를 흉내 냅니다.
        "spring.data.redis.port=63999"
})
class MenuCacheFailureTest {
    @Autowired MenuService menus;
    @MockitoBean OrderEventSender sender;

    @Test
    void menusRemainAvailableWhenRedisIsDown() {
        // 캐시가 없어도 MySQL의 초기 메뉴 네 개를 읽을 수 있는지 확인합니다.
        // Redis가 죽어도 MySQL에서 메뉴 네 개를 읽을 수 있어야 합니다.
        assertThat(menus.list()).hasSize(4);
    }
}
```

## 5. 테스트에서 확인할 것

- `MenuCacheFailureTest`는 연결할 수 없는 Redis 포트를 지정한 뒤에도 초기 메뉴 네 개를 조회한다.
- [통합 테스트의 `zsetProjectionUsesRollingSevenDayCounts`](../src/test/java/com/example/coffee/CoffeeOrderIntegrationTest.java)는 7일 경계에서 주문 횟수가 올바르게 ZSET에 복사되는지 검증한다.
- [통합 테스트의 `popularMenusUseRollingSevenDaysAndDeterministicTies`](../src/test/java/com/example/coffee/CoffeeOrderIntegrationTest.java)는 API가 MySQL에서 계산한 순위·동점 순서를 확인한다.

## 6. 직접 설명해 보기

1. `menus-v2::all`과 `popular:7d:counts`는 왜 서로 다른 자료인가?
2. Redis가 중단되면 메뉴 API와 인기 메뉴 API는 각각 어떤 경로로 동작하는가?
3. 임시 키를 완성한 후 `rename`하는 이유는 무엇인가?
4. ZSET 점수를 주문마다 1씩 올리기만 하면 7일 뒤 왜 틀리게 되는가?

## 원본 확인용 SHA-256

| 파일 | SHA-256 |
| --- | --- |
| `build.gradle` | `298db43900a8781e3be7514f5a6723a5d77296f4d32ad361d11a6b701d2edb45` |
| `compose.yaml` | `7bf30651e83a5bd40ada052be40ad977ae02b1ba337522b12cb0833aa519c349` |
| `src/main/resources/application.yml` | `606a0a353cfe0d1d2352bbc6d4db5b0f8ef61a5c2047b65472b379adeb577c84` |
| `src/main/resources/db/migration/V1__init.sql` | `8f3e9af804cecfa591aadba1defd8a99945d2043bb311ceb04be74b883521d3b` |
| `src/main/java/com/example/coffee/config/redis/MenuCacheConfig.java` | `e8f910cff75f2705396f8017e62ae6ec919613b349cf30695481459fc3b34cee` |
| `src/main/java/com/example/coffee/menu/dto/Menu.java` | `5754ba59bad1d027748b92108965390fdddbdcad445a72af9393b1db688a05f2` |
| `src/main/java/com/example/coffee/menu/controller/MenuController.java` | `64dc2574cbe97202578ee118ee8f826e366962b538797c1ffaad0bdee3b8978a` |
| `src/main/java/com/example/coffee/menu/service/MenuService.java` | `6fa26a5d26ed1a01e2d8412061873823abbd6e3f708db2b7368727d6f58ba371` |
| `src/main/java/com/example/coffee/menu/service/PopularMenuService.java` | `46f216726281afe5751e06c1ac0422897fdc6913c57391e92060f80984c8fadd` |
| `src/main/java/com/example/coffee/menu/projection/PopularMenuZsetProjection.java` | `2f066406b41af110d1a0b6e03165543b98264695065ae7027c8dbe05bb05a829` |
| `src/test/java/com/example/coffee/menu/service/MenuCacheFailureTest.java` | `9c8a52cd758c88a8c4d3e4a37ae6f087d739f96ccbf58a412b96b47cb09da1a6` |
