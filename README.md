# 🔴 Redis with Spring Boot — Complete Notes

> **Interview-Friendly | Production-Ready | Code Examples Included**

---

## 📚 Table of Contents

1. [What is Redis & Why Use It?](#1-what-is-redis--why-use-it)
2. [Redis Data Structures](#2-redis-data-structures)
3. [Spring Boot Redis Setup](#3-spring-boot-redis-setup)
4. [Spring Cache Abstraction with Redis](#4-spring-cache-abstraction-with-redis)
5. [RedisTemplate — Manual Cache Control](#5-redistemplate--manual-cache-control)
6. [Redis for Session Management](#6-redis-for-session-management)
7. [Redis TTL & Eviction Policies](#7-redis-ttl--eviction-policies)
8. [Redis Pub/Sub Messaging](#8-redis-pubsub-messaging)
9. [Redis as Distributed Lock](#9-redis-as-distributed-lock)
10. [Redis with Kafka — Pattern Comparison](#10-redis-with-kafka--pattern-comparison)
11. [Redis in Microservices Architecture](#11-redis-in-microservices-architecture)
12. [Common Interview Questions & Answers](#12-common-interview-questions--answers)

---

## 1. What is Redis & Why Use It?

### 🔍 What is Redis?

**Redis** (Remote Dictionary Server) is an **in-memory, key-value data store** that is blazing fast because it keeps all data in RAM. It supports rich data structures (not just strings) and is used for caching, session storage, message brokering, leaderboards, and more.

| Feature | Redis | Traditional DB (MySQL/Postgres) |
|---|---|---|
| Storage | In-Memory (RAM) | Disk |
| Read Speed | ~100K ops/sec | ~1K ops/sec |
| Data Structures | String, List, Set, Hash, ZSet | Tables with rows |
| Persistence | Optional (RDB/AOF) | Yes (default) |
| Use Case | Cache, Session, Pub/Sub | Persistent data storage |

### ✅ Why Redis in Microservices?

- **Reduce DB load** — Cache frequently read data (product catalog, user profile)
- **Improve latency** — Sub-millisecond responses from RAM
- **Distributed Session** — Share user sessions across multiple service instances
- **Rate Limiting** — Track API call counts with `INCR` + TTL
- **Distributed Locks** — Prevent race conditions across pods

---

## 2. Redis Data Structures

Understanding these is critical for interviews. Each structure maps to a Java type in Spring.

### 🔤 String
Simplest type. Stores any value — text, number, JSON blob.

```bash
SET user:101 "Rabbani"
GET user:101         # → "Rabbani"
INCR visit:count     # Atomic increment → great for counters
SETEX token:abc 3600 "jwt-value"  # Set with TTL (seconds)
```

### 📋 List (Ordered, allows duplicates)
Like a Java `LinkedList`. Used for queues, activity feeds.

```bash
LPUSH notifications:user:1 "New login"
RPUSH notifications:user:1 "Transfer done"
LRANGE notifications:user:1 0 -1   # Get all items
LPOP notifications:user:1           # Remove from front
```

### 🎯 Set (Unordered, unique values)
Like a Java `HashSet`. Used for unique tracking (unique visitors, tags).

```bash
SADD online:users "user1" "user2" "user1"  # Deduped automatically
SMEMBERS online:users     # → {user1, user2}
SISMEMBER online:users "user1"  # → 1 (true)
```

### 🗃️ Hash (Field-Value inside a key)
Like a Java `HashMap` stored under a single Redis key. Perfect for objects.

```bash
HSET user:101 name "Rabbani" role "Engineer" city "Hyderabad"
HGET user:101 name        # → "Rabbani"
HGETALL user:101          # → all fields
HINCRBY user:101 loginCount 1
```

### 📊 Sorted Set / ZSet (Score-based ranking)
Like a `TreeSet` with scores. Used for leaderboards, priority queues.

```bash
ZADD leaderboard 1500 "player:1"
ZADD leaderboard 2200 "player:2"
ZRANGE leaderboard 0 -1 WITHSCORES   # Ascending by score
ZREVRANGE leaderboard 0 2            # Top 3 players
```

---

## 3. Spring Boot Redis Setup

### 📦 Maven Dependencies

```xml
<!-- pom.xml -->
<dependencies>
    <!-- Spring Data Redis -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-redis</artifactId>
    </dependency>

    <!-- Lettuce client (default, production-grade, async) -->
    <!-- Already included above via starter -->

    <!-- OR switch to Jedis (thread-per-connection, simpler) -->
    <dependency>
        <groupId>redis.clients</groupId>
        <artifactId>jedis</artifactId>
    </dependency>

    <!-- Spring Cache abstraction -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-cache</artifactId>
    </dependency>
</dependencies>
```

> 💡 **Interview Tip:** Lettuce uses Netty (non-blocking, async), shares a single connection. Jedis uses a thread-per-connection pool. Use **Lettuce** for reactive apps; **Jedis** for simpler synchronous workloads.

### ⚙️ application.yml Configuration

```yaml
spring:
  redis:
    host: localhost          # Redis server host
    port: 6379               # Default Redis port
    password: yourpassword   # If Redis is password-protected
    database: 0              # Redis DB index (0–15)
    timeout: 2000ms          # Connection timeout

    # Lettuce connection pool (production recommended)
    lettuce:
      pool:
        max-active: 10       # Max concurrent connections
        max-idle: 5
        min-idle: 2
        max-wait: 1000ms
```

### 🔧 Redis Configuration Bean

```java
@Configuration
@EnableCaching
public class RedisConfig {

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        // Lettuce (default) — async, non-blocking
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
            new RedisStandaloneConfiguration("localhost", 6379)
        );
        return factory;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);

        // Key serializer: human-readable string keys
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        // Value serializer: JSON (readable + type-safe)
        Jackson2JsonRedisSerializer<Object> jsonSerializer =
            new Jackson2JsonRedisSerializer<>(Object.class);

        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        jsonSerializer.setObjectMapper(mapper);

        template.setValueSerializer(jsonSerializer);
        template.setHashValueSerializer(jsonSerializer);

        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public CacheManager cacheManager(RedisConnectionFactory factory) {
        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
            .entryTtl(Duration.ofMinutes(10))             // Global TTL: 10 minutes
            .disableCachingNullValues()                   // Don't cache nulls
            .serializeKeysWith(
                RedisSerializationContext.SerializationPair
                    .fromSerializer(new StringRedisSerializer()))
            .serializeValuesWith(
                RedisSerializationContext.SerializationPair
                    .fromSerializer(new GenericJackson2JsonRedisSerializer()));

        return RedisCacheManager.builder(factory)
            .cacheDefaults(config)
            .build();
    }
}
```

---

## 4. Spring Cache Abstraction with Redis

Spring's cache abstraction lets you **annotate methods** instead of writing cache logic manually.

### 🏷️ Core Annotations

| Annotation | What it does |
|---|---|
| `@Cacheable` | Returns cached result if exists; else calls method + stores result |
| `@CachePut` | Always calls method, always updates cache (used for updates) |
| `@CacheEvict` | Removes entry from cache (used on delete/update) |
| `@Caching` | Combine multiple cache operations on one method |

### ✅ Enable Caching

```java
@SpringBootApplication
@EnableCaching  // Required to activate cache annotations
public class CitiCoreApplication {
    public static void main(String[] args) {
        SpringApplication.run(CitiCoreApplication.class, args);
    }
}
```

### 📝 Service Layer Examples

```java
@Service
public class AccountService {

    @Autowired
    private AccountRepository accountRepository;

    // ─── @Cacheable ───────────────────────────────────────────────────────────
    // First call: hits DB, stores in Redis under key "accounts::101"
    // Subsequent calls: returns from Redis without hitting DB
    @Cacheable(value = "accounts", key = "#accountId")
    public Account getAccountById(Long accountId) {
        System.out.println("Fetching from DB..."); // Only printed on cache miss
        return accountRepository.findById(accountId)
            .orElseThrow(() -> new RuntimeException("Account not found"));
    }

    // Conditional caching — only cache if balance > 0
    @Cacheable(value = "accounts", key = "#accountId", condition = "#accountId > 0")
    public Account getAccount(Long accountId) {
        return accountRepository.findById(accountId).orElse(null);
    }

    // ─── @CachePut ────────────────────────────────────────────────────────────
    // Always executes method AND refreshes the cache
    // Use this on UPDATE operations so cache stays in sync
    @CachePut(value = "accounts", key = "#account.id")
    public Account updateAccount(Account account) {
        return accountRepository.save(account);
    }

    // ─── @CacheEvict ──────────────────────────────────────────────────────────
    // Removes specific entry from cache on delete
    @CacheEvict(value = "accounts", key = "#accountId")
    public void deleteAccount(Long accountId) {
        accountRepository.deleteById(accountId);
    }

    // Evict ALL entries from the "accounts" cache
    @CacheEvict(value = "accounts", allEntries = true)
    public void clearAllAccountsCache() {
        // Useful after bulk update operations
    }

    // ─── @Caching ─────────────────────────────────────────────────────────────
    // Multiple cache operations in one annotation
    @Caching(evict = {
        @CacheEvict(value = "accounts", key = "#accountId"),
        @CacheEvict(value = "userAccounts", key = "#userId")
    })
    public void closeAccount(Long accountId, Long userId) {
        accountRepository.deleteById(accountId);
    }
}
```

### 🔑 SpEL (Spring Expression Language) in Cache Keys

```java
// Use method parameter
@Cacheable(value = "users", key = "#userId")

// Use field of parameter object
@Cacheable(value = "accounts", key = "#account.id")

// Combine multiple params
@Cacheable(value = "transactions", key = "#accountId + ':' + #status")

// Use root object (method name)
@Cacheable(value = "data", key = "#root.methodName + #id")
```

---

## 5. RedisTemplate — Manual Cache Control

When you need **fine-grained control** over Redis operations (TTL per key, specific data structures), use `RedisTemplate` directly.

### 📝 ValueOperations — String/Object storage

```java
@Service
public class TokenService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // Store a value with TTL
    public void storeToken(String userId, String token) {
        ValueOperations<String, Object> ops = redisTemplate.opsForValue();
        ops.set("token:" + userId, token, Duration.ofHours(1));
    }

    // Retrieve value
    public String getToken(String userId) {
        return (String) redisTemplate.opsForValue().get("token:" + userId);
    }

    // Check if key exists
    public boolean tokenExists(String userId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey("token:" + userId));
    }

    // Delete key
    public void deleteToken(String userId) {
        redisTemplate.delete("token:" + userId);
    }

    // Atomic counter (for rate limiting)
    public Long incrementLoginAttempts(String userId) {
        String key = "loginAttempts:" + userId;
        Long count = redisTemplate.opsForValue().increment(key);
        redisTemplate.expire(key, Duration.ofMinutes(15)); // Reset window in 15 min
        return count;
    }
}
```

### 📝 HashOperations — Object fields

```java
@Service
public class UserCacheService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // Store user as hash (field-level access is efficient)
    public void cacheUser(User user) {
        HashOperations<String, String, Object> ops = redisTemplate.opsForHash();
        String key = "user:" + user.getId();
        ops.put(key, "name", user.getName());
        ops.put(key, "email", user.getEmail());
        ops.put(key, "role", user.getRole());
        redisTemplate.expire(key, Duration.ofMinutes(30));
    }

    // Get a single field (no need to deserialize entire object)
    public String getUserName(Long userId) {
        return (String) redisTemplate.opsForHash().get("user:" + userId, "name");
    }

    // Get all fields as a Map
    public Map<String, Object> getUserData(Long userId) {
        return redisTemplate.opsForHash().entries("user:" + userId);
    }

    // Update a single field
    public void updateUserRole(Long userId, String newRole) {
        redisTemplate.opsForHash().put("user:" + userId, "role", newRole);
    }
}
```

### 📝 ListOperations — Queues / Feeds

```java
@Service
public class NotificationService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // Push notification to user's list (left = newest first)
    public void pushNotification(Long userId, String message) {
        ListOperations<String, Object> ops = redisTemplate.opsForList();
        ops.leftPush("notifications:" + userId, message);
        // Keep only last 50 notifications
        redisTemplate.opsForList().trim("notifications:" + userId, 0, 49);
    }

    // Get recent notifications
    public List<Object> getNotifications(Long userId) {
        return redisTemplate.opsForList().range("notifications:" + userId, 0, 9); // last 10
    }
}
```

### 📝 SetOperations — Unique tracking

```java
@Service
public class OnlineUserService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public void markOnline(String userId) {
        redisTemplate.opsForSet().add("online:users", userId);
        redisTemplate.expire("online:users", Duration.ofMinutes(5)); // Refresh TTL on heartbeat
    }

    public void markOffline(String userId) {
        redisTemplate.opsForSet().remove("online:users", userId);
    }

    public Long getOnlineCount() {
        return redisTemplate.opsForSet().size("online:users");
    }

    public boolean isOnline(String userId) {
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember("online:users", userId));
    }
}
```

### 📝 ZSetOperations — Leaderboard / Ranked data

```java
@Service
public class LeaderboardService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public void updateScore(String playerId, double score) {
        redisTemplate.opsForZSet().add("leaderboard", playerId, score);
    }

    // Get top 10 players (highest score first)
    public Set<Object> getTopPlayers() {
        return redisTemplate.opsForZSet()
            .reverseRange("leaderboard", 0, 9);
    }

    // Get rank of a player (0-indexed)
    public Long getPlayerRank(String playerId) {
        return redisTemplate.opsForZSet().reverseRank("leaderboard", playerId);
    }
}
```

---

## 6. Redis for Session Management

Instead of storing user sessions in server memory (which breaks with multiple instances), store them in Redis. All pods share the same session store.

### 📦 Dependency

```xml
<dependency>
    <groupId>org.springframework.session</groupId>
    <artifactId>spring-session-data-redis</artifactId>
</dependency>
```

### ⚙️ Configuration

```yaml
spring:
  session:
    store-type: redis          # Use Redis as session store
    timeout: 30m               # Session expiry
    redis:
      namespace: myapp:session # Prefix for Redis keys
```

```java
@Configuration
@EnableRedisHttpSession(maxInactiveIntervalInSeconds = 1800) // 30 minutes
public class SessionConfig {
    // Spring auto-configures the rest using your RedisConnectionFactory bean
}
```

### 📝 Usage in Controller

```java
@RestController
@RequestMapping("/auth")
public class AuthController {

    @PostMapping("/login")
    public ResponseEntity<String> login(@RequestBody LoginRequest req,
                                        HttpSession session) {
        // Validate credentials...
        // Store user info in session (goes to Redis automatically)
        session.setAttribute("userId", req.getUserId());
        session.setAttribute("role", "ADMIN");
        return ResponseEntity.ok("Login successful");
    }

    @GetMapping("/profile")
    public ResponseEntity<String> profile(HttpSession session) {
        String userId = (String) session.getAttribute("userId");
        if (userId == null) return ResponseEntity.status(401).body("Not logged in");
        return ResponseEntity.ok("Welcome " + userId);
    }

    @PostMapping("/logout")
    public ResponseEntity<String> logout(HttpSession session) {
        session.invalidate(); // Removes from Redis
        return ResponseEntity.ok("Logged out");
    }
}
```

> 💡 **Why this matters in microservices:** Without Redis sessions, if User Service Pod-1 handles login and Pod-2 handles the next request, Pod-2 has no session. Redis solves this by being a **shared, centralized session store**.

---

## 7. Redis TTL & Eviction Policies

### ⏱️ TTL (Time To Live)

Every key in Redis can have an expiry. Once expired, Redis automatically deletes it.

```java
// Set key with TTL
redisTemplate.opsForValue().set("otp:" + phone, "123456", Duration.ofMinutes(5));

// Set TTL on an existing key
redisTemplate.expire("session:user:101", Duration.ofHours(2));

// Check remaining TTL
Long secondsLeft = redisTemplate.getExpire("otp:" + phone, TimeUnit.SECONDS);

// Remove TTL (make key persistent)
redisTemplate.persist("cache:config");
```

### 🚨 Eviction Policies (When Redis runs out of memory)

Configured in `redis.conf` or AWS ElastiCache settings:

| Policy | Behavior | Best For |
|---|---|---|
| `noeviction` | Returns error when memory full | Not for caches |
| `allkeys-lru` | Evict least recently used among ALL keys | General caching ✅ |
| `volatile-lru` | Evict LRU keys that have TTL set | Mixed use |
| `allkeys-lfu` | Evict least frequently used | When recent ≠ important |
| `volatile-ttl` | Evict keys with shortest TTL first | Short-lived data |
| `allkeys-random` | Evict random key | Avoid in production |

```bash
# redis.conf
maxmemory 512mb
maxmemory-policy allkeys-lru
```

> 💡 **Interview Answer:** "In production, we set `maxmemory-policy allkeys-lru` because we use Redis purely as a cache. LRU ensures the least recently accessed data is dropped first when memory pressure occurs, preserving hot data."

---

## 8. Redis Pub/Sub Messaging

Redis Pub/Sub is a lightweight **fire-and-forget** messaging mechanism. Publishers send to a channel; all subscribers receive it.

> ⚠️ Unlike Kafka, messages are **not persisted**. If a subscriber is offline, it misses the message.

### 🔧 Setup

```java
@Configuration
public class RedisPubSubConfig {

    @Bean
    public MessageListenerAdapter messageListenerAdapter(NotificationSubscriber subscriber) {
        return new MessageListenerAdapter(subscriber, "onMessage");
    }

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory factory,
            MessageListenerAdapter listenerAdapter) {

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        // Subscribe to channel "notifications"
        container.addMessageListener(listenerAdapter, new PatternTopic("notifications"));
        return container;
    }
}
```

### 📩 Subscriber

```java
@Component
public class NotificationSubscriber {

    public void onMessage(String message, String channel) {
        System.out.println("Received on [" + channel + "]: " + message);
        // Process notification...
    }
}
```

### 📤 Publisher

```java
@Service
public class NotificationPublisher {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public void publish(String message) {
        redisTemplate.convertAndSend("notifications", message);
    }
}
```

### Usage

```java
notificationPublisher.publish("Transaction of ₹5000 credited to account 101");
```

---

## 9. Redis as Distributed Lock

In a microservices world with multiple pods, you need a distributed lock to prevent **race conditions** (e.g., two pods processing the same payment simultaneously).

### 🔐 Using `SET NX EX` (Atomic lock)

```java
@Service
public class DistributedLockService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String LOCK_PREFIX = "lock:";

    /**
     * Acquire lock. Returns true if lock acquired, false if already locked.
     * NX = Set only if Not eXists (atomic)
     * EX = Expire in 30 seconds (auto-release on crash)
     */
    public boolean acquireLock(String resourceId, String ownerId) {
        String lockKey = LOCK_PREFIX + resourceId;
        Boolean acquired = redisTemplate.opsForValue()
            .setIfAbsent(lockKey, ownerId, Duration.ofSeconds(30));
        return Boolean.TRUE.equals(acquired);
    }

    /**
     * Release lock only if the caller owns it (prevents accidental release)
     */
    public boolean releaseLock(String resourceId, String ownerId) {
        String lockKey = LOCK_PREFIX + resourceId;
        String currentOwner = (String) redisTemplate.opsForValue().get(lockKey);
        if (ownerId.equals(currentOwner)) {
            redisTemplate.delete(lockKey);
            return true;
        }
        return false; // Someone else holds the lock
    }
}
```

### 📝 Usage in Transaction Service

```java
@Service
public class TransactionService {

    @Autowired
    private DistributedLockService lockService;

    public void processPayment(Long accountId, double amount) {
        String ownerId = UUID.randomUUID().toString(); // Unique per request

        if (!lockService.acquireLock("account:" + accountId, ownerId)) {
            throw new RuntimeException("Account is being processed. Try again.");
        }

        try {
            // Only one pod executes this at a time
            performDebit(accountId, amount);
        } finally {
            lockService.releaseLock("account:" + accountId, ownerId);
        }
    }
}
```

> 💡 **Interview Tip:** "For production-grade distributed locking, consider **Redisson** library which implements the **Redlock algorithm** — a consensus-based approach for lock safety across Redis cluster nodes."

---

## 10. Redis with Kafka — Pattern Comparison

| Aspect | Redis Pub/Sub | Kafka |
|---|---|---|
| Persistence | ❌ No | ✅ Yes (configurable) |
| Message Replay | ❌ No | ✅ Yes (offset-based) |
| Consumer Groups | ❌ No | ✅ Yes |
| Latency | ~1ms (ultra-low) | ~5–10ms |
| Throughput | High | Very High |
| Use Case | Real-time notifications, invalidation | Event streaming, audit log, retries |

### 🏗️ Recommended Pattern: Kafka + Redis Together

```
Transaction Service → Kafka Topic (transactions) → Notification Service
                                                          ↓
                                                    Cache invalidation via Redis
```

```java
// Kafka consumer that also invalidates Redis cache
@KafkaListener(topics = "account.updated", groupId = "cache-invalidator")
public void onAccountUpdated(AccountUpdatedEvent event) {
    // Evict stale cache after DB update via Kafka event
    redisTemplate.delete("accounts::" + event.getAccountId());
    log.info("Cache evicted for account: {}", event.getAccountId());
}
```

---

## 11. Redis in Microservices Architecture

### 🏗️ CitiCore Banking Platform — Redis Usage Pattern

```
┌─────────────────────────────────────────────────────────────┐
│                    API Gateway                               │
│              (Rate Limiting via Redis INCR)                  │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        ↓             ↓             ↓
   Auth Service   Account Svc   Transaction Svc
  (JWT blacklist) (Account     (Distributed Lock
  (Session Store)  Cache)       prevents double-spend)
        │             │             │
        └─────────────┴─────────────┘
                      │
               ┌──────▼──────┐
               │    REDIS     │
               │  Cluster     │
               └─────────────┘
```

### 🔒 JWT Blacklisting (Logout invalidation)

```java
@Service
public class JwtBlacklistService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // On logout, blacklist the token until its natural expiry
    public void blacklistToken(String token, long expiryMs) {
        redisTemplate.opsForValue()
            .set("blacklist:" + token, "revoked",
                 Duration.ofMillis(expiryMs));
    }

    public boolean isBlacklisted(String token) {
        return redisTemplate.hasKey("blacklist:" + token);
    }
}

// In JWT Filter
@Component
public class JwtAuthFilter extends OncePerRequestFilter {

    @Autowired private JwtBlacklistService blacklistService;

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String token = extractToken(request);

        if (token != null && blacklistService.isBlacklisted(token)) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Token revoked");
            return;
        }
        chain.doFilter(request, response);
    }
}
```

### ⚡ Rate Limiting with Redis

```java
@Component
public class RateLimitFilter extends OncePerRequestFilter {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final int MAX_REQUESTS = 100; // per minute
    private static final Duration WINDOW = Duration.ofMinutes(1);

    @Override
    protected void doFilterInternal(HttpServletRequest req,
                                    HttpServletResponse res,
                                    FilterChain chain) throws ServletException, IOException {
        String clientIp = req.getRemoteAddr();
        String key = "rate:" + clientIp;

        Long count = redisTemplate.opsForValue().increment(key);
        if (count == 1) {
            redisTemplate.expire(key, WINDOW); // Set window on first request
        }

        if (count > MAX_REQUESTS) {
            res.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            res.getWriter().write("Rate limit exceeded. Try again later.");
            return;
        }
        chain.doFilter(req, res);
    }
}
```

---

## 12. Common Interview Questions & Answers

---

### Q1: What is the difference between `@Cacheable` and `@CachePut`?

**Answer:**
- `@Cacheable` — **checks cache first**. If cache hit → returns cached value (method NOT executed). If miss → executes method, stores result.
- `@CachePut` — **always executes the method** AND updates the cache. Used on update operations to keep cache fresh.

```java
@Cacheable("users")  // Skip method if cache has it
public User getUser(Long id) { return repo.findById(id).get(); }

@CachePut("users", key="#user.id")  // Always runs, always updates
public User updateUser(User user) { return repo.save(user); }
```

---

### Q2: What is the difference between Lettuce and Jedis?

**Answer:**
| | Lettuce | Jedis |
|---|---|---|
| Concurrency | Single shared connection (non-blocking, async via Netty) | One thread → one connection (pool needed) |
| Reactive support | ✅ Yes | ❌ No |
| Default in Spring Boot | ✅ Yes | ❌ No |
| Best for | High-concurrency, reactive apps | Simple synchronous use |

---

### Q3: How do you handle cache stampede (thundering herd)?

**Answer:** Cache stampede happens when a popular key expires and hundreds of threads simultaneously miss cache and hit the DB.

**Solutions:**
1. **Distributed Lock on miss** — Only one thread fetches from DB; others wait.
2. **Soft TTL** — Set a "refresh before expiry" flag; background thread refreshes before actual expiry.
3. **Jitter TTL** — Add randomness to TTL (`baseExpiry + random(0, 60s)`) to spread out expirations.

```java
// Jitter example
long jitter = ThreadLocalRandom.current().nextLong(0, 60);
redisTemplate.expire(key, Duration.ofSeconds(300 + jitter));
```

---

### Q4: How does Redis ensure atomicity?

**Answer:** Redis is **single-threaded** for command execution — every command is processed one at a time, so single commands are inherently atomic. For **multi-step atomic operations**, Redis provides:

1. **Transactions (`MULTI/EXEC`)** — Queue commands, execute all at once.
2. **Lua scripts** — Executed atomically server-side.
3. **Atomic commands** — `INCR`, `SETNX`, `GETSET` are single atomic operations.

```java
// Atomic conditional set using Lua script via Spring
redisTemplate.execute(new DefaultRedisScript<>(
    "if redis.call('get', KEYS[1]) == ARGV[1] then " +
    "return redis.call('del', KEYS[1]) else return 0 end",
    Long.class
), List.of(lockKey), List.of(ownerId));
```

---

### Q5: What is the difference between RDB and AOF persistence?

**Answer:**
| | RDB (Snapshot) | AOF (Append Only File) |
|---|---|---|
| How | Periodic full snapshots to disk | Logs every write command |
| Recovery speed | Fast (load one snapshot) | Slow (replay all commands) |
| Data loss on crash | Up to last snapshot (minutes) | Near zero (fsync every second) |
| Disk usage | Small | Large |
| Default | ✅ Yes | ❌ No |

> Production recommendation: Use **both** — RDB for fast restarts, AOF for durability.

---

### Q6: What happens if Redis goes down in your Spring Boot app?

**Answer:**
- `@Cacheable` methods **fall through to the DB** (cache miss behavior).
- The app should **not crash** — configure `spring.cache.redis.cache-null-values=false` and handle Redis connection errors gracefully.
- Use **Resilience4j CircuitBreaker** to wrap Redis calls and fall back to DB when Redis is down.

```java
@Cacheable(value = "accounts", key = "#id")
@CircuitBreaker(name = "redis", fallbackMethod = "getFromDb")
public Account getAccount(Long id) {
    return accountRepository.findById(id).orElseThrow();
}

public Account getFromDb(Long id, Exception ex) {
    log.warn("Redis down, falling back to DB: {}", ex.getMessage());
    return accountRepository.findById(id).orElseThrow();
}
```

---

### Q7: When would you NOT use Redis for caching?

**Answer:**
- Data that **changes very frequently** (every second) — cache would be stale too often.
- **Large objects** (> 100KB) — RAM is expensive; put in S3 + store only URL in Redis.
- Data that **must always be consistent** (financial balances during active transactions).
- When the **cache key cardinality is too high** (e.g., every unique combination of filters) — memory explodes.

---

### Q8: Explain your Redis architecture in CitiCore Banking Platform.

**Sample Answer:**
> "In CitiCore, we use Redis in three ways. First, as a **cache** for the Account and User services — we use `@Cacheable` with a 10-minute TTL to reduce PostgreSQL load. Second, as a **JWT blacklist** in Auth Service — when a user logs out, we store the token hash in Redis until its natural JWT expiry so it can't be reused. Third, for **distributed locking** in the Transaction Service — when processing a payment, we use `SETNX` to acquire a lock on the account ID so two concurrent requests can't create duplicate transactions. We deploy Redis on AWS ElastiCache with `allkeys-lru` eviction policy and Multi-AZ for high availability."

---

## 🔑 Quick Reference Cheat Sheet

```bash
# Key operations
SET key value
GET key
DEL key
EXISTS key
EXPIRE key 3600        # TTL in seconds
TTL key                # Get remaining TTL
KEYS pattern*          # List keys (avoid in prod!)
SCAN 0 MATCH "user:*" COUNT 100  # Safe key iteration

# String
INCR counter           # Atomic increment
INCRBY counter 5
SETEX key 300 value    # Set with TTL atomically

# Hash
HSET key field value
HGET key field
HGETALL key
HDEL key field

# List
LPUSH key val          # Add to left
RPUSH key val          # Add to right
LRANGE key 0 -1        # Get all
LLEN key               # Length

# Set
SADD key member
SMEMBERS key
SISMEMBER key member
SREM key member

# ZSet (Sorted Set)
ZADD key score member
ZRANGE key 0 -1 WITHSCORES
ZREVRANGE key 0 9      # Top 10
ZSCORE key member

# Server
INFO memory            # Memory stats
MONITOR                # Live command stream (debug only)
FLUSHDB                # Clear current DB (DANGER)
```

---

## 📖 Resources

- [Spring Data Redis Docs](https://docs.spring.io/spring-data/redis/docs/current/reference/html/)
- [Redis Official Docs](https://redis.io/docs/)
- [Spring Cache Abstraction](https://docs.spring.io/spring-framework/docs/current/reference/html/integration.html#cache)
- [Redisson — Redis Java client for distributed locks](https://github.com/redisson/redisson)

---

*Prepared for 3 YOE level — Covers caching, session, pub/sub, distributed locks, and microservices patterns.*
