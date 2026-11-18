# Event Driven Architecture — Kafka & Spring Cloud Stream

TP 3 (Prof. Mohamed YOUSSFI, ENSET Mohammedia) : producer, consumer, supplier et analytics temps réel avec Kafka Streams.

## Stack
- Java 21, Spring Boot 4.0.9, Spring Cloud 2025.1.x
- Spring Cloud Stream (binders Kafka et Kafka Streams)
- Kafka + Zookeeper avec Docker Compose
- Smoothie Charts (page web temps réel)

## Lancer
```bash
docker compose up -d
./mvnw spring-boot:run
```

## Flux
| Élément | Rôle | Topic |
|---|---|---|
| `GET /publish?name=P1&topic=T1` | Producer REST (StreamBridge) | au choix |
| `pageEventConsumer` | Consumer | `T1` |
| `pageEventSupplier` | Supplier (toutes les 200 ms) | `T6` |
| `kStream` | Visites par page sur une fenêtre de 5 s | `T6` → `T7` |
| `GET /analytics` | Stats en SSE depuis `count-store` | — |
| `http://localhost:8080` | Courbes temps réel | — |

## Auteur
Yousef ELHAID