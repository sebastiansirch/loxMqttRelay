# Lasttest-Setup für loxMqttRelay

Baut eine isolierte Docker-Umgebung mit drei Komponenten auf:

- **mosquitto** – echter MQTT-Broker (eclipse-mosquitto:2)
- **loxmqttrelay** – das Relay selbst, gebaut aus dem Dockerfile im Projekt-Root
- **miniserver-mock** – ein minimaler HTTP-Server, der die Loxone-Miniserver-Endpunkte
  (`/dev/sps/io/{topic}/{value}`) nachbildet und jeden eingehenden Request protokolliert
- **loadtest-runner** – erzeugt Last und prüft, dass jede gesendete Nachricht auch tatsächlich ankommt

```
UDP-Client → [loxmqttrelay] → MQTT publish → [mosquitto] → MQTT subscribe → [loxmqttrelay] → HTTP GET → [miniserver-mock]
```

## Setup starten

```bash
cd loadtest
docker compose build
docker compose up -d mosquitto miniserver-mock loxmqttrelay
```

## Lasttest ausführen

```bash
docker compose run --rm loadtest-runner
```

Konfigurierbar über Umgebungsvariablen (vor dem Befehl setzen oder in `.env`):

| Variable       | Default | Bedeutung                                              |
|----------------|---------|----------------------------------------------------------|
| `NUM_MESSAGES` | 5000    | Anzahl Nachrichten pro Szenario                          |
| `NUM_TOPICS`   | 50      | Anzahl verschiedener Topics, über die verteilt wird      |
| `SCENARIOS`    | 1,2,3   | Welche Szenarien laufen sollen (kommagetrennt)           |

Beispiel für höhere Last:

```bash
NUM_MESSAGES=50000 NUM_TOPICS=200 docker compose run --rm loadtest-runner
```

## Szenarien

1. **`udp_to_mqtt`** – Nachrichten werden per UDP an das Relay geschickt (Topic außerhalb
   des Whitelist-Bereichs), Ankunft wird direkt am MQTT-Broker geprüft. Testet ausschließlich
   den UDP-Eingang des Relays.
2. **`mqtt_to_http`** – Nachrichten werden direkt am Broker publiziert (Relay-Subscription
   `loadtest/cmd/#`), Ankunft wird am `miniserver-mock` per HTTP geprüft. Testet ausschließlich
   den Forwarding-Pfad Richtung Miniserver.
3. **`e2e_udp_to_http`** – volle Kette: UDP → Relay → MQTT → Relay → HTTP → Miniserver-Mock.
   Entspricht dem realen Pfad eines Geräts, das per UDP an das Relay sendet.

Für jedes Szenario wird eine eindeutige ID pro Nachricht vergeben und am Ende die Menge der
gesendeten mit der Menge der empfangenen IDs verglichen (Mengendifferenz = Verlust). Der Runner
beendet sich mit Exit-Code `0`, wenn in keinem Szenario eine Nachricht verloren ging, sonst `1`.

## Realistischere Lastszenarien simulieren

`miniserver-mock` kann künstliche Latenz und Fehlerquote simulieren, um zu prüfen, wie sich das
Relay unter einem langsamen/unzuverlässigen echten Miniserver verhält:

```bash
MOCK_DELAY_MS=200 NUM_MESSAGES=20000 docker compose up -d --build miniserver-mock
docker compose run --rm loadtest-runner
```

`MOCK_FAIL_RATE=0.05` simuliert z. B. 5 % HTTP-5xx-Antworten des Miniservers.

`loadtest/relay-config/config.toml` → `miniserver_max_parallel_connections` steuert, wie viele
HTTP-Requests das Relay parallel an den Miniserver schickt (Default hier: 20; produktiv-Default
im Projekt: 5). Bei aktivierter künstlicher Latenz zeigt sich hier deutlich der Zusammenhang
zwischen Parallelität, Durchsatz und Verlustrate.

## Bekannte Grenzen, die der Lasttest sichtbar macht

- **UDP ist verbindungslos.** Bei sehr hohen Burst-Raten können Pakete bereits auf
  Betriebssystem-/Netzwerkebene verworfen werden, bevor das Relay sie überhaupt sieht – das ist
  inhärentes UDP-Verhalten, kein Relay-Bug. Szenario 1/3 machen das sichtbar; falls Verluste dort
  auftreten, senkt eine niedrigere Senderate (statt Bursts) i. d. R. den Verlust auf 0.
- **Kein Retry beim HTTP-Forwarding.** `http_miniserver_handler.py` loggt Timeouts/Fehler beim
  Senden an den Miniserver nur, versucht es aber nicht erneut. Unter Dauerlast mit langsamem
  Miniserver (hohe `MOCK_DELAY_MS` kombiniert mit niedrigem `miniserver_max_parallel_connections`
  und Timeout von 10s) können dadurch Nachrichten dauerhaft verloren gehen. Szenario 2/3 mit
  gesetztem `MOCK_DELAY_MS`/`MOCK_FAIL_RATE` reproduzieren das gezielt.

## Aufräumen

```bash
docker compose down -v
```
