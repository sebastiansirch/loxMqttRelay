# Lasttest & Fix: Nachrichtenverlust unter Last (HTTP-Forwarding an den Miniserver)

Branch: `fix/miniserver-http-session-reuse-and-basetopic-warning`

## Ausgangsfrage

Prüfen, ob loxMqttRelay unter höherer Last zuverlässig alle Nachrichten von MQTT
an den Loxone Miniserver (bzw. über UDP an MQTT) weiterleitet, ohne welche zu
verlieren. Dazu wurde ein Docker-basierter Lasttest-Aufbau erstellt (mosquitto
+ Miniserver-Mock + Relay) und gegen den echten Relay-Code gefahren.

Ergebnis: **Nein** — unter Last gingen tatsächlich Nachrichten verloren, und
zwar aus einem konkreten, reproduzierbaren Grund (Bug 1). Beim Aufbau des
Testharnischs wurde zusätzlich ein zweites, unabhängiges Problem entdeckt
(Bug 2), das ebenfalls zu stillem Nachrichtenverlust führen kann.

---

## Bug 1: Neue TCP-Verbindung pro HTTP-Request → Port-Exhaustion → Verlust

### Symptom

Bei anhaltender Last (reproduzierbar ab ca. 25.000–30.000 Nachrichten in
kurzer Zeit) wurden Nachrichten, die eigentlich per HTTP an den Miniserver
weitergeleitet werden sollten, **nicht** beim Miniserver empfangen — ohne
dass der Absender (MQTT-Publisher) davon etwas mitbekam. Gemessene Verlustrate
bei 30.000 Nachrichten: 5,9–17,4 % (je nach Anzahl paralleler Topics/Timing).

Im Relay-Log erschienen parallel dazu Fehler wie:

```
ERROR [loxmqttrelay.http_miniserver_handler] Error 503: Connection error sending
loadtest/cmd/sensor140 (as loadtest_cmd_sensor140)=s3-29940 to Miniserver
(URL: http://miniserver-mock:8080/dev/sps/io/loadtest_cmd_sensor140/s3-29940):
Cannot assign requested address
```

`Cannot assign requested address` ist `EADDRNOTAVAIL` — das Betriebssystem hat
keine freien lokalen (Ephemeral-)Ports mehr, um eine neue ausgehende
TCP-Verbindung zu öffnen.

### Root Cause

`src/loxmqttrelay/http_miniserver_handler.py`, Methode
`send_to_miniserver_via_http`, öffnete **für jede einzelne Nachricht** eine
neue `aiohttp.ClientSession`:

```python
async def send_to_miniserver_via_http(self, topic, normalized_topic, value):
    ...
    async with aiohttp.ClientSession(auth=self.auth, timeout=self.timeout) as session:
        ...
        async with self.connection_semaphore:
            async with session.get(url) as resp:
                ...
```

Eine `aiohttp.ClientSession` besitzt ihren eigenen Connection-Pool
(`TCPConnector`). Wird pro Request eine neue Session erzeugt und am Ende des
`async with`-Blocks sofort wieder geschlossen, wird **keine** Verbindung
wiederverwendet — jede einzelne HTTP-Anfrage baut eine komplett neue
TCP-Verbindung auf und reißt sie danach wieder ab. Die dabei entstehenden
Sockets bleiben (wie bei TCP üblich) für eine gewisse Zeit im `TIME_WAIT`-
Zustand hängen (Linux-Default ca. 60 s) und belegen in dieser Zeit weiterhin
einen lokalen Port. Bei hoher Nachrichtenrate wird der verfügbare
Ephemeral-Port-Bereich schneller aufgebraucht, als Ports durch Ablauf von
`TIME_WAIT` wieder frei werden → `EADDRNOTAVAIL`.

Der `miniserver_max_parallel_connections`-Semaphore begrenzt zwar, wie viele
Requests *gleichzeitig* in Flug sind, verhindert aber nicht die
Verbindungs-Churn über die Zeit — bei genügend Gesamtvolumen tritt das Problem
unabhängig vom Semaphore-Wert irgendwann auf.

Entscheidend für den tatsächlichen Datenverlust: In den `except`-Zweigen von
`send_to_miniserver_via_http` gibt es **kein Retry** — ein Fehler wird nur
geloggt (`logger.error(...)`) und die Funktion kehrt mit `return` (implizit
`None`) zurück. Ein fehlgeschlagener Request ist damit endgültig verloren,
ohne dass der MQTT-Publisher oder irgendein anderer Teil des Systems davon
erfährt.

### Fix

`HttpMiniserverHandler` erzeugt jetzt **eine** `aiohttp.ClientSession` lazy
beim ersten Request und hält sie für die Lebensdauer des Prozesses offen
(`_get_session()`, abgesichert mit einem `asyncio.Lock` gegen doppelte
Erzeugung bei parallelen ersten Requests). Alle weiteren Requests nutzen
dieselbe Session und profitieren damit von aiohttps eingebautem
Keep-Alive-Connection-Pool — TCP-Verbindungen werden wiederverwendet statt bei
jeder Nachricht neu aufgebaut zu werden. Der bestehende Semaphore zur
Begrenzung paralleler Requests bleibt unverändert erhalten.

```python
async def _get_session(self) -> aiohttp.ClientSession:
    if self._session is None or self._session.closed:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(auth=self.auth, timeout=self.timeout)
    return self._session
```

### Verifikation

Mit demselben Lasttest-Setup, das den Bug ursprünglich aufgedeckt hat:

| Lauf | Nachrichten | Verlust vorher | Verlust nachher |
|---|---|---|---|
| Szenario 2 (MQTT → HTTP) | 30.000 | 5,9 % – 17,4 % (2x reproduziert) | 0 % |
| Szenario 2 (MQTT → HTTP) | 50.000 | — | 0 % |
| Szenario 2, direkt danach nochmal (kumulativ 150.000 in derselben Session) | 50.000 | — | 0 % |

Relay-Log nach dem Fix: `grep -c "Cannot assign requested address"` → `0`,
`grep -c "ERROR"` → `0` über den gesamten Testlauf.

---

## Bug 2: `base_topic`-Überlappung führt zu stillem Nachrichtenverlust

### Symptom

Beim ersten Aufbau des Lasttests mit `base_topic = "loadtest/"` und
`subscriptions = ["loadtest/cmd/#"]` kamen **0 von 1000** Testnachrichten
beim Miniserver-Mock an — ganz ohne Last, ohne jede Fehlermeldung im Log.

### Root Cause

Der Rust-Message-Dispatcher (`src/lib.rs`, `handle_mqtt_message`, Zeile 461)
behandelt jede empfangene Nachricht, deren Topic mit `base_topic` beginnt,
als reservierte Steuer-Nachricht:

```rust
if topic.starts_with(&self.base_topic) {
    if topic == topics.miniserver_startup_topic { ... }
    else if topic == topics.config_get_topic { ... }
    else if topic == topics.config_set_topic || ... { ... }
    else if topic == topics.config_update_topic || topic == topics.config_restart_topic { ... }
    // kein else-Zweig — passt keine der obigen Bedingungen, passiert schlicht nichts
}
else {
    let _ = self.process_data(py, &topic, &message);  // normale Datennachricht -> Miniserver
}
```

Ist `base_topic` ein Präfix eines Daten-Subscription-Topics (z. B.
`base_topic="loadtest/"` und Subscription `"loadtest/cmd/#"`), landen alle
Nachrichten auf diesem Topic im `if`-Zweig, matchen dort aber keinen der
bekannten Steuer-Topics — es passiert **nichts**: keine Weiterleitung, kein
Log, kein Fehler. Der mitgelieferte `default_config.toml` vermeidet das per
Konvention (`base_topic="test/"` vs. `subscriptions=["topic3"]`), aber nichts
im Code verhindert oder meldet eine Fehlkonfiguration.

### Fix

`src/loxmqttrelay/main.py`: neue Funktion `warn_on_base_topic_overlap()`,
aufgerufen beim Start in `connect_and_subscribe_mqtt()`, bevor die
Subscriptions beim Broker angemeldet werden. Sie prüft für jede Subscription,
ob deren fixer (nicht-Wildcard-)Präfix in den `base_topic`-Namensraum fällt
(oder umgekehrt), und loggt in dem Fall eine deutliche Warnung mit
Erklärung. Das Verhalten selbst (reservierter Namensraum für Steuer-Topics)
wurde bewusst **nicht** geändert, um keine stillschweigende
Verhaltensänderung für bestehende Deployments einzuführen — die Warnung macht
die Fehlkonfiguration nur sichtbar.

---

## Lasttest-Harness (`loadtest/`)

### Architektur

```
UDP-Client → [loxmqttrelay] → MQTT publish → [mosquitto]
                                                   │
                                          MQTT subscribe
                                                   ▼
                                            [loxmqttrelay]
                                                   │
                                              HTTP GET
                                                   ▼
                                          [miniserver-mock]
```

Komponenten (`loadtest/docker-compose.yml`):

- **mosquitto** — echter MQTT-Broker (`eclipse-mosquitto:2`)
- **loxmqttrelay** — aus dem echten Projekt-`Dockerfile` gebaut, keine Mocks im Relay selbst
- **miniserver-mock** — eigener aiohttp-Server (`loadtest/miniserver_mock/app.py`), bildet die
  Loxone-Endpunkte (`/dev/sps/io/{topic}/{value}`) nach und protokolliert jeden eingehenden
  Request (Topic, Wert, Zeitstempel); kann künstliche Latenz (`MOCK_DELAY_MS`) und Fehlerquote
  (`MOCK_FAIL_RATE`) simulieren
- **loadtest-runner** — erzeugt Last und vergleicht gesendete mit empfangenen Nachrichten
  (eindeutige IDs pro Nachricht, Mengendifferenz = Verlust)

### Szenarien

1. **`udp_to_mqtt`** — UDP-Eingang des Relays isoliert getestet (Topic außerhalb der
   Relay-Subscriptions, damit nur der UDP→MQTT-Pfad gemessen wird)
2. **`mqtt_to_http`** — Forwarding-Pfad Richtung Miniserver isoliert getestet (Nachrichten werden
   direkt am Broker publiziert, nicht über UDP)
3. **`e2e_udp_to_http`** — volle Kette wie bei einem realen Gerät: UDP → Relay → MQTT → Relay →
   HTTP → Miniserver

Für jedes Szenario wird eine eindeutige ID pro Nachricht vergeben und am Ende die Menge der
gesendeten mit der Menge der tatsächlich empfangenen IDs verglichen. Der Runner beendet sich mit
Exit-Code `0`, wenn kein Verlust aufgetreten ist, sonst `1`.

### Ausführen

```bash
cd loadtest
docker compose build
docker compose up -d mosquitto miniserver-mock loxmqttrelay
docker compose run --rm loadtest-runner
```

Konfigurierbar über Umgebungsvariablen: `NUM_MESSAGES` (Default 5000), `NUM_TOPICS` (Default 50),
`SCENARIOS` (Default `1,2,3`). Details, inkl. Simulation eines langsamen/unzuverlässigen
Miniservers über `MOCK_DELAY_MS`/`MOCK_FAIL_RATE`, in `loadtest/README.md`.

### Testergebnisse im Überblick

| Szenario | Nachrichten | Verlust (vor Fix) | Verlust (nach Fix) |
|---|---|---|---|
| `udp_to_mqtt` | 1.000 – 50.000 | 0 % (nie betroffen) | 0 % |
| `mqtt_to_http` | 1.000 – 20.000 | 0 % | 0 % |
| `mqtt_to_http` | 30.000 | 5,9 % – 17,4 % | 0 % |
| `mqtt_to_http` | 50.000 (2x hintereinander, 150.000 gesamt) | nicht getestet | 0 % |
| `e2e_udp_to_http` | 30.000 | 17,4 % | 0 % |
| `e2e_udp_to_http` | 50.000 | nicht getestet | 0 % |

Der `udp_to_mqtt`-Pfad (reiner UDP-Eingang → MQTT-Publish) war zu keinem Zeitpunkt betroffen — der
Verlust trat ausschließlich auf dem HTTP-Forwarding-Pfad zum Miniserver auf, konsistent mit Bug 1.

---

## Pull Request Description

> Der folgende Abschnitt kann unverändert als PR-Beschreibung verwendet werden.

### Summary
- Lasttest-Setup (`loadtest/`) hinzugefügt: mosquitto + Miniserver-HTTP-Mock + Relay in Docker
  Compose, mit drei Szenarien (UDP→MQTT, MQTT→HTTP, End-to-End), die per eindeutiger
  Nachrichten-ID gesendete gegen empfangene Nachrichten abgleichen.
- Bug gefunden und gefixt: `send_to_miniserver_via_http` öffnete pro Nachricht eine neue
  `aiohttp.ClientSession` (= neue TCP-Verbindung) statt sie wiederzuverwenden; unter Last
  (~25k+ Nachrichten) führte das zu Port-Exhaustion (`EADDRNOTAVAIL`) und da es keinen Retry
  gibt, zu dauerhaftem, stillem Nachrichtenverlust (5,9–17,4 % bei 30k Nachrichten gemessen).
  Fix: eine wiederverwendete, lazy erzeugte `ClientSession` pro Handler-Instanz.
- Zweiten Bug gefunden und gefixt: Nachrichten auf Subscriptions, die im `base_topic`-Namensraum
  liegen, wurden vom Rust-Dispatcher stillschweigend verworfen (als nicht erkannter
  Steuer-Topic), ohne Log oder Fehler. Fix: Startup-Warnung, wenn eine Subscription mit
  `base_topic` überlappt (Verhalten selbst bewusst unverändert gelassen, um keine
  Breaking Changes für bestehende Deployments einzuführen).

### Warum
Es sollte geprüft werden, ob loxMqttRelay unter höherer Last zuverlässig alle Requests an den
Miniserver weiterleitet. Antwort vor diesem Fix: nein — reproduzierbarer Nachrichtenverlust ab
ca. 25.000–30.000 Nachrichten in kurzer Zeit, verursacht durch fehlende HTTP-Connection-Reuse.

### Änderungen
- `src/loxmqttrelay/http_miniserver_handler.py`: geteilte, wiederverwendete `ClientSession`
  statt einer neuen Session pro Request.
- `src/loxmqttrelay/main.py`: `warn_on_base_topic_overlap()` beim Verbindungsaufbau.
- `tests/test_http_miniserver_handler.py`, `tests/test_mqtt_relay.py`: Tests für beide Fixes.
- `loadtest/`: neuer, eigenständiger Docker-Compose-Lasttest-Aufbau (nicht Teil der
  regulären Test-Suite/CI, manuell auszuführen).

### Test Plan
- [x] `pytest` (204 bestehende + 6 neue Tests) grün, ausgeführt im Docker-Build-Image (Projekt
      benötigt Python 3.14 + kompilierte Rust-Extension).
- [x] Lasttest vor dem Fix: `mqtt_to_http`/`e2e_udp_to_http` bei 30.000 Nachrichten zeigen
      5,9–17,4 % Verlust, `Cannot assign requested address`-Fehler im Relay-Log.
- [x] Lasttest nach dem Fix: 50.000 Nachrichten, zweimal hintereinander ohne Neustart
      (150.000 gesamt), 0 % Verlust in allen drei Szenarien; keine Fehler im Relay-Log.
- [x] `udp_to_mqtt`-Szenario als Kontrolle: zu keinem Zeitpunkt Verlust (bestätigt, dass das
      Problem spezifisch am HTTP-Forwarding-Pfad lag).

### Hinweise für Reviewer
- Der `base_topic`-Fix ändert **kein** Laufzeitverhalten, nur Sichtbarkeit (Log-Warnung) — bewusst
  minimal-invasiv, da eine Verhaltensänderung (z. B. überlappende Topics doch weiterleiten)
  bestehende Deployments überraschen könnte.
- `loadtest/` ist ein eigenständiger, manuell auszuführender Aufbau (eigenes `docker-compose.yml`),
  nicht in die reguläre CI/Test-Suite eingebunden.
