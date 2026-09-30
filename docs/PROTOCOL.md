# Specificația protocolului

## Transport și încadrare

TCP, port implicit 5000. Fiecare mesaj este un cadru:

```
+-------------------------+----------------------------------+
| lungime: 4 octeți, u32  | corp: UTF-8, JSON sau XML        |
| big-endian              | (max. 1 MiB)                     |
+-------------------------+----------------------------------+
```

Formatul corpului se detectează după primul caracter (ignorând spațiile și BOM): `{` înseamnă JSON, `<` înseamnă XML.

## Modelul mesajului

Un mesaj este o listă plată de câmpuri, toate de tip text. Același mesaj în ambele formate:

```json
{"type": "publish", "id": "5c1e…", "topic": "news.sport", "payload": "Gol!", "checksum": "a3f1…"}
```
```xml
<message><type>publish</type><id>5c1e…</id><topic>news.sport</topic><payload>Gol!</payload><checksum>a3f1…</checksum></message>
```

| Câmp | Semnificație |
|---|---|
| `type` | tipul comenzii (vezi mai jos) |
| `id` | identificator unic al mesajului (UUID); folosit la corelare, ACK și deduplicare |
| `ref` | în răspunsuri: `id`-ul mesajului la care se răspunde |
| `sender` | numele clientului (la `connect`) sau al expeditorului (la `deliver`) |
| `topic` | subiectul (canalul); la abonare poate conține `*` și `#` |
| `payload` | conținutul util, transportat neschimbat |
| `checksum` | SHA-256 hex al lui `payload` (opțional la publicare; brokerul îl completează) |
| `format` | la `connect`: formatul dorit pentru livrări; la `deliver`: formatul în care a fost publicat |
| `role` | la `connect`: `sender`, `receiver` sau alt text informativ |
| `attempt` | la `deliver`: a câta încercare de livrare |
| `error` | la `error`: descrierea problemei |
| `timestamp` | ISO-8601, UTC |

## Comenzi client → broker

| `type` | Câmpuri obligatorii | Răspuns |
|---|---|---|
| `connect` | `sender` | `connack` (`payload` = JSON cu abonările restaurate și numărul de mesaje în așteptare) |
| `subscribe` | `topic` (tipar) | `suback` (`payload` = câte mesaje au venit din backlog) |
| `unsubscribe` | `topic` | `unsuback` |
| `publish` | `topic`, `payload` | `puback` (`payload` = `routed:N`, `backlog` sau `duplicate`) |
| `ack` | `ref` | — |
| `stats` | — | `stats` (`payload` = JSON cu starea brokerului) |
| `ping` | — | `pong` |

`connect` trebuie să fie primul mesaj. Orice altă comandă înainte de el primește `error`.

## Mesaje broker → client

- `deliver`: un mesaj publicat pe un subiect la care clientul este abonat. Clientul trebuie să trimită `ack` cu `ref` = `id`.
- `error`: comanda precedentă a fost respinsă; conexiunea rămâne deschisă.

## Reguli pentru nume

- nume client: `[A-Za-z0-9_.-]{1,64}`
- subiect de publicare: segmente `[A-Za-z0-9_-]+` separate prin `.` (ex. `news.sport`)
- tipar de abonare: ca mai sus, iar un segment poate fi `*` (exact un segment) sau `#` (toate segmentele rămase)

## Exemplu de sesiune

```
C → {"type":"connect","sender":"go-recv","format":"xml","id":"1"}
B → <message><type>connack</type><ref>1</ref>…</message>
C → <message><type>subscribe</type><topic>news.*</topic><id>2</id></message>
B → <message><type>suback</type><ref>2</ref><topic>news.*</topic><payload>0</payload></message>
            … alt client publică pe news.sport în JSON …
B → <message><type>deliver</type><id>9f…</id><topic>news.sport</topic><sender>web-sender</sender>
    <payload>Gol!</payload><checksum>…</checksum><format>json</format><attempt>1</attempt></message>
C → <message><type>ack</type><ref>9f…</ref></message>
```
