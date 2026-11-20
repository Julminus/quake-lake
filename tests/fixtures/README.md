# Recorded responses

Real answers from the two sources, recorded on 2026-11-20 at 18:11 UTC with the
package's own `fetch` and URL builder, and stored byte for byte. Tests replay
them, so CI never touches the network.

| File | Request |
|---|---|
| `usgs-palu-2018-09-28.json` | USGS FDSN query, Indonesia box, 2018-09-28T00:00:00 to 2018-09-29T00:00:00 UTC: the day of the M7.5 Palu earthquake, 81 events |
| `usgs-empty.json` | the same query for one quiet minute (2026-11-19T00:00:00 to 00:01:00): `features` is empty, status 200 |
| `bmkg-autogempa.json` | `https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json`: the latest event (one object) |
| `bmkg-gempaterkini.json` | `.../gempaterkini.json`: the latest 15 events of M5.0+ |
| `bmkg-gempadirasakan.json` | `.../gempadirasakan.json`: the latest 15 felt events |

USGS data is in the public domain. BMKG asks that its data be credited to
BMKG (Badan Meteorologi, Klimatologi, dan Geofisika).
