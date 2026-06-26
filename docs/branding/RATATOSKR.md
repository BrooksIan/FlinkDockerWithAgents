# Ratatoskr — name, myth, and icon

Ratatoskr is the project name for this Flink Agents platform (CLI, Control API, Designer, Studio).

## Mythology

**Ratatoskr** (Old Norse *Ratatoskr*, often glossed as “drill-tooth” or “bore-tooth” from *rata* “to gnaw/bore” + *tönn* “tooth”) is a squirrel in Norse myth that lives on **Yggdrasil**, the world ash tree.

### Primary sources

| Source | What it says |
|--------|----------------|
| **Poetic Edda**, *Grímnismál* stanza 32 | Odin (as Grímnir) names the squirrel and describes its job: run on Yggdrasil’s ash and carry the eagle’s words from above to **Níðhöggr** below. |
| **Prose Edda**, *Gylfaginning* ch. 16 | Same scene, but Snorri adds that Ratatoskr carries **öfundarorð** — envious, spiteful, or slanderous words — and thereby provokes conflict between the eagle and the serpent/dragon at the roots. |

The Poetic Edda stanza (Bellows translation):

> Ratatosk is the squirrel who there shall run  
> On the ash-tree Yggdrasil;  
> From above the words of the eagle he bears,  
> And tells them to Nithhogg beneath.

### Cosmic layout on Yggdrasil

```
        🦅 Eagle (top; knows many things)
              │
         Vedrfolnir (hawk between eagle’s eyes)
              │
    ━━━━━━━━━●━━━━━━━━━  ← Ratatoskr runs the trunk
              │
         🌳 Yggdrasil
         (three roots:
          Ásgard, Jötunheim, Hel/Níðhöggr)
              │
        🐉 Níðhöggr (gnaws the root)
```

Scholars disagree on how much weight to give the squirrel: Rudolf Simek treated him as a decorative detail on the world-ash; others (e.g. Hilda Ellis Davidson) read the gnawing squirrel as part of Yggdrasil’s cycle of decay and renewal — existence as constant change.

## Why this name fits the project

| Myth | Platform |
|------|----------|
| Squirrel runs up and down the world tree | Events and records flow through **pipelines** (sources → agents → sinks) |
| Messenger between top and bottom of the cosmos | **Kafka** topics, Flink streams, and the Control API bridge UI, cluster, and agents |
| Small creature, structurally important | Lightweight CLI + dashboard orchestrating heavy Flink cluster workloads |
| Carries words between distant endpoints | **Agents** transform and relay payloads; Designer publishes definitions into the runtime |
| Perpetual motion on the tree | **Streaming** jobs and session windows — always-on processing on Yggdrasil-like infrastructure |

The Snorri gloss (malicious gossip) is *not* the product metaphor — the Poetic Edda’s neutral messenger role matches event-driven systems better.

## Icon design language

Assets live in [`dashboard/public/`](../dashboard/public/):

- **`ratatoskr-icon.svg`** — full mark (tree + squirrel + event dots)
- **`favicon.svg`** — simplified mark for small sizes

Visual elements:

1. **Ash trunk** — vertical stem with three root lines (Yggdrasil’s three roots).
2. **Squirrel** — amber/rust body on the trunk; curved tail (recognizable at 16–32 px).
3. **Event dots** — gold and blue beads on the trunk (messages in flight; aligns with dashboard `--warn` / `--accent`).
4. **Palette** — Norway flag colors: red squirrel `#BA0C2F`, blue accents `#00205B`, white fimbriation `#FFFFFF` on trunk and event dots; dark dashboard background retained for contrast.

## Usage

- Dashboard favicon and sidebar brand mark
- README and docs headers
- Future: CLI `--version` banner, Docker label icon, GitHub social preview

## References

- [Ratatoskr — Wikipedia](https://en.wikipedia.org/wiki/Ratatosk)
- [Grímnismál — Open Book Publishers (Poetic Edda)](https://books.openbookpublishers.com/10.11647/obp.0308/ch4.xhtml)
- Snorri Sturluson, *Edda*, *Gylfaginning* (Prose Edda)
