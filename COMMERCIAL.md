# Commercial Licensing

Synthigy is **dual-licensed**. This file describes the second option; the
first is the AGPL in [LICENSE](LICENSE).

## 1. The open source license — AGPL-3.0

Synthigy is free software under the GNU Affero General Public License,
version 3. It is genuinely open source: OSI-approved, no revenue threshold,
no field-of-use restriction, no "free until you get big" clause.

Under the AGPL you may use, study, modify, self-host and redistribute
Synthigy, commercially or not, for any purpose. What the AGPL asks in return
is reciprocity:

- If you distribute Synthigy or a modified version, you pass on the same
  freedoms and provide the corresponding source.
- **Section 13** is the part that matters here: if you let users interact
  with a modified Synthigy **over a network**, those users must be able to
  get its source — even though you never shipped them a binary.

If you self-host Synthigy and your modifications stay yours, or you publish
them, the AGPL costs you nothing. That is the intended case, and it is most
of them.

## 2. The commercial license

A commercial license removes the AGPL's reciprocity obligations. You need one
when you want to keep your own source closed while building on Synthigy:

- **Embedding** Synthigy in a proprietary product you distribute or sell;
- **Offering** Synthigy, or a service built on it, to your users over a
  network without releasing your modifications and surrounding source
  (AGPL §13) — this includes hosting or managed-service offerings by cloud
  and platform providers;
- **White-labelling** or reselling Synthigy under your own name;
- Any case where your legal team will not accept AGPL obligations.

It also comes with what the AGPL cannot give you: a warranty, support terms,
and an indemnity.

Contact: **r.gersak@gmail.com**

## 3. Why dual licensing, plainly

The AGPL is the honest way to say *this is really free, and it stays free*.
Self-hosters, individuals, academics, startups and enterprises alike get the
whole engine with no threshold and no asterisk.

It is also the reason a hyperscaler cannot quietly build a managed Synthigy
service on our work and keep their improvements private. They can — the AGPL
permits it — but §13 requires them to give that work back. If they would
rather not, the commercial license is the door, and that is the trade this
project is funded by.

No feature is withheld from the AGPL build to force an upgrade. The engine is
the engine.

## 4. What is separate

The engine in this repository is AGPL. Two things around it are not, and are
licensed separately:

- **The Synthigy portal** — the CLI, supervisor daemon, operator console,
  provisioning, and release/upgrade machinery. This is the operational plane
  for running Synthigy as a product, and it is commercially licensed.
- **Client SDKs** — Clojure, Go, JavaScript, Python and PHP — are **MIT**.
  They are libraries you embed in your own application, and calling a
  Synthigy server over HTTP does not make your application a derivative
  work of the engine. Use them freely, in anything.

## 5. Contributions

Contributions are accepted under a CLA ([CLA.md](CLA.md)) that grants the
right to license contributed code under both the AGPL and commercial terms.
Without it, contributed code could not appear in a commercially licensed
build, and the project would fragment into parts that can be dual-licensed
and parts that cannot. You keep your copyright.
