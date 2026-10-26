# Commercial Licensing

Synthigy is licensed under the **Business Source License 1.1** ([LICENSE](LICENSE)),
and is also available under a commercial license. This file describes when
you need the second one.

## 1. Free, under the Business Source License

- **Anyone** may read, copy, modify and redistribute the code, and use it for
  development, testing and evaluation.
- **In production, free** for:
  - individuals, for personal, non-commercial use;
  - non-profit organisations and accredited educational or research
    institutions, at any size;
  - companies whose Total Finances (with affiliates) are **EUR 3M or less** —
    including building and selling your own products on Synthigy.

"Total Finances" is the greater of gross revenue and capital raised (equity
or debt) in the last twelve months.

Each version is governed by the LICENSE it was released with. Synthigy 0.3.1
and earlier carry a EUR 5M threshold, and keep it.

**Building a product on Synthigy is not hosting Synthigy.** When your
customers use your product's own interface and API, you are not offering
Synthigy as a hosted or managed service. Giving them Synthigy itself — its
modeler, deployment, data console, log cockpit, operator console, or direct
access to its `/data` API — is. Neither the free license nor the
Entrepreneur license allows it; it is agreed separately under an Enterprise
license (§3). Using the modeler to build your own apps is always fine.

## 2. When you need a commercial license

- **Your organisation is over EUR 3M.** Banks, telcos, enterprises, public
  bodies of that size.
- **You deploy Synthigy for a client over EUR 3M.** System integrators,
  consultancies and agencies installing, operating or customising Synthigy
  for a specific client — or shipping it to them as part of a product the
  client runs — need a license when that client is over the threshold. An
  integrator over the threshold needs one for any production use.
- **You offer Synthigy itself as a hosted or managed service**, at any size —
  including giving the customers of your product its modeler, deployment,
  consoles or direct `/data` access (§1).
- **OEM / reseller** arrangements, a warranty, support terms, or an indemnity.

Contact: **r.gersak@gmail.com**

## 3. Prices

EUR, excluding VAT, billed annually. Customers billed outside Europe pay
the same amounts in USD.

**Enterprise — you run Synthigy for your own organisation.**

- **EUR 15,000 per production deployment per year.** A production deployment
  is one Synthigy installation serving production traffic under its own root
  URL: single node, PostgreSQL or SQLite. Non-production deployments are free.
- Licensed modules start at their default limits. Higher limits, additional
  nodes, robotics and premium support (SLA, named contact, negotiated terms)
  are priced on top.
- An integrator licenses each deployment it runs for a client over EUR 3M.
- The modeler is included for your own use. Offering it to third parties
  (white-label) is agreed separately.
- A company that grows past EUR 3M while running Synthigy in production pays
  50% for its first year.

**Entrepreneur — you sell an app or service built on Synthigy.**

- Below EUR 3M the free license covers it. Above, the Entrepreneur license
  lets you run your app for as many customers as you like — shared, one
  instance per customer, or installed at theirs; your customers need no
  Synthigy license of their own.
- Your customers use your app. Synthigy's modeler, deployment and consoles
  are not part of it.
- A royalty on your product revenue above the first EUR 3M, as with
  embedded software; a fixed annual fee is available instead. Terms on
  request.

## 4. Every version becomes open source

Four years after a version is first released, that version converts to the
**GNU GPL v2.0 or later, or the Apache License 2.0** at your option, with no
restrictions. Newer versions stay under the Business Source License until
their own date.

## 5. What is separate

- **Client SDKs** — Clojure, Go, JavaScript, Python and PHP — are **MIT**.
- The bundled **web components** (`tooling.js`) are **MIT**.
- **The Synthigy portal** — the CLI, supervisor daemon, operator console,
  provisioning, and release/upgrade machinery — is under the same Business
  Source License and the same free-use terms as Synthigy itself.

## 6. Contributions

Contributions are accepted under a CLA ([CLA.md](CLA.md)) that grants the
right to license contributed code under both the Business Source License and
commercial terms. You keep your copyright.
