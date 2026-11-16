# Synthigy documentation

## Tasks

- [Link a record to a file](FILES.md#linking-records-to-files)
- [Upload a file](FILES.md#http)
- [List a folder](FILES.md#listing-a-folder-through-data)
- [Share a folder](FILES.md#access)
- [Give a role access to an entity](ACCESS.md#entity-and-relation-rules)
- [Add a row rule](ACCESS.md#row-rules)
- [Write an XSQL read](XSQL.md#a-query-is-a-shape)
- [Write records with stack or sync](DATA.md#writing)
- [Generate SDK functions](SDKS.md#code-generation)
- [Read the logs of a failed request](DATALINE.md#logs)

## Start

- [Getting started](GETTING_STARTED.md) — install, first instance, first model, first query
- [Glossary](GLOSSARY.md) — the words these pages use

## Concepts

- [Modeling data](MODELING.md) — datasets, entities, attributes, relations, deploying
- [The data API](DATA.md) — reading and writing over `/data`, live data
- [XSQL](XSQL.md) — queries as documents, SQL templates
- [Files](FILES.md) — uploads, downloads, versions, folders, S3 storage

## Access

- [Access control](ACCESS.md) — users, groups, roles, entity, attribute and row rules
- [Sign-in and OAuth](OAUTH.md) — applications, flows, tokens, identity providers
- [Signing users in to your app](SIGN_IN.md) — browser apps and backends, step by step
- [Login connectors](IAM_CONNECTORS.md) — checking credentials against your own directory
- [Custom login pages](LOGIN_PAGES.md) — your own sign-in, device, error and status pages
- [Browser origins](CORS.md) — which web apps may call Synthigy

## Operate

- [Running Synthigy](OPERATIONS.md) — instances, services, upgrades, backups, containers
- [Portal and console](CONSOLE.md) — the portal runs the instance, the console administers it
- [Reverse proxy](PROXY.md) — TLS, client addresses, streaming
- [AWS provisioning](AWS_PROVISIONING.md) — a small EC2 server: swap, apt, a service that survives the OOM killer
- [Encryption](ENCRYPTION.md) — data keys, master key custody, rotation
- [Audit, logs and traffic](DATALINE.md) — history of your data, engine logs
- [Troubleshooting](TROUBLESHOOTING.md) — symptoms, causes, fixes

## Build

- [SDKs](SDKS.md) — five languages, connecting, code generation
- [Embedding](EMBEDDED.md) — your application and the engine in one jar

## Reference

- [Errors](ERRORS.md) — what each refusal means and how to fix it
- [Agent brief](AGENT_BRIEF.md) — the rules an AI agent needs before it writes anything
- [Environment variables](ENV.md)
- [CLI](CLI.md)
