# Access control

Every request to Synthigy runs as a **principal** — a user, or the service
identity of an application — and every read and write is checked against that
principal's roles. There are three layers, each answering a different question:

| Layer | Question | When it says no |
|---|---|---|
| Entity and relation rules | May this principal touch this kind of record at all? | the request fails with `ENTITY_FORBIDDEN` / `FORBIDDEN` |
| Attribute rules | May it read or write this field? | the field is left out of reads; writes to it are refused |
| Row rules | Which records? | the records are simply not there |

## Users, groups and roles

- A **user** signs in with a password, an external identity provider or a
  connector ([IAM_CONNECTORS.md](IAM_CONNECTORS.md)).
- A **service user** is created for every confidential OAuth client; tokens the
  client gets with its own credentials run as it ([OAUTH.md](OAUTH.md)).
- A **group** collects users. **Roles** are given to users directly or to a
  group, and every member then holds them.
- The **SUPERUSER** role passes every check. The portal creates the first
  superuser; keep the list short.

Users, groups and roles are records like any other: manage them in the engine
console (Administration → Users, Groups, Roles) or over `/data`.

## Entity and relation rules

Access control is switched on **per entity**, in the data modeling tool.

> An entity with access control switched off is open to every signed-in
> caller, for every operation. Switch it on for anything that is not meant to
> be shared with all of them.

Once it is on, a role may do on that entity only what it is granted:

| Grant | Allows |
|---|---|
| `create` | insert records |
| `read` | read records |
| `update` | change records |
| `delete` | delete records |
| `browse` | read — for directory-style visibility without other rights |
| `owners` | everything above |

Relations have their own grants — `read`, `write` (link and unlink) and
`delete`. Owning both entities of a relation implies its grants.

Grants are attached to roles, not stored in the model version, so they can be
changed on a deployed model without deploying anything.

## Attribute rules

On an entity with access control on, an attribute can **deny** a role `read`
or `write`. A denied attribute is silently left out of that role's reads — a
query that selects it still succeeds — and a write that sets it is refused
with `ATTRIBUTE_FORBIDDEN`.

Passwords are an example: `User.password` is readable and writable by
superusers only. Users change their own password in the console, under
Profile.

## Row rules

Row rules decide which records of an entity a principal sees, changes and
deletes. They are written in the data modeling tool, per entity:

- A rule is a **guard** for one or more operations: `read`, `write` (create and
  update) or `delete`.
- A guard has **conditions**. Each condition ties the record to the principal:
  - an attribute of type `user`, `group` or `role` on the record matches the
    principal, one of its groups, or one of its roles —
    *`owner` is me*, *`team` is one of my groups*;
  - or a path of relations from the record ends at the principal, its groups or
    its roles — *the project's members include me*.
- All conditions of a guard must hold. A record is allowed when **any** guard
  for that operation holds.
- Row rules on with no guard for an operation means **no** record for that
  operation.

Records outside the rules are filtered out, never reported as errors: a search
returns fewer rows, and an update of a hidden record does not happen
(`DELETE_FORBIDDEN` for an explicit delete). Every user can always read their
own `User` record.

A deploy is refused when a guard would stop working — for example because a
new version removes an attribute the guard depends on.

## Scopes

Some operations are not about records and are granted as **scopes** on a role:

| Scope | Allows |
|---|---|
| `dataset:load` | read the model and its versions |
| `dataset:deploy` | deploy model versions |
| `dataset:delete` | destroy a dataset |
| `dataset:subscription` | be notified when a model is deployed |
| `schema:read` | read the model as a schema, for code generation and editor tooling |
| `log:read` | query logs and traffic |
| `log:configure` | change log levels and destinations at runtime |

## Built-in roles

Synthigy ships these roles; use them as they are or as a starting point.

| Role | For |
|---|---|
| IAM Admin | Full administrator of users, groups, roles, OAuth, federation and service identities |
| IAM Auditor | Read-only view of the whole IAM and OAuth configuration |
| User Admin | Day-to-day users, groups and memberships; cannot assign roles |
| Role Admin | Creating roles and assigning them |
| Access Admin | The grants matrix: which roles may do what on which entities |
| Department Administrator | Administration delegated to the groups it manages |
| User Provisioner | Creating accounts and their onboarding links, delegated to its groups |
| Internal Member | Baseline for staff: browse users, groups and public profiles |
| Dataset Developer | Designing, deploying and administering every dataset |
| Dataset Modeler | Modeling the datasets shared with it |
| Dataset Explorer | Reading the whole dataset catalog |
| XSQL Developer | The schema for code generation and editor tooling; no data access |

## Delegated administration

To let a team or an application manage only its own users, give users an
**owner group** and give the administrators a role scoped to it (**User
Provisioner**, **Department Administrator**). Row rules on `User` keep each
administrator to the users of the groups it belongs to.

New accounts are created over `/data`. The person then sets their own first
credential: a confidential client calls `POST /oauth/onboard` with the user's
`xid` and gets a one-time link (24 hours by default) to deliver by mail or any
other way. The link lets the person choose a password or link an identity
provider. Synthigy never sends mail itself.
