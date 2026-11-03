# Files

Synthigy stores files: uploads, downloads, versions and folders, under the
same access control as your data. The bytes go through the engine at `/files`;
each file is also an ordinary `File` record you can query and relate to your
own entities. The files module is part of every released bundle and starts
with the engine.

## Where the bytes live

By default files are kept on the engine's disk, in `<home>/files/`. For an S3
bucket (AWS, SeaweedFS, MinIO or any S3-compatible store):

```bash
SYNTHIGY_FILES_STORAGE=s3
SYNTHIGY_FILES_S3_BUCKET=synthigy-files
SYNTHIGY_FILES_S3_REGION=eu-central-1
```

A self-hosted store also needs its endpoint, and usually path-style addressing:

```bash
SYNTHIGY_FILES_S3_ENDPOINT=http://seaweedfs:8333
SYNTHIGY_FILES_S3_PATH_STYLE=true
SYNTHIGY_FILES_S3_ACCESS_KEY_ID=...
SYNTHIGY_FILES_S3_SECRET_ACCESS_KEY=...
```

Without keys the engine uses `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, then
the EC2 instance role. Give the engine a bucket of its own: it deletes objects
it no longer needs (below), and it decides that from the object names.

More than one engine behind a load balancer needs S3: local storage keeps each
engine's files on its own disk.

The portal's setup page asks for this when it creates an instance, with a
**Test** that writes, reads, lists and deletes one object in the bucket.

To change the storage later, use **Move files** in the portal's File storage
panel. It copies every file and every kept version to the new store while
the engine keeps serving, pauses uploads (they answer 503 `FILES_MIGRATING`)
only while the last few are copied, then switches. Downloads keep working
throughout, and the old store keeps its copies. Editing the environment
variables by hand moves nothing: files stored before the change stop
downloading until you switch back.

Every version of a file is a separate object, never overwritten. Back up the
database and the store together; one without the other holds rows without
bytes, or bytes nobody can find.

## Size

One request carries up to `SYNTHIGY_SERVER_MAX_BODY` bytes, 25 MB by default;
more is refused with `FILE_TOO_LARGE`. A reverse proxy has a limit of its own
(nginx `client_max_body_size`, see [Reverse proxy](PROXY.md)); keep it at
least as large.

## Access

Three roles come with the module:

| Role | For |
|---|---|
| File Admin | Every file and folder, whatever the folder access lists say |
| File User | Uploads and manages files where a folder's access lists, or authorship, allow |
| File Reader | Downloads what is shared with it; a folder's write list never gives it write |

**Folders** carry three access lists, read, write and delete, each naming users
and groups. A new folder starts with its parent's lists plus its creator on all
three; later changes to the parent do not flow down. Whoever may write a folder
may share it through `/files/folders`. File Admin and File User can read the
lists on `/data` as well. Folders at the top, inside `ROOT`, are created by File
Admin.

**Files without a folder** are allowed, for a file that belongs to a task or an
invoice rather than a place. Their uploader and File Admin can reach them.

**Your own datasets** may use the `File` entity: relate it to your entities and
add row-rule guards on `File` in the modeling tool, for example *the invoice's
owner is me*. Guards add up ([Access control](ACCESS.md#row-rules)), so yours
gives access on top of the uploader, the folders and File Admin.

The facts of a file (size, hash, status, uploader) are set by the engine, and a
`File` record is created only by an upload. Files and folders are read on
`/data` like any record, and your records can link to them there, but they are
created, changed and deleted only through `/files`: a write, delete, purge or
slice of them on `/data` is refused with `ENGINE_OWNED`, for every role. The
storage key is never readable.

## Names

Names are unique inside a folder and compared exactly: `Report.pdf` and
`report.pdf` are two files. A taken name is `FILE_NAME_TAKEN`. Files without a
folder are never compared. A name is refused (`INVALID_NAME`) when it is empty,
longer than 255 characters, holds `/`, `\` or control characters, or is `.` or
`..`.

## Versions and deletion

Replacing a file keeps the old bytes as a version. The last ten are kept, and
any of them can be downloaded or restored; restoring copies no bytes. A deleted
file's bytes stay for 30 days. Every six hours the engine deletes what nothing
needs any more: versions past the tenth, files deleted more than 30 days ago,
and uploads whose record was never written. Nothing younger than an hour is
touched.

Versions are read from [audit history](DATALINE.md), so the files module needs
a durable audit store and does not start on the in-memory one. While audit is
unavailable, uploads, replacements, deletions and version calls answer
`AUDIT_UNAVAILABLE` (503); downloads keep working.

## HTTP

Every call needs a bearer token and runs as its identity, except downloads of
public files ([Serving files to browsers](#serving-files-to-browsers)).
Errors are `{"error": {"code", "message"}}`.

A folder is named by its xid or by its path: `/` is `ROOT`, and
`/projects/acme` and `/projects/acme/` name the same folder. A path works
everywhere a folder xid does, in `folder` and `parent` parameters and bodies,
and as `?path=` in place of `/<xid>` in the folder URLs below
(`GET /files/folders?path=/projects/acme/`, `GET /files/folders/access?path=...`).
A path that names no folder is `FOLDER_NOT_FOUND`, never `ROOT`.

A file is named by its xid or by its path, `/projects/acme/report.pdf`: as
`?path=` in place of `/<xid>` on `GET`, `HEAD`, `PUT`, `PATCH` and `DELETE
/files` and on `/files/info`. Files without a folder are not under `/` and
have no path: list them with `folderless=true`, or without `folder` among all
you can see.

| Request | Does |
|---|---|
| `POST /files?name=<name>&folder=<xid\|path>` | Upload the body as a new file; `folder` is optional. 201 with the file |
| `GET /files?folder=&folderless=&name=&q=&order=&limit=&offset=` | Files you can see, newest first. `folderless=true` keeps only files in no folder you can see (not with `folder`); `name` matches one name exactly; `q` matches names holding its words in order, ignoring case (a space stands for anything between; `%` and `_` are plain characters); `order` is `name` or `oldest`, `limit` up to 1000 (default 100), `offset` for the next page |
| `GET /files/<xid>/info` | One file |
| `GET /files/search?q=&folder=&limit=&offset=` | Folders and files whose paths hold the words of `q` in order, ignoring case (`site logo` finds `/site/img/logo.png`), shortest path first: `{"results": [...], "truncated"}`. Each result is a folder or a file with `type`; a file's `path` is its full path, null when it is in no folder or in one you cannot see (then it matches by its name). `folder` searches below one folder; `limit` up to 1000 (default 100), `offset` for the next page |
| `GET`, `HEAD /files/<xid>` | The bytes |
| `GET`, `HEAD /files/path/<folder path>/<name>` | The bytes, by a readable link ([below](#serving-files-to-browsers)) |
| `PUT /files/<xid>` | A new version from the body; `If-Match: "<hash>"` refuses it when the file changed since (`FILE_CHANGED`, 412) |
| `PUT /files?path=/<folder path>/<name>` | The body becomes that file: created (201), or a new version when it is there (200, `If-Match` as above). The folder must exist: `FOLDER_NOT_FOUND` names the first one missing in `details.path` |
| `PATCH /files/<xid>` | `{"name", "folder"}`: rename and/or move; `"folder": null` takes it out of every folder. A taken name is `FILE_NAME_TAKEN` |
| `DELETE /files/<xid>` | Delete the file. 204 |
| `GET /files/<xid>/versions` | Kept versions, newest first: `id`, `at`, `by`, `hash`, `size`, `content_type`, `current` |
| `GET /files/<xid>/versions/<id>` | The bytes of one version |
| `POST /files/<xid>/versions/<id>/restore` | Make a version current again |
| `GET /files/folders?parent=<xid\|path>` | Folders inside `parent` (default `ROOT`): `xid`, `name`, `path`, `parent`, `writable`, `public` and `cache` as they apply, and the folder's own values under `settings` |
| `POST /files/folders` | `{"name", "parent"}`; a new folder. 201 |
| `PUT /files/folders?path=/a/b/` | `{"public", "cache"}`, both optional: the folder, with every missing folder above it, then its settings. When a setting is refused nothing is created |
| `GET`, `PATCH`, `DELETE /files/folders/<xid>` | One folder; `PATCH {"name", "parent"}` renames or moves it (`"parent": null` or `"/"` to the top), paths below follow; only an empty folder is deleted |
| `GET /files/folders/<xid>/access` | `{"read"\|"write"\|"delete": {"users": [...], "groups": [...]}}`; File Admin and File User, not File Reader |
| `POST /files/folders/<xid>/share`, `/unshare` | `{"access": "read"\|"write"\|"delete", "users": [xid...], "groups": [xid...]}` |

Send the bytes as the body with their own `Content-Type`, or none and the
engine guesses from the name. A form-encoded body is refused
(`UNSUPPORTED_MEDIA_TYPE`): it would be parsed as a form, not stored.

```bash
TOKEN=$(synthigy token)
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/pdf' \
     --data-binary @report.pdf 'https://synthigy.example.com/files?name=report.pdf'
```

```json
{
  "xid": "rkpYAgQpiwaSnUhP7z5hU1",
  "name": "report.pdf",
  "size": 48213,
  "hash": "906ed25f…",
  "status": "READY",
  "content_type": "application/pdf",
  "uploaded_at": "2026-10-29T13:15:07Z",
  "uploaded_by": {"xid": "…", "name": "jane"},
  "folder": null,
  "public": false
}
```

`hash` is the SHA-256 of the bytes and the download's `ETag`. `public` says
whether the file downloads without a token. Downloads answer `Range` (206) and
`If-None-Match` (304), and are sent as attachments; `?disposition=inline` shows
images, audio, video, PDF and plain text in the browser instead. A plain
`<img src>` cannot carry a token: give it a public file's link (below), or
download into a `Blob` and use `URL.createObjectURL`.

The [JavaScript SDK](SDKS.md) wraps all of this as `client.files`.

## Serving files to browsers

Images, styles and documents that a web page shows its visitors. Two `Folder`
attributes decide how they are served. Set them with
`PUT /files/folders?path=`:

| Attribute | Means |
|---|---|
| `Public` | `true`: the folder's files download without signing in; `false`: they don't, also inside a public folder |
| `Cache` | Seconds browsers and CDNs may keep the folder's files; without one they check every time |

Each folder has its own value or none, as on a filesystem: without one, it
takes the value of the nearest folder above that has one. Clearing a value
(`null`) makes the folder inherit again.

Whoever may write a folder may set them, within the attribute permissions of the
model: File User may set `Cache` but not `Public`, so making files public is up
to File Admin unless you change that attribute's permissions in the data
modeling tool. Files without a folder are never public.

**Links.** `/files/<xid>` and `/files/path/<folder path>/<name>` (each part
percent-encoded) both download a file. The first survives a rename or a move;
the second reads well and is the same in every environment. Both always serve
the current bytes. Without a token, only downloads of files in a public
folder answer, with `Access-Control-Allow-Origin: *`; everything else is 401,
the same for a file that does not exist. `?disposition=inline` opens images,
video, audio, PDF and plain text in the browser instead of downloading them.

`Cache-Control` follows the folder's `Cache`: `public, max-age=<Cache>`
without a token, `private, max-age=<Cache>` with one. With no value it is
`no-cache`: browsers ask each time and get 304 while the file is unchanged.
With a CDN in front of the engine, public files come from its cache for
`Cache` seconds; a replaced file shows its new bytes once that time is up.

A request that carries a token follows the access lists, also for a public
file, and a token that fails is refused (401) even for a public file.

## Errors

| Code | Status | When |
|---|---|---|
| `FILE_NOT_FOUND` | 404 | No such file, or you cannot see it |
| `FOLDER_NOT_FOUND` | 404 | No such folder, or you cannot see it |
| `FORBIDDEN` | 403 | Your roles or the folder's access lists refuse it |
| `FILE_NAME_TAKEN` | 409 | The folder already has a file of that name |
| `FOLDER_NAME_TAKEN` | 409 | The parent already has a folder of that name |
| `FOLDER_NOT_EMPTY` | 409 | Deleting a folder that holds files or folders |
| `FOLDER_CYCLE` | 409 | Moving a folder inside itself |
| `FILE_NOT_READY` | 409 | The file is still being processed, or failed |
| `FILE_CHANGED` | 412 | `If-Match` no longer matches |
| `NAME_REQUIRED` | 400 | An upload without `?name=` |
| `INVALID_NAME` | 400 | See [Names](#names) |
| `INVALID_ACCESS` | 400 | `access` is not `read`, `write` or `delete` |
| `FILE_TOO_LARGE` | 413 | Over `SYNTHIGY_SERVER_MAX_BODY` |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | A form-encoded body |
| `RANGE_NOT_SATISFIABLE` | 416 | A range past the end of the file |
| `VERSION_EXPIRED` | 410 | A version, or bytes, no longer kept |
| `AUDIT_UNAVAILABLE` | 503 | A write or version call while audit history is unavailable |
| `FILES_MIGRATING` | 503 | An upload while files move to a new store; retry in a moment, downloads keep working |
| `INVALID_PATH` | 400 | Not a file path (`/folder/name`) or not a folder path |
| `INVALID_SETTINGS` | 400 | `public` is not true or false, or `cache` is not whole seconds |
| `ATTRIBUTE_FORBIDDEN` | 403 | Your roles may not set that folder setting |
