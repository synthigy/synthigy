;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;   SPDX-License-Identifier: BUSL-1.1
;
;   Licensed under the Business Source License 1.1 — see LICENSE. Free in
;   production for individuals, non-profits, education and organisations
;   up to EUR 5M; larger organisations, integrators deploying for them and
;   hosted-service providers need a commercial license. Each version
;   becomes GPL-2.0-or-later / Apache-2.0 four years after release.
;   Commercial licensing: r.gersak@gmail.com  See COMMERCIAL.md.

(ns synthigy.apps.model
  "Ids of the Synthigy Apps dataset the apps module owns."
  (:require
   [synthigy.dataset.id :refer [defdata defentity defrelation]]))

(defdata :apps/dataset
  :euuid #uuid "fb086c79-d2b0-424d-a587-7fdd243a8edc" :xid "XzvLSwLGikdoqNU1qeKQ11")

(defentity :apps/package
  :euuid #uuid "d04372fd-20a8-4ba7-91c0-ff69a5a71569" :xid "SibsLcBKQStgE3nnix9Kfv")

(defentity :apps/variable
  :euuid #uuid "a39ef6ff-5b89-4313-8a5a-34b354ae37c3" :xid "MCsKVzxJgzPhaq7JUZHhoQ")

(defentity :apps/configuration
  :euuid #uuid "e51aad7b-849f-42aa-8f66-06e86db77c2c" :xid "VHs8edb4LphZJ8X3DviL8s")

(defentity :apps/hostname
  :euuid #uuid "19659a65-c4ad-4302-bab8-04dc32c0dfc0" :xid "48ttr3UJwRPRv5RMHxLQhD")

(defrelation :apps/app->packages
  :euuid #uuid "e9961f98-a7e1-4067-a534-16aaefbae1f0" :xid "Vqy3hyYWRjUK9r6C2e8NHM")

(defrelation :apps/app->deployers
  :euuid #uuid "e956d680-786a-47ed-8d35-961bd40d3070" :xid "VpCMdjdLwW8UL2SkHicVQ3")

(defrelation :apps/app->configuration
  :euuid #uuid "376bb65e-adf5-4b26-83b0-89b817646e95" :xid "7qvqfPrMYA7MXCqcJK2Nur")

(defrelation :apps/app->hostnames
  :euuid #uuid "3211a074-bb8f-45aa-a2b6-af0d22cadb0e" :xid "7Bbf1cbfDF2tYgzsWcbVRF")

(defrelation :apps/package->archive
  :euuid #uuid "4861a909-2dd3-46e4-aee1-70e3a6c1a0c4" :xid "9wQM4gnEn6uKJkt2RVmC99")

(defrelation :apps/app->variables
  :euuid #uuid "c07f043a-b9fe-4ce0-9ed1-1fda7e1e36ba" :xid "Qmg7Hx5pR7pfGqgEPwVZcD")

(defrelation :apps/package->variables
  :euuid #uuid "12c589a7-2a96-46b4-b4d6-5c8416c06722" :xid "3KSkqFEjihGW2hDmKzVW4q")

(defdata :apps.package/status
  :euuid #uuid "24e8b3b2-e025-4548-b2b2-fb0aaa3a8373" :xid "5ZM2ztXFg4eDs3qCBFPkPU")

(defdata :apps.package/last-error
  :euuid #uuid "15133965-8472-405e-99ff-03946bc2b2ca" :xid "3bwcmQSoM4TH57taSt39pd")

(defdata :apps.package/active
  :euuid #uuid "0f965935-d66f-4adb-aa96-a7034ccd745a" :xid "2vdysDCkG73JKeA7xqJecq")

(defdata :apps.client/active
  :euuid #uuid "5628eca6-4d4a-4779-8368-fbcda6a23d94" :xid "Be5u4uWvdssKER3kKZvRz3")

(defdata :apps.client/secret
  :euuid #uuid "a005577b-fc9e-4274-b1c0-a3fc2e81001d" :xid "Lm5qau8pHiHXy7eA2DeBbS")

(defdata :apps.variable/value
  :euuid #uuid "042838f9-50e2-4957-8157-237a11a6f28a" :xid "1WmsZi3z2o1nkhPnSYk3b7")

(defentity :apps/volume
  :euuid #uuid "0e20bd70-b6df-44e7-aeb9-1c1d39c92c19" :xid "2kBjxvzeLMG4v2bSrTcv3z")

(defrelation :apps/app->volumes
  :euuid #uuid "be2cf5d2-e1ee-4224-afbf-1e4be351a765" :xid "QV4A6pFbtyiauaWozY4uEU")

(defdata :apps.volume/state
  :euuid #uuid "65e2c3bc-85c2-4210-80de-6bb2fd17642b" :xid "DaiUDBq6fsn1NMEG2pN5oQ")

(defdata :apps.volume/used-mb
  :euuid #uuid "c1a36b34-9611-429a-882b-2ac194de5b59" :xid "QuraXX7xEhqn4cV7yU2Emv")

(defdata :apps.volume/delete-after
  :euuid #uuid "fae8a105-31e5-4a64-8d18-71843f39d3c6" :xid "Xz2k7Bcyms3jbYeTDYtKXB")

(defdata :apps.volume/last-error
  :euuid #uuid "37fe236e-14d8-4a9d-a325-069317c205f8" :xid "7v2SSn3wSD6x1SkTWMVz6K")

(defdata :apps.volume/size-mb
  :euuid #uuid "b5e7b219-1850-4c80-af52-5423c0d792ae" :xid "PTpZDuarURagh3Fo5ANTF3")

(defdata :apps.volume/mount-path
  :euuid #uuid "3f1d4396-041a-4f38-9e5a-50ccdeaadd77" :xid "8o2ke9Pmu9DFDmoWx6FN2i")
