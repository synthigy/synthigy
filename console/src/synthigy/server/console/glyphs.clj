;   Synthigy — model-driven IAM and data platform
;   Copyright (C) 2026 Robert Geršak
;
;   This program is free software: you can redistribute it and/or modify
;   it under the terms of the GNU Affero General Public License as
;   published by the Free Software Foundation, either version 3 of the
;   License, or (at your option) any later version.
;
;   This program is distributed in the hope that it will be useful,
;   but WITHOUT ANY WARRANTY; without even the implied warranty of
;   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;   GNU Affero General Public License for more details.
;
;   You should have received a copy of the GNU Affero General Public
;   License along with this program.  If not, see
;   <https://www.gnu.org/licenses/>.
;
;   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
;   in a proprietary product, or offering it as a service without
;   releasing your source under section 13 — a commercial license is
;   available: r.gersak@gmail.com  See COMMERCIAL.md.

;; GENERATED FILE — do not edit.
;; Source: tyrell.lucide (dev.gersak/tyrell-icons). Regenerate with
;;   cd console && clj -M:icons -m gen-icons
;; Add a glyph by adding its name to `used` in console/dev/gen_icons.clj.
(ns synthigy.server.console.glyphs
  "Lucide SVG source for the glyphs the console renders. Upstream strings
   with the XML prolog stripped — see console/dev/gen_icons.clj for why these
   are generated rather than pulled from `tyrell.lucide` at runtime.")

(def glyphs
  {:activity
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2'/>\n</svg>"

   :bell
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M10.268 21a2 2 0 0 0 3.464 0'/>\n<path d='M3.262 15.326A1 1 0 0 0 4 17h16a1 1 0 0 0 .74-1.673C19.41 13.956 18 12.499 18 8A6 6 0 0 0 6 8c0 4.499-1.411 5.956-2.738 7.326'/>\n</svg>"

   :bot
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M12 8V4H8'/>\n<rect rx='2' y='8' x='4' height='12' width='16'/>\n<path d='M2 14h2'/>\n<path d='M20 14h2'/>\n<path d='M15 13v2'/>\n<path d='M9 13v2'/>\n</svg>"

   :box
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z'/>\n<path d='m3.3 7 8.7 5 8.7-5'/>\n<path d='M12 22V12'/>\n</svg>"

   :check
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M20 6 9 17l-5-5'/>\n</svg>"

   :chevron-right
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='m9 18 6-6-6-6'/>\n</svg>"

   :copy
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<rect ry='2' rx='2' y='8' x='8' height='14' width='14'/>\n<path d='M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2'/>\n</svg>"

   :database
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<ellipse ry='3' rx='9' cy='5' cx='12'/>\n<path d='M3 5V19A9 3 0 0 0 21 19V5'/>\n<path d='M3 12A9 3 0 0 0 21 12'/>\n</svg>"

   :globe
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<circle r='10' cy='12' cx='12'/>\n<path d='M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20'/>\n<path d='M2 12h20'/>\n</svg>"

   :inbox
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<polyline points='22 12 16 12 14 15 10 15 8 12 2 12'/>\n<path d='M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z'/>\n</svg>"

   :key
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='m15.5 7.5 2.3 2.3a1 1 0 0 0 1.4 0l2.1-2.1a1 1 0 0 0 0-1.4L19 4'/>\n<path d='m21 2-9.6 9.6'/>\n<circle r='5.5' cy='15.5' cx='7.5'/>\n</svg>"

   :layers
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83z'/>\n<path d='M2 12a1 1 0 0 0 .58.91l8.6 3.91a2 2 0 0 0 1.65 0l8.58-3.9A1 1 0 0 0 22 12'/>\n<path d='M2 17a1 1 0 0 0 .58.91l8.6 3.91a2 2 0 0 0 1.65 0l8.58-3.9A1 1 0 0 0 22 17'/>\n</svg>"

   :link
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71'/>\n<path d='M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71'/>\n</svg>"

   :lock
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<rect ry='2' rx='2' y='11' x='3' height='11' width='18'/>\n<path d='M7 11V7a5 5 0 0 1 10 0v4'/>\n</svg>"

   :log-out
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='m16 17 5-5-5-5'/>\n<path d='M21 12H9'/>\n<path d='M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4'/>\n</svg>"

   :microscope
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M6 18h8'/>\n<path d='M3 22h18'/>\n<path d='M14 22a7 7 0 1 0 0-14h-1'/>\n<path d='M9 14h2'/>\n<path d='M9 12a2 2 0 0 1-2-2V6h6v4a2 2 0 0 1-2 2Z'/>\n<path d='M12 6V3a1 1 0 0 0-1-1H9a1 1 0 0 0-1 1v3'/>\n</svg>"

   :minus
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M5 12h14'/>\n</svg>"

   :monitor
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<rect rx='2' y='3' x='2' height='14' width='20'/>\n<line y2='21' y1='21' x2='16' x1='8'/>\n<line y2='21' y1='17' x2='12' x1='12'/>\n</svg>"

   :pencil
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M21.174 6.812a1 1 0 0 0-3.986-3.987L3.842 16.174a2 2 0 0 0-.5.83l-1.321 4.352a.5.5 0 0 0 .623.622l4.353-1.32a2 2 0 0 0 .83-.497z'/>\n<path d='m15 5 4 4'/>\n</svg>"

   :plus
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M5 12h14'/>\n<path d='M12 5v14'/>\n</svg>"

   :rocket
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z'/>\n<path d='m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z'/>\n<path d='M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0'/>\n<path d='M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5'/>\n</svg>"

   :search
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='m21 21-4.34-4.34'/>\n<circle r='8' cy='11' cx='11'/>\n</svg>"

   :server
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<rect ry='2' rx='2' y='2' x='2' height='8' width='20'/>\n<rect ry='2' rx='2' y='14' x='2' height='8' width='20'/>\n<line y2='6' y1='6' x2='6.01' x1='6'/>\n<line y2='18' y1='18' x2='6.01' x1='6'/>\n</svg>"

   :shield
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z'/>\n</svg>"

   :trash-2
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M10 11v6'/>\n<path d='M14 11v6'/>\n<path d='M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6'/>\n<path d='M3 6h18'/>\n<path d='M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2'/>\n</svg>"

   :tv
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='m17 2-5 5-5-5'/>\n<rect rx='2' y='7' x='2' height='15' width='20'/>\n</svg>"

   :user
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2'/>\n<circle r='4' cy='7' cx='12'/>\n</svg>"

   :users
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2'/>\n<path d='M16 3.128a4 4 0 0 1 0 7.744'/>\n<path d='M22 21v-2a4 4 0 0 0-3-3.87'/>\n<circle r='4' cy='7' cx='9'/>\n</svg>"

   :wrench
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.106-3.105c.32-.322.863-.22.983.218a6 6 0 0 1-8.259 7.057l-7.91 7.91a1 1 0 0 1-2.999-3l7.91-7.91a6 6 0 0 1 7.057-8.259c.438.12.54.662.219.984z'/>\n</svg>"

   :x
   "<svg stroke='currentColor' fill='none' stroke-linejoin='round' width='24' xmlns='http://www.w3.org/2000/svg' stroke-linecap='round' stroke-width='2' viewBox='0 0 24 24' height='24'>\n<path d='M18 6 6 18'/>\n<path d='m6 6 12 12'/>\n</svg>"})
