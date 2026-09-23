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

(ns synthigy.server.console.pages.topology
  (:require
   [clojure.string :as str]
   [hiccup2.core :refer [raw]]
   [synthigy.server.console.icon :as icon]
   [synthigy.server.console.pages.system :as system]
   [synthigy.server.console.ui :as ui]))

(defn node-status
  [{:keys [status error missing-dependencies] :as row}]
  (cond
    (system/locked? row)          [:locked "Requires license"]
    error                         [:failed "Failed"]
    (seq missing-dependencies)    [:missing "Missing deps"]
    (= :started status)           [:running "Running"]
    :else                         [:stopped "Stopped"]))

(defn closure
  "Transitive closure of `k` (:depends-on or :dependents) from `start`."
  [index k start]
  (loop [seen #{} frontier (vec (get-in index [start k]))]
    (if-let [t (peek frontier)]
      (if (seen t)
        (recur seen (pop frontier))
        (recur (conj seen t) (into (pop frontier) (get-in index [t k]))))
      seen)))

(defn layered
  "Rows grouped by depth, each layer ordered by the mean position of its deps in the layer below."
  [rows]
  (let [by-depth (group-by :depth rows)
        maxd     (reduce max 0 (map :depth rows))]
    (loop [d 0 order {} out []]
      (if (> d maxd)
        out
        (let [layer (sort-by (fn [{:keys [depends-on topic]}]
                               (let [xs (keep order depends-on)]
                                 [(if (seq xs)
                                    (/ (reduce + xs) (double (count xs)))
                                    99.0)
                                  (str topic)]))
                             (by-depth d))]
          (recur (inc d)
                 (into order (map-indexed (fn [i m] [(:topic m) i]) layer))
                 (conj out (vec layer))))))))

(defn node
  [index {:keys [topic doc depends-on] :as row}]
  (let [[st label] (node-status row)
        up         (closure index :depends-on topic)
        down       (closure index :dependents topic)]
    [:div.console-topo-node
     {:class (name st)
      :data-id (str topic)
      :data-status label
      :data-doc (or doc "")
      :data-deps (str/join "," (map str depends-on))
      :data-up (str/join "," (map str (sort-by str up)))
      :data-down (str/join "," (map str (sort-by str down)))
      :data-start (when (contains? #{:stopped :failed :missing} st)
                    (str "/console/system/start?module=" (system/module-param topic)))}
     [:div.console-topo-name [:i.console-topo-dot] (str topic)]
     (when doc [:div.console-topo-doc doc])]))

(def script
  "(function(){
  var graph=document.getElementById('topo-graph');
  var svg=document.getElementById('topo-edges');
  var nodes=Array.prototype.slice.call(graph.querySelectorAll('.console-topo-node'));
  var byId={}; nodes.forEach(function(n){byId[n.dataset.id]=n;});
  function draw(){
    var gr=graph.getBoundingClientRect();
    svg.setAttribute('width',graph.scrollWidth); svg.setAttribute('height',graph.scrollHeight);
    svg.innerHTML='';
    nodes.forEach(function(n){
      (n.dataset.deps||'').split(',').filter(Boolean).forEach(function(d){
        var m=byId[d]; if(!m) return;
        var a=m.getBoundingClientRect(), b=n.getBoundingClientRect();
        var sx=a.left-gr.left+a.width/2, sy=a.top-gr.top;
        var ex=b.left-gr.left+b.width/2, ey=b.bottom-gr.top;
        var dy=Math.max(18,Math.abs(ey-sy)/2);
        var p=document.createElementNS('http://www.w3.org/2000/svg','path');
        p.setAttribute('d','M '+sx+' '+sy+' C '+sx+' '+(sy-dy)+', '+ex+' '+(ey+dy)+', '+ex+' '+ey);
        p.dataset.from=d; p.dataset.to=n.dataset.id;
        svg.appendChild(p);
      });
    });
  }
  var card=document.getElementById('topo-detail'), pinned=null;
  function setFor(id){
    var me=byId[id];
    var up=new Set((me.dataset.up||'').split(',').filter(Boolean));
    var down=new Set((me.dataset.down||'').split(',').filter(Boolean));
    nodes.forEach(function(n){var i=n.dataset.id;
      n.classList.remove('focus','chain-up','chain-down','dimmed');
      if(i===id)n.classList.add('focus');
      else if(up.has(i))n.classList.add('chain-up');
      else if(down.has(i))n.classList.add('chain-down');
      else n.classList.add('dimmed');});
    svg.querySelectorAll('path').forEach(function(p){
      var f=p.dataset.from,t=p.dataset.to;
      var onUp=(t===id&&up.has(f))||(up.has(f)&&up.has(t));
      var onDown=(f===id&&down.has(t))||(down.has(f)&&down.has(t));
      p.classList.toggle('on',onUp);
      p.classList.toggle('on-soft',!onUp&&onDown);
      p.classList.toggle('dimmed',!onUp&&!onDown);});
    card.querySelector('.name').textContent=me.dataset.id+' \\u2014 '+me.dataset.status;
    card.querySelector('.doc').textContent=me.dataset.doc;
    card.querySelector('.needs').textContent=me.dataset.up?me.dataset.up.split(',').join(', '):'nothing';
    card.querySelector('.needed').textContent=me.dataset.down?me.dataset.down.split(',').join(', '):'nothing';
    var s=card.querySelector('#topo-start');
    if(me.dataset.start){ s.hidden=false;
      s.onclick=function(){fetch(me.dataset.start,{method:'POST'}).then(function(){location.reload();});};
    } else { s.hidden=true; }
    card.classList.add('show');
  }
  function clearAll(){
    nodes.forEach(function(n){n.classList.remove('focus','chain-up','chain-down','dimmed');});
    svg.querySelectorAll('path').forEach(function(p){p.classList.remove('on','on-soft','dimmed');});
    card.classList.remove('show');
  }
  function unpin(){pinned=null;clearAll();}
  nodes.forEach(function(n){
    n.addEventListener('mouseenter',function(){if(!pinned)setFor(n.dataset.id);});
    n.addEventListener('mouseleave',function(){if(!pinned)clearAll();});
    n.addEventListener('click',function(e){e.stopPropagation();
      pinned=(pinned===n.dataset.id?null:n.dataset.id);
      if(pinned)setFor(pinned); else clearAll();});
  });
  card.querySelector('#topo-close').addEventListener('click',function(e){e.stopPropagation();unpin();});
  card.addEventListener('click',function(e){e.stopPropagation();});
  var vp=document.getElementById('topo-viewport'), drag=null;
  vp.addEventListener('mousedown',function(e){
    drag={x:e.clientX,y:e.clientY,l:vp.scrollLeft,t:vp.scrollTop,m:false};
    vp.classList.add('grabbing');});
  window.addEventListener('mousemove',function(e){if(!drag)return;
    var dx=e.clientX-drag.x,dy=e.clientY-drag.y;
    if(Math.abs(dx)+Math.abs(dy)>3)drag.m=true;
    vp.scrollLeft=drag.l-dx; vp.scrollTop=drag.t-dy;});
  window.addEventListener('mouseup',function(){vp.classList.remove('grabbing');setTimeout(function(){drag=null;},0);});
  document.body.addEventListener('click',function(){
    if(drag&&drag.m)return;
    if(pinned)unpin();},true);
  window.addEventListener('resize',draw);
  draw();
})();")

(defn graph
  []
  (let [rows  (system/module-rows)
        index (into {} (map (juxt :topic identity)) rows)]
    [:div#topo-viewport.console-topo-viewport
     [:div#topo-graph.console-topo-graph
      [:svg#topo-edges.console-topo-edges]
      (for [layer (reverse (layered rows))]
        [:div.console-topo-layer
         (for [row layer] (node index row))])]]))

(defn detail-card
  []
  [:div#topo-detail.console-topo-detail
   [:ty-button {:id "topo-close" :type "button" :size "xs" :appearance "ghost"
                :muted true :action true}
    (icon/icon :x {:size "12"})]
   [:div.name]
   [:div.doc]
   [:div.rel [:b "needs "] [:span.needs]]
   [:div.rel [:b "needed by "] [:span.needed]]
   [:ty-button {:id "topo-start" :type "button" :size "xs" :appearance "outlined"
                :muted true :flavor "success" :hidden true}
    (icon/icon :check {:size "11" :slot "start"})
    "Start"]])

(defn render
  [request]
  (ui/admin-shell
   {:title "Topology"
    :user (:console/principal request)
    :uri "/console/system/topology"
    :session (:console/session request)
    :search :none
    :body
    [:div.console-page
     [:div.console-page-head
      [:div.console-eyebrow "System"]
      [:h1.console-title "Topology"]
      [:p.console-subtitle
       (str "The dependency graph of this deployment — what you run on top, "
            "what it stands on below. Hover a module for its chain, click to "
            "pin, drag to pan.")]]
     (graph)
     (detail-card)
     [:script (raw script)]]}))
