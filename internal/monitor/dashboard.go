package monitor

const dashboardHTML = `<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>全链路压测 · 流量隔离监控</title>
<style>
body{font-family:-apple-system,"PingFang SC",sans-serif;margin:0;background:#0f1420;color:#dde3f0}
header{padding:14px 24px;background:#171e30;display:flex;align-items:center;gap:16px;position:sticky;top:0}
h1{font-size:18px;margin:0}
#breaker{padding:4px 14px;border-radius:12px;font-size:13px;background:#1f7a4d;color:#fff}
#breaker.open{background:#c0392b}
main{padding:20px 24px;display:flex;flex-direction:column;gap:18px}
section{background:#171e30;border-radius:10px;padding:16px}
h2{font-size:15px;margin:0 0 12px;color:#8fa3c8;font-weight:600}
.topo{display:flex;gap:16px;flex-wrap:wrap;align-items:flex-start}
.col{display:flex;flex-direction:column;gap:10px;min-width:200px}
.coltitle{font-size:12px;color:#6b7a99;text-align:center}
.node{background:#212b45;border:1px solid #2e3a5c;border-radius:8px;padding:8px 10px;font-size:12px;line-height:1.7}
.node .name{font-weight:600;font-size:13px}
.node.bad{border-color:#e74c3c;background:#3a2230}
.ok{color:#2ecc71}.bad{color:#e74c3c}.muted{color:#6b7a99;font-size:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px}
.card{background:#212b45;border-radius:8px;padding:10px;font-size:12px}
.card .k{color:#8fa3c8}.card .v{font-size:18px;font-weight:600;margin-top:2px}
.card.shadow{border-left:3px solid #8e44ad}
table{width:100%;border-collapse:collapse;font-size:12px}
td,th{padding:5px 8px;border-bottom:1px solid #2e3a5c;text-align:left}
th{color:#8fa3c8;font-weight:500}
.alert{background:#3a2230;border:1px solid #e74c3c;border-radius:6px;padding:8px 10px;margin:6px 0;font-size:12px}
</style>
</head>
<body>
<header>
  <h1>全链路压测 · 流量隔离监控</h1>
  <span id="breaker">熔断器:闭合</span>
  <span class="muted" id="ts"></span>
</header>
<main>
<section><h2>链路拓扑 · 各服务压测流量与标记透传</h2><div class="topo" id="topo"></div></section>
<section><h2>影子表 / 真实表(行数)</h2><div class="grid" id="tables"></div><div class="muted" id="shadowmiss"></div></section>
<section><h2>缓存 / 消息主题 / 第三方挡板</h2><div class="grid" id="infra"></div></section>
<section><h2>中间层额外延迟</h2><table><thead><tr><th>组件</th><th>调用次数</th><th>平均</th><th>P99</th><th>最大</th></tr></thead><tbody id="lat"></tbody></table></section>
<section><h2>漏标告警</h2><div id="leaks"><span class="muted">暂无告警</span></div></section>
</main>
<script>
const TOPO=[
 {title:"入口",nodes:[{label:"gateway",entry:"gateway"}]},
 {title:"下单链路(同步调用)",nodes:[
   {label:"order",entry:"order"},{label:"user",entry:"user"},{label:"risk",entry:"risk"},
   {label:"coupon",entry:"coupon"},{label:"inventory",entry:"inventory"},
   {label:"payment",entry:"payment"},{label:"notification",entry:"notification"}]},
 {title:"异步消费(MQ)",nodes:[
   {label:"notification ← order_created",prop:["notification","mq"]},
   {label:"logistics ← order_created",prop:["logistics","mq"]},
   {label:"notification ← order_timeout",prop:["notification","mq"]}]},
 {title:"线程池 / 定时任务",nodes:[
   {label:"order · 审计日志(线程池)",prop:["order","threadpool"]},
   {label:"order · 超时关单(定时任务)",text:"无入口流量,不带标记,只扫真实表"}]},
];
function pct(ok,total){return total?((100*ok/total).toFixed(1)+"%"):"-"}
function nodeHTML(n,S){
  let inner="",bad=false;
  if(n.entry){
    const e=S.metrics.entries[n.entry];
    if(e){inner="<div>压测流入 "+e.stress_in+" · 带标 "+e.marked+" · <span class='"+(e.lost?"bad":"ok")+"'>丢标 "+e.lost+"</span></div>"+
      "<div>透传率 <span class='"+(e.lost?"bad":"ok")+"'>"+pct(e.marked,e.stress_in)+"</span></div>";bad=e.lost>0}
    else inner='<div class="muted">暂无压测流量</div>';
  }else if(n.prop){
    const p=(S.metrics.propagation[n.prop[0]]||{})[n.prop[1]];
    if(p){inner="<div>"+n.prop[1]+" 透传 "+p.ok+"/"+p.total+"</div><div>成功率 <span class='"+(p.ok===p.total?"ok":"bad")+"'>"+pct(p.ok,p.total)+"</span></div>";bad=p.ok!==p.total}
    else inner='<div class="muted">暂无记录</div>';
  }else inner='<div class="muted">'+n.text+'</div>';
  return '<div class="node '+(bad?"bad":"")+'"><div class="name">'+n.label+'</div>'+inner+'</div>';
}
async function refresh(){
  try{
  const S=await (await fetch("/api/stats")).json();
  document.getElementById("ts").textContent="更新于 "+S.generated_at;
  const b=document.getElementById("breaker");
  if(S.metrics.breaker_open){b.className="open";b.textContent="熔断器:已熔断 · "+S.metrics.breaker_reason+" · "+S.metrics.breaker_since}
  else{b.className="";b.textContent="熔断器:闭合"}
  document.getElementById("topo").innerHTML=TOPO.map(c=>
    '<div class="col"><div class="coltitle">'+c.title+'</div>'+c.nodes.map(n=>nodeHTML(n,S)).join("")+'</div>').join("");
  const names=Object.keys(S.tables).sort();
  document.getElementById("tables").innerHTML=names.map(t=>{
    const sh=t.startsWith("shadow_");
    return '<div class="card '+(sh?"shadow":"")+'"><div class="k">'+(sh?"影子表 ":"真实表 ")+t+'</div><div class="v">'+S.tables[t]+'</div></div>'}).join("");
  const sm=S.metrics.shadow_miss,smk=Object.keys(sm);
  document.getElementById("shadowmiss").textContent=smk.length?("影子表缺失,已拒绝写入: "+smk.map(k=>k+"×"+sm[k]).join(", ")):"";
  const sw=Object.entries(S.metrics.shadow_writes).map(([k,v])=>k+" × "+v).join("<br>")||'<span class="muted">-</span>';
  const mk=Object.entries(S.metrics.mock_calls).map(([k,v])=>k+" × "+v).join("<br>")||'<span class="muted">-</span>';
  const topics=Object.keys(S.topics).sort().map(t=>{
    const sh=t.endsWith("__stress");
    return '<div class="card '+(sh?"shadow":"")+'"><div class="k">'+(sh?"影子主题 ":"真实主题 ")+t+'</div><div class="v">'+S.topics[t]+'</div></div>'}).join("");
  document.getElementById("infra").innerHTML=
    '<div class="card"><div class="k">影子表写入</div><div style="margin-top:4px">'+sw+'</div></div>'+
    '<div class="card"><div class="k">缓存键</div><div class="v">'+S.cache_real_keys+' <span class="muted">真实</span> / '+S.cache_stress_keys+' <span style="color:#8e44ad">压测</span></div></div>'+
    '<div class="card"><div class="k">第三方挡板拦截(mock)</div><div style="margin-top:4px">'+mk+'</div></div>'+
    '<div class="card"><div class="k">真实第三方调用</div><div class="v">'+S.third_real_calls+' <span class="muted">总</span> / <span class="'+(S.third_stress_calls?"bad":"ok")+'">'+S.third_stress_calls+' 压测</span></div></div>'+
    topics;
  document.getElementById("lat").innerHTML=Object.keys(S.metrics.latency).sort().map(k=>{
    const l=S.metrics.latency[k];
    return "<tr><td>"+k+"</td><td>"+l.count+"</td><td>"+l.avg_micros.toFixed(1)+"µs</td><td>"+l.p99_micros.toFixed(1)+"µs</td><td>"+l.max_micros.toFixed(1)+"µs</td></tr>"}).join("");
  const L=S.metrics.leaks||[];
  document.getElementById("leaks").innerHTML=L.length?L.map(l=>
    '<div class="alert">🚨 ['+l.time+'] <b>'+l.component+'</b> — '+l.detail+'</div>').join(""):'<span class="muted">暂无告警</span>';
  }catch(e){}
}
refresh();setInterval(refresh,1000);
</script>
</body>
</html>`
