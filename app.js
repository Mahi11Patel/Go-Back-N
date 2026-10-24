const $=id=>document.getElementById(id);
let polling=null, lastLogCount=0, paused=false;

async function post(path, body={}){
  await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
}
async function configure(){
  await post('/config',{window:+$('window').value,total:+$('total').value,timeout:+$('timeout').value});
}
function frameHTML(f, receiver=false){
  let label=f.status;
  if(receiver && f.status==='received') label='received';
  return `<div class="frame ${f.status}"><b>F${f.id}</b><small>${label}</small></div>`;
}
function render(s){
  $('base').textContent=s.base;$('next').textContent=s.next_seq;$('expected').textContent=s.expected;
  $('ack').textContent=s.last_ack===null?'—':s.last_ack;$('retrans').textContent=s.retransmissions;
  const end=Math.min(s.base+s.window,s.total);
  $('windowText').textContent=`${s.base} – ${end-1}`;
  $('expectedText').textContent=`Frame ${s.expected}`;
  $('senderStatus').textContent=s.phase;
  $('receiverStatus').textContent=s.expected>=s.total?'Complete':(s.phase==='Paused'?'Paused':'Waiting');
  $('sender').innerHTML=s.sender.map(f=>frameHTML(f)).join('');
  $('receiver').innerHTML=s.receiver.map(f=>frameHTML(f,true)).join('');
  if(s.logs.length>lastLogCount){
    for(const x of s.logs.slice(lastLogCount)){
      const d=document.createElement('div');d.className='line '+x.kind;d.textContent=`[${x.time}] ${x.message}`;$('log').appendChild(d);
    }
    $('log').scrollTop=$('log').scrollHeight;lastLogCount=s.logs.length;
  }
  paused=s.paused;$('pause').textContent=paused?'▶ Resume':'Ⅱ Pause';
}
async function refresh(){try{const r=await fetch('/state');render(await r.json())}catch(e){}}
$('start').onclick=async()=>{await configure();await post('/start');refresh()};
$('pause').onclick=async()=>{await post(paused?'/resume':'/pause');refresh()};
$('reset').onclick=async()=>{await post('/reset');lastLogCount=0;$('log').innerHTML='';await configure();refresh()};
$('clear').onclick=async()=>{await post('/clear-log');lastLogCount=0;$('log').innerHTML=''};
configure().then(refresh);polling=setInterval(refresh,180);
