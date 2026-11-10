import {test,expect} from '@playwright/test';
import fs from 'node:fs';
function fixture(){
 const created=1794280000;
 const runtime={actual_backend:'cuda',gpu:{name:'测试GPU',uuid:'GPU-fixture'},cuda_compute_events:203,
   initialization_stages:[{name:'device',started_at:created+4,seconds:1},{name:'model_load',started_at:created+5,seconds:.5},{name:'warmup',started_at:created+5.5,seconds:1.5}]};
 const result={attempt_id:'a1',sampled_frames:2,inference_runtime:runtime,
   segment_timing:{version:1,started_at:created+8,total_seconds:2,phases:[{name:'download',offset_seconds:0,seconds:.2},{name:'analyze',offset_seconds:.3,seconds:1.4},{name:'upload',offset_seconds:1.8,seconds:.1}]},
   analysis_timing:{inference_seconds:.4,analysis_seconds:1.4,decode_seconds:.1,preprocess_seconds:.1,postprocess_seconds:.1,preview_seconds:.1,other_seconds:.6},
   frames:[{frame:0,timing:{inference_offset_seconds:.1,inference_seconds:.2}},{frame:10,timing:{inference_offset_seconds:.7,inference_seconds:.2}}]};
 return {id:'timing-fixture',name:'分段计时夹具',status:'SUCCEEDED',total:1,completed:1,created,updated:created+11,finished:created+11,
   params:{algorithm:'round_robin'},metadata:{application:{execution_mode:'task_session'}},result:null,
   segments:[{id:'s1',ordinal:0,start:0,end:4,status:'SUCCEEDED',active_attempt:'a1',result}],
   attempts:[{id:'a1',segment_id:'s1',node_id:'lab-1',status:'SUCCEEDED',created:created+1,started:created+8,finished:created+10,updated:created+10}],
   sessions:[{id:'session-one',node_id:'lab-1',created:created+1,ready_at:created+7,model_load_seconds:3,model_load_count:1,completed:1,
     workload:{timing_version:1,pod_uid:'p',inference_runtime:runtime,
       job_submit_started_at:created+1,job_submit_finished_at:created+1.1,
       lifecycle:{p:{pod_uid:'p',pod_created_at:created+1.2,scheduled_at:created+1.4,image_ready_at:created+2,image_cached:true,container_started_at:created+2.2}},
       startup_timing:{entry_at:created+2.5,imports_seconds:1,claim_started_at:created+3.6,claim_seconds:.1}}}]};
}
async function open(page,getTask){
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/api/**',async route=>{
   const t=getTask(),p=new URL(route.request().url()).pathname;
   const body=p==='/api/tasks'?[t]:p==='/api/tasks/'+t.id?t:p==='/api/overview'?{totals:{SUCCEEDED:1},nodes:[],daily:[]}:p==='/api/alerts'?{summary:{unread:0,open:0,acknowledged:0},items:[],total:0}:[];
   await route.fulfill({json:body});
 });
 await page.goto('/?page=detail&task='+getTask().id);
 await expect(page.getByRole('heading',{name:'启动与推理分段计时'})).toBeVisible();
 return errors;
}
test('shows actual startup, cached image and per-frame timings',async({page})=>{
 const t=fixture(),errors=await open(page,()=>t),panel=page.getByRole('region',{name:'启动与推理分段计时'});
 await expect(panel).toContainText('GPU / CUDA');
 await expect(panel).toContainText('200.000 ms');
 await expect(panel).toContainText('镜像已缓存（未重新拉取）');
 await panel.getByText('启动阶段明细与设备',{exact:true}).click();
 await expect(panel).toContainText('GPU-fixture');
 await panel.locator('.timing-segments summary').first().click();
 await expect(panel.locator('.timing-segments .timing-forward')).toHaveCount(2);
 await expect(panel).toContainText('解码累计');
 fs.mkdirSync('../docs/gpu-timing-20261110',{recursive:true});
 await page.screenshot({path:'../docs/gpu-timing-20261110/fixture-desktop.png',fullPage:true});
 expect(errors).toEqual([]);
});
test('legacy results remain missing, with no invented zero timings',async({page})=>{
 const t=fixture();t.sessions=[];delete t.segments[0].result.segment_timing;delete t.segments[0].result.analysis_timing;
 const errors=await open(page,()=>t),panel=page.getByRole('region',{name:'启动与推理分段计时'});
 await expect(panel.getByRole('status')).toContainText('未采集启动分段计时');
 await expect(panel).toContainText('尚无已完成的分片分段计时');
 await expect(panel).not.toContainText('NaN');
 await expect(panel).not.toContainText('Infinity');
 expect(errors).toEqual([]);
});
test('refreshes incoming real evidence',async({page})=>{
 const t=fixture(),saved=structuredClone(t.sessions);t.sessions=[];
 await open(page,()=>t);
 await expect(page.getByRole('status')).toContainText('未采集启动分段计时');
 t.sessions=saved;
 await expect(page.locator('.timing-startup-row')).toHaveCount(1,{timeout:10000});
});
test('mobile timing content fits, tables scroll',async({page})=>{
 await page.setViewportSize({width:390,height:844});const t=fixture(),errors=await open(page,()=>t);
 await page.locator('.timing-segments summary').first().click();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
 await expect(page.locator('.execution-timing')).toContainText('200.000 ms');
 await page.screenshot({path:'../docs/gpu-timing-20261110/fixture-mobile.png',fullPage:true});
 expect(errors).toEqual([]);
});
