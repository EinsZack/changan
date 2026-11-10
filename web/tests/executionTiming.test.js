import test from 'node:test';
import assert from 'node:assert/strict';
import {taskTiming,sessionPhases,segmentPhases,seconds} from '../src/executionTiming.js';
test('legacy timing is absent rather than zero or inferred',()=>{
  assert.equal(taskTiming({segments:[]}).forward,null);
  assert.deepEqual(sessionPhases({workload:{},created:1,ready_at:5}).phases,[]);
  assert.deepEqual(segmentPhases({started:1,finished:8}),[]);
  assert.equal(seconds(null),'未采集');
});
test('cache hit does not become a pull interval',()=>{
  const {phases,life}=sessionPhases({workload:{pod_uid:'p',lifecycle:{p:{pod_uid:'p',image_cached:true,image_ready_at:2,container_started_at:3}}}});
  assert.equal(life.image_cached,true);
  assert.equal(phases.some(p=>p.name==='镜像拉取'),false);
  assert.equal(phases[0].seconds,1);
});
test('effective results only and frame-weighted forward; parallel sums stay separate',()=>{
  const result=(id,n,s)=>({attempt_id:id,sampled_frames:n,segment_timing:{version:1},
      analysis_timing:{inference_seconds:s,analysis_seconds:s+1},inference_runtime:{actual_backend:'cuda'}});
  const t={segments:[{status:'SUCCEEDED',active_attempt:'a',result:result('a',10,1)},
      {status:'SUCCEEDED',active_attempt:'b',result:result('b',30,2)},{status:'RUNNING',result:result('old',100,100)}]};
  const v=taskTiming(t);
  assert.equal(v.measured,2);assert.equal(v.perFrameMs,75);assert.equal(v.forward,3);
  assert.equal(v.backends,'GPU / CUDA');
});
test('invalid durations cannot produce Infinity, NaN or negative intervals',()=>{
  assert.equal(seconds(NaN),'未采集');assert.equal(seconds(-1),'未采集');assert.equal(seconds(0),'0.000 s');
  assert.deepEqual(sessionPhases({workload:{job_submit_started_at:3,job_submit_finished_at:2}}).phases,[]);
});
