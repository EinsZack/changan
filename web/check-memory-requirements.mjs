import {chromium,expect} from '@playwright/test';
import fs from 'node:fs';
const base=process.env.CHANGAN_TEST_BASE||'http://127.0.0.1:18410';
const out='../tmp/memory-limit-fix-20261104';fs.mkdirSync(out,{recursive:true});
const browser=await chromium.launch();
try {
 const context=await browser.newContext({viewport:{width:1500,height:1000},deviceScaleFactor:2,timezoneId:'Asia/Shanghai'});
 const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await expect.poll(async()=>{try{const r=await context.request.get(base+'/api/nodes');return (await r.json()).filter(n=>n.online).length;}catch{return 0;}},{timeout:30000}).toBe(4);
 await page.goto(base+'/?page=candidates');
 await expect(page.locator('.weekly')).toContainText('4 / 4 个节点满足要求');
 const memory=page.getByLabel('最低内存',{exact:true});
 await expect(memory).not.toHaveAttribute('max',/.+/);
 const records=[];
 for(const required of [131072,999999]){
  await memory.fill(String(required));expect(await memory.evaluate(e=>e.checkValidity())).toBe(true);
  const reply=page.waitForResponse(r=>r.url().endsWith('/api/scheduling/candidates')&&r.request().method()==='POST');
  await page.getByRole('button',{name:'筛选候选节点',exact:true}).click();
  const response=await reply;expect(response.status()).toBe(200);const result=await response.json();
  expect(result.requirements.min_memory_mb).toBe(required);
  for(const n of result.candidates)expect(n.reasons.includes('可用内存不足')).toBe(n.snapshot.available_memory_mb<required);
  if(required===131072)expect(result.candidates.filter(n=>n.eligible)).toHaveLength(4);
  await expect(page.locator('.weekly .error')).toHaveCount(0);
  await page.locator('.weekly').screenshot({path:out+'/memory-'+required+'.png'});
  records.push({required,status:response.status(),eligible:result.candidates.filter(n=>n.eligible).length,nodes:result.candidates.map(n=>({id:n.node_id,available:n.snapshot.available_memory_mb,reasons:n.reasons}))});
 }
 await memory.fill('63');expect(await memory.evaluate(e=>e.checkValidity())).toBe(false);
 await memory.fill('');expect(await memory.evaluate(e=>e.checkValidity())).toBe(false);
 await page.goto(base+'/?page=tasks');await page.getByRole('button',{name:'新建分析任务',exact:true}).click();
 await page.getByText('高级执行参数',{exact:true}).click();
 const taskMemory=page.getByLabel('最低可用内存（MiB）',{exact:true});
 await taskMemory.fill('999999');expect(await taskMemory.evaluate(e=>e.checkValidity())).toBe(true);
 await taskMemory.fill('131072');await page.getByLabel('任务名称',{exact:true}).fill('内存上限修复 浏览器验证');
 const createdReply=page.waitForResponse(r=>r.url().endsWith('/api/tasks')&&r.request().method()==='POST');
 await page.getByRole('button',{name:'创建并运行',exact:true}).click();
 const created=await createdReply;expect(created.status()).toBe(200);const taskId=(await created.json()).id;
 let task;
 await expect.poll(async()=>{task=await (await context.request.get(base+'/api/tasks/'+taskId)).json();return task.status;},{timeout:120000}).toBe('SUCCEEDED');
 expect(task.params.min_memory_mb).toBe(131072);expect(task.result.sampled_frames).toBe(48);
 expect(errors).toEqual([]);
 const report={date:'2026-11-04',environment:'isolated server processes',records,new_task:{id:taskId,min_memory_mb:task.params.min_memory_mb,status:task.status,frames:task.result.sampled_frames},invalid_input_checks:['63 rejected','empty rejected'],browser_errors:errors};
 fs.writeFileSync(out+'/browser-report.json',JSON.stringify(report,null,2));console.log(JSON.stringify(report));
} finally {await browser.close();}
