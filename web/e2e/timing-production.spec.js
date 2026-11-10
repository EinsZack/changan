import {test,expect} from '@playwright/test';
import fs from 'node:fs';
test('production GPU video with four reusable sessions and preserved historical timing',async({page,request})=>{
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 if(process.env.CHANGAN_PRODUCTION_TASK_ID){
   await page.goto('/?page=detail&task='+process.env.CHANGAN_PRODUCTION_TASK_ID);
 }else{
   await page.goto('/');
   await page.getByRole('button',{name:'新建分析任务',exact:true}).click();
   await page.getByLabel('任务名称',{exact:true}).fill('GPU分段计时正式验证 · 四lab');
   await page.getByRole('button',{name:'创建并运行',exact:true}).click();
 }
 await expect(page.locator('.task-summary').getByText('已完成',{exact:true})).toBeVisible({timeout:180000});
 const panel=page.getByRole('region',{name:'启动与推理分段计时'});
 await expect(panel).toContainText('GPU / CUDA');
 await expect(panel.locator('.timing-startup-row')).toHaveCount(4);
 await expect(panel.locator('.timing-segments details')).toHaveCount(6);
 await panel.getByText('启动阶段明细与设备',{exact:true}).click();
 await expect(panel).toContainText('NVIDIA GeForce RTX 5090');
 await panel.locator('.timing-segments summary').first().click();
 await page.screenshot({path:'../docs/gpu-timing-20261110/production-timing.png',fullPage:true});
 await page.getByRole('button',{name:'容器与镜像',exact:true}).click();
 await expect(page.locator('.session-summary')).toContainText('模型加载 4 次');
 await expect(page.locator('.session-summary')).toContainText('已回收 4 / 4',{timeout:30000});
 await expect(page.locator('.session-table')).toContainText('GPU / CUDA');
 const [download]=await Promise.all([page.waitForEvent('download'),page.getByRole('link',{name:'导出证据',exact:true}).click()]);
 await download.saveAs('../docs/gpu-timing-20261110/production-export.json');
 const t=JSON.parse(fs.readFileSync('../docs/gpu-timing-20261110/production-export.json','utf8'));
 expect(t.result.sampled_frames).toBe(48);expect(t.sessions).toHaveLength(4);
 expect(new Set(t.attempts.map(a=>a.workload.pod_uid)).size).toBe(4);
 expect(t.sessions.every(s=>s.model_load_count===1&&s.cleanup_state==='DONE')).toBe(true);
 expect(t.sessions.every(s=>s.workload.inference_runtime.actual_backend==='cuda'&&s.workload.inference_runtime.cuda_compute_events>0)).toBe(true);
 expect(t.segments.every(s=>s.result.analysis_timing.inference_seconds>=0&&s.result.segment_timing.version===1)).toBe(true);
 await page.getByRole('button',{name:'传输与吞吐',exact:true}).click();
 await expect(page.getByRole('region',{name:'片段执行实测明细'}).locator('tbody tr')).toHaveCount(6);
 await page.screenshot({path:'../docs/gpu-timing-20261110/production-network.png',fullPage:true});
 await page.getByRole('button',{name:'分析结果',exact:true}).click();
 await expect(page.locator('.preview-grid img').first()).toBeVisible();
 expect(await page.locator('.preview-grid img').first().evaluate(el=>el.complete&&el.naturalWidth>0)).toBe(true);
 await page.setViewportSize({width:390,height:844});
 await page.getByRole('button',{name:'工作流与执行',exact:true}).click();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
 await page.screenshot({path:'../docs/gpu-timing-20261110/production-mobile.png',fullPage:true});
 await page.goto('/?page=detail&task=task_11568a1701b84c54');
 await expect(page.getByRole('region',{name:'启动与推理分段计时'}).getByRole('status')).toContainText('未采集启动分段计时');
 expect(errors).toEqual([]);
});
