"""Validate the runtime evidence against the frozen application contract."""
from fastapi import HTTPException


def validate_inference(workload, evidence):
    backend = workload.get('inference_backend')
    if backend is None:
        return  # Historical CPU images predate this protocol.
    expected = 'CUDAExecutionProvider' if backend == 'cuda' else 'CPUExecutionProvider'
    if not isinstance(evidence, dict) or evidence.get('backend') != backend or evidence.get('provider') != expected or evidence.get('model_sha256') != workload['model_sha256']:
        raise HTTPException(422, '实际推理后端或模型与冻结应用不一致')
    if backend == 'cuda':
        gpu = evidence.get('gpu') or {}
        events = evidence.get('gpu_compute_events', 0)
        if not isinstance(events, int) or isinstance(events, bool) or events <= 0 or not gpu.get('uuid') or gpu.get('visible_device_count') != 1:
            raise HTTPException(422, '缺少实际 GPU 推理与唯一设备证据')
        if workload.get('expected_gpu_uuid') and gpu['uuid'] != workload['expected_gpu_uuid']:
            raise HTTPException(422, '实际 GPU UUID 与分配不一致')
