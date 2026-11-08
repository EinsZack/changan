"""Approved per-architecture inference images; tasks freeze their execution contract."""
import hashlib
import os
import re
from .config import MODEL, MODEL_ID

BACKENDS = {'cpu': 'CPUExecutionProvider', 'cuda': 'CUDAExecutionProvider'}


def application(params=None):
    params = params or {}
    backend = params.get('inference_backend', 'cpu')
    if backend not in BACKENDS:
        raise ValueError('不支持的推理后端')
    images = {}
    key = 'CHANGAN_VIDEO_IMAGE' if backend == 'cpu' else 'CHANGAN_VIDEO_CUDA_IMAGE'
    arch = os.getenv('CHANGAN_IMAGE_ARCH' if backend == 'cpu' else 'CHANGAN_CUDA_IMAGE_ARCH', 'amd64')
    for architecture, image in [(arch, os.getenv(key, '')),
                                ('arm64', os.getenv(key + '_ARM64', ''))]:
        if not image:
            continue
        if not re.fullmatch(r'[^\s@]+@sha256:[0-9a-f]{64}', image):
            raise ValueError(key + ' 必须使用已验证的镜像 sha256 摘要')
        images[architecture] = image
    if not images:
        raise ValueError('尚未配置 ' + backend.upper() + ' 推理镜像；请选择已配置后端')
    requested_arch = params.get('architecture')
    aliases = {'x86_64': 'amd64', 'aarch64': 'arm64'}
    requested_arch = aliases.get(requested_arch, requested_arch)
    if requested_arch:
        if requested_arch not in images:
            raise ValueError('所选推理后端没有经验证的 ' + requested_arch + ' 镜像')
        images = {requested_arch: images[requested_arch]}
    first = next(iter(images))
    return {'id': 'video-analysis', 'version': '3', 'execution_mode': 'task_session',
            'image': images[first], 'architecture': first, 'images': images,
            'inference_backend': backend, 'expected_provider': BACKENDS[backend],
            'runtime': 'ONNX Runtime ' + ('CUDA' if backend == 'cuda' else 'CPU'),
            'gpu_count': int(backend == 'cuda'), 'host_memory_min_mb': 2048 if backend == 'cuda' else 1024,
            'gpu_memory_mb': params.get('min_gpu_memory_mb', 2048) if backend == 'cuda' else 0,
            'model_id': MODEL_ID, 'model_sha256': hashlib.sha256(MODEL.read_bytes()).hexdigest(),
            'image_pull_secret': os.getenv('CHANGAN_IMAGE_PULL_SECRET')}


def bind_node(app, snapshot):
    images = app.get('images')
    if images:
        arch = snapshot['architecture']
        if arch not in images:
            raise ValueError('节点架构与冻结镜像不兼容')
        return app | {'image': images[arch], 'architecture': arch}
    return app


def scheduling_params(params, app):
    if not app:
        return params
    return params | {'architecture': {'x86_64':'amd64','aarch64':'arm64'}.get(params.get('architecture'),params.get('architecture')), 'inference_backend': app.get('inference_backend', 'cpu'),
                     'application_architectures': list(app.get('images', {app['architecture']: app['image']})),
                     'application_images': app.get('images', {app['architecture']: app['image']}),
                     'model_sha256': app['model_sha256'], 'host_memory_min_mb': app.get('host_memory_min_mb',1024)}
