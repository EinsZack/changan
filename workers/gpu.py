"""CUDA driver identity without depending on nvidia-smi (also usable on ARM64)."""
import ctypes
import uuid


def device_identity():
    cuda = ctypes.CDLL('libcuda.so.1')
    def call(name, *args):
        status = getattr(cuda, name)(*args)
        if status:
            raise RuntimeError(f'{name} failed: CUDA status {status}')
    call('cuInit', 0)
    count = ctypes.c_int()
    call('cuDeviceGetCount', ctypes.byref(count))
    if count.value != 1:
        raise RuntimeError('任务需要恰好一个可见 GPU，实际为 ' + str(count.value))
    device = ctypes.c_int()
    call('cuDeviceGet', ctypes.byref(device), 0)
    name = ctypes.create_string_buffer(256)
    call('cuDeviceGetName', name, 256, device)
    raw = (ctypes.c_ubyte * 16)()
    call('cuDeviceGetUuid', ctypes.byref(raw), device)
    memory = ctypes.c_size_t()
    call('cuDeviceTotalMem_v2', ctypes.byref(memory), device)
    major, minor, driver = ctypes.c_int(), ctypes.c_int(), ctypes.c_int()
    call('cuDeviceComputeCapability', ctypes.byref(major), ctypes.byref(minor), device)
    call('cuDriverGetVersion', ctypes.byref(driver))
    return {'uuid': 'GPU-' + str(uuid.UUID(bytes=bytes(raw))), 'name': name.value.decode(),
            'device_id': 0, 'visible_device_count': count.value, 'memory_total_mb': memory.value // 1048576,
            'compute_capability': f'{major.value}.{minor.value}', 'cuda_driver_version': driver.value}
