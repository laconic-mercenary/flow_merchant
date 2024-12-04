import hashlib

def hash(target: str, times: int = 10) -> str:
    if times < 0:
        raise ValueError("times must be >= 0")
    while times > 0:
        target = hashlib.sha256(target.encode("utf-8")).hexdigest()
        times -= 1
    return target