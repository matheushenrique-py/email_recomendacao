import base64
from io import BytesIO

def base64_to_buffer(bases):
    buffer = {}
    for base in bases:
        buffer[base] = BytesIO(base64.b64decode(str(bases[base]).split(',')[-1]))
#         buffer[base] = BytesIO(base64.b64decode(bases[base]))
    return buffer